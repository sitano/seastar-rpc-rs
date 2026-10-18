//! Compression envelope (wire guide §5).
//!
//! Once `COMPRESS` is negotiated, every frame travels as a `u32` LE length
//! followed by compressed data that decompresses to exactly one frame.
//! [`CompressionCodec`] wraps a frame codec and adds the envelope in both
//! directions; without a negotiated algorithm it passes frames through.
//!
//! Buffers and the LZ4 hash table are kept between frames, so a warm codec
//! allocates nothing per frame.

use std::fmt;

use bytes::{Buf, BufMut, BytesMut};
use lz4_flex::block::{
    CompressTable, compress_into_with_table, decompress_into_with_dict, get_maximum_output_size,
};
use tokio_util::codec::{Decoder, Encoder};

use crate::rpc::codec::frame::FrameLayout;
use crate::rpc::error::{Error, Result};

/// Largest frame header: a request with a timeout.
const MAX_FRAME_HEADER: usize = 28;
/// `LZ4_FRAGMENTED` chunk size: every chunk but the last decompresses to it.
const CHUNK: usize = 32 * 1024;
/// `LZ4_FRAGMENTED` chunk header bit marking the last chunk.
const LAST_CHUNK: u32 = 1 << 31;
/// Least the decompression buffer grows by, so one buffer holds many small
/// frames, like a `FramedRead` buffer does.
const OUT_CHUNK: usize = 8 * 1024;
/// Largest encode buffer kept between frames; one outlier frame shouldn't
/// pin its size for the life of the connection.
const MAX_KEPT_FRAME: usize = 4 * CHUNK;

/// A compression algorithm named in the `COMPRESS` feature (wire guide §4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Compression {
    /// `LZ4`: the decompressed size as `u32`, then one LZ4 block.
    Lz4,
    /// `LZ4_FRAGMENTED`: 32 KiB chunks, each after a `u32` header.
    Lz4Fragmented,
}

impl Compression {
    /// The name used in the `COMPRESS` feature.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Compression::Lz4 => "LZ4",
            Compression::Lz4Fragmented => "LZ4_FRAGMENTED",
        }
    }

    /// The algorithm a `COMPRESS` feature names, if this crate supports it.
    #[must_use]
    pub fn from_name(name: &[u8]) -> Option<Self> {
        match name {
            b"LZ4" => Some(Compression::Lz4),
            b"LZ4_FRAGMENTED" => Some(Compression::Lz4Fragmented),
            _ => None,
        }
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Wraps a frame codec in the compression envelope.
#[derive(Debug)]
pub struct CompressionCodec<C> {
    inner: C,
    /// Largest decompressed frame accepted: payload limit plus header.
    max_frame: usize,
    /// `None` when compression is off.
    state: Option<State>,
}

struct State {
    algo: Compression,
    table: CompressTable,
    /// Plain frame being encoded, before compression.
    frame: BytesMut,
    /// Decompressed frames; decoded payloads are split off it.
    out: BytesMut,
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("State")
            .field("algo", &self.algo)
            .finish_non_exhaustive()
    }
}

impl<C> CompressionCodec<C> {
    /// Wrap `inner` in the envelope for `compression`, or pass frames through
    /// when it is `None`. `inner` must encode and decode exactly one frame per
    /// call: an envelope holding none or several is a protocol error.
    /// Decompressed frames are limited to `layout.max_frame_size` plus the
    /// largest frame header.
    pub fn new(inner: C, layout: FrameLayout, compression: Option<Compression>) -> Self {
        CompressionCodec {
            inner,
            max_frame: layout.max_frame_size.saturating_add(MAX_FRAME_HEADER),
            state: compression.map(|algo| State {
                algo,
                table: CompressTable::default(),
                frame: BytesMut::new(),
                out: BytesMut::new(),
            }),
        }
    }

    /// The negotiated algorithm, if any.
    pub fn compression(&self) -> Option<Compression> {
        self.state.as_ref().map(|s| s.algo)
    }
}

impl<T, C: Encoder<T, Error = Error>> Encoder<T> for CompressionCodec<C> {
    type Error = Error;

    fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<()> {
        let Some(state) = &mut self.state else {
            return self.inner.encode(item, dst);
        };

        // Encode the plain frame aside, then compress it into `dst`.
        state.frame.clear();
        self.inner.encode(item, &mut state.frame)?;

        // Checked before writing, so an error leaves `dst` untouched.
        let size = wire_u32(state.frame.len())?;
        let envelope = dst.len();

        // Envelope length, patched once the data is written.
        dst.put_u32_le(0);

        match state.algo {
            Compression::Lz4 => {
                dst.put_u32_le(size);
                compress(&state.frame, dst, &mut state.table);
            }
            Compression::Lz4Fragmented => {
                // shortcut: chunks are compressed without the previous chunk as
                // a dictionary (valid for the C++ streaming decoder, a bit
                // larger on the wire for frames over 32 KiB), because lz4_flex
                // allocates a hash table per call when given a dictionary;
                // upgrade once it can reuse a table with a dictionary.
                let mut chunks = state.frame.chunks(CHUNK).peekable();

                while let Some(chunk) = chunks.next() {
                    let header = dst.len();

                    dst.put_u32_le(0);

                    let n = compress(chunk, dst, &mut state.table);

                    // A middle chunk records its compressed size, the last one
                    // its decompressed size.
                    let value = if chunks.peek().is_some() {
                        n as u32
                    } else {
                        LAST_CHUNK | chunk.len() as u32
                    };

                    dst[header..header + 4].copy_from_slice(&value.to_le_bytes());
                }
            }
        }

        // Drop the partial envelope on error, so later frames don't follow
        // garbage.
        let len = wire_u32(dst.len() - envelope - 4).inspect_err(|_| dst.truncate(envelope))?;

        dst[envelope..envelope + 4].copy_from_slice(&len.to_le_bytes());

        // Frames above the limit allocate each time, which is small next to
        // compressing them.
        if state.frame.capacity() > MAX_KEPT_FRAME {
            state.frame = BytesMut::new();
        }

        Ok(())
    }
}

impl<C: Decoder<Error = Error>> Decoder for CompressionCodec<C> {
    type Item = C::Item;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<C::Item>> {
        let Some(state) = &mut self.state else {
            return self.inner.decode(src);
        };

        loop {
            let Some(len) = src
                .first_chunk::<4>()
                .map(|b| u32::from_le_bytes(*b) as usize)
            else {
                src.reserve(4 - src.len());
                return Ok(None);
            };

            // Bound the length before reserving room for it.
            if len > max_envelope_len(self.max_frame) {
                return Err(Error::Protocol(format!(
                    "compressed frame of {len} bytes exceeds the frame size limit"
                )));
            }

            if src.len() < 4 + len {
                src.reserve(4 + len - src.len());
                return Ok(None);
            }

            // Data too short to hold a size is an empty frame a compressor
            // sent its peer out of band; skip it (wire guide §5).
            if len < 4 {
                src.advance(4 + len);
                continue;
            }

            state.decompress(&src[4..4 + len], self.max_frame)?;
            src.advance(4 + len);

            // The skip rule is on the decompressed size, so a fragmented
            // `LAST_CHUNK | 0` with an empty LZ4 block is skipped too.
            if state.out.is_empty() {
                continue;
            }

            let frame = self.inner.decode(&mut state.out)?;

            if frame.is_none() || !state.out.is_empty() {
                return Err(Error::Protocol(
                    "compressed frame must hold exactly one frame".into(),
                ));
            }

            return Ok(frame);
        }
    }
}

impl State {
    /// Decompress envelope `data`, at least 4 bytes, into the empty `out`.
    fn decompress(&mut self, data: &[u8], max_frame: usize) -> Result<()> {
        let size = match self.algo {
            Compression::Lz4 => match data.first_chunk::<4>().map(|b| u32::from_le_bytes(*b)) {
                None => return Err(Error::Protocol("truncated LZ4 frame".into())),
                Some(0) => return Err(Error::Protocol("LZ4 frame with zero size".into())),
                Some(n) => n as usize,
            },
            Compression::Lz4Fragmented => fragmented_len(data)?,
        };

        if size > max_frame {
            return Err(Error::Protocol(format!(
                "decompressed frame of {size} bytes exceeds limit of {max_frame} bytes"
            )));
        }

        // `resize` alone would reserve exactly `size`, so each new buffer
        // would hold a single frame.
        if self.out.capacity() < size {
            self.out.reserve(size.max(OUT_CHUNK));
        }

        // lz4_flex writes into an initialized slice.
        self.out.resize(size, 0);

        let res = match self.algo {
            Compression::Lz4 => decompress_exact(&data[4..], &mut self.out, &[]),
            Compression::Lz4Fragmented => decompress_chunks(data, &mut self.out),
        };

        if res.is_err() {
            self.out.clear();
        }

        res
    }
}

/// Decompressed size of an `LZ4_FRAGMENTED` frame, from its chunk headers.
fn fragmented_len(mut data: &[u8]) -> Result<usize> {
    let mut size: usize = 0;

    loop {
        let Some((header, rest)) = data.split_first_chunk::<4>() else {
            return Err(Error::Protocol("truncated LZ4_FRAGMENTED frame".into()));
        };

        let header = u32::from_le_bytes(*header);

        if header & LAST_CHUNK != 0 {
            return Ok(size.saturating_add((header & !LAST_CHUNK) as usize));
        }

        let Some(tail) = rest.get(header as usize..) else {
            return Err(Error::Protocol("truncated LZ4_FRAGMENTED chunk".into()));
        };

        data = tail;
        size = size.saturating_add(CHUNK);
    }
}

/// Decompress the chunks of an `LZ4_FRAGMENTED` frame validated by
/// [`fragmented_len`] into `out`, sized to its total. Each chunk may refer
/// back to the ones before it (LZ4 streaming).
fn decompress_chunks(mut data: &[u8], out: &mut [u8]) -> Result<()> {
    let mut pos = 0;

    loop {
        // `fragmented_len` already checked these; an error beats a panic if
        // the two ever disagree.
        let Some((header, rest)) = data.split_first_chunk::<4>() else {
            return Err(Error::Protocol("truncated LZ4_FRAGMENTED frame".into()));
        };

        let header = u32::from_le_bytes(*header);
        let (dict, dst) = out.split_at_mut(pos);

        if header & LAST_CHUNK != 0 {
            return decompress_exact(rest, dst, dict);
        }

        let Some((chunk, tail)) = rest.split_at_checked(header as usize) else {
            return Err(Error::Protocol("truncated LZ4_FRAGMENTED chunk".into()));
        };

        decompress_exact(chunk, &mut dst[..CHUNK], dict)?;

        data = tail;
        pos += CHUNK;
    }
}

/// Decompress one block into all of `dst`.
fn decompress_exact(block: &[u8], dst: &mut [u8], dict: &[u8]) -> Result<()> {
    let n = decompress_into_with_dict(block, dst, dict)
        .map_err(|e| Error::Protocol(format!("LZ4 decompression failed: {e}")))?;

    if n != dst.len() {
        return Err(Error::Protocol(format!(
            "LZ4 block decompressed to {n} bytes, expected {}",
            dst.len()
        )));
    }

    Ok(())
}

/// Append the LZ4 block of `input` to `dst`; returns its size.
fn compress(input: &[u8], dst: &mut BytesMut, table: &mut CompressTable) -> usize {
    let start = dst.len();

    // lz4_flex writes into an initialized slice: make room for the worst
    // case, then trim.
    dst.resize(start + get_maximum_output_size(input.len()), 0);

    let n = compress_into_with_table(input, &mut dst[start..], table)
        .expect("output sized by get_maximum_output_size");

    dst.truncate(start + n);

    n
}

/// Worst-case envelope data for a frame of `frame` bytes, chunk headers
/// included.
fn max_envelope_len(frame: usize) -> usize {
    let chunks = frame / CHUNK + 1;

    get_maximum_output_size(frame).saturating_add(chunks * (get_maximum_output_size(0) + 4))
}

fn wire_u32(n: usize) -> Result<u32> {
    u32::try_from(n).map_err(|_| Error::Protocol(format!("frame of {n} bytes is too large")))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::rpc::codec::frame::{RequestCodec, RequestFrame, ResponseCodec, ResponseFrame};

    fn layout() -> FrameLayout {
        FrameLayout {
            timeout: true,
            handler_duration: true,
            max_frame_size: 1 << 20,
        }
    }

    fn codec(algo: Compression) -> CompressionCodec<ResponseCodec> {
        CompressionCodec::new(ResponseCodec::new(layout()), layout(), Some(algo))
    }

    fn response(msg_id: i64, payload: Vec<u8>) -> ResponseFrame {
        ResponseFrame {
            msg_id,
            handler_duration: Some(3),
            payload: Bytes::from(payload),
        }
    }

    /// Compressible but not trivial: 200 KiB spans several chunks.
    fn big() -> Vec<u8> {
        (0..200 * 1024u32).map(|i| (i / 7 % 251) as u8).collect()
    }

    const ALGOS: [Compression; 2] = [Compression::Lz4, Compression::Lz4Fragmented];

    #[test]
    fn names() {
        for algo in ALGOS {
            assert_eq!(Compression::from_name(algo.name().as_bytes()), Some(algo));
        }
        assert_eq!(Compression::from_name(b"ZSTD"), None);
    }

    #[test]
    fn roundtrip_small_and_large() {
        for algo in ALGOS {
            let mut c = codec(algo);
            let mut wire = BytesMut::new();
            let frames = [response(1, b"abc".to_vec()), response(-2, big())];

            for f in frames.clone() {
                c.encode(f, &mut wire).unwrap();
            }

            // The large frame really is compressed.
            assert!(wire.len() < 100 * 1024, "{algo:?}: {}", wire.len());

            for f in frames {
                assert_eq!(c.decode(&mut wire).unwrap(), Some(f));
            }

            assert_eq!(c.decode(&mut wire).unwrap(), None);
        }
    }

    /// Frames ending just below, at and just past a chunk boundary, so the
    /// last `LZ4_FRAGMENTED` chunk is short, exactly `CHUNK`, or one byte.
    #[test]
    fn roundtrip_chunk_boundaries() {
        let mut empty = BytesMut::new();
        ResponseCodec::new(layout())
            .encode(response(0, vec![]), &mut empty)
            .unwrap();
        let header = empty.len();

        for algo in ALGOS {
            let mut c = codec(algo);

            for frame in [CHUNK - 1, CHUNK, CHUNK + 1, 2 * CHUNK] {
                let f = response(9, big()[..frame - header].to_vec());
                let mut wire = BytesMut::new();

                c.encode(f.clone(), &mut wire).unwrap();

                assert_eq!(c.decode(&mut wire).unwrap(), Some(f), "{algo:?}: {frame}");
                assert!(wire.is_empty());
            }
        }
    }

    #[test]
    fn requests_roundtrip_byte_by_byte() {
        for algo in ALGOS {
            let mut c = CompressionCodec::new(RequestCodec::new(layout()), layout(), Some(algo));
            let frame = RequestFrame::new(7, 2, Bytes::from_static(b"hello hello hello"));
            let mut wire = BytesMut::new();

            c.encode(frame.clone(), &mut wire).unwrap();

            let mut src = BytesMut::new();

            for (i, b) in wire.iter().enumerate() {
                src.put_u8(*b);
                let out = c.decode(&mut src).unwrap();
                if i + 1 < wire.len() {
                    assert!(out.is_none());
                } else {
                    assert_eq!(out, Some(frame.clone()));
                }
            }
        }
    }

    #[test]
    fn empty_frames_skipped() {
        for algo in ALGOS {
            let mut c = codec(algo);
            let mut wire = BytesMut::new();

            wire.put_u32_le(0);
            c.encode(response(1, vec![1]), &mut wire).unwrap();
            wire.put_u32_le(3);
            wire.put_slice(&[1, 2, 3]);
            c.encode(response(2, vec![2]), &mut wire).unwrap();
            wire.put_u32_le(0);

            // A fragmented frame whose last and only chunk is empty.
            if algo == Compression::Lz4Fragmented {
                wire.put_u32_le(5);
                wire.put_u32_le(LAST_CHUNK);
                wire.put_u8(0);
            }

            assert_eq!(c.decode(&mut wire).unwrap().unwrap().msg_id, 1);
            assert_eq!(c.decode(&mut wire).unwrap().unwrap().msg_id, 2);
            assert_eq!(c.decode(&mut wire).unwrap(), None);
            assert!(wire.is_empty());
        }
    }

    /// C++ compresses each chunk against the ones before it; build such a
    /// frame by hand and check it decodes.
    #[test]
    fn fragmented_with_dictionary_references() {
        let mut plain = BytesMut::new();

        ResponseCodec::new(layout())
            .encode(response(5, big()), &mut plain)
            .unwrap();

        let mut data = BytesMut::new();
        let mut pos = 0;

        while plain.len() - pos > CHUNK {
            let block =
                lz4_flex::block::compress_with_dict(&plain[pos..pos + CHUNK], &plain[..pos]);
            data.put_u32_le(block.len() as u32);
            data.put_slice(&block);
            pos += CHUNK;
        }

        let block = lz4_flex::block::compress_with_dict(&plain[pos..], &plain[..pos]);
        data.put_u32_le(LAST_CHUNK | (plain.len() - pos) as u32);
        data.put_slice(&block);

        // Without the dictionary the later chunks don't decode, so this test
        // does exercise references back.
        let (_, first) =
            data.split_at(4 + u32::from_le_bytes(data[..4].try_into().unwrap()) as usize);
        let second_len = u32::from_le_bytes(first[..4].try_into().unwrap()) as usize;
        let mut scratch = vec![0; CHUNK];
        assert!(lz4_flex::block::decompress_into(&first[4..4 + second_len], &mut scratch).is_err());

        let mut wire = BytesMut::new();
        wire.put_u32_le(data.len() as u32);
        wire.put_slice(&data);

        let decoded = codec(Compression::Lz4Fragmented)
            .decode(&mut wire)
            .unwrap()
            .unwrap();

        assert_eq!(decoded, response(5, big()));
    }

    #[test]
    fn oversized_frame_rejected() {
        let mut wire = BytesMut::new();
        wire.put_u32_le(8);
        wire.put_u32_le(1 << 30);
        wire.put_u32_le(0);

        let err = codec(Compression::Lz4).decode(&mut wire).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)), "{err}");

        let mut wire = BytesMut::new();
        wire.put_u32_le(u32::MAX);

        let err = codec(Compression::Lz4).decode(&mut wire).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)), "{err}");
    }

    #[test]
    fn garbage_rejected() {
        for algo in ALGOS {
            let mut wire = BytesMut::new();
            wire.put_u32_le(12);
            wire.put_u32_le(LAST_CHUNK | 16);
            wire.put_slice(&[0xff; 8]);

            let err = codec(algo).decode(&mut wire).unwrap_err();
            assert!(matches!(err, Error::Protocol(_)), "{algo:?}: {err}");
        }
    }

    /// Empty middle chunks claim far more data than the limit allows.
    #[test]
    fn fragmented_empty_chunks_rejected() {
        let mut wire = BytesMut::new();
        let chunks = 64;

        wire.put_u32_le((chunks + 1) * 4);
        for _ in 0..chunks {
            wire.put_u32_le(0);
        }
        wire.put_u32_le(LAST_CHUNK);

        let err = codec(Compression::Lz4Fragmented)
            .decode(&mut wire)
            .unwrap_err();
        assert!(matches!(err, Error::Protocol(_)), "{err}");
    }

    #[test]
    fn two_frames_in_one_envelope_rejected() {
        let mut plain = BytesMut::new();
        let mut inner = ResponseCodec::new(layout());

        inner.encode(response(1, vec![]), &mut plain).unwrap();
        inner.encode(response(2, vec![]), &mut plain).unwrap();

        let block = lz4_flex::block::compress(&plain);
        let mut wire = BytesMut::new();
        wire.put_u32_le(4 + block.len() as u32);
        wire.put_u32_le(plain.len() as u32);
        wire.put_slice(&block);

        let err = codec(Compression::Lz4).decode(&mut wire).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)), "{err}");
    }

    #[test]
    fn off_passes_through() {
        let mut c = CompressionCodec::new(ResponseCodec::new(layout()), layout(), None);
        let mut wire = BytesMut::new();

        c.encode(response(1, b"abc".to_vec()), &mut wire).unwrap();

        assert_eq!(&wire[..8], &1i64.to_le_bytes());
        assert_eq!(
            c.decode(&mut wire).unwrap(),
            Some(response(1, b"abc".to_vec()))
        );
    }
}
