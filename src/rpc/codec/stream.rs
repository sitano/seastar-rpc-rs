//! Stream frames (wire guide §6, §9).
//!
//! A stream connection carries only `u32` LE length + payload frames, with
//! no `msg_id` or verb. A length of `0xFFFFFFFF` with no payload is the end
//! of stream (EOS) in the writer's direction. The codec encodes and decodes,
//! so it serves both ends of a stream.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::rpc::codec::frame::FrameLayout;
use crate::rpc::codec::serialize::Encode;
use crate::rpc::error::{Error, Result};

/// Length value marking the end of stream.
const EOS: u32 = u32::MAX;

/// One message on a stream connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamFrame {
    Data(Bytes),
    /// The writer closed its direction; nothing follows from it.
    Eos,
}

/// Codec for [`StreamFrame`]s. Only `layout.max_frame_size` applies: stream
/// frames carry no timeout or handler duration.
#[derive(Debug, Clone)]
pub struct StreamCodec {
    layout: FrameLayout,
}

impl StreamCodec {
    pub fn new(layout: FrameLayout) -> Self {
        StreamCodec { layout }
    }
}

impl Encoder<StreamFrame> for StreamCodec {
    type Error = Error;

    fn encode(&mut self, frame: StreamFrame, dst: &mut BytesMut) -> Result<()> {
        match frame {
            StreamFrame::Eos => dst.put_u32_le(EOS),
            StreamFrame::Data(payload) => {
                dst.reserve(4 + payload.len());
                dst.put_u32_le(data_len(payload.len())?);
                dst.put_slice(&payload);
            }
        }

        Ok(())
    }
}

/// Encodes a value straight into the frame, with no payload buffer between.
impl<T: Encode + ?Sized> Encoder<&T> for StreamCodec {
    type Error = Error;

    fn encode(&mut self, item: &T, dst: &mut BytesMut) -> Result<()> {
        let header = dst.len();

        // Length, patched once the payload is written.
        dst.put_u32_le(0);
        item.encode(dst);

        // Drop the partial frame on error, so later frames don't follow garbage.
        let len = data_len(dst.len() - header - 4).inspect_err(|_| dst.truncate(header))?;

        dst[header..header + 4].copy_from_slice(&len.to_le_bytes());

        Ok(())
    }
}

/// Wire length of a data payload. `u32::MAX` is taken by EOS.
fn data_len(len: usize) -> Result<u32> {
    match u32::try_from(len) {
        Ok(n) if n != EOS => Ok(n),
        _ => Err(Error::Protocol(format!(
            "stream payload of {len} bytes is too long"
        ))),
    }
}

impl Decoder for StreamCodec {
    type Item = StreamFrame;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<StreamFrame>> {
        let Some(len) = src.first_chunk::<4>().map(|b| u32::from_le_bytes(*b)) else {
            src.reserve(4 - src.len());
            return Ok(None);
        };

        if len == EOS {
            src.advance(4);
            return Ok(Some(StreamFrame::Eos));
        }

        let len = len as usize;

        self.layout.check_len(len)?;

        if src.len() < 4 + len {
            src.reserve(4 + len - src.len());
            return Ok(None);
        }

        src.advance(4);

        Ok(Some(StreamFrame::Data(src.split_to(len).freeze())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::codec::serialize::to_payload;

    fn codec() -> StreamCodec {
        StreamCodec::new(FrameLayout {
            timeout: true,
            handler_duration: true,
            max_frame_size: 1024,
        })
    }

    #[test]
    fn exact_bytes() {
        let mut buf = BytesMut::new();
        let mut c = codec();

        c.encode(StreamFrame::Data(Bytes::from_static(b"abc")), &mut buf)
            .unwrap();
        c.encode(StreamFrame::Eos, &mut buf).unwrap();

        assert_eq!(&buf[..], b"\x03\0\0\0abc\xff\xff\xff\xff");
        assert_eq!(
            c.decode(&mut buf).unwrap(),
            Some(StreamFrame::Data(Bytes::from_static(b"abc")))
        );
        assert_eq!(c.decode(&mut buf).unwrap(), Some(StreamFrame::Eos));
        assert_eq!(c.decode(&mut buf).unwrap(), None);
    }

    #[test]
    fn typed_matches_payload() {
        let value = (7i32, "hi");
        let mut typed = BytesMut::new();
        let mut plain = BytesMut::new();
        let mut c = codec();

        c.encode(&value, &mut typed).unwrap();
        c.encode(StreamFrame::Data(to_payload(&value)), &mut plain)
            .unwrap();

        assert_eq!(typed, plain);
    }

    #[test]
    fn roundtrip_byte_by_byte() {
        let mut c = codec();
        let mut wire = BytesMut::new();

        c.encode(&42u64, &mut wire).unwrap();

        let mut src = BytesMut::new();

        for (i, b) in wire.iter().enumerate() {
            src.put_u8(*b);
            let out = c.decode(&mut src).unwrap();
            if i + 1 < wire.len() {
                assert!(out.is_none());
            } else {
                assert_eq!(out, Some(StreamFrame::Data(to_payload(&42u64))));
            }
        }

        assert!(src.is_empty());
    }

    #[test]
    fn empty_payload_is_data() {
        let mut buf = BytesMut::from(&[0u8, 0, 0, 0][..]);

        assert_eq!(
            codec().decode(&mut buf).unwrap(),
            Some(StreamFrame::Data(Bytes::new()))
        );
    }

    #[test]
    fn oversized_frame_rejected() {
        let mut buf = BytesMut::new();
        buf.put_u32_le(4096);

        assert!(matches!(codec().decode(&mut buf), Err(Error::Protocol(_))));
    }
}
