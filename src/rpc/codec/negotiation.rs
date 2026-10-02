//! Negotiation frame (wire guide §3, §4).
//!
//! Both sides send one negotiation frame right after connect/accept:
//! `b"SSTARRPC"`, a `u32` LE length, then feature records
//! (`u32` feature number, `u32` length, payload). The same encoder and
//! decoder serve the client's offer and the server's reply.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::rpc::error::{Error, Result};

pub const MAGIC: &[u8; 8] = b"SSTARRPC";

/// Upper bound on the records section; real frames are a few dozen bytes.
const MAX_NEGOTIATION_LEN: u32 = 64 * 1024;

const COMPRESS: u32 = 0;
const TIMEOUT: u32 = 1;
const CONNECTION_ID: u32 = 2;
const STREAM_PARENT: u32 = 3;
const ISOLATION: u32 = 4;
const HANDLER_DURATION: u32 = 5;

/// Feature set carried by one negotiation frame.
///
/// A client fills this in as its offer; the server's reply, parsed into the
/// same type, is what was actually negotiated and decides the frame layout.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Features {
    /// `COMPRESS`: compressor name list (offer) or chosen name (reply).
    pub compress: Option<Bytes>,
    /// `TIMEOUT`: request frames carry a leading `u64` timeout in ms.
    pub timeout: bool,
    /// `CONNECTION_ID`: server-assigned id of this connection.
    pub connection_id: Option<u64>,
    /// `STREAM_PARENT`: marks a stream connection belonging to this parent id.
    pub stream_parent: Option<u64>,
    /// `ISOLATION`: opaque cookie selecting a server scheduling group.
    pub isolation: Option<Bytes>,
    /// `HANDLER_DURATION`: response frames carry a `u32` handler time in µs.
    pub handler_duration: bool,
}

impl Features {
    /// Encode a complete negotiation frame, magic included.
    pub fn encode(&self) -> Bytes {
        let mut records = BytesMut::new();
        let mut record = |id: u32, data: &[u8]| {
            records.put_u32_le(id);
            records.put_u32_le(data.len() as u32);
            records.put_slice(data);
        };

        // Ascending feature order, like the reference implementation.
        if let Some(c) = &self.compress {
            record(COMPRESS, c);
        }
        if self.timeout {
            record(TIMEOUT, &[]);
        }
        if let Some(id) = self.connection_id {
            record(CONNECTION_ID, &id.to_le_bytes());
        }
        if let Some(id) = self.stream_parent {
            record(STREAM_PARENT, &id.to_le_bytes());
        }
        if let Some(cookie) = &self.isolation {
            record(ISOLATION, cookie);
        }
        if self.handler_duration {
            record(HANDLER_DURATION, &[]);
        }

        let mut frame = BytesMut::with_capacity(MAGIC.len() + 4 + records.len());
        frame.put_slice(MAGIC);
        frame.put_u32_le(records.len() as u32);
        frame.put_slice(&records);
        frame.freeze()
    }

    /// Parse the records section (everything after magic and length).
    /// Records may come in any order; unknown feature numbers are skipped.
    pub fn decode_records(mut data: Bytes) -> Result<Self> {
        let mut f = Features::default();

        while data.has_remaining() {
            if data.remaining() < 8 {
                return Err(Error::Protocol(
                    "truncated negotiation record header".into(),
                ));
            }

            let id = data.get_u32_le();
            let len = data.get_u32_le() as usize;

            if data.remaining() < len {
                return Err(Error::Protocol(format!(
                    "negotiation record {id} claims {len} bytes, {} left",
                    data.remaining()
                )));
            }

            let mut value = data.split_to(len);
            let mut connection_id = || {
                if value.len() != 8 {
                    return Err(Error::Protocol(format!(
                        "feature {id}: expected 8-byte connection id, got {} bytes",
                        value.len()
                    )));
                }
                Ok(value.get_u64_le())
            };

            match id {
                COMPRESS => f.compress = Some(value),
                TIMEOUT => f.timeout = true,
                CONNECTION_ID => f.connection_id = Some(connection_id()?),
                STREAM_PARENT => f.stream_parent = Some(connection_id()?),
                ISOLATION => f.isolation = Some(value),
                HANDLER_DURATION => f.handler_duration = true,
                _ => {}
            }
        }
        Ok(f)
    }
}

/// Write a negotiation frame.
pub async fn write_negotiation<W: AsyncWrite + Unpin>(
    io: &mut W,
    features: &Features,
) -> Result<()> {
    io.write_all(&features.encode()).await?;
    io.flush().await?;
    Ok(())
}

/// Read and parse the peer's negotiation frame.
pub async fn read_negotiation<R: AsyncRead + Unpin>(io: &mut R) -> Result<Features> {
    let mut header = [0u8; 12];

    io.read_exact(&mut header).await.map_err(eof_is_closed)?;

    if &header[..8] != MAGIC {
        return Err(Error::Protocol(format!(
            "bad negotiation magic {:?}",
            &header[..8]
        )));
    }

    let len = u32::from_le_bytes(header[8..].try_into().unwrap());

    if len > MAX_NEGOTIATION_LEN {
        return Err(Error::Protocol(format!(
            "negotiation frame too large: {len} bytes"
        )));
    }

    let mut data = vec![0u8; len as usize];
    io.read_exact(&mut data).await.map_err(eof_is_closed)?;

    Features::decode_records(data.into())
}

fn eof_is_closed(e: std::io::Error) -> Error {
    if e.kind() == std::io::ErrorKind::UnexpectedEof {
        Error::Closed
    } else {
        Error::Io(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_offer_exact_bytes() {
        let f = Features {
            timeout: true,
            handler_duration: true,
            ..Default::default()
        };

        let mut expect = Vec::new();
        expect.extend_from_slice(b"SSTARRPC");
        expect.extend_from_slice(&16u32.to_le_bytes());
        expect.extend_from_slice(&[1, 0, 0, 0, 0, 0, 0, 0]); // TIMEOUT, len 0
        expect.extend_from_slice(&[5, 0, 0, 0, 0, 0, 0, 0]); // HANDLER_DURATION, len 0

        assert_eq!(&f.encode()[..], &expect[..]);
    }

    #[test]
    fn roundtrip_all_features() {
        let f = Features {
            compress: Some(Bytes::from_static(b"LZ4")),
            timeout: true,
            connection_id: Some((7 << 16) | 3),
            stream_parent: Some(42),
            isolation: Some(Bytes::from_static(b"statement")),
            handler_duration: true,
        };

        let frame = f.encode();
        assert_eq!(Features::decode_records(frame.slice(12..)).unwrap(), f);
    }

    #[test]
    fn records_out_of_order_and_unknown_skipped() {
        let mut d = BytesMut::new();
        d.put_u32_le(HANDLER_DURATION);
        d.put_u32_le(0);
        d.put_u32_le(99); // unknown
        d.put_u32_le(3);
        d.put_slice(b"xyz");
        d.put_u32_le(CONNECTION_ID);
        d.put_u32_le(8);
        d.put_u64_le(0x1_0000);
        d.put_u32_le(TIMEOUT);
        d.put_u32_le(0);

        let f = Features::decode_records(d.freeze()).unwrap();
        assert!(f.timeout && f.handler_duration);
        assert_eq!(f.connection_id, Some(0x1_0000));
    }

    #[test]
    fn truncated_record_rejected() {
        let mut d = BytesMut::new();
        d.put_u32_le(ISOLATION);
        d.put_u32_le(10);
        d.put_slice(b"abc");

        assert!(matches!(
            Features::decode_records(d.freeze()),
            Err(Error::Protocol(_))
        ));
    }

    #[tokio::test]
    async fn read_rejects_bad_magic() {
        let mut input: &[u8] = b"NOTSTARR\0\0\0\0";

        assert!(matches!(
            read_negotiation(&mut input).await,
            Err(Error::Protocol(_))
        ));
    }

    #[tokio::test]
    async fn read_eof_is_closed() {
        let mut input: &[u8] = b"SSTAR";

        assert!(matches!(
            read_negotiation(&mut input).await,
            Err(Error::Closed)
        ));
    }

    #[tokio::test]
    async fn write_then_read() {
        let f = Features {
            timeout: true,
            ..Default::default()
        };

        let mut buf = Vec::new();
        write_negotiation(&mut buf, &f).await.unwrap();

        assert_eq!(read_negotiation(&mut &buf[..]).await.unwrap(), f);
    }
}
