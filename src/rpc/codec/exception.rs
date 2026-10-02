//! Exception payloads (wire guide §8).
//!
//! A response with a negative `msg_id` carries `u32 type, u32 len, data[len]`
//! instead of a return value.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::rpc::error::{Error, RemoteMessage, Result};
use crate::rpc::verb::Verb;

const USER: u32 = 0;
const UNKNOWN_VERB: u32 = 1;

/// An exception sent back in place of a reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteException {
    /// The handler threw; holds its formatted message (usually UTF-8).
    User(Bytes),
    /// No handler is registered for this verb id.
    UnknownVerb(Verb),
    /// An exception type this crate doesn't know; `data` is uninterpreted.
    Unknown(u32, Bytes),
}

impl RemoteException {
    pub fn encode(&self) -> Bytes {
        let (kind, data) = match self {
            RemoteException::User(msg) => (USER, msg.clone()),
            RemoteException::UnknownVerb(verb) => {
                (UNKNOWN_VERB, Bytes::copy_from_slice(&verb.to_le_bytes()))
            }
            RemoteException::Unknown(kind, data) => (*kind, data.clone()),
        };

        let mut out = BytesMut::with_capacity(8 + data.len());
        out.put_u32_le(kind);
        out.put_u32_le(data.len() as u32);
        out.put_slice(&data);
        out.freeze()
    }

    pub fn decode(mut payload: Bytes) -> Result<Self> {
        if payload.remaining() < 8 {
            return Err(Error::Protocol("truncated exception header".into()));
        }

        let kind = payload.get_u32_le();
        let len = payload.get_u32_le() as usize;

        if payload.remaining() < len {
            return Err(Error::Protocol(format!(
                "exception claims {len} bytes, {} left",
                payload.remaining()
            )));
        }

        let mut data = payload.split_to(len);

        if payload.has_remaining() {
            return Err(Error::Protocol(format!(
                "{} trailing byte(s) after exception data",
                payload.remaining()
            )));
        }

        Ok(match kind {
            USER => RemoteException::User(data),
            UNKNOWN_VERB if data.len() == 8 => RemoteException::UnknownVerb(data.get_u64_le()),
            UNKNOWN_VERB => {
                return Err(Error::Protocol(format!(
                    "UNKNOWN_VERB exception with {} data bytes",
                    data.len()
                )));
            }
            other => RemoteException::Unknown(other, data),
        })
    }
}

impl From<RemoteException> for Error {
    fn from(e: RemoteException) -> Self {
        match e {
            RemoteException::User(msg) => Error::Remote(RemoteMessage::new(msg)),
            RemoteException::UnknownVerb(verb) => Error::UnknownVerb(verb),
            RemoteException::Unknown(kind, data) => Error::UnknownException { kind, data },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_verb_fixture() {
        // Wire guide §10.3: LE32(1) LE32(8) LE64(999)
        let mut p = Vec::new();
        p.extend_from_slice(&1u32.to_le_bytes());
        p.extend_from_slice(&8u32.to_le_bytes());
        p.extend_from_slice(&999u64.to_le_bytes());

        let e = RemoteException::decode(Bytes::from(p.clone())).unwrap();

        assert_eq!(e, RemoteException::UnknownVerb(999));
        assert_eq!(&e.encode()[..], &p[..]);
    }

    #[test]
    fn user_and_unknown() {
        for e in [
            RemoteException::User(Bytes::from_static(b"exception!")),
            RemoteException::Unknown(7, Bytes::from_static(b"\x01\x02")),
        ] {
            assert_eq!(RemoteException::decode(e.encode()).unwrap(), e);
        }

        assert!(matches!(
            Error::from(RemoteException::User(Bytes::from_static(b"boom"))),
            Error::Remote(m) if m.to_str() == Ok("boom")
        ));
    }

    #[test]
    fn user_non_utf8_preserved() {
        let raw = Bytes::from_static(b"bad \xff key");
        let Error::Remote(m) = Error::from(RemoteException::User(raw.clone())) else {
            panic!("expected Error::Remote");
        };

        assert!(m.to_str().is_err());
        assert_eq!(m.as_bytes(), &raw[..]);
        assert_eq!(m.to_string(), "bad \u{fffd} key");
    }

    #[test]
    fn truncated_rejected() {
        assert!(
            RemoteException::decode(Bytes::from_static(&[0, 0, 0, 0, 5, 0, 0, 0, b'a'])).is_err()
        );
        assert!(RemoteException::decode(Bytes::from_static(&[0, 0])).is_err());
    }

    #[test]
    fn trailing_rejected() {
        // USER, len=1, "ab": the 'b' is not covered by data_len.
        assert!(
            RemoteException::decode(Bytes::from_static(&[0, 0, 0, 0, 1, 0, 0, 0, b'a', b'b']))
                .is_err()
        );
    }
}
