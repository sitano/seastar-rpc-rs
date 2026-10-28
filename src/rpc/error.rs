//! Error types.

use std::borrow::Cow;
use std::{fmt, io, str};

use bytes::Bytes;

use crate::rpc::verb::Verb;

/// Failure to decode a payload with [`Decode`](crate::rpc::Decode).
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DecodeError {
    /// The input ended before the value was complete.
    #[error("unexpected end of payload: needed {needed} more byte(s)")]
    UnexpectedEof { needed: usize },
    /// The bytes are not a valid encoding of the target type.
    #[error("invalid payload: {0}")]
    Invalid(String),
}

/// Errors returned by the client.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// The connection is closed (by the peer, or after an earlier failure).
    #[error("connection closed")]
    Closed,
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    /// The remote handler threw (`USER` exception). Holds its formatted message.
    #[error("remote exception: {0}")]
    Remote(RemoteMessage),
    /// The server has no handler for this verb (`UNKNOWN_VERB` exception).
    #[error("unknown verb {0}")]
    UnknownVerb(Verb),
    /// The server sent an exception type this crate doesn't know; `data` is
    /// uninterpreted.
    #[error("unknown remote exception type {kind}")]
    UnknownException { kind: u32, data: Bytes },
    #[error("call timed out")]
    Timeout,
    /// The request queue is full and the call asked not to wait for room
    /// ([`CallOptions::no_wait_send`](crate::rpc::CallOptions::no_wait_send)).
    #[error("request queue is full")]
    QueueFull,
    /// The peer violated the wire protocol (bad magic, oversized frame, ...).
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("decode error: {0}")]
    Decode(#[from] DecodeError),
    /// The call is not valid in this state: a send on a closed stream sink,
    /// or a stream asked of a client that can't reopen its transport. The
    /// connection itself is fine.
    #[error("invalid use: {0}")]
    InvalidState(&'static str),
    /// The server did not negotiate a feature this call needs, e.g. it sent
    /// no `CONNECTION_ID`, so it can't take streams. The connection itself
    /// is fine.
    #[error("not supported by the server: {0}")]
    Unsupported(&'static str),
}

/// Message of a `USER` exception: opaque bytes that are usually, but not
/// necessarily, UTF-8. `Display` replaces invalid sequences with U+FFFD.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteMessage(Bytes);

impl RemoteMessage {
    pub fn new(bytes: Bytes) -> Self {
        RemoteMessage(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    pub fn to_str(&self) -> Result<&str, str::Utf8Error> {
        str::from_utf8(&self.0)
    }

    pub fn to_string_lossy(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.0)
    }
}

impl fmt::Display for RemoteMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_string_lossy())
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
