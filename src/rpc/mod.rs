//! Seastar RPC protocol: codecs, raw connections and a multiplexed client.
//!
//! Layers, bottom up:
//!
//! - [`codec`]: negotiation, frames, exceptions and payload serialization.
//!   Role-agnostic: each codec both encodes and decodes.
//! - [`raw`]: a negotiated connection as a `Sink` of requests and a `Stream`
//!   of every response, with caller-chosen `msg_id`s and no bookkeeping.
//! - [`Client`]: multiplexes concurrent calls over one connection and
//!   matches replies to callers. Calls name a [`Verb`] id and pick their
//!   request/reply types, or pass raw payloads.

pub mod client;
pub mod codec;
pub mod error;
pub mod raw;
mod verb;

pub use client::{
    BATCH_LEAKED_PANIC, Batch, CallOptions, Client, ClientBuilder, ManyReplies, NO_WAIT, Replies,
    with_timeout,
};
pub use codec::serialize::{Decode, Encode};
pub use error::{DecodeError, Error, RemoteMessage, Result};
pub use verb::Verb;
