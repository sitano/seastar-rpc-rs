//! Streams: Seastar's `rpc::sink` / `rpc::source` (wire guide §9).
//!
//! A stream is its own TCP connection, negotiated with `STREAM_PARENT` set
//! to the id of an open [`Client`](crate::rpc::Client) connection. It carries
//! ordered messages in both directions: a [`StreamSink`] writes ours, a
//! [`StreamSource`] reads the peer's. Each stream gets a new connection;
//! streams never share one, and never use the parent's.
//!
//! To hand a stream to a verb, put its [`StreamId`] in the call where the
//! C++ signature has `rpc::sink<..>` or `rpc::source<..>`. A verb returning
//! `rpc::sink<..>` replies with the same id; its messages arrive on this
//! connection's source.
//!
//! ```no_run
//! # async fn f(client: seastar_rpc::rpc::Client) -> seastar_rpc::rpc::Result<()> {
//! use seastar_rpc::rpc::{StreamId, Verb};
//!
//! /// `rpc::sink<int> (rpc::source<int>)`
//! const DOUBLE: Verb = 18;
//!
//! let mut stream = client.open_stream().await?;
//! let back: StreamId = client.call(DOUBLE, stream.id).await?;
//! assert_eq!(back, stream.id);
//!
//! stream.sink.send_item(&21i32).await?;
//! stream.sink.close().await?;
//!
//! while let Some(x) = stream.source.recv_item::<i32>().await? {
//!     assert_eq!(x, 42);
//! }
//! # Ok(()) }
//! ```
//!
//! Closing follows Seastar's rule: close the sink, read the source until it
//! ends, then drop the connection. Seastar tears the connection down only
//! after both directions closed; dropping it earlier aborts the stream, and
//! the peer may lose messages it already received but not yet read.

use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::{Bytes, BytesMut};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::rpc::codec::compress::{Compression, CompressionCodec};
use crate::rpc::codec::frame::FrameLayout;
use crate::rpc::codec::serialize::{Decode, Encode, from_payload};
use crate::rpc::codec::stream::{StreamCodec, StreamFrame};
use crate::rpc::error::{DecodeError, Error, Result};

/// Server-assigned id of a stream connection. It is what a call carries for
/// an `rpc::sink` or `rpc::source` argument or return value: 8 raw LE bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamId(pub u64);

impl Encode for StreamId {
    fn encode(&self, out: &mut BytesMut) {
        self.0.encode(out)
    }
}

impl Decode for StreamId {
    fn decode(input: &mut Bytes) -> std::result::Result<Self, DecodeError> {
        u64::decode(input).map(StreamId)
    }
}

/// A negotiated stream connection. Take it apart with
/// `let StreamConnection { sink, source, id, .. } = stream;` to use the
/// halves from different tasks.
#[derive(Debug)]
#[non_exhaustive]
pub struct StreamConnection<R, W> {
    pub sink: StreamSink<W>,
    pub source: StreamSource<R>,
    /// Pass this to the verb that takes the stream.
    pub id: StreamId,
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> StreamConnection<R, W> {
    pub(crate) fn new(
        read: R,
        write: W,
        layout: FrameLayout,
        compression: Option<Compression>,
        id: StreamId,
    ) -> Self {
        let codec = || CompressionCodec::new(StreamCodec::new(layout), layout, compression);

        StreamConnection {
            sink: StreamSink {
                inner: FramedWrite::new(write, codec()),
                closed: false,
            },
            source: StreamSource {
                inner: FramedRead::new(read, codec()),
                done: false,
            },
            id,
        }
    }
}

/// Write half: our messages to the peer. A `Sink` of encoded payloads; use
/// [`send_item`](Self::send_item) to encode a value in place.
#[derive(Debug)]
pub struct StreamSink<W> {
    inner: FramedWrite<W, CompressionCodec<StreamCodec>>,
    /// EOS was queued; nothing may follow it.
    closed: bool,
}

impl<W: AsyncWrite + Unpin> StreamSink<W> {
    /// Encode `item` into the next message and flush. Allocates nothing
    /// once the write buffer is warm. Use the `Sink` methods (`feed`) to
    /// queue several payloads under one flush.
    pub async fn send_item<T: Encode + ?Sized>(&mut self, item: &T) -> Result<()> {
        self.check_open()?;
        self.inner.send(item).await
    }

    /// Send end of stream and flush. Later sends fail; closing again is a
    /// no-op. The connection stays open so the peer's messages can still be
    /// read from the source.
    pub async fn close(&mut self) -> Result<()> {
        SinkExt::<Bytes>::close(self).await
    }

    fn check_open(&self) -> Result<()> {
        if self.closed {
            return Err(Error::Protocol("stream sink is closed".into()));
        }
        Ok(())
    }
}

impl<W: AsyncWrite + Unpin> Sink<Bytes> for StreamSink<W> {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Sink::<StreamFrame>::poll_ready(Pin::new(&mut self.inner), cx)
    }

    fn start_send(mut self: Pin<&mut Self>, payload: Bytes) -> Result<()> {
        self.check_open()?;
        Pin::new(&mut self.inner).start_send(StreamFrame::Data(payload))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Sink::<StreamFrame>::poll_flush(Pin::new(&mut self.inner), cx)
    }

    /// Sends EOS and flushes, without shutting the socket down: Seastar
    /// drops a queued EOS it hasn't read yet if the connection ends early.
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if !self.closed {
            ready!(Sink::<StreamFrame>::poll_ready(
                Pin::new(&mut self.inner),
                cx
            ))?;
            Pin::new(&mut self.inner).start_send(StreamFrame::Eos)?;
            self.closed = true;
        }

        Sink::<StreamFrame>::poll_flush(Pin::new(&mut self.inner), cx)
    }
}

/// Read half: the peer's messages, in order. A `Stream` of payloads that
/// ends at the peer's EOS. If the connection ends before EOS, it yields
/// [`Error::Closed`] once, as some messages may be missing.
#[derive(Debug)]
pub struct StreamSource<R> {
    inner: FramedRead<R, CompressionCodec<StreamCodec>>,
    /// EOS or end of connection seen.
    done: bool,
}

impl<R: AsyncRead + Unpin> StreamSource<R> {
    /// Decode the next message as `T`; `None` at end of stream.
    pub async fn recv_item<T: Decode>(&mut self) -> Result<Option<T>> {
        match self.next().await {
            None => Ok(None),
            Some(payload) => Ok(Some(from_payload(payload?)?)),
        }
    }
}

impl<R: AsyncRead + Unpin> Stream for StreamSource<R> {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        let item = match ready!(Pin::new(&mut self.inner).poll_next(cx)) {
            Some(Ok(StreamFrame::Data(payload))) => return Poll::Ready(Some(Ok(payload))),
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(StreamFrame::Eos)) => None,
            None => Some(Err(Error::Closed)),
        };

        self.done = true;

        Poll::Ready(item)
    }
}
