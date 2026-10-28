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
//! stream.finish().await?;
//! # Ok(()) }
//! ```
//!
//! To end a stream, read the source until it ends, then call
//! [`StreamConnection::finish`] (or [`StreamSink::close`] followed by
//! [`StreamSource::wait_closed`] on split halves). It sends EOS and waits
//! for the peer to close the connection. Seastar does that only after
//! both of its directions are closed, so once `finish` returns the peer
//! has read everything we sent. Dropping the connection earlier aborts the
//! stream on the peer: it throws away whatever it has not read yet, EOS
//! included (wire guide §9).
//!
//! The same holds for the sink alone: dropping a [`StreamSink`] over a TCP
//! write half shuts that direction down, which Seastar takes as the end of
//! the connection. With split halves, keep the sink alive until
//! [`StreamSource::wait_closed`] returns, e.g. return it from a writer task
//! instead of letting the task drop it.
//!
//! A direction the peer never made an `rpc::sink`/`rpc::source` for counts
//! as closed on its side from the start:
//!
//! - when the verb only reads from us, the server never sends EOS and
//!   closes the connection once it has read ours; reading the source then
//!   ends with [`Error::Closed`], while `finish` returns `Ok`;
//! - when the verb only writes to us and never reads its `rpc::source`,
//!   the server keeps the connection until we drop it, so `finish` would
//!   wait forever. Read the source to its end and drop the connection.
//!
//! Neither `finish` nor `wait_closed` has a timeout; wrap them in
//! `tokio::time::timeout` when the peer may not close.

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

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Encode for StreamId {
    fn encode(&self, out: &mut BytesMut) {
        self.0.encode(out);
    }
}

impl Decode for StreamId {
    fn decode(input: &mut Bytes) -> std::result::Result<Self, DecodeError> {
        u64::decode(input).map(StreamId)
    }
}

/// Server-assigned id of a connection (its `CONNECTION_ID`), from
/// [`Client::connection_id`](crate::rpc::Client::connection_id). A stream
/// names its parent connection by it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

impl std::fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A negotiated stream connection. Take it apart with
/// `let StreamConnection { sink, source, id, .. } = stream;` to use the
/// halves from different tasks. Keep the sink alive until the source's
/// [`wait_closed`](StreamSource::wait_closed) returns: dropping it early
/// can make the peer discard our EOS (see the module docs).
#[derive(Debug)]
#[non_exhaustive]
pub struct StreamConnection<R, W> {
    /// Our direction: messages to the peer.
    pub sink: StreamSink<W>,
    /// The peer's direction: messages from it.
    pub source: StreamSource<R>,
    /// The server-assigned id to pass in calls for this stream.
    pub id: StreamId,
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> StreamConnection<R, W> {
    /// Wrap an already negotiated transport. `layout` and `compression` must
    /// be what the negotiation settled on, and `id` the stream's
    /// `CONNECTION_ID`; [`ClientBuilder::handshake_stream`](crate::rpc::ClientBuilder::handshake_stream)
    /// negotiates and does this for you.
    pub fn new(
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
                failed: false,
            },
            id,
        }
    }

    /// Send EOS, then wait until the peer closes the connection: the peer
    /// has read everything we sent. Read the source to its end first;
    /// a message still unread fails with [`Error::Protocol`]. Waits forever
    /// if the peer never reads our direction (see the module docs); no
    /// builder timeout applies, so bound it with
    /// `tokio::time::timeout(d, stream.finish())` if the verb is not trusted.
    ///
    /// # Errors
    ///
    /// Fails if writing EOS fails, if the peer sends a message we have not
    /// read, or if the source already failed (so the connection end can't
    /// be trusted).
    pub async fn finish(mut self) -> Result<()> {
        self.sink.close().await?;
        self.source.wait_closed().await
    }
}

/// Write half: our messages to the peer. A `Sink` of encoded payloads; use
/// [`send_item`](Self::send_item) to encode a value in place.
///
/// The inherent [`flush`](Self::flush) and [`close`](Self::close) take
/// precedence over the `SinkExt` methods of the same name, even with
/// `SinkExt` in scope; they behave the same but return the `Result`.
///
/// Dropping the sink over a TCP write half shuts that direction down, which
/// can make Seastar discard an EOS it has not read yet; keep it until the
/// stream is finished (see the module docs).
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
    ///
    /// # Errors
    ///
    /// [`Error::InvalidState`] after [`close`](Self::close); otherwise an
    /// encode or I/O error.
    pub async fn send_item<T: Encode + ?Sized>(&mut self, item: &T) -> Result<()> {
        self.check_open()?;
        self.inner.send(item).await
    }

    /// Queue `item` for sending under the next flush. Allocates nothing once
    /// the write buffer is warm. Use [`flush`](Self::flush) to actually send.
    ///
    /// # Errors
    ///
    /// Same as [`send_item`](Self::send_item).
    pub async fn feed_item<T: Encode + ?Sized>(&mut self, item: &T) -> Result<()> {
        self.check_open()?;
        self.inner.feed(item).await
    }

    /// Flush queued items to the peer.
    ///
    /// # Errors
    ///
    /// An I/O error from the connection.
    pub async fn flush(&mut self) -> Result<()> {
        SinkExt::<Bytes>::flush(self).await
    }

    /// Send end of stream and flush. Later sends fail; closing again is a
    /// no-op. The connection stays open so the peer's messages can still be
    /// read from the source.
    ///
    /// # Errors
    ///
    /// An I/O error from the connection.
    pub async fn close(&mut self) -> Result<()> {
        SinkExt::<Bytes>::close(self).await
    }

    fn check_open(&self) -> Result<()> {
        if self.closed {
            return Err(Error::InvalidState("stream sink is closed"));
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
    /// A read failed, so the end of the connection can't be trusted.
    failed: bool,
}

impl<R: AsyncRead + Unpin> StreamSource<R> {
    /// Decode the next message as `T`; `None` at end of stream.
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] if the connection ended before EOS, or a protocol
    /// or I/O error; each is returned once and the source then ends. An
    /// [`Error::Decode`] skips only that message: reading can continue.
    pub async fn recv_item<T: Decode>(&mut self) -> Result<Option<T>> {
        match self.next().await {
            None => Ok(None),
            Some(payload) => Ok(Some(from_payload(payload?)?)),
        }
    }

    /// Wait until the peer closes the connection, after our sink sent EOS.
    /// The end of the connection counts as success with or without the
    /// peer's EOS: Seastar sends none when it has no sink for this stream.
    /// Like [`StreamConnection::finish`], it has no timeout of its own; wrap
    /// it in `tokio::time::timeout` to bound it.
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`] if a message is still unread, [`Error::Closed`]
    /// if the source already failed, or the read error that ends it now.
    pub async fn wait_closed(&mut self) -> Result<()> {
        // After a read error the framed reader yields `None` once without
        // reaching the real end of the connection, so it proves nothing.
        if self.failed {
            return Err(Error::Closed);
        }

        // Poll the framed reader directly: `done` may already be set by the
        // peer's EOS (or by the EOF itself), and the connection end comes
        // after it.
        while let Some(frame) = self.inner.next().await {
            self.done = true;

            let frame = frame.inspect_err(|_| self.failed = true)?;

            if let StreamFrame::Data(_) = frame {
                return Err(Error::Protocol(
                    "stream closed with an unread message from the peer".into(),
                ));
            }
        }

        self.done = true;

        Ok(())
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
            Some(Err(e)) => {
                self.failed = true;
                Some(Err(e))
            }
            Some(Ok(StreamFrame::Eos)) => None,
            None => Some(Err(Error::Closed)),
        };

        self.done = true;

        Poll::Ready(item)
    }
}
