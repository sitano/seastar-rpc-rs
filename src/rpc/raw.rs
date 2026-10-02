//! Raw, unmultiplexed connection halves.
//!
//! After negotiation a Seastar RPC connection is a pipelined, out-of-order
//! exchange of frames. This layer exposes it as exactly that: a
//! [`RawSender`] (`Sink<RequestFrame>`) where the caller picks every
//! `msg_id`, and a [`RawReceiver`] (`Stream<Item = Result<ResponseFrame>>`)
//! that yields every response, in arrival order, with nothing kept in a
//! pending-call table. The multiplexed [`Client`](crate::rpc::Client) is built on
//! top of these two halves.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures_util::{Sink, SinkExt, Stream};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::rpc::codec::frame::{
    FrameLayout, RequestCodec, RequestFrame, ResponseCodec, ResponseFrame,
};
use crate::rpc::codec::negotiation::Features;
use crate::rpc::error::{Error, Result};
use crate::rpc::verb::Verb;

/// A negotiated connection, split into its two directions.
#[derive(Debug)]
#[non_exhaustive]
pub struct RawConnection<R, W> {
    pub sender: RawSender<W>,
    pub receiver: RawReceiver<R>,
    /// Features the server accepted.
    pub features: Features,
}

impl<R, W> RawConnection<R, W> {
    pub fn into_parts(self) -> (RawSender<W>, RawReceiver<R>, Features) {
        (self.sender, self.receiver, self.features)
    }
}

/// Write half: a `Sink` of request frames.
#[derive(Debug)]
pub struct RawSender<W> {
    inner: FramedWrite<W, RequestCodec>,
    layout: FrameLayout,
}

impl<W: AsyncWrite + Unpin> RawSender<W> {
    pub(crate) fn new(write: W, layout: FrameLayout) -> Self {
        RawSender {
            inner: FramedWrite::new(write, RequestCodec::new(layout)),
            layout,
        }
    }

    /// Send one request and flush. `msg_id` must be positive and unique on
    /// this connection. `timeout` is dropped if `TIMEOUT` wasn't negotiated.
    pub async fn send_request(
        &mut self,
        msg_id: i64,
        verb: Verb,
        payload: Bytes,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let frame = RequestFrame {
            timeout_ms: RequestFrame::timeout_ms_for(timeout),
            verb,
            msg_id,
            payload,
        };
        self.send(frame).await
    }

    /// The header layout in use.
    pub fn layout(&self) -> FrameLayout {
        self.layout
    }
}

impl<W: AsyncWrite + Unpin> Sink<RequestFrame> for RawSender<W> {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: RequestFrame) -> Result<()> {
        if item.msg_id <= 0 {
            return Err(Error::Protocol(format!(
                "msg_id must be positive, got {}",
                item.msg_id
            )));
        }
        Pin::new(&mut self.inner).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

/// Read half: a `Stream` of every response frame. Ends on clean EOF.
#[derive(Debug)]
pub struct RawReceiver<R> {
    inner: FramedRead<R, ResponseCodec>,
}

impl<R: AsyncRead + Unpin> RawReceiver<R> {
    pub(crate) fn new(read: R, layout: FrameLayout) -> Self {
        RawReceiver {
            inner: FramedRead::new(read, ResponseCodec::new(layout)),
        }
    }
}

impl<R: AsyncRead + Unpin> Stream for RawReceiver<R> {
    type Item = Result<ResponseFrame>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}
