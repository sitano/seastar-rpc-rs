//! Background tasks behind a multiplexed [`Client`](super::Client).
//!
//! Writing and reading run as separate tasks: if one task blocked on a
//! socket write, it could not read the replies the peer is blocked trying to
//! send, and both sides would stall. Callers register a slot in the
//! shared pending map before queueing their request; the reader completes
//! it by `msg_id` (the writer does, for no-wait calls that wait for the
//! write, once flushed). Fire-and-forget calls register nothing.
//! Slots hold the caller's waker, so a call allocates nothing per request.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::Instant;

use crate::rpc::codec::frame::RequestFrame;
use crate::rpc::error::{Error, Result};
use crate::rpc::raw::{RawReceiver, RawSender};
use crate::rpc::verb::Verb;

/// Most requests written before one flush.
const MAX_BATCH: usize = 64;

/// A request queued for the writer.
pub(super) struct Outgoing {
    pub msg_id: i64,
    pub verb: Verb,
    pub payload: Bytes,
    pub deadline: Option<Instant>,
    /// The verb never replies but the caller waits for the write: the writer
    /// completes the pending slot with an empty payload once the frame is
    /// flushed.
    pub ack: bool,
}

/// A caller's entry in the pending map.
enum Slot {
    /// Waker of the caller's last poll, if it has polled yet.
    Waiting(Option<Waker>),
    Done(Result<Bytes>),
}

#[derive(Default)]
struct State {
    pending: HashMap<i64, Slot>,
    /// Why the connection ended; set once.
    closed: Option<Error>,
}

/// State shared by callers, the writer and the reader.
#[derive(Default)]
pub(super) struct Shared {
    state: Mutex<State>,
    /// Wakes the writer when the reader closes the connection.
    closed_notify: Notify,
}

impl Shared {
    pub fn register(&self, msg_id: i64) -> Result<()> {
        let mut state = self.state.lock().unwrap();

        if let Some(e) = &state.closed {
            return Err(replicate(e));
        }

        state.pending.insert(msg_id, Slot::Waiting(None));

        Ok(())
    }

    /// Take the result for `msg_id` if it is done, else park the caller.
    pub fn poll_reply(&self, msg_id: i64, cx: &mut Context<'_>) -> Poll<Result<Bytes>> {
        let mut state = self.state.lock().unwrap();

        match state.pending.get_mut(&msg_id) {
            Some(Slot::Waiting(waker)) => {
                match waker {
                    Some(w) if w.will_wake(cx.waker()) => {}
                    _ => *waker = Some(cx.waker().clone()),
                }
                Poll::Pending
            }
            Some(Slot::Done(_)) => match state.pending.remove(&msg_id) {
                Some(Slot::Done(result)) => Poll::Ready(result),
                _ => unreachable!(),
            },
            // Not registered (or already taken): nothing will complete it.
            None => Poll::Ready(Err(Error::Closed)),
        }
    }

    pub fn forget(&self, msg_id: i64) {
        self.state.lock().unwrap().pending.remove(&msg_id);
    }

    pub fn check_open(&self) -> Result<()> {
        match &self.state.lock().unwrap().closed {
            Some(e) => Err(replicate(e)),
            None => Ok(()),
        }
    }

    fn complete(&self, msg_id: i64, result: Result<Bytes>) {
        let waker = {
            let mut state = self.state.lock().unwrap();

            // No entry: the caller timed out or gave up. Late replies are normal.
            let Some(slot) = state.pending.get_mut(&msg_id) else {
                return;
            };

            // Already done (e.g. failed by `close`): keep the first result.
            let Slot::Waiting(waker) = slot else {
                return;
            };

            let waker = waker.take();
            *slot = Slot::Done(result);

            waker
        };
        if let Some(w) = waker {
            w.wake();
        }
    }

    /// Mark the connection dead and fail every pending call with `reason`.
    fn close(&self, reason: Error) {
        let wakers: Vec<Waker> = {
            let mut state = self.state.lock().unwrap();
            if state.closed.is_some() {
                return;
            }

            let wakers = state
                .pending
                .values_mut()
                .filter_map(|slot| match slot {
                    Slot::Waiting(waker) => {
                        let waker = waker.take();
                        *slot = Slot::Done(Err(replicate(&reason)));
                        waker
                    }
                    Slot::Done(_) => None,
                })
                .collect();

            state.closed = Some(reason);

            wakers
        };

        for w in wakers {
            w.wake();
        }

        self.closed_notify.notify_one();
    }
}

/// Recreate an error for each waiter (`io::Error` is not `Clone`).
fn replicate(e: &Error) -> Error {
    match e {
        Error::Io(e) => Error::Io(io::Error::new(e.kind(), e.to_string())),
        Error::Protocol(msg) => Error::Protocol(msg.clone()),
        _ => Error::Closed,
    }
}

pub(super) async fn writer<W: AsyncWrite + Unpin>(
    mut sink: RawSender<W>,
    mut rx: mpsc::Receiver<Outgoing>,
    shared: Arc<Shared>,
    _stop_reader: oneshot::Sender<()>,
) {
    // Only the writer awaits connection close, so one `Notify` waiter is
    // enough. If more tasks need to observe it (e.g. sink/source streams),
    // we will switch to a `CancellationToken`: `notify_one` wakes a single
    // waiter and `notify_waiters` stores no permit for tasks not yet waiting.
    //
    // Registered once for the connection and polled by both selects below.
    let closed = shared.closed_notify.notified();
    tokio::pin!(closed);
    closed.as_mut().enable();

    let mut acks = Vec::new();
    loop {
        let first = tokio::select! {
            next = rx.recv() => match next {
                Some(o) => o,
                // Every Client handle is gone; dropping `_stop_reader` ends the reader.
                None => return,
            },
            _ = &mut closed => break,
        };

        let batch = async {
            let mut batch = Some(first);
            let mut written = 0;

            while let Some(o) = batch.take() {
                feed(&mut sink, o, &shared, &mut acks).await?;
                written += 1;
                if written < MAX_BATCH {
                    batch = rx.try_recv().ok();
                }
            }

            sink.flush().await
        };

        // A write blocks for as long as the peer doesn't read. If the reader
        // closes the connection meanwhile, abandon the write instead.
        let flushed = tokio::select! {
            r = batch => r,
            _ = &mut closed => break,
        };

        match flushed {
            Ok(()) => {
                for msg_id in acks.drain(..) {
                    shared.complete(msg_id, Ok(Bytes::new()));
                }
            }
            Err(e) => {
                shared.close(e);
                break;
            }
        }
    }

    // Connection is dead: `close` already failed every pending call,
    // including queued and unflushed no-wait ones. Dropping `rx` fails
    // callers still trying to queue.
}

async fn feed<W: AsyncWrite + Unpin>(
    sink: &mut RawSender<W>,
    o: Outgoing,
    shared: &Shared,
    acks: &mut Vec<i64>,
) -> Result<()> {
    let remaining = match o.deadline {
        None => None,
        Some(d) => match d.checked_duration_since(Instant::now()) {
            Some(r) if !r.is_zero() => Some(r),
            // Already expired: don't send (the server would drop it anyway).
            _ => {
                shared.complete(o.msg_id, Err(Error::Timeout));
                return Ok(());
            }
        },
    };

    let frame = RequestFrame {
        timeout_ms: RequestFrame::timeout_ms_for(remaining),
        verb: o.verb,
        msg_id: o.msg_id,
        payload: o.payload,
    };

    sink.feed(frame).await?;

    if o.ack {
        acks.push(o.msg_id);
    }

    Ok(())
}

pub(super) async fn reader<R: AsyncRead + Unpin>(
    mut stream: RawReceiver<R>,
    shared: Arc<Shared>,
    mut stop: oneshot::Receiver<()>,
) {
    let reason = loop {
        tokio::select! {
            _ = &mut stop => break Error::Closed,
            next = stream.next() => match next {
                Some(Ok(frame)) => shared.complete(frame.call_id(), frame.into_result()),
                Some(Err(e)) => break e,
                None => break Error::Closed,
            },
        }
    };

    shared.close(reason);
}
