//! Background tasks behind a multiplexed [`Client`](super::Client).
//!
//! Writing and reading run as separate tasks: if one task blocked on a
//! socket write, it could not read the replies the peer is blocked trying to
//! send, and both sides would stall. Callers register a slot in the
//! shared pending map before queueing their request; the reader completes
//! it by `msg_id` (the writer does, for no-wait calls that wait for the
//! write, once flushed). Fire-and-forget calls register nothing.
//! Slots hold the caller's waker, so a call allocates nothing per request.
//! A batch's slots point into its pooled [`BatchState`]: replies land in its
//! result list, and a countdown wakes the batch on the last one.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use bytes::{Bytes, BytesMut};
use futures_util::task::AtomicWaker;
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
/// Most batch states kept for reuse.
const POOL_MAX: usize = 16;
/// A pooled state drops an arena larger than this.
pub(super) const POOL_ARENA_MAX: usize = 1 << 20;
/// A pooled state shrinks request and result lists longer than this.
const POOL_SLOTS_MAX: usize = 1024;

/// A request for the writer.
pub(super) struct Request {
    pub msg_id: i64,
    pub verb: Verb,
    pub payload: Bytes,
    pub deadline: Option<Instant>,
    /// The verb never replies but the caller waits for the write: the writer
    /// completes the pending slot with an empty payload once the frame is
    /// flushed.
    pub ack: bool,
}

/// A message queued for the writer. A `Many` is written back to back, with
/// no other frame in between, and flushed together.
pub(super) enum Outgoing {
    One(Request),
    Many(Arc<BatchState>),
}

/// A batch's reusable buffers, shared by its [`Batch`](super::Batch), the
/// writer (requests) and the reader (results), and pooled in [`Shared`].
#[derive(Default)]
pub(super) struct BatchState {
    /// Payload arena. Touched only through a unique `Arc`.
    pub arena: BytesMut,
    /// Most payload bytes one send has pushed, capped at `POOL_ARENA_MAX`:
    /// the arena room to reserve before a send.
    pub arena_hint: usize,
    /// Second result list, swapped with `results` after each send. Touched
    /// only through a unique `Arc`.
    pub spare: Vec<Option<Result<Bytes>>>,
    /// Requests of the next send. The writer takes them and hands the empty
    /// list back.
    pub requests: Mutex<Vec<Request>>,
    /// Result of request k of the send in flight. Written under the `Shared`
    /// lock.
    pub results: Mutex<Vec<Option<Result<Bytes>>>>,
    /// Requests of the send in flight still waiting. Changed under the
    /// `Shared` lock.
    remaining: AtomicUsize,
    waker: AtomicWaker,
}

impl BatchState {
    /// Prepare for a send of `n` requests: set `remaining` to `n` and fill
    /// `results` with `n` empty entries, one per request, for the reader to
    /// fill in.
    ///
    /// Call it before the slots are registered with
    /// [`Shared::register_many`]. It takes `&mut self`, so only the owner of
    /// a unique `Arc` can call it: no writer or reader holds the state at
    /// that point.
    pub fn arm(&mut self, n: usize) {
        *self.remaining.get_mut() = n;

        let results = self.results.get_mut().unwrap();

        results.clear();
        results.resize_with(n, || None);
    }

    /// Ready once every request of the send is done.
    pub fn poll_done(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.remaining.load(Ordering::Acquire) == 0 {
            return Poll::Ready(());
        }

        self.waker.register(cx.waker());

        if self.remaining.load(Ordering::Acquire) == 0 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    /// Apply the pool limits: drop an oversized arena and shrink the lists.
    pub(super) fn reset(&mut self) {
        self.arena.clear();

        // `capacity()` only counts what follows the last split; reclaiming
        // more than the cap succeeds only if the whole buffer is larger.
        if self.arena.try_reclaim(POOL_ARENA_MAX + 1) {
            self.arena = BytesMut::new();
        }

        shrink(&mut self.spare);
        shrink(self.requests.get_mut().unwrap());
        shrink(self.results.get_mut().unwrap());
    }
}

pub(super) fn shrink<T>(v: &mut Vec<T>) {
    v.clear();
    v.shrink_to(POOL_SLOTS_MAX);
}

/// Who waits for a slot.
enum Waiter {
    /// Waker of a single call's last poll, if it has polled yet.
    One(Option<Waker>),
    /// Request `k` of a batch.
    Batch(Arc<BatchState>, usize),
}

/// A caller's entry in the pending map. Batch slots are removed as soon as
/// they complete, so only single calls ever reach `Done`.
enum Slot {
    Waiting(Waiter),
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
    /// Batch states ready for reuse, all unique.
    pool: Mutex<Vec<Arc<BatchState>>>,
}

impl Shared {
    pub fn register(&self, msg_id: i64) -> Result<()> {
        let mut state = self.state.lock().unwrap();

        if let Some(e) = &state.closed {
            return Err(replicate(e));
        }

        state
            .pending
            .insert(msg_id, Slot::Waiting(Waiter::One(None)));

        Ok(())
    }

    /// Register ids `base..base + n` as requests `0..n` of `batch`.
    pub fn register_many(&self, base: i64, n: usize, batch: &Arc<BatchState>) -> Result<()> {
        let mut state = self.state.lock().unwrap();

        if let Some(e) = &state.closed {
            return Err(replicate(e));
        }

        for k in 0..n {
            state.pending.insert(
                base + k as i64,
                Slot::Waiting(Waiter::Batch(batch.clone(), k)),
            );
        }

        Ok(())
    }

    /// Take the result for `msg_id` if it is done, else park the caller.
    pub fn poll_reply(&self, msg_id: i64, cx: &mut Context<'_>) -> Poll<Result<Bytes>> {
        let mut state = self.state.lock().unwrap();

        match state.pending.get_mut(&msg_id) {
            Some(Slot::Waiting(Waiter::One(waker))) => {
                match waker {
                    Some(w) if w.will_wake(cx.waker()) => {}
                    _ => *waker = Some(cx.waker().clone()),
                }
                Poll::Pending
            }
            Some(Slot::Waiting(Waiter::Batch(..))) => {
                unreachable!("batch slot polled as a single call")
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

    /// Drop a batch's remaining slots. Afterwards no reply touches the
    /// batch's state.
    pub fn forget_many(&self, base: i64, n: usize) {
        let mut state = self.state.lock().unwrap();
        for k in 0..n {
            state.pending.remove(&(base + k as i64));
        }
    }

    /// Wait out a completion in progress: afterwards a batch whose countdown
    /// reached zero is no longer referenced by the reader.
    pub fn fence(&self) {
        drop(self.state.lock().unwrap());
    }

    pub fn check_open(&self) -> Result<()> {
        match &self.state.lock().unwrap().closed {
            Some(e) => Err(replicate(e)),
            None => Ok(()),
        }
    }

    pub fn take_batch_state(&self) -> Arc<BatchState> {
        self.pool.lock().unwrap().pop().unwrap_or_default()
    }

    /// Pool `state` if nobody else holds it; otherwise its last holder does.
    pub fn recycle(&self, mut state: Arc<BatchState>) {
        let Some(s) = Arc::get_mut(&mut state) else {
            return;
        };

        s.reset();

        let mut pool = self.pool.lock().unwrap();

        if pool.len() < POOL_MAX {
            pool.push(state);
        }
    }

    fn complete(&self, msg_id: i64, result: Result<Bytes>) {
        let waker = finish(&mut self.state.lock().unwrap().pending, msg_id, result);
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

            let waiting: Vec<i64> = state
                .pending
                .iter()
                .filter(|(_, slot)| matches!(slot, Slot::Waiting(_)))
                .map(|(&msg_id, _)| msg_id)
                .collect();
            let wakers = waiting
                .into_iter()
                .filter_map(|msg_id| finish(&mut state.pending, msg_id, Err(replicate(&reason))))
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

/// Deliver `result` to `msg_id`'s caller; returns the waker to call once the
/// lock is released. Runs under the `Shared` lock, so a batch's results and
/// countdown only change there and `forget_many`/`fence` order against it.
fn finish(pending: &mut HashMap<i64, Slot>, msg_id: i64, result: Result<Bytes>) -> Option<Waker> {
    if let Some(Slot::Waiting(Waiter::Batch(..))) = pending.get(&msg_id) {
        let Some(Slot::Waiting(Waiter::Batch(batch, k))) = pending.remove(&msg_id) else {
            unreachable!()
        };

        batch.results.lock().unwrap()[k] = Some(result);

        // Take the waker and drop our `Arc` before the batch can run, so it
        // finds its state unique again.
        return if batch.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            batch.waker.take()
        } else {
            None
        };
    }

    // No entry: the caller timed out or gave up. Late replies are normal.
    let slot = pending.get_mut(&msg_id)?;
    // Already done (e.g. failed by `close`): keep the first result.
    let Slot::Waiting(Waiter::One(waker)) = slot else {
        return None;
    };

    let waker = waker.take();

    *slot = Slot::Done(result);

    waker
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
            let mut next = Some(first);
            let mut written = 0;

            // `MAX_BATCH` is checked between messages, so a `Many` is never
            // split across flushes.
            while let Some(o) = next.take() {
                match o {
                    Outgoing::One(r) => {
                        feed(&mut sink, r, &shared, &mut acks).await?;
                        written += 1;
                    }
                    Outgoing::Many(batch) => {
                        let mut requests = std::mem::take(&mut *batch.requests.lock().unwrap());
                        for r in requests.drain(..) {
                            feed(&mut sink, r, &shared, &mut acks).await?;
                            written += 1;
                        }
                        // Hand the empty list back, then pool the state if
                        // its batch is gone (fire-and-forget or abandoned).
                        *batch.requests.lock().unwrap() = requests;
                        shared.recycle(batch);
                    }
                }
                if written < MAX_BATCH {
                    next = rx.try_recv().ok();
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
    o: Request,
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

#[cfg(test)]
mod tests {
    use futures_util::task::noop_waker_ref;

    use super::*;

    fn cx() -> Context<'static> {
        Context::from_waker(noop_waker_ref())
    }

    fn armed(n: usize) -> Arc<BatchState> {
        let mut state = Arc::new(BatchState::default());
        Arc::get_mut(&mut state).unwrap().arm(n);
        state
    }

    #[test]
    fn batch_completes_once_every_slot_is_done() {
        let shared = Shared::default();
        let batch = armed(3);
        shared.register_many(10, 3, &batch).unwrap();
        assert!(batch.poll_done(&mut cx()).is_pending());

        shared.complete(12, Ok(Bytes::from_static(b"c")));
        shared.complete(10, Ok(Bytes::from_static(b"a")));
        assert!(batch.poll_done(&mut cx()).is_pending());

        // A duplicate reply finds no slot and must not count twice.
        shared.complete(10, Ok(Bytes::from_static(b"dup")));
        assert!(batch.poll_done(&mut cx()).is_pending());

        shared.complete(11, Err(Error::Timeout));
        assert!(batch.poll_done(&mut cx()).is_ready());

        let results = batch.results.lock().unwrap();
        assert!(matches!(&results[0], Some(Ok(b)) if &b[..] == b"a"));
        assert!(matches!(results[1], Some(Err(Error::Timeout))));
        assert!(matches!(&results[2], Some(Ok(b)) if &b[..] == b"c"));
        drop(results);
        assert!(shared.state.lock().unwrap().pending.is_empty());
        // Slots released their references: the batch can reuse its state.
        assert_eq!(Arc::strong_count(&batch), 1);
    }

    #[test]
    fn forget_many_fences_late_replies() {
        let shared = Shared::default();
        let batch = armed(2);

        shared.register_many(1, 2, &batch).unwrap();
        shared.complete(1, Ok(Bytes::from_static(b"x")));
        shared.forget_many(1, 2);
        shared.complete(2, Ok(Bytes::from_static(b"late")));

        let results = batch.results.lock().unwrap();
        assert!(results[0].is_some());
        assert!(results[1].is_none());
        drop(results);
        assert_eq!(Arc::strong_count(&batch), 1);
    }

    #[test]
    fn close_completes_batch_and_rejects_new_ones() {
        let shared = Shared::default();
        let batch = armed(2);

        shared.register_many(1, 2, &batch).unwrap();
        shared.close(Error::Closed);

        assert!(batch.poll_done(&mut cx()).is_ready());
        assert!(
            batch
                .results
                .lock()
                .unwrap()
                .iter()
                .all(|r| matches!(r, Some(Err(Error::Closed))))
        );
        assert!(matches!(
            shared.register_many(3, 1, &armed(1)),
            Err(Error::Closed)
        ));
    }

    #[test]
    fn pool_keeps_only_unique_states() {
        let shared = Shared::default();
        let state = shared.take_batch_state();
        let held = state.clone();

        shared.recycle(state);
        assert_eq!(shared.pool.lock().unwrap().len(), 0);
        shared.recycle(held);
        assert_eq!(shared.pool.lock().unwrap().len(), 1);

        let _reused = shared.take_batch_state();
        assert_eq!(shared.pool.lock().unwrap().len(), 0);

        for _ in 0..POOL_MAX + 1 {
            shared.recycle(Arc::default());
        }
        assert_eq!(shared.pool.lock().unwrap().len(), POOL_MAX);
    }

    #[test]
    fn recycle_drops_oversized_arena() {
        let shared = Shared::default();
        let mut state = Arc::new(BatchState::default());

        Arc::get_mut(&mut state)
            .unwrap()
            .arena
            .reserve(2 * POOL_ARENA_MAX);

        shared.recycle(state);

        assert!(shared.take_batch_state().arena.capacity() <= POOL_ARENA_MAX);
    }

    #[test]
    fn recycle_drops_oversized_arena_after_split() {
        let shared = Shared::default();
        let mut state = Arc::new(BatchState::default());
        let arena = &mut Arc::get_mut(&mut state).unwrap().arena;

        // A large payload, already written: only the buffer behind it remains.
        arena.resize(2 * POOL_ARENA_MAX, 0);
        drop(arena.split().freeze());
        shared.recycle(state);

        let mut arena =
            std::mem::take(&mut Arc::get_mut(&mut shared.take_batch_state()).unwrap().arena);
        arena.reserve(1);

        assert!(arena.capacity() <= POOL_ARENA_MAX, "{}", arena.capacity());
    }
}
