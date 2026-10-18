//! Explicit request batches: one queue message whose frames are written back
//! to back, with no other caller's frame in between, and flushed together.
//!
//! A `Batch` keeps its buffers across sends and returns them to the client's
//! pool when dropped, so a warmed-up batch allocates nothing per send.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use tokio::time::Instant;

use super::dispatch::{
    BatchState, Outgoing, POOL_ARENA_MAX, POOL_SLOTS_MAX, Request, Shared, shrink,
};
use super::{CallOptions, Inner};
use crate::rpc::codec::serialize::{Decode, Encode, from_payload};
use crate::rpc::error::{Error, Result};
use crate::rpc::verb::Verb;

/// Smallest arena reservation.
const ARENA_CHUNK: usize = 4096;
/// Free arena space below which `push` reserves a new chunk rather than let
/// the encoder grow the arena a few bytes at a time.
const ARENA_MIN_FREE: usize = 64;

const LIVE: &str = "batch state is taken only on drop";

/// Start of the panic message after a [leaked send](Batch#leaked-sends), for
/// telling that panic apart once caught:
///
/// ```
/// # use seastar_rpc::rpc::BATCH_LEAKED_PANIC;
/// # let payload: Box<dyn std::any::Any + Send> = Box::new(String::new());
/// let leaked = payload
///     .downcast_ref::<String>()
///     .is_some_and(|m| m.starts_with(BATCH_LEAKED_PANIC));
/// # assert!(!leaked);
/// ```
pub const BATCH_LEAKED_PANIC: &str = "Batch lost unsent requests to a leaked send() future";

/// Requests sent together; see [`Client::batch`](super::Client::batch).
///
/// # Memory
///
/// A batch keeps the capacity of its largest send, so a warm batch allocates
/// nothing per send. A long-lived batch that once sent a large outlier keeps
/// that memory: call [`shrink`](Self::shrink) after it, or use a separate
/// batch for bulk loads and drop it. Dropped batches go back to the client's
/// pool, which applies the same limits.
///
/// # Leaked sends
///
/// Dropping a [`send`](Self::send) future is always safe. *Leaking* one
/// while it waits for queue room (`mem::forget`, an `Rc` cycle, or keeping it
/// alive without ever polling it again) loses the requests it carries: the
/// leaked future keeps them and never sends them. It also holds one slot of
/// the client's request queue for good, as any leaked call does.
///
/// The batch doesn't let that pass silently. The next [`push`](Self::push),
/// [`push_raw`](Self::push_raw), [`clear`](Self::clear) or
/// [`send`](Self::send) panics, naming how many requests were lost. The
/// message starts with [`BATCH_LEAKED_PANIC`]. Before
/// panicking it empties itself and takes fresh buffers, so a caller that
/// catches the panic can keep using it.
pub struct Batch {
    inner: Arc<Inner>,
    /// `None` only while dropping.
    state: Option<Arc<BatchState>>,
    /// Results of the last send. Swapped with the state's list on the next
    /// one, so both keep their capacity.
    replies: Vec<Option<Result<Bytes>>>,
    /// Requests pushed since the last send was queued.
    len: usize,
    /// Payload bytes pushed since the last send was queued.
    encoded: usize,
}

impl std::fmt::Debug for Batch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Batch").field("len", &self.len).finish()
    }
}

impl Batch {
    pub(super) fn new(inner: Arc<Inner>) -> Self {
        let mut state = inner.shared.take_batch_state();
        let replies = std::mem::take(
            Arc::get_mut(&mut state)
                .expect("pooled batch state is unique")
                .spare
                .get_mut()
                .unwrap(),
        );

        Batch {
            inner,
            state: Some(state),
            len: 0,
            replies,
            encoded: 0,
        }
    }

    /// Encode `request` for `verb` and append it. Returns its index among the
    /// replies.
    ///
    /// # Panics
    ///
    /// If `request.encode` panics, or after a [leaked send](Self#leaked-sends).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn f(client: seastar_rpc::rpc::Client) -> Result<(), Box<dyn std::error::Error>> {
    /// use seastar_rpc::rpc::Verb;
    ///
    /// /// `int (int, int)`
    /// const ADD: Verb = 2;
    /// /// `sstring (sstring)`
    /// const ECHO: Verb = 15;
    ///
    /// let mut batch = client.batch();
    /// let sum = batch.push(ADD, (1, 2));
    /// let echo = batch.push(ECHO, "hi");
    /// assert_eq!((sum, echo), (0, 1));
    /// # Ok(()) }
    /// ```
    pub fn push<Req: Encode>(&mut self, verb: Verb, request: Req) -> usize {
        // Not `len`: `push_raw` counts there but leaves the arena unique.
        let first = self.encoded == 0;
        let encoded = self.encoded;
        let state = self.state_mut();

        // Payloads of a send share the arena, which can only be reclaimed
        // once they are all gone. So reserve room for a whole send up front,
        // when the last send's payloads are written and the arena is unique
        // again: then the send fits without allocating.
        let want = if first { state.arena_hint } else { 0 };
        if state.arena.capacity() < want.max(ARENA_MIN_FREE) {
            state.arena.reserve(state.arena_hint.max(ARENA_CHUNK));
        }

        // Drop what an `encode` that panicked part-way left behind, or it
        // would prefix this payload. Only touches bytes after the last split.
        state.arena.clear();

        request.encode(&mut state.arena);

        let payload = state.arena.split().freeze();
        let encoded = encoded + payload.len();

        state.arena_hint = state.arena_hint.max(encoded.min(POOL_ARENA_MAX));

        self.encoded = encoded;
        self.push_payload(verb, payload)
    }

    /// Append an already-encoded request. Returns its index among the
    /// replies.
    ///
    /// # Panics
    ///
    /// After a [leaked send](Self#leaked-sends).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn f(client: seastar_rpc::rpc::Client) -> Result<(), Box<dyn std::error::Error>> {
    /// use seastar_rpc::rpc::Verb;
    /// use seastar_rpc::rpc::codec::serialize::to_payload;
    ///
    /// /// `sstring (sstring)`
    /// const ECHO: Verb = 15;
    ///
    /// // Encode once, send many times: `Bytes` clones share the buffer.
    /// let payload = to_payload("hi");
    /// let mut batch = client.batch();
    /// for _ in 0..3 {
    ///     batch.push_raw(ECHO, payload.clone());
    /// }
    /// # Ok(()) }
    /// ```
    pub fn push_raw(&mut self, verb: Verb, payload: impl Into<Bytes>) -> usize {
        self.push_payload(verb, payload.into())
    }

    /// Requests pushed and not yet sent.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Drop the requests pushed and not yet sent.
    ///
    /// # Panics
    ///
    /// After a [leaked send](Self#leaked-sends).
    pub fn clear(&mut self) {
        self.state_mut().requests.get_mut().unwrap().clear();

        self.len = 0;
        self.encoded = 0;
    }

    /// Release memory kept from earlier sends, down to the limits the
    /// client's pool keeps for idle batches.
    ///
    /// A `Batch` keeps the capacity of its largest send so later sends don't
    /// allocate. After an outlier (say a bulk load of 100k items), call this,
    /// or drop the batch and take a new one, so a long-lived batch doesn't
    /// hold that memory for good.
    ///
    /// Does nothing while requests are pushed and not sent. To pick your own
    /// limits, see [`shrink_arena`](Self::shrink_arena) and
    /// [`shrink_slots`](Self::shrink_slots).
    pub fn shrink(&mut self) {
        self.shrink_arena(POOL_ARENA_MAX);
        self.shrink_slots(POOL_SLOTS_MAX);
    }

    /// Drop the payload buffer if it holds more than `size` bytes, and stop
    /// reserving room for the largest earlier send. The next send grows the
    /// buffer again as it needs.
    ///
    /// Does nothing while requests are pushed and not sent.
    pub fn shrink_arena(&mut self, size: usize) {
        if self.len != 0 {
            return;
        }

        self.state_mut().shrink_arena(size);
    }

    /// Reserve `bytes` of payload room at the start of each later send, so a
    /// send that size encodes without allocating, even right after a
    /// [`shrink`](Self::shrink) or on a batch fresh from the pool.
    ///
    /// The batch raises the hint by itself to its largest send (up to 1 MiB),
    /// and shrinking resets it to 0.
    ///
    /// # Panics
    ///
    /// After a [leaked send](Self#leaked-sends).
    pub fn set_arena_hint(&mut self, bytes: usize) {
        self.state_mut().arena_hint = bytes;
    }

    /// Shrink the request and reply lists to room for `count` requests.
    ///
    /// Does nothing while requests are pushed and not sent.
    pub fn shrink_slots(&mut self, count: usize) {
        if self.len != 0 {
            return;
        }

        self.state_mut().shrink_slots(count);

        shrink(&mut self.replies, count);
    }

    /// Queue the pushed requests and wait for every reply, in push order.
    ///
    /// The error means the batch was never queued (connection closed,
    /// [`Error::QueueFull`] with `no_wait_send`, or the deadline passed while
    /// waiting for queue room): nothing was sent and the requests stay in the
    /// batch, so it can be sent again. Once queued, the batch is empty and
    /// each item has its own result. Dropping the future after queueing
    /// abandons the outstanding items; late replies are discarded.
    ///
    /// # Panics
    ///
    /// After a [leaked send](Self#leaked-sends).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn f(client: seastar_rpc::rpc::Client) -> Result<(), Box<dyn std::error::Error>> {
    /// use seastar_rpc::rpc::Verb;
    /// use seastar_rpc::rpc::codec::serialize::from_payload;
    ///
    /// /// `int (int, int)`
    /// const ADD: Verb = 2;
    ///
    /// // Keep the batch across sends: its buffers are reused.
    /// let mut batch = client.batch();
    /// for i in 0..3 {
    ///     batch.push(ADD, (i, 1));
    ///     batch.push(ADD, (i, 2));
    ///     for reply in batch.send().await? {
    ///         let sum: i32 = from_payload(reply?)?;
    ///     }
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn send(&mut self) -> Result<Replies<'_>> {
        self.send_with(CallOptions::default()).await
    }

    /// [`send`](Self::send) with `opts` applied to the whole batch: on its
    /// deadline the call returns what has arrived and `Err(Timeout)` for the
    /// rest.
    ///
    /// # Panics
    ///
    /// After a [leaked send](Self#leaked-sends).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn f(client: seastar_rpc::rpc::Client) -> Result<(), Box<dyn std::error::Error>> {
    /// use std::time::Duration;
    ///
    /// use seastar_rpc::rpc::{Error, Verb, with_timeout};
    ///
    /// /// `int (int, int)`
    /// const ADD: Verb = 2;
    ///
    /// let mut batch = client.batch();
    /// batch.push(ADD, (1, 2));
    /// batch.push(ADD, (3, 4));
    /// for reply in batch.send_with(with_timeout(Duration::from_millis(100))).await? {
    ///     match reply {
    ///         Ok(payload) => println!("{} bytes", payload.len()),
    ///         // This one didn't arrive in time; the others still count.
    ///         Err(Error::Timeout) => println!("timed out"),
    ///         Err(e) => return Err(e.into()),
    ///     }
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn send_with(&mut self, opts: CallOptions) -> Result<Replies<'_>> {
        self.exchange(opts).await?;

        Ok(Replies(self.replies.drain(..)))
    }

    /// [`send`](Self::send), decoding every reply as `Rep`.
    pub(super) async fn send_decoded<Rep: Decode>(
        mut self,
        opts: CallOptions,
    ) -> Result<ManyReplies<Rep>> {
        self.exchange(opts).await?;

        Ok(ManyReplies {
            batch: self,
            next: 0,
            _rep: PhantomData,
        })
    }

    fn push_payload(&mut self, verb: Verb, payload: Bytes) -> usize {
        self.state_mut().requests.get_mut().unwrap().push(Request {
            msg_id: 0,
            verb,
            payload,
            deadline: None,
            ack: false,
        });

        self.len += 1;
        self.len - 1
    }

    /// # Panics
    ///
    /// After a [leaked send](Self#leaked-sends), once the batch is reset.
    fn state_mut(&mut self) -> &mut BatchState {
        let state = self.state.as_mut().expect(LIVE);

        if Arc::get_mut(state).is_none() {
            // Requests still counted mean a send leaked while waiting for
            // queue room: it holds the state and the requests in it.
            if self.len != 0 {
                leaked(&self.inner, state, &mut self.len, &mut self.encoded);
            }

            // The last send was fire-and-forget or abandoned: the writer
            // still holds that state and pools it when done. Give it back
            // our result list, so it returns to the pool with a spare.
            *state.spare.lock().unwrap() = std::mem::take(&mut self.replies);

            // The writer may have let go since the check: then pool it here.
            let old = std::mem::replace(state, self.inner.shared.take_batch_state());
            self.inner.shared.recycle(old);

            let fresh = Arc::get_mut(state).expect("pooled batch state is unique");
            self.replies = std::mem::take(fresh.spare.get_mut().unwrap());

            return fresh;
        }

        Arc::get_mut(state).expect("pooled batch state is unique")
    }

    /// Send the pushed requests and leave their results in `replies`.
    async fn exchange(&mut self, opts: CallOptions) -> Result<()> {
        self.replies.clear();

        if self.len == 0 {
            return Ok(());
        }

        let deadline = self.inner.deadline(&opts);
        let base = self.stamp(&opts, deadline);

        if opts.no_wait_send && opts.no_wait_recv {
            return self.fire_and_forget();
        }

        self.round_trip(&opts, deadline, base).await
    }

    /// Give the pushed requests their msg_ids, deadline and ack flag. Returns
    /// the first msg_id.
    ///
    /// # Panics
    ///
    /// After a [leaked send](Self#leaked-sends).
    fn stamp(&mut self, opts: &CallOptions, deadline: Option<Instant>) -> i64 {
        let base = self
            .inner
            .next_id
            .fetch_add(self.len as i64, Ordering::Relaxed);
        let ack = opts.no_wait_recv && !opts.no_wait_send;

        let state = self.state.as_mut().expect(LIVE);

        // Unsent requests in a state the batch doesn't own: a send leaked.
        let Some(owned) = Arc::get_mut(state) else {
            leaked(&self.inner, state, &mut self.len, &mut self.encoded);
        };

        let requests = owned.requests.get_mut().unwrap();
        for (r, msg_id) in requests.iter_mut().zip(base..) {
            r.msg_id = msg_id;
            r.deadline = deadline;
            r.ack = ack;
        }

        base
    }

    /// Queue the requests without waiting for anything. Nothing will
    /// complete a slot, so none is registered; every result is an empty `Ok`.
    fn fire_and_forget(&mut self) -> Result<()> {
        let state = self.state.as_ref().expect(LIVE);

        self.inner.shared.check_open()?;
        self.inner.try_queue(Outgoing::Many(state.clone()))?;

        let n = std::mem::take(&mut self.len);
        self.encoded = 0;

        self.replies.resize_with(n, || Some(Ok(Bytes::new())));

        Ok(())
    }

    /// Register the requests, queue them and wait for every reply or the
    /// deadline. Unfinished results stay `None`, read as `Err(Timeout)`.
    async fn round_trip(
        &mut self,
        opts: &CallOptions,
        deadline: Option<Instant>,
        base: i64,
    ) -> Result<()> {
        let n = self.len;
        let Batch {
            inner,
            state,
            replies,
            len,
            encoded,
        } = self;

        let state = state.as_mut().expect(LIVE);

        let unique = "a batch with unsent requests owns its state";
        Arc::get_mut(state).expect(unique).arm(n);
        inner.shared.register_many(base, n, state)?;

        // Removes the pending entries if this future is dropped (caller
        // cancellation) before every reply arrives.
        let _guard = BatchGuard {
            shared: &inner.shared,
            base,
            n,
        };

        let exchange = async {
            let batch = Outgoing::Many(state.clone());

            if opts.no_wait_send {
                inner.try_queue(batch)?;
            } else {
                inner.tx.send(batch).await.map_err(|_| Error::Closed)?;
            }

            // Queued: the requests are the writer's now. Reset before
            // waiting, so a cancelled send leaves an empty batch.
            *len = 0;
            *encoded = 0;

            std::future::poll_fn(|cx| state.poll_done(cx)).await;

            Ok::<(), Error>(())
        };

        // After fence() or forget_many(), no reply touches the state: it can
        // be reused at once.
        match before(deadline, exchange).await {
            Some(r) => {
                r?;
                inner.shared.fence();
            }

            // Deadline passed before there was queue room: nothing sent.
            None if *len != 0 => return Err(Error::Timeout),

            // Deadline passed while waiting for replies.
            None => inner.shared.forget_many(base, n),
        }

        std::mem::swap(&mut *state.results.lock().unwrap(), replies);

        Ok(())
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        let Some(mut state) = self.state.take() else {
            return;
        };

        // Shared: the writer still holds it and pools it when done.
        let replies = std::mem::take(&mut self.replies);
        match Arc::get_mut(&mut state) {
            Some(s) => *s.spare.get_mut().unwrap() = replies,
            None => *state.spare.lock().unwrap() = replies,
        }

        self.inner.shared.recycle(state);
    }
}

/// Replies of a [`Batch::send`], in push order.
pub struct Replies<'a>(std::vec::Drain<'a, Option<Result<Bytes>>>);

impl Iterator for Replies<'_> {
    type Item = Result<Bytes>;

    fn next(&mut self) -> Option<Result<Bytes>> {
        self.0.next().map(reply)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl ExactSizeIterator for Replies<'_> {}

/// Decoded replies of [`Client::call_many`](super::Client::call_many), in
/// request order. Dropping it returns the batch's buffers to the pool.
pub struct ManyReplies<Rep> {
    batch: Batch,
    next: usize,
    _rep: PhantomData<fn() -> Rep>,
}

impl<Rep: Decode> Iterator for ManyReplies<Rep> {
    type Item = Result<Rep>;

    fn next(&mut self) -> Option<Result<Rep>> {
        let slot = self.batch.replies.get_mut(self.next)?.take();
        self.next += 1;
        Some(reply(slot).and_then(|p| Ok(from_payload(p)?)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.batch.replies.len() - self.next;
        (n, Some(n))
    }
}

impl<Rep: Decode> ExactSizeIterator for ManyReplies<Rep> {}

/// A result never filled in timed out.
fn reply(slot: Option<Result<Bytes>>) -> Result<Bytes> {
    slot.unwrap_or_else(|| Err(Error::Timeout))
}

/// Move a batch whose state a [leaked send](Batch#leaked-sends) kept onto a
/// fresh one, empty, then panic.
#[cold]
#[inline(never)]
fn leaked(inner: &Inner, state: &mut Arc<BatchState>, len: &mut usize, encoded: &mut usize) -> ! {
    *state = inner.shared.take_batch_state();

    let lost = std::mem::take(len);
    *encoded = 0;

    panic!(
        "{BATCH_LEAKED_PANIC} ({lost} request(s)) while waiting for queue \
         room; drop send futures instead of leaking them (e.g. with \
         mem::forget)"
    );
}

/// Run `f` until `deadline`; `None` if the deadline passed first.
async fn before<F: Future>(deadline: Option<Instant>, f: F) -> Option<F::Output> {
    match deadline {
        Some(d) => tokio::time::timeout_at(d, f).await.ok(),
        None => Some(f.await),
    }
}

struct BatchGuard<'a> {
    shared: &'a Shared,
    base: i64,
    n: usize,
}

impl Drop for BatchGuard<'_> {
    fn drop(&mut self) {
        self.shared.forget_many(self.base, self.n);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicI64;

    use tokio::sync::mpsc;

    use super::*;

    fn inner() -> (Arc<Inner>, mpsc::Receiver<Outgoing>) {
        let (tx, rx) = mpsc::channel(1);

        let inner = Inner {
            features: Default::default(),
            default_timeout: None,
            next_id: AtomicI64::new(1),
            shared: Arc::new(Shared::default()),
            tx,
        };

        (Arc::new(inner), rx)
    }

    #[test]
    fn state_switch_keeps_both_spares() {
        let (inner, _rx) = inner();
        let mut batch = Batch::new(inner.clone());
        batch.replies.reserve(8);

        // Stand-in for the writer holding the state after a fire-and-forget send.
        let held = batch.state.clone().unwrap();
        batch.push(0, ());
        assert!(!Arc::ptr_eq(&held, batch.state.as_ref().unwrap()));

        // The held state got the batch's result list back.
        assert!(held.spare.lock().unwrap().capacity() >= 8);

        // Drop with the new state shared too: its spare is kept.
        batch.clear();
        batch.replies.reserve(4);
        let held2 = batch.state.clone().unwrap();
        drop(batch);
        assert!(held2.spare.lock().unwrap().capacity() >= 4);

        inner.shared.recycle(held);
        inner.shared.recycle(held2);

        for _ in 0..2 {
            let mut s = inner.shared.take_batch_state();
            let spare = Arc::get_mut(&mut s).unwrap().spare.get_mut().unwrap();
            assert!(spare.capacity() >= 4);
        }
    }
}
