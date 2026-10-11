# TODO: leaked send futures (panic or lost requests)

Status: deferred minor from the client batch review (branch `ivan/batching`).

Summary: if a send future that is waiting for queue room is **leaked** instead
of dropped, it ties up one queue slot for good. That comes from tokio's bounded
channel, and single calls already have it. A leaked `Batch::send` has two
extra problems of its own, which we can fix.

## How the queue works

`Client` talks to its writer task through `mpsc::channel(queue)`
(`src/rpc/client/mod.rs`, `Client::new`). Tokio builds that bounded channel on
a semaphore with `queue` permits:

- **`tx.send(msg).await`** first takes a permit, then puts the message in the
  queue.
- **`rx.recv()`**, run by the writer, takes a message out and returns its
  permit.
- **When no permit is free**, the send future adds a waiter node to the
  semaphore's FIFO list and returns `Pending`.
- **When a permit is freed**, tokio gives it straight to the first waiter in
  the list, then wakes that waiter's task. It does not go back to a shared
  pool that anyone can grab.

When the waiting future is **dropped**, its `Drop` removes the waiter from the
list and hands back any permit it was given, so nothing is lost. When it is
**leaked**, `Drop` never runs:

1. The waiter stays in the list.
2. The next freed permit is handed to that waiter.
3. Nothing will ever poll the future again, so it never uses the permit to send
   and never returns it.
4. The channel's capacity is now one lower, permanently. The message the leaked
   future was carrying is never sent.

## Demo (tokio 1.53.1)

A channel with capacity 2, filled up, plus one waiting sender that gets leaked:

```rust
use std::future::Future;
use std::task::Context;
use futures_util::task::noop_waker;
use tokio::sync::mpsc;

fn poll_once<F: Future + ?Sized>(f: std::pin::Pin<&mut F>) -> bool {
    let w = noop_waker();
    f.poll(&mut Context::from_waker(&w)).is_ready()
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let (tx, mut rx) = mpsc::channel::<u32>(2);
    tx.send(1).await.unwrap();
    tx.send(2).await.unwrap();                   // full

    let mut waiting = Box::pin(tx.send(3));
    assert!(!poll_once(waiting.as_mut()));        // waiter is now in the list
    std::mem::forget(waiting);                    // leaked: Drop never runs

    rx.recv().await.unwrap();   // frees a slot, which goes to the leaked waiter
    rx.recv().await.unwrap();   // frees a slot, which goes back to the channel
    println!("{:?}", tx.try_send(4)); // Ok
    println!("{:?}", tx.try_send(5)); // Err: no available capacity
    println!("{:?}", rx.try_recv());  // Ok(4); message 3 never arrives
}
```

Output:

```
full:          capacity=0
after 1 recv:  capacity=0  (slot handed to the leaked waiter)
after 2 recvs: capacity=1  (of max 2)
try_send 4:    Ok(())
try_send 5:    Err("no available capacity")
nothing more arrives: Ok(4)          <- message 3 never shows up
dropped waiter: capacity=1 of 1      <- the same steps with drop() lose nothing
```

On a `Client` built with queue size `Q`, each leaked waiting send makes this
permanently worse:

- Pipelining depth drops by one.
- `no_wait_send` calls hit `QueueFull` sooner.
- After `Q` leaks, every later send waits forever, or until its deadline
  returns `Timeout`.

## What counts as leaking

Ordinary cancellation drops the future, so it is safe:

- `select!` choosing another branch
- a `timeout` / `timeout_at` that fires (our deadline wrapper `before()` in
  `batch.rs`)
- aborting the task
- simply dropping the future

Only these keep the waiter registered with nobody polling it:

- `std::mem::forget(fut)` or `Box::leak(Box::pin(fut))`
- an `Rc`/`Arc` cycle that holds the future
- keeping a pending future alive without ever polling it again, for example
  parked in a struct or an idle `FuturesUnordered` that is never polled or
  dropped

A send that already got its message into the queue no longer holds a permit,
because the writer's `recv()` returned it. Only a send still **waiting for
room** can lose a slot.

## Why single calls already have it

The single-call path (`Inner::exchange` in `src/rpc/client/mod.rs`) does:

```rust
self.tx.send(Outgoing::One(request)).await.map_err(|_| Error::Closed)?;
```

and so does the batch path (`Batch::exchange` in `src/rpc/client/batch.rs`):

```rust
inner.tx.send(batch).await.map_err(|_| Error::Closed)?;
```

So batching adds no new kind of queue leak. It actually makes it smaller: a
batch is one message, so a leaked waiting `batch.send` with 100 requests costs
**one** slot, while 100 leaked `client.call` futures cost 100. Fixing this
inside the client would mean working around tokio's semaphore, and a plain call
couldn't be protected either. Leaking futures is a misuse that safe Rust
allows, not something a library can prevent. **Not fixable here; document
only.**

## What's specific to batches (TODO)

A leaked batch send has two problems of its own, beyond the lost slot.

**The batch's buffers stay held.** The leaked future keeps a clone of the
batch's buffers (`Arc<BatchState>`), so they can't be reused or returned to the
client's pool. The guard that clears the batch's pending-reply entries
(`BatchGuard`) never runs either, so those `n` entries stay in the pending
table.

**The `Batch` itself is left in a bad state.** Once the boxed future is
forgotten, the borrow checker lets you use the `Batch` again. But the line that
empties the batch (`*len = 0; *encoded = 0`) never ran, so it still claims to
hold `n` requests while sharing its buffers with the leaked future. What
happens next depends on the next call:

- **`send()` without a new `push`:** it panics on
  `expect("a batch with unsent requests owns its state")`.
- **`push()` and then `send()`:** `state_mut` sees the buffers are shared and
  switches to fresh ones from the pool. The `n` old requests are silently
  lost, and `len` (old `n` plus new pushes) no longer matches the requests
  actually sent, so `register_many`/`arm` expect replies that never come and
  that send waits until its deadline.

### Proposed fix

The cheapest fix is to have `send` notice it doesn't own its buffers, reset
the count and return an error instead of panicking:

- In `Batch::exchange`, if `len != 0` and `Arc::get_mut(state)` is `None`,
  set `len = 0`, `encoded = 0`, take a fresh state from the pool and return
  an error (e.g. `Error::Closed`).
- In `state_mut`, when switching to a fresh state, also reset `len` and
  `encoded`, so later pushes start from a consistent count.
- Add a test: push, start a send against a full queue (`queue = 1`, slot
  already taken), `mem::forget` the boxed future, then check that `send`
  returns an error rather than panicking, and that a later push + send
  round-trips.

The lost queue slot and the leaked pending entries remain. They are the
caller's leak, just like with single calls.
