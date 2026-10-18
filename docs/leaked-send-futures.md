# Leaked send futures

What happens when a send future is **leaked** instead of dropped, and how the
client handles it.

| | Single call (`client.call`) | Batch (`batch.send`) |
|---|---|---|
| Request queue | loses one slot for good | loses one slot for good |
| Requests | the leaked call's request is never sent | the batch's requests are never sent |
| Pending-reply entries | — | `n` entries stay in the pending table |
| Next use | — | the next `push` / `push_raw` / `clear` / `send` **panics** with [`BATCH_LEAKED_PANIC`](#batches-panic-on-the-next-use) |

The lost queue slot can't be fixed in the client (see
[below](#why-the-client-cant-prevent-it)). A batch makes sure its lost
requests never go unnoticed.

## What counts as leaking

Ordinary cancellation **drops** the future, and that is always safe:

- `select!` choosing another branch
- a `timeout` / `timeout_at` that fires, including a call's own deadline
- aborting the task
- simply dropping the future

Leaking means the future stays alive, but nothing will ever poll it again:

- `std::mem::forget(fut)` or `Box::leak(Box::pin(fut))`
- an `Rc`/`Arc` cycle that holds the future
- a pending future parked somewhere that is never polled or dropped, for
  example in a struct or an idle `FuturesUnordered`

Only a send still **waiting for queue room** is affected. Once its message is
in the queue, the send no longer holds a queue slot, and there is nothing left
to lose.

## The lost queue slot (every send)

### How the queue works

`Client` talks to its writer task through `mpsc::channel(queue)`
(`Client::new` in `src/rpc/client/mod.rs`). Tokio builds that bounded channel
on a semaphore with `queue` permits:

- **`tx.send(msg).await`** first takes a permit, then puts the message in the
  queue.
- **`rx.recv()`**, run by the writer, takes a message out and returns its
  permit.
- **When no permit is free**, the send future adds a waiter node to the
  semaphore's FIFO list and returns `Pending`.
- **When a permit is freed**, tokio gives it straight to the first waiter in
  the list, then wakes that waiter's task. It does not go back to a shared
  pool that anyone can grab.

When a waiting future is **dropped**, its `Drop` removes the waiter from the
list and hands back any permit it was given, so nothing is lost. When it is
**leaked**, `Drop` never runs:

1. The waiter stays in the list.
2. The next freed permit is handed to that waiter.
3. Nothing will ever poll the future again, so it never uses the permit to
   send and never returns it.
4. The channel's capacity is now one lower, permanently. The message the
   leaked future was carrying is never sent.

### Demo (tokio 1.53.1)

A channel with capacity 2, filled up, plus one waiting sender that gets
leaked:

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
    tx.send(2).await.unwrap();                    // full

    let mut waiting = Box::pin(tx.send(3));
    assert!(!poll_once(waiting.as_mut()));        // waiter is now in the list
    std::mem::forget(waiting);                    // leaked: Drop never runs

    rx.recv().await.unwrap();   // frees a slot, which goes to the leaked waiter
    rx.recv().await.unwrap();   // frees a slot, which goes back to the channel
    println!("{:?}", tx.try_send(4)); // Ok(())
    println!("{:?}", tx.try_send(5)); // Err(Full(..))
    println!("{:?}", rx.try_recv());  // Ok(4); message 3 never arrives
}
```

Doing the same with `drop(waiting)` instead of `mem::forget` loses nothing:
the channel is back to its full capacity of 2.

### What it costs

On a `Client` built with queue size `Q`, each leaked waiting send makes things
permanently worse:

- Pipelining depth drops by one.
- `no_wait_send` calls hit `QueueFull` sooner.
- After `Q` leaks, every later send waits forever, or until its deadline
  returns `Timeout`.

A batch is one queue message, so a leaked `batch.send` with 100 requests costs
**one** slot, while 100 leaked `client.call` futures cost 100.

### Why the client can't prevent it

Both send paths wait on the same channel:

```rust
// Inner::exchange, src/rpc/client/mod.rs
self.tx.send(Outgoing::One(request)).await.map_err(|_| Error::Closed)?;
// Batch::exchange, src/rpc/client/batch.rs
inner.tx.send(batch).await.map_err(|_| Error::Closed)?;
```

Avoiding the lost slot would mean working around tokio's semaphore. Safe Rust
allows leaking futures, and no library can stop a caller from doing it. So
this is documented, not fixed.

## Batches: panic on the next use

### What a leaked batch send leaves behind

By the time `batch.send` waits for queue room, it has already registered `n`
pending-reply entries and holds a clone of the batch's buffers
(`Arc<BatchState>`). When the future is leaked:

- The buffers stay held by the leaked future, so they never return to the
  client's pool.
- The guard that removes the pending entries (`BatchGuard`) never runs, so
  the `n` entries stay in the pending table.
- The `Batch` is still usable, since the borrow ended with `mem::forget`,
  but the line that empties it (`len = 0`) never ran. It still counts `n`
  requests that now live only in the leaked future and will never be sent.

Before this was handled, the next `send` panicked on an internal `expect`.
A `push` then `send` silently dropped the `n` requests, and the send then
waited until its deadline for replies to requests it never sent.

### What the batch does

`push`, `push_raw`, `clear` and `send` all check, before touching the
batch's buffers, that the batch owns them (`Arc::get_mut`). If it doesn't
and the batch still counts requests, they call `leaked`
(`src/rpc/client/batch.rs`), which:

1. takes fresh buffers from the pool and resets the count to 0;
2. panics with a `String` message that starts with the public constant
   `seastar_rpc::rpc::BATCH_LEAKED_PANIC`:

   ```
   Batch lost unsent requests to a leaked send() future (2 request(s)) while
   waiting for queue room; drop send futures instead of leaking them (e.g.
   with mem::forget)
   ```

**Correct code can't trigger it.** Every legitimate way for the batch to
share its buffers leaves the count at 0:

- A fire-and-forget send (`NO_WAIT`) resets the count once queued, while the
  writer may still hold the buffers.
- A send cancelled after queueing resets the count before it waits for
  replies.
- A send dropped before queueing releases its clone, and its guard removes
  the pending entries.

So "doesn't own its buffers, count non-zero" only happens after a leak.

**It costs nothing on the normal path.** The ownership check is one the batch
already did before this was handled: `push` switched to fresh buffers when
they were shared, and `send` asserted it owned them. The count is only looked
at once the buffers turn out to be shared, which is rare, and the panic lives
in a separate `#[cold]` function. A leaked send can't be caught when it
happens, because a leaked future runs no code, so the next use of the batch
is the earliest point to notice it.

### Why a panic

- A leak is a bug in the caller's code, and the lost requests can't be
  recovered. Silently resetting would hide that bug.
- `push` returns the request's index (`usize`) and has no way to report an
  error.
- An `Err` from `send` means "never queued, the requests are still in the
  batch, retry". That isn't true after a leak.
- An error is easy to swallow (`let _ = ...`, or a generic `?`).

The payload is a plain `String`, not a custom type: Rust's default panic hook
only prints `&str`/`String` payloads, and anything else shows up in logs as
`Box<dyn Any>` with no message.

### Catching it

A caller can catch the panic with `std::panic::catch_unwind`, with
`futures_util::FutureExt::catch_unwind` for `send`, or as a panicked tokio
task's `JoinError` (`is_panic()`, `into_panic()`). It isn't catchable when the
binary is built with `panic = "abort"`.

To tell it apart from other panics, match on the prefix:

```rust
let leaked = payload
    .downcast_ref::<String>()
    .is_some_and(|m| m.starts_with(seastar_rpc::rpc::BATCH_LEAKED_PANIC));
```

The batch resets before panicking, so after catching it the batch is empty
and works normally. The lost queue slot and the leaked pending entries remain.

## Where this lives

- Code: `Batch::state_mut`, `Batch::exchange`, `leaked` and
  `BATCH_LEAKED_PANIC` in
  `src/rpc/client/batch.rs`.
- API docs: the `# Leaked sends` section on `Batch`, linked from the
  `# Panics` notes on `push`, `push_raw`, `clear` and `send`.
- Tests in `tests/fake_server/batch.rs`: `batch_send_after_leaked_send_panics` and
  `batch_is_usable_after_leak_panic`.
