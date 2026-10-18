# TODO: batch test gaps

Status: deferred minor from the client batch review (branch `ivan/batching`).

The review found three paths that no test covers deterministically.

## 1. "Writer still holds the state" switch

`Batch::state_mut` (`src/rpc/client/batch.rs`) switches to a fresh pooled
state when the writer still holds the current one. The unit test
`state_switch_keeps_both_spares` in `batch.rs` covers the switch itself by
holding a clone of the state. End to end, `batch_is_reusable` in
`tests/fake_server.rs` only says the switch *may* happen: whether the writer
has released the state by the next push depends on timing.

Proposed test: make the writer slow to release the state, for example a
fake-server socket the test doesn't read from yet, so the writer is blocked
writing a large `NO_WAIT` batch. Then:

1. `push` + `send(no_wait())` returns once queued.
2. `push` again; the state must switch (assert with a debug-only counter or
   by checking the next send still round-trips).
3. Unblock the server, check every request of both sends arrives in order and
   the second send's replies are correct.

## 2. Writer error in the middle of a `Many`

Nothing covers a socket error while the writer is feeding a batch.

Proposed test: a fake server that reads part of a large batch and then
closes the connection (or a test `AsyncWrite` that fails after N bytes):

- `send` returns `Ok`, with `Err(Closed | Io)` for the items not answered.
- Items answered before the close keep their `Ok` results.
- The client then reports closed; a new `send` returns the outer `Err(Closed)`
  and keeps the requests in the batch.

The writer puts the batch's request list back and recycles the state before
propagating the error, so the list keeps its capacity.

## 3. Arena size check after a split, end to end

`recycle_drops_oversized_arena_after_split` (unit test in `dispatch.rs`)
covers `reset()`. Nothing checks it through the public API.

Proposed test: `call_many` with one 8 MiB payload against the fake server, then
a small `call_many`; using a debug-only accessor or dhat, check that the pooled
state's arena didn't keep 8 MiB.

## Also worth adding

- A test for each fix in the other `TODO_*` files (leaked send,
  `push_raw` first, `arena_hint` decay).
- The review's stress test (random 0-3 ms reply delays, duplicate replies,
  1 ms deadlines, sends abandoned mid-flight, `NO_WAIT` then push and `clear`,
  interleaved `call_many_with`, 3000 rounds on one `Batch`). It passed 5 of 5
  runs but was deleted (`tests/zz_batch_review.rs`). Bringing it back as an
  `#[ignore]` test would keep it runnable.
