# TODO: switching batch states loses a spare result list

Status: deferred minor from the client batch review (branch `ivan/batching`).
Costs one allocation on a rare path.

## Background

A `BatchState` has two result vectors that ping-pong so neither is
reallocated:

- `results` (in the state, filled by the reader), and
- `spare`, which `Batch::new` takes into `Batch::replies`.

After each send, `exchange` swaps `results` with `replies`. On drop,
`Batch::drop` puts `replies` back into `state.spare` if the state is unique,
then recycles it.

`Batch::state_mut` switches to a fresh state from the pool when the current
one is still held by the writer (the last send was fire-and-forget or
abandoned after queueing):

```rust
if Arc::get_mut(state).is_none() {
    *state = self.inner.shared.take_batch_state();
}
```

## What happens

Say state A was taken by `Batch::new` (A.spare moved into `replies`), and a
`NO_WAIT` send leaves A with the writer. The next push switches to B:

1. **A comes back without a spare.** The writer recycles A once it has written
   the frames, but A.spare is still in the batch's `replies`. The next `Batch`
   that takes A starts with an empty `replies` and allocates on its first
   send.
2. **B's spare is overwritten.** When the batch is dropped, `Batch::drop`
   moves `replies` into B.spare and drops whatever B.spare held.

Also, when a `Batch` is dropped while its state is still shared, `replies`
is dropped and the state is later recycled without a spare.

## Example

```rust
let mut b = client.batch();               // takes A, replies = A.spare
b.push(ADD, (1, 2));
b.send(CallOptions::default().no_wait()).await?;    // A goes to the writer
b.push(ADD, (3, 4));                      // A still shared: switch to B
b.send(CallOptions::default()).await?;
drop(b);                                  // B.spare = replies; old B.spare dropped
// The writer recycles A with an empty spare: the next batch on A allocates once.
```

## Proposed fix

Cheapest option: on a switch, swap the batch's `replies` with the new state's
`spare` (`mem::swap(&mut self.replies, &mut new.spare)`) and drop nothing;
in `Batch::drop`, only overwrite `spare` if it's empty. A still comes back
spare-less.

A complete fix makes `spare` a `Mutex` (like `requests`) so the batch can hand
`replies` back to the old, shared state before switching. Probably not worth
the extra lock for a path that only follows fire-and-forget or abandoned sends.

Test: deterministic state switch (see `TODO_batch_test_gaps.md`), then check
with dhat that the next batch on each state doesn't allocate a result list.
