# TODO: `push_raw` before `push` skips the whole-send reservation

Status: deferred minor from the client batch review (branch `ivan/batching`).
Not confirmed by running; found by reading the code.

## What happens

`Batch::push` (`src/rpc/client/batch.rs`) decides whether this is the first
push of a send with:

```rust
let first = self.len == 0;
...
let want = if first { state.arena_hint } else { 0 };
if state.arena.capacity() < want.max(ARENA_MIN_FREE) {
    state.arena.reserve(state.arena_hint.max(ARENA_CHUNK));
}
```

`push_raw` doesn't encode into the arena, but it does increment `len`. So if a
send starts with `push_raw`, the first `push` sees `len != 0`, reserves only
when fewer than `ARENA_MIN_FREE` (64) bytes are free, and the up-front
reservation of `arena_hint` bytes is skipped.

The reservation exists because a send's payloads share one arena buffer: once
the first payload is split off, the buffer is no longer unique and `reserve`
can't reclaim it. Later pushes that run out of room have to allocate a new
buffer.

## Example

```rust
let mut batch = client.batch();
loop {
    batch.push_raw(ECHO, header.clone()); // len = 1, arena untouched
    for i in 0..32 {
        batch.push(ADD, (i, 1));          // first = false: no hint-sized reserve
    }
    for r in batch.send(CallOptions::default()).await? { r?; }
}
```

Each send can allocate once or more in the arena, instead of zero. Only
performance is affected, not correctness.

## Proposed fix

Use the payload byte counter, which only `push` advances:

```rust
let first = self.encoded == 0;
```

`encoded` is reset together with `len` when a send is queued and on `clear()`,
so it marks "no arena payload in this send yet" exactly.

Test: a variant of `tests/batch_allocs.rs` that starts every send with a
`push_raw`, then pushes 32 encoded requests; assert < 1 allocation per send.
It should fail before the change and pass after.
