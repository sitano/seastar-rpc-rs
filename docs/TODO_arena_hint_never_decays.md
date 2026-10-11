# TODO: `arena_hint` never decays

Status: deferred minor from the client batch review (branch `ivan/batching`).

## What happens

`BatchState::arena_hint` (`src/rpc/client/dispatch.rs`) records the largest
payload total a send of that state has produced, capped at `POOL_ARENA_MAX`
(1 MiB). `Batch::push` (`src/rpc/client/batch.rs`) reserves
`max(arena_hint, ARENA_CHUNK)` bytes on the first push of every send, so the
whole send fits in the arena without allocating while earlier payloads still
pin it.

The hint only goes up. `BatchState::reset()`, which runs when a state goes
back to the pool, clears the arena and shrinks the vectors but leaves
`arena_hint` alone.

## Example

```rust
// One big call: the pooled state now remembers ~1 MiB.
client.call_many::<_, ()>(PUT, std::iter::once(vec![0u8; 900 * 1024])).await?;

// Every later small call that picks up this state reserves ~1 MiB
// before encoding a few bytes.
for i in 0..1_000_000 {
    client.call_many::<_, i32>(ADD, [(i, 1)]).await?;
}
```

- The pool holds up to `POOL_MAX` (16) states, so in the worst case about
  16 MiB stays reserved for traffic that needs a few KiB.
- When `reset()` drops an oversized arena (`try_reclaim(POOL_ARENA_MAX + 1)`),
  the hint survives. The next send allocates a full hint-sized buffer again.

It is bounded (1 MiB per state), it doesn't affect correctness, and it was
within the cost we accepted, which is why it was deferred.

## Proposed fix

Pick one:

- **Reset on recycle:** `self.arena_hint = 0` in `BatchState::reset()`. A
  `Batch` the user keeps (never pooled in between) keeps its hint, so
  steady-state reuse stays allocation-free. A pooled state may allocate once
  on its first send after being taken.
- **Decay:** `self.arena_hint /= 2` in `reset()`, so a single outlier fades
  after a few recycles while a steady large workload stays warm.

Either way, also reset the hint when `reset()` drops the arena.

Test: recycle a state with `arena_hint = 1 MiB`, take it again, push one small
request, and check that `arena.capacity()` is near `ARENA_CHUNK`, not 1 MiB.
Check `tests/batch_allocs.rs` still passes (< 1 allocation per send).
