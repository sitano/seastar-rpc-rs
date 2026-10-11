# TODO: a user-owned `Batch` keeps its peak memory

Status: left undecided by the client batch review ("declined to judge": the
spec only bounds pooled states). Needs a decision.

## What happens

The pool limits only apply in `BatchState::reset()`, which runs when a state
goes back to the pool (`Shared::recycle`):

- the arena is dropped above `POOL_ARENA_MAX` (1 MiB),
- the vectors are shrunk to `POOL_SLOTS_MAX` (1024) slots.

A `Batch` that the user keeps alive never goes through `reset()`, so it keeps
the capacity of its largest send:

- `requests`, `results` and `Batch::replies`: capacity of the most items ever
  sent in one go;
- the arena: as large as the biggest send's payloads, because `push` reserves
  and the encoder grows it with no cap (`arena_hint` is capped at 1 MiB, the
  arena is not).

## Example

```rust
let mut batch = client.batch();         // kept for the life of the service
// Once, at startup: a bulk load.
for row in &rows_100k { batch.push(PUT, row); }   // say 100k items, 64 MiB
batch.send(CallOptions::default()).await?;
// From now on, small sends of ~10 items, but the batch holds
// ~64 MiB of arena plus three vectors with 100k slots each.
```

This is by design for reuse: a hot batch keeps its buffers so it doesn't
allocate. It only hurts when a long-lived batch sees one outlier.

## Options

1. **Document it:** "a `Batch` keeps the capacity of its largest send; drop it
   or use a separate batch for bulk loads".
2. **Add `Batch::shrink(&mut self)`:** run the same limits as `reset()` on
   demand (needs a unique state; if it's shared, there is nothing to shrink).
3. **Apply the pool limits after every send:** keeps memory bounded but makes
   sends above the limits allocate every time, which defeats reuse for large
   batches.

Recommendation: 1 + 2.
