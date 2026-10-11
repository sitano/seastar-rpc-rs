# TODO: `send`'s result borrows the batch

Status: ergonomics note from the client batch review (branch `ivan/batching`).
Not a bug; it follows the agreed API.

## What happens

```rust
pub async fn send(&mut self, opts: CallOptions) -> Result<Replies<'_>>
```

`Replies<'_>` drains `Batch::replies` in place (no allocation), so it borrows
the batch mutably. The whole `Result` carries that borrow, **including the
`Err` case**, for as long as it's in scope.

## Example

This doesn't compile:

```rust
let r = batch.send(CallOptions::default().no_wait_send()).await;
if r.is_err() {
    // error[E0502]: cannot borrow `batch` as immutable because it is also
    // borrowed as mutable
    assert!(!batch.is_empty()); // "requests stay in the batch on Err"
}
```

The same thing came up while implementing the tests
(`batch_dropped_then_late_replies_are_ignored`). The workarounds:

```rust
// End the borrow right away.
let failed = batch.send(opts).await.is_err();
if failed { retry_later(&mut batch); }

// Or match and let each arm end the borrow.
match batch.send(opts).await {
    Ok(replies) => for r in replies { handle(r?); },
    Err(Error::QueueFull) => { /* batch usable again here */ }
    Err(e) => return Err(e),
}
```

## Options

1. **Document it** (minimum): add the example above to the `Batch::send` doc
   comment and to "Explicit batches" in `docs/client-api-examples.md`.
2. **Split the API:** `send(&mut self, opts) -> Result<usize>` (number of
   results), plus `replies(&mut self) -> Replies<'_>` to drain them. The error
   no longer borrows anything; the cost is a second call and results that can
   be left undrained (they would be cleared on the next send, as now).

Recommendation: option 1 now; option 2 if users hit it.
