# TODO: a writer error in the middle of a `Many` drops the request list

Status: deferred minor from the client batch review (branch `ivan/batching`).
Harmless today; recorded for completeness.

## What happens

The writer (`dispatch::writer` in `src/rpc/client/dispatch.rs`) handles a
batch like this:

```rust
Outgoing::Many(batch) => {
    let mut requests = std::mem::take(&mut *batch.requests.lock().unwrap());
    for r in requests.drain(..) {
        feed(&mut sink, r, &shared, &mut acks).await?;   // <- early return on error
        written += 1;
    }
    *batch.requests.lock().unwrap() = requests;
    shared.recycle(batch);
}
```

If `feed` fails (socket write error), `?` returns from the writer:

- The taken `requests` Vec, with its capacity and any requests not yet fed,
  is dropped. The state keeps the empty, capacity-less `Vec` that
  `mem::take` left behind.
- `shared.recycle(batch)` is skipped, so if the `Batch` was already gone
  (fire-and-forget or abandoned), the state is freed rather than pooled.

## Why it's harmless

A writer error closes the connection: every pending slot fails with
`Closed`/`Io`, and the client can't send again. The lost capacity only
matters for a `Batch` on a dead client, and the pool dies with the client.

## Proposed fix (if wanted)

Put the list back before propagating the error:

```rust
let mut requests = std::mem::take(&mut *batch.requests.lock().unwrap());
let mut res = Ok(());
for r in requests.drain(..) {
    if let Err(e) = feed(&mut sink, r, &shared, &mut acks).await {
        res = Err(e);
        break;
    }
    written += 1;
}
requests.clear();
*batch.requests.lock().unwrap() = requests;
shared.recycle(batch);
res?;
```

Test: see `TODO_batch_test_gaps.md` (writer error in the middle of a `Many`).
