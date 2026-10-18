# Release notes

## 0.2.0 (unreleased)

Changes since 0.1.0. This release breaks the 0.1 API; see
[Breaking changes](#breaking-changes) for how to migrate.

### Highlights

- **Batches.** `Client::batch()` returns a `Batch`: push several requests,
  then `send()` writes them back to back and flushes them together. Replies
  come back in push order, each with its own result. A batch reuses its
  buffers, so once warm a send allocates nothing (asserted by
  `tests/batch_allocs.rs`).
- **`call_many` / `call_many_with`** call one verb once per request as a
  single batch and decode each reply as it is read.

### Added

- `Batch` with `push`, `push_raw` (already-encoded payloads), `len`,
  `is_empty`, `clear`, `send` and `send_with(CallOptions)`. `send_with`
  applies the options to the whole batch: on the deadline it returns what
  arrived and `Err(Timeout)` for the rest.
- `Batch::shrink`, `shrink_arena`, `shrink_slots` and `set_arena_hint`, to
  release memory after an outlier send or to pre-size the payload buffer.
- `Replies` and `ManyReplies` iterators over a batch's results.
- `BATCH_LEAKED_PANIC`: the start of the panic message a `Batch` raises when
  it is used after one of its `send` futures was leaked (for example with
  `mem::forget`). See `docs/leaked-send-futures.md`.
- `RequestFrame::new(msg_id, verb, payload)` and
  `RequestFrame::with_timeout(Option<Duration>)`.
- `rpc::raw` re-exports `RequestFrame` and `ResponseFrame`; `rpc` re-exports
  `from_payload` and `to_payload`.
- Examples: `raw.rs` (pipelined calls over a raw connection) and `serde.rs`
  (`Encode` / `Decode` for serde types through bincode). The C++ reference
  server gained verb 16 (silent `add`) and verb 17 (struct sum).

### Breaking changes

- **`RequestFrame` is `#[non_exhaustive]`.** Struct literals no longer
  compile outside the crate:

  ```rust
  // 0.1
  let frame = RequestFrame { timeout_ms: 0, verb, msg_id, payload };
  // 0.2
  let frame = RequestFrame::new(msg_id, verb, payload).with_timeout(timeout);
  ```

- **`RawConnection::into_parts` is removed.** Destructure the struct instead:

  ```rust
  // 0.1
  let (sender, receiver, features) = conn.into_parts();
  // 0.2
  let RawConnection { sender, receiver, features, .. } = conn;
  ```

- **For users of 0.1.1-rc.1 only:** `Batch::send(opts)` is now `send()`;
  pass options with `send_with(opts)`.

### Fixed (since 0.1.1-rc.1)

- A `Batch` no longer keeps leftover bytes from an `encode` that panicked
  part-way; they used to prefix the next payload.
- A `Batch` used after a leaked `send` future panics with
  `BATCH_LEAKED_PANIC` instead of silently losing its requests.
- The result of `Batch::send` no longer keeps the batch borrowed for the
  whole scope of the result, `Err` included.
- A send that starts with `push_raw` now reserves room for the whole send.
- Result lists and the batch request list keep their capacity across a
  state switch and after a write error, instead of being reallocated.
- A pooled batch state keeps its arena-size hint, so a send larger than one
  chunk does not allocate every time.

### Documentation

- `docs/leaked-send-futures.md`: what leaking a send future costs and why a
  `Batch` panics afterwards.
- README: batches, raw connections, the new examples, and Tower middleware
  listed as not supported yet.
