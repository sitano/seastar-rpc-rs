# Release notes

## 0.3.0 (unreleased)

Changes since 0.2.0. One small break: see
[Breaking changes](#breaking-changes).

### Highlights

- **Compression.** `ClientBuilder::offer_compression` offers `LZ4` and
  `LZ4_FRAGMENTED` (the Seastar `COMPRESS` feature) in preference order. A
  compressed connection reuses its buffers, so a warm one allocates nothing
  per frame.
- **Streams.** `rpc::sink` / `rpc::source` arguments and return values are
  supported. Each stream runs on its own connection, opened with
  `Client::open_stream` or `ClientBuilder::connect_stream` /
  `handshake_stream`. A warm stream allocates nothing per item.

### Added

- `Compression` (re-exported from `rpc`) and `ClientBuilder::offer_compression`.
  If the server picks an algorithm that was not offered, the connect fails.
  Before, the client rejected any server that enabled compression.
- `rpc::stream` with `StreamConnection`, `StreamSink`, `StreamSource` and
  `StreamId` (the value a call passes for a sink or source).
  `StreamFrame` is `#[non_exhaustive]`.
  - `send_item` sends one item and flushes. `feed_item` queues items for one
    `flush`; it is about 2.5x faster than `send_item` on many items (see
    `docs/benchmarks.md`).
  - `recv_item` reads the next item, `None` at end of stream.
  - `close` sends end-of-stream and flushes but keeps the socket open, since
    Seastar drops unread data, end-of-stream included, when the connection
    ends early.
  - `StreamConnection::finish` (or `StreamSource::wait_closed` on split
    halves) waits for the peer to close the connection, so the peer has read
    everything sent.
  - `StreamConnection::new` builds a stream over any socket.
- `Error::Unsupported`: the server did not negotiate a feature the call
  needs, e.g. `open_stream` on a server without `CONNECTION_ID`.
  `Error::InvalidState`: the call is not valid in this state, e.g. a send
  after `StreamSink::close`.
- `Client::open_stream` opens a stream to the address and with the builder
  the client was connected with.
- `codec::compress` (`CompressionCodec`) and `codec::stream`
  (`StreamCodec`, `StreamFrame`) for users of the raw codecs.
- Examples: `streaming.rs`; the Rust examples take `--compress`. The C++
  reference server enables LZ4 and LZ4_FRAGMENTED, adds a streaming domain
  and verbs 18–21 (see `examples/seastar/HACKING.md`), and builds in any
  Seastar mode with `SEASTAR_MODE` (`make seastar-<mode>`).
- Benchmark `benches/stream.rs`.

### Breaking changes

- `Client::connection_id` returns `Option<ConnectionId>` instead of
  `Option<u64>`; use `.0` for the number. `ConnectionId` is what
  `ClientBuilder::connect_stream` / `handshake_stream` take as the parent,
  so a `StreamId` can't be passed there by mistake.

### Dependencies

- Added `lz4_flex` 0.14 (decoder in its checked `safe-decode` mode).

### Documentation

- `docs/benchmarks.md`: call and stream throughput against the C++ server.
- Wire guide §9: how streams close, including the one-direction cases.
- README: compression, streams, and building the C++ server per mode.

## 0.2.0 (2026-10-18)

Changes since 0.1.0. This release breaks the 0.1 API; see
[Breaking changes](#breaking-changes-1) for how to migrate.

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
