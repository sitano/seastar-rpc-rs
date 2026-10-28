# seastar-rpc

[![CI](https://github.com/sitano/seastar-rpc-rs/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/sitano/seastar-rpc-rs/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Rust 1.85+](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)

Seastar RPC async Rust server/client.

Status: early. The client side works against the C++ Seastar reference
server; the server side is not implemented yet.
See [Features](#features).

- [Features](#features)
- [Quick start](#quick-start)
- [Usage](#usage)
- [Architecture](#architecture)
- [Testing](#testing)
- [Benchmarks](#benchmarks)
- [Documentation](#documentation)

## Features

### Supported

| Area | What works |
|---|---|
| Transport | tokio TCP (`connect`), or any `AsyncRead` + `AsyncWrite` pair (`handshake`) |
| Negotiation | `SSTARRPC` handshake; `TIMEOUT`, `HANDLER_DURATION` and isolation cookie offered; `CONNECTION_ID` read |
| Calls | Typed `call::<Req, Rep>`, raw payloads `call_raw`, many concurrent calls pipelined on one connection |
| Compression | `offer_compression`: `COMPRESS` with `LZ4` and `LZ4_FRAGMENTED`, applied below the raw layer; warm compressed sends and receives allocate nothing per frame |
| Batches | `Batch` / `call_many`: requests written back to back and flushed together, replies in push order, no allocation per send once warm |
| Deadlines | Per call or client default; sent to the server when `TIMEOUT` is negotiated |
| No-wait verbs | `no_wait()`, `no_wait_send()`, `no_wait_recv()` |
| Errors | Remote exceptions (user, unknown verb, unknown kind), timeouts, queue full, closed connection, decode errors |
| Serialization | `Encode` / `Decode` for integers, floats, `bool`, strings, byte buffers, tuples, trailing `Option`; implement them for your own types |
| Streaming | `open_stream`: `rpc::sink` / `rpc::source` streams on their own connection (`STREAM_PARENT`), passed to verbs as a `StreamId`, both directions, EOS close, compressed like calls |
| Raw layer | `Sink` of request frames with caller-chosen `msg_id`s and a `Stream` of every response, no pending-call table |
| Limits | Connect timeout, max frame size, bounded request queue |

### Not supported yet

| Area | Status |
|---|---|
| Server | Codecs decode both directions, but there is no server/dispatcher yet |
| Custom compressors | Only the built-in `LZ4` / `LZ4_FRAGMENTED`; no pluggable compressor and no out-of-band empty frames sent (received ones are skipped) |
| Stream abort on close | Closing a `Client` doesn't abort its open streams; the server aborts them, which ends their sources with `Closed` |
| Connection management | Reconnect, connection pools, load balancing |
| TLS | Not built in; bring your own stream through `handshake` |
| Tower middleware | No `tower::Service` impl for `Client`, so timeout, retry, rate-limit and load-balance layers can't be stacked on calls |
| Handler duration | Negotiated and parsed, visible on the raw layer only, not exposed by `Client` |
| Transactional batches | Out of scope: a batch is contiguous on the wire, not all-or-nothing on the server |

Known follow-ups are tracked in [`docs/`](docs/) as `{KIND}_{COMPONENT}_P{N}_{name}.md` files.

## Quick start

The Rust client talks to the C++ Seastar reference server in
[`examples/seastar/`](examples/seastar/). Building it needs Seastar, either
installed or as a local build tree (by default `~/Projects/seastar/build/debug`).
See [`HACKING.md`](examples/seastar/HACKING.md) for how Seastar is found.

1. Build and start the server (listens on `127.0.0.1:10000`):

   ```sh
   make seastar-server
   ```

   This configures and builds `examples/seastar/build/debug` with CMake +
   Ninja and `clang++` (override with `make seastar-server CXX=g++`), then
   runs `rpc_server`. Pick another Seastar build mode with
   `SEASTAR_MODE=release` (or `dev`, `sanitize`, `fuzz`); `make seastar-<mode>`
   only builds. Use `SEASTAR_RPC_ADDR=host:port` to change the port.

2. In another terminal, call it:

   ```rust
   use seastar_rpc::rpc::{Client, Verb};

   /// `int (int, int)`
   const ADD: Verb = 2;

   #[tokio::main(flavor = "current_thread")]
   async fn main() -> Result<(), Box<dyn std::error::Error>> {
       let client = Client::builder().connect("127.0.0.1:10000").await?;
       let sum: i32 = client.call(ADD, (3, 4)).await?;
       assert_eq!(sum, 7);
       Ok(())
   }
   ```

   Or use the bundled example, which does the same call from the command line:

   ```sh
   make example ARGS="2 3 4"   # verb 2: 3 + 4 -> 7
   ```

## Usage

### Typed calls

```rust
use seastar_rpc::rpc::{CallOptions, Client, Verb};
use std::time::Duration;

const ADD: Verb = 2;

let client = Client::builder()
    .connect_timeout(Duration::from_secs(1))
    .default_timeout(Duration::from_secs(5))
    .connect("127.0.0.1:10000")
    .await?;

let sum: i32 = client.call(ADD, (3, 4)).await?;
let sum: i32 = client
    .call_with(ADD, (3, 4), CallOptions::default().timeout(Duration::from_millis(100)))
    .await?;
```

`Client` is a cheap-to-clone handle; clones share one connection, which
closes once every clone is dropped. Concurrent calls are pipelined and
matched to replies by `msg_id`.

### No-wait verbs

```rust
// Send and return once queued; no reply is expected.
client.call_with::<_, ()>(LOG, "hello", CallOptions::default().no_wait()).await?;
```

### Batches

```rust
// Reused batch: frames are contiguous and flushed together.
let mut batch = client.batch();
batch.push(ADD, (1, 2));
batch.push(ADD, (3, 4));
for reply in batch.send().await? {
    let payload = reply?;
}

// Typed, homogeneous.
for sum in client.call_many::<_, i32>(ADD, (0..10).map(|i| (i, 1))).await? {
    println!("{}", sum?);
}
```

An outer `Err` means the batch was never queued and its requests are still in
it; once queued, each item has its own result. See section 8 of
[`docs/client-api-examples.md`](docs/client-api-examples.md).

### Raw connection

```rust
let RawConnection { sender: mut tx, receiver: rx, features, .. } =
    Client::builder().connect_raw("127.0.0.1:10000").await?;
tx.send_request(1, ADD, payload, None).await?;
```

### Streams

```rust
use seastar_rpc::rpc::StreamId;

// `rpc::sink<int> (rpc::source<int>)`
const DOUBLE: Verb = 18;

// A new TCP connection to the same server, tied to `client`'s connection.
let mut stream = client.open_stream().await?;
let back: StreamId = client.call(DOUBLE, stream.id).await?;

stream.sink.send_item(&21i32).await?;
stream.sink.close().await?; // sends EOS, keeps the connection open
while let Some(x) = stream.source.recv_item::<i32>().await? {
    println!("{x}");
}
```

Each stream is its own connection, which carries both directions. Close
the sink and read the source to its end before dropping the stream;
dropping it earlier aborts it on the server.

### Runnable examples

[`examples/`](examples/) has programs that run against the C++ server
(`make seastar-server` first, then `make examples` runs the first six):

| Example | Shows |
|---|---|
| [`calls.rs`](examples/calls.rs) | `call` and `call_with`: scalars, strings, tuples, trailing `Option`, timeouts, no-wait, remote exceptions, unknown verbs, decode errors |
| [`concurrent.rs`](examples/concurrent.rs) | Sequential loop vs `try_join_all`, overlapping slow calls with `try_join!`, spawned tasks sharing a cloned `Client`, cancellation with `select!` |
| [`batches.rs`](examples/batches.rs) | A reused `Batch` with mixed verbs, per-item errors, `call_many` in a loop, `call_many_with` with a deadline and no-wait, batches from several tasks |
| [`raw.rs`](examples/raw.rs) | A raw connection with no client: pipelines `count` `add` calls with caller-chosen `msg_id`s through `send_all` and checks every reply while sending |
| [`streaming.rs`](examples/streaming.rs) | A stream handed to a verb that returns a sink on it: writes ints and reads them back doubled, both directions at once |
| [`serde.rs`](examples/serde.rs) | `Encode` / `Decode` for any serde type through a bincode wrapper, in single calls and `call_many` |
| [`client.rs`](examples/client.rs) | Command-line client for one verb (`make example ARGS="2 3 4"`) |

```sh
cargo run --release --example batches -- 127.0.0.1:10000
```

### Custom types

Implement `Encode` / `Decode` to match the peer's C++ serializer; the
built-ins follow `examples/seastar/rpc_serializer.hh`.

## Architecture

Layers, each usable on its own:

- `codec` — negotiation, frames, exceptions, payload `Encode`/`Decode`
  (built-ins match `examples/seastar/rpc_serializer.hh`; implement the
  traits for your own types). Codecs encode and decode both directions.
- `raw` — `Client::builder().connect_raw(addr)` gives a `Sink` of request
  frames with caller-chosen `msg_id`s and a `Stream` of every response,
  with no pending-call table.
- `stream` — `client.open_stream()` gives a `StreamSink` and a
  `StreamSource` on a new connection, plus the `StreamId` a call passes
  for `rpc::sink` / `rpc::source`. No background task: each half is used
  directly.
- `Client` — multiplexes concurrent calls by `msg_id` over one connection,
  with one writer task (coalesces queued frames into one flush) and one
  reader task (matches replies to callers); `Batch` sends a group of
  requests as one queue message.

## Testing

- `cargo test` runs unit tests, in-process fake-server tests and the batch
  allocation test (`tests/batch_allocs.rs`, dhat).
- Interop tests against the C++ server are opt-in:
  `SEASTAR_RPC_ADDR=127.0.0.1:10000 cargo test --test interop -- --ignored`.
- `make ci` runs what CI runs.

## Benchmarks

Benchmarks run against the C++ reference server (start it with
`make seastar-server` first) and are skipped if it can't be reached:

```sh
SEASTAR_RPC_ADDR=127.0.0.1:10000 cargo bench --bench call --bench stream
```

They measure single-call latency, pipelined calls and batches at several
depths, and for streams the open/close cost and items sent over one stream.

Rough numbers for one CPU core on the server: one connection, which Seastar
serves on one shard, a single-threaded client, release builds, loopback,
4-byte payloads. Details in
[`docs/benchmarks.md`](docs/benchmarks.md).

| Workload | Rate |
|----------|------|
| One call at a time | 5.3 µs, 189k calls/s |
| Pipelined, 16 / 128 in flight | 1.17M / 1.70M calls/s |
| Batch of 16 / 128 | 1.50M / 2.73M calls/s |
| Stream, `send_item` (flush per item) | 1.0M items/s |
| Stream, flush every 16+ items | 2.5M items/s |

## Documentation

- [`docs/rpc-wire-protocol-client-guide.md`](docs/rpc-wire-protocol-client-guide.md) —
  byte-level `seastar::rpc` wire protocol from the client side.
- [`docs/rpc-wire-protocol-server-guide.md`](docs/rpc-wire-protocol-server-guide.md) —
  the same, from the server side.
- [`docs/client-api-examples.md`](docs/client-api-examples.md) — design
  notes for the Rust client API.
- [`docs/benchmarks.md`](docs/benchmarks.md) — rough benchmark results
  for one server core.
- [`docs/leaked-send-futures.md`](docs/leaked-send-futures.md) — what
  leaking a send future costs, and why a `Batch` panics afterwards.
- [`examples/seastar/`](examples/seastar/) — C++ Seastar RPC echo server and
  client used as the interop reference; see its
  [`HACKING.md`](examples/seastar/HACKING.md) for build steps, the verb
  table and the payload encoding.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this crate by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
