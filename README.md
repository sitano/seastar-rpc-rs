# seastar-rpc

[![CI](https://github.com/sitano/seastar-rpc-rs/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/sitano/seastar-rpc-rs/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Rust 1.85+](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)

Seastar RPC async Rust server/client.

Status: early. The client side works against the C++ Seastar reference
server; the server side, compression and streaming are not implemented yet.
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
| Batches | `Batch` / `call_many`: requests written back to back and flushed together, replies in push order, no allocation per send once warm |
| Deadlines | Per call or client default; sent to the server when `TIMEOUT` is negotiated |
| No-wait verbs | `no_wait()`, `no_wait_send()`, `no_wait_recv()` |
| Errors | Remote exceptions (user, unknown verb, unknown kind), timeouts, queue full, closed connection, decode errors |
| Serialization | `Encode` / `Decode` for integers, floats, `bool`, strings, byte buffers, tuples, trailing `Option`; implement them for your own types |
| Raw layer | `Sink` of request frames with caller-chosen `msg_id`s and a `Stream` of every response, no pending-call table |
| Limits | Connect timeout, max frame size, bounded request queue |

### Not supported yet

| Area | Status |
|---|---|
| Server | Codecs decode both directions, but there is no server/dispatcher yet |
| Compression | `COMPRESS` (LZ4, LZ4_FRAGMENTED) is not offered; a server enabling it anyway is rejected |
| Streaming | Sink/source streams (`STREAM_PARENT`, stream frames) |
| Connection management | Reconnect, connection pools, load balancing |
| TLS | Not built in; bring your own stream through `handshake` |
| Tower middleware | No `tower::Service` impl for `Client`, so timeout, retry, rate-limit and load-balance layers can't be stacked on calls |
| Handler duration | Negotiated and parsed, visible on the raw layer only, not exposed by `Client` |
| Transactional batches | Out of scope: a batch is contiguous on the wire, not all-or-nothing on the server |

Known batch-related follow-ups are tracked in [`docs/TODO_*.md`](docs/).

## Quick start

The Rust client talks to the C++ Seastar reference server in
[`examples/seastar/`](examples/seastar/). Building it needs Seastar, either
installed or as a local build tree (by default `~/Projects/seastar/build/debug`).
See [`HACKING.md`](examples/seastar/HACKING.md) for how Seastar is found.

1. Build and start the server (listens on `127.0.0.1:10000`):

   ```sh
   make seastar-server
   ```

   This configures and builds `examples/seastar/build` with CMake + Ninja
   and `clang++` (override with `make seastar-server CXX=g++`), then runs
   `rpc_server`. Use `SEASTAR_RPC_ADDR=host:port` to change the port.

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
for reply in batch.send(CallOptions::default()).await? {
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
let conn = Client::builder().connect_raw("127.0.0.1:10000").await?;
let (mut tx, rx, features) = conn.into_parts();
tx.send_request(1, ADD, payload, None).await?;
```

### Runnable examples

[`examples/`](examples/) has programs that run against the C++ server
(`make seastar-server` first, then `make examples` runs the first four):

| Example | Shows |
|---|---|
| [`calls.rs`](examples/calls.rs) | `call` and `call_with`: scalars, strings, tuples, trailing `Option`, timeouts, no-wait, remote exceptions, unknown verbs, decode errors |
| [`concurrent.rs`](examples/concurrent.rs) | Sequential loop vs `try_join_all`, overlapping slow calls with `try_join!`, spawned tasks sharing a cloned `Client`, cancellation with `select!` |
| [`batches.rs`](examples/batches.rs) | A reused `Batch` with mixed verbs, per-item errors, `call_many` in a loop, `call_many_with` with a deadline and no-wait, batches from several tasks |
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
SEASTAR_RPC_ADDR=127.0.0.1:10000 cargo bench --bench call
```

They measure single-call latency, pipelined calls and batches at several
depths.

## Documentation

- [`docs/rpc-wire-protocol-client-guide.md`](docs/rpc-wire-protocol-client-guide.md) —
  byte-level `seastar::rpc` wire protocol from the client side.
- [`docs/rpc-wire-protocol-server-guide.md`](docs/rpc-wire-protocol-server-guide.md) —
  the same, from the server side.
- [`docs/client-api-examples.md`](docs/client-api-examples.md) — design
  notes for the Rust client API.
- [`docs/leaked-send-futures.md`](docs/leaked-send-futures.md) — what
  leaking a send future costs, and why a `Batch` panics afterwards.
- [`examples/seastar/`](examples/seastar/) — C++ Seastar RPC echo server and
  client used as the interop reference; see its
  [`HACKING.md`](examples/seastar/HACKING.md) for build steps, the verb
  table and the payload encoding.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this crate by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
