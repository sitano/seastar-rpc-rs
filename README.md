# seastar-rpc

[![CI](https://github.com/sitano/seastar-rpc-rs/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/sitano/seastar-rpc-rs/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Rust 1.85+](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)

Seastar RPC async Rust server/client.

Status: early. A tokio client is implemented (negotiation, pipelined
request/response, typed verbs, no-wait verbs, remote exceptions, `TIMEOUT`).
LZ4 compression, sink/source streaming and the server side are not yet.

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

Layers, each usable on its own:

- `codec` — negotiation, frames, exceptions, payload `Encode`/`Decode`
  (built-ins match `examples/seastar/rpc_serializer.hh`; implement the
  traits for your own types). Codecs encode and decode both directions.
- `raw` — `Client::builder().connect_raw(addr)` gives a `Sink` of request
  frames with caller-chosen `msg_id`s and a `Stream` of every response,
  with no pending-call table.
- `Client` — multiplexes concurrent calls by `msg_id`;
  `call::<Request, Reply>(verb, request)` for typed calls,
  `call_raw(verb, payload, opts)` for raw payloads;
  `CallOptions::default().no_wait()` for no-wait verbs.

Tests: `cargo test` runs unit and in-process fake-server tests. Interop
tests against the C++ server are opt-in:
`SEASTAR_RPC_ADDR=127.0.0.1:10000 cargo test --test interop -- --ignored`.

- [`docs/rpc-wire-protocol-client-guide.md`](docs/rpc-wire-protocol-client-guide.md) —
  byte-level `seastar::rpc` wire protocol from the client side.
- [`docs/rpc-wire-protocol-server-guide.md`](docs/rpc-wire-protocol-server-guide.md) —
  the same, from the server side.
- [`docs/client-api-examples.md`](docs/client-api-examples.md) — design
  notes for the Rust client API.
- [`examples/seastar/`](examples/seastar/) — C++ Seastar RPC echo server and
  client used as the interop reference; see its
  [`HACKING.md`](examples/seastar/HACKING.md) for build steps, the verb
  table and the payload encoding.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this crate by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
