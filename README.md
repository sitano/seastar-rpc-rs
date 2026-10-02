# seastar-rpc

Seastar RPC async Rust server/client.

Status: early. A tokio client is implemented (negotiation, pipelined
request/response, typed verbs, no-wait verbs, remote exceptions, `TIMEOUT`).
LZ4 compression, sink/source streaming and the server side are not yet.

```rust
use seastar_rpc::rpc::{Client, Verb};

/// `int (int, int)`
const ADD: Verb = 2;

let client = Client::builder().connect("127.0.0.1:10000").await?;
let sum: i32 = client.call(ADD, (3, 4)).await?;
assert_eq!(sum, 7);
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
