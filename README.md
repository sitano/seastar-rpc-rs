# seastar-rpc-rs

Seastar RPC async Rust server/client.

Status: early. The crate is a placeholder; the protocol has been documented
and a C++ reference peer is available to test against.

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
