# AGENTS.md

Rust (edition 2024, MSRV 1.85) client/server for the C++ Seastar RPC wire protocol, crate `seastar-rpc`. Only the client side exists; server and streaming are not implemented yet (see README "Not supported yet"). Repowise tooling notes live in `.claude/CLAUDE.md`.

## Commands

`make help` lists all targets.

- `make build` — `cargo build --all-targets`
- `make test` — unit tests, fake-server tests, dhat batch-allocation test
- `make lint` — clippy on all targets/features with `-D warnings`
- `make fmt` / `make fmt-check`
- `make ci` — what CI runs: fmt-check, lint, test, doc
- Single test: `cargo test --test fake_server <name>`, or `cargo test --lib <name>` for unit tests in `src/`

Needs the C++ reference server (`make seastar-server`, listens on `127.0.0.1:10000`; requires a Seastar build, see `examples/seastar/HACKING.md`, default `~/Projects/seastar/build/debug`):

- `make test-interop` — `tests/interop.rs`, all `#[ignore]`d and run with `--ignored`, `SEASTAR_RPC_ADDR` picks the server
- `make bench` — criterion `benches/call.rs`, skips if the server is unreachable
- `make examples`, `make example ARGS="2 3 4"`
- `make dhat` — heap profile of the client example (`--cfg dhat_heap`)

## Architecture

Three layers under `src/rpc/`, each usable on its own:

- `codec/` — role-agnostic wire format: every codec both encodes and decodes, so the same code serves a client and (future) server. `negotiation.rs` (`SSTARRPC` handshake, `Features`), `frame.rs` (request/response frames; header layout depends on negotiated features, captured once in `FrameLayout`), `compress.rs` (`COMPRESS` envelope, LZ4 / LZ4_FRAGMENTED; `Compressed<C>` wraps a frame codec and passes through when off), `exception.rs` (negative `msg_id` responses), `serialize.rs` (`Encode`/`Decode`). Module docs cite sections of `docs/rpc-wire-protocol-client-guide.md` ("wire guide §N").
- `raw.rs` — a negotiated connection as `RawSender` (`Sink<RequestFrame>`, caller picks `msg_id`) and `RawReceiver` (`Stream` of every response). No pending-call table.
- `client/` — `Client` multiplexes calls over one raw connection. `dispatch.rs` runs separate writer and reader tasks (one task blocking on a write could deadlock against a peer blocked writing replies). Callers register a slot in a shared pending map before queueing; the reader completes it by `msg_id`. Slots hold the caller's waker, so a call allocates nothing per request. `batch.rs`: a `Batch` is one queue message whose frames are written contiguously and flushed together; its buffers are pooled and reused so a warm batch allocates nothing per send — `tests/batch_allocs.rs` asserts this with dhat (own binary because dhat replaces the global allocator).

Things that aren't obvious from one file:

- Payload encoding is not defined by Seastar; built-in `Encode`/`Decode` impls must match `examples/seastar/rpc_serializer.hh` (LE fixed-width scalars, `u32`-length-prefixed strings/bytes, tuples unframed, `Option` only in trailing position). Change both sides together.
- The verb table for the reference server is in `examples/seastar/HACKING.md`; `examples/seastar/rpc_server.cc` implements it. Adding a verb used by interop tests means editing the C++ server too.
- Leaked (`mem::forget`) send futures permanently lose a queue slot; a leaked `Batch` panics on next use with `BATCH_LEAKED_PANIC`. Dropping/cancelling is always safe. See `docs/leaked-send-futures.md`.
- `Batch::send` returning outer `Err` means nothing was queued and the requests are still in the batch; once queued each item has its own result.
- `tests/fake_server/` builds a scripted in-process server (`server.rs`) from the crate's own codecs in the server direction — the place for deterministic client behaviour tests without C++. Tests are split by topic: `calls.rs`, `connection.rs`, `batch.rs`.
- Known batch follow-ups are tracked in `docs/TODO_index.md` / `docs/TODO_*.md` (excluded from the published crate).
- API design rationale (builder + cheap-clone handle, actor-per-connection, `#[non_exhaustive]` public types, transport vs application errors): `docs/client-api-examples.md`.

## Rules

- Never add AI co-author to commits.
- Commit messages follow `.gitmessage` (`git config commit.template .gitmessage`): `type(scope): subject`, then a body saying why, then how. Commit with `-s`.
- When you write English, ensure a human can understand what you are saying.
- We track TODOs in `docs/TODO_*.md` and index them by priority in `docs/TODO_index.md`.
- When we implement a TODO from the index we remove it. If the result comment are important write a separate doc in `docs/`.
- When you write code, separate logical blocks by empty lines to be easy to read with an eye.
- When you write code, comment non-obvious actions in the code for a reader to disambiguate.
- When you write code, try minimize allocations to zero on hot-paths.
