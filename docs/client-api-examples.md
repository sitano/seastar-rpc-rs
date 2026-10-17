# Rust client API design notes

Reference crates and structural patterns for the public client API of
`seastar-rpc`. Wire-level details live in
[`rpc-wire-protocol-client-guide.md`](rpc-wire-protocol-client-guide.md).

## Recommended reading order

1. **tokio tutorial, "Channels" chapter** — the manager-task + `mpsc` +
   `oneshot` request/response pattern in its smallest form. Seastar RPC's
   msg-id multiplexing is this pattern plus a pending-call map.
   (`tokio-rs/mini-redis` is a good read for framing and the connection
   actor, but its client issues one command at a time and does not
   multiplex.)
2. **tarpc** (`google/tarpc`) — best example of typed-verb client
   generation without gRPC's protobuf baggage, and of a multiplexed
   request-id client; closest philosophically to what a Seastar verb table
   needs.
3. **tonic** — best example of builder / `Channel` / interceptor /
   `tower` layer composition, if you want production-grade extensibility
   (retries, TLS, load balancing).
4. **jsonrpsee** — modern, well-documented client/server symmetry via
   proc-macros; good API-ergonomics reference.
5. **redis-rs** — good example of a client for a lean, non-protobuf binary
   wire protocol (closer in spirit to Seastar's own hand-rolled framing
   than gRPC is).

## Core structural patterns

### 1. Builder → cheap-clone client handle

Don't make `connect()` block-and-return a raw socket. Split configuration
from connection from the handle users hold:

```rust
let client = Client::builder()
    .connect_timeout(Duration::from_secs(3))
    .default_timeout(Duration::from_secs(5))
    .max_frame_size(16 * 1024 * 1024)
    .connect("host:port").await?;   // TCP connect + negotiation handshake

let reply: Reply = client.call(SOME_VERB, req).await?;  // client is Arc-backed, Clone, Send + Sync
```

Study tonic's `Endpoint`/`Channel` and reqwest's `ClientBuilder`/`Client`.
The handle is just an `Arc<Inner>` — cloning it is cheap and shares the
connection.

### 2. Actor-per-connection + oneshot demux for multiplexed RPC

This is the one that matters most for Seastar RPC specifically, since its
wire format multiplexes many in-flight requests by `msg_id` over one TCP
stream. The idiomatic Rust shape is a background task owning the socket,
with callers talking to it over a channel:

```rust
struct Inner {
    tx: mpsc::Sender<(Frame, oneshot::Sender<Reply>)>,
}

impl Client {
    pub async fn call<Req: Encode, Rep: Decode>(&self, verb: Verb, req: Req) -> Result<Rep, Error> {
        let (tx, rx) = oneshot::channel();
        self.inner.tx.send((encode(verb, req), tx)).await?;
        decode(rx.await??)
    }
}
```

The background task owns a `HashMap<MsgId, oneshot::Sender<Reply>>`, reads
frames off the socket, and completes the matching oneshot; replies for ids
no longer in the map (caller gave up) are dropped silently. Use a
**bounded** channel so a flood of callers gets backpressure instead of
unbounded memory growth, and remove the map entry when a caller's future
is dropped (cancellation).

### 3. Plain verb ids, typed payloads per call

Seastar RPC verbs are numeric ids (64-bit on the wire). Keep the id a plain
alias and pass it as an argument; the request/reply types are chosen at the
call site:

```rust
pub type Verb = u64;

const PING: Verb = 1;
const ADD: Verb = 2;

client.call::<(), ()>(PING, ()).await?;
let sum: i32 = client.call(ADD, (3, 4)).await?;
let raw: Bytes = client.call_raw(ADD, payload, CallOptions::default()).await?;
client.call_with::<(), ()>(FIRE, (), CallOptions::default().no_wait()).await?;  // rpc::no_wait_type
```

`Encode`/`Decode` are the crate's own traits, not serde: the payload format
is dictated by the peer application's C++ `Serializer` (e.g. host-order
raw integers, `u32`-length-prefixed strings, trailing `rpc::optional`
encoded as absent bytes), which doesn't map cleanly onto serde's data
model.

No-wait verbs must be known by the caller, because nothing on the wire
marks a request as replyless; the caller opts in via `CallOptions`, and the
typed reply is `()`. `no_wait_recv()` completes once the frame is written;
`no_wait()` also sets `no_wait_send`, so the call returns as soon as the
request is queued, failing with `Error::QueueFull` instead of waiting for
room in the request queue.

A verb id + payload pair is also exactly what a `tower::Service` request or
a generated stub layer (à la tonic, tarpc, jsonrpsee) would build on, so
typed verb tables can be layered on top later without changing the core.

### 4. Errors: separate transport failure from application-level failure

Seastar RPC has its own exception propagation (remote exceptions get
serialized back as `USER` / `UNKNOWN_VERB` / other). Model that as
variants, not a stringly-typed catch-all:

```rust
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("connection closed")]
    Closed,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("remote exception: {0}")]
    Remote(RemoteMessage), // raw bytes; lossy UTF-8 Display
    #[error("unknown verb {0}")]
    UnknownVerb(Verb),
    #[error("unknown remote exception type {kind}")]
    UnknownException { kind: u32, data: Bytes },
    #[error("call timed out")]
    Timeout,
}
```

aws-sdk-rust / smithy-generated clients are the gold standard here — they
distinguish `SdkError<E>` (transport/serialization) from the operation's
own modeled error enum. Worth skimming for the layering even if it's
heavier than you need.

### 5. Timeouts are part of the wire protocol

Don't hardcode timeouts inside `call()`, but don't leave them entirely to
`tokio::time::timeout` either: when the `TIMEOUT` feature is negotiated,
the request frame carries the remaining time in milliseconds, and the
server uses it to skip expired work. So `call` should accept an optional
deadline (e.g. `call_with_deadline`, or a per-call options struct), use it
both to fill in `timeout_ms` right before the frame is written and to
bound the local wait. Baking retry policy into the client itself
(à la `tower::Layer`) is the more "advanced library" move — tonic builds on
`tower::Service` for exactly this, so interceptors/retries/timeouts compose
instead of being special-cased.

### 6. Connection lifecycle and pooling as a separate concern

If you need multiple connections (not just multiplexing on one), don't
reinvent pooling — look at bb8's or deadpool's manager trait
(`connect` / `is_valid` / `recycle`) and implement that for your
connection type rather than writing bespoke pool logic.

### 7. `#[non_exhaustive]` on public enums/structs

Since this is a wire-format-coupled client, new frame types, features and
exception types will show up. Mark `Error`, config structs, etc.
`#[non_exhaustive]` so adding variants isn't a breaking change — standard
practice in aws-sdk-rust and tonic.

### 8. Explicit batches

Concurrent calls are pipelined and usually share a write, but nothing stops
another caller's frame from landing between them. `Client::batch` makes the
grouping explicit: the batch is one message on the writer queue, its frames
are written back to back and flushed together, and one lock, one queue slot
and one timer serve all of it.

```rust
let mut batch = client.batch();          // keep it: buffers are reused
loop {
    batch.push(ADD, (1, 2));             // encoded into the batch's arena
    batch.push_raw(ECHO, payload);
    for reply in batch.send().await? {
        let payload: Bytes = reply?;
    }
}

for sum in client.call_many::<_, i32>(ADD, (0..10).map(|i| (i, 1))).await? {
    sum?;
}
```

A warm batch allocates nothing per send: payloads share one arena that is
reclaimed once the frames are written, and request/result lists keep their
capacity. `call_many` borrows the same buffers from a small per-client pool.

The outer `Result` fails only if the batch was never queued; the requests
then stay in the batch for a retry. With `no_wait_send` the whole batch is
queued or rejected with `QueueFull`. Once queued, each item has its own
result. On the batch deadline, replies already in are kept and the rest are
`Err(Timeout)`. A batch is not a transaction: the server runs each request
independently.
