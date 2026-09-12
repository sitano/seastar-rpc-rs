# Rust client API design notes

Reference crates and structural patterns for the public client API of
`seastar-rpc-rs`. Wire-level details live in
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
let client = Client::builder("host:port")
    .compression(true)
    .connect_timeout(Duration::from_secs(3))
    .max_frame_size(16 * 1024 * 1024)
    .connect().await?;   // does the negotiation handshake

let reply = client.call::<SomeVerb>(req).await?;  // client is Arc-backed, Clone, Send + Sync
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
    pub async fn call<V: Verb>(&self, req: V::Request) -> Result<V::Reply, Error> {
        let (tx, rx) = oneshot::channel();
        self.inner.tx.send((encode::<V>(req), tx)).await?;
        rx.await?
    }
}
```

The background task owns a `HashMap<MsgId, oneshot::Sender<Reply>>`, reads
frames off the socket, and completes the matching oneshot; replies for ids
no longer in the map (caller gave up) are dropped silently. Use a
**bounded** channel so a flood of callers gets backpressure instead of
unbounded memory growth, and remove the map entry when a caller's future
is dropped (cancellation).

### 3. Typed verbs instead of stringly/opaque calls

Seastar RPC verbs are numeric ids (64-bit on the wire) with a request/reply
type pair. Encode that as a trait rather than exposing raw ids:

```rust
pub trait Verb {
    const ID: u64;
    /// `true` for `rpc::no_wait_type` verbs: no response frame is ever sent.
    const NO_WAIT: bool = false;
    type Request: Encode;
    type Reply: Decode;
}

pub struct Ping;
impl Verb for Ping {
    const ID: u64 = 1;
    type Request = ();
    type Reply = ();
}

client.call::<Ping>(()).await?;
```

`Encode`/`Decode` are the crate's own traits, not serde: the payload format
is dictated by the peer application's C++ `Serializer` (e.g. host-order
raw integers, `u32`-length-prefixed strings, trailing `rpc::optional`
encoded as absent bytes), which doesn't map cleanly onto serde's data
model. No-wait verbs must be known per verb, because nothing on the wire
marks a request as replyless; `call` for such a verb resolves once the
frame is written.

This mirrors the generated-stub ergonomics of tonic (from `.proto`) and
tarpc (from a `#[tarpc::service]` trait) without needing codegen — a plain
trait + const is enough for a hand-rolled verb table. jsonrpsee is a good
reference for proc-macro-driven client/server symmetry if you later want to
generate verbs from a shared definition.

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
    Remote(String),
    #[error("unknown verb {0}")]
    UnknownVerb(u64),
    #[error("unknown remote exception type {0}")]
    UnknownException(u32),
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
