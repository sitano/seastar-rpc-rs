# Seastar RPC wire protocol — server implementation reference

This is the server-side counterpart to [`rpc-wire-protocol-client-guide.md`](rpc-wire-protocol-client-guide.md),
for implementing an interoperable `seastar::rpc` **server** (e.g. in Rust)
that real Seastar RPC clients can connect to. The wire formats are shared
with the client guide and are not repeated in full detail here — this
document focuses on what's different when you're the accepting/responding
side: negotiation is reactive instead of initiating, you assign connection
identity, you choose the compressor, you're responsible for the unknown-verb
protocol, and you own handler-duration measurement and exception encoding.
Cross-references to [`rpc-wire-protocol-client-guide.md`](rpc-wire-protocol-client-guide.md) are marked
**[client guide §N]**.

Source anchors: `src/rpc/rpc.cc`, `include/seastar/rpc/rpc.hh`,
`include/seastar/rpc/rpc_impl.hh`, `include/seastar/rpc/rpc_types.hh`,
`src/rpc/lz4_compressor.cc`, `src/rpc/lz4_fragmented_compressor.cc` — all
paths and line numbers are relative to the Seastar repository at commit
`86228808` and will drift as Seastar evolves. `doc/*.md` references are
likewise Seastar's own docs, not files in this repository.

**Read this first**: same caveat as the client guide — Seastar RPC fixes the
envelope (negotiation, frame headers, compression wrapper, exception
encoding, connection IDs) but the actual argument/return-value payload
encoding is defined by the application's C++ `Serializer`, not by the RPC
layer. A from-scratch server is only wire-compatible with real clients of a
*specific* application once you've also matched that application's
`Serializer` for every verb you implement. See **[client guide §7]**.

## Table of contents

1. [Server connection lifecycle](#1-server-connection-lifecycle)
2. [Accepting negotiation](#2-accepting-negotiation)
3. [Assigning connection identity](#3-assigning-connection-identity)
4. [Choosing compression](#4-choosing-compression)
5. [Request loop & verb dispatch](#5-request-loop--verb-dispatch)
6. [Building response frames](#6-building-response-frames)
7. [Unknown verb handling](#7-unknown-verb-handling)
8. [Exception encoding on the server](#8-exception-encoding-on-the-server)
9. [Accepting streams](#9-accepting-streams)
10. [Resource limiting & backpressure (design, not wire)](#10-resource-limiting--backpressure-design-not-wire)
11. [Shutdown semantics](#11-shutdown-semantics)
12. [End-to-end data flow diagrams](#12-end-to-end-data-flow-diagrams)
13. [Rust server implementation checklist](#13-rust-server-implementation-checklist)
14. [Gotchas found only in the source](#14-gotchas-found-only-in-the-source)

---

## 1. Server connection lifecycle

```
listen(addr) ──► accept() loop, one task/connection per accepted socket
                    │
                    ▼
        recv negotiation_frame from client   (server does NOT speak first)
        validate magic "SSTARRPC"
        decide accepted features, build reply feature map
        send negotiation_frame back
                    │
                    ▼
        [[ connection is "negotiated" ]]
        if STREAM_PARENT was in the client's offer:
            → this connection now only exchanges stream frames (§9)
        else:
            → loop: read request_frame, dispatch by verb, send response_frame
              (fully asynchronous — do not serialize handling of independent
              requests; a slow verb must not block replies for other
              in-flight requests on the same connection)
                    │
                    ▼
        loop exits on EOF / read error / explicit abort
        shut down the socket; do not send any closing frame for
        request/response connections (only stream connections have an
        explicit EOS frame, §9 / [client guide §9])
```

Each accepted TCP connection is handled independently and concurrently;
nothing about the wire protocol requires (or forbids) sharing state between
connections other than what an application layers on top (e.g. shared
verb-handler table, shared resource limiter).

`server::connection::process()` (`rpc.cc:1238-1301`) is the reference
implementation of this whole loop: negotiate → optional scheduling-group
switch → `while (!eof && !error) { read request_frame; dispatch; }` → on
exit, shut down input, drain in-flight replies, close.

## 2. Accepting negotiation

The server **reads first**, then decides, then writes. This is the mirror
image of the client guide's §3 — same frame format, opposite initiative:

```
1. read_exactly(12)                       // magic[8] + len(4), negotiation_frame header
2. validate magic == "SSTARRPC" (8 bytes, not null-terminated); if not, drop the connection
3. read_exactly(len)                      // the feature records
4. parse records generically: while bytes remain, read u32 feature_number,
   u32 record_len, record_len bytes of data; ignore unrecognized feature
   numbers rather than erroring
5. for each feature you understand, decide whether/how to accept it
   (see the per-feature rules below and the table in [client guide §4])
6. build your own reply feature map (only the features you're accepting —
   omit anything you decline) and write it as a negotiation_frame back
```

(`receive_negotiation_frame`, `rpc.cc:373-417`; server-side decision logic
`server::connection::negotiate`, `rpc.cc:1093-1184`; framing helper
`send_negotiation_frame`, `rpc.cc:242-264` — used symmetrically by both
roles)

Per-feature server behavior (numbers from **[client guide §4]**):

| # | Feature | Server behavior on receipt |
|---|---|---|
| 0 `COMPRESS` | Client sends its supported-algorithm string. Server picks one it supports (§4 below), replies with **just that one** algorithm's name. If the server supports none of them, omit `COMPRESS` from the reply entirely — the connection proceeds uncompressed. |
| 1 `TIMEOUT` | Payload is empty; if you accept it, echo it back (empty payload). From here every request frame you read has the extra `timeout_ms` field — see §5. |
| 2 `CONNECTION_ID` | Client never sends this — it's server→client only. See §3: send it unconditionally whenever your server has streaming enabled at all, even if the client didn't ask. |
| 3 `STREAM_PARENT` | If present, this whole connection is a **stream** connection, not a request/response one (§9). The payload is the 8-byte LE `connection_id` of an *already-negotiated* connection on this same server that will act as the stream's parent. Never echo this feature back. If your server doesn't support streaming, treat this as a hard error and refuse the connection (send a negotiation reply with no features, or simply drop the connection — the reference server throws before responding, `rpc.cc:1118-1122`). |
| 4 `ISOLATION` | Opaque cookie, application-defined. If you use it (e.g. to pick a thread pool / priority class), echo the **same bytes back unchanged** in your reply, as the reference server does (`rpc.cc:1170`). The reference client ignores this echo entirely (`client::negotiate` has no `ISOLATION` case), so echoing or omitting it has no observable effect on real Seastar clients. |
| 5 `HANDLER_DURATION` | Payload is empty; if you accept it, echo it back (empty payload) and from here every response frame you send needs the extra `handler_duration` field — see §6. |

## 3. Assigning connection identity

`connection_id` is an opaque 64-bit value from the client's point of view;
only the server's own bookkeeping needs to interpret it. The reference
layout (`rpc_types.hh:66-89`) is:

```
bits 63..16 : a per-server sequential counter, incremented per accepted
              connection (server::_next_client_id, starts at 1)
bits 15..0  : "shard" id — Seastar's per-core partition number; a
              single-threaded/non-sharded server can just always use 0
              here, or any fixed constant, since nothing in the protocol
              requires this to mean anything to a non-Seastar peer.
              0xffff is reserved to mean "invalid id" — never assign it.
```

You only need to hand out real (non-invalid) `connection_id`s if you
support streaming (§9) — a request/response-only server can either skip
`CONNECTION_ID` entirely (never send it) or send a trivially incrementing
id per connection; nothing depends on its internal structure except that
it round-trips byte-for-byte through §9's `STREAM_PARENT` and sink/source
encodings.

Send `CONNECTION_ID` in your negotiation reply, as an 8-byte LE integer,
**whenever your server is configured to support streaming at all** — this
is unconditional, not contingent on the client having asked for it
(`rpc.cc:1178-1180`, `if (get_server()._options.streaming_domain) ret[CONNECTION_ID] = ...`).
A server that never supports streaming simply never sends this feature.

## 4. Choosing compression

If you want to support `COMPRESS`, implement the two algorithms from
**[client guide §5]**:

* `"LZ4"` — single-shot `LZ4_compress_default`/`LZ4_decompress_safe`, with a
  4-byte LE decompressed-length prefix before the LZ4 block. Simpler to
  implement; requires linearizing the whole message in memory.
* `"LZ4_FRAGMENTED"` — chunked streaming LZ4 (32 KiB decompressed chunks,
  4-byte chunk headers with a last-chunk flag bit). More code, but scales
  to large/streamed messages without full linearization.

You can implement either, both, or neither. If the client's `COMPRESS`
offer is a comma-separated list (the `multi_algo_compressor_factory`
convention, **[client guide §4]**), pick the first name in the client's
list that you support and reply with just that name. If you support none
of the offered names, omit `COMPRESS` from your reply — do not send an
empty or garbage compressor name.

Once you've accepted `COMPRESS`, every subsequent frame in **both**
directions is wrapped per **[client guide §5]** — including your own
responses and the unknown-verb replies from §7.

## 5. Request loop & verb dispatch

Header shape depends on whether `TIMEOUT` was negotiated in §2
(`request_frame` vs `request_frame_with_timeout`, `rpc.cc:624-680`):

```
// TIMEOUT not negotiated:
uint64   verb_id
int64    msg_id
uint32   payload_len
byte     payload[payload_len]

// TIMEOUT negotiated:
uint64   timeout_ms      // 0 = caller specified no timeout
uint64   verb_id
int64    msg_id
uint32   payload_len
byte     payload[payload_len]
```

Dispatch algorithm per request:

1. Read the header (20 or 28 bytes depending on §2's outcome — plus the
   compression envelope's own 4-byte length prefix if `COMPRESS` was
   negotiated, which wraps around the *whole* frame including this
   header).
2. Read `payload_len` bytes of payload (zero bytes read if the field is
   `0`).
3. Look up `verb_id` in your handler table. Not found → **[§7](#7-unknown-verb-handling)**.
4. If `timeout_ms != 0`, convert to an absolute deadline now
   (`relative_timeout_to_absolute`, `rpc_impl.hh:531-535` — the reference
   implementation clamps against overflow; if the deadline has already
   passed you may skip running the handler and skip replying, since the
   client may no longer be waiting — sending a reply anyway is not
   incorrect, just possibly wasted work, `doc/rpc.md:53-54`).
5. Decode `payload` into the handler's argument types per that verb's
   application-defined `Serializer` rules (**[client guide §7]** for the
   framework-fixed parts: verb id was raw, not serializer-decoded;
   trailing `rpc::optional<T>` is present iff bytes remain; `rpc::tuple`
   is flattened; `rpc::sink`/`rpc::source` arguments are just 8 raw LE
   bytes naming a `connection_id` you must already have registered via
   §9).
6. Run the handler. Time it if you're supporting `HANDLER_DURATION`.
7. Encode the result (or an exception, §8) and send a response frame (§6)
   — **unless** this verb is a "no-wait" / fire-and-forget verb in your
   application's IDL, in which case send nothing at all (see note below).
8. Continue the loop for the next request — do not wait for step 6/7 to
   finish before reading the next request header; multiple requests must
   be able to be in flight and replied to out of order (`doc/rpc.md:187`).
   The reference server runs each handler as an independent background
   task, but its read loop does `co_await` the per-request memory
   admission (`wait_for_resources`, §10) before reading the next frame
   (`rpc.cc:1271-1276`, `recv_helper`, `rpc_impl.hh:662-700`) — that is
   how backpressure reaches the socket.

If decoding the payload into the handler's argument types throws (i.e. the
client sent bytes that don't match the verb's `Serializer` encoding), the
reference server **logs the error and sends no response at all**
(`recv_helper` catch block, `rpc_impl.hh:694-697`). The caller only finds
out via its own timeout. A from-scratch server may choose to reply with a
`USER` exception instead — that is strictly more helpful and still
conformant.

**No-wait verbs**: some application protocols define verbs whose caller
does not wait for and does not expect a reply (Seastar's `rpc::no_wait_type`
/ `future<rpc::no_wait_type>` return type, `rpc_impl.hh:516-526`). Whether a
given `verb_id` is no-wait is entirely an **application-level** convention
— nothing in the frame format marks a request as "replyless". You must know
this per-verb from the application's IDL/spec, the same way you must know
its argument encoding. If you get this wrong in the "reply when you
shouldn't" direction it's usually harmless (the client either has no
pending waiter for that `msg_id` and silently drops the response, or — if
you also reuse `msg_id`s — could misdeliver; don't reuse `msg_id`s that
matter for correlation). Getting it wrong in the other direction (never
replying to a verb the client expects a reply for) will manifest as
client-side timeouts.

## 6. Building response frames

Header shape depends on whether `HANDLER_DURATION` was negotiated in §2
(`response_frame` vs `response_frame_with_handler_time`, `rpc.cc:727-797`):

```
// HANDLER_DURATION not negotiated:
int64    msg_id            // negative ⇒ exception for verb call abs(msg_id)
uint32   payload_len
byte     payload[payload_len]

// HANDLER_DURATION negotiated:
int64    msg_id
uint32   payload_len
uint32   handler_duration  // microseconds; 0xFFFFFFFF = not measured
byte     payload[payload_len]
```

* `payload_len` is the length of `payload` alone — it does **not** include
  the header fields (including `handler_duration` when present).
* `handler_duration`, when you support the feature, should be wall-clock
  time spent actually running the verb's handler, in microseconds. Use the
  `0xFFFFFFFF` sentinel if you didn't measure it for this particular call
  (e.g. a no-wait verb, or a verb that failed before you started timing) —
  never conflate "not measured" with "measured as zero".
* Successful result: `payload` = application-`Serializer`-encoded return
  value(s) per **[client guide §7]** (concatenated with no framing for
  multi-value/`rpc::tuple` returns; empty payload for `void`). `msg_id`
  stays positive (same value as the request's `msg_id`).
* Handler threw / failed: negate `msg_id` and encode an exception payload
  instead — see §8.
* If you don't intend to reply at all (no-wait verb, §5), don't send a
  response frame — there is no "empty ack" convention.

## 7. Unknown verb handling

If `verb_id` doesn't match any handler you have registered, you must
still send a properly formed exception response — silently dropping the
request is not conformant and will cause the caller to hang until its own
timeout:

```
msg_id_out = -msg_id_in
payload    = LE32(1)              // exception_type::UNKNOWN_VERB
             LE32(8)               // data_len
             LE64(verb_id)         // the verb id that had no handler
```

(reference: `server::connection::send_unknown_verb_reply`, `rpc.cc:1216-1236`,
building `exception_type::UNKNOWN_VERB = 1`, `rpc_impl.hh:40-43`)

Send this through the normal response-frame path (§6), including the
`handler_duration` field if negotiated (use the `0xFFFFFFFF` sentinel — you
didn't run a handler). This still respects the request's `timeout_ms` if
present: don't bother sending it if the deadline has already passed
(same rule as step 4 in §5).

## 8. Exception encoding on the server

Encode any handler failure as (`unmarshal_exception`'s inverse,
conceptually `rpc_impl.hh:604-621` for the `USER` case and
`rpc.cc:1216-1236` for `UNKNOWN_VERB`):

```
uint32   exception_type    // 0 = USER, 1 = UNKNOWN_VERB (§7), anything else
                            // is reserved / will be treated as opaque by
                            // real clients (unknown_exception_error)
uint32   data_len
byte     data[data_len]
```

For `USER` (the general "your handler threw"), `data` is the exception's
formatted message as a raw byte string (whatever your handler's error
message renders to — the reference server uses
`fmt::format("{}", exception)`, `rpc_impl.hh:610`, and treats it as opaque
bytes, not a length-prefixed structured value beyond the `data_len` above).
There is no stack trace, error code, or type tag beyond this free-text
message and the outer `exception_type` — if your application needs richer
structured errors, that has to be layered inside `data` by convention
between your specific client and server (i.e. it becomes part of your
application's `Serializer`-level contract, not the RPC envelope).

Always send `msg_id = -original_msg_id` for an exception response, using
the same response-frame header shape (§6) as a success reply.

## 9. Accepting streams

A stream is a second TCP connection whose negotiation offer includes
`STREAM_PARENT` (§2 table). As the server:

```
1. Accept the new TCP connection normally, read its negotiation frame.
2. Its feature set includes STREAM_PARENT = pid (an 8-byte LE
   connection_id you must have previously handed out via CONNECTION_ID,
   §3, on some other, already-negotiated connection).
3. Look up `pid` among your currently-open connections. Not found, or
   found but already errored/closed ⇒ refuse this stream connection
   (reference: throws before completing negotiation, `rpc.cc:1136-1141`).
4. Register this new connection as a stream keyed by its own freshly
   assigned connection_id, associated with the parent connection `pid`.
   Do NOT echo STREAM_PARENT back in your negotiation reply.
5. From here this connection only exchanges stream frames
   (LE32 len + payload, len==0xFFFFFFFF ⇒ EOS, [client guide §6]) —
   never request/response frames.
6. Later, when a request/response verb call on the *parent* connection
   `pid` carries an `rpc::sink<...>`/`rpc::source<...>` argument, its wire
   encoding is just the 8 raw LE bytes of this stream connection's own
   connection_id (embedded in that call's payload per
   [client guide §7]/§9). Look it up in your per-parent-connection stream
   table to bind it to the handler's actual sink/source object.
```

Closing: honor an incoming stream frame with `len == 0xFFFFFFFF` as "peer
closed their half"; you must still independently close your own half (send
your own `len == 0xFFFFFFFF` frame) when you're done writing — the two
directions close independently, and the underlying TCP connection should
only be torn down once both halves are closed (**[client guide §9]**,
`connection::stream_close`, `rpc.cc:548-573`).

If your server never needs streaming, the simplest conformant behavior is:
never send `CONNECTION_ID` (§3), and refuse (drop) any incoming connection
whose negotiation offer includes `STREAM_PARENT`, since you gave out no
valid parent ids for it to reference.

## 10. Resource limiting & backpressure (design, not wire)

Nothing here affects wire compatibility, but it's part of the reference
server's contract that a serious server implementation should replicate in
spirit:

* Before running a handler, the reference server estimates memory cost of
  the request as `basic_request_size + payload_len * bloat_factor` and
  waits on a semaphore bounded by a configured `max_memory` before
  proceeding (`resource_limits`, `rpc.hh:97-107`; `estimate_request_size`,
  `rpc.hh:166-170`; `wait_for_resources`, `rpc.hh:689-695`). A request
  whose *estimated* size alone already exceeds `max_memory` is rejected
  immediately with a `USER` exception rather than queued
  (`recv_helper`, `rpc_impl.hh:670-681`).
* This bounds how much unread/unprocessed request data you buffer
  in-flight per server, independent of TCP-level flow control — useful if
  you don't want one connection's oversized requests to exhaust server
  memory while others are waiting.
* None of this changes any byte on the wire; a client cannot detect
  whether you implement it, except indirectly via the `USER` exception
  message text for oversized requests, which is entirely up to you to
  word however you like.

## 11. Shutdown semantics

* Reference "shutdown": stop admitting new work — stop accepting new TCP
  connections, and stop admitting new requests/replies on already-open
  connections — while letting already-running handlers finish
  (`server::shutdown`, `rpc.cc:1391-1408`).
* Reference "stop": shutdown, plus wait for all in-flight reply sends to
  actually complete before returning (`server::stop`, `rpc.cc:1410-1415`,
  via a "reply gate" that in-flight `respond()` calls hold open,
  `rpc.hh:762-764`, `rpc_impl.hh:675-698`).
* Practically for a from-scratch server: on shutdown, stop `accept()`ing;
  for each open connection, stop reading new request frames but let
  handlers that already started finish and send their (already-earned)
  response before you close that connection's write side; then close.
* There is no wire-level "server is shutting down" notification frame —
  a client simply sees the TCP connection close (and, for any request
  still in flight at that moment with no reply sent, eventually times out
  client-side if it set a timeout, or waits forever if it didn't).

## 12. End-to-end data flow diagrams

### 12.1 Accept → negotiate → serve, uncompressed, TIMEOUT+HANDLER_DURATION accepted

```
Client                                                    Server
--------------------------------------------------------------------------------
TCP connect ─────────────────────────────────────────────►
send negotiation_frame(COMPRESS?, TIMEOUT="", HANDLER_DURATION="", ...)
                                          ◄────────────────  accept()
                                                              read negotiation_frame
                                                              validate magic
                                                              decide: accept TIMEOUT,
                                                                accept HANDLER_DURATION,
                                                                decline/accept COMPRESS
                         ◄──────────────────────────────────  send negotiation_frame(reply)
apply negotiated features
                                                              loop:
frame = LE64(timeout_ms) LE64(verb) LE64... (per §5) ────►    read header (28 bytes: TIMEOUT on)
                                                              read payload_len bytes
                                                              dispatch verb, run handler, time it
                         ◄──────────────────────────────────  resp = LE64(msg_id) LE32(len)
                                                                      LE32(duration_us) payload
                                                                      (16-byte header: HANDLER_DURATION on)
```

### 12.2 Unknown verb (server perspective)

```
Client                                                    Server
--------------------------------------------------------------------------------
frame(verb_id = 999, msg_id = 7, payload) ───────────────►
                                                              verb_id 999 not in handler table
                         ◄──────────────────────────────────  resp(msg_id = -7,
                                                                    payload = LE32(1) LE32(8) LE64(999))
```

### 12.3 Stream acceptance

```
Client                                                    Server
--------------------------------------------------------------------------------
[[ already has parent connection with id `pid`, learned via CONNECTION_ID ]]
TCP connect (2nd connection) ────────────────────────────►
send negotiation_frame(STREAM_PARENT = pid)
                                          ◄────────────────  accept(), read negotiation frame
                                                              look up `pid`: found, still open
                                                              register new connection_id `sid`
                                                              as a stream of parent `pid`
                         ◄──────────────────────────────────  send negotiation_frame(reply,
                                                                    no STREAM_PARENT echoed)
[[ this connection now only exchanges stream frames ]]

# later, on the PARENT connection:
request(verb = OPEN_CHANNEL, payload = ...{sink encoded as LE64(sid)}...) ►
                                                              decode sink arg ⇒ connection_id `sid`
                                                              look up `sid` in this-parent's stream table
                                                              bind to an rpc::source-like object
                         ◄──────────────────────────────────  resp(...)
```

## 13. Rust server implementation checklist

1. `TcpListener::accept()` loop; spawn one task per connection.
2. Per connection: read the negotiation frame first (you're the responder,
   not the initiator) — validate magic, parse feature records generically.
3. Decide which features you support among what the client offered; build
   and send your reply feature map. Remember, per connection: was
   `TIMEOUT` accepted? `HANDLER_DURATION`? Which compressor (if any)? Was
   this a `STREAM_PARENT` connection?
4. If you support streaming at all, always include `CONNECTION_ID` (an
   8-byte LE id you generate and can look up later) in your reply — even
   when the client didn't ask for it.
5. If `STREAM_PARENT` was present, this connection becomes stream-only
   (§9) — validate the referenced parent id exists, register the stream,
   and skip straight to a stream-frame read/write loop; never enter the
   request/response loop for it.
6. Otherwise, loop: read the request header (size depends on step 3's
   `TIMEOUT` outcome), read the payload, look up the verb, decode args per
   that verb's application-defined `Serializer`, run the handler
   concurrently with other in-flight requests on the same connection
   (don't serialize handler execution behind request reads), and reply
   (§6) unless it's a no-wait verb.
7. Always reply to unmatched verbs with the exact `UNKNOWN_VERB` exception
   bytes (§7) rather than dropping the request.
8. Wrap outgoing / unwrap incoming frames in the compression envelope if
   you negotiated `COMPRESS` — including unknown-verb and exception
   replies, which go through the same path as normal responses.
9. On shutdown: stop accepting, stop reading new requests, let in-flight
   handlers finish and send their replies, then close sockets. No special
   wire signal is needed or expected for this.
10. Only implement §9 (streams) if your target application actually uses
    `rpc::sink`/`rpc::source` — otherwise never send `CONNECTION_ID` and
    reject any connection offering `STREAM_PARENT`.

## 14. Gotchas found only in the source

* Don't gate reading the *next* request frame on finishing the *current*
  handler — the reference server dispatches each request onto its own
  concurrent task and continues its read loop as soon as the request has
  been admitted by the memory limiter (`rpc.cc:1271-1276`, §10). A server
  that processes requests strictly sequentially on one connection is
  protocol-legal but will look bizarrely slow to well-behaved pipelining clients and may cause them to
  hit timeouts waiting for replies that are stuck behind an unrelated slow
  call.
* `payload_len` in a response you send must be the length of the encoded
  return value **alone**; don't include `handler_duration`'s 4 bytes (when
  present) in that count — it has its own fixed slot in the header, after
  `payload_len`, before `payload`.
* The `handler_duration` "not measured" sentinel is `0xFFFFFFFF`, distinct
  from an actual `0`-microsecond measurement — always default unmeasured
  cases (no-wait verbs, requests you reject before touching the handler,
  the `UNKNOWN_VERB` path) to the sentinel, never to `0`. The reference
  server also sends the sentinel when the measured duration doesn't fit
  in a `uint32` of microseconds (≈71 minutes,
  `encode_handler_duration`, `rpc.cc:782-790`).
* Echo `ISOLATION`'s cookie bytes back unchanged if you accept it
  (`rpc.cc:1170`) to match the reference server; the reference client
  ignores the echo, so this is convention rather than a requirement.
* `CONNECTION_ID` must be sent **unconditionally** whenever your server
  supports streaming — not only when the client's offer included
  `STREAM_PARENT` (which it can't, on a brand new non-stream connection,
  precisely because the client doesn't have an id yet) and not only when
  the client explicitly asked for it (there's no client-side "please give
  me a connection id" feature bit — the server just always volunteers it).
* An empty compressed frame (**[client guide §5]**) can legitimately
  arrive between any two real frames, not just during negotiation setup —
  make sure your server's read loop for request frames tolerates and skips
  it rather than special-casing it out of the steady-state loop.
* Don't reuse a `connection_id` you've handed out, ever, for the lifetime
  of the server process (or at least not while any peer could still
  plausibly hold a reference to it via an outstanding sink/source or a
  cached `STREAM_PARENT` value) — the reference server uses a
  monotonically increasing counter that is never reset
  (`server::_next_client_id`, `rpc.hh:721`).
