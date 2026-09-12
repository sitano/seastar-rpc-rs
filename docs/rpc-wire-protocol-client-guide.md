# Seastar RPC wire protocol — client implementation reference

This document is a byte-level reference for implementing an interoperable
client (e.g. in Rust) for `seastar::rpc`, without linking against Seastar.
It consolidates and cross-checks `doc/rpc.md`, `doc/rpc-compression.md`,
`doc/rpc-streaming.md` against the actual implementation in
`src/rpc/rpc.cc`, `include/seastar/rpc/rpc.hh`, `include/seastar/rpc/rpc_impl.hh`,
`include/seastar/rpc/rpc_types.hh`, `src/rpc/lz4_compressor.cc`, and
`src/rpc/lz4_fragmented_compressor.cc`. Every claim below is anchored to a
source location so it can be re-verified as the code evolves. All paths
(including `doc/*.md`) and line numbers refer to the Seastar repository at
commit `86228808`, not to this repository.

**Read this first**: Seastar RPC fixes the *envelope* (negotiation, frame
headers, compression wrapper, exception encoding, connection IDs) but does
**not** fix the encoding of RPC argument/return-value payloads. That is
defined by an application-supplied C++ `Serializer` type, agreed upon out of
band by whichever two Seastar programs speak the protocol. To be
wire-compatible with a specific application you must also know that
application's `Serializer` — this doc tells you exactly which parts of the
payload are serializer-defined and which are fixed regardless of serializer
(see [§7](#7-payload-marshalling-what-is-and-isnt-serializer-defined)).

## Table of contents

1. [Transport & byte order](#1-transport--byte-order)
2. [Connection lifecycle](#2-connection-lifecycle)
3. [Negotiation frame](#3-negotiation-frame)
4. [Feature catalog](#4-feature-catalog)
5. [Compression envelope](#5-compression-envelope)
6. [Request / response / stream frames](#6-request--response--stream-frames)
7. [Payload marshalling](#7-payload-marshalling-what-is-and-isnt-serializer-defined)
8. [Exception encoding](#8-exception-encoding)
9. [Streaming (sink/source)](#9-streaming-sinksource)
10. [End-to-end data flow diagrams](#10-end-to-end-data-flow-diagrams)
11. [Rust implementation checklist](#11-rust-implementation-checklist)
12. [Gotchas found only in the source](#12-gotchas-found-only-in-the-source)

---

## 1. Transport & byte order

* Transport is a plain TCP stream (`seastar::connected_socket`); there is no
  message framing at the TCP layer beyond what's described here.
* **All multi-byte integers are little-endian**, written with
  `write_le<T>`/read with `read_le<T>` (`rpc.cc:143`, `rpc.cc:250-258`, etc).
  There is no varint/LEB128 anywhere in the envelope.
* Whichever side initiates the TCP connection is the protocol **client**;
  the accepting side is the **server**. Role is purely about who connects —
  after negotiation, a "stream" connection also has this same client/server
  shape (see §9).

## 2. Connection lifecycle

```
Client                                          Server
------------------------------------------------------------------------
TCP connect()
send negotiation_frame  ───────────────────►
                                              recv negotiation_frame
                                              validate magic
                         ◄───────────────────  send negotiation_frame (features it accepts)
recv negotiation_frame
validate magic
apply negotiated features
[[ connection is "negotiated" — normal frame loop begins, fully pipelined ]]

send request_frame(verb, msg_id, args)  ───►
                                              dispatch by verb id
                         ◄───────────────────  send response_frame(msg_id, ret | exception)
   ...                                            ...
(repeat, out of order, any number of in-flight requests)

EOF / TCP close ends the loop; no explicit "goodbye" frame exists for
request/response connections (streams have an explicit EOS frame, §9).
```

Implementation notes:

* `client::loop()` (`rpc.cc:980`) does: connect → build its own feature map →
  `negotiate_protocol()` → mark connection "negotiated" → loop reading
  `response_frame`s until EOF or error.
* `server::connection::process()` (`rpc.cc:1238`) mirrors this: negotiate →
  loop reading `request_frame`s, dispatching to the registered handler for
  the verb, then asynchronously sending back a `response_frame`.
* Requests and responses are **fully asynchronous / pipelined**: a client
  may have many requests in flight, and responses may arrive in any order
  (`doc/rpc.md:187`). `msg_id` is how you demux replies to your pending call
  table; there is no requirement of in-order delivery.
* A request that hits an unknown verb gets an exception response bearing
  the *original* client `msg_id` negated (see §8); a response whose
  `msg_id` no longer has a pending waiter (e.g. it timed out) is just
  logged and dropped by the client, not a protocol error.
* Bad magic during negotiation → the receiving side logs and drops the TCP
  connection (`receive_negotiation_frame`, `rpc.cc:373-417`); no error frame
  is sent back.

## 3. Negotiation frame

Sent by **both sides**, always uncompressed, immediately after connect /
accept, exactly once each way:

```
char     magic[8]  = "SSTARRPC"     // NOT null terminated, no length prefix
uint32   len                        // length in bytes of the records that follow
byte     data[len]                  // len bytes of feature records, back to back
```

(`negotiation_frame`, `rpc.hh:172-175`; magic constant `rpc_magic`, `rpc.hh:69`)

Each record inside `data`:

```
uint32   feature_number             // one of the enum values in §4
uint32   len
byte     data[len]                  // feature-specific payload, may be zero-length
```

(`send_negotiation_frame`, `rpc.cc:242-264`; parsed in
`receive_negotiation_frame`, `rpc.cc:373-417`)

* Records may appear in **any order** — parse generically by feature number,
  don't assume ordering. (The reference implementation happens to emit them
  in ascending numeric order because it stores them in a `std::map`, but a
  conformant peer must not rely on that.)
* A feature the client asked for that the server doesn't support/accept is
  simply **absent** from the server's reply — there's no explicit "reject"
  record.
* The server may include feature records the client never asked for
  (`CONNECTION_ID`, unconditionally, if the server has a streaming domain
  configured — see §4).
* Unknown feature numbers in a received frame should be ignored (skip `len`
  bytes and continue) rather than treated as an error — this is how the
  protocol is meant to evolve.

## 4. Feature catalog

| # | Name | Direction of data | Payload | Effect once negotiated |
|---|------|---|---------|---|
| 0 | `COMPRESS` | client→server (offer), server→client (choice) | opaque string, app/compressor defined | All frames after negotiation are wrapped in the [compression envelope](#5-compression-envelope) using the algorithm named in the **server's** reply payload. |
| 1 | `TIMEOUT` | either direction, empty payload | `""` (zero bytes) | Every [request frame](#6-request--response--stream-frames) gains an extra leading `uint64` timeout-in-ms field. |
| 2 | `CONNECTION_ID` | server→client only | `uint64` connection id, LE | Client learns its connection id (needed to later open streams against this connection, or to check `bool(id)`). Sent by the server whenever the server has a streaming domain configured — **whether or not the client asked for it.** |
| 3 | `STREAM_PARENT` | client→server only | `uint64` connection id, LE | Marks this whole TCP connection as a **stream** connection (see §9), belonging to the parent RPC connection named by the id. This is never echoed back by the server. |
| 4 | `ISOLATION` | client→server (offer), server→client (ack) | opaque byte string ("isolation cookie") | Server picks a scheduling group for the connection based on the cookie. The server ack echoes the **same** cookie bytes back unchanged (`rpc.cc:1170`, `ret.emplace(e)`); the reference client ignores the ack. |
| 5 | `HANDLER_DURATION` | either direction, empty payload | `""` (zero bytes) | Every [response frame](#6-request--response--stream-frames) gains an extra `uint32` `handler_duration` (microseconds) field, `0xffffffff` sentinel = "not measured". |

Sources: enum `protocol_features` (`rpc.hh:177-184`); client offer assembly
(`rpc.cc:990-1006`); server negotiation logic (`rpc.cc:1093-1184`).

`connection_id` bit layout (relevant to `CONNECTION_ID` and `STREAM_PARENT`
payloads, and to sink/source encoding in §9):

```
bits 63..16 : sequential id assigned by the server (server::_next_client_id)
bits 15..0  : shard id (0xffff means "invalid id")
```

`make_id(id, shard) == (id << 16) | shard` (`rpc_types.hh:66-89`). The whole
64-bit value is written/read as a single LE `uint64` wherever a
`connection_id` appears on the wire — never as two separate fields.
`serialize_connection_id`/`deserialize_connection_id` (`rpc_impl.hh:178-190`)
confirm this: 8 raw LE bytes, nothing else.

### Compression algorithm sub-negotiation (built into `multi_algo_compressor_factory`)

This is not part of the core protocol — it's a convention used by the
optional helper `multi_algo_compressor_factory` (`multi_algo_compressor_factory.hh`,
`rpc.cc:1454-1482`) for the *contents* of the `COMPRESS` feature payload,
which apps are free to not use:

* Client's `COMPRESS` payload = comma-separated list of algorithm names it
  supports, in preference order, e.g. `"LZ4,LZ4_FRAGMENTED"`.
* Server picks the first name (scanning **client's preference order**) that
  it also supports, and replies with just that one algorithm's name as its
  `COMPRESS` payload, e.g. `"LZ4"`.
* Built-in algorithm names: `"LZ4"` (`lz4_compressor.cc:35`) and
  `"LZ4_FRAGMENTED"` (`lz4_fragmented_compressor.cc:41`).

## 5. Compression envelope

If `COMPRESS` was negotiated, **every** frame described in §6 (request,
response, stream) is wrapped like this instead of being sent raw:

```
uint32   compressed_len
byte     compressed_data[compressed_len]     // decompresses to exactly one raw frame
```

(`connection::compress`, `rpc.cc:139-147`; reader side
`connection::read_frame_compressed`, `rpc.cc:480-514`)

* After decompressing `compressed_data`, the result is byte-for-byte the
  same content that a request/response/stream frame would have had on an
  uncompressed connection — the compression wrapper sits **outside** the
  frame, it does not change the frame's own header format.
* **Special case**: an envelope whose `compressed_data` *decompresses* to
  zero bytes is a legal, empty frame that carries no payload and must be
  silently skipped by the reader, then the reader goes back to waiting for
  the next frame (`read_frame_compressed`, `rpc.cc:499-504`). The check is
  on the decompressor's output, not on `compressed_len`; `compressed_len == 0`
  qualifies only because the built-in `LZ4` decompressor returns an empty
  buffer for inputs shorter than 4 bytes (`lz4_compressor.cc:149-150`).
  This exists so a compressor can exchange metadata with its peer
  compressor out-of-band: `compressor::factory::negotiate` receives a
  `send_empty_frame` callback for exactly this purpose (`rpc_types.hh:318-321`).
  `LZ4` and `LZ4_FRAGMENTED` never call it, but a conformant reader must
  still handle it.
* If the server's reply names an algorithm the client's compressor factory
  cannot instantiate, the reference client throws and drops the connection
  (`client::negotiate`, `rpc.cc:693-700`) — never reply with a name the
  client didn't offer.
* Compression is **symmetric per direction** but the algorithm is chosen
  once, for the whole connection, during negotiation — there's no
  per-frame algorithm selector.

### `LZ4` compressor payload format

Used when the whole message fits comfortably in memory (it linearizes
fragmented buffers before compressing). Inside the compression envelope's
`compressed_data`:

```
uint32   decompressed_len          // LE, size of the original uncompressed frame
byte     lz4_block[...]            // LZ4 "default" block format (LZ4_compress_default),
                                    // consumes the rest of compressed_data
```

(`lz4_compressor.cc:130-171`) `decompressed_len == 0` is rejected as a
decompression failure (`lz4_compressor.cc:157`). This is a single,
non-streaming LZ4 block —
equivalent to what `LZ4_decompress_safe(src, dst, compressed_size, decompressed_len)`
expects, i.e. the same format produced by the reference `lz4` CLI's frame-less
block API (not the `.lz4` frame format with magic numbers).

### `LZ4_FRAGMENTED` compressor payload format

Used for large/streamed messages; uses LZ4's streaming API so it can
compress/decompress without linearizing. Inside the compression envelope's
`compressed_data` there are one or more **chunks**, each self-contained
except for LZ4 dictionary state carried across chunks:

```
uint32   chunk_header                // LE
byte     chunk_payload[...]
... (repeated) ...
```

Chunk header bit layout (`lz4_fragmented_compressor.cc:49-64`):

* Bit 31 (`0x8000_0000`, `last_chunk_flag`) **clear** → intermediate chunk.
  Remaining 31 bits = **compressed** size of `chunk_payload` (how many
  bytes to read off the wire for this chunk). Decompressed size is always
  exactly 32 KiB (`chunk_size = 32*1024`) for intermediate chunks.
* Bit 31 **set** → this is the **last** chunk. Remaining 31 bits =
  **decompressed** size of this chunk. The compressed size of this last
  chunk is implicitly "however many bytes remain in `compressed_data`"
  (there is no separate compressed-length field for the last chunk).
* Each chunk is compressed/decompressed with `LZ4_compress_fast_continue`/
  `LZ4_decompress_safe_continue` against a stream state that is reset
  (`LZ4_resetStream`/`LZ4_setStreamDecode(..., nullptr, 0)`) once per
  top-level frame, then carried across chunks within that frame — i.e.
  chunk *N* is only decodable after chunk *N-1* of the **same frame** has
  been decoded (standard LZ4 streaming dependency), but state does **not**
  persist across separate top-level (post-decompression-envelope) frames.
* A message that fits in a single chunk still gets exactly one 4-byte
  header (the "last chunk" one) — there's no way to have zero chunks for a
  non-empty message. An empty (`compressed_len == 0`) frame from §5's
  special case bypasses chunking entirely.

## 6. Request / response / stream frames

These are the three "inner" frame kinds. Each is optionally wrapped in the
compression envelope from §5, and the header layout it has depends on
which features were negotiated (§4). All fields LE.

### Request frame (client → server)

```
uint64   timeout_ms      // ONLY PRESENT if TIMEOUT was negotiated; 0 = no timeout
uint64   verb_id         // "message type" / MsgType, cast to uint64 verbatim (§7)
int64    msg_id          // > 0, must never be reused on this connection
uint32   payload_len
byte     payload[payload_len]
```

(`request_frame` / `request_frame_with_timeout`, `rpc.cc:624-680`;
`client::request`, `rpc.cc:682-685`)

If `TIMEOUT` was negotiated, `timeout_ms` is the number of milliseconds
remaining until the call's deadline **at the moment the frame is actually
sent** (recomputed from the caller's original relative/absolute deadline
right before writing to the socket, not when the call was made) — a
request that has already expired by send time is silently dropped by the
sender rather than transmitted (`connection::send_entry`, `rpc.cc:163-191`).
`0` unambiguously means "no timeout was requested".

### Response frame (server → client)

```
int64    msg_id            // negative ⇒ this is an exception for verb call abs(msg_id)
uint32   payload_len
uint32   handler_duration  // ONLY PRESENT if HANDLER_DURATION was negotiated, microseconds,
                            // 0xffffffff = not measured / disregard
byte     payload[payload_len]
```

(`response_frame` / `response_frame_with_handler_time`, `rpc.cc:727-797`;
`server::connection::respond`, `rpc.cc:1204-1214`)

If `msg_id < 0`, `payload` holds a [serialized exception](#8-exception-encoding)
instead of a normal return value, keyed to the original request's
`abs(msg_id)`.

### Stream frame (either direction, on a stream connection — §9)

```
uint32   len          // 0xFFFFFFFF ⇒ end of stream, and NO payload bytes follow
byte     payload[len] // present only if len != 0xFFFFFFFF
```

(`stream_frame`, `rpc.cc:516-541`)

Note this frame kind has **no msg_id and no verb id** — a stream carries an
ordered sequence of independent, self-delimited messages in one direction,
consumed strictly in order by `rpc::source`.

### Frame reading algorithm (applies uniformly to all three kinds)

1. Read the fixed-size header for the frame kind currently expected
   (size depends on negotiated features, see above).
2. If the header's `len`/`payload_len` field is `0`, there is no payload
   to read at all — proceed directly to step 4 with an empty payload.
3. Otherwise read exactly that many payload bytes.
4. Hand `(header fields, payload bytes)` to the frame-kind-specific
   consumer (dispatch by verb for requests, demux by `msg_id` for
   responses, feed into the stream queue for stream frames).

A header read that returns fewer bytes than expected (including zero, i.e.
clean EOF) means the peer closed the connection — the reader must stop the
loop, not treat it as a malformed frame (`connection::read_frame`,
`rpc.cc:453-478`).

## 7. Payload marshalling: what is and isn't serializer-defined

The bytes inside `payload[...]` above are produced by an app-supplied
`Serializer` type: for every argument/return type `T` it must provide

```cpp
template <typename Output> void write(const Serializer&, Output&, const T&);
template <typename Input>  T    read(const Serializer&, Input&, rpc::type<T>);
```

**Seastar does not standardize this encoding** — two Seastar apps built with
different `Serializer`s cannot talk to each other even though they speak
byte-identical envelopes. To write a Rust client for a specific application
you must reverse-engineer or obtain the spec of *that app's* `Serializer`
for every type it puts in an RPC signature. That said, the following rules
are baked into `rpc_impl.hh` and hold **regardless of which `Serializer` is
used**:

* **Verb ids bypass the serializer entirely.** `client::request()` receives
  `uint64_t(t)` — the `MsgType` value is `static_cast` straight to
  `uint64_t`, never passed through `Serializer::write` (`rpc_impl.hh:557`,
  `rpc.cc:682`). If your app's `MsgType` is a `uint32_t` enum, the wire
  `verb_id` is just that enum value zero-extended to 64 bits.
* **Multiple arguments/return values are concatenated with no framing
  between them** — `do_marshall` calls `write()` once per argument, in
  declaration order, into the same output buffer/stream, back to back
  (`rpc_impl.hh:211-259`). There is no per-argument length prefix unless
  the `Serializer`'s own encoding of that type happens to include one.
* **`rpc::optional<T>`** is encoded as: nothing at all if absent, or
  exactly `T`'s normal encoding if present. Presence is detected on the
  *decode* side purely by whether any bytes remain in the input stream
  (`unmarshal_one::helper<optional<T>>`, `rpc_impl.hh:300-307`) — which
  means `rpc::optional<T>` **only works as a trailing argument** (or
  followed only by other trailing optionals); it cannot be optional in the
  middle of an argument list.
* **`rpc::tuple<T...>`** (used for multi-value returns) is encoded by
  flattening: each element is marshalled in turn with no wrapper/length
  around the tuple as a whole (`marshall_one::helper<tuple<T...>>`,
  `rpc_impl.hh:241-253`; symmetric on decode, `rpc_impl.hh:329-333`) — the
  wire bytes are indistinguishable from having passed the same elements as
  separate trailing arguments.
* **`rpc::sink<T...>` and `rpc::source<T...>` arguments are never passed to
  the `Serializer` at all.** They are encoded as exactly the 8 raw LE bytes
  of a `connection_id` (§4), written directly into the output stream
  (`marshall_one::helper<sink<T...>>` / `helper<source<T...>>`,
  `rpc_impl.hh:227-240`; decode mirror, `rpc_impl.hh:319-328`). See §9.
* **`void` return** ⇒ zero-length payload (`reply<Serializer, void>` skips
  marshalling entirely, `rpc_impl.hh:599-601`).

## 8. Exception encoding

When a response's `msg_id` is negative, `payload` holds:

```
uint32   exception_type
uint32   data_len
byte     data[data_len]
```

(`unmarshal_exception`, `rpc_impl.hh:424-452`)

`exception_type` values (`enum class exception_type`, `rpc_impl.hh:40-43`):

| Value | Name | `data` |
|---|---|---|
| 0 | `USER` | The exception's formatted message, as raw bytes of length `data_len` (produced from `fmt::format("{}", ...)` server-side — treat as an opaque byte string, not guaranteed valid UTF-8 in general but will be in practice for ASCII/UTF-8 `what()` messages). Delivered to the caller as `rpc::remote_verb_error(message)`. |
| 1 | `UNKNOWN_VERB` | `uint64 verb_id` (LE) — the verb id from the offending request. Sent automatically by the server when a request names a verb with no registered handler (`server::connection::send_unknown_verb_reply`, `rpc.cc:1216-1236`); the response's negated `msg_id` matches the original request's `msg_id`. |
| other | — | Treat as `unknown_exception_error` — don't try to interpret `data`. |

A `USER` exception is generated whenever a registered handler throws;
the server catches it, formats it, and encodes it exactly this way
(`reply<Serializer, RetTypes>` catch block, `rpc_impl.hh:604-621`).

## 9. Streaming (sink/source)

A stream is a **second, independent TCP connection**, opened by the client,
that negotiates the `STREAM_PARENT` feature (§4) carrying the parent RPC
connection's id. Once negotiated as a stream, the connection stops
exchanging request/response frames entirely and only exchanges
[stream frames](#6-request--response--stream-frames) (`connection::process`/`loop`
branch to `handle_stream_frame()` when `is_stream()`, `rpc.cc:1247-1250`,
`rpc.cc:1012-1016`).

Sequence to establish a client→server stream (see `doc/rpc-streaming.md`
for the API-level view; this is the wire-level view):

```
1. Client already has a negotiated normal RPC connection C with id `pid`
   (learned via CONNECTION_ID, §4 — this requires the server to have a
   streaming domain configured).
2. Client opens a NEW TCP connection S to the same server.
3. Client's negotiation frame on S includes feature STREAM_PARENT = pid.
4. Server's negotiation reply on S does NOT echo STREAM_PARENT, but it
   does carry CONNECTION_ID = `sid` — S's own id, distinct from `pid`.
   From here, S carries only stream frames, one direction at a time
   or interleaved per the app's protocol; nothing about the frame format
   distinguishes "client→server" vs "server→client" data — direction is
   just "whoever writes to S".
5. The client then makes a normal RPC call on connection C whose
   arguments/return include a `rpc::sink<...>`/`rpc::source<...>` value.
   That value is marshalled (per §7) as just S's connection_id (`sid`,
   not `pid`) — 8 raw LE bytes — inside the request/response payload of
   connection C.
6. The far end looks up connection_id → its local handle for connection S
   (`connection::get_stream`, `rpc.cc:616-622`) and constructs an
   `rpc::source`/`rpc::sink` object bound to it.
```

Closing: a stream is closed by sending a stream frame with `len ==
0xFFFFFFFF` (no payload) in that direction — this is a real frame on the
wire, not a TCP-level shutdown (`sink_impl::close`, `rpc_impl.hh:900-903`
writes `-1U` as the length). Both directions must be explicitly closed by
the application; the underlying TCP connection for the stream is only torn
down once both directions have signaled closed (`connection::stream_close`,
`rpc.cc:548-573`).

## 10. End-to-end data flow diagrams

### 10.1 Normal request/response round trip, uncompressed, TIMEOUT+HANDLER_DURATION negotiated

```
Client                                                    Server
--------------------------------------------------------------------------------
build payload = Serializer-encode(args...)
frame = LE64(timeout_ms) LE64(verb_id) LE64(msg_id)
        LE32(len(payload)) payload
write(frame) ──────────────────────────────────────────►
                                          read 28-byte header
                                          read `len` payload bytes
                                          lookup handler for verb_id
                                          run handler ⇒ Ret or throw
                                          ret_payload = Serializer-encode(Ret)  (or exception, §8)
                                          resp = LE64(msg_id) LE32(len(ret_payload))
                                                 LE32(handler_duration_us) ret_payload
                         ◄──────────────────────────────  write(resp)
read 16-byte header
read `len` payload bytes
if msg_id > 0: resolve pending call `msg_id` with Serializer-decode(payload)
if msg_id < 0: resolve pending call `abs(msg_id)` with exception (§8)
```

### 10.2 Compressed variant

Identical to 10.1, except each `write(frame)`/`write(resp)` above is
replaced by:

```
compressed = LZ4-or-LZ4_FRAGMENTED-compress(frame_bytes)
write( LE32(len(compressed)) compressed )
```

and each read is: read `LE32 compressed_len`, read `compressed_len` bytes,
decompress to get `frame_bytes`, then proceed exactly as in 10.1 using
`frame_bytes` as if it had arrived uncompressed. A `compressed_len == 0`
read must be discarded and reading must continue for the *actual* next
frame (§5).

### 10.3 Unknown verb

```
Client                                                    Server
--------------------------------------------------------------------------------
frame(verb_id = 999, msg_id = 7, payload) ──────────────►
                                          no handler registered for 999
                         ◄──────────────────────────────  resp(msg_id = -7,
                                                              payload = LE32(1) LE32(8) LE64(999))
                                                              // exception_type=UNKNOWN_VERB, data_len=8, verb_id=999
caller of msg_id 7 gets rpc::unknown_verb_error{type = 999}
```

## 11. Rust implementation checklist

1. TCP connect, then immediately write your negotiation frame (magic +
   feature records for whatever you support: at minimum you probably want
   `TIMEOUT` and `HANDLER_DURATION` as empty-payload offers, and optionally
   `COMPRESS`).
2. Read the peer's negotiation frame; validate the 8-byte magic
   `b"SSTARRPC"`; parse feature records generically (loop while bytes
   remain, read `feature_number`/`len`, read `len` bytes, skip anything you
   don't recognize).
3. Record which of `TIMEOUT`/`HANDLER_DURATION`/`COMPRESS` the **server**
   echoed back — that determines your frame header shapes from here on
   (§6). If the server sent `CONNECTION_ID`, remember it (needed only if
   you plan to open streams).
4. If `COMPRESS` was accepted, wrap/unwrap every subsequent frame per §5,
   using whichever of LZ4 (§5, single block) / LZ4_FRAGMENTED (§5, chunked)
   was agreed. If you don't want to implement a compressor, simply don't
   offer `COMPRESS`.
5. For each outgoing call: pick a `msg_id` > 0, never reuse it on this
   connection (a simple monotonically increasing counter starting at 1
   matches the reference client, `client::_message_id`, `rpc.hh:502`).
   Encode `verb_id` as the raw 64-bit numeric value of your verb (§7 — not
   run through any app serializer). Encode the payload per the target
   app's `Serializer` (§7 rules for `optional`/`tuple`/`sink`/`source` still
   apply even though the leaf-type encoding is app-defined). Send the
   request frame (§6), including the timeout field iff you and the server
   negotiated `TIMEOUT`.
6. Maintain a `msg_id → pending call` map. On each incoming response frame,
   look up by `abs(msg_id)`; if `msg_id < 0` decode an exception (§8)
   instead of a normal reply; if the id isn't in your map, silently drop
   the response (it's a legitimate late/timed-out reply, not a protocol
   error).
7. Treat header-read-returns-0-bytes as clean shutdown, not an error.
8. Only implement streaming (§9) if the target app actually uses
   `rpc::sink`/`rpc::source` in its verb signatures.

## 12. Gotchas found only in the source

* The request frame's timeout field, when present, is **not** simply
  "the timeout the caller originally specified" — it's recomputed to the
  *remaining* milliseconds right before the bytes hit the socket
  (`connection::send_entry`, `rpc.cc:163-191`). A from-scratch client is
  free to just send the full deadline-relative-to-now value at send time;
  you don't need to replicate the internal queuing delay, just don't send
  a stale/precomputed value if your own call was queued for a while
  client-side.
* If `TIMEOUT` is offered by the client but the *server* doesn't negotiate
  it (rare, but the protocol allows asymmetric feature sets in principle —
  in practice both sides run the same negotiation code so this mostly
  matters for a from-scratch implementation talking to a real Seastar
  peer), do **not** send the 8-byte timeout prefix at all — its presence is
  controlled by whether the feature was actually accepted, not by whether
  you offered it.
* `handler_duration`'s sentinel is `0xFFFFFFFF`, not `0` — a real duration
  of exactly 0 microseconds is valid and distinct from "not measured". The
  server also sends the sentinel when the duration overflows a `uint32` of
  microseconds (≈71 minutes, `encode_handler_duration`, `rpc.cc:782-790`).
* A request whose payload fails to decode on the server (your encoding
  doesn't match the app's `Serializer`) gets **no response at all** — the
  reference server logs and drops it (`recv_helper`, `rpc_impl.hh:694-697`).
  Always set a timeout while developing a new client, or such calls hang
  forever.
* Response `payload_len` is computed server-side as "total buffer size
  minus the *full* header size for whichever response frame variant is in
  use" — i.e. it already accounts for the optional `handler_duration`
  field being present or not; don't double-subtract.
* `ISOLATION`'s server reply payload is byte-identical to what the client
  sent (the server doesn't transform the cookie). The reference client
  ignores the echo, but comparing it is a useful self-consistency check
  when testing your negotiation code.
* The compression envelope's empty-frame special case (§5) can appear at
  *any* point in the frame stream, not just right after negotiation —
  keep your reader loop general ("read one compressed sub-frame, if empty
  loop back for another, otherwise decode it as one request/response/stream
  frame") rather than special-casing it only during setup.
* `LZ4_FRAGMENTED`'s per-chunk LZ4 dictionary state is reset once per
  top-level (post-envelope) frame — don't try to carry LZ4 stream state
  across separate request/response frames, only across chunks *within* one
  frame's `compressed_data`.
