# Dropping a split StreamSink can lose EOS

TL;DR: dropping a `StreamSink` over tokio's `OwnedWriteHalf` shuts the
socket's write side down (FIN). Seastar's stream read loop takes that EOF
as the end of the connection and aborts `_stream_queue`, which throws away
an EOS the handler has not read yet. Today only the docs warn about it; the
API should make the safe ordering the default, and an interop test should
confirm the behaviour.

## Where it bites

`StreamConnection` is meant to be taken apart so the halves can run in
different tasks. A writer task that ends with `sink.close().await` and
returns drops the sink, while the reader is still in
`StreamSource::wait_closed`. The FIN follows the EOS on the wire, and
whether Seastar's handler reads the EOS first is a race.

`examples/streaming.rs` and the tests avoid it only because they keep the
sink borrowed or alive until the end.

## Options

- Keep the socket's write side open until the read half is dropped too:
  tokio's `OwnedWriteHalf::forget` does this, but it consumes the half, so
  `StreamSink` would need to own `Option<OwnedWriteHalf>` and call it in
  `Drop`, which only works for the concrete TCP type, not a generic `W`.
- Store the whole `TcpStream` behind a shared handle (`Arc`) used by both
  halves, so the socket closes only when both are gone. Changes the public
  type parameters of `StreamConnection`.
- Leave it documented (current state).

## Test to add

In `tests/interop.rs`: split the stream, send items, `close()` the sink and
drop it at once, then check the server saw every item and its EOS (for
example with the sum verb), repeated enough times to hit the race.
