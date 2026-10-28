# Abort open streams when their parent client closes

TL;DR: Seastar aborts every stream a client opened when that client's
connection closes. Our streams are independent connections and keep going
until the server tears them down. We should end them ourselves too, so
their readers see the end right away.

## Details

In Seastar, `client::make_stream_sink` registers each stream connection
with its parent (`register_stream`), and the parent's read loop ends with
`abort_all_streams()` (`rpc.cc:1069`). A stream left open after its parent
dies fails at once on both sides.

`Client::open_stream` returns a `StreamConnection` that holds nothing of the
parent. When the parent connection fails or every `Client` clone is
dropped:

- the server aborts the streams of that parent, so a `StreamSource` ends
  with `Error::Closed` and a `StreamSink` write fails once the server has
  closed the socket. This is correct, but it takes a network round trip;
- a peer that doesn't do this (a future Rust server, or a custom one) would
  leave the stream open.

A possible shape: the client's reader task holds a `CancellationToken` (or
a `watch`) that it fires on exit, and each stream half checks it in
`poll_next` / `poll_ready`. That costs one atomic load per poll and no
allocation per message.

It isn't needed for the reference server, so it was left out of the first
streaming version.
