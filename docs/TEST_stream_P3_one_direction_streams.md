# Test streams that carry data in one direction only

TL;DR: every stream test so far has data flowing both ways. We haven't
checked a verb that takes only a sink (`void (rpc::source<int>)`), or one
that returns only a source. Seastar keeps a stream's TCP connection open
until both directions are closed, so a stream with one unused direction
may hang, leak a connection, or lose the EOS on the server.

## Why it may break

Seastar tears down a stream connection only after both its sink and its
source are closed (`connection::stream_close`, `rpc.cc:548`).

- **Client → server only.** We send items, close the sink with EOS, and
  then the client has nothing to read, because the server never makes a
  sink. If we drop the connection right after EOS, the server's read loop
  sees EOF and may call `_stream_queue.abort()` before its handler has read
  the queued EOS (`rpc.cc:1062`). The handler then gets `stream_closed`
  instead of a clean end. If we keep the connection open instead, nothing
  ever closes it until the parent client closes.
- **Server → client only.** The server writes and closes its sink. Our
  sink is never used. Should we still send EOS so the server can close its
  source? Seastar's own client would have to call `close()` on its sink
  for that.

## What to add

1. Verbs in `examples/seastar/rpc_server.cc` (and the verb table in
   `examples/seastar/HACKING.md`), for example:
   - 19 `int (rpc::source<int>)`: sums what it reads until EOS, and logs
     the sum or the error;
   - 20 `rpc::sink<int> (int n, rpc::source<int>)`: writes `0..n` and
     closes, without reading the source.
   Verb 19's reply comes back before the stream ends, so the server must
   report the result in another way: its log, or a third verb that returns
   the last sum.
2. Interop tests in `tests/interop.rs` for both cases. Check:
   - the server saw a clean EOS (not `stream_closed`);
   - the stream connection goes away without the parent being closed;
   - nothing hangs.
3. The same scenarios in `tests/fake_server/streaming.rs`, if the fake
   server can model the relevant behaviour.

## Possible outcome

If dropping right after EOS loses the EOS, `StreamSink::close` may need to
wait for the peer to close the connection when the source is unused, or
the docs must tell callers to drain the source to EOF in every case. Write
down the rule we settle on in `src/rpc/stream.rs` module docs.
