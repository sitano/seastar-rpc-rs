# Streams flush after every item

TL;DR: the slow streams (12–19k items/s) came from the debug Seastar build.
With a release server, `examples/streaming.rs` runs at about 1M items/s, and
from there the per-item flush in `StreamSink::send_item` is the limit:
feeding items and flushing them together reaches about 2.5M items/s, the
same as batched calls (rough numbers for one server core). What's left is
a non-flushing typed send.

## Measurements

Full tables are in [`benchmarks.md`](benchmarks.md). They are rough numbers
for one CPU core on the server: one stream connection on one Seastar shard,
a single-threaded client, release builds, loopback, 4-byte items.

With one flush per item the client spends 0.64 s of a 1.0 s run in the
kernel (one `write` per int). With 16 or more items per flush, sys time
drops to about 0.01–0.06 s and the client is idle about 80% of the time,
so the server or the round trip sets the remaining rate.

## What we ruled out

- **Seastar's per-stream memory limit.** It doesn't show up at 2.5M items/s
  with 4-byte items. We didn't test larger items: the reference server only
  has the int stream (verb 18).
- **Compression.** LZ4 makes no measurable difference at this item size.

## What to do

- Add `StreamSink::feed_item` (encode into the write buffer without
  flushing, like `SinkExt::feed`). Callers then `flush` when they want to.
  `FramedWrite` already writes once its buffer passes its backpressure
  boundary, so a long run of `feed_item` calls doesn't grow memory.
- Make `examples/streaming.rs` use it and flush every so many items, or
  only at `close`.
- Check that a warm `feed_item` allocates nothing, as `send_item` doesn't.
- Optionally, a criterion bench for streams next to `benches/call.rs`.
