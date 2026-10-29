# Benchmark results

Rough numbers for one CPU core on the server. Each run uses one connection,
which Seastar serves on one shard, so one server core; a stream adds its own
connection, which may land on a second shard. The client is a single-threaded
tokio runtime. Both sides ran on the same host over loopback, so take these
as an order of magnitude, not as per-machine throughput.

Setup, 2026-10-29: release C++ server
(`examples/seastar/build/release/rpc_server --compress 1`), release client,
4-byte payloads.

## Calls

`SEASTAR_RPC_ADDR=127.0.0.1:10000 cargo bench --bench call`

| Mode | Time per iteration | Calls/s |
|------|--------------------|---------|
| One call at a time | 5.3 µs | 189k |
| Pipelined, 16 in flight | 13.7 µs | 1.17M |
| Pipelined, 128 in flight | 75.2 µs | 1.70M |
| Batch of 16 | 10.7 µs | 1.50M |
| Batch of 128 | 46.8 µs | 2.73M |

One call at a time is bound by the round trip: the client waits idle for
each reply.

## Streams

`cargo run --release --example streaming -- 127.0.0.1:10000 <count>`: ints
sent to verb 18 and read back doubled, both directions at once.

| Run | Items/s |
|-----|---------|
| Debug server, 10k ints (for reference) | 18.7k |
| 10k / 100k / 1M ints | 0.8M / 1.0M / 1.03M |
| 100k ints, `--compress lz4` or `lz4_fragmented` | 1.01M |
| 1M ints, flush every 4 items | 2.43M |
| 1M ints, flush every 16 / 64 / 256 items, or only at close | 2.51M / 2.53M / 2.56M / 2.56M |

The flush-every-N rows come from a throwaway client that sends through
`Sink<Bytes>` with `feed` and a `send` every N items; `send_item` flushes
after every item. With one flush per item the client spends 0.64 s of a 1.0 s
run in the kernel, one `write` per int. With 16 or more items per flush that
drops to 0.01–0.06 s, the client is idle about 80% of the time, and the
server or the round trip sets the rate.
