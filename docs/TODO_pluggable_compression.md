# TODO: pluggable compressors and empty-frame sends

Status: deferred feature. The `COMPRESS` support shipped with a closed
`Compression` enum (`LZ4`, `LZ4_FRAGMENTED`) in `src/rpc/codec/compress.rs`.

## What is missing

Seastar lets an application bring its own compressor
(`rpc::compressor::factory`, wire guide §5). This crate can't do that yet:

1. **A `Compressor` trait.** Roughly `name()`, `compress(frame: &[u8], dst:
   &mut BytesMut)` and `decompress(data: &[u8], out: &mut BytesMut)`, with one
   instance per direction because the sender and receiver halves live in
   different tasks. A factory picks the implementation from the server's
   `COMPRESS` reply. `LZ4` and `LZ4_FRAGMENTED` become its built-in
   implementations, and the `Compression` enum remains as the built-in set.
2. **Sending empty frames.** Seastar's `factory::negotiate` gets a
   `send_empty_frame` callback. A stateful compressor (for example one that
   switches dictionaries) uses it to send its peer an envelope whose data
   decompresses to zero bytes, outside any RPC frame. We already skip such
   frames when reading. Sending them needs a hook from the compressor into the
   writer task (`src/rpc/client/dispatch.rs`) and the raw sender
   (`src/rpc/raw.rs`), for example a queue message that writes one empty
   envelope.

## Constraints

- Keep the hot path allocation-free once warm: the trait works on caller
  buffers, never returns owned ones. `tests/batch_allocs.rs` checks this.
- One dyn call per frame is fine; nothing per byte.
- The negotiation rule stays: a server's choice must be one we offered.
