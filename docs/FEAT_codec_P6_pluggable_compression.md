# Pluggable compressors and empty-frame sends

TL;DR: let applications bring their own compressor through a
`CompressorFactory` trait, with `LZ4` and `LZ4_FRAGMENTED` as built-in
factories that cost nothing extra per frame, and add a way to send empty
compressed frames. Wanted once someone needs a stateful (for example
dictionary) compressor.

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

## Plan

Do not add a `Custom(...)` variant that carries data to the public
`Compression` enum. A plain unit `Custom` is fine (see step 5).
`Compression` derives `Copy, Eq, Hash`, and a variant holding `Box<dyn ...>`
or `Arc<dyn ...>` would force us to drop those derives, which is a breaking
change that `#[non_exhaustive]` does not cover. The enum also describes
*which* algorithm to use, while a compressor is mutable state and we need one
per direction. So the variant would end up holding a factory, which is just
the trait wrapped in the enum.

Instead:

1. **Public traits.** `CompressorFactory` has `name()` and a method that makes
   one `Compressor` per direction. `Compressor` has `compress` and
   `decompress`, and both work on caller buffers.
2. **Built-ins are factories.** `Compression` implements `CompressorFactory`.
   The enum stays as it is, the list of built-in algorithms.
3. **Builder.** The offer list becomes `Vec<Arc<dyn CompressorFactory>>`.
   `offer_compression([Compression::Lz4])` keeps compiling, and a new
   `offer_compressor(f)` adds custom ones. Negotiation matches the server's
   choice against the offered names, and the server must still pick one we
   offered.
4. **Codec state.** Inside `CompressionCodec`, add a private enum:
   `Builtin(Compression, CompressTable)` or `Custom(Box<dyn Compressor>)`.
   Built-ins never go through a `Box` or a dyn call.
5. **Accessors and `Compression::Custom`.** Add a unit variant
   `Compression::Custom`, with no data, so the enum keeps `Copy, Eq, Hash`.
   Adding it is not breaking because the enum is `#[non_exhaustive]`.
   - `compression() -> Option<Compression>` stays. It returns
     `Some(Compression::Custom)` when a custom compressor was negotiated.
   - `compression_name() -> Option<&str>` is new. It returns the real
     negotiated name, built-in or custom.
   - `Compression::Custom.name()` returns a placeholder (`"CUSTOM"`) that is
     never sent on the wire. Its docs point to `compression_name()`.
   - `from_name` never returns `Custom`.
   - `offer_compression([Compression::Custom])` is a configuration error,
     because there is no compressor behind it. The `CompressorFactory` impl
     for `Compression` is never reached with `Custom`.
6. **Empty frames.** This is a separate step (item 2 above) and does not
   change the shape of the traits.

### Cost for built-ins

- Per frame: the same single `match` that `encode`/`decode` do today, now on
  the private enum. On one connection that branch always goes the same way,
  so the CPU predicts it. There is no dyn call and no allocation, and the LZ4
  code and its buffers stay unchanged.
- Per connection: a few string compares to match the server's choice. A
  built-in gives back its `Compression` value with no allocation. A custom
  factory allocates its two compressors here, once.
- Per builder: one `Arc` per offered algorithm, at configuration time.

Avoid putting built-ins behind `Box<dyn Compressor>` as well. That would still
allocate nothing per frame, but it adds one indirect call per frame for no
gain.

## Constraints

- Keep the hot path allocation-free once warm: the trait works on caller
  buffers, never returns owned ones. `tests/batch_allocs.rs` checks this.
- One dyn call per frame is fine; nothing per byte.
- The negotiation rule stays: a server's choice must be one we offered.
