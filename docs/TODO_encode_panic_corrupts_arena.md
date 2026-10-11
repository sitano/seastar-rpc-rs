# TODO: a panicking `Encode` corrupts the next payload

Status: left undecided by the client batch review ("declined to judge": the
spec doesn't define unwind safety). Needs a decision.

## What happens

`Batch::push` (`src/rpc/client/batch.rs`) encodes straight into the shared
arena and then splits the payload off:

```rust
request.encode(&mut state.arena);
let payload = state.arena.split().freeze();
```

If `encode` panics part-way, the bytes it already wrote stay in the arena,
after the last split point. If the caller catches the panic and keeps using
the batch, the next `push` appends to those bytes, and `split()` takes
**both**: the next request goes out with a garbage prefix.

`len`/`encoded` aren't updated (the panic happens before), so the batch looks
consistent.

## Example

```rust
struct Bad;
impl Encode for Bad {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(0xdead_beef);
        panic!("oops");
    }
}

let mut batch = client.batch();
let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
    batch.push(ADD, Bad);
}));
batch.push(ADD, (1i32, 2i32));  // payload = de ad be ef + (1, 2): wrong request
batch.send(CallOptions::default()).await?;
```

`Batch` isn't `UnwindSafe`, so the caller had to write `AssertUnwindSafe`,
which is why this was left out of scope. Without `catch_unwind`, the panic
unwinds the task and the batch is dropped, which is fine.

## Proposed fix

One line, so it's worth doing anyway: drop leftover bytes before encoding.

```rust
state.arena.clear();   // only clears what follows the last split
request.encode(&mut state.arena);
```

`BytesMut::clear` after `split()` only touches the unsplit tail, so it doesn't
affect earlier payloads and costs nothing.

Test: the example above, asserting that the server receives exactly
`(1, 2)` for the second push.
