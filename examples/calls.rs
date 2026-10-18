//! Single calls: `call`, `call_with`, the reply types and the error kinds.
//!
//! Needs the C++ reference server from `examples/seastar` (`make seastar-server`).
//!
//! ```text
//! cargo run --example calls [-- [--compress lz4|lz4_fragmented] host:port]
//! ```

use std::time::{Duration, Instant};

use seastar_rpc::rpc::{CallOptions, Client, Error, NO_WAIT, Verb, with_timeout};

mod common;

/// `int (int, int)`: returns `a + b`.
const ADD: Verb = 2;
/// `double (double)`: returns `sin(x)`.
const SIN: Verb = 3;
/// `void ()`: always throws.
const THROW: Verb = 4;
/// `no_wait_type ()`: fire and forget, the server sends no reply.
const NO_WAIT_VERB: Verb = 5;
/// `long (long, long)`: returns `a - b` after a 1 s delay.
const SUB_SLOW: Verb = 7;
/// `long (long, long, optional<int>)`: returns 3 if the optional is present, else 2.
const OPTIONAL: Verb = 9;
/// `tuple<long, int> ()`: returns `(1, 2)`.
const PAIR: Verb = 10;
/// `sstring (sstring)`: echoes the string.
const ECHO: Verb = 15;
/// Not registered on the server.
const MISSING: Verb = 99;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (args, compression) = common::args()?;
    let addr = args
        .into_iter()
        .next()
        .unwrap_or_else(|| "127.0.0.1:10000".into());
    let client = Client::builder()
        .offer_compression(compression)
        .connect_timeout(Duration::from_secs(3))
        // Used by every call that doesn't set its own timeout.
        .default_timeout(Duration::from_secs(5))
        .connect(&addr)
        .await?;
    println!(
        "connected to {addr}, connection id {:?}",
        client.connection_id()
    );

    // `call` encodes the request, waits for the reply and decodes it. The
    // request and reply types pick the payload encoding.
    let sum: i32 = client.call(ADD, (3, 4)).await?;
    println!("add(3, 4) = {sum}");

    let y: f64 = client.call(SIN, std::f64::consts::FRAC_PI_2).await?;
    println!("sin(pi/2) = {y}");

    let echoed: String = client.call(ECHO, "hello").await?;
    println!("echo(\"hello\") = {echoed:?}");

    // No arguments is `()`; a tuple reply decodes into a tuple.
    let pair: (i64, i32) = client.call(PAIR, ()).await?;
    println!("pair() = {pair:?}");

    // A trailing `Option` is sent only when `Some`, like `rpc::optional`.
    let without: i64 = client.call(OPTIONAL, (1i64, 2i64, None::<i32>)).await?;
    let with: i64 = client.call(OPTIONAL, (1i64, 2i64, Some(7i32))).await?;
    println!("optional: without = {without}, with = {with}");

    // `call_with` takes per-call options. A timeout shorter than the handler:
    // the call returns `Timeout`, and the server is told the deadline too.
    let started = Instant::now();
    match client
        .call_with::<_, i64>(
            SUB_SLOW,
            (10i64, 3i64),
            with_timeout(Duration::from_millis(200)),
        )
        .await
    {
        Err(Error::Timeout) => println!("sub_slow: timed out after {:?}", started.elapsed()),
        other => println!("sub_slow: unexpected {other:?}"),
    }

    // The same verb with enough time.
    let diff: i64 = client
        .call_with(
            SUB_SLOW,
            (10i64, 3i64),
            CallOptions::default().timeout(Duration::from_secs(3)),
        )
        .await?;
    println!("sub_slow(10, 3) = {diff} after {:?}", started.elapsed());

    // Fire and forget: returns once the request is queued; no reply is read.
    client.call_with::<_, ()>(NO_WAIT_VERB, (), NO_WAIT).await?;
    println!("no_wait: queued");

    // Errors from the server are values, and the connection stays usable.
    match client.call::<_, ()>(THROW, ()).await {
        Err(Error::Remote(msg)) => println!("throw: remote exception {:?}", msg.to_string_lossy()),
        other => println!("throw: unexpected {other:?}"),
    }
    match client.call::<_, ()>(MISSING, ()).await {
        Err(Error::UnknownVerb(v)) => println!("verb {v}: unknown on the server"),
        other => println!("missing verb: unexpected {other:?}"),
    }
    // Decoding the reply as the wrong type is a local error.
    match client.call::<_, String>(ADD, (1, 2)).await {
        Err(Error::Decode(e)) => println!("add as String: decode error: {e}"),
        other => println!("add as String: unexpected {other:?}"),
    }

    let sum: i32 = client.call(ADD, (40, 2)).await?;
    println!("still connected: add(40, 2) = {sum}");
    Ok(())
}
