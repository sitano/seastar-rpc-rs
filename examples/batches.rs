//! Batches: a reused `Batch` with mixed verbs, per-item errors, `call_many`,
//! `call_many_with` and a deadline, and batches from several tasks.
//!
//! A batch's frames are written back to back with no other call in between
//! and flushed together. Replies come back in push order.
//!
//! Needs the C++ reference server from `examples/seastar` (`make seastar-server`).
//!
//! ```text
//! cargo run --release --example batches [-- host:port]
//! ```

use std::time::{Duration, Instant};

use seastar_rpc::rpc::codec::serialize::from_payload;
use seastar_rpc::rpc::{CallOptions, Client, Error, NO_WAIT, Verb, with_timeout};

/// `int (int, int)`: returns `a + b` without printing on the server.
const ADD: Verb = 16;
/// `double (double)`: returns `sin(x)`.
const SIN: Verb = 3;
/// `void ()`: always throws.
const THROW: Verb = 4;
/// `no_wait_type ()`: fire and forget.
const NO_WAIT_VERB: Verb = 5;
/// `void (int sleep_ms, sstring)`: sleeps `sleep_ms`.
const SLEEP_MS: Verb = 12;
/// `sstring (sstring)`: echoes the string.
const ECHO: Verb = 15;
/// Not registered on the server.
const MISSING: Verb = 99;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:10000".into());
    let client = Client::builder()
        .default_timeout(Duration::from_secs(5))
        .connect(&addr)
        .await?;

    // 1. Keep one batch and reuse it in a loop: its buffers survive across
    //    sends, so once warm a send allocates nothing. Verbs can be mixed;
    //    each reply is raw bytes, decoded by position.
    let mut batch = client.batch();
    for round in 0..3 {
        batch.push(ADD, (round, 100));
        batch.push(SIN, round as f64);
        batch.push(ECHO, format!("round {round}"));
        let mut replies = batch.send(CallOptions::default()).await?;
        let sum: i32 = from_payload(replies.next().unwrap()?)?;
        let sin: f64 = from_payload(replies.next().unwrap()?)?;
        let echoed: String = from_payload(replies.next().unwrap()?)?;
        println!("round {round}: add = {sum}, sin = {sin:.3}, echo = {echoed:?}");
    }

    // 2. One item failing doesn't fail the batch: each item has its own result.
    batch.push(ADD, (1, 2));
    batch.push(THROW, ());
    batch.push(MISSING, ());
    batch.push(ADD, (3, 4));
    for (i, reply) in batch.send(CallOptions::default()).await?.enumerate() {
        match reply {
            Ok(payload) if payload.is_empty() => println!("item {i}: ok, no value"),
            Ok(payload) => println!("item {i}: ok, {}", from_payload::<i32>(payload)?),
            Err(Error::Remote(msg)) => {
                println!("item {i}: remote exception {:?}", msg.to_string_lossy())
            }
            Err(e) => println!("item {i}: {e}"),
        }
    }

    // 3. `call_many`: one verb, many requests, typed replies in request order.
    let started = Instant::now();
    let mut total = 0i64;
    for chunk in 0..100 {
        let replies = client
            .call_many::<_, i32>(ADD, (0..100).map(|i| (chunk, i)))
            .await?;
        for sum in replies {
            total += sum? as i64;
        }
    }
    println!(
        "call_many: 100 x 100 adds, sum {total}, in {:?}",
        started.elapsed()
    );

    // 4. One deadline for the whole batch: items done by then keep their
    //    result, the rest are `Timeout`.
    let sleeps = [0, 50, 100, 1000, 2000];
    let replies = client
        .call_many_with::<_, ()>(
            SLEEP_MS,
            sleeps.iter().map(|&ms| (ms, "zz")),
            with_timeout(Duration::from_millis(500)),
        )
        .await?;
    for (ms, reply) in sleeps.iter().zip(replies) {
        println!(
            "sleep {ms:>4} ms: {}",
            if reply.is_ok() { "done" } else { "timed out" }
        );
    }

    // 5. Fire-and-forget batch: returns once queued.
    client
        .call_many_with::<_, ()>(NO_WAIT_VERB, std::iter::repeat_n((), 10), NO_WAIT)
        .await?;
    println!("no_wait: 10 requests queued");

    // 6. Several tasks, each with its own reused batch, sharing the
    //    connection. Each batch stays contiguous on the wire.
    let started = Instant::now();
    let tasks: Vec<_> = (0..4)
        .map(|t| {
            let client = client.clone();
            tokio::spawn(async move {
                let mut batch = client.batch();
                let mut total = 0i64;
                for _ in 0..250 {
                    for i in 0..32 {
                        batch.push(ADD, (t, i));
                    }
                    for reply in batch.send(CallOptions::default()).await? {
                        total += from_payload::<i32>(reply?)? as i64;
                    }
                }
                Ok::<_, Error>(total)
            })
        })
        .collect();
    let mut total = 0;
    for t in tasks {
        total += t.await??;
    }
    println!(
        "4 tasks x 250 batches x 32 adds, sum {total}, in {:?}",
        started.elapsed()
    );
    Ok(())
}
