//! Many calls at once over one connection: sequential vs pipelined loops,
//! `join`, spawned tasks sharing a `Client`, and cancellation.
//!
//! Needs the C++ reference server from `examples/seastar` (`make seastar-server`).
//!
//! ```text
//! cargo run --release --example concurrent [-- host:port]
//! ```

use std::time::{Duration, Instant};

use futures_util::future::try_join_all;
use seastar_rpc::rpc::{Client, Verb};

/// `int (int, int)`: returns `a + b` without printing on the server.
const ADD: Verb = 16;
/// `long (long, long)`: returns `a - b` after a 1 s delay.
const SUB_SLOW: Verb = 7;
/// `void ()`: sleeps 2 s.
const SLEEP: Verb = 8;

const CALLS: i32 = 10_000;
const TASKS: i32 = 4;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:10000".into());
    let client = Client::builder()
        .default_timeout(Duration::from_secs(5))
        .connect(&addr)
        .await?;

    // 1. One call at a time: every call waits a full round trip.
    let started = Instant::now();
    for i in 0..CALLS {
        let sum: i32 = client.call(ADD, (i, 1)).await?;
        assert_eq!(sum, i + 1);
    }
    report("sequential loop", CALLS, started);

    // 2. All calls in flight at once: the writer coalesces queued frames into
    //    few socket writes and the reader matches replies by message id.
    let started = Instant::now();
    let sums: Vec<i32> = try_join_all((0..CALLS).map(|i| client.call(ADD, (i, 1)))).await?;
    assert!(sums.iter().enumerate().all(|(i, &s)| s == i as i32 + 1));
    report("try_join_all", CALLS, started);

    // 3. Slow calls overlap: three 1 s handlers finish in about 1 s, not 3 s.
    let started = Instant::now();
    let (a, b, c) = tokio::try_join!(
        client.call::<_, i64>(SUB_SLOW, (10i64, 1i64)),
        client.call::<_, i64>(SUB_SLOW, (20i64, 2i64)),
        client.call::<_, i64>(SUB_SLOW, (30i64, 3i64)),
    )?;
    println!("3 slow calls -> ({a}, {b}, {c}) in {:?}", started.elapsed());

    // 4. Tasks share one connection: `Client` is a cheap handle, clone it.
    let started = Instant::now();
    let workers: Vec<_> = (0..TASKS)
        .map(|t| {
            let client = client.clone();
            tokio::spawn(async move {
                let mut total = 0i64;
                for i in 0..CALLS / TASKS {
                    total += client.call::<_, i32>(ADD, (t, i)).await? as i64;
                }
                Ok::<_, seastar_rpc::rpc::Error>(total)
            })
        })
        .collect();
    let mut total = 0;
    for w in workers {
        total += w.await??;
    }
    report(
        &format!("{TASKS} spawned tasks (sum {total})"),
        CALLS,
        started,
    );

    // 5. Cancellation: dropping a call future sends no cancellation message;
    //    the server continues processing it, and its late reply is dropped.
    let started = Instant::now();
    tokio::select! {
        r = client.call::<_, ()>(SLEEP, ()) => println!("sleep finished first: {r:?}"),
        _ = tokio::time::sleep(Duration::from_millis(100)) => {
            println!("gave up on sleep after {:?}", started.elapsed());
        }
    }
    let sum: i32 = client.call(ADD, (1, 1)).await?;
    println!("connection still fine: add(1, 1) = {sum}");
    Ok(())
}

fn report(what: &str, calls: i32, started: Instant) {
    let elapsed = started.elapsed();
    println!(
        "{what}: {calls} calls in {elapsed:?} ({:.0} calls/s)",
        calls as f64 / elapsed.as_secs_f64()
    );
}
