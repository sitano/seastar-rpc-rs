//! Call benchmarks against the C++ reference server in `examples/seastar`.
//!
//! Opt-in: build and start the server (see `examples/seastar/HACKING.md`),
//! then run
//!
//! ```text
//! SEASTAR_RPC_ADDR=127.0.0.1:10000 cargo bench --bench call
//! ```
//!
//! Uses verb 16 (`add(int, int) -> int`, like verb 2 but the server prints
//! nothing): fixed-size arguments and reply, so no per-call allocation on
//! either side beyond the frames themselves, and no stdout on the server.

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures_util::future::join_all;
use seastar_rpc::rpc::Client;
use tokio::runtime::Runtime;

const ADD: u64 = 16;

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn connect(rt: &Runtime) -> Option<Client> {
    let addr = std::env::var("SEASTAR_RPC_ADDR").unwrap_or_else(|_| "127.0.0.1:10000".into());
    let res = rt.block_on(
        Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .default_timeout(Duration::from_secs(5))
            .connect(&addr),
    );
    match res {
        Ok(c) => Some(c),
        Err(e) => {
            eprintln!("skipping: can't connect to C++ rpc_server at {addr}: {e}");
            None
        }
    }
}

async fn add(c: &Client, a: i32, b: i32) {
    let r = c.call::<(i32, i32), i32>(ADD, (a, b)).await.unwrap();
    assert_eq!(r, a + b);
}

fn bench_add(cr: &mut Criterion) {
    let rt = runtime();
    let Some(client) = connect(&rt) else {
        return;
    };

    let mut g = cr.benchmark_group("add");

    // One call in flight: round-trip latency.
    g.throughput(Throughput::Elements(1));
    g.bench_function("latency", |b| b.to_async(&rt).iter(|| add(&client, 3, 4)));

    // `depth` concurrent calls multiplexed over one connection.
    for depth in [1usize, 16, 128] {
        g.throughput(Throughput::Elements(depth as u64));
        g.bench_with_input(BenchmarkId::new("pipelined", depth), &depth, |b, &depth| {
            b.to_async(&rt)
                .iter(|| join_all((0..depth as i32).map(|i| add(&client, i, 1))))
        });
    }

    g.finish();
}

criterion_group!(benches, bench_add);
criterion_main!(benches);
