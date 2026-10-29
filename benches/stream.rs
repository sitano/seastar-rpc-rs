//! Stream benchmarks against the C++ reference server in `examples/seastar`.
//!
//! Opt-in: build and start the server (see `examples/seastar/HACKING.md`),
//! then run
//!
//! ```text
//! SEASTAR_RPC_ADDR=127.0.0.1:10000 cargo bench --bench stream
//! ```
//!
//! Uses verb 19 (`rpc::sink<int> (rpc::source<int>)`, like verb 18 but the
//! server prints nothing): writes back every int doubled on the same stream
//! and closes after our EOS.

use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use seastar_rpc::rpc::{Client, StreamConnection, StreamId, Verb};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::runtime::Runtime;

const DOUBLE: Verb = 19;

type Stream = StreamConnection<OwnedReadHalf, OwnedWriteHalf>;

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

/// Open a stream and hand it to the doubling verb.
async fn open(c: &Client) -> Stream {
    let s = c.open_stream().await.unwrap();
    let back: StreamId = c.call(DOUBLE, s.id).await.unwrap();
    assert_eq!(back, s.id);
    s
}

/// Send `n` ints and read `n` doubled ints back, both directions at once:
/// the server stops reading once its replies fill a buffer nobody drains.
async fn round(s: &mut Stream, n: i32) {
    let Stream { sink, source, .. } = s;

    let send = async {
        for i in 0..n {
            sink.send_item(&i).await.unwrap();
        }
    };

    let receive = async {
        for i in 0..n {
            assert_eq!(source.recv_item::<i32>().await.unwrap(), Some(i * 2));
        }
    };

    tokio::join!(send, receive);
}

fn bench_stream(cr: &mut Criterion) {
    let rt = runtime();
    let Some(client) = connect(&rt) else {
        return;
    };

    let mut g = cr.benchmark_group("stream");

    // Whole stream life with no items: connect, negotiate, verb call, EOS
    // both ways.
    g.throughput(Throughput::Elements(1));
    g.bench_function("open_close", |b| {
        b.to_async(&rt).iter(|| async {
            let mut s = open(&client).await;
            s.sink.close().await.unwrap();
            assert_eq!(s.source.recv_item::<i32>().await.unwrap(), None);
        })
    });

    // `n` items over one long-lived stream; `n = 1` is the item round trip.
    for n in [1usize, 16, 1024] {
        let mut s = rt.block_on(open(&client));

        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::new("items", n), &n, |b, &n| {
            // `iter_custom` so the stream can be borrowed mutably across
            // iterations.
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let started = Instant::now();
                    for _ in 0..iters {
                        round(&mut s, n as i32).await;
                    }
                    started.elapsed()
                })
            })
        });

        rt.block_on(async {
            s.sink.close().await.unwrap();
            assert_eq!(s.source.recv_item::<i32>().await.unwrap(), None);
        });
    }

    g.finish();
}

criterion_group!(benches, bench_stream);
criterion_main!(benches);
