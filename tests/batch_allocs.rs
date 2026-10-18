//! A warmed-up batch allocates nothing per send: neither a reused `Batch`
//! nor the pooled `call_many`, small or larger than the arena's first 4 KiB
//! chunk, nor a send that starts with `push_raw`.
//! `Batch::shrink` frees an outlier's memory, and the pool drops an outlier
//! `call_many`'s arena.
//!
//! Its own test binary: dhat replaces the global allocator, and its testing
//! profiler admits one test at a time.

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use seastar_rpc::rpc::codec::frame::{FrameLayout, RequestCodec, ResponseCodec, ResponseFrame};
use seastar_rpc::rpc::codec::negotiation::{Features, read_negotiation, write_negotiation};
use seastar_rpc::rpc::codec::serialize::{from_payload, to_payload};
use seastar_rpc::rpc::{Batch, CallOptions, Client, Verb};
use tokio::net::TcpListener;
use tokio_util::codec::{FramedRead, FramedWrite};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const ADD: Verb = 2;
/// Items per send.
const N: i32 = 32;
const WARMUP: usize = 64;
const ROUNDS: usize = 1000;

/// Reply payload: `7i32`, static so the server allocates nothing either.
static SEVEN: [u8; 4] = 7i32.to_le_bytes();

/// Encoded per `push` in `raw_first` and `pooled_large`: big enough that a
/// send outgrows the arena's first 4 KiB chunk. The server doesn't decode it.
static BLOB: [u8; 256] = [0; 256];

async fn server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (mut read, mut write) = stream.into_split();
        read_negotiation(&mut read).await.unwrap();
        let features = Features::default();
        write_negotiation(&mut write, &features).await.unwrap();
        let layout = FrameLayout::new(&features, 4 << 20);
        let mut requests = FramedRead::new(read, RequestCodec::new(layout));
        let mut responses = FramedWrite::new(write, ResponseCodec::new(layout));
        while let Some(Ok(r)) = requests.next().await {
            let frame = ResponseFrame {
                msg_id: r.msg_id,
                handler_duration: None,
                payload: Bytes::from_static(&SEVEN),
            };
            responses.send(frame).await.unwrap();
        }
    });
    addr
}

async fn reused(batch: &mut Batch) {
    for i in 0..N {
        batch.push(ADD, (i, 1));
    }
    for r in batch.send().await.unwrap() {
        assert_eq!(from_payload::<i32>(r.unwrap()).unwrap(), 7);
    }
}

/// A send led by `push_raw`, whose payload doesn't go into the arena.
async fn raw_first(batch: &mut Batch) {
    batch.push_raw(ADD, Bytes::from_static(&SEVEN));
    for _ in 0..N {
        batch.push(ADD, &BLOB[..]);
    }
    for r in batch.send().await.unwrap() {
        assert_eq!(from_payload::<i32>(r.unwrap()).unwrap(), 7);
    }
}

async fn pooled(client: &Client) {
    let replies = client
        .call_many::<_, i32>(ADD, (0..N).map(|i| (i, 1)))
        .await
        .unwrap();
    for r in replies {
        assert_eq!(r.unwrap(), 7);
    }
}

/// A pooled send larger than the arena's first chunk.
async fn pooled_large(client: &Client) {
    let replies = client
        .call_many::<_, i32>(ADD, (0..N).map(|_| &BLOB[..]))
        .await
        .unwrap();
    for r in replies {
        assert_eq!(r.unwrap(), 7);
    }
}

fn blocks() -> u64 {
    dhat::HeapStats::get().total_blocks
}

#[tokio::test]
async fn warm_batches_do_not_allocate_per_send() {
    let _profiler = dhat::Profiler::builder().testing().build();
    let client = Client::builder().connect(server().await).await.unwrap();

    let mut batch = client.batch();
    for _ in 0..WARMUP {
        reused(&mut batch).await;
    }
    let before = blocks();
    for _ in 0..ROUNDS {
        reused(&mut batch).await;
    }
    let per_send = (blocks() - before) as f64 / ROUNDS as f64;
    assert!(
        per_send < 1.0,
        "reused Batch: {per_send} allocations per send of {N}"
    );
    drop(batch);

    let mut batch = client.batch();
    for _ in 0..WARMUP {
        raw_first(&mut batch).await;
    }
    let before = blocks();
    for _ in 0..ROUNDS {
        raw_first(&mut batch).await;
    }
    let per_send = (blocks() - before) as f64 / ROUNDS as f64;
    assert!(
        per_send < 1.0,
        "push_raw first: {per_send} allocations per send of {}",
        N + 1
    );
    drop(batch);

    for _ in 0..WARMUP {
        pooled(&client).await;
    }
    let before = blocks();
    for _ in 0..ROUNDS {
        pooled(&client).await;
    }
    let per_send = (blocks() - before) as f64 / ROUNDS as f64;
    assert!(
        per_send < 1.0,
        "call_many: {per_send} allocations per send of {N}"
    );

    for _ in 0..WARMUP {
        pooled_large(&client).await;
    }
    let before = blocks();
    for _ in 0..ROUNDS {
        pooled_large(&client).await;
    }
    let per_send = (blocks() - before) as f64 / ROUNDS as f64;
    assert!(
        per_send < 1.0,
        "large call_many: {per_send} allocations per send of {N}"
    );

    // One outlier send grows the batch; `shrink` gives the memory back.
    let mut batch = client.batch();
    for i in 0..10_000 {
        batch.push(ADD, (i, 1));
    }

    batch.send().await.unwrap().for_each(drop);

    let grown = dhat::HeapStats::get().curr_bytes;
    batch.shrink();
    let freed = grown - dhat::HeapStats::get().curr_bytes;

    assert!(freed > 100_000, "shrink freed {freed} bytes");

    // An outlier `call_many` leaves no large arena in the pool. 2 MiB is
    // twice the pool's arena limit. A first call that uses no arena grows
    // the connection's read and write buffers, so they don't count below.
    // It sends the same encoded payload, length prefix included: a frame a
    // few bytes larger than the warm-up one would grow the write buffer.
    let big = vec![0u8; 2 << 20];
    client
        .call_raw(ADD, to_payload(&big[..]), CallOptions::default())
        .await
        .unwrap();

    let before = dhat::HeapStats::get().curr_bytes;
    client
        .call_many::<_, i32>(ADD, [&big[..]])
        .await
        .unwrap()
        .for_each(drop);
    let kept = dhat::HeapStats::get().curr_bytes.saturating_sub(before);

    assert!(kept < 1 << 20, "pooled call_many state kept {kept} bytes");
}
