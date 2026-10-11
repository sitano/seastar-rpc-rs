//! A warmed-up batch allocates nothing per send: neither a reused `Batch`
//! nor the pooled `call_many`.
//!
//! Its own test binary: dhat replaces the global allocator, and its testing
//! profiler admits one test at a time.

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use seastar_rpc::rpc::codec::frame::{FrameLayout, RequestCodec, ResponseCodec, ResponseFrame};
use seastar_rpc::rpc::codec::negotiation::{Features, read_negotiation, write_negotiation};
use seastar_rpc::rpc::codec::serialize::from_payload;
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
        let layout = FrameLayout::new(&features, 1 << 20);
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
    for r in batch.send(CallOptions::default()).await.unwrap() {
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
}
