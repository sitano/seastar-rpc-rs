//! Compression: negotiation, compressed calls and batches, empty frames.

use bytes::Bytes;
use futures_util::StreamExt;
use seastar_rpc::rpc::codec::negotiation::Features;
use seastar_rpc::rpc::codec::serialize::{from_payload, to_payload};
use seastar_rpc::rpc::{CallOptions, Client, Compression, Error};
use tokio::io::AsyncWriteExt;

use crate::server::{ADD, Server, accepting, add_server, serve};

const ALGOS: [Compression; 2] = [Compression::Lz4, Compression::Lz4Fragmented];

/// Verb whose reply is the request payload.
const ECHO: u64 = 15;

fn choosing(algo: Compression) -> Features {
    let mut f = accepting(true, true);
    f.compress = Some(Bytes::from_static(algo.name().as_bytes()));
    f
}

async fn echo_server(mut s: Server) {
    while let Some(Ok(req)) = s.requests.next().await {
        if req.verb == ECHO {
            s.reply(req.msg_id, req.payload).await;
        } else {
            let (a, b): (i32, i32) = from_payload(req.payload).unwrap();
            s.reply(req.msg_id, to_payload(&(a + b))).await;
        }
    }
}

#[tokio::test]
async fn offers_algorithms_in_order() {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let addr = serve(accepting(false, false), |s| async move {
        tx.send(s.offer.compress.clone()).unwrap();
        add_server(s).await
    })
    .await;
    let client = Client::builder()
        .offer_compression(ALGOS)
        .connect(addr)
        .await
        .unwrap();
    assert_eq!(
        rx.await.unwrap().as_deref(),
        Some(&b"LZ4,LZ4_FRAGMENTED"[..])
    );
    // The server didn't pick one: frames stay plain.
    assert_eq!(client.call::<_, i32>(ADD, (1, 2)).await.unwrap(), 3);
}

#[tokio::test]
async fn not_offered_by_default() {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let addr = serve(accepting(false, false), |s| async move {
        tx.send(s.offer.compress.clone()).unwrap();
    })
    .await;
    let _client = Client::builder().connect(addr).await.unwrap();
    assert_eq!(rx.await.unwrap(), None);
}

#[tokio::test]
async fn calls_and_batches() {
    for algo in ALGOS {
        let addr = serve(choosing(algo), echo_server).await;
        let client = Client::builder()
            .offer_compression([algo])
            .connect(addr)
            .await
            .unwrap();

        assert_eq!(client.call::<_, i32>(ADD, (3, 4)).await.unwrap(), 7);

        // Spans several `LZ4_FRAGMENTED` chunks in both directions.
        let big: Bytes = (0..200 * 1024u32).map(|i| (i % 97) as u8).collect();
        let echoed = client
            .call_raw(ECHO, big.clone(), CallOptions::default())
            .await
            .unwrap();
        assert_eq!(echoed, big, "{algo:?}");

        let sums: Vec<i32> = client
            .call_many(ADD, (0..10).map(|i| (i, i)))
            .await
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(sums, (0..10).map(|i| 2 * i).collect::<Vec<_>>());
    }
}

#[tokio::test]
async fn empty_frames_are_skipped() {
    let addr = serve(choosing(Compression::Lz4), |mut s| async move {
        let r = s.next().await;
        // An empty envelope before and after the reply.
        s.responses.get_mut().write_all(&[0; 4]).await.unwrap();
        s.reply(r.msg_id, to_payload(&5i32)).await;
        s.responses.get_mut().write_all(&[0; 4]).await.unwrap();
        echo_server(s).await
    })
    .await;
    let client = Client::builder()
        .offer_compression([Compression::Lz4])
        .connect(addr)
        .await
        .unwrap();
    assert_eq!(client.call::<_, i32>(ADD, (0, 0)).await.unwrap(), 5);
    assert_eq!(client.call::<_, i32>(ADD, (1, 1)).await.unwrap(), 2);
}

#[tokio::test]
async fn unoffered_choice_fails_connect() {
    // Offered nothing.
    let addr = serve(choosing(Compression::Lz4), |_| async {}).await;
    let err = Client::builder().connect(addr).await.unwrap_err();
    assert!(matches!(err, Error::Protocol(_)), "{err}");

    // Offered a different algorithm.
    let addr = serve(choosing(Compression::Lz4Fragmented), |_| async {}).await;
    let err = Client::builder()
        .offer_compression([Compression::Lz4])
        .connect(addr)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Protocol(_)), "{err}");

    // Named an algorithm this crate doesn't know.
    let mut reply = accepting(false, false);
    reply.compress = Some(Bytes::from_static(b"ZSTD"));
    let addr = serve(reply, |_| async {}).await;
    let err = Client::builder()
        .offer_compression(ALGOS)
        .connect(addr)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Protocol(_)), "{err}");
}
