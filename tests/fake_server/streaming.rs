//! Streams: opening, passing the id through a call, both directions, close
//! and abort, compression.

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use seastar_rpc::rpc::codec::serialize::{from_payload, to_payload};
use seastar_rpc::rpc::codec::stream::StreamFrame;
use seastar_rpc::rpc::{Client, Compression, Error, StreamId};

use crate::server::{STREAM_ID, Server, StreamPeer, accepting, add_server, serve, serve_streaming};

/// `rpc::sink<int> (rpc::source<int>)`: doubles every int it reads.
const DOUBLE: u64 = 18;

/// Answer the call with the stream's id, then double each int until EOS
/// and close our direction.
async fn double_server(mut s: Server, mut peer: StreamPeer) {
    let req = s.next().await;
    assert_eq!(req.verb, DOUBLE);
    assert_eq!(from_payload::<u64>(req.payload).unwrap(), STREAM_ID);
    s.reply(req.msg_id, to_payload(&STREAM_ID)).await;

    while let Some(frame) = peer.source.next().await {
        match frame.unwrap() {
            StreamFrame::Data(p) => {
                let x: i32 = from_payload(p).unwrap();
                peer.sink
                    .send(StreamFrame::Data(to_payload(&(x * 2))))
                    .await
                    .unwrap();
            }
            StreamFrame::Eos => break,
        }
    }

    peer.sink.send(StreamFrame::Eos).await.unwrap();

    // Hold the connection until the client hangs up, like Seastar does
    // until both directions closed.
    assert!(peer.source.next().await.is_none());
}

#[tokio::test]
async fn roundtrip_both_directions() {
    let addr = serve_streaming(accepting(true, true), double_server).await;
    let client = Client::builder().connect(addr).await.unwrap();

    let mut stream = client.open_stream().await.unwrap();
    assert_eq!(stream.id, StreamId(STREAM_ID));

    let back: StreamId = client.call(DOUBLE, stream.id).await.unwrap();
    assert_eq!(back, stream.id);

    for i in 1..=10 {
        stream.sink.send_item(&i).await.unwrap();
    }
    stream.sink.close().await.unwrap();

    let mut got = Vec::new();
    while let Some(x) = stream.source.recv_item::<i32>().await.unwrap() {
        got.push(x);
    }
    assert_eq!(got, (1..=10).map(|i| i * 2).collect::<Vec<_>>());

    // Ended at EOS: stays ended.
    assert!(stream.source.next().await.is_none());
}

#[tokio::test]
async fn peer_closes_first() {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let addr = serve_streaming(accepting(false, false), |_s, mut peer| async move {
        peer.sink.send(StreamFrame::Eos).await.unwrap();

        let mut got = Vec::new();
        while let Some(frame) = peer.source.next().await {
            got.push(frame.unwrap());
        }
        tx.send(got).unwrap();
    })
    .await;
    let client = Client::builder().connect(addr).await.unwrap();
    let stream = client.open_stream().await.unwrap();
    let (mut sink, mut source) = (stream.sink, stream.source);

    assert!(source.next().await.is_none());

    sink.send(Bytes::from_static(b"raw")).await.unwrap();
    sink.close().await.unwrap();
    // A second close is a no-op, but sending after close fails.
    sink.close().await.unwrap();
    assert!(matches!(
        sink.send_item(&1i32).await,
        Err(Error::Protocol(_))
    ));
    drop((sink, source));

    assert_eq!(
        rx.await.unwrap(),
        [
            StreamFrame::Data(Bytes::from_static(b"raw")),
            StreamFrame::Eos
        ]
    );
}

#[tokio::test]
async fn connection_lost_before_eos() {
    let addr = serve_streaming(accepting(false, false), |_s, mut peer| async move {
        peer.sink
            .send(StreamFrame::Data(to_payload(&7i32)))
            .await
            .unwrap();
    })
    .await;
    let client = Client::builder().connect(addr).await.unwrap();
    let mut stream = client.open_stream().await.unwrap();

    assert_eq!(stream.source.recv_item::<i32>().await.unwrap(), Some(7));
    assert!(matches!(
        stream.source.next().await,
        Some(Err(Error::Closed))
    ));
    assert!(stream.source.next().await.is_none());
}

#[tokio::test]
async fn server_without_streaming() {
    let mut reply = accepting(false, false);
    reply.connection_id = None;

    let addr = serve(reply, add_server).await;
    let client = Client::builder().connect(addr).await.unwrap();

    assert!(matches!(
        client.open_stream().await,
        Err(Error::Protocol(_))
    ));
}

#[tokio::test]
async fn handshake_client_cannot_reopen() {
    let addr = serve(accepting(false, false), add_server).await;
    let (read, write) = tokio::net::TcpStream::connect(addr)
        .await
        .unwrap()
        .into_split();
    let client = Client::builder().handshake(read, write).await.unwrap();

    assert!(matches!(
        client.open_stream().await,
        Err(Error::Protocol(_))
    ));
}

#[tokio::test]
async fn compressed() {
    for algo in [Compression::Lz4, Compression::Lz4Fragmented] {
        let mut reply = accepting(true, true);
        reply.compress = Some(Bytes::from_static(algo.name().as_bytes()));

        let addr = serve_streaming(reply, |_s, mut peer| async move {
            // Echo every message back, then close.
            while let Some(StreamFrame::Data(p)) = peer.source.next().await.map(Result::unwrap) {
                peer.sink.send(StreamFrame::Data(p)).await.unwrap();
            }
            peer.sink.send(StreamFrame::Eos).await.unwrap();
            assert!(peer.source.next().await.is_none());
        })
        .await;
        let client = Client::builder()
            .offer_compression([algo])
            .connect(addr)
            .await
            .unwrap();
        let mut stream = client.open_stream().await.unwrap();

        // Spans several `LZ4_FRAGMENTED` chunks in both directions.
        let big: Bytes = (0..200 * 1024u32).map(|i| (i % 97) as u8).collect();
        stream.sink.send_item(&big).await.unwrap();
        stream.sink.close().await.unwrap();

        assert_eq!(
            stream.source.recv_item::<Bytes>().await.unwrap(),
            Some(big),
            "{algo}"
        );
        assert!(stream.source.next().await.is_none());
    }
}
