//! Single calls: replies, exceptions, timeouts, fire-and-forget, disconnects.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use seastar_rpc::rpc::codec::exception::RemoteException;
use seastar_rpc::rpc::codec::serialize::to_payload;
use seastar_rpc::rpc::{CallOptions, Client, Error, NO_WAIT, with_timeout};

use crate::server::{ADD, FIRE, accepting, add_server, serve};

#[tokio::test]
async fn negotiates_and_calls() {
    let addr = serve(accepting(true, true), |s| async move {
        assert!(s.offer.timeout && s.offer.handler_duration);
        add_server(s).await
    })
    .await;
    let client = Client::builder().connect(addr).await.unwrap();
    assert_eq!(client.connection_id(), Some(1 << 16));
    assert_eq!(client.call::<_, i32>(ADD, (3, 4)).await.unwrap(), 7);
}

#[tokio::test]
async fn replies_out_of_order() {
    let (got_r1_tx, got_r1_rx) = tokio::sync::oneshot::channel();
    let addr = serve(accepting(false, false), |mut s| async move {
        let r1 = s.next().await;
        got_r1_tx.send(()).unwrap();
        let r2 = s.next().await;
        s.reply(r2.msg_id, to_payload(&2i32)).await;
        s.reply(r1.msg_id, to_payload(&1i32)).await;
    })
    .await;
    let client = Client::builder().connect(addr).await.unwrap();
    let c1 = client.clone();
    let first = tokio::spawn(async move { c1.call::<_, i32>(ADD, (0, 0)).await });
    // The second call can only become r2 once the server has read r1.
    got_r1_rx.await.unwrap();
    let second = client.call::<_, i32>(ADD, (0, 0)).await.unwrap();
    assert_eq!(second, 2);
    assert_eq!(first.await.unwrap().unwrap(), 1);
}

#[tokio::test]
async fn exceptions() {
    let addr = serve(accepting(false, true), |mut s| async move {
        let r = s.next().await;
        s.throw(
            r.msg_id,
            RemoteException::User(Bytes::from_static(b"exception!")),
        )
        .await;
        let r = s.next().await;
        s.throw(r.msg_id, RemoteException::UnknownVerb(r.verb))
            .await;
        let r = s.next().await;
        s.throw(r.msg_id, RemoteException::Unknown(42, Bytes::new()))
            .await;
    })
    .await;
    let client = Client::builder().connect(addr).await.unwrap();
    let err = client.call::<_, i32>(ADD, (1, 1)).await.unwrap_err();
    assert!(matches!(err, Error::Remote(m) if m.to_str() == Ok("exception!")));
    let err = client
        .call_raw(999, Bytes::new(), CallOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::UnknownVerb(999)));
    let err = client.call::<_, i32>(ADD, (1, 1)).await.unwrap_err();
    assert!(matches!(err, Error::UnknownException { kind: 42, .. }));
}

#[tokio::test]
async fn timeout_then_late_reply_is_dropped() {
    let addr = serve(accepting(true, false), |mut s| async move {
        let slow = s.next().await;
        let fast = s.next().await;
        // Late reply for the call that already timed out, then the real one.
        s.reply(slow.msg_id, to_payload(&100i32)).await;
        s.reply(fast.msg_id, to_payload(&5i32)).await;
        let _ = s.requests.next().await;
    })
    .await;
    let client = Client::builder().connect(addr).await.unwrap();
    let err = client
        .call_with::<_, i32>(ADD, (0, 0), with_timeout(Duration::from_millis(100)))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Timeout));
    assert_eq!(client.call::<_, i32>(ADD, (2, 3)).await.unwrap(), 5);
    assert!(!client.is_closed());
}

#[tokio::test]
async fn deadline_is_sent_as_remaining_ms() {
    let addr = serve(accepting(true, false), |mut s| async move {
        let with = s.next().await;
        assert!(
            with.timeout_ms > 0 && with.timeout_ms <= 1000,
            "{}",
            with.timeout_ms
        );
        s.reply(with.msg_id, to_payload(&0i32)).await;
        let without = s.next().await;
        assert_eq!(without.timeout_ms, 0);
        s.reply(without.msg_id, to_payload(&0i32)).await;
    })
    .await;
    let client = Client::builder().connect(addr).await.unwrap();
    client
        .call_with::<_, i32>(ADD, (0, 0), with_timeout(Duration::from_secs(1)))
        .await
        .unwrap();
    client.call::<_, i32>(ADD, (0, 0)).await.unwrap();
}

async fn fire_server() -> (SocketAddr, tokio::sync::oneshot::Receiver<(u64, usize)>) {
    let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
    let addr = serve(accepting(false, false), |mut s| async move {
        let r = s.next().await;
        seen_tx.send((r.verb, r.payload.len())).unwrap();
        // Never replies; keep the connection open.
        let _ = s.requests.next().await;
    })
    .await;
    (addr, seen_rx)
}

#[tokio::test]
async fn no_wait_recv_completes_after_write() {
    let (addr, seen_rx) = fire_server().await;
    let client = Client::builder().connect(addr).await.unwrap();
    client
        .call_with::<_, ()>(FIRE, (), CallOptions::default().no_wait_recv())
        .await
        .unwrap();
    assert_eq!(seen_rx.await.unwrap(), (5, 0));
}

#[tokio::test]
async fn no_wait_returns_once_queued() {
    let (addr, seen_rx) = fire_server().await;
    let client = Client::builder().connect(addr).await.unwrap();
    client.call_with::<_, ()>(FIRE, (), NO_WAIT).await.unwrap();
    // The writer still sends it.
    assert_eq!(seen_rx.await.unwrap(), (5, 0));
}

#[tokio::test]
async fn no_wait_send_fails_on_full_queue() {
    let (addr, _seen_rx) = fire_server().await;
    let client = Client::builder()
        .request_queue(1)
        .connect(addr)
        .await
        .unwrap();
    // Fire-and-forget calls never yield, so the writer can't drain the queue
    // in between on this single-threaded runtime.
    client.call_with::<_, ()>(FIRE, (), NO_WAIT).await.unwrap();
    let err = client
        .call_with::<_, ()>(FIRE, (), NO_WAIT)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::QueueFull), "{err:?}");
    let err = client
        .call_with::<_, i32>(ADD, (1, 2), CallOptions::default().no_wait_send())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::QueueFull), "{err:?}");
}

#[tokio::test]
async fn disconnect_fails_pending_and_later_calls() {
    let addr = serve(accepting(false, false), |mut s| async move {
        let _ = s.next().await;
        // Drop the connection with the call outstanding.
    })
    .await;
    let client = Client::builder().connect(addr).await.unwrap();
    let err = client.call::<_, i32>(ADD, (1, 2)).await.unwrap_err();
    assert!(matches!(err, Error::Closed), "{err:?}");
    assert!(client.is_closed());
    assert!(matches!(
        client.call::<_, i32>(ADD, (1, 2)).await,
        Err(Error::Closed)
    ));
    assert!(matches!(
        client
            .call_with::<_, ()>(FIRE, (), CallOptions::default().no_wait())
            .await,
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn many_concurrent_calls() {
    let addr = serve(accepting(true, true), add_server).await;
    let client = Client::builder()
        .request_queue(8)
        .connect(addr)
        .await
        .unwrap();
    let calls: Vec<_> = (0..100)
        .map(|i| {
            let c = client.clone();
            tokio::spawn(async move { (i, c.call::<_, i32>(ADD, (i, 1000)).await.unwrap()) })
        })
        .collect();
    for call in calls {
        let (i, sum) = call.await.unwrap();
        assert_eq!(sum, i + 1000);
    }
}

#[tokio::test]
async fn reply_type_mismatch_is_decode_error() {
    let addr = serve(accepting(false, false), |mut s| async move {
        let r = s.next().await;
        s.reply(r.msg_id, to_payload(&1i64)).await;
    })
    .await;
    let client = Client::builder().connect(addr).await.unwrap();
    assert!(matches!(
        client.call::<_, i32>(ADD, (0, 0)).await,
        Err(Error::Decode(_))
    ));
}
