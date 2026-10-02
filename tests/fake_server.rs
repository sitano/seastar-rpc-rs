//! Client behaviour against a scripted in-process server built from the
//! crate's own codecs, used in the server direction.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use seastar_rpc::rpc::codec::exception::RemoteException;
use seastar_rpc::rpc::codec::frame::{
    FrameLayout, RequestCodec, RequestFrame, ResponseCodec, ResponseFrame,
};
use seastar_rpc::rpc::codec::negotiation::{Features, read_negotiation, write_negotiation};
use seastar_rpc::rpc::codec::serialize::{from_payload, to_payload};
use seastar_rpc::rpc::{CallOptions, Client, Error, NO_WAIT, Verb, with_timeout};
use tokio::net::TcpListener;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_util::codec::{FramedRead, FramedWrite};

struct Server {
    requests: FramedRead<OwnedReadHalf, RequestCodec>,
    responses: FramedWrite<OwnedWriteHalf, ResponseCodec>,
    /// What the client offered.
    offer: Features,
}

impl Server {
    async fn next(&mut self) -> RequestFrame {
        self.requests.next().await.expect("client hung up").unwrap()
    }

    async fn reply(&mut self, msg_id: i64, payload: Bytes) {
        let frame = ResponseFrame {
            msg_id,
            handler_duration: Some(5),
            payload,
        };
        self.responses.send(frame).await.unwrap();
    }

    async fn throw(&mut self, msg_id: i64, e: RemoteException) {
        self.reply(-msg_id, e.encode()).await;
    }
}

fn accepting(timeout: bool, handler_duration: bool) -> Features {
    let mut f = Features::default();
    f.timeout = timeout;
    f.handler_duration = handler_duration;
    f.connection_id = Some(1 << 16);
    f
}

/// Start a one-connection server that replies to negotiation with `reply`
/// and then runs `script`.
async fn serve<F, Fut>(reply: Features, script: F) -> SocketAddr
where
    F: FnOnce(Server) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (mut read, mut write) = stream.into_split();
        let offer = read_negotiation(&mut read).await.unwrap();
        write_negotiation(&mut write, &reply).await.unwrap();
        let layout = FrameLayout::new(&reply, 1 << 20);
        script(Server {
            requests: FramedRead::new(read, RequestCodec::new(layout)),
            responses: FramedWrite::new(write, ResponseCodec::new(layout)),
            offer,
        })
        .await;
    });
    addr
}

const ADD: Verb = 2;
const FIRE: Verb = 5;

async fn add_server(mut s: Server) {
    while let Some(Ok(req)) = s.requests.next().await {
        let (a, b): (i32, i32) = from_payload(req.payload).unwrap();
        s.reply(req.msg_id, to_payload(&(a + b))).await;
    }
}

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

#[tokio::test]
async fn raw_connection_yields_every_response() {
    let addr = serve(accepting(true, true), |mut s| async move {
        let a = s.next().await;
        let b = s.next().await;
        assert_eq!((a.msg_id, a.verb, b.msg_id, b.verb), (10, 2, 20, 777));
        assert_eq!(a.timeout_ms, 250);
        s.reply(b.msg_id, Bytes::from_static(b"b")).await;
        s.throw(a.msg_id, RemoteException::UnknownVerb(2)).await;
        // A reply for an id the client never used is still delivered.
        s.reply(30, Bytes::new()).await;
    })
    .await;

    let raw = Client::builder().connect_raw(addr).await.unwrap();
    let (mut tx, mut rx, features) = raw.into_parts();
    assert!(features.timeout);
    tx.send_request(
        10,
        2,
        to_payload(&(1i32, 2i32)),
        Some(Duration::from_millis(250)),
    )
    .await
    .unwrap();
    tx.send_request(20, 777, Bytes::new(), None).await.unwrap();

    let r = rx.next().await.unwrap().unwrap();
    assert_eq!(
        (r.msg_id, r.handler_duration, &r.payload[..]),
        (20, Some(5), &b"b"[..])
    );
    let r = rx.next().await.unwrap().unwrap();
    assert_eq!(r.call_id(), 10);
    assert!(matches!(r.into_result(), Err(Error::UnknownVerb(2))));
    assert_eq!(rx.next().await.unwrap().unwrap().msg_id, 30);
    assert!(rx.next().await.is_none(), "stream ends on server EOF");
}

#[tokio::test]
async fn handshake_over_any_transport() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let (mut read, mut write) = tokio::io::split(server_io);
        read_negotiation(&mut read).await.unwrap();
        let reply = accepting(false, false);
        write_negotiation(&mut write, &reply).await.unwrap();
        let layout = FrameLayout::new(&reply, 1024);
        let mut requests = FramedRead::new(read, RequestCodec::new(layout));
        let mut responses = FramedWrite::new(write, ResponseCodec::new(layout));
        let r = requests.next().await.unwrap().unwrap();
        let frame = ResponseFrame {
            msg_id: r.msg_id,
            handler_duration: None,
            payload: to_payload(&9i32),
        };
        responses.send(frame).await.unwrap();
    });
    let (read, write) = tokio::io::split(client_io);
    let client = Client::builder().handshake(read, write).await.unwrap();
    assert_eq!(client.call::<_, i32>(ADD, (4, 5)).await.unwrap(), 9);
}

#[tokio::test]
async fn bad_magic_fails_connect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut stream, b"HTTP/1.1 400 Bad\r\n")
            .await
            .unwrap();
    });
    let err = Client::builder().connect(addr).await.unwrap_err();
    assert!(matches!(err, Error::Protocol(_)), "{err:?}");
}

/// Signals `dropped` when the wrapped write half is dropped.
struct DropSignal<W> {
    inner: W,
    _dropped: tokio::sync::oneshot::Sender<()>,
}

impl<W: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for DropSignal<W> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn reader_eof_cancels_blocked_write() {
    // Separate pipes per direction so the server can end its replies while
    // keeping the request pipe open without reading it.
    let (client_read, mut server_write) = tokio::io::duplex(1024);
    let (client_write, mut server_read) = tokio::io::duplex(1024);
    let (hang_up_tx, hang_up_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        read_negotiation(&mut server_read).await.unwrap();
        write_negotiation(&mut server_write, &accepting(false, false))
            .await
            .unwrap();
        let _ = hang_up_rx.await;
        // EOF on the client's reader; never read the requests.
        drop(server_write);
        std::future::pending::<()>().await;
        drop(server_read);
    });

    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let write = DropSignal {
        inner: client_write,
        _dropped: dropped_tx,
    };
    let client = Client::builder()
        .handshake(client_read, write)
        .await
        .unwrap();

    // Larger than the pipe: the writer blocks mid-write.
    let call = client.call_raw(
        ADD,
        Bytes::from(vec![0u8; 64 << 10]),
        CallOptions::default(),
    );
    let call = tokio::spawn(call);
    tokio::time::sleep(Duration::from_millis(50)).await;
    hang_up_tx.send(()).unwrap();

    assert!(matches!(call.await.unwrap(), Err(Error::Closed)));
    tokio::time::timeout(Duration::from_secs(5), dropped_rx)
        .await
        .expect("writer still holds the write half")
        .unwrap_err();
    drop(client);
}
