//! Client behaviour against a scripted in-process server built from the
//! crate's own codecs, used in the server direction.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use seastar_rpc::rpc::codec::exception::RemoteException;
use seastar_rpc::rpc::codec::frame::{
    FrameLayout, RequestCodec, RequestFrame, ResponseCodec, ResponseFrame,
};
use seastar_rpc::rpc::codec::negotiation::{Features, read_negotiation, write_negotiation};
use seastar_rpc::rpc::codec::serialize::{from_payload, to_payload};
use seastar_rpc::rpc::{
    BATCH_LEAKED_PANIC, Batch, CallOptions, Client, Encode, Error, NO_WAIT, Verb, with_timeout,
};
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

#[tokio::test]
async fn batch_replies_in_push_order() {
    let addr = serve(accepting(false, false), |mut s| async move {
        let mut reqs = Vec::new();
        for _ in 0..3 {
            reqs.push(s.next().await);
        }
        for r in reqs.into_iter().rev() {
            let (a, b): (i32, i32) = from_payload(r.payload).unwrap();
            s.reply(r.msg_id, to_payload(&(a + b))).await;
        }
        let _ = s.requests.next().await;
    })
    .await;
    let client = Client::builder().connect(addr).await.unwrap();
    let mut batch = client.batch();
    assert!(batch.is_empty());
    assert_eq!(batch.push(ADD, (1, 1)), 0);
    assert_eq!(batch.push(ADD, (2, 2)), 1);
    assert_eq!(batch.push_raw(ADD, to_payload(&(3i32, 3i32))), 2);
    assert_eq!(batch.len(), 3);
    let replies = batch.send().await.unwrap();
    assert_eq!(replies.len(), 3);
    let sums: Vec<i32> = replies.map(|r| from_payload(r.unwrap()).unwrap()).collect();
    assert_eq!(sums, [2, 4, 6]);
    assert!(batch.is_empty());
}

#[tokio::test]
async fn batch_push_after_encode_panic_sends_clean_payload() {
    struct Bad;
    impl Encode for Bad {
        fn encode(&self, buf: &mut BytesMut) {
            buf.put_u32_le(0xdead_beef);
            panic!("encode failed");
        }
    }
    let addr = serve(accepting(false, false), |mut s| async move {
        let r = s.next().await;
        assert_eq!(r.payload, to_payload(&(1i32, 2i32)));
        s.reply(r.msg_id, to_payload(&3i32)).await;
        let _ = s.requests.next().await;
    })
    .await;
    let client = Client::builder().connect(addr).await.unwrap();
    let mut batch = client.batch();
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        batch.push(ADD, Bad);
    }));
    assert!(panicked.is_err());
    assert!(batch.is_empty());
    batch.push(ADD, (1i32, 2i32));
    let mut replies = batch.send().await.unwrap();
    let sum: i32 = from_payload(replies.next().unwrap().unwrap()).unwrap();
    assert_eq!(sum, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_frames_are_contiguous() {
    const MARK: i32 = 1_000_000;
    const SINGLES: i32 = 100;
    const BATCH: i32 = 200; // > MAX_BATCH (64)
    let (marks_tx, marks_rx) = tokio::sync::oneshot::channel();
    let addr = serve(accepting(false, false), |mut s| async move {
        let mut marks = Vec::new();
        for _ in 0..SINGLES + BATCH {
            let r = s.next().await;
            let (a, b): (i32, i32) = from_payload(r.payload).unwrap();
            marks.push(b == MARK);
            s.reply(r.msg_id, to_payload(&(a + b))).await;
        }
        marks_tx.send(marks).unwrap();
        let _ = s.requests.next().await;
    })
    .await;
    let client = Client::builder()
        .request_queue(4)
        .connect(addr)
        .await
        .unwrap();
    let singles: Vec<_> = (0..SINGLES)
        .map(|i| {
            let c = client.clone();
            tokio::spawn(async move { c.call::<_, i32>(ADD, (i, 0)).await.unwrap() })
        })
        .collect();
    let sums: Vec<i32> = client
        .call_many::<_, i32>(ADD, (0..BATCH).map(|i| (i, MARK)))
        .await
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(sums, (0..BATCH).map(|i| i + MARK).collect::<Vec<_>>());
    for s in singles {
        s.await.unwrap();
    }
    let marks = marks_rx.await.unwrap();
    let first = marks.iter().position(|&m| m).unwrap();
    let run = &marks[first..first + BATCH as usize];
    assert!(run.iter().all(|&m| m), "batch interleaved: {marks:?}");
}

#[tokio::test]
async fn batch_deadline_keeps_completed() {
    let addr = serve(accepting(true, false), |mut s| async move {
        let mut reqs = Vec::new();
        for _ in 0..4 {
            reqs.push(s.next().await);
        }
        s.reply(reqs[0].msg_id, to_payload(&7i32)).await;
        s.reply(reqs[2].msg_id, to_payload(&9i32)).await;
        let _ = s.requests.next().await;
    })
    .await;
    let client = Client::builder().connect(addr).await.unwrap();
    let replies: Vec<_> = client
        .call_many_with::<_, i32>(ADD, [(0, 0); 4], with_timeout(Duration::from_millis(200)))
        .await
        .unwrap()
        .collect();
    assert!(
        matches!(
            replies[..],
            [Ok(7), Err(Error::Timeout), Ok(9), Err(Error::Timeout)]
        ),
        "{replies:?}"
    );
    assert!(!client.is_closed());
}

#[tokio::test]
async fn batch_zero_timeout_times_out_each_item() {
    let addr = serve(accepting(true, false), add_server).await;
    let client = Client::builder().connect(addr).await.unwrap();
    let replies: Vec<_> = client
        .call_many_with::<_, i32>(ADD, [(1, 1); 3], with_timeout(Duration::ZERO))
        .await
        .unwrap()
        .collect();
    assert!(
        replies.iter().all(|r| matches!(r, Err(Error::Timeout))),
        "{replies:?}"
    );
    assert_eq!(client.call::<_, i32>(ADD, (2, 3)).await.unwrap(), 5);
}

/// Server that forwards the verb of every request it reads, never replying.
async fn verb_recorder() -> (SocketAddr, tokio::sync::mpsc::UnboundedReceiver<u64>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let addr = serve(accepting(false, false), |s| async move {
        // Hold the write half too: dropping it would close the connection.
        let Server {
            mut requests,
            responses: _responses,
            ..
        } = s;
        while let Some(Ok(r)) = requests.next().await {
            let _ = tx.send(r.verb);
        }
    })
    .await;
    (addr, rx)
}

#[tokio::test]
async fn batch_no_wait_send_queue_full_keeps_requests() {
    const OTHER: Verb = 7;
    let (addr, mut verbs) = verb_recorder().await;
    let client = Client::builder()
        .request_queue(1)
        .connect(addr)
        .await
        .unwrap();
    // Fills the queue without yielding (see no_wait_send_fails_on_full_queue).
    client.call_with::<_, ()>(FIRE, (), NO_WAIT).await.unwrap();
    let mut batch = client.batch();
    batch.push(OTHER, ());
    batch.push(OTHER, ());
    assert!(matches!(
        batch.send_with(NO_WAIT).await,
        Err(Error::QueueFull)
    ));
    assert_eq!(batch.len(), 2, "a rejected batch keeps its requests");
    assert!(matches!(
        batch.send_with(CallOptions::default().no_wait_send()).await,
        Err(Error::QueueFull)
    ));
    assert_eq!(batch.len(), 2);
    // Waits for the write, so everything queued before it has been sent.
    client
        .call_with::<_, ()>(FIRE, (), CallOptions::default().no_wait_recv())
        .await
        .unwrap();
    assert_eq!(verbs.recv().await, Some(FIRE));
    assert_eq!(verbs.recv().await, Some(FIRE));
    assert!(verbs.try_recv().is_err(), "a rejected batch was sent");
    // Retrying once there is room sends it.
    assert_eq!(batch.send_with(NO_WAIT).await.unwrap().len(), 2);
    assert_eq!(verbs.recv().await, Some(OTHER));
    assert_eq!(verbs.recv().await, Some(OTHER));
}

#[tokio::test]
async fn batch_no_wait_recv_completes_after_write() {
    let (addr, mut verbs) = verb_recorder().await;
    let client = Client::builder().connect(addr).await.unwrap();
    let mut batch = client.batch();
    for _ in 0..3 {
        batch.push(FIRE, ());
    }
    let replies: Vec<_> = batch
        .send_with(CallOptions::default().no_wait_recv())
        .await
        .unwrap()
        .collect();
    assert_eq!(replies.len(), 3);
    assert!(
        replies
            .iter()
            .all(|r| r.as_ref().is_ok_and(|b| b.is_empty()))
    );
    for _ in 0..3 {
        assert_eq!(verbs.recv().await, Some(FIRE));
    }
    assert!(
        client
            .call_many_with::<_, ()>(FIRE, [(); 2], NO_WAIT)
            .await
            .unwrap()
            .all(|r| r.is_ok())
    );
}

#[tokio::test]
async fn batch_disconnect_fails_remaining() {
    let addr = serve(accepting(false, false), |mut s| async move {
        let r = s.next().await;
        let _ = s.next().await;
        let _ = s.next().await;
        s.reply(r.msg_id, to_payload(&1i32)).await;
        // Drop the connection with two calls outstanding.
    })
    .await;
    let client = Client::builder().connect(addr).await.unwrap();
    let replies: Vec<_> = client
        .call_many::<_, i32>(ADD, [(0, 0); 3])
        .await
        .unwrap()
        .collect();
    assert!(
        matches!(replies[..], [Ok(1), Err(Error::Closed), Err(Error::Closed)]),
        "{replies:?}"
    );
    assert!(client.is_closed());
    assert!(matches!(
        client.call_many::<_, i32>(ADD, [(0, 0)]).await,
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn batch_mixed_exceptions_and_decode_errors() {
    let addr = serve(accepting(false, false), |mut s| async move {
        let a = s.next().await;
        let b = s.next().await;
        let c = s.next().await;
        s.reply(a.msg_id, to_payload(&7i32)).await;
        s.throw(b.msg_id, RemoteException::User(Bytes::from_static(b"boom")))
            .await;
        s.reply(c.msg_id, to_payload(&1i64)).await;
        let _ = s.requests.next().await;
    })
    .await;
    let client = Client::builder().connect(addr).await.unwrap();
    let replies: Vec<_> = client
        .call_many::<_, i32>(ADD, [(0, 0); 3])
        .await
        .unwrap()
        .collect();
    assert!(matches!(replies[0], Ok(7)), "{replies:?}");
    assert!(
        matches!(&replies[1], Err(Error::Remote(m)) if m.to_str() == Ok("boom")),
        "{replies:?}"
    );
    assert!(matches!(replies[2], Err(Error::Decode(_))), "{replies:?}");
}

#[tokio::test]
async fn batch_dropped_then_late_replies_are_ignored() {
    let addr = serve(accepting(false, false), |mut s| async move {
        let a = s.next().await;
        let b = s.next().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        s.reply(a.msg_id, to_payload(&1i32)).await;
        s.reply(b.msg_id, to_payload(&2i32)).await;
        add_server(s).await
    })
    .await;
    let client = Client::builder().connect(addr).await.unwrap();
    let mut batch = client.batch();
    batch.push(ADD, (0, 0));
    batch.push(ADD, (0, 0));
    let abandoned = tokio::time::timeout(Duration::from_millis(20), batch.send())
        .await
        .is_err();
    assert!(abandoned);
    // The batch was queued before it was abandoned: it is empty and reusable.
    assert!(batch.is_empty());
    batch.push(ADD, (2, 3));
    let sums: Vec<i32> = batch
        .send()
        .await
        .unwrap()
        .map(|r| from_payload(r.unwrap()).unwrap())
        .collect();
    assert_eq!(sums, [5]);
    assert!(!client.is_closed());
}

/// Like `add_server`, but ignores non-`ADD` verbs and never answers `a < 0`.
async fn selective_add_server(mut s: Server) {
    while let Some(Ok(req)) = s.requests.next().await {
        if req.verb != ADD {
            continue;
        }
        let (a, b): (i32, i32) = from_payload(req.payload).unwrap();
        if a >= 0 {
            s.reply(req.msg_id, to_payload(&(a + b))).await;
        }
    }
}

async fn sums(batch: &mut Batch, opts: CallOptions) -> Vec<Result<i32, Error>> {
    batch
        .send_with(opts)
        .await
        .unwrap()
        .map(|r| r.and_then(|p| Ok(from_payload::<i32>(p)?)))
        .collect()
}

#[tokio::test]
async fn batch_is_reusable() {
    let addr = serve(accepting(true, false), selective_add_server).await;
    let client = Client::builder().connect(addr).await.unwrap();
    let mut batch = client.batch();

    batch.push(ADD, (1, 1));
    batch.push(ADD, (2, 2));
    let r = sums(&mut batch, CallOptions::default()).await;
    assert!(matches!(r[..], [Ok(2), Ok(4)]), "{r:?}");

    // An unanswered item times out; the other still arrives.
    batch.push(ADD, (-1, 0));
    batch.push(ADD, (3, 3));
    let r = sums(&mut batch, with_timeout(Duration::from_millis(100))).await;
    assert!(matches!(r[..], [Err(Error::Timeout), Ok(6)]), "{r:?}");

    // Fire and forget: the queue still holds this state, so the next push
    // moves to another one (see batch_push_after_no_wait_switches_state).
    batch.push(FIRE, ());
    let r: Vec<_> = batch.send_with(NO_WAIT).await.unwrap().collect();
    assert!(matches!(r[..], [Ok(_)]), "{r:?}");

    batch.push(ADD, (4, 4));
    batch.push(ADD, (5, 5));
    batch.push(ADD, (6, 6));
    let r = sums(&mut batch, CallOptions::default()).await;
    assert!(matches!(r[..], [Ok(8), Ok(10), Ok(12)]), "{r:?}");

    batch.push(ADD, (7, 7));
    batch.clear();
    assert!(batch.is_empty());
    assert!(sums(&mut batch, CallOptions::default()).await.is_empty());
}

#[tokio::test]
async fn batch_push_after_no_wait_switches_state() {
    let (tx, mut verbs) = tokio::sync::mpsc::unbounded_channel();
    let addr = serve(accepting(false, false), |mut s| async move {
        while let Some(Ok(req)) = s.requests.next().await {
            let _ = tx.send(req.verb);
            if req.verb == ADD {
                let (a, b): (i32, i32) = from_payload(req.payload).unwrap();
                s.reply(req.msg_id, to_payload(&(a + b))).await;
            }
        }
    })
    .await;
    let client = Client::builder().connect(addr).await.unwrap();
    let mut batch = client.batch();

    batch.push(FIRE, ());
    batch.push(FIRE, ());
    assert_eq!(batch.send_with(NO_WAIT).await.unwrap().len(), 2);

    // A NO_WAIT send never yields, and this single-threaded runtime runs the
    // writer only when the test yields: the queue still holds the state, so
    // this push must switch to a fresh one.
    batch.push(ADD, (1, 2));
    batch.push(ADD, (3, 4));
    let r = sums(&mut batch, CallOptions::default()).await;
    assert!(matches!(r[..], [Ok(3), Ok(7)]), "{r:?}");

    for verb in [FIRE, FIRE, ADD, ADD] {
        assert_eq!(verbs.recv().await, Some(verb));
    }
}

/// Writes `budget` bytes, then fails every write once `fail` fires.
struct FailAfter<W> {
    inner: W,
    budget: usize,
    fail: tokio::sync::oneshot::Receiver<()>,
}

impl<W: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for FailAfter<W> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.budget == 0 {
            let _ = std::task::ready!(std::pin::Pin::new(&mut self.fail).poll(cx));
            return std::task::Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()));
        }

        let n = buf.len().min(self.budget);
        let written =
            std::task::ready!(std::pin::Pin::new(&mut self.inner).poll_write(cx, &buf[..n]))?;
        self.budget -= written;

        std::task::Poll::Ready(Ok(written))
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
async fn batch_writer_error_fails_unsent_items() {
    const ITEM: usize = 16 << 10;

    let (client_read, mut server_write) = tokio::io::duplex(1 << 16);
    let (client_write, mut server_read) = tokio::io::duplex(1 << 16);
    let (fail_tx, fail_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let features = accepting(false, false);
        read_negotiation(&mut server_read).await.unwrap();
        write_negotiation(&mut server_write, &features)
            .await
            .unwrap();

        let layout = FrameLayout::new(&features, 1 << 20);
        let mut requests = FramedRead::new(server_read, RequestCodec::new(layout));
        let mut responses = FramedWrite::new(server_write, ResponseCodec::new(layout));

        let first = requests.next().await.unwrap().unwrap();
        let frame = ResponseFrame {
            msg_id: first.msg_id,
            handler_duration: None,
            payload: to_payload(&1i32),
        };
        responses.send(frame).await.unwrap();

        // Let the client's reader take the reply before the write fails.
        tokio::time::sleep(Duration::from_millis(50)).await;
        fail_tx.send(()).unwrap();

        // Keep both pipes open: only the client's writer sees the failure.
        std::future::pending::<()>().await;
        drop((requests, responses));
    });

    // Room for the handshake and the first item, not the second.
    let write = FailAfter {
        inner: client_write,
        budget: ITEM + 4096,
        fail: fail_rx,
    };
    let client = Client::builder()
        .handshake(client_read, write)
        .await
        .unwrap();

    let mut batch = client.batch();
    for _ in 0..3 {
        batch.push_raw(ADD, vec![0u8; ITEM]);
    }
    let r = sums(&mut batch, CallOptions::default()).await;
    assert!(
        matches!(
            r[..],
            [
                Ok(1),
                Err(Error::Io(_) | Error::Closed),
                Err(Error::Io(_) | Error::Closed)
            ]
        ),
        "{r:?}"
    );

    assert!(client.is_closed());
    batch.push_raw(ADD, vec![0u8; ITEM]);
    assert!(matches!(
        batch.send().await,
        Err(Error::Io(_) | Error::Closed)
    ));
    assert_eq!(batch.len(), 1, "a rejected batch keeps its requests");
}

/// A batch whose send is leaked while waiting for queue room. The leaked
/// send holds one of the two queue slots for good.
/// See docs/leaked-send-futures.md.
async fn leaked_batch(client: &Client) -> Batch {
    // Fill the queue without yielding (see no_wait_send_fails_on_full_queue).
    client.call_with::<_, ()>(FIRE, (), NO_WAIT).await.unwrap();
    client.call_with::<_, ()>(FIRE, (), NO_WAIT).await.unwrap();
    let mut batch = client.batch();
    // Negative `a`: the server never answers these, should they ever be sent.
    batch.push(ADD, (-1, 0));
    batch.push(ADD, (-2, 0));
    // One poll registers the pending replies and parks the send on the full
    // queue, holding a clone of the batch's buffers.
    let mut send = Box::pin(batch.send());
    assert!(futures_util::poll!(send.as_mut()).is_pending());
    // Leak it: its Drop never runs, so the batch still counts 2 requests
    // while the leaked future keeps them and never sends them. That also
    // ends the borrow, so the batch can be used again.
    std::mem::forget(send);
    batch
}

/// A client whose queue has room for two messages: the leaked send takes
/// one for good, the other keeps the client usable.
async fn leak_client() -> Client {
    let addr = serve(accepting(false, false), selective_add_server).await;
    Client::builder()
        .request_queue(2)
        .connect(addr)
        .await
        .unwrap()
}

#[tokio::test]
#[should_panic(expected = "leaked send() future (2 request(s))")]
async fn batch_send_after_leaked_send_panics() {
    let client = leak_client().await;
    let mut batch = leaked_batch(&client).await;
    // No new push: the send itself notices the 2 lost requests and panics,
    // rather than sending nothing and returning Ok.
    let _ = batch.send().await;
}

#[tokio::test]
async fn batch_is_usable_after_leak_panic() {
    let client = leak_client().await;
    let mut batch = leaked_batch(&client).await;
    // The first use after the leak panics, even a push.
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        batch.push(ADD, (1, 1));
    }));

    // A caller can tell this panic apart by the message prefix.
    let payload = panicked.unwrap_err();
    let msg = payload.downcast_ref::<String>().unwrap();

    assert!(msg.starts_with(BATCH_LEAKED_PANIC), "{msg}");
    // The batch reset itself before panicking: the lost requests are no
    // longer counted, and the push that panicked added nothing.
    assert!(batch.is_empty());
    // So pushing starts again from index 0, on fresh buffers.
    assert_eq!(batch.push(ADD, (1, 1)), 0);

    // And the send waits only for that one reply, not the 2 lost ones: with
    // a stale count it would wait out the deadline instead.
    let r = sums(&mut batch, with_timeout(Duration::from_secs(5))).await;
    assert!(matches!(r[..], [Ok(2)]), "{r:?}");
}

#[tokio::test]
async fn empty_batch_resolves_immediately() {
    let (addr, mut verbs) = verb_recorder().await;
    let client = Client::builder().connect(addr).await.unwrap();
    assert_eq!(client.batch().send().await.unwrap().len(), 0);
    let none: Vec<(i32, i32)> = Vec::new();
    assert_eq!(
        client.call_many::<_, i32>(ADD, none).await.unwrap().len(),
        0
    );
    client
        .call_with::<_, ()>(FIRE, (), CallOptions::default().no_wait_recv())
        .await
        .unwrap();
    assert_eq!(verbs.recv().await, Some(FIRE));
}
