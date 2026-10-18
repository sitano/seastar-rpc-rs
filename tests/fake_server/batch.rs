//! Batches: ordering, deadlines, queue limits, reuse and leaked sends.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use seastar_rpc::rpc::codec::exception::RemoteException;
use seastar_rpc::rpc::codec::frame::{FrameLayout, RequestCodec, ResponseCodec, ResponseFrame};
use seastar_rpc::rpc::codec::negotiation::{read_negotiation, write_negotiation};
use seastar_rpc::rpc::codec::serialize::{from_payload, to_payload};
use seastar_rpc::rpc::{
    BATCH_LEAKED_PANIC, Batch, CallOptions, Client, Encode, Error, NO_WAIT, Verb, with_timeout,
};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::server::{ADD, FIRE, Server, accepting, add_server, serve};

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
