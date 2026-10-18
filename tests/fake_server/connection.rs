//! Connection setup and transport: raw connections, handshakes over any
//! I/O, bad peers and writer shutdown.

use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use seastar_rpc::rpc::codec::exception::RemoteException;
use seastar_rpc::rpc::codec::frame::{FrameLayout, RequestCodec, ResponseCodec, ResponseFrame};
use seastar_rpc::rpc::codec::negotiation::{read_negotiation, write_negotiation};
use seastar_rpc::rpc::codec::serialize::to_payload;
use seastar_rpc::rpc::raw::RawConnection;
use seastar_rpc::rpc::{CallOptions, Client, Error};
use tokio::net::TcpListener;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::server::{ADD, accepting, serve};

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

    let RawConnection {
        sender: mut tx,
        receiver: mut rx,
        features,
        ..
    } = Client::builder().connect_raw(addr).await.unwrap();
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
