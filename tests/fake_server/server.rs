//! The scripted server and the helpers every test module shares.

use std::net::SocketAddr;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use seastar_rpc::rpc::Verb;
use seastar_rpc::rpc::codec::compress::{Compressed, Compression};
use seastar_rpc::rpc::codec::exception::RemoteException;
use seastar_rpc::rpc::codec::frame::{
    FrameLayout, RequestCodec, RequestFrame, ResponseCodec, ResponseFrame,
};
use seastar_rpc::rpc::codec::negotiation::{Features, read_negotiation, write_negotiation};
use seastar_rpc::rpc::codec::serialize::{from_payload, to_payload};
use tokio::net::TcpListener;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_util::codec::{FramedRead, FramedWrite};

pub struct Server {
    pub requests: FramedRead<OwnedReadHalf, Compressed<RequestCodec>>,
    pub responses: FramedWrite<OwnedWriteHalf, Compressed<ResponseCodec>>,
    /// What the client offered.
    pub offer: Features,
}

impl Server {
    pub async fn next(&mut self) -> RequestFrame {
        self.requests.next().await.expect("client hung up").unwrap()
    }

    pub async fn reply(&mut self, msg_id: i64, payload: Bytes) {
        let frame = ResponseFrame {
            msg_id,
            handler_duration: Some(5),
            payload,
        };
        self.responses.send(frame).await.unwrap();
    }

    pub async fn throw(&mut self, msg_id: i64, e: RemoteException) {
        self.reply(-msg_id, e.encode()).await;
    }
}

pub fn accepting(timeout: bool, handler_duration: bool) -> Features {
    let mut f = Features::default();
    f.timeout = timeout;
    f.handler_duration = handler_duration;
    f.connection_id = Some(1 << 16);
    f
}

/// Start a one-connection server that replies to negotiation with `reply`
/// and then runs `script`. Frames are compressed if `reply` names an
/// algorithm.
pub async fn serve<F, Fut>(reply: Features, script: F) -> SocketAddr
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
        let compression = reply.compress.as_deref().and_then(Compression::from_name);
        script(Server {
            requests: FramedRead::new(
                read,
                Compressed::new(RequestCodec::new(layout), layout, compression),
            ),
            responses: FramedWrite::new(
                write,
                Compressed::new(ResponseCodec::new(layout), layout, compression),
            ),
            offer,
        })
        .await;
    });
    addr
}

pub const ADD: Verb = 2;
pub const FIRE: Verb = 5;

pub async fn add_server(mut s: Server) {
    while let Some(Ok(req)) = s.requests.next().await {
        let (a, b): (i32, i32) = from_payload(req.payload).unwrap();
        s.reply(req.msg_id, to_payload(&(a + b))).await;
    }
}
