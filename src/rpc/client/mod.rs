//! Multiplexed client: many concurrent calls over one connection, replies
//! matched to callers by `msg_id`.

mod batch;
mod builder;
mod dispatch;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

pub use batch::{BATCH_LEAKED_PANIC, Batch, ManyReplies, Replies};
pub use builder::ClientBuilder;
use dispatch::{Outgoing, Request, Shared};

use crate::rpc::codec::negotiation::Features;
use crate::rpc::codec::serialize::{Decode, Encode, from_payload, to_payload};
use crate::rpc::error::{Error, Result};
use crate::rpc::raw::RawConnection;
use crate::rpc::stream::StreamConnection;
use crate::rpc::verb::Verb;

/// Per-call options.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct CallOptions {
    /// Overrides the client's default timeout.
    pub timeout: Option<Duration>,
    /// Don't wait to queue the request: fail with [`Error::QueueFull`]
    /// instead of waiting for room in the request queue. Combined with
    /// `no_wait_recv`, the call returns as soon as the request is queued,
    /// without waiting for it to be written.
    pub no_wait_send: bool,
    /// The verb never replies (`rpc::no_wait_type`): don't wait for a reply.
    /// The call yields an empty payload, so a typed call should expect `()`.
    /// Unless `no_wait_send` is set too, it completes once the request is
    /// written.
    pub no_wait_recv: bool,
}

impl CallOptions {
    /// Sets the timeout for this call. Overrides the client's default timeout if specified.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Fire and forget: sets both `no_wait_send` and `no_wait_recv`, so the
    /// call never blocks.
    pub fn no_wait(self) -> Self {
        self.no_wait_send().no_wait_recv()
    }

    /// Sets `no_wait_send`.
    pub fn no_wait_send(mut self) -> Self {
        self.no_wait_send = true;
        self
    }

    /// Sets `no_wait_recv`.
    pub fn no_wait_recv(mut self) -> Self {
        self.no_wait_recv = true;
        self
    }
}

/// Predefined `CallOptions` for fire-and-forget calls (see [`CallOptions::no_wait`]).
pub const NO_WAIT: CallOptions = CallOptions {
    timeout: None,
    no_wait_send: true,
    no_wait_recv: true,
};

/// Predefined `CallOptions` with a custom timeout.
pub fn with_timeout(timeout: Duration) -> CallOptions {
    CallOptions::default().timeout(timeout)
}

/// Handle to a multiplexed connection. Cheap to clone; all clones share the
/// connection, which closes once every clone is dropped.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    features: Features,
    default_timeout: Option<Duration>,
    next_id: AtomicI64,
    shared: Arc<Shared>,
    tx: mpsc::Sender<Outgoing>,
    /// Where and how to open streams; `None` for a client made by
    /// `handshake`, whose transport we can't reopen.
    stream_from: Option<(SocketAddr, ClientBuilder)>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("features", &self.inner.features)
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub(crate) fn spawn<R, W>(
        raw: RawConnection<R, W>,
        queue: usize,
        default_timeout: Option<Duration>,
        stream_from: Option<(SocketAddr, ClientBuilder)>,
    ) -> Client
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let RawConnection {
            sender,
            receiver,
            features,
            ..
        } = raw;
        let (tx, rx) = mpsc::channel(queue);
        let (stop_tx, stop_rx) = oneshot::channel();
        let shared = Arc::new(Shared::default());

        tokio::spawn(dispatch::writer(sender, rx, shared.clone(), stop_tx));
        tokio::spawn(dispatch::reader(receiver, shared.clone(), stop_rx));

        Client {
            inner: Arc::new(Inner {
                tx,
                shared,
                next_id: AtomicI64::new(1),
                features,
                default_timeout,
                stream_from,
            }),
        }
    }

    /// Call `verb` with the default timeout and decode its reply.
    ///
    /// ```no_run
    /// # async fn f() -> seastar_rpc::rpc::Result<()> {
    /// use seastar_rpc::rpc::{Client, Verb};
    ///
    /// /// `int (int, int)`
    /// const ADD: Verb = 2;
    ///
    /// let client = Client::builder().connect("127.0.0.1:10000").await?;
    /// let sum: i32 = client.call(ADD, (2, 3)).await?;
    /// assert_eq!(sum, 5);
    /// # Ok(()) }
    /// ```
    ///
    /// Calls are pipelined: start several and await them together and they
    /// usually go out in one write. Use [`batch`](Self::batch) or
    /// [`call_many`](Self::call_many) to guarantee it.
    ///
    /// ```no_run
    /// # async fn f(client: seastar_rpc::rpc::Client) -> seastar_rpc::rpc::Result<()> {
    /// # const ADD: seastar_rpc::rpc::Verb = 2;
    /// use futures_util::future::try_join_all;
    ///
    /// let sums: Vec<i32> = try_join_all((0..10).map(|i| client.call(ADD, (i, 1)))).await?;
    /// # Ok(()) }
    /// ```
    pub async fn call<Req: Encode, Rep: Decode>(&self, verb: Verb, request: Req) -> Result<Rep> {
        self.call_with(verb, request, CallOptions::default()).await
    }

    /// Call `verb` with per-call options and decode its reply.
    ///
    /// ```no_run
    /// # async fn f(client: seastar_rpc::rpc::Client) -> seastar_rpc::rpc::Result<()> {
    /// use std::time::Duration;
    /// use seastar_rpc::rpc::{NO_WAIT, Verb, with_timeout};
    ///
    /// /// `double (double)`
    /// const SIN: Verb = 3;
    /// /// `rpc::no_wait_type ()`
    /// const NO_WAIT_VERB: Verb = 5;
    ///
    /// let y: f64 = client
    ///     .call_with(SIN, 1.0f64, with_timeout(Duration::from_millis(100)))
    ///     .await?;
    ///
    /// // Fire and forget: returns once the request is queued.
    /// client.call_with::<_, ()>(NO_WAIT_VERB, (), NO_WAIT).await?;
    /// # Ok(()) }
    /// ```
    pub async fn call_with<Req: Encode, Rep: Decode>(
        &self,
        verb: Verb,
        request: Req,
        opts: CallOptions,
    ) -> Result<Rep> {
        let reply = self.call_raw(verb, to_payload(&request), opts).await?;
        Ok(from_payload(reply)?)
    }

    /// Call `verb` with an already-encoded payload and wait for its raw reply
    /// payload.
    ///
    /// The returned future owns what it needs (`'static`), so it can be
    /// boxed or spawned. Dropping it before completion abandons the call: no
    /// cancel is sent (the protocol has none) and a late reply is discarded.
    ///
    /// ```no_run
    /// # async fn f(client: seastar_rpc::rpc::Client) -> Result<(), Box<dyn std::error::Error>> {
    /// use seastar_rpc::rpc::codec::serialize::{from_payload, to_payload};
    /// use seastar_rpc::rpc::{CallOptions, Verb};
    ///
    /// /// `sstring (sstring)`
    /// const ECHO: Verb = 15;
    ///
    /// let call = client.call_raw(ECHO, to_payload("hello"), CallOptions::default());
    /// let reply = tokio::spawn(call).await??;
    /// let echoed: String = from_payload(reply)?;
    /// # Ok(()) }
    /// ```
    pub fn call_raw(
        &self,
        verb: Verb,
        payload: impl Into<Bytes>,
        opts: CallOptions,
    ) -> impl Future<Output = Result<Bytes>> + Send + 'static {
        self.submit(verb, payload.into(), opts)
    }

    /// Start a batch: requests pushed to it are written back to back, with no
    /// other call's frame in between, and flushed together (a large batch may
    /// still take several socket writes). One queue slot, one lock and one
    /// timer serve the whole batch.
    ///
    /// Keep the batch and reuse it: its buffers survive across sends, so once
    /// warm it allocates nothing per send. Dropped, it returns them to the
    /// client's pool.
    ///
    /// ```no_run
    /// # async fn f(client: seastar_rpc::rpc::Client) -> Result<(), Box<dyn std::error::Error>> {
    /// use seastar_rpc::rpc::codec::serialize::from_payload;
    /// use seastar_rpc::rpc::Verb;
    ///
    /// /// `int (int, int)`
    /// const ADD: Verb = 2;
    /// /// `sstring (sstring)`
    /// const ECHO: Verb = 15;
    ///
    /// let mut batch = client.batch();
    /// for i in 0..3 {
    ///     batch.push(ADD, (i, 1));
    ///     batch.push(ECHO, "hi");
    ///     let mut replies = batch.send().await?;
    ///     let sum: i32 = from_payload(replies.next().unwrap()?)?;
    ///     let echoed: String = from_payload(replies.next().unwrap()?)?;
    /// }
    /// # Ok(()) }
    /// ```
    pub fn batch(&self) -> Batch {
        Batch::new(self.inner.clone())
    }

    /// Call `verb` once per request as one [`batch`](Self::batch), with the
    /// default timeout. Replies come back in request order, each decoded on
    /// its own. Uses pooled buffers, so it allocates nothing once warm.
    ///
    /// ```no_run
    /// # async fn f(client: seastar_rpc::rpc::Client) -> seastar_rpc::rpc::Result<()> {
    /// # const ADD: seastar_rpc::rpc::Verb = 2;
    /// for sum in client.call_many::<_, i32>(ADD, (0..10).map(|i| (i, 1))).await? {
    ///     println!("{}", sum?);
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn call_many<Req: Encode, Rep: Decode>(
        &self,
        verb: Verb,
        requests: impl IntoIterator<Item = Req>,
    ) -> Result<ManyReplies<Rep>> {
        self.call_many_with(verb, requests, CallOptions::default())
            .await
    }

    /// [`call_many`](Self::call_many) with options for the whole batch.
    pub async fn call_many_with<Req: Encode, Rep: Decode>(
        &self,
        verb: Verb,
        requests: impl IntoIterator<Item = Req>,
        opts: CallOptions,
    ) -> Result<ManyReplies<Rep>> {
        let mut batch = self.batch();
        for request in requests {
            batch.push(verb, request);
        }
        batch.send_decoded(opts).await
    }

    fn submit(
        &self,
        verb: Verb,
        payload: Bytes,
        opts: CallOptions,
    ) -> impl Future<Output = Result<Bytes>> + Send + 'static {
        let inner = self.inner.clone();
        async move {
            let deadline = inner.deadline(&opts);

            let request = Request {
                msg_id: inner.next_id.fetch_add(1, Ordering::Relaxed),
                verb,
                payload,
                deadline,
                ack: opts.no_wait_recv && !opts.no_wait_send,
            };
            let exchange = inner.exchange(request, &opts);

            match deadline {
                Some(d) => tokio::time::timeout_at(d, exchange)
                    .await
                    .unwrap_or(Err(Error::Timeout)),
                None => exchange.await,
            }
        }
    }

    /// Features the server accepted during negotiation.
    pub fn features(&self) -> &Features {
        &self.inner.features
    }

    /// Server-assigned id of this connection, if the server sent one.
    pub fn connection_id(&self) -> Option<u64> {
        self.inner.features.connection_id
    }

    /// Open a stream belonging to this connection: a new TCP connection to
    /// the same server, negotiated with the same builder settings. See
    /// [`stream`](crate::rpc::stream) for how to pass it to a verb and close
    /// it. Fails if the server sent no connection id (streaming is not
    /// configured there) or if this client was made by `handshake`; use
    /// [`ClientBuilder::handshake_stream`] for those.
    pub async fn open_stream(&self) -> Result<StreamConnection<OwnedReadHalf, OwnedWriteHalf>> {
        let parent = self.connection_id().ok_or_else(|| {
            Error::Protocol("server does not support streaming (no CONNECTION_ID)".into())
        })?;

        let (addr, builder) = self.inner.stream_from.clone().ok_or_else(|| {
            Error::Protocol(
                "client was not opened with connect(); use ClientBuilder::handshake_stream".into(),
            )
        })?;

        builder.connect_stream(addr, parent).await
    }

    /// Whether the connection has failed or been closed by the peer.
    pub fn is_closed(&self) -> bool {
        self.inner.shared.check_open().is_err()
    }
}

impl Inner {
    async fn exchange(&self, request: Request, opts: &CallOptions) -> Result<Bytes> {
        if opts.no_wait_send && opts.no_wait_recv {
            // Fire and forget: nothing will complete a slot, so don't register one.
            self.shared.check_open()?;
            return self
                .try_queue(Outgoing::One(request))
                .map(|()| Bytes::new());
        }

        let msg_id = request.msg_id;
        self.shared.register(msg_id)?;

        // Removes the pending entry if this future is dropped (timeout or
        // caller cancellation) before the reply arrives.
        let _guard = PendingGuard {
            shared: &self.shared,
            msg_id,
        };

        if opts.no_wait_send {
            self.try_queue(Outgoing::One(request))?;
        } else {
            self.tx
                .send(Outgoing::One(request))
                .await
                .map_err(|_| Error::Closed)?;
        }

        std::future::poll_fn(|cx| self.shared.poll_reply(msg_id, cx)).await
    }

    fn deadline(&self, opts: &CallOptions) -> Option<Instant> {
        opts.timeout
            .or(self.default_timeout)
            .and_then(|t| Instant::now().checked_add(t))
    }

    fn try_queue(&self, request: Outgoing) -> Result<()> {
        self.tx.try_send(request).map_err(|e| match e {
            TrySendError::Full(_) => Error::QueueFull,
            TrySendError::Closed(_) => Error::Closed,
        })
    }
}

struct PendingGuard<'a> {
    shared: &'a Shared,
    msg_id: i64,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.shared.forget(self.msg_id);
    }
}
