//! Multiplexed client: many concurrent calls over one connection, replies
//! matched to callers by `msg_id`.

mod builder;
mod dispatch;

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

pub use builder::ClientBuilder;
use dispatch::{Outgoing, Shared};

use crate::rpc::codec::negotiation::Features;
use crate::rpc::codec::serialize::{Decode, Encode, from_payload, to_payload};
use crate::rpc::error::{Error, Result};
use crate::rpc::raw::RawConnection;
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
    ) -> Client
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (sender, receiver, features) = raw.into_parts();
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
    /// Calls are pipelined: start several and await them together to send
    /// them as a batch over the connection.
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

    fn submit(
        &self,
        verb: Verb,
        payload: Bytes,
        opts: CallOptions,
    ) -> impl Future<Output = Result<Bytes>> + Send + 'static {
        let inner = self.inner.clone();
        async move {
            let deadline = opts
                .timeout
                .or(inner.default_timeout)
                .and_then(|t| Instant::now().checked_add(t));

            let request = Outgoing {
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

    /// Whether the connection has failed or been closed by the peer.
    pub fn is_closed(&self) -> bool {
        self.inner.shared.check_open().is_err()
    }
}

impl Inner {
    async fn exchange(&self, request: Outgoing, opts: &CallOptions) -> Result<Bytes> {
        if opts.no_wait_send && opts.no_wait_recv {
            // Fire and forget: nothing will complete a slot, so don't register one.
            self.shared.check_open()?;
            return self.try_queue(request).map(|()| Bytes::new());
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
            self.try_queue(request)?;
        } else {
            self.tx.send(request).await.map_err(|_| Error::Closed)?;
        }

        std::future::poll_fn(|cx| self.shared.poll_reply(msg_id, cx)).await
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
