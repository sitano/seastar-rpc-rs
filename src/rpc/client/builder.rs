use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::rpc::client::Client;
use crate::rpc::codec::compress::Compression;
use crate::rpc::codec::frame::FrameLayout;
use crate::rpc::codec::negotiation::{Features, read_negotiation, write_negotiation};
use crate::rpc::error::{Error, Result};
use crate::rpc::raw::{RawConnection, RawReceiver, RawSender};
use crate::rpc::stream::{StreamConnection, StreamId};

/// Configures and opens client connections.
///
/// ```no_run
/// # async fn f() -> seastar_rpc::rpc::Result<()> {
/// use std::time::Duration;
/// let client = seastar_rpc::rpc::Client::builder()
///     .connect_timeout(Duration::from_secs(3))
///     .default_timeout(Duration::from_secs(5))
///     .connect("127.0.0.1:10000")
///     .await?;
/// # Ok(()) }
/// ```
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    connect_timeout: Option<Duration>,
    default_timeout: Option<Duration>,
    max_frame_size: usize,
    request_queue: usize,
    offer_timeout: bool,
    offer_handler_duration: bool,
    isolation_cookie: Option<Bytes>,
    compression: Vec<Compression>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        ClientBuilder {
            connect_timeout: None,
            default_timeout: None,
            max_frame_size: 16 * 1024 * 1024,
            request_queue: 1024,
            offer_timeout: true,
            offer_handler_duration: true,
            isolation_cookie: None,
            compression: Vec::new(),
        }
    }
}

impl ClientBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bound on TCP connect plus negotiation.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Timeout for calls that don't set their own. Without one, a call the
    /// server never answers (e.g. a payload it fails to decode) waits forever.
    pub fn default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = Some(timeout);
        self
    }

    /// Largest response payload accepted; bigger frames close the connection.
    pub fn max_frame_size(mut self, bytes: usize) -> Self {
        self.max_frame_size = bytes;
        self
    }

    /// Capacity of the queue between callers and the writer task. A full
    /// queue makes callers wait (backpressure).
    pub fn request_queue(mut self, capacity: usize) -> Self {
        self.request_queue = capacity.max(1);
        self
    }

    /// Offer the `TIMEOUT` feature (default on), so the server learns each
    /// call's remaining time budget.
    pub fn offer_timeout(mut self, on: bool) -> Self {
        self.offer_timeout = on;
        self
    }

    /// Offer the `HANDLER_DURATION` feature (default on).
    pub fn offer_handler_duration(mut self, on: bool) -> Self {
        self.offer_handler_duration = on;
        self
    }

    /// Offer an `ISOLATION` cookie selecting a server scheduling group.
    pub fn isolation_cookie(mut self, cookie: impl Into<Bytes>) -> Self {
        self.isolation_cookie = Some(cookie.into());
        self
    }

    /// Offer `COMPRESS` with these algorithms, most preferred first (default
    /// none). They are sent as a comma-separated list, e.g.
    /// `"LZ4,LZ4_FRAGMENTED"`, which Seastar's `multi_algo_compressor_factory`
    /// picks from; the server's choice then applies to every frame.
    pub fn offer_compression(mut self, algos: impl IntoIterator<Item = Compression>) -> Self {
        self.compression = algos.into_iter().collect();
        self
    }

    /// Connect over TCP and start a multiplexed [`Client`].
    pub async fn connect(self, addr: impl ToSocketAddrs) -> Result<Client> {
        let (raw, peer) = self
            .bounded(async {
                let stream = TcpStream::connect(addr).await?;
                stream.set_nodelay(true)?;

                // Streams are opened to the address this connection reached.
                let peer = stream.peer_addr()?;
                let (read, write) = stream.into_split();

                Ok((self.negotiate(read, write).await?, peer))
            })
            .await?;

        let queue = self.request_queue;
        let default_timeout = self.default_timeout;

        Ok(Client::spawn(
            raw,
            queue,
            default_timeout,
            Some((peer, self)),
        ))
    }

    /// Connect over TCP and return the raw halves, without a dispatcher.
    /// `request_queue` and `default_timeout` only apply to a [`Client`] and
    /// are ignored here.
    pub async fn connect_raw(
        self,
        addr: impl ToSocketAddrs,
    ) -> Result<RawConnection<OwnedReadHalf, OwnedWriteHalf>> {
        self.bounded(async {
            let stream = TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;

            let (read, write) = stream.into_split();

            self.negotiate(read, write).await
        })
        .await
    }

    /// Negotiate over an already-open transport and start a [`Client`].
    pub async fn handshake<R, W>(self, read: R, write: W) -> Result<Client>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let raw = self.clone().handshake_raw(read, write).await?;
        Ok(Client::spawn(
            raw,
            self.request_queue,
            self.default_timeout,
            None,
        ))
    }

    /// Negotiate over an already-open transport and return the raw halves.
    pub async fn handshake_raw<R, W>(self, read: R, write: W) -> Result<RawConnection<R, W>>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        self.bounded(self.negotiate(read, write)).await
    }

    /// Open a stream connection belonging to the open connection `parent`
    /// (its [`Client::connection_id`]); see [`stream`](crate::rpc::stream).
    /// [`Client::open_stream`] does this for a client made with
    /// [`connect`](Self::connect).
    pub async fn connect_stream(
        self,
        addr: impl ToSocketAddrs,
        parent: u64,
    ) -> Result<StreamConnection<OwnedReadHalf, OwnedWriteHalf>> {
        self.bounded(async {
            let stream = TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;

            let (read, write) = stream.into_split();

            self.negotiate_stream(read, write, parent).await
        })
        .await
    }

    /// Negotiate a stream connection belonging to `parent` over an
    /// already-open transport.
    pub async fn handshake_stream<R, W>(
        self,
        read: R,
        write: W,
        parent: u64,
    ) -> Result<StreamConnection<R, W>>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        self.bounded(self.negotiate_stream(read, write, parent))
            .await
    }

    fn offer(&self, stream_parent: Option<u64>) -> Features {
        Features {
            // Stream frames have no timeout field, so Seastar doesn't offer
            // it on a stream connection either.
            timeout: self.offer_timeout && stream_parent.is_none(),
            handler_duration: self.offer_handler_duration,
            isolation: self.isolation_cookie.clone(),
            compress: (!self.compression.is_empty()).then(|| {
                let names: Vec<&str> = self.compression.iter().map(|c| c.name()).collect();
                Bytes::from(names.join(","))
            }),
            stream_parent,
            ..Features::default()
        }
    }

    async fn negotiate<R, W>(&self, mut read: R, mut write: W) -> Result<RawConnection<R, W>>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        let (features, compression) = self.exchange(&mut read, &mut write, None).await?;

        let layout = FrameLayout::new(&features, self.max_frame_size);
        Ok(RawConnection {
            sender: RawSender::new(write, layout, compression),
            receiver: RawReceiver::new(read, layout, compression),
            features,
        })
    }

    async fn negotiate_stream<R, W>(
        &self,
        mut read: R,
        mut write: W,
        parent: u64,
    ) -> Result<StreamConnection<R, W>>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        let (features, compression) = self.exchange(&mut read, &mut write, Some(parent)).await?;

        // The stream's own id is what calls carry; without it the stream
        // can't be named.
        let id = features.connection_id.ok_or_else(|| {
            Error::Protocol("server sent no CONNECTION_ID on stream connection".into())
        })?;

        let layout = FrameLayout::new(&features, self.max_frame_size);
        Ok(StreamConnection::new(
            read,
            write,
            layout,
            compression,
            StreamId(id),
        ))
    }

    /// Send our offer, read the server's reply and resolve its compression.
    async fn exchange<R, W>(
        &self,
        read: &mut R,
        write: &mut W,
        stream_parent: Option<u64>,
    ) -> Result<(Features, Option<Compression>)>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        write_negotiation(write, &self.offer(stream_parent)).await?;
        let features = read_negotiation(read).await?;
        // The server's reply decides the frame layout and compression. An
        // algorithm we didn't offer would desync us.
        let compression = match &features.compress {
            None => None,
            Some(name) => Some(
                Compression::from_name(name)
                    .filter(|c| self.compression.contains(c))
                    .ok_or_else(|| {
                        Error::Protocol(format!(
                            "server chose compression {:?}, which was not offered",
                            String::from_utf8_lossy(name)
                        ))
                    })?,
            ),
        };

        Ok((features, compression))
    }

    async fn bounded<T>(&self, fut: impl Future<Output = Result<T>>) -> Result<T> {
        match self.connect_timeout {
            Some(t) => tokio::time::timeout(t, fut)
                .await
                .map_err(|_| Error::Timeout)?,
            None => fut.await,
        }
    }
}
