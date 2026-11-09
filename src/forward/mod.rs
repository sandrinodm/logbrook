//! Native Fluent Forward ingress. Bind every required listener before serving.
pub mod codec;
pub mod config;
pub mod decode;
pub mod docker;
pub mod metrics;
pub mod normalize;
pub mod protocol;
pub mod routing;
pub mod security;
pub mod session;

use crate::{indexes::IndexRegistry, model::Error};
use config::{ForwardConfig, ListenerConfig, SecurityMode};
use security::Security;
use session::{BoxIo, Budgets, SessionContext};
#[cfg(unix)]
use std::{
    fs::symlink_metadata,
    path::{Path, PathBuf},
};
use std::{
    future::Future,
    net::{IpAddr, SocketAddr},
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::{
    net::{TcpListener, UdpSocket},
    sync::{OwnedSemaphorePermit, Semaphore, watch},
    task::{JoinError, JoinHandle, JoinSet},
    time::Instant,
};
use tokio_rustls::server::TlsStream;

/// Bytes read from a socket per call. Each read reserves this many bytes of the raw budget first.
const READ_SCRATCH_BYTES: usize = 1024;

/// A frame's bytes are reserved this many times over before they are appended: once for the
/// buffer's capacity, once for the copy made when it grows, and once for the frame itself.
const FRAME_PEAK_MULTIPLIER: usize = 3;

/// Permission bits a Unix socket's `unix_mode` may set: owner and group access, never other.
const UNIX_MODE_ALLOWED_BITS: u32 = 0o770;

/// Permission bits a Unix socket's `unix_mode` must set: owner read and write.
const UNIX_MODE_REQUIRED_BITS: u32 = 0o600;

/// The bit that makes a directory writable by every user.
const WORLD_WRITE_BIT: u32 = 0o002;

/// The owner that a Unix socket's parent directory may have besides the process's own user.
const ROOT_UID: u32 = 0;

/// Session failures whose diagnostic is logged: the first few, then each power of two.
const LOGGED_SESSION_ERRORS: u64 = 8;

/// A heartbeat request is one byte, and the response is the same byte.
const HEARTBEAT_BYTE: u8 = 0;

/// Length of a UDP heartbeat request. Datagrams of any other length are ignored.
const HEARTBEAT_REQUEST_BYTES: usize = 1;

/// Period over which heartbeat responses are counted toward `HEARTBEAT_RESPONSES_PER_WINDOW`.
const HEARTBEAT_WINDOW: Duration = Duration::from_secs(1);

/// Most heartbeat responses a listener sends per `HEARTBEAT_WINDOW`, across all peers. Requests
/// over the limit go unanswered.
const HEARTBEAT_RESPONSES_PER_WINDOW: usize = 10;

/// Error returned when a bound TCP listener's address cannot be read, from
/// [`Receiver::local_addresses`] or when binding its UDP heartbeat.
const LOCAL_ADDRESS_UNAVAILABLE: &str = "Forward local address unavailable";

/// Error a listener returns when one of its connection tasks panics or is cancelled.
const CONNECTION_TASK_FAILED: &str = "Forward connection task failed";

/// Error a listener returns when its heartbeat task panics or is cancelled.
const HEARTBEAT_TASK_FAILED: &str = "Forward heartbeat task failed";

// Messages of the TLS and certificate refusals. Each one is counted as an authentication error
// and closes the connection, whose failure log records the reason code named below.

/// The TLS handshake did not finish within `handshake_timeout_ms` (reason
/// `tls_handshake_deadline`).
const TLS_DEADLINE: &str = "Forward TLS deadline";

/// The TLS handshake failed (reason `tls_handshake_failed`).
const TLS_FAILED: &str = "Forward TLS failed";

/// An `mtls` client presented no certificate (reason `client_certificate_required`).
const CLIENT_CERTIFICATE_REQUIRED: &str = "Forward client certificate required";

/// The outcome of an operation that shutdown can interrupt.
enum Step<T> {
    /// The operation finished with this value.
    Ready(T),
    /// Shutdown was requested first, and the operation was dropped.
    Stopped,
}

/// Resolves once shutdown has been requested, or once every sender is gone, which is a shutdown
/// too.
async fn stopped(stop: &mut watch::Receiver<bool>) {
    drop(stop.wait_for(|stopped| *stopped).await);
}

/// Runs `work` until it finishes or shutdown is requested. Shutdown wins a tie, so a stopping
/// receiver starts no new work.
async fn until_stopped<T>(
    stop: &mut watch::Receiver<bool>,
    work: impl Future<Output = T>,
) -> Step<T> {
    tokio::select! {
        biased;
        () = stopped(stop) => Step::Stopped,
        output = work => Step::Ready(output),
    }
}

/// A listening socket that has been bound.
enum BoundSocket {
    Tcp(TcpListener),
    #[cfg(unix)]
    Unix(tokio::net::UnixListener, UnixPath),
}

/// A listener that has bound its socket and is ready to serve.
struct BoundListener {
    socket: BoundSocket,
    config: Arc<ListenerConfig>,
    security: Arc<Security>,
    heartbeat: Option<UdpSocket>,
}

/// Bound Forward listeners, ready to be served with [`Receiver::serve`].
pub struct Receiver {
    listeners: Vec<BoundListener>,
    config: Arc<ForwardConfig>,
    registry: IndexRegistry,
    budgets: Arc<Budgets>,
}

/// What every listener task shares with the receiver.
#[derive(Clone)]
struct Shared {
    config: Arc<ForwardConfig>,
    registry: IndexRegistry,
    budgets: Arc<Budgets>,
    /// Limits the connections open at once across all listeners.
    capacity: Arc<Semaphore>,
    stop: watch::Receiver<bool>,
}

impl Receiver {
    /// Bound TCP addresses, including the addresses assigned to listeners configured with port
    /// zero.
    ///
    /// # Errors
    ///
    /// Returns an unavailable error when a bound address cannot be read.
    pub fn local_addresses(&self) -> Result<Vec<SocketAddr>, Error> {
        self.listeners
            .iter()
            .filter_map(|listener| match &listener.socket {
                BoundSocket::Tcp(socket) => Some(socket.local_addr().map_err(|error| {
                    Error::unavailable(LOCAL_ADDRESS_UNAVAILABLE).with_source(error)
                })),
                #[cfg(unix)]
                BoundSocket::Unix(_, _) => None,
            })
            .collect()
    }

    /// Binds every listener of an enabled receiver. Nothing accepts a connection until
    /// [`Receiver::serve`] runs.
    ///
    /// # Errors
    ///
    /// Returns an invalid or unavailable error when a listener's security settings cannot be
    /// loaded or its socket cannot be bound.
    pub async fn bind(config: ForwardConfig, registry: IndexRegistry) -> Result<Self, Error> {
        let budgets = Arc::new(Budgets::new(&config));
        let mut listeners = Vec::new();
        if config.enabled {
            for listener in &config.listeners {
                listeners.push(BoundListener::bind(listener).await?);
            }
        }

        Ok(Self {
            listeners,
            config: Arc::new(config),
            registry,
            budgets,
        })
    }

    /// Serves every listener until `stop` becomes true, then waits for their connections to end.
    ///
    /// # Errors
    ///
    /// Returns an unavailable error when a listener, heartbeat or connection task fails.
    pub async fn serve(self, stop: watch::Receiver<bool>) -> Result<(), Error> {
        let shared = Shared {
            config: Arc::clone(&self.config),
            registry: self.registry.clone(),
            budgets: Arc::clone(&self.budgets),
            capacity: Arc::new(Semaphore::new(self.config.max_connections)),
            stop: stop.clone(),
        };

        let mut tasks = JoinSet::new();
        for listener in self.listeners {
            tasks.spawn(serve_listener(listener, shared.clone()));
        }

        if tasks.is_empty() {
            let mut stop = stop;
            stopped(&mut stop).await;
            return Ok(());
        }

        while let Some(result) = tasks.join_next().await {
            result.map_err(|error| {
                Error::unavailable("Forward listener task failed").with_source(error)
            })??;
        }
        Ok(())
    }
}

impl BoundListener {
    /// Loads the listener's security settings and binds its socket.
    async fn bind(listener: &ListenerConfig) -> Result<Self, Error> {
        let security = Arc::new(Security::load(listener)?);
        let (socket, heartbeat) = match listener.bind {
            Some(address) => {
                let tcp = TcpListener::bind(address).await.map_err(|error| {
                    Error::invalid("cannot bind required Forward TCP listener").with_source(error)
                })?;
                let heartbeat = if listener.udp_heartbeat {
                    Some(bind_heartbeat(&tcp).await?)
                } else {
                    None
                };
                (BoundSocket::Tcp(tcp), heartbeat)
            }
            None => (bind_local_socket(listener)?, None),
        };
        Ok(Self {
            socket,
            config: Arc::new(listener.clone()),
            security,
            heartbeat,
        })
    }
}

/// Binds the UDP heartbeat on the same address as the TCP listener.
async fn bind_heartbeat(tcp: &TcpListener) -> Result<UdpSocket, Error> {
    let address = tcp
        .local_addr()
        .map_err(|error| Error::unavailable(LOCAL_ADDRESS_UNAVAILABLE).with_source(error))?;
    UdpSocket::bind(address).await.map_err(|error| {
        Error::invalid("cannot bind required Forward UDP heartbeat").with_source(error)
    })
}

#[cfg(unix)]
fn bind_local_socket(listener: &ListenerConfig) -> Result<BoundSocket, Error> {
    let (socket, path) = bind_unix(listener)?;
    Ok(BoundSocket::Unix(socket, path))
}

#[cfg(not(unix))]
fn bind_local_socket(_listener: &ListenerConfig) -> Result<BoundSocket, Error> {
    Err(Error::invalid(
        "Unix Forward sockets unavailable on this platform",
    ))
}

/// Accepts connections on one listener until shutdown. Then waits for its heartbeat and its
/// connections to finish.
async fn serve_listener(mut listener: BoundListener, mut shared: Shared) -> Result<(), Error> {
    let mut connections = JoinSet::new();
    let mut heartbeat = listener.heartbeat.take().map(|socket| {
        tokio::spawn(run_heartbeat(
            socket,
            Arc::clone(&listener.config),
            shared.stop.clone(),
        ))
    });
    loop {
        tokio::select! {
            biased;
            () = stopped(&mut shared.stop) => break,
            Some(joined) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = joined {
                    return Err(Error::unavailable(CONNECTION_TASK_FAILED).with_source(error));
                }
            }
            result = heartbeat_finished(&mut heartbeat), if heartbeat.is_some() => {
                return Err(unexpected_heartbeat_exit(result));
            }
            accepted = accept(&listener.socket, &listener.config.allow_peers) => {
                admit_connection(&mut connections, &listener, &shared, accepted)?;
            }
        }
    }

    if let Some(heartbeat) = heartbeat.take() {
        heartbeat
            .await
            .map_err(|error| Error::unavailable(HEARTBEAT_TASK_FAILED).with_source(error))??;
    }

    while let Some(result) = connections.join_next().await {
        result.map_err(|error| {
            Error::unavailable("Forward connection task failed during drain").with_source(error)
        })?;
    }
    Ok(())
}

/// Serves an accepted connection when the capacity and the peer allow it. Refused connections
/// are counted as rejected.
fn admit_connection(
    connections: &mut JoinSet<()>,
    listener: &BoundListener,
    shared: &Shared,
    accepted: std::io::Result<Option<BoxIo>>,
) -> Result<(), Error> {
    let Some(stream) =
        accepted.map_err(|error| Error::unavailable("Forward accept failed").with_source(error))?
    else {
        metrics::increment(&metrics::REJECTED);
        return Ok(());
    };
    let Ok(permit) = Arc::clone(&shared.capacity).try_acquire_owned() else {
        metrics::increment(&metrics::REJECTED);
        drop(stream);
        return Ok(());
    };
    metrics::increment(&metrics::ACCEPTED);
    spawn_connection(connections, listener, shared, permit, stream);
    Ok(())
}

/// Maps an unexpectedly completed heartbeat task to its established listener error.
fn unexpected_heartbeat_exit(result: Result<Result<(), Error>, JoinError>) -> Error {
    match result {
        Ok(Ok(())) => Error::unavailable("Forward heartbeat stopped unexpectedly"),
        Ok(Err(error)) => error,
        Err(error) => Error::unavailable(HEARTBEAT_TASK_FAILED).with_source(error),
    }
}

/// Waits for the heartbeat task. It is polled only while the task exists.
async fn heartbeat_finished(
    task: &mut Option<JoinHandle<Result<(), Error>>>,
) -> Result<Result<(), Error>, JoinError> {
    task.as_mut()
        .expect("heartbeat task exists while its branch is enabled")
        .await
}

/// Accepts the next connection. Yields `None` for a TCP peer outside the allow list.
async fn accept(socket: &BoundSocket, allow_peers: &[IpAddr]) -> std::io::Result<Option<BoxIo>> {
    match socket {
        BoundSocket::Tcp(tcp) => {
            let (stream, peer) = tcp.accept().await?;
            if !peer_allowed(allow_peers, peer.ip()) {
                return Ok(None);
            }

            stream.set_nodelay(true)?;
            let io: BoxIo = Box::new(stream);
            Ok(Some(io))
        }
        #[cfg(unix)]
        BoundSocket::Unix(unix, _path) => {
            let (stream, _) = unix.accept().await?;
            let io: BoxIo = Box::new(stream);
            Ok(Some(io))
        }
    }
}

/// Whether `peer` may connect. An empty allow list admits every peer.
fn peer_allowed(allow_peers: &[IpAddr], peer: IpAddr) -> bool {
    allow_peers.is_empty() || allow_peers.contains(&peer)
}

/// Starts serving an accepted connection. The connection holds its capacity permit until it ends.
fn spawn_connection(
    connections: &mut JoinSet<()>,
    listener: &BoundListener,
    shared: &Shared,
    permit: OwnedSemaphorePermit,
    stream: BoxIo,
) {
    let session = SessionContext {
        listener: Arc::clone(&listener.config),
        security: Arc::clone(&listener.security),
        config: Arc::clone(&shared.config),
        registry: shared.registry.clone(),
        budgets: Arc::clone(&shared.budgets),
        stop: shared.stop.clone(),
    };
    connections.spawn(async move {
        let _permit = permit;
        let _active = metrics::GaugeGuard::new(&metrics::CONNECTIONS, 1);
        let listener = Arc::clone(&session.listener);
        if let Err(error) = serve_connection(stream, session).await {
            log_session_error(&listener.name, &error);
        }
    });
}

/// Runs one connection: the TLS handshake when the listener uses TLS, then the session.
async fn serve_connection(io: BoxIo, mut session: SessionContext) -> Result<(), Error> {
    match establish_transport(io, &mut session).await? {
        Transport::Stopped => Ok(()),
        Transport::Ready { io, principal } => session::run(io, principal, session).await,
    }
}

/// What the transport step leaves for the session.
enum Transport {
    /// Shutdown interrupted the TLS handshake, so no session starts.
    Stopped,
    /// The connection is ready, with the principal it was granted.
    Ready {
        io: BoxIo,
        principal: Option<String>,
    },
}

/// Completes the TLS handshake when the listener uses TLS. Returns the principal the connection
/// is granted: the listener's, or the one its client certificate maps to.
async fn establish_transport(io: BoxIo, session: &mut SessionContext) -> Result<Transport, Error> {
    let Some(acceptor) = &session.security.tls else {
        let principal = session.security.principal.clone();
        return Ok(Transport::Ready { io, principal });
    };

    let deadline = Duration::from_millis(session.config.handshake_timeout_ms);
    let handshake = tokio::time::timeout(deadline, acceptor.accept(io));
    let tls = match until_stopped(&mut session.stop, handshake).await {
        Step::Stopped => return Ok(Transport::Stopped),
        Step::Ready(result) => result
            .map_err(|error| {
                authentication_error(TLS_DEADLINE, "tls_handshake_deadline").with_source(error)
            })?
            .map_err(|error| {
                authentication_error(TLS_FAILED, "tls_handshake_failed").with_source(error)
            })?,
    };

    let principal = if session.security.mode == SecurityMode::Mtls {
        Some(certificate_principal(&session.security, &tls)?)
    } else {
        session.security.principal.clone()
    };

    Ok(Transport::Ready {
        io: Box::new(tls),
        principal,
    })
}

/// The principal a client certificate maps to. A certificate with no mapping is an authentication
/// failure.
fn certificate_principal(security: &Security, tls: &TlsStream<BoxIo>) -> Result<String, Error> {
    let certificates = tls.get_ref().1.peer_certificates().ok_or_else(|| {
        authentication_error(CLIENT_CERTIFICATE_REQUIRED, "client_certificate_required")
    })?;
    let leaf = certificates.first().ok_or_else(|| {
        authentication_error(CLIENT_CERTIFICATE_REQUIRED, "client_certificate_required")
    })?;
    security
        .certificate_principal(leaf.as_ref())
        .inspect_err(|_| metrics::increment(&metrics::AUTH_ERRORS))
}

/// Counts an authentication failure and returns it with `message` and `reason`.
fn authentication_error(message: &'static str, reason: &'static str) -> Error {
    metrics::increment(&metrics::AUTH_ERRORS);
    Error::invalid(message).with_reason(reason)
}

/// Logs a failed session. The first few failures are logged, then one in each power of two, so a
/// reconnect storm does not flood the log.
fn log_session_error(listener: &str, error: &Error) {
    let diagnostic_id = metrics::SESSION_ERRORS
        .fetch_add(1, Ordering::Relaxed)
        .wrapping_add(1);
    if diagnostic_id <= LOGGED_SESSION_ERRORS || diagnostic_id.is_power_of_two() {
        tracing::warn!(
            listener = %listener,
            reason_code = %diagnostic_reason(error),
            diagnostic_id,
            "Forward session closed"
        );
    }
}

/// Answers one-byte UDP heartbeats from allowed peers, up to a fixed number per window.
async fn run_heartbeat(
    socket: UdpSocket,
    config: Arc<ListenerConfig>,
    mut stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    // One byte more than a request, so an oversized datagram is visibly longer than one byte.
    let mut buffer = [0u8; HEARTBEAT_REQUEST_BYTES + 1];
    let mut window_start = Instant::now();
    let mut responses = 0usize;
    loop {
        let received = tokio::select! {
            biased;
            () = stopped(&mut stop) => return Ok(()),
            result = socket.recv_from(&mut buffer) => result,
        };
        let (count, peer) = received.map_err(|error| {
            Error::unavailable("Forward heartbeat socket failed").with_source(error)
        })?;
        if count != HEARTBEAT_REQUEST_BYTES
            || buffer[0] != HEARTBEAT_BYTE
            || !peer_allowed(&config.allow_peers, peer.ip())
        {
            continue;
        }

        if window_start.elapsed() >= HEARTBEAT_WINDOW {
            window_start = Instant::now();
            responses = 0;
        }

        if responses >= HEARTBEAT_RESPONSES_PER_WINDOW {
            continue;
        }

        responses += 1;
        if *stop.borrow() {
            return Ok(());
        }

        if socket.send_to(&[HEARTBEAT_BYTE], peer).await.is_ok() {
            metrics::increment(&metrics::HEARTBEATS);
        } else {
            metrics::increment(&metrics::HEARTBEAT_FAILED);
        }
    }
}

/// The path of a bound Unix socket. The file is removed on drop, but only while it is still the
/// socket this listener created.
#[cfg(unix)]
struct UnixPath {
    path: PathBuf,
    device: u64,
    inode: u64,
}

#[cfg(unix)]
impl Drop for UnixPath {
    fn drop(&mut self) {
        use std::os::unix::fs::{FileTypeExt, MetadataExt};
        let Ok(metadata) = symlink_metadata(&self.path) else {
            return;
        };
        let is_bound_socket = metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode;
        if is_bound_socket {
            drop(std::fs::remove_file(&self.path));
        }
    }
}

/// Binds the Unix socket at the listener's path, after checking that its directory is trusted and
/// the path is free.
#[cfg(unix)]
fn bind_unix(config: &ListenerConfig) -> Result<(tokio::net::UnixListener, UnixPath), Error> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let path = config
        .unix_path
        .as_ref()
        .ok_or_else(|| Error::invalid("Forward Unix path missing"))?;
    let mode = config
        .unix_mode
        .ok_or_else(|| Error::invalid("Forward Unix mode missing"))?;
    check_unix_parent(path)?;
    check_unix_path_free(path)?;

    let listener = tokio::net::UnixListener::bind(path).map_err(|error| {
        Error::invalid("cannot bind required Forward Unix socket").with_source(error)
    })?;

    let metadata = symlink_metadata(path).map_err(|error| {
        Error::invalid("cannot inspect bound Forward Unix socket").with_source(error)
    })?;
    let cleanup = UnixPath {
        path: path.clone(),
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    if metadata.uid() != effective_uid() {
        return Err(Error::invalid("Forward Unix socket ownership mismatch"));
    }

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).map_err(|error| {
        Error::invalid("cannot set Forward Unix permissions").with_source(error)
    })?;

    Ok((listener, cleanup))
}

/// The directory of a Unix socket must be a directory owned by this user or root, and must not be
/// writable by everyone.
#[cfg(unix)]
fn check_unix_parent(path: &Path) -> Result<(), Error> {
    use std::os::unix::fs::MetadataExt;
    let parent = path
        .parent()
        .ok_or_else(|| Error::invalid("Forward Unix parent missing"))?;
    let metadata = std::fs::metadata(parent)
        .map_err(|error| Error::invalid("Forward Unix parent unavailable").with_source(error))?;
    let owner = metadata.uid();
    let trusted_owner = owner == effective_uid() || owner == ROOT_UID;
    if !metadata.is_dir() || !trusted_owner || metadata.mode() & WORLD_WRITE_BIT != 0 {
        return Err(Error::invalid(
            "Forward Unix parent must have trusted ownership and no world write permission",
        ));
    }

    Ok(())
}

/// A stale socket from an earlier run must be removed by an operator, not replaced silently.
#[cfg(unix)]
fn check_unix_path_free(path: &Path) -> Result<(), Error> {
    match symlink_metadata(path) {
        Ok(_) => Err(Error::invalid(concat!(
            "Forward Unix path already exists; verify ownership and ",
            "remove stale socket before restart",
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(Error::invalid("Forward Unix path cannot be inspected").with_source(error))
        }
    }
}

#[cfg(unix)]
fn effective_uid() -> u32 {
    rustix::process::geteuid().as_raw()
}

/// The reason code for a session failure, or the fallback for its error kind.
fn diagnostic_reason(error: &Error) -> &str {
    error
        .reason
        .as_deref()
        .unwrap_or_else(|| kind_reason(error))
}

/// The fallback reason code for a failure that carries no explicit reason.
fn kind_reason(error: &Error) -> &'static str {
    match error.kind {
        crate::model::ErrorKind::Overloaded => "storage_capacity",
        crate::model::ErrorKind::TooLarge => "normalized_or_storage_limit",
        crate::model::ErrorKind::Timeout => "storage_deadline",
        crate::model::ErrorKind::Invalid => "event_policy_rejected",
        _ => "storage_or_transport_unavailable",
    }
}
