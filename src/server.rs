//! HTTP transport limits live here, independently of route/body admission.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::Router;
use hyper::server::conn::http1;
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, watch},
    task::{JoinError, JoinSet},
};

/// Serves HTTP on `listener` until `stop` is set, then waits for every open connection.
///
/// At most `max_connections` connections run at once. A socket accepted beyond that is closed
/// at once rather than queued. A connection that has not finished its request headers within
/// `header_timeout` is closed.
///
/// # Errors
///
/// Fails when accepting a connection fails.
pub async fn serve(
    listener: TcpListener,
    router: Router,
    max_connections: usize,
    header_timeout: Duration,
    mut stop: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let capacity = Arc::new(Semaphore::new(max_connections));
    let mut connections = JoinSet::new();

    loop {
        tokio::select! {
            biased;
            () = stopped(&mut stop) => break,
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                report_connection_exit(result);
            }

            accepted = listener.accept() => {
                let (stream, address) = accepted?;
                let policy = ConnectionPolicy {
                    router: &router,
                    header_timeout,
                    stop: &stop,
                };
                admit(&mut connections, &capacity, stream, address, &policy);
            }
        }
    }

    // The process supervisor supplies the overall shutdown deadline, including streaming bodies.
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            tracing::warn!(%error, "HTTP connection task failed during shutdown");
        }
    }

    Ok(())
}

fn report_connection_exit(result: Result<(), JoinError>) {
    if let Err(error) = result {
        tracing::warn!(%error, "HTTP connection task failed");
    }
}

/// Resolves once shutdown is requested. A dropped sender also means shutdown, so both cases
/// resolve.
async fn stopped(stop: &mut watch::Receiver<bool>) {
    // An error means every sender was dropped, which is a shutdown too.
    drop(stop.wait_for(|stopped| *stopped).await);
}

/// The settings every connection on a listener shares.
struct ConnectionPolicy<'a> {
    router: &'a Router,
    header_timeout: Duration,
    stop: &'a watch::Receiver<bool>,
}

/// Starts serving one accepted connection, or closes it when every connection slot is taken.
fn admit(
    connections: &mut JoinSet<()>,
    capacity: &Arc<Semaphore>,
    stream: TcpStream,
    address: SocketAddr,
    policy: &ConnectionPolicy<'_>,
) {
    let Ok(permit) = Arc::clone(capacity).try_acquire_owned() else {
        // Do not queue sockets or spawn unbounded tasks when transport capacity is full.
        drop(stream);
        tracing::debug!(%address, "HTTP connection capacity exhausted");
        return;
    };

    let service = TowerToHyperService::new(policy.router.clone());
    let header_timeout = policy.header_timeout;
    let mut connection_stop = policy.stop.clone();
    connections.spawn(async move {
        let _permit = permit;
        let mut builder = http1::Builder::new();
        builder
            .timer(TokioTimer::new())
            .header_read_timeout(header_timeout);
        let connection = builder.serve_connection(TokioIo::new(stream), service);
        tokio::pin!(connection);
        let result = tokio::select! {
            result = &mut connection => result,
            () = stopped(&mut connection_stop) => {
                connection.as_mut().graceful_shutdown();
                connection.await
            }
        };
        if let Err(error) = result {
            tracing::debug!(%address, %error, "HTTP connection closed");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    #[tokio::test]
    async fn stalled_headers_release_transport_capacity_and_shutdown_drains() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, receiver) = watch::channel(false);
        let task = tokio::spawn(serve(
            listener,
            Router::new().route("/", axum::routing::get(|| async { "ok" })),
            1,
            Duration::from_millis(150),
            receiver,
        ));
        let mut stalled = TcpStream::connect(address).await.unwrap();
        stalled.write_all(b"GET / HTTP/1.1\r\nHost:").await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;

        let mut excess = TcpStream::connect(address).await.unwrap();
        let mut byte = [0];
        let closed = tokio::time::timeout(Duration::from_secs(1), excess.read(&mut byte))
            .await
            .unwrap();
        assert!(
            matches!(closed, Ok(0) | Err(_)),
            "excess connections must not be queued"
        );

        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), stalled.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();

        // Hyper may emit 408 before closing; either way the capacity must return.
        let mut healthy = TcpStream::connect(address).await.unwrap();
        healthy
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut result = String::new();
        healthy.read_to_string(&mut result).await.unwrap();
        assert!(result.starts_with("HTTP/1.1 200"));

        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
