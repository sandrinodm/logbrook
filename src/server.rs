//! HTTP transport limits live here, independently of route/body admission.

use std::{sync::Arc, time::Duration};

use axum::Router;
use hyper::server::conn::http1;
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
    task::JoinSet,
};

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
            _ = async {
                let _ = stop.wait_for(|stopped| *stopped).await;
            } => break,
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    tracing::warn!(%error, "HTTP connection task failed");
                }
            }

            accepted = listener.accept() => {
                let (stream, address) = accepted?;
                let Ok(permit) = capacity.clone().try_acquire_owned() else {
                    // Do not queue sockets or spawn unbounded tasks when transport capacity is full.
                    drop(stream);
                    tracing::debug!(%address, "HTTP connection capacity exhausted");
                    continue;
                };
                let service = TowerToHyperService::new(router.clone());
                let mut connection_stop = stop.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    let mut builder = http1::Builder::new();
                    builder.timer(TokioTimer::new()).header_read_timeout(header_timeout);
                    let connection = builder.serve_connection(TokioIo::new(stream), service);
                    tokio::pin!(connection);
                    let result = tokio::select! {
                        result = &mut connection => result,
                        _ = async {
                            let _ = connection_stop.wait_for(|stopped| *stopped).await;
                        } => {
                            connection.as_mut().graceful_shutdown();
                            connection.await
                        }
                    };

                    if let Err(error) = result {
                        tracing::debug!(%address, %error, "HTTP connection closed");
                    }
                });
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
