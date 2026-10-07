//! Real HTTP regressions exercise the wire format, bounded body reads and accounting.
use super::{http::Http, index, mixed};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    task::JoinHandle,
};

type Requests = Arc<Mutex<Vec<(String, String, Vec<u8>)>>>;

struct Server {
    base: String,
    requests: Requests,
    task: JoinHandle<()>,
    connections: Arc<AtomicU64>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn line(socket: &mut TcpStream) -> Option<String> {
    let mut bytes = Vec::new();
    loop {
        let mut byte = [0];
        if socket.read_exact(&mut byte).await.is_err() {
            return None;
        }
        bytes.push(byte[0]);
        if bytes.ends_with(b"\r\n") {
            return String::from_utf8(bytes).ok();
        }
        if bytes.len() > 65536 {
            return None;
        }
    }
}

async fn fixture(truncated: bool) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let seen = requests.clone();
    let connections = Arc::new(AtomicU64::new(0));
    let accepted = connections.clone();
    let (broadcast, _) = tokio::sync::broadcast::channel::<(String, Value)>(1024);
    let records = Arc::new(Mutex::new(Vec::<Value>::new()));
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            accepted.fetch_add(1, Ordering::Relaxed);
            let seen = seen.clone();
            let broadcast = broadcast.clone();
            let records = records.clone();
            tokio::spawn(async move {
                while let Some(first) = line(&mut socket).await {
                    let mut headers = String::new();
                    while let Some(header) = line(&mut socket).await {
                        if header == "\r\n" {
                            break;
                        }
                        headers.push_str(&header);
                    }
                    let length = headers
                        .lines()
                        .find_map(|h| {
                            h.to_lowercase()
                                .strip_prefix("content-length:")
                                .map(str::trim)
                                .and_then(|v| v.parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    let mut body = vec![0; length];
                    if socket.read_exact(&mut body).await.is_err() {
                        break;
                    }
                    if headers
                        .to_lowercase()
                        .contains("transfer-encoding: chunked")
                    {
                        loop {
                            let Some(chunk) = line(&mut socket).await else {
                                return;
                            };
                            let size =
                                usize::from_str_radix(chunk.trim().split(';').next().unwrap(), 16)
                                    .unwrap();
                            if size == 0 {
                                let _ = line(&mut socket).await;
                                break;
                            }
                            let mut bytes = vec![0; size];
                            if socket.read_exact(&mut bytes).await.is_err() {
                                return;
                            }
                            body.extend(bytes);
                            let _ = line(&mut socket).await;
                        }
                    }
                    let path = first.split_whitespace().nth(1).unwrap_or("/").to_string();
                    seen.lock()
                        .await
                        .push((first.clone(), headers.clone(), body.clone()));
                    if path == "/redirect" {
                        let _=socket.write_all(b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /logs/ingest\r\nContent-Length: 2\r\n\r\n{}").await;
                        continue;
                    }
                    if path.contains("/logs/tail") {
                        let index = path
                            .split('/')
                            .nth(2)
                            .filter(|_| path.starts_with("/indexes/"))
                            .unwrap_or("default")
                            .to_string();
                        let mut changes = broadcast.subscribe();
                        let _=socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n").await;
                        while let Ok((event_index, event)) = changes.recv().await {
                            if event_index != index {
                                continue;
                            }
                            let id = event["id"].as_str().unwrap();
                            let frame = format!("event: log\nid: {index}:{id}\ndata: {event}\n\n");
                            if socket.write_all(frame.as_bytes()).await.is_err() {
                                break;
                            }
                        }
                        break;
                    }
                    if truncated && path != "/metrics" && path != "/ready" {
                        let _ = socket
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{")
                            .await;
                        break;
                    }
                    let text = if path == "/metrics" {
                        "logbrook_storage_archive_files{index=\"default\"} 0\nlogbrook_storage_wal_bytes{index=\"default\"} 100\nlogbrook_test{index=\"payments\"} 0\nlogbrook_test{index=\"orders\"} 0\n".to_string()
                    } else if first.starts_with("POST") {
                        let events: Vec<Value> =
                            if headers.to_lowercase().contains("application/x-ndjson") {
                                body.split(|b| *b == b'\n')
                                    .filter(|s| !s.is_empty())
                                    .map(|s| serde_json::from_slice(s).unwrap())
                                    .collect()
                            } else {
                                serde_json::from_slice(&body).unwrap()
                            };
                        let count = events.len();
                        let mut records = records.lock().await;
                        let index = path
                            .split('/')
                            .nth(2)
                            .filter(|_| path.starts_with("/indexes/"))
                            .unwrap_or("default")
                            .to_string();
                        for event in events {
                            let id = records.len() + 1;
                            records.push(json!({"id":id.to_string(),"event_time":event["time"],"message":event["msg"],"service":event["service"],"level":event["level"],"fixture_index":index}));
                            let _ =
                                broadcast.send((index.clone(), records.last().unwrap().clone()));
                        }
                        json!({"accepted":count}).to_string()
                    } else {
                        let url = reqwest::Url::parse(&format!("http://localhost{path}")).unwrap();
                        let p = url
                            .query_pairs()
                            .collect::<std::collections::BTreeMap<_, _>>();
                        let lower = p
                            .get("from")
                            .and_then(|v| v.parse::<i64>().ok())
                            .unwrap_or(0);
                        let upper = p
                            .get("to")
                            .and_then(|v| v.parse::<i64>().ok())
                            .unwrap_or(i64::MAX);
                        let mut rows: Vec<_> = records
                            .lock()
                            .await
                            .iter()
                            .filter(|e| {
                                let time = e["event_time"].as_i64().unwrap();
                                let index = url
                                    .path()
                                    .split('/')
                                    .nth(2)
                                    .filter(|_| url.path().starts_with("/indexes/"))
                                    .unwrap_or("default");
                                e["fixture_index"] == index
                                    && time >= lower
                                    && time < upper
                                    && e["message"].as_str().unwrap().contains(
                                        p.get("message").map(|s| s.as_ref()).unwrap_or(""),
                                    )
                                    && p.get("service").is_none_or(|s| e["service"] == s.as_ref())
                                    && e["level"].as_i64().unwrap()
                                        >= p.get("min_level")
                                            .and_then(|v| v.parse().ok())
                                            .unwrap_or(0)
                            })
                            .cloned()
                            .collect();
                        if url.path().ends_with("/count") {
                            json!({"count":rows.len()}).to_string()
                        } else if url.path().ends_with("/facets") {
                            json!({"services":[],"hosts":[],"loggers":[],"sources":[],"levels":[]})
                                .to_string()
                        } else if url.path().ends_with("/histogram") {
                            json!({"buckets":[]}).to_string()
                        } else {
                            rows.sort_by_key(|r| {
                                std::cmp::Reverse((
                                    r["event_time"].as_i64().unwrap(),
                                    r["id"].as_str().unwrap().parse::<u64>().unwrap(),
                                ))
                            });
                            rows.truncate(
                                p.get("limit").and_then(|v| v.parse().ok()).unwrap_or(100),
                            );
                            json!({"events":rows,"next_cursor":null}).to_string()
                        }
                    };
                    let wire = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{text}",
                        text.len()
                    );
                    if socket.write_all(wire.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    Server {
        base,
        requests,
        task,
        connections,
    }
}

#[tokio::test]
async fn real_http_mixed_shapes_final_accounting_and_cleanup() {
    let server = fixture(false).await;
    let args = mixed::Args {
        url: server.base.clone(),
        seed_events: 0,
        duration: 0.3,
        rate: 100.0,
        batch_size: 10,
        ingest_clients: 1,
        query_clients: 1,
        query_rate: 20.0,
        request_shape: "mixed".into(),
        retention_ms: Some(604800000),
        tail_drain_seconds: 0.0,
        ..mixed::Args::default()
    };
    let report = mixed::report(&args, "reader", "writer").await.unwrap();
    assert_eq!(report["offered_events"], 30);
    assert_eq!(report["accepted_events"], 30);
    assert_eq!(report["matching_events_after"]["count"], 30);
    assert_eq!(report["qualification"]["passed"], true);
    assert_eq!(report["correctness_problems"], json!([]));
    assert_eq!(report["accounting"]["sent_query"], 6);
    assert_eq!(report["expected_visibility"]["exact"], true);
    assert_eq!(report["latency_ms"]["ingest"]["samples"], 3);
    let requests = server.requests.lock().await;
    let writes: Vec<_> = requests
        .iter()
        .filter(|(line, _, _)| line.starts_with("POST"))
        .collect();
    assert_eq!(writes.len(), 3);
    assert!(writes[0].1.contains("application/json"));
    assert!(writes[1].1.contains("application/x-ndjson"));
    assert!(
        writes[2]
            .1
            .to_lowercase()
            .contains("transfer-encoding: chunked")
    );
    assert!(!writes[2].1.to_lowercase().contains("content-length:"));
    assert!(
        writes
            .iter()
            .all(|(_, headers, _)| headers.contains("Bearer writer"))
    );
}

#[tokio::test]
async fn failed_ingest_is_ambiguous_and_not_retried() {
    let server = fixture(true).await;
    let http = Http::new(&server.base, "reader", "writer").unwrap();
    let (code, body) = http
        .request(
            "/logs/ingest",
            Some((b"[]".to_vec(), "application/json", false)),
            false,
        )
        .await;
    assert_eq!(code, 0);
    assert_eq!(body["commit_ambiguous"], true);
    assert_eq!(server.requests.lock().await.len(), 1);
}

#[tokio::test]
async fn truncated_responses_account_all_units_without_worker_death() {
    let server = fixture(true).await;
    let args = index::Args {
        binary: "unused".into(),
        image: None,
        duration: 0.03,
        rate: 200.0,
        query_rate: 200.0,
        batch_size: 1,
        padding_bytes: 0,
        output: "unused".into(),
    };
    let report = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        index::workload(&server.base, "reader", "writer", None, &args),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(report["passed"], false);
    assert_eq!(report["worker_errors"], json!([]));
    assert_eq!(report["counts"]["ingest_failed_units"], 6);
    assert_eq!(report["counts"]["ambiguous_ingest_events"], 6);
    assert_eq!(report["counts"]["query_failed_units"], 6);
    assert_eq!(report["verification_errors"].as_array().unwrap().len(), 4);
    assert_eq!(
        server
            .requests
            .lock()
            .await
            .iter()
            .filter(|(line, _, _)| line.starts_with("POST"))
            .count(),
        6
    );
}

#[tokio::test]
async fn http_pool_reuses_active_connections_and_ignores_redirects() {
    let server = fixture(false).await;
    let http = Http::new(&server.base, "r", "w").unwrap();
    for _ in 0..2 {
        assert_eq!(http.request("/metrics", None, true).await.0, 200);
    }
    assert_eq!(server.requests.lock().await.len(), 2);
    assert_eq!(server.connections.load(Ordering::Relaxed), 1);
    assert_eq!(
        http.request(
            "/redirect",
            Some((b"[]".to_vec(), "application/json", false)),
            false
        )
        .await
        .0,
        307
    );
    assert_eq!(server.requests.lock().await.len(), 3);
    assert!(Http::new("http://user:secret@localhost", "r", "w").is_err());
}

#[tokio::test]
async fn normal_index_work_drains_and_verifies_tails_counts_and_isolation() {
    let server = fixture(false).await;
    let args = index::Args {
        binary: "unused".into(),
        image: None,
        duration: 0.03,
        rate: 200.0,
        query_rate: 200.0,
        batch_size: 1,
        padding_bytes: 0,
        output: "unused".into(),
    };
    let report = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        index::workload(&server.base, "reader", "writer", None, &args),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(report["passed"], true, "{report}");
    assert_eq!(report["counts"]["successful_queries"], 6);
    assert!(
        report["tails"]
            .as_array()
            .unwrap()
            .iter()
            .all(|tail| tail["observed"] == 3 && tail["isolated"] == true)
    );
}
