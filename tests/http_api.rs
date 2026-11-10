//! HTTP API regression tests for authentication, queries, limits and streaming.
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use logbrook::{config::Config, http::router, storage::Storage};
use serde_json::{Value, json};
use tower::ServiceExt;

fn setup() -> (tempfile::TempDir, Storage, Router) {
    setup_with(|_| {})
}

fn setup_with(change: impl FnOnce(&mut Config)) -> (tempfile::TempDir, Storage, Router) {
    let directory = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = directory.path().into();
    config
        .ingest_tokens
        .insert("producer-token-123456".into(), "alpha".into());
    config
        .ingest_tokens
        .insert("producer-beta-123456".into(), "beta".into());
    config
        .read_tokens
        .insert("reader-token-123456".into(), vec!["alpha".into()]);
    config
        .read_tokens
        .insert("admin-token-1234567".into(), vec![]);

    // Historical fixtures exercise API normalization separately from live admission.
    config.retention_ms = i64::MAX / 2;
    change(&mut config);

    let storage = Storage::open(config.storage.clone()).unwrap();
    (directory, storage.clone(), router(storage, config))
}

fn tail_token(after: i64, index: Option<&str>, sources: &[&str], fields: Value) -> String {
    let mut value = json!({
        "from": 0,
        "to": 253_402_300_799_999_i64,
        "sources": sources,
        "limit": 1000,
    });
    value
        .as_object_mut()
        .unwrap()
        .extend(fields.as_object().unwrap().clone());

    let mut query: logbrook::model::Query = serde_json::from_value(value).unwrap();
    query.cursor_scope = index.map_or(logbrook::query_cursor::CursorScope::Legacy, |index| {
        logbrook::query_cursor::CursorScope::Named(index.into())
    });
    logbrook::query_cursor::encode_tail(&query, after).unwrap()
}

fn encoded(value: &str) -> String {
    let mut url = reqwest::Url::parse("http://localhost/").unwrap();
    url.query_pairs_mut().append_pair("v", value);
    url.query().unwrap().strip_prefix("v=").unwrap().to_owned()
}

fn frame_id(frame: &str) -> &str {
    frame
        .lines()
        .find_map(|line| line.strip_prefix("id: "))
        .expect("SSE ID")
}

async fn call(
    app: &Router,
    method: &str,
    path: &str,
    token: &str,
    kind: &str,
    body: String,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", kind)
        .body(Body::from(body))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn durable_ingest_normalizes_and_scopes_every_query() {
    let (_directory, storage, app) = setup();
    let event = json!([{
        "time": 1000,
        "level": 30,
        "msg": "hello needle",
        "name": "api",
        "hostname": "worker",
        "service": "checkout",
        "source": "spoof",
        "id": "spoof",
        "extra": { "ok": true },
    }]);
    let (status, result) = call(
        &app,
        "POST",
        "/logs/ingest",
        "producer-token-123456",
        "application/json",
        event.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["accepted"], 1);
    assert_eq!(
        call(
            &app,
            "POST",
            "/logs/ingest",
            "producer-beta-123456",
            "application/x-ndjson",
            "{\"time\":1000,\"level\":40,\"msg\":\"hidden\"}\n".into()
        )
        .await
        .0,
        StatusCode::OK
    );

    let (status, result) = call(
        &app,
        "GET",
        "/logs?from=0&to=2000&message=needle",
        "reader-token-123456",
        "application/json",
        String::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["events"].as_array().unwrap().len(), 1);

    let event = &result["events"][0];
    assert_eq!(event["source"], "alpha");
    assert_eq!(event["logger"], "api");
    assert_eq!(event["attributes"]["extra"]["ok"], true);
    assert_eq!(event["attributes"]["source"], "spoof");
    assert_eq!(event["attributes"]["id"], "spoof");

    for endpoint in ["/logs", "/logs/count", "/logs/facets", "/logs/histogram"] {
        let path = format!("{endpoint}?from=0&to=2000&source=beta");

        assert_eq!(
            call(
                &app,
                "GET",
                &path,
                "reader-token-123456",
                "application/json",
                String::new()
            )
            .await
            .0,
            StatusCode::FORBIDDEN
        );
    }

    let result = call(
        &app,
        "GET",
        "/logs/count?from=0&to=2000",
        "reader-token-123456",
        "application/json",
        String::new(),
    )
    .await
    .1;
    assert_eq!(result["count"], 1);

    let result = call(
        &app,
        "GET",
        "/logs/count?from=0&to=2000",
        "admin-token-1234567",
        "application/json",
        String::new(),
    )
    .await
    .1;
    assert_eq!(result["count"], 2);

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejects_entire_invalid_batch_and_separates_credentials() {
    let (_directory, storage, app) = setup();
    let body = json!([
        { "time": 1000, "level": 30, "msg": "valid" },
        { "time": "wrong", "level": 30, "msg": "invalid" },
    ])
    .to_string();
    assert_eq!(
        call(
            &app,
            "POST",
            "/logs/ingest",
            "producer-token-123456",
            "application/json",
            body
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );

    assert_eq!(
        call(
            &app,
            "GET",
            "/logs?from=0&to=2000",
            "producer-token-123456",
            "application/json",
            String::new()
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );

    assert_eq!(
        call(
            &app,
            "POST",
            "/logs/ingest",
            "reader-token-123456",
            "application/json",
            "[]".into()
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );

    let result = call(
        &app,
        "GET",
        "/logs/count?from=0&to=2000",
        "admin-token-1234567",
        "application/json",
        String::new(),
    )
    .await
    .1;
    assert_eq!(result["count"], 0);

    assert_eq!(
        call(
            &app,
            "GET",
            "/logs?from=2000&to=0",
            "reader-token-123456",
            "application/json",
            String::new()
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn enforces_body_limit_without_content_length_and_depth() {
    let (_directory, storage, app) = setup();
    assert_eq!(
        call(
            &app,
            "POST",
            "/logs/ingest",
            "producer-token-123456",
            "application/json",
            " ".repeat(1024 * 1024 + 1)
        )
        .await
        .0,
        StatusCode::PAYLOAD_TOO_LARGE
    );

    let mut extra = json!(null);
    for _ in 0..20 {
        extra = json!({
            "nested": extra,
        });
    }

    let event = json!([{ "time": 1000, "level": 30, "msg": "too deep", "extra": extra }]);
    assert_eq!(
        call(
            &app,
            "POST",
            "/logs/ingest",
            "producer-token-123456",
            "application/json",
            event.to_string()
        )
        .await
        .0,
        StatusCode::PAYLOAD_TOO_LARGE
    );

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn tail_resumes_by_ingestion_id_and_limits_subscribers() {
    use http_body_util::BodyExt;

    let (_directory, storage, app) = setup();
    assert_eq!(
        call(
            &app,
            "POST",
            "/logs/ingest",
            "producer-token-123456",
            "application/json",
            json!([{ "time": 100, "level": 30, "msg": "late arrival" }]).to_string()
        )
        .await
        .0,
        StatusCode::OK
    );

    let request = Request::builder()
        .uri("/logs/tail")
        .header("authorization", "Bearer reader-token-123456")
        .header("last-event-id", tail_token(0, None, &["alpha"], json!({})))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");

    let mut body = response.into_body();
    let frame = tokio::time::timeout(std::time::Duration::from_secs(2), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let data = String::from_utf8(frame.into_data().unwrap().to_vec()).unwrap();
    assert!(data.contains("event: log"));
    assert!(data.contains("late arrival"));
    assert_eq!(frame_id(&data), tail_token(1, None, &["alpha"], json!({})));
    drop(body);

    let mut subscribers = Vec::new();
    for _ in 0..8 {
        let request = Request::builder()
            .uri("/logs/tail")
            .header("authorization", "Bearer reader-token-123456")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        subscribers.push(response);
    }

    let response = call(
        &app,
        "GET",
        "/logs/tail",
        "reader-token-123456",
        "application/json",
        String::new(),
    )
    .await;
    assert_eq!(response.0, StatusCode::TOO_MANY_REQUESTS);
    drop(subscribers);

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn streamed_body_limit_and_decoded_admission_are_bounded() {
    let (_directory, storage, app) = setup_with(|config| {
        config.max_body_bytes = 256;
        config.max_decoded_bytes = 256 * 64;
    });
    let chunks = futures_util::stream::iter(vec![
        Ok::<_, std::io::Error>(axum::body::Bytes::from(vec![b' '; 128])),
        Ok(axum::body::Bytes::from(vec![b' '; 129])),
    ]);
    let request = Request::builder()
        .method("POST")
        .uri("/logs/ingest")
        .header("authorization", "Bearer producer-token-123456")
        .body(Body::from_stream(chunks))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(request).await.unwrap().status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );

    let pending = futures_util::stream::pending::<Result<axum::body::Bytes, std::io::Error>>();
    let request = Request::builder()
        .method("POST")
        .uri("/logs/ingest")
        .header("authorization", "Bearer producer-token-123456")
        .body(Body::from_stream(pending))
        .unwrap();
    let task = tokio::spawn(app.clone().oneshot(request));
    tokio::task::yield_now().await;

    assert_eq!(
        call(
            &app,
            "POST",
            "/logs/ingest",
            "producer-token-123456",
            "application/json",
            json!([{ "time": 1, "level": 30, "msg": "concurrent" }]).to_string()
        )
        .await
        .0,
        StatusCode::OK
    );
    task.abort();

    let _ = task.await;

    assert_eq!(
        call(
            &app,
            "POST",
            "/logs/ingest",
            "producer-token-123456",
            "application/json",
            json!([{ "time": 1, "level": 30, "msg": "released" }]).to_string()
        )
        .await
        .0,
        StatusCode::OK
    );

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn archived_events_keep_read_scope_and_tail_replays_pages_and_shutdown() {
    use http_body_util::BodyExt;

    let (_directory, storage, app) = setup_with(|config| {
        config.max_page_size = 1;
        config.tail_poll_ms = 1;
    });
    for token in ["producer-token-123456", "producer-beta-123456"] {
        assert_eq!(
            call(
                &app,
                "POST",
                "/logs/ingest",
                token,
                "application/json",
                json!([
                    { "time": 1, "level": 30, "msg": "first" },
                    { "time": 2, "level": 30, "msg": "second" },
                ])
                .to_string()
            )
            .await
            .0,
            StatusCode::OK
        );
    }

    assert_eq!(storage.archive(10).await.unwrap(), 4);

    let (status, count) = call(
        &app,
        "GET",
        "/logs/count?from=0&to=10",
        "reader-token-123456",
        "application/json",
        String::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(count["count"], 2);

    let tail_request = |after: &str| {
        Request::builder()
            .uri("/logs/tail")
            .header("authorization", "Bearer reader-token-123456")
            .header("last-event-id", after)
            .body(Body::empty())
            .unwrap()
    };
    let mut replay = app
        .clone()
        .oneshot(tail_request(&tail_token(0, None, &["alpha"], json!({}))))
        .await
        .unwrap()
        .into_body();
    for _ in 0..2 {
        let frame = replay.frame().await.unwrap().unwrap().into_data().unwrap();
        let frame = String::from_utf8(frame.to_vec()).unwrap();
        assert!(frame.contains("event: log"));
        assert!(!frame.contains("gap"));
        assert!(frame.contains("alpha"));
    }

    drop(replay);

    let mut body = app
        .clone()
        .oneshot(tail_request(&tail_token(4, None, &["alpha"], json!({}))))
        .await
        .unwrap()
        .into_body();
    let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert!(
        String::from_utf8(frame.to_vec())
            .unwrap()
            .contains("event: cursor")
    );

    storage.stop_admission();

    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), body.frame())
            .await
            .unwrap()
            .is_none()
    );

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn retention_expiry_emits_terminal_gap_and_metrics_count_writer_commits() {
    use http_body_util::BodyExt;

    let (_directory, storage, app) = setup_with(|config| config.tail_poll_ms = 1);

    // A storage caller owns admission independently from any HTTP response lifetime.
    let events = logbrook::ingest::normalize(
        br#"[{"time":1,"level":30,"msg":"removed"},{"time":100,"level":30,"msg":"retained"}]"#,
        false,
        &Config::default(),
    )
    .unwrap();
    storage.append("alpha".into(), events).await.unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("authorization", "Bearer admin-token-1234567")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = to_bytes(response.into_body(), 65536).await.unwrap();
    assert!(
        String::from_utf8(bytes.to_vec())
            .unwrap()
            .contains("logbrook_ingested_events_total 2\n")
    );

    storage.retain(10).await.unwrap();

    let mut body = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/logs/tail")
                .header("authorization", "Bearer reader-token-123456")
                .header("last-event-id", tail_token(0, None, &["alpha"], json!({})))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body();
    let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
    let frame = String::from_utf8(frame.to_vec()).unwrap();
    assert!(frame.contains("event: gap"));
    assert!(frame.contains("expired by retention"));
    assert!(body.frame().await.is_none());

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn live_timestamps_security_and_endpoint_contracts() {
    let (_directory, storage, app) =
        setup_with(|config| config.retention_ms = Config::default().retention_ms);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    for time in [now - Config::default().retention_ms - 1000, now + 301_000] {
        assert_eq!(
            call(
                &app,
                "POST",
                "/logs/ingest",
                "producer-token-123456",
                "application/json",
                json!([{ "time": time, "level": 30, "msg": "bad" }]).to_string()
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }

    assert_eq!(
        call(
            &app,
            "POST",
            "/logs/ingest",
            "producer-token-123456",
            "application/json",
            json!([{
                "time": now,
                "level": 30,
                "msg": "ok",
                "id": "producer-id",
                "host": "producer-host",
                "attributes": { "nested": true },
            }])
            .to_string(),
        )
        .await
        .0,
        StatusCode::OK
    );

    for (path, irrelevant) in [
        ("/logs", "interval_ms=1"),
        ("/logs/count", "limit=1"),
        ("/logs/facets", "cursor=abc"),
        ("/logs/histogram", "limit=1"),
        ("/logs/tail", "from=0"),
    ] {
        let query = if path == "/logs/tail" {
            format!("{path}?{irrelevant}")
        } else {
            format!("{path}?from={}&to={}&{irrelevant}", now - 1000, now + 1000)
        };
        assert_eq!(
            call(
                &app,
                "GET",
                &query,
                "reader-token-123456",
                "application/json",
                String::new()
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }

    assert_eq!(
        call(
            &app,
            "GET",
            "/metrics",
            "reader-token-123456",
            "application/json",
            String::new()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("authorization", "Bearer admin-token-1234567")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    assert_eq!(response.headers()["x-frame-options"], "DENY");
    assert!(
        response.headers()["content-security-policy"]
            .to_str()
            .unwrap()
            .contains("frame-ancestors 'none'")
    );

    let text = String::from_utf8(
        to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(text.contains("# TYPE logbrook_http_duration_seconds histogram"));
    assert!(text.contains("endpoint=\"/logs/ingest\""));

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn chunked_idle_requests_do_not_monopolize_default_memory_and_timeout_releases_admission() {
    let (_directory, storage, app) = setup_with(|config| config.body_idle_timeout_ms = 50);
    let pending = futures_util::stream::pending::<Result<axum::body::Bytes, std::io::Error>>();
    let request = Request::builder()
        .method("POST")
        .uri("/logs/ingest")
        .header("authorization", "Bearer producer-token-123456")
        .body(Body::from_stream(pending))
        .unwrap();
    let task = tokio::spawn(app.clone().oneshot(request));
    tokio::task::yield_now().await;

    let chunks = futures_util::stream::iter(vec![Ok::<_, std::io::Error>(
        axum::body::Bytes::from_static(br#"[{"time":1000,"level":30,"msg":"parallel"}]"#),
    )]);
    let request = Request::builder()
        .method("POST")
        .uri("/logs/ingest")
        .header("authorization", "Bearer producer-token-123456")
        .body(Body::from_stream(chunks))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(request).await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        task.await.unwrap().unwrap().status(),
        StatusCode::GATEWAY_TIMEOUT
    );

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn tail_replays_three_thousand_matching_events_without_backlog_gaps() {
    use http_body_util::BodyExt;

    let (_directory, storage, app) = setup();
    let events = (0..3000)
        .map(|_| logbrook::model::NormalizedEvent {
            event_time: 1000,
            level: 30,
            service: Some("wanted".into()),
            logger: None,
            host: None,
            pid: None,
            message: "match".into(),
            attributes: json!({}),
        })
        .collect();
    storage.append("alpha".into(), events).await.unwrap();
    storage
        .append(
            "alpha".into(),
            vec![logbrook::model::NormalizedEvent {
                event_time: 1000,
                level: 30,
                service: Some("other".into()),
                logger: None,
                host: None,
                pid: None,
                message: "hidden".into(),
                attributes: json!({}),
            }],
        )
        .await
        .unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/logs/tail?service=wanted&message=match&min_level=30")
                .header("authorization", "Bearer reader-token-123456")
                .header(
                    "last-event-id",
                    tail_token(
                        0,
                        None,
                        &["alpha"],
                        json!({
                            "service": "wanted",
                            "message": "match",
                            "min_level": 30,
                        }),
                    ),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let mut body = response.into_body();
    for expected in 1..=3000 {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(2), body.frame())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        let frame = String::from_utf8(frame.to_vec()).unwrap();
        assert!(frame.contains("event: log"), "{frame}");
        assert_eq!(
            frame_id(&frame),
            tail_token(
                expected,
                None,
                &["alpha"],
                json!({
                    "service": "wanted",
                    "message": "match",
                    "min_level": 30,
                })
            )
        );
        assert!(!frame.contains("hidden"));
    }

    drop(body);

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn checked_openapi_matches_endpoint_parameter_acceptance() {
    let (_directory, storage, app) = setup();
    let spec: Value = serde_json::from_str(include_str!("../src/openapi.json")).unwrap();
    for (path, extras) in [
        ("/logs", vec!["limit", "cursor"]),
        ("/logs/count", vec![]),
        ("/logs/facets", vec![]),
        ("/logs/histogram", vec!["interval_ms"]),
    ] {
        let names = spec["paths"][path]["get"]["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["name"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(names.contains(&"filter"));
        for field in ["limit", "cursor", "interval_ms"] {
            assert_eq!(names.contains(&field), extras.contains(&field));

            if !names.contains(&field) {
                assert_eq!(
                    call(
                        &app,
                        "GET",
                        &format!("{path}?from=0&to=2000&{field}=1"),
                        "reader-token-123456",
                        "application/json",
                        String::new()
                    )
                    .await
                    .0,
                    StatusCode::BAD_REQUEST
                );
            }
        }
    }

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_full_limit_chunked_bodies_are_admitted_with_default_budgets() {
    let (_directory, storage, app) = setup();
    let event = br#"[{"time":1000,"level":30,"msg":"full wire body"}]"#;
    let mut wire = event.to_vec();
    wire.resize(Config::default().max_body_bytes, b' ');

    let make = || {
        let first = axum::body::Bytes::copy_from_slice(&wire[..wire.len() / 2]);
        let second = axum::body::Bytes::copy_from_slice(&wire[wire.len() / 2..]);
        let chunks = futures_util::stream::unfold(
            (Some(first), Some(second)),
            |(first, second)| async move {
                if let Some(first) = first {
                    Some((Ok::<_, std::io::Error>(first), (None, second)))
                } else if let Some(second) = second {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    Some((Ok(second), (None, None)))
                } else {
                    None
                }
            },
        );
        Request::builder()
            .method("POST")
            .uri("/logs/ingest")
            .header("authorization", "Bearer producer-token-123456")
            .body(Body::from_stream(chunks))
            .unwrap()
    };
    let (first, second) = tokio::join!(app.clone().oneshot(make()), app.clone().oneshot(make()));
    assert_eq!(first.unwrap().status(), StatusCode::OK);
    assert_eq!(second.unwrap().status(), StatusCode::OK);
    assert_eq!(storage.committed_events(), 2);

    storage.shutdown().await.unwrap();
}

fn assert_contract(value: &Value, schema: &Value, spec: &Value) {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        return assert_contract(
            value,
            spec.pointer(reference.trim_start_matches('#')).unwrap(),
            spec,
        );
    }

    if let Some(kind) = schema.get("type") {
        let matches = |kind: &str| match kind {
            "object" => value.is_object(),
            "array" => value.is_array(),
            "string" => value.is_string(),
            "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
            "number" => value.is_number(),
            "boolean" => value.is_boolean(),
            "null" => value.is_null(),
            other => panic!("unvalidated contract type {other}"),
        };
        assert!(
            if let Some(kinds) = kind.as_array() {
                kinds.iter().any(|kind| matches(kind.as_str().unwrap()))
            } else {
                matches(kind.as_str().unwrap())
            },
            "response {value} violates schema {schema}"
        );
    }

    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        for key in required {
            assert!(
                value.get(key.as_str().unwrap()).is_some(),
                "missing required {key}: {value}"
            );
        }
    }

    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (key, child) in properties {
            if let Some(value) = value.get(key) {
                assert_contract(value, child, spec);
            }
        }
    }

    if let Some(items) = schema.get("items") {
        for item in value.as_array().unwrap() {
            assert_contract(item, items, spec);
        }
    }
}

#[tokio::test]
async fn successful_and_error_responses_match_checked_openapi_schemas() {
    let (_directory, storage, app) = setup();
    let spec: Value = serde_json::from_str(include_str!("../src/openapi.json")).unwrap();
    let (status, accepted) = call(
        &app,
        "POST",
        "/logs/ingest",
        "producer-token-123456",
        "application/json",
        json!([{ "time": 1000,
            "level": 30,
            "msg": "schema",
            "service": "api",
            "extra": [1,
            true] }])
        .to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_contract(
        &accepted,
        &spec["paths"]["/logs/ingest"]["post"]["responses"]["200"]["content"]["application/json"]["schema"],
        &spec,
    );

    for path in ["/logs", "/logs/count", "/logs/facets", "/logs/histogram"] {
        let (status, value) = call(
            &app,
            "GET",
            &format!("{path}?from=0&to=2000"),
            "reader-token-123456",
            "application/json",
            String::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_contract(
            &value,
            &spec["paths"][path]["get"]["responses"]["200"]["content"]["application/json"]["schema"],
            &spec,
        );
    }

    let (status, error) = call(
        &app,
        "GET",
        "/logs?from=0&to=2000",
        "invalid",
        "application/json",
        String::new(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_contract(&error, &spec["components"]["schemas"]["Error"], &spec);

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn ingest_alias_documents_and_returns_the_named_route_size_limit_response() {
    let (_directory, storage, app) = setup_with(|config| config.storage.max_size_bytes = Some(1));
    let spec: Value = serde_json::from_str(include_str!("../src/openapi.json")).unwrap();
    let alias = spec["paths"]["/logs/ingest"]["post"]["responses"]
        .as_object()
        .unwrap();
    let named = spec["paths"]["/indexes/{index}/logs/ingest"]["post"]["responses"]
        .as_object()
        .unwrap();
    // A named route additionally reports missing indexes. Shared ingest outcomes
    // must retain the same documented status and body contract on the alias.
    for (status, response) in named.iter().filter(|(status, _)| *status != "404") {
        assert_eq!(alias.get(status), Some(response), "ingest status {status}");
    }

    for (path, documented) in [
        ("/logs/ingest", "/logs/ingest"),
        (
            "/indexes/default/logs/ingest",
            "/indexes/{index}/logs/ingest",
        ),
    ] {
        let (status, error) = call(
            &app,
            "POST",
            path,
            "producer-token-123456",
            "application/json",
            json!([{ "time": 1000, "level": 30, "msg": "size limited" }]).to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::INSUFFICIENT_STORAGE);

        let schema = &spec["paths"][documented]["post"]["responses"]["507"]["content"]["application/json"]
            ["schema"];
        assert_eq!(schema["$ref"], "#/components/schemas/Error");
        assert_contract(&error, schema, &spec);
    }

    assert_eq!(storage.committed_events(), 0);

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn live_admission_honors_persisted_retention_cutoff_atomically() {
    let (_directory, storage, app) = setup();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    storage.retain(now - 1000).await.unwrap();

    let (status, _) = call(
        &app,
        "POST",
        "/logs/ingest",
        "producer-token-123456",
        "application/json",
        json!([
            { "time": now, "level": 30, "msg": "valid" },
            {
                "time": now - 2000,
                "level": 30,
                "msg": "older than persisted cutoff",
            },
        ])
        .to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        storage.committed_events(),
        0,
        "mixed live batch must remain atomic"
    );

    assert_eq!(
        call(
            &app,
            "POST",
            "/logs/ingest",
            "producer-token-123456",
            "application/json",
            json!([{ "time": now, "level": 30, "msg": "valid" }]).to_string()
        )
        .await
        .0,
        StatusCode::OK
    );

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn filtered_tail_wakes_on_commit_and_detects_retention_during_stream() {
    use http_body_util::BodyExt;

    let (_directory, storage, app) = setup_with(|config| config.tail_poll_ms = 10_000);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/logs/tail?service=wanted")
                .header("authorization", "Bearer reader-token-123456")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let mut body = response.into_body();
    let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert!(
        String::from_utf8(frame.to_vec())
            .unwrap()
            .contains("event: cursor")
    );

    let events = logbrook::ingest::normalize(
        b"[{\"time\":1,\"level\":30,\"msg\":\"visible\",\"service\":\"wanted\"},\
            {\"time\":100,\"level\":30,\"msg\":\"hidden\",\"service\":\"other\"}]",
        false,
        &Config::default(),
    )
    .unwrap();
    storage.append("alpha".into(), events).await.unwrap();

    let frame = tokio::time::timeout(std::time::Duration::from_millis(500), body.frame())
        .await
        .expect("commit notification should avoid the ten-second poll delay")
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    let frame = String::from_utf8(frame.to_vec()).unwrap();
    assert!(frame.contains("visible"));
    assert!(!frame.contains("hidden"));

    // Keep the stream cursor at 1, then expire through an unseen ID 3.
    let events = logbrook::ingest::normalize(
        br#"[{"time":1,"level":30,"msg":"expired unseen","service":"other"}]"#,
        false,
        &Config::default(),
    )
    .unwrap();
    storage.append("alpha".into(), events).await.unwrap();
    storage.retain(10).await.unwrap();

    let frame = tokio::time::timeout(std::time::Duration::from_millis(500), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert!(
        String::from_utf8(frame.to_vec())
            .unwrap()
            .contains("event: gap")
    );
    assert!(body.frame().await.is_none());

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn histogram_bucket_limit_counts_fractional_final_bucket_consistently() {
    let (_directory, storage, app) = setup();
    let maximum = logbrook::model::MAX_HISTOGRAM_BUCKETS;
    let exact = maximum * 10;
    for (to, expected) in [
        (exact, StatusCode::OK),
        (exact + 1, StatusCode::BAD_REQUEST),
    ] {
        assert_eq!(
            call(
                &app,
                "GET",
                &format!("/logs/histogram?from=0&to={to}&interval_ms=10"),
                "reader-token-123456",
                "application/json",
                String::new()
            )
            .await
            .0,
            expected
        );

        let query = logbrook::model::Query {
            from: 0,
            to,
            limit: 1,
            ..Default::default()
        };
        let result = storage.histogram(query, 10).await;
        if expected == StatusCode::OK {
            assert!(result.is_ok());
        } else {
            assert_eq!(
                result.unwrap_err().kind,
                logbrook::model::ErrorKind::Invalid
            );
        }
    }

    storage.shutdown().await.unwrap();
}

async fn index_call(
    app: &Router,
    method: &str,
    path: &str,
    token: &str,
    body: String,
) -> (StatusCode, Value) {
    call(app, method, path, token, "application/json", body).await
}

async fn ingested_metric(app: &Router) -> u64 {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("authorization", "Bearer admin-token-1234567")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    String::from_utf8(bytes.to_vec())
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("logbrook_ingested_events_total "))
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn ingested_metric_survives_index_deletion_recreation_and_resets_on_restart() {
    use logbrook::{http::router_with_registry, indexes::IndexRegistry};

    let directory = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = directory.path().into();
    config.retention_ms = i64::MAX / 2;
    config.archive_after_ms = i64::MAX / 3;
    config
        .ingest_tokens
        .insert("producer-token-123456".into(), "alpha".into());
    config
        .read_tokens
        .insert("admin-token-1234567".into(), vec![]);
    config.admin_tokens.push("manager-token-123456".into());

    let registry = IndexRegistry::open(config.clone()).await.unwrap();
    let events = logbrook::ingest::normalize(
        br#"[{"time":1000,"level":30,"msg":"direct writer"}]"#,
        false,
        &config,
    )
    .unwrap();
    // Commits before router creation and outside HTTP must count too.
    registry
        .get("default")
        .unwrap()
        .storage
        .append("alpha".into(), events.clone())
        .await
        .unwrap();

    let app = router_with_registry(registry.clone(), config.clone());
    assert_eq!(ingested_metric(&app).await, 1);

    let (status, accepted) = index_call(
        &app,
        "POST",
        "/indexes/payments/logs/ingest",
        "producer-token-123456",
        json!(vec![
            json!({
                "time": 1000,
                "level": 30,
                "msg": "indexed",
            });
            10
        ])
        .to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(accepted["accepted"], 10);
    assert_eq!(ingested_metric(&app).await, 11);

    let payments = registry.get("payments").unwrap().storage;
    payments.retain(1500).await.unwrap();

    assert_eq!(ingested_metric(&app).await, 11);

    let concurrent_writer = payments.clone();
    let concurrent_events = events.clone();
    let writing = async move {
        let mut accepted = 0;
        for _ in 0..16 {
            if let Ok(count) = concurrent_writer
                .append("alpha".into(), concurrent_events.clone())
                .await
            {
                accepted += count as u64;
            }
        }
        accepted
    };

    let deleting = index_call(
        &app,
        "DELETE",
        "/indexes/payments",
        "manager-token-123456",
        String::new(),
    );
    let (committed_during_delete, (status, _)) = tokio::join!(writing, deleting);
    assert_eq!(status, StatusCode::OK);
    assert!(registry.get("payments").is_none());

    let total = 11 + committed_during_delete;
    assert_eq!(ingested_metric(&app).await, total);
    assert!(payments.append("alpha".into(), events).await.is_err());

    let (status, accepted) = index_call(
        &app,
        "POST",
        "/indexes/payments/logs/ingest",
        "producer-token-123456",
        json!([{ "time": 2000, "level": 30, "msg": "recreated" }]).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(accepted["accepted"], 1);
    assert_eq!(ingested_metric(&app).await, total + 1);

    registry.shutdown().await.unwrap();

    assert_eq!(ingested_metric(&app).await, total + 1);

    let restarted = IndexRegistry::open(config.clone()).await.unwrap();
    let restarted_app = router_with_registry(restarted.clone(), config);
    assert_eq!(ingested_metric(&restarted_app).await, 0);

    let (_, retained) = index_call(
        &restarted_app,
        "GET",
        "/indexes/payments/logs/count?from=0&to=10000",
        "admin-token-1234567",
        String::new(),
    )
    .await;
    assert_eq!(retained["count"], 1, "persisted rows are not new commits");

    let (status, _) = index_call(
        &restarted_app,
        "POST",
        "/logs/ingest",
        "producer-token-123456",
        json!([{ "time": 2000, "level": 30, "msg": "new process" }]).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ingested_metric(&restarted_app).await, 1);

    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn index_management_scopes_sizes_and_deletion() {
    use logbrook::{config::IndexSettings, http::router_with_registry, indexes::IndexRegistry};

    let directory = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = directory.path().into();
    config.retention_ms = i64::MAX / 2;
    config.archive_after_ms = i64::MAX / 3;
    config
        .ingest_tokens
        .insert("producer-token-123456".into(), "alpha".into());
    config
        .read_tokens
        .insert("reader-all-123456789".into(), vec![]);
    config
        .read_tokens
        .insert("reader-source-123456".into(), vec!["alpha".into()]);
    config
        .read_tokens
        .insert("reader-index-1234567".into(), vec![]);
    config
        .read_index_scopes
        .insert("reader-index-1234567".into(), vec!["payments".into()]);
    config
        .read_index_scopes
        .insert("reader-source-123456".into(), vec!["payments".into()]);
    config.admin_tokens.push("manager-token-123456".into());
    config.indexes.insert(
        "configured".into(),
        IndexSettings {
            max_size_bytes: Some(64 * 1024 * 1024),
            ..Default::default()
        },
    );

    let registry = IndexRegistry::open(config.clone()).await.unwrap();
    let app = router_with_registry(registry.clone(), config.clone());

    for (name, token) in [
        ("payments", "manager-token-123456"),
        ("orders", "producer-token-123456"),
    ] {
        let (status, info) = index_call(
            &app,
            "PUT",
            &format!("/indexes/{name}"),
            token,
            String::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            info,
            json!({
                "name": name,
            })
        );
        assert_eq!(
            index_call(
                &app,
                "POST",
                &format!("/indexes/{name}/logs/ingest"),
                "producer-token-123456",
                json!([{ "time": 1000, "level": 30, "msg": name }]).to_string()
            )
            .await
            .0,
            StatusCode::OK
        );
    }

    let root = directory.path().join("indexes/payments");
    std::fs::create_dir_all(root.join("temp/nested")).unwrap();
    std::fs::write(root.join("temp/nested/scratch"), b"1234567").unwrap();
    std::fs::write(root.join("archives/unpublished.bin"), b"12345").unwrap();
    std::fs::write(root.join("unknown.bin"), b"123").unwrap();

    let (status, info) = index_call(
        &app,
        "GET",
        "/indexes/payments",
        "manager-token-123456",
        String::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(info["name"], "payments");
    assert_eq!(info["max_size_bytes"], Value::Null);
    assert_eq!(info["retention_ms"], config.retention_ms);
    assert_eq!(info["ready"], true);
    assert_eq!(info["temp_bytes"], 7);
    assert_eq!(info["archive_bytes"], 5);
    assert_eq!(info["other_bytes"], 3);
    assert!(info["database_bytes"].as_u64().unwrap() > 0);

    let total: u64 = [
        "database_bytes",
        "wal_bytes",
        "archive_bytes",
        "temp_bytes",
        "other_bytes",
    ]
    .iter()
    .map(|field| info[field].as_u64().unwrap())
    .sum();
    assert_eq!(info["size_bytes"], total);

    std::fs::write(root.join("unknown.bin"), b"123456789").unwrap();

    let (_, fresh) = index_call(
        &app,
        "GET",
        "/indexes/payments",
        "reader-index-1234567",
        String::new(),
    )
    .await;
    assert_eq!(
        fresh["other_bytes"], 9,
        "information must refresh physical files without a size target"
    );

    for token in [
        "manager-token-123456",
        "reader-all-123456789",
        "reader-index-1234567",
    ] {
        let (status, listing) = index_call(&app, "GET", "/indexes", token, String::new()).await;
        assert_eq!(status, StatusCode::OK);

        let entries = listing["indexes"].as_array().unwrap();
        assert!(
            entries
                .iter()
                .all(|entry| entry["name"].is_string() && entry["size_bytes"].is_u64())
        );

        let payments = entries
            .iter()
            .find(|entry| entry["name"] == "payments")
            .unwrap();
        assert_eq!(payments["other_bytes"], 9);

        if token == "reader-index-1234567" {
            assert_eq!(entries.len(), 1);
        } else {
            assert_eq!(entries.len(), 4);

            let configured = entries
                .iter()
                .find(|entry| entry["name"] == "configured")
                .unwrap();
            assert_eq!(configured["max_size_bytes"], 64 * 1024 * 1024);
        }
    }

    assert_eq!(
        index_call(
            &app,
            "GET",
            "/indexes",
            "reader-source-123456",
            String::new()
        )
        .await
        .1,
        json!({
            "indexes": [{ "name": "payments" }],
        })
    );

    assert_eq!(
        index_call(
            &app,
            "GET",
            "/indexes/payments",
            "reader-source-123456",
            String::new()
        )
        .await
        .1,
        json!({
            "name": "payments",
        })
    );

    assert_eq!(
        index_call(
            &app,
            "GET",
            "/indexes/orders",
            "reader-index-1234567",
            String::new()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );

    for name in ["payments", "missing", "Uppercase"] {
        for (token, expected) in [
            ("invalid", StatusCode::UNAUTHORIZED),
            ("producer-token-123456", StatusCode::FORBIDDEN),
            ("reader-all-123456789", StatusCode::FORBIDDEN),
            ("reader-source-123456", StatusCode::FORBIDDEN),
        ] {
            assert_eq!(
                index_call(
                    &app,
                    "DELETE",
                    &format!("/indexes/{name}"),
                    token,
                    String::new()
                )
                .await
                .0,
                expected
            );
        }
    }

    assert!(registry.get("payments").is_some());
    assert!(root.exists());

    for name in ["default", "configured"] {
        let (status, error) = index_call(
            &app,
            "DELETE",
            &format!("/indexes/{name}"),
            "manager-token-123456",
            String::new(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(error["error"].as_str().unwrap().contains(name));
        assert!(registry.get(name).is_some());
    }

    for (method, path) in [
        ("GET", "/logs?from=0&to=10000"),
        ("GET", "/indexes/payments/logs?from=0&to=10000"),
        ("POST", "/indexes/payments/logs/ingest"),
        ("POST", "/logs/ingest"),
        ("GET", "/metrics"),
    ] {
        assert_eq!(
            index_call(&app, method, path, "manager-token-123456", "[]".into())
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
    }

    for path in ["/indexes", "/indexes/missing", "/indexes/Uppercase"] {
        assert_eq!(
            index_call(&app, "GET", path, "invalid", String::new())
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
    }

    assert_eq!(
        index_call(
            &app,
            "GET",
            "/indexes/missing",
            "manager-token-123456",
            String::new()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );

    assert_eq!(
        index_call(
            &app,
            "DELETE",
            "/indexes/missing",
            "manager-token-123456",
            String::new()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );

    let (status, deleted) = index_call(
        &app,
        "DELETE",
        "/indexes/payments",
        "manager-token-123456",
        String::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        deleted,
        json!({
            "deleted": "payments",
        })
    );
    assert!(registry.get("payments").is_none());
    assert!(!root.exists());
    assert_eq!(
        index_call(
            &app,
            "GET",
            "/indexes/payments",
            "reader-all-123456789",
            String::new()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );

    let (_, listing) = index_call(
        &app,
        "GET",
        "/indexes",
        "manager-token-123456",
        String::new(),
    )
    .await;
    assert!(
        !listing["indexes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["name"] == "payments")
    );
    assert_eq!(
        index_call(
            &app,
            "GET",
            "/indexes/orders/logs/count?from=0&to=10000",
            "reader-all-123456789",
            String::new()
        )
        .await
        .1["count"],
        1
    );

    assert_eq!(
        index_call(
            &app,
            "PUT",
            "/indexes/payments",
            "manager-token-123456",
            String::new()
        )
        .await
        .0,
        StatusCode::OK
    );

    assert_eq!(
        index_call(
            &app,
            "GET",
            "/indexes/payments/logs/count?from=0&to=10000",
            "reader-all-123456789",
            String::new()
        )
        .await
        .1["count"],
        0
    );

    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn named_indexes_isolate_data_scopes_and_cursors() {
    use logbrook::{http::router_with_registry, indexes::IndexRegistry};

    let directory = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = directory.path().into();
    config.retention_ms = i64::MAX / 2;
    config.max_tail_subscribers = 1;
    config
        .ingest_tokens
        .insert("producer-token-123456".into(), "alpha".into());
    config
        .read_tokens
        .insert("admin-token-1234567".into(), vec![]);
    config
        .read_tokens
        .insert("limited-token-123456".into(), vec!["alpha".into()]);
    config
        .read_index_scopes
        .insert("limited-token-123456".into(), vec!["payments".into()]);
    config
        .ingest_tokens
        .insert("limited-producer-123".into(), "alpha".into());
    config
        .ingest_index_scopes
        .insert("limited-producer-123".into(), vec!["payments".into()]);

    let registry = IndexRegistry::open(config.clone()).await.unwrap();
    let app = router_with_registry(registry.clone(), config);
    assert_eq!(
        index_call(
            &app,
            "GET",
            "/indexes/missing/logs?from=0&to=10000",
            "admin-token-1234567",
            String::new()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );

    assert!(registry.get("missing").is_none());
    assert_eq!(
        index_call(
            &app,
            "POST",
            "/indexes/denied/logs/ingest",
            "invalid",
            "[]".into()
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );

    assert!(registry.get("denied").is_none());
    assert_eq!(
        index_call(
            &app,
            "POST",
            "/indexes/denied/logs/ingest",
            "limited-producer-123",
            "[]".into()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );

    assert!(registry.get("denied").is_none());
    assert_eq!(
        index_call(
            &app,
            "PUT",
            "/indexes/orders",
            "producer-token-123456",
            String::new()
        )
        .await
        .0,
        StatusCode::OK
    );

    for name in ["payments", "orders"] {
        assert_eq!(
            index_call(
                &app,
                "POST",
                &format!("/indexes/{name}/logs/ingest"),
                "producer-token-123456",
                json!([
                    { "time": 1000, "level": 30, "msg": name },
                    { "time": 2000, "level": 30, "msg": name },
                ])
                .to_string()
            )
            .await
            .0,
            StatusCode::OK
        );

        let (_, page) = index_call(
            &app,
            "GET",
            &format!("/indexes/{name}/logs?from=0&to=10000&limit=1"),
            "admin-token-1234567",
            String::new(),
        )
        .await;
        assert_eq!(page["events"][0]["message"], name);

        let cursor = page["next_cursor"].as_str().unwrap();
        assert!(cursor.starts_with(&format!("{name}:")));

        let other = if name == "payments" {
            "orders"
        } else {
            "payments"
        };
        assert_eq!(
            index_call(
                &app,
                "GET",
                &format!("/indexes/{other}/logs?from=0&to=10000&cursor={cursor}"),
                "admin-token-1234567",
                String::new()
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }

    let (_, names) = index_call(
        &app,
        "GET",
        "/indexes",
        "limited-token-123456",
        String::new(),
    )
    .await;
    assert_eq!(
        names,
        json!({
            "indexes": [{ "name": "payments" }],
        })
    );
    assert_eq!(
        index_call(
            &app,
            "GET",
            "/logs/count?from=0&to=10000",
            "limited-token-123456",
            String::new()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );

    assert_eq!(
        index_call(
            &app,
            "GET",
            "/metrics",
            "limited-token-123456",
            String::new()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );

    assert_eq!(
        index_call(
            &app,
            "GET",
            "/logs/count?from=0&to=10000",
            "admin-token-1234567",
            String::new()
        )
        .await
        .1["count"],
        0
    );

    assert_eq!(
        index_call(
            &app,
            "PUT",
            "/indexes/manual",
            "producer-token-123456",
            String::new()
        )
        .await
        .0,
        StatusCode::OK
    );

    assert!(registry.get("manual").is_some());

    let request = Request::builder()
        .uri("/indexes/payments/logs/tail")
        .header("authorization", "Bearer admin-token-1234567")
        .header(
            "last-event-id",
            tail_token(0, Some("orders"), &[], json!({})),
        )
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.clone().oneshot(request).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );

    use http_body_util::BodyExt;

    let request = Request::builder()
        .uri("/indexes/payments/logs/tail")
        .header("authorization", "Bearer admin-token-1234567")
        .header(
            "last-event-id",
            tail_token(0, Some("payments"), &[], json!({})),
        )
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let mut body = response.into_body();
    let frame = tokio::time::timeout(std::time::Duration::from_secs(2), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    let frame = String::from_utf8(frame.to_vec()).unwrap();
    assert!(frame.contains("id: payments:"));
    assert!(frame.contains("event: log"));

    let competing = Request::builder()
        .uri("/indexes/orders/logs/tail")
        .header("authorization", "Bearer admin-token-1234567")
        .header(
            "last-event-id",
            tail_token(0, Some("orders"), &[], json!({})),
        )
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.clone().oneshot(competing).await.unwrap().status(),
        StatusCode::TOO_MANY_REQUESTS
    );

    drop(body);

    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_endpoints_return_json_and_openapi_remains_available() {
    let (_directory, _storage, app) = setup();
    for path in [
        "/",
        "/index.html",
        "/assets/app.js",
        "/explorer",
        "/logs/unknown",
        "/api/unknown",
        "/indexes/default/unknown",
    ] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        assert_eq!(response.headers()["content-type"], "application/json");
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        assert_eq!(response.headers()["x-frame-options"], "DENY");

        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let result: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result["error"], format!("unknown endpoint: {path}"));
    }

    let (status, document) = call(
        &app,
        "GET",
        "/openapi.json",
        "",
        "application/json",
        String::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(document["openapi"].is_string());
    assert_eq!(document["info"]["version"], env!("CARGO_PKG_VERSION"));
    assert!(document["paths"]["/logs"].is_object());
}

#[tokio::test]
async fn attribute_filters_have_route_and_endpoint_parity() {
    use http_body_util::BodyExt;

    let (_directory, storage, app) = setup();
    let events = json!([
        {"time":1000,
            "level":40,
            "name":"api",
            "hostname":"node",
            "service":"jobs",
            "msg":"match",
            "runId":"run +/☃",
            "ok":true,
            "attempt":123,
            "a/b~c.d": "literal"},
        {"time":1001,
            "level":40,
            "name":"api",
            "hostname":"node",
            "service":"jobs",
            "msg":"wrong string",
            "runId":"run +/☃",
            "ok":"true",
            "attempt":123},
        {"time":1002,
            "level":40,
            "name":"api",
            "hostname":"node",
            "service":"jobs",
            "msg":"wrong number",
            "runId":"run +/☃",
            "ok":true,
            "attempt":"123"},
        {"time":1003,
            "level":30,
            "name":"api",
            "hostname":"node",
            "service":"jobs",
            "msg":"wrong severity",
            "runId":"run +/☃",
            "ok":true,
            "attempt":123,
            "a/b~c.d": "literal"},
        {"time":1004,
            "level":40,
            "name":"api",
            "hostname":"node",
            "service":"jobs",
            "msg":"wrong run",
            "runId":"other",
            "ok":true,
            "attempt":123},
        {"time":1005,"level":40,"msg":"null","runId":null,"ok":true,"attempt":123}
    ]);
    assert_eq!(
        call(
            &app,
            "POST",
            "/logs/ingest",
            "producer-token-123456",
            "application/json",
            events.to_string()
        )
        .await
        .0,
        StatusCode::OK
    );

    let filter = json!({
        "attributes": [
                                {"key":"runId","op":"eq","value":"run +/☃"},
                                {"key":"ok","op":"eq","value":true},
                                {"key":"attempt","op":"eq","value":123.0},
                                {"key":"a/b~c.d","op":"eq","value":"literal"}
                            ],
    });
    let selection = format!(
        "service=jobs&logger=api&host=node&min_level=40&filter={}",
        encoded(&filter.to_string())
    );
    for prefix in ["", "/indexes/default"] {
        for endpoint in ["", "/count", "/facets", "/histogram"] {
            let path = format!("{prefix}/logs{endpoint}?from=0&to=2000&{selection}");
            let (status, result) = call(
                &app,
                "GET",
                &path,
                "reader-token-123456",
                "application/json",
                String::new(),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{path}: {result}");
            match endpoint {
                "" => assert_eq!(result["events"][0]["message"], "match"),
                "/count" => assert_eq!(result["count"], 1),
                "/facets" => {
                    for dimension in ["services", "loggers", "hosts", "levels", "sources"] {
                        assert_eq!(result[dimension][0]["count"], 1);
                    }
                }
                _ => assert_eq!(
                    result["buckets"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|bucket| bucket["count"].as_u64().unwrap())
                        .sum::<u64>(),
                    1
                ),
            }
        }

        let token = tail_token(
            0,
            (!prefix.is_empty()).then_some("default"),
            &["alpha"],
            json!({
                "service": "jobs",
                "logger": "api",
                "host": "node",
                "min_level": 40,
                "attributes": filter,
            }),
        );
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("{prefix}/logs/tail?{selection}"))
                    .header("authorization", "Bearer reader-token-123456")
                    .header("last-event-id", token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body();
        let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
        let frame = String::from_utf8(frame.to_vec()).unwrap();
        assert!(frame.contains("event: log"), "{frame}");
        assert!(frame.contains("match"), "{frame}");
        assert!(
            !frame_id(&frame)
                .chars()
                .all(|character| character.is_ascii_digit())
        );
        drop(body);
    }

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn attribute_filter_validation_and_raw_target_limits_precede_execution() {
    let (_directory, storage, app) = setup_with(|config| config.max_field_bytes = 64);
    for prefix in ["", "/indexes/default"] {
        for endpoint in ["", "/count", "/facets", "/histogram", "/tail"] {
            let base = if endpoint == "/tail" {
                String::new()
            } else {
                "from=0&to=2000&".into()
            };
            for filter in [
                "",
                "null",
                "{}",
                r#"{"attributes":[],"attributes":[]}"#,
                r#"{"attributes":[{"key":"runId","op":"ne","value":"secret"}]}"#,
                r#"{"attributes":[{"key":"runId","op":"eq","value":null}]}"#,
                r#"{"attributes":[],"extra":true}"#,
            ] {
                let path = format!("{prefix}/logs{endpoint}?{base}filter={}", encoded(filter));
                let (status, result) = call(
                    &app,
                    "GET",
                    &path,
                    "reader-token-123456",
                    "application/json",
                    String::new(),
                )
                .await;
                assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {result}");
                assert!(!result["error"].as_str().unwrap().contains("secret"));
            }

            let duplicate = format!(
                "{prefix}/logs{endpoint}?{base}filter=%7B%22attributes%22%3A%5B%5D%7D\
                    &filter=%7B%22attributes%22%3A%5B%5D%7D"
            );
            assert_eq!(
                call(
                    &app,
                    "GET",
                    &duplicate,
                    "reader-token-123456",
                    "application/json",
                    String::new()
                )
                .await
                .0,
                StatusCode::BAD_REQUEST
            );

            let denied = format!(
                "{prefix}/logs{endpoint}?{base}source=beta&filter=%7B%22attributes%22%3A%5B%5D%7D"
            );
            assert_eq!(
                call(
                    &app,
                    "GET",
                    &denied,
                    "reader-token-123456",
                    "application/json",
                    String::new()
                )
                .await
                .0,
                StatusCode::FORBIDDEN
            );

            // No filter is needed to trigger the encoded request-target limit.
            let mut path = format!("{prefix}/logs{endpoint}?{base}message=");
            path.push_str(&"x".repeat(32_769 - path.len()));
            let (status, result) = call(
                &app,
                "GET",
                &path,
                "reader-token-123456",
                "application/json",
                String::new(),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(result["error"], "request target exceeds 32768 bytes");
        }
    }

    // The exact encoded-target boundary reaches endpoint validation.
    let mut path = "/logs/count?from=0&to=2000&message=".to_owned();
    path.push_str(&"x".repeat(32_768 - path.len()));
    let (status, result) = call(
        &app,
        "GET",
        &path,
        "reader-token-123456",
        "application/json",
        String::new(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(result["error"], "query field exceeds limit");

    for cursor in ["0".to_owned(), "1:2:3".to_owned(), "a".repeat(1025)] {
        let path = format!("/logs?from=0&to=2000&cursor={}", encoded(&cursor));
        assert_eq!(
            call(
                &app,
                "GET",
                &path,
                "reader-token-123456",
                "application/json",
                String::new()
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/logs/tail")
                    .header("authorization", "Bearer reader-token-123456")
                    .header("last-event-id", cursor)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response.headers()["content-type"], "application/json");
    }

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn search_cursors_bind_effective_filters_and_accept_equivalent_selections() {
    let (_directory, storage, app) = setup_with(|config| config.max_field_bytes = 8);
    let events = json!([
        {"time":100,"level":40,"msg":"same","service":"jobs","runId":1,"ok":true},
        {"time":200,"level":40,"msg":"same","service":"jobs","runId":1,"ok":true},
        {"time":300,"level":40,"msg":"same","service":"jobs","runId":1,"ok":true}
    ]);
    assert_eq!(
        call(
            &app,
            "POST",
            "/logs/ingest",
            "producer-token-123456",
            "application/json",
            events.to_string()
        )
        .await
        .0,
        StatusCode::OK
    );

    let filter = r#"{"attributes":[{"key":"runId","op":"eq","value":1},{"key":"ok","op":"eq","value":true}]}"#;
    let path = format!(
        "/logs?from=0&to=1000&limit=1&service=jobs&filter={}",
        encoded(filter)
    );
    let (status, first) = call(
        &app,
        "GET",
        &path,
        "reader-token-123456",
        "application/json",
        String::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let token = encoded(first["next_cursor"].as_str().unwrap());
    let equivalent = r#"{"attributes":[{"key":"ok","op":"eq","value":true},{"key":"runId","op":"eq","value":1.0},{"key":"runId","op":"eq","value":1e0}]}"#;
    let path = format!(
        "/logs?from=0&to=1000&limit=1&service=jobs&source=alpha,alpha&filter={}&cursor={token}",
        encoded(equivalent)
    );
    let (status, second) = call(
        &app,
        "GET",
        &path,
        "reader-token-123456",
        "application/json",
        String::new(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "cursor must not use max_field_bytes: {second}"
    );
    assert_ne!(first["events"][0]["id"], second["events"][0]["id"]);

    for changed in [
        "from=1&to=1000&limit=1&service=jobs".to_owned(),
        "from=0&to=1001&limit=1&service=jobs".to_owned(),
        "from=0&to=1000&limit=2&service=jobs".to_owned(),
        "from=0&to=1000&limit=1&service=other".to_owned(),
        "from=0&to=1000&limit=1&service=jobs&logger=api".to_owned(),
        "from=0&to=1000&limit=1&service=jobs&host=node".to_owned(),
        "from=0&to=1000&limit=1&service=jobs&message=same".to_owned(),
        "from=0&to=1000&limit=1&service=jobs&min_level=30".to_owned(),
    ] {
        let path = format!("/logs?{changed}&filter={}&cursor={token}", encoded(filter));
        let (status, result) = call(
            &app,
            "GET",
            &path,
            "reader-token-123456",
            "application/json",
            String::new(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {result}");
        assert!(result["error"].as_str().unwrap().contains("match query"));
    }

    let path = format!(
        "/indexes/default/logs?from=0&to=1000&limit=1&service=jobs&filter={}&cursor={token}",
        encoded(filter)
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &path,
            "reader-token-123456",
            "application/json",
            String::new()
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );

    for value in [json!("1"), json!(2)] {
        let changed = json!({
            "attributes": [{"key":"runId",
                                                "op":"eq",
                                                "value":value},
                                                {"key":"ok",
                                                "op":"eq",
                                                "value":true}],
        });
        let path = format!(
            "/logs?from=0&to=1000&limit=1&service=jobs&filter={}&cursor={token}",
            encoded(&changed.to_string())
        );
        assert_eq!(
            call(
                &app,
                "GET",
                &path,
                "reader-token-123456",
                "application/json",
                String::new()
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn attribute_tail_checkpoints_resume_logs_and_reject_changed_queries() {
    use http_body_util::BodyExt;

    let (_directory, storage, app) = setup_with(|config| config.tail_poll_ms = 10_000);
    let filter = r#"{"attributes":[{"key":"runId","op":"eq","value":"run-1"}]}"#;
    let path = format!("/logs/tail?filter={}", encoded(filter));
    let request = |token: Option<&str>| {
        let mut request = Request::builder()
            .uri(&path)
            .header("authorization", "Bearer reader-token-123456");
        if let Some(token) = token {
            request = request.header("last-event-id", token);
        }
        request.body(Body::empty()).unwrap()
    };
    let response = app.clone().oneshot(request(None)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body();
    let checkpoint = stream.frame().await.unwrap().unwrap().into_data().unwrap();
    let checkpoint = String::from_utf8(checkpoint.to_vec()).unwrap();
    assert!(checkpoint.contains("event: cursor"));

    // Only nonmatching events arrive. Their committed IDs still advance the checkpoint.
    assert_eq!(
        call(
            &app,
            "POST",
            "/logs/ingest",
            "producer-token-123456",
            "application/json",
            json!([{"time":100,"level":30,"msg":"hidden","runId":"other"}]).to_string()
        )
        .await
        .0,
        StatusCode::OK
    );

    let checkpoint = tokio::time::timeout(std::time::Duration::from_millis(500), stream.frame())
        .await
        .expect("commit wakes filtered stream")
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    let checkpoint = String::from_utf8(checkpoint.to_vec()).unwrap();
    assert!(checkpoint.contains("event: cursor"));
    let checkpoint = frame_id(&checkpoint).to_owned();
    assert_eq!(
        checkpoint,
        tail_token(
            1,
            None,
            &["alpha"],
            json!({
                "attributes": serde_json::from_str::<Value>(filter).unwrap(),
            })
        )
    );
    drop(stream);

    assert_eq!(
        call(
            &app,
            "POST",
            "/logs/ingest",
            "producer-token-123456",
            "application/json",
            json!([
                {"time":50,"level":30,"msg":"late first","runId":"run-1"},
                {"time":200,"level":30,"msg":"hidden again","runId":"other"},
                {"time":10,"level":30,"msg":"late second","runId":"run-1"}
            ])
            .to_string()
        )
        .await
        .0,
        StatusCode::OK
    );

    let response = app
        .clone()
        .oneshot(request(Some(&checkpoint)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body();
    let first = stream.frame().await.unwrap().unwrap().into_data().unwrap();
    let first = String::from_utf8(first.to_vec()).unwrap();
    assert!(first.contains("late first"));
    let first_token = frame_id(&first).to_owned();
    drop(stream);

    // Resume from an individual log, then from the caught-up checkpoint, with no duplicate.
    let response = app
        .clone()
        .oneshot(request(Some(&first_token)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body();
    let second = stream.frame().await.unwrap().unwrap().into_data().unwrap();
    let second = String::from_utf8(second.to_vec()).unwrap();
    assert!(second.contains("late second"));
    assert!(!second.contains("late first"));
    let last = stream.frame().await.unwrap().unwrap().into_data().unwrap();
    let last = String::from_utf8(last.to_vec()).unwrap();
    assert!(last.contains("event: cursor"));
    drop(stream);

    let response = app
        .clone()
        .oneshot(request(Some(frame_id(&last))))
        .await
        .unwrap();
    let mut stream = response.into_body();
    let frame = stream.frame().await.unwrap().unwrap().into_data().unwrap();
    assert!(
        String::from_utf8(frame.to_vec())
            .unwrap()
            .contains("event: cursor")
    );
    drop(stream);

    let changed = format!(
        "/logs/tail?filter={}",
        encoded(&filter.replace("run-1", "run-2"))
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(changed)
                .header("authorization", "Bearer reader-token-123456")
                .header("last-event-id", &first_token)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.headers()["content-type"], "application/json");

    let search = format!("/logs?from=0&to=1000&cursor={}", encoded(&first_token));
    assert_eq!(
        call(
            &app,
            "GET",
            &search,
            "reader-token-123456",
            "application/json",
            String::new()
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn tail_pending_byte_budget_counts_full_tokens_and_bounds_terminal_frames() {
    use http_body_util::BodyExt;

    let (_directory, storage, app) = setup();
    assert_eq!(
        call(
            &app,
            "POST",
            "/logs/ingest",
            "producer-token-123456",
            "application/json",
            json!([{"time":100,"level":30,"msg":"one"}]).to_string()
        )
        .await
        .0,
        StatusCode::OK
    );

    let (_, page) = call(
        &app,
        "GET",
        "/logs?from=0&to=1000",
        "reader-token-123456",
        "application/json",
        String::new(),
    )
    .await;
    let token = tail_token(1, None, &["alpha"], json!({}));
    let exact_bytes = page["events"][0].to_string().len() + token.len() + 24;

    let budget_router = |budget| {
        let mut config = Config::default();
        config
            .read_tokens
            .insert("reader-token-123456".into(), vec!["alpha".into()]);
        config.max_tail_buffer_bytes = budget;
        router(storage.clone(), config)
    };

    let request = || {
        Request::builder()
            .uri("/logs/tail")
            .header("authorization", "Bearer reader-token-123456")
            .header("last-event-id", tail_token(0, None, &["alpha"], json!({})))
            .body(Body::empty())
            .unwrap()
    };
    let response = budget_router(exact_bytes - 1)
        .oneshot(request())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let response = budget_router(exact_bytes).oneshot(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body();
    let frame = stream.frame().await.unwrap().unwrap().into_data().unwrap();
    assert_eq!(frame.len(), exact_bytes);
    drop(stream);

    let checkpoint_bytes = token.len() + 36;
    let checkpoint_request = || {
        Request::builder()
            .uri("/logs/tail")
            .header("authorization", "Bearer reader-token-123456")
            .header("last-event-id", &token)
            .body(Body::empty())
            .unwrap()
    };
    assert_eq!(
        budget_router(checkpoint_bytes - 1)
            .oneshot(checkpoint_request())
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );

    let response = budget_router(checkpoint_bytes)
        .oneshot(checkpoint_request())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body();
    let checkpoint = stream.frame().await.unwrap().unwrap().into_data().unwrap();
    assert_eq!(checkpoint.len(), checkpoint_bytes);

    // Once SSE has started, a newly committed event exceeding the budget is a bounded gap.
    assert_eq!(
        call(
            &app,
            "POST",
            "/logs/ingest",
            "producer-token-123456",
            "application/json",
            json!([{"time":200,"level":30,"msg":"x".repeat(2048)}]).to_string()
        )
        .await
        .0,
        StatusCode::OK
    );

    let frame = tokio::time::timeout(std::time::Duration::from_secs(2), stream.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert!(frame.len() <= checkpoint_bytes);
    assert!(
        String::from_utf8(frame.to_vec())
            .unwrap()
            .contains("event: gap")
    );
    assert!(stream.frame().await.is_none());

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn real_http_attribute_encoding_and_tail_resume_smoke() {
    let (_directory, storage, app) = setup();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });

    let client = reqwest::Client::new();
    let filter = r#"{"attributes":[{"key":"runId","op":"eq","value":"run +/☃&source=beta"}]}"#;
    let mut url = reqwest::Url::parse(&format!("{origin}/indexes/default/logs/tail")).unwrap();
    url.query_pairs_mut().append_pair("filter", filter);
    let mut tail = client
        .get(url.clone())
        .bearer_auth("reader-token-123456")
        .send()
        .await
        .unwrap();
    assert_eq!(tail.status(), 200);
    let checkpoint = tail.chunk().await.unwrap().unwrap();
    let checkpoint = String::from_utf8(checkpoint.to_vec()).unwrap();
    assert!(checkpoint.contains("event: cursor"));
    let token = frame_id(&checkpoint).to_owned();
    drop(tail);

    let response = client
        .post(format!("{origin}/indexes/default/logs/ingest"))
        .bearer_auth("producer-token-123456")
        .header("content-type", "application/json")
        .body(
            json!([
                {"time":100,"level":30,"msg":"over HTTP","runId":"run +/☃&source=beta"},
                {"time":200,"level":30,"msg":"hidden","runId":"other"}
            ])
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    let mut resumed = client
        .get(url)
        .bearer_auth("reader-token-123456")
        .header("last-event-id", token)
        .send()
        .await
        .unwrap();
    assert_eq!(resumed.status(), 200);

    let frame = tokio::time::timeout(std::time::Duration::from_secs(2), resumed.chunk())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let frame = String::from_utf8(frame.to_vec()).unwrap();
    assert!(frame.contains("over HTTP"), "{frame}");
    assert!(!frame.contains("hidden"), "{frame}");
    drop(resumed);

    let mut url = reqwest::Url::parse(&format!("{origin}/indexes/default/logs/count")).unwrap();
    url.query_pairs_mut()
        .extend_pairs([("from", "0"), ("to", "1000"), ("filter", filter)]);
    let response = client
        .get(url)
        .bearer_auth("reader-token-123456")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    let result: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(result["count"], 1);

    stop.send(()).unwrap();
    server.await.unwrap();

    storage.shutdown().await.unwrap();
}

async fn metrics_text(app: &Router) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("authorization", "Bearer admin-token-1234567")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    String::from_utf8(
        to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

fn assert_metrics_declared_once_before_samples(text: &str) {
    use std::collections::BTreeSet;

    let mut help = BTreeSet::new();
    let mut kinds = BTreeSet::new();
    let mut series = BTreeSet::new();
    for line in text.lines() {
        if let Some(declaration) = line.strip_prefix("# HELP ") {
            let family = declaration.split_once(' ').unwrap().0;
            assert!(help.insert(family), "duplicate HELP for {family}");
        } else if let Some(declaration) = line.strip_prefix("# TYPE ") {
            let family = declaration.split_once(' ').unwrap().0;
            assert!(kinds.insert(family), "duplicate TYPE for {family}");
        } else if !line.starts_with('#') && !line.is_empty() {
            let sample = line.split_once(' ').unwrap().0;
            let metric = sample.split('{').next().unwrap();
            let family = ["_bucket", "_sum", "_count"]
                .iter()
                .find_map(|suffix| {
                    metric
                        .strip_suffix(suffix)
                        .filter(|family| kinds.contains(family))
                })
                .unwrap_or(metric);
            if !family.starts_with("logbrook_forward_") {
                assert!(help.contains(family), "sample precedes HELP: {line}");
            }

            assert!(kinds.contains(family), "sample precedes TYPE: {line}");
            assert!(series.insert(sample), "duplicate series: {sample}");
        }
    }

    assert!(!series.is_empty());
    assert!(help.is_subset(&kinds));
}

#[tokio::test]
async fn metrics_scrape_declares_each_family_once_before_samples() {
    let (_directory, storage, app) = setup();
    let text = metrics_text(&app).await;
    assert!(text.contains("# TYPE logbrook_forward_"));
    assert_metrics_declared_once_before_samples(&text);

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn indexed_metrics_declare_storage_before_alphabetically_first_index() {
    use logbrook::{http::router_with_registry, indexes::IndexRegistry};

    let directory = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = directory.path().into();
    config
        .ingest_tokens
        .insert("producer-token-123456".into(), "alpha".into());
    config
        .read_tokens
        .insert("admin-token-1234567".into(), vec![]);

    let registry = IndexRegistry::open(config.clone()).await.unwrap();
    registry.ensure("aaa").await.unwrap();

    let app = router_with_registry(registry.clone(), config);
    let text = metrics_text(&app).await;
    assert!(text.contains("index=\"aaa\""));
    assert!(text.contains("index=\"default\""));

    for index in ["aaa", "default"] {
        for role in ["reader", "maintenance"] {
            let labels = format!("{{index=\"{index}\",role=\"{role}\",worker=\"0\"}}");
            assert!(text.contains(&format!(
                "logbrook_storage_query_progress_available{labels} 0\n"
            )));
            assert!(text.contains(&format!(
                "logbrook_storage_query_progress_fraction{labels} NaN\n"
            )));
        }
    }

    assert_metrics_declared_once_before_samples(&text);

    registry.shutdown().await.unwrap();
}

#[test]
fn openapi_tail_resume_schemas_describe_versioned_tokens() {
    let spec: Value = serde_json::from_str(include_str!("../src/openapi.json")).unwrap();
    for (path, index, expected) in [
        ("/logs/tail", None, r"^v1\.[A-Za-z0-9_-]+$"),
        (
            "/indexes/{index}/logs/tail",
            Some("orders"),
            r"^[a-z0-9][a-z0-9_-]*:v1\.[A-Za-z0-9_-]+$",
        ),
    ] {
        let parameter = spec["paths"][path]["get"]["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .find(|parameter| parameter["name"] == "Last-Event-ID")
            .unwrap();
        assert_eq!(parameter["schema"]["pattern"], expected);
        assert_eq!(parameter["schema"]["maxLength"], 1024);
        assert!(
            !parameter["description"]
                .as_str()
                .unwrap()
                .contains("search")
        );

        let token = tail_token(1, index, &[], json!({}));
        let unscoped = index.map_or(token.as_str(), |index| {
            token.strip_prefix(&format!("{index}:")).unwrap()
        });
        let payload = unscoped.strip_prefix("v1.").unwrap();
        assert!(!payload.is_empty());
        assert!(
            payload
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        );
        assert!(token.len() <= parameter["schema"]["maxLength"].as_u64().unwrap() as usize);
    }
}
