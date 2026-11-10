//! HTTP regression tests for attribute projection configuration and filtering.
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use logbrook::{config::Config, http::router, storage::Storage};
use serde_json::{Value, json};
use tower::ServiceExt;

fn encoded(text: &str) -> String {
    let mut url = reqwest::Url::parse("http://localhost/").unwrap();
    url.query_pairs_mut().append_pair("v", text);
    url.query().unwrap().strip_prefix("v=").unwrap().to_owned()
}

async fn get(app: &Router, path: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(path)
                .header("authorization", "Bearer reader-token-123456")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn durable_projection_keeps_http_auth_cursor_binding_and_selection_semantics() {
    for promoted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.data_dir = dir.path().into();
        config.storage.reader_threads = 1;
        config.storage.duckdb_threads = 1;
        config.storage.promoted_string_keys = if promoted {
            vec!["runId".into(), "userId".into()]
        } else {
            vec![]
        };
        config.retention_ms = i64::MAX / 2;
        config
            .read_tokens
            .insert("reader-token-123456".into(), vec!["alpha".into()]);

        let storage = Storage::open(config.storage.clone()).unwrap();
        for source in ["alpha", "beta"] {
            let events = logbrook::ingest::normalize(
                &serde_json::to_vec(&json!([
                    {"time":10,"level":30,"msg":"first","runId":"r","userId":"a"},
                    {"time":20,"level":30,"msg":"second","runId":"r","userId":"a"},
                    {"time":30,"level":30,"msg":"wrong type","runId":true,"userId":"a"}
                ]))
                .unwrap(),
                false,
                &config,
            )
            .unwrap();
            storage.append(source.into(), events).await.unwrap();
        }

        storage.archive(15).await.unwrap();

        let app = router(storage.clone(), config);

        let filter = encoded(
            r#"{"attributes":[{"key":"runId","op":"eq","value":"r"},{"key":"userId","op":"eq","value":"a"}]}"#,
        );
        let path = format!("/logs?from=0&to=100&limit=1&filter={filter}");
        let (status, first) = get(&app, &path).await;
        assert_eq!(status, StatusCode::OK, "{first}");
        assert_eq!(first["events"][0]["message"], "second");
        assert_eq!(first["events"][0]["source"], "alpha");

        let cursor = encoded(first["next_cursor"].as_str().unwrap());
        let (status, second) = get(&app, &format!("{path}&cursor={cursor}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(second["events"][0]["message"], "first");

        let equivalent = encoded(
            r#"{"attributes":[{"key":"userId","op":"eq","value":"a"},{"key":"runId","op":"eq","value":"r"}]}"#,
        );
        assert_eq!(
            get(
                &app,
                &format!("/logs?from=0&to=100&limit=1&filter={equivalent}&cursor={cursor}")
            )
            .await
            .0,
            StatusCode::OK
        );

        let changed = encoded(
            r#"{"attributes":[{"key":"runId","op":"eq","value":"other"},{"key":"userId","op":"eq","value":"a"}]}"#,
        );
        assert_eq!(
            get(
                &app,
                &format!("/logs?from=0&to=100&limit=1&filter={changed}&cursor={cursor}")
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );

        for endpoint in ["", "/count", "/histogram", "/facets", "/tail"] {
            let from_to = if endpoint == "/tail" {
                ""
            } else {
                "from=0&to=100&"
            };
            assert_eq!(
                get(
                    &app,
                    &format!("/logs{endpoint}?{from_to}source=beta&filter={filter}")
                )
                .await
                .0,
                StatusCode::FORBIDDEN,
                "{endpoint}"
            );
        }

        let (status, count) =
            get(&app, &format!("/logs/count?from=0&to=100&filter={filter}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(count["count"], 2);

        storage.shutdown().await.unwrap();
    }
}
