use logbrook::{config::Config, model::Query, storage::Storage};
use std::process::Command;

#[tokio::test]
async fn ndjson_import_accepts_event_at_body_limit_without_empty_batch_flush() {
    let directory = tempfile::tempdir().unwrap();
    let event = r#"{"time":1700000000000,"level":30,"msg":"boundary"}"#;
    let mut config = Config {
        max_body_bytes: event.len(),
        max_event_bytes: event.len(),
        max_field_bytes: 16,
        ..Config::default()
    };
    config.storage.data_dir = directory.path().join("data");
    config
        .ingest_tokens
        .insert("synthetic-cli-write-token".into(), "app".into());
    config
        .read_tokens
        .insert("synthetic-cli-read-token".into(), vec![]);

    let config_file = directory.path().join("config.toml");
    std::fs::write(&config_file, toml::to_string(&config).unwrap()).unwrap();

    let input = directory.path().join("input.ndjson");
    std::fs::write(&input, format!("{event}\r\n{event}")).unwrap();

    let mut command = Command::new(env!("CARGO_BIN_EXE_logbrook"));
    for (key, _) in std::env::vars().filter(|(key, _)| key.starts_with("LOGBROOK_")) {
        command.env_remove(key);
    }

    let result = command
        .args([
            "--config",
            config_file.to_str().unwrap(),
            "import",
            input.to_str().unwrap(),
            "--ndjson",
            "--source",
            "app",
        ])
        .output()
        .unwrap();

    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stdout)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&result.stdout).unwrap(),
        serde_json::json!({ "imported": 2 })
    );

    let storage = Storage::open(config.for_index("default").unwrap().storage).unwrap();

    assert_eq!(
        storage
            .count(Query {
                from: 0,
                to: i64::MAX,
                limit: 100,
                ..Query::default()
            })
            .await
            .unwrap(),
        2
    );

    storage.shutdown().await.unwrap();
}

#[test]
fn configuration_failure_reports_diagnostics_only_on_stderr() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing.toml");

    let result = Command::new(env!("CARGO_BIN_EXE_logbrook"))
        .args(["--config", missing.to_str().unwrap(), "check-config"])
        .env("RUST_LOG", "logbrook=error")
        .output()
        .unwrap();

    assert!(!result.status.success());
    assert!(result.stdout.is_empty());

    let diagnostic: serde_json::Value = serde_json::from_slice(&result.stderr).unwrap();

    assert_eq!(diagnostic["level"], "ERROR");
    assert!(
        diagnostic["fields"]["error"]
            .as_str()
            .unwrap()
            .contains("cannot read config")
    );
}

#[test]
fn healthcheck_reads_only_bind_and_needs_no_credentials() {
    use std::io::{Read, Write};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let stub = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut request = [0u8; 1024];
        let count = stream.read(&mut request).unwrap();

        assert!(request[..count].starts_with(b"GET /ready HTTP/1.1"));
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            .unwrap();
    });
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("probe.toml");
    std::fs::write(
        &file,
        format!("bind = \"{address}\"\n[storage]\ndata_dir = \"bad[glob]\"\n"),
    )
    .unwrap();

    let mut command = Command::new(env!("CARGO_BIN_EXE_logbrook"));
    for (key, _) in std::env::vars().filter(|(key, _)| key.starts_with("LOGBROOK_")) {
        command.env_remove(key);
    }

    let result = command
        .args(["--config", file.to_str().unwrap(), "healthcheck"])
        .output()
        .unwrap();

    assert!(
        result.status.success(),
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    stub.join().unwrap();
}
