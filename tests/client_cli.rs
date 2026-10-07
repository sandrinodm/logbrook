use std::{
    io::{Read, Write},
    net::TcpListener,
    process::{Command, Output},
    time::Duration,
};

fn client(args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_logbrook"));
    for (key, _) in std::env::vars().filter(|(key, _)| key.starts_with("LOGBROOK_")) {
        command.env_remove(key);
    }

    command.env("LOGBROOK_TOKEN", "synthetic-default");
    command.env("TOKIO_WORKER_THREADS", "2");
    command.args(args);
    command
}

fn successful(output: &Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn stub(response: Vec<u8>) -> (String, std::thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut request = Vec::new();
        let mut chunk = [0; 4096];
        while !request.ends_with(b"\r\n\r\n") {
            let count = stream.read(&mut chunk).unwrap();

            assert_ne!(count, 0);
            request.extend_from_slice(&chunk[..count]);

            assert!(request.len() < 65_536);
        }

        let _ = stream.write_all(&response);
        String::from_utf8(request).unwrap()
    });
    (url, task)
}

fn json_response(body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    )
    .into_bytes()
}

#[test]
fn remote_list_bypasses_local_config_and_formats_sizes() {
    let body = "{\"indexes\":[{\"name\":\"app\",\"size_bytes\":1536,\"max_size_bytes\":4096,\
                \"retention_ms\":86400000,\"ready\":true},{\"name\":\"scoped\"}]}";
    let (url, server) = stub(json_response(body));
    let output = client(&["indexes", "list", "--url", &url])
        .env_remove("LOGBROOK_TOKEN")
        .env("LOGBROOK_DATA_DIR", "/invalid[local]")
        .env("LOGBROOK_READ_TOKEN", "synthetic-read")
        .env("LOGBROOK_ADMIN_TOKEN", "synthetic-admin")
        .output()
        .unwrap();

    successful(&output);

    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(stdout.contains("app"));
    assert!(stdout.contains("1.5 KiB"));
    assert!(stdout.contains("scoped"));

    let request = server.join().unwrap().to_lowercase();

    assert!(request.starts_with("get /indexes http/1.1"));
    assert!(request.contains("authorization: bearer synthetic-admin\r\n"));
}

#[test]
fn query_encodes_filters_and_preserves_cursor_in_json() {
    let body = r#"{"events":[],"next_cursor":"app:opaque+/="}"#;
    let (url, server) = stub(json_response(body));
    let output = client(&[
        "--url",
        &format!("{url}/prefix/"),
        "--json",
        "query",
        "app",
        "--from",
        "1000",
        "--to",
        "2000",
        "--message",
        "a&source=other +/☃",
        "--service",
        "worker&api",
        "--limit",
        "7",
        "--cursor",
        "app:old+/=",
        "--logger",
        "log",
        "--host",
        "node",
        "--source",
        "app",
        "--min-level",
        "40",
    ])
    .env_remove("LOGBROOK_TOKEN")
    .env("LOGBROOK_READ_TOKEN", "synthetic-read")
    .env("LOGBROOK_ADMIN_TOKEN", "synthetic-admin")
    .output()
    .unwrap();

    successful(&output);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::from_str::<serde_json::Value>(body).unwrap()
    );

    let request = server.join().unwrap();
    let first = request.lines().next().unwrap();

    assert!(first.starts_with("GET /prefix/indexes/app/logs?"));
    assert!(first.contains("message=a%26source%3Dother+%2B%2F%E2%98%83"));
    assert!(first.contains("service=worker%26api"));
    assert!(first.contains("cursor=app%3Aold%2B%2F%3D"));
    assert!(first.contains("limit=7"));
    assert!(
        request
            .to_lowercase()
            .contains("authorization: bearer synthetic-read\r\n")
    );
}

#[test]
fn count_defaults_to_one_hour_and_explicit_token_has_precedence() {
    let (url, server) = stub(json_response(r#"{"count":12}"#));
    let output = client(&[
        "--url",
        &url,
        "--token",
        "synthetic-explicit",
        "count",
        "default",
    ])
    .env("LOGBROOK_TOKEN", "synthetic-generic")
    .env("LOGBROOK_READ_TOKEN", "synthetic-role")
    .output()
    .unwrap();

    successful(&output);
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), "12");

    let request = server.join().unwrap();
    let target = request
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap();
    let fields: std::collections::HashMap<_, _> = target
        .split('?')
        .nth(1)
        .unwrap()
        .split('&')
        .map(|pair| pair.split_once('=').unwrap())
        .collect();

    assert_eq!(
        fields["to"].parse::<i64>().unwrap() - fields["from"].parse::<i64>().unwrap(),
        3_600_000
    );
    assert!(!fields.contains_key("limit"));
    assert!(
        request
            .to_lowercase()
            .contains("authorization: bearer synthetic-explicit\r\n")
    );
}

#[test]
fn generic_env_token_overrides_role_and_create_uses_ingest_fallback() {
    for (generic, expected) in [
        (Some("synthetic-generic"), "synthetic-generic"),
        (None, "synthetic-ingest"),
    ] {
        let (url, server) = stub(json_response(r#"{"name":"app"}"#));
        let mut command = client(&["indexes", "create", "app", "--url", &url, "--json"]);
        command
            .env_remove("LOGBROOK_TOKEN")
            .env("LOGBROOK_INGEST_TOKEN", "synthetic-ingest");
        if let Some(generic) = generic {
            command.env("LOGBROOK_TOKEN", generic);
        }

        successful(&command.output().unwrap());

        let request = server.join().unwrap().to_lowercase();

        assert!(request.starts_with("put /indexes/app http/1.1"));
        assert!(request.contains(&format!("authorization: bearer {expected}\r\n")));
    }
}

#[test]
fn delete_confirmation_precedes_network_and_config_is_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();

    let url = format!("http://{}", listener.local_addr().unwrap());
    let output = client(&["--url", &url, "indexes", "delete", "app"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--yes"));
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );

    let output = client(&[
        "--url",
        &url,
        "--config",
        "/does-not-exist.toml",
        "indexes",
        "list",
    ])
    .output()
    .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--config is for local commands"));
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[test]
fn delete_uses_only_admin_role_and_supports_explicit_authorization() {
    let (url, server) = stub(json_response(r#"{"deleted":"app"}"#));
    let output = client(&["--url", &url, "--json", "indexes", "delete", "app", "--yes"])
        .env_remove("LOGBROOK_TOKEN")
        .env("LOGBROOK_READ_TOKEN", "synthetic-read")
        .env("LOGBROOK_INGEST_TOKEN", "synthetic-ingest")
        .env("LOGBROOK_ADMIN_TOKEN", "synthetic-admin")
        .output()
        .unwrap();

    successful(&output);

    let request = server.join().unwrap().to_lowercase();

    assert!(request.starts_with("delete /indexes/app http/1.1"));
    assert!(request.contains("authorization: bearer synthetic-admin\r\n"));
}

#[test]
fn invalid_urls_ranges_and_names_fail_without_exposing_secrets() {
    for url in [
        "file:///tmp/app",
        "ftp://localhost",
        "http://user:synthetic-secret@localhost",
        "http://@localhost",
        "http:/@localhost",
        "http://localhost?token=synthetic-secret",
        "http://localhost/#synthetic-secret",
    ] {
        let output = client(&["--url", url, "indexes", "list"]).output().unwrap();

        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("synthetic-secret"));
    }

    for args in [
        vec!["query", "app", "--from", "100", "--to", "10"],
        vec!["query", "app", "--since", "0h"],
        vec!["query", "app", "--since", "999999999999999999w"],
        vec!["query", "../other", "--since", "1h"],
        vec!["count", "app", "--from", "0"],
        vec!["query", "app", "--since", "1h", "--from", "0", "--to", "10"],
    ] {
        let output = client(&args).output().unwrap();

        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
    }
}

#[test]
fn redirect_is_not_followed_and_error_body_cannot_echo_token() {
    let destination = TcpListener::bind("127.0.0.1:0").unwrap();
    destination.set_nonblocking(true).unwrap();

    let response = format!(
        "HTTP/1.1 302 Found\r\n\
         Location: http://{}/secret\r\n\
         Content-Length: 0\r\n\
         Connection: close\r\n\
         \r\n",
        destination.local_addr().unwrap()
    );
    let (url, server) = stub(response.into_bytes());
    let output = client(&[
        "--url",
        &url,
        "--token",
        "synthetic-secret",
        "indexes",
        "list",
    ])
    .output()
    .unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("302"));
    server.join().unwrap();

    assert_eq!(
        destination.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );

    let body = r#"{"error":"rejected synthetic-secret"}"#;
    let (url, server) = stub(
        format!(
            "HTTP/1.1 401 Unauthorized\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n\
             {body}",
            body.len()
        )
        .into_bytes(),
    );
    let output = client(&[
        "--url",
        &url,
        "--token",
        "synthetic-secret",
        "indexes",
        "list",
    ])
    .output()
    .unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());

    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(stderr.contains("401"));
    assert!(!stderr.contains("synthetic-secret"));
    server.join().unwrap();
}

#[test]
fn malformed_and_oversized_responses_fail_on_stderr() {
    let mut oversized = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n".to_vec();
    oversized.resize(17 * 1024 * 1024, b' ');
    for response in [
        json_response("not-json"),
        b"HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\nConnection: close\r\n\r\n".to_vec(),
        oversized,
    ] {
        let (url, server) = stub(response);
        let output = client(&["--url", &url, "--json", "indexes", "list"])
            .output()
            .unwrap();

        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
        server.join().unwrap();
    }
}

#[test]
fn deadline_includes_a_stalled_response_body() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (sent, received) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0; 4096];

        assert!(stream.read(&mut request).unwrap() > 0);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{")
            .unwrap();
        sent.send(std::time::Instant::now()).unwrap();
        std::thread::sleep(Duration::from_millis(1500));
    });
    let output = client(&["--url", &url, "--timeout", "1", "indexes", "list"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());

    let started = received.recv().unwrap();

    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(String::from_utf8_lossy(&output.stderr).contains("deadline"));
    server.join().unwrap();
}

#[test]
fn environment_url_and_json_show_are_supported() {
    let body = r#"{"name":"app","size_bytes":1234,"ready":true}"#;
    let (url, server) = stub(json_response(body));
    let output = client(&["indexes", "show", "app", "--json"])
        .env("LOGBROOK_URL", &url)
        .output()
        .unwrap();

    successful(&output);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::from_str::<serde_json::Value>(body).unwrap()
    );
    assert!(
        server
            .join()
            .unwrap()
            .starts_with("GET /indexes/app HTTP/1.1")
    );
}

#[test]
fn credentials_stay_out_of_help_and_invalid_header_errors() {
    let output = client(&["--help"])
        .env("LOGBROOK_TOKEN", "synthetic-help-secret")
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("synthetic-help-secret"));

    let output = client(&[
        "--token",
        "synthetic-header-secret\r\nInjected: value",
        "indexes",
        "list",
    ])
    .output()
    .unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("synthetic-header-secret"));
}

#[test]
fn delete_does_not_fall_back_to_read_or_ingest_and_query_does_not_use_admin() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();

    let url = format!("http://{}", listener.local_addr().unwrap());
    let output = client(&["--url", &url, "indexes", "delete", "app", "--yes"])
        .env_remove("LOGBROOK_TOKEN")
        .env("LOGBROOK_INGEST_TOKEN", "synthetic-ingest")
        .env("LOGBROOK_READ_TOKEN", "synthetic-read")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("LOGBROOK_ADMIN_TOKEN"));
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );

    let output = client(&["--url", &url, "query", "app"])
        .env_remove("LOGBROOK_TOKEN")
        .env("LOGBROOK_ADMIN_TOKEN", "synthetic-admin")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("LOGBROOK_READ_TOKEN"));
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}
