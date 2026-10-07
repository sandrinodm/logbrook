//! Exercise success and failure through the complete index-load command lifecycle.

#![cfg(unix)]

use logbrook_dev::{Result, runtime};
use serde_json::Value;
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    process::Command,
    task::JoinSet,
};

fn server_binary() -> PathBuf {
    std::env::current_exe()
        .expect("test executable")
        .parent()
        .expect("deps directory")
        .parent()
        .expect("profile directory")
        .join(format!("logbrook{}", std::env::consts::EXE_SUFFIX))
}

fn executable(path: &std::path::Path, text: &str) -> Result<()> {
    fs::write(path, text)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

fn quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "'\\''"))
}

fn command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_logbrook-dev"));
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("LOGBROOK_") {
            command.env_remove(key);
        }
    }
    command
}

async fn native_probe(fail_ingestion: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let lifecycle_path = directory.path().join("lifecycle");
    let wrapper = directory.path().join("server");
    let report_path = directory.path().join("report.json");

    // Record the actual server PID and owned directory before exec replaces the wrapper.
    executable(
        &wrapper,
        &format!(
            "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$$\" \"$LOGBROOK_DATA_DIR\" > {}\nexec {} \"$@\"\n",
            quote(lifecycle_path.to_str().unwrap()),
            quote(server_binary().to_str().unwrap())
        ),
    )?;

    let mut command = command();
    command
        .args(["index-load", "--binary"])
        .arg(&wrapper)
        .args(if fail_ingestion {
            // These legal probe options produce batches above the default HTTP body limit.
            [
                "--duration",
                "0.3",
                "--rate",
                "10000",
                "--batch-size",
                "1000",
                "--padding-bytes",
                "4096",
            ]
        } else {
            [
                "--duration",
                "1.5",
                "--rate",
                "200",
                "--batch-size",
                "10",
                "--padding-bytes",
                "32",
            ]
        })
        .args(["--query-rate", "6", "--output"])
        .arg(&report_path);

    let output = runtime::output(&mut command, 60).await?;
    let report: Value = serde_json::from_slice(&fs::read(&report_path)?)?;
    assert_eq!(output.status.success(), !fail_ingestion, "{report}");
    assert_eq!(report["passed"], !fail_ingestion, "{report}");
    assert_eq!(report["lifecycle"]["cleanup_succeeded"], true, "{report}");
    assert_eq!(report["lifecycle"]["exit_code"], 0, "{report}");
    assert_eq!(report["workload"]["worker_errors"], serde_json::json!([]));

    if fail_ingestion {
        assert!(
            report["workload"]["counts"]["ingest_failed_units"]
                .as_u64()
                .unwrap()
                > 0,
            "{report}"
        );
        assert!(
            report["workload"]["counts"]["ingest_status_413"]
                .as_u64()
                .unwrap()
                > 0,
            "{report}"
        );
    } else {
        for index in ["payments", "orders"] {
            assert_eq!(report["workload"]["final_counts"][index]["count"], 150);
            assert_eq!(report["workload"]["isolated_queries"][index], true);
        }
        let tails = report["workload"]["tails"].as_array().unwrap();
        assert_eq!(tails.len(), 4);
        assert!(
            tails.iter().all(|tail| tail["observed"] == 150
                && tail["gaps"] == 0
                && tail["isolated"] == true
                && tail["monotonic"] == true),
            "{tails:?}"
        );
    }

    let lifecycle = fs::read_to_string(lifecycle_path)?;
    let mut lines = lifecycle.lines();
    let pid = lines.next().expect("recorded PID");
    let data = PathBuf::from(lines.next().expect("recorded data directory"));
    let exists = runtime::output(Command::new("kill").args(["-0", pid]), 5).await?;
    assert!(
        !exists.status.success(),
        "server PID {pid} survived command cleanup"
    );
    assert!(
        !data.parent().unwrap().exists(),
        "fixture directory survived cleanup: {data:?}"
    );
    Ok(())
}

#[tokio::test]
async fn successful_index_cli_drains_tails_and_removes_native_fixture() -> Result<()> {
    native_probe(false).await
}

#[tokio::test]
async fn rejected_ingestion_still_stops_native_process_and_removes_data() -> Result<()> {
    native_probe(true).await
}

#[tokio::test]
async fn truncated_worker_requests_remove_mock_container_and_volume() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let trace = directory.path().join("docker.trace");
    let owned_root = directory.path().join("owned-root");
    let report_path = directory.path().join("report.json");
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();

    // Mock Docker alone; real sockets still exercise truncation and worker accounting.
    let docker = directory.path().join("docker");
    executable(
        &docker,
        &format!(
            r#"#!/bin/sh
set -eu
printf '%s\n' "$*" >> {trace}
case "$1 ${{2-}}" in
  'volume create') printf '%s\n' "$3" ;;
  'volume rm'|'rm -f'|'stop --time') : ;;
  'port '* ) printf '127.0.0.1:{port}\n' ;;
  'image inspect') printf '[{{"Id":"mock-image","Architecture":"amd64","Os":"linux"}}]\n' ;;
  'inspect --format') printf '0\n' ;;
  'stats --no-stream') printf '{{}}\n' ;;
  'run --rm') printf '0\0000\000' ;;
  'run -d')
    for argument do
      case "$argument" in
        */config.toml:/config.toml:ro)
          path=${{argument%:/config.toml:ro}}
          printf '%s\n' "${{path%/config.toml}}" > {owned_root}
          ;;
      esac
    done
    printf 'mock-container\n'
    ;;
  *) exit 2 ;;
esac
"#,
            trace = quote(trace.to_str().unwrap()),
            owned_root = quote(owned_root.to_str().unwrap())
        ),
    )?;

    let responder = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((mut socket, _)) = accepted else { break; };
                    connections.spawn(async move {
                        let mut headers = Vec::new();
                        while !headers.ends_with(b"\r\n\r\n") && headers.len() < 65536 {
                            let mut byte = [0];
                            if socket.read_exact(&mut byte).await.is_err() { return; }
                            headers.push(byte[0]);
                        }
                        let headers = String::from_utf8_lossy(&headers);
                        let path = headers.split_whitespace().nth(1).unwrap_or("/");
                        let length = headers.lines().find_map(|line| line.to_ascii_lowercase().strip_prefix("content-length:").and_then(|value| value.trim().parse::<usize>().ok())).unwrap_or(0);
                        if length > 1024 * 1024 { return; }
                        let mut body = vec![0; length];
                        if socket.read_exact(&mut body).await.is_err() { return; }
                        let payload = match path {
                            "/ready" => "{}",
                            "/metrics" => "probe{index=\"payments\"} 0\nprobe{index=\"orders\"} 0\n",
                            _ => {
                                // Advertising a longer body then closing must count as one failed request.
                                let _ = socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{").await;
                                return;
                            }
                        };
                        let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}", payload.len());
                        let _ = socket.write_all(response.as_bytes()).await;
                    });
                }
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    });

    let mut command = command();
    let mut paths = vec![directory.path().to_path_buf()];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    command
        .env("PATH", std::env::join_paths(paths)?)
        .args([
            "index-load",
            "--image",
            "mock:fixture",
            "--duration",
            "0.03",
            "--rate",
            "200",
            "--query-rate",
            "200",
            "--batch-size",
            "1",
            "--padding-bytes",
            "0",
            "--output",
        ])
        .arg(&report_path);
    let output = runtime::output(&mut command, 30).await;
    responder.abort();
    let _ = responder.await;
    let output = output?;
    assert!(!output.status.success());
    let report: Value = serde_json::from_slice(&fs::read(report_path)?)?;
    assert_eq!(report["passed"], false);
    assert_eq!(report["workload"]["worker_errors"], serde_json::json!([]));
    assert_eq!(report["workload"]["counts"]["ambiguous_ingest_events"], 6);
    assert_eq!(report["workload"]["counts"]["query_failed_units"], 6);
    assert_eq!(report["lifecycle"]["container_and_volume_removed"], true);
    let trace = fs::read_to_string(trace)?;
    let container = trace
        .lines()
        .find_map(|line| {
            line.strip_prefix("run -d --name ")
                .and_then(|rest| rest.split_whitespace().next())
        })
        .expect("created container");
    assert!(
        trace
            .lines()
            .any(|line| line == format!("rm -f -v {container}")),
        "{trace}"
    );
    assert!(
        trace
            .lines()
            .any(|line| line == format!("volume rm {container}-data")),
        "{trace}"
    );
    assert!(!PathBuf::from(fs::read_to_string(owned_root)?.trim()).exists());
    Ok(())
}

#[tokio::test]
async fn interrupting_index_cli_reaps_native_server_and_removes_data() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let lifecycle_path = directory.path().join("lifecycle");
    let wrapper = directory.path().join("server");
    executable(
        &wrapper,
        &format!(
            "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$$\" \"$LOGBROOK_DATA_DIR\" > {}\nexec {} \"$@\"\n",
            quote(lifecycle_path.to_str().unwrap()),
            quote(server_binary().to_str().unwrap())
        ),
    )?;
    let mut command = command();
    command
        .args(["index-load", "--binary"])
        .arg(&wrapper)
        .args([
            "--duration",
            "30",
            "--rate",
            "200",
            "--query-rate",
            "6",
            "--batch-size",
            "10",
            "--padding-bytes",
            "32",
            "--output",
        ])
        .arg(directory.path().join("report.json"));
    command
        .kill_on_drop(true)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let mut child = command.spawn()?;

    // Interrupt after ownership of a live fixture is established, before the long run ends.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    let lifecycle = loop {
        if let Ok(record) = fs::read_to_string(&lifecycle_path)
            && record.lines().count() == 2
        {
            break record;
        }
        assert!(
            child.try_wait()?.is_none(),
            "tool exited before starting its fixture"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "fixture startup timed out"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    let pid = child.id().expect("running tool PID").to_string();
    let signal = runtime::output(Command::new("kill").args(["-INT", &pid]), 5).await?;
    assert!(signal.status.success());
    let status = tokio::time::timeout(std::time::Duration::from_secs(40), child.wait()).await??;
    assert!(!status.success(), "interrupted workload reported success");
    let mut record = lifecycle.lines();
    let server_pid = record.next().expect("server PID");
    let data = PathBuf::from(record.next().expect("server data directory"));
    let exists = runtime::output(Command::new("kill").args(["-0", server_pid]), 5).await?;
    assert!(
        !exists.status.success(),
        "server PID {server_pid} survived interruption"
    );
    assert!(
        !data.parent().unwrap().exists(),
        "fixture directory survived interruption"
    );
    Ok(())
}
