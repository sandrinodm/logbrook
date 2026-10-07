//! Exercise the actual load command against a server that closes idle connections.

use std::{path::PathBuf, time::Duration};

use logbrook_dev::{Result, runtime};
use serde_json::Value;
use tokio::process::Command;

fn server_binary() -> PathBuf {
    std::env::current_exe()
        .expect("test executable path")
        .parent()
        .expect("deps directory")
        .parent()
        .expect("build profile directory")
        .join(format!("logbrook{}", std::env::consts::EXE_SUFFIX))
}

#[tokio::test]
async fn metrics_scrapes_survive_server_idle_connection_timeout() -> Result<()> {
    let mut fixture =
        runtime::Fixture::start_config(server_binary(), None, "header_timeout_ms = 100\n").await?;
    let directory = tempfile::tempdir()?;
    let report_path = directory.path().join("report.json");

    let result = async {
        let mut command = Command::new(env!("CARGO_BIN_EXE_logbrook-dev"));
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("LOGBROOK_") {
                command.env_remove(key);
            }
        }
        command
            .env("LOGBROOK_INGEST_TOKEN", &fixture.ingest_token)
            .env("LOGBROOK_READ_TOKEN", &fixture.read_token)
            .args([
                "mixed-load",
                "--url",
                &fixture.base,
                "--seed-events",
                "0",
                "--duration",
                "2",
                "--rate",
                "200",
                "--batch-size",
                "10",
                "--ingest-clients",
                "1",
                "--query-clients",
                "0",
                "--query-rate",
                "0",
                "--tail-subscribers",
                "0",
                "--metrics-interval",
                "0.25",
                "--retention-ms",
                "604800000",
                "--require-qualified",
                "--output",
            ])
            .arg(&report_path);

        // Each scrape interval exceeds the server's header/idle timeout.
        let output = runtime::output(&mut command, 45).await?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report: Value = serde_json::from_slice(&std::fs::read(report_path)?)?;
        let samples = report["metrics_samples"]
            .as_array()
            .expect("metrics sample list");
        assert!(samples.len() >= 4, "{report}");
        assert!(
            samples.iter().all(|sample| sample["status"] == 200),
            "{samples:?}"
        );
        assert_eq!(report["qualification"]["passed"], true, "{report}");
        assert_eq!(report["accounting"]["ambiguous_ingest"], 0, "{report}");
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;

    let cleanup = tokio::time::timeout(Duration::from_secs(40), fixture.shutdown()).await?;
    result?;
    cleanup
}
