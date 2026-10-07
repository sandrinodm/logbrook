//! Disposable fixtures with isolated synthetic credentials and bounded operations.
use crate::Result;
use serde_json::Value;
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::process::{Child, Command};

/// Retain the workload error and a cleanup failure when both occur.
pub fn finish(work: Result<()>, cleanup: Result<()>) -> Result<()> {
    match (work, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(a), Err(b)) => Err(format!("{a}; cleanup: {b}").into()),
        (Err(error), _) | (_, Err(error)) => Err(error),
    }
}

pub const INGEST_TOKEN: &str = "synthetic-cli-ingest-token";
pub const READ_TOKEN: &str = "synthetic-cli-reader-token";
pub const ADMIN_TOKEN: &str = "synthetic-cli-admin-token";
pub const HARDENED: &[&str] = &[
    "--read-only",
    "--tmpfs",
    "/tmp:rw,noexec,nosuid,size=256m,mode=1777",
    "--cap-drop",
    "ALL",
    "--security-opt",
    "no-new-privileges:true",
    "--cpus",
    "2",
    "--memory",
    "2g",
    "--memory-swap",
    "2g",
    "--pids-limit",
    "256",
];

fn redacted_diagnostic(text: &str) -> String {
    let mut text = text.to_owned();
    for token in [INGEST_TOKEN, READ_TOKEN, ADMIN_TOKEN] {
        text = text.replace(token, "[redacted]");
    }
    text.chars().take(8192).collect()
}

pub async fn output(command: &mut Command, seconds: u64) -> Result<std::process::Output> {
    command.kill_on_drop(true);
    Ok(tokio::time::timeout(Duration::from_secs(seconds), command.output()).await??)
}

pub async fn docker_bytes(args: &[&str]) -> Result<Vec<u8>> {
    docker_bytes_with_timeout(args, 60).await
}

pub async fn docker_bytes_with_timeout(args: &[&str], seconds: u64) -> Result<Vec<u8>> {
    let result = output(Command::new("docker").args(args), seconds).await?;
    if !result.status.success() {
        return Err(format!(
            "Docker operation {} failed ({}): {}",
            args.first().unwrap_or(&"unknown"),
            result.status,
            redacted_diagnostic(&String::from_utf8_lossy(&result.stderr)).trim()
        )
        .into());
    }
    Ok(result.stdout)
}

pub async fn docker(args: &[&str]) -> Result<String> {
    Ok(String::from_utf8(docker_bytes(args).await?)?
        .trim()
        .to_owned())
}

pub async fn docker_with_timeout(args: &[&str], seconds: u64) -> Result<String> {
    Ok(
        String::from_utf8(docker_bytes_with_timeout(args, seconds).await?)?
            .trim()
            .to_owned(),
    )
}

pub struct DockerResource {
    pub name: String,
    pub volume: Option<String>,
    removed: bool,
}

impl DockerResource {
    pub fn new(prefix: &str) -> Self {
        static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        Self {
            name: format!(
                "logbrook-{prefix}-{}-{}-{}",
                std::process::id(),
                crate::now_ms(),
                SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ),
            volume: None,
            removed: false,
        }
    }

    pub fn mark_removed(&mut self) {
        self.removed = true;
    }

    pub async fn cleanup(&mut self) -> Result<()> {
        let mut errors = Vec::new();
        if !self.removed {
            match docker(&["rm", "-f", "-v", &self.name]).await {
                Ok(_) => self.removed = true,
                Err(error) => errors.push(error.to_string()),
            }
        }
        if let Some(volume) = &self.volume {
            match docker(&["volume", "rm", volume]).await {
                Ok(_) => self.volume = None,
                Err(error) => errors.push(error.to_string()),
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; ").into())
        }
    }
}

impl Drop for DockerResource {
    fn drop(&mut self) {
        // A synchronous bounded fallback also runs when an async operation fails.
        let cleanup = |args: &[&str]| {
            if let Ok(mut child) = std::process::Command::new("docker")
                .args(args)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
            {
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                loop {
                    match child.try_wait() {
                        Ok(Some(_)) | Err(_) => break,
                        Ok(None) if std::time::Instant::now() < deadline => {
                            std::thread::sleep(Duration::from_millis(50))
                        }
                        _ => {
                            let _ = child.kill();
                            let _ = child.wait();
                            break;
                        }
                    }
                }
            }
        };
        if !self.removed {
            cleanup(&["rm", "-f", "-v", &self.name]);
        }
        if let Some(volume) = &self.volume {
            cleanup(&["volume", "rm", volume]);
        }
    }
}

pub struct Fixture {
    pub base: String,
    pub root: tempfile::TempDir,
    pub container: Option<String>,
    pub volume: Option<String>,
    pub ingest_token: String,
    pub read_token: String,
    pub admin_token: String,
    binary: PathBuf,
    child: Option<Child>,
    resource: Option<DockerResource>,
    client: reqwest::Client,
}

impl Fixture {
    pub async fn start(binary: PathBuf, image: Option<String>) -> Result<Self> {
        Self::start_config(binary, image, "").await
    }

    pub async fn start_config(
        binary: PathBuf,
        image: Option<String>,
        config: &str,
    ) -> Result<Self> {
        let mut fixture = Self {
            base: String::new(),
            root: tempfile::tempdir()?,
            container: None,
            volume: None,
            ingest_token: INGEST_TOKEN.into(),
            read_token: READ_TOKEN.into(),
            admin_token: ADMIN_TOKEN.into(),
            binary: std::fs::canonicalize(&binary).unwrap_or(binary),
            child: None,
            resource: None,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()?,
        };
        if !config.is_empty() {
            std::fs::write(fixture.root.path().join("config.toml"), config)?;
        }
        if let Some(image) = image {
            let mut resource = DockerResource::new("fixture");
            let volume = format!("{}-data", resource.name);
            docker(&["volume", "create", &volume]).await?;
            resource.volume = Some(volume.clone());
            fixture.container = Some(resource.name.clone());
            fixture.volume = Some(volume.clone());
            fixture.resource = Some(resource);
            let mut args = vec!["run", "-d", "--name", fixture.container.as_deref().unwrap()];
            args.extend(HARDENED);
            let mount = format!("{volume}:/data");
            args.extend([
                "-p",
                "127.0.0.1::3100",
                "-v",
                &mount,
                "-e",
                "LOGBROOK_INGEST_TOKEN=synthetic-cli-ingest-token",
                "-e",
                "LOGBROOK_READ_TOKEN=synthetic-cli-reader-token",
                "-e",
                "LOGBROOK_ADMIN_TOKEN=synthetic-cli-admin-token",
                "-e",
                "LOGBROOK_SOURCE=smoke",
                &image,
            ]);
            let config_mount = format!(
                "{}:/config.toml:ro",
                fixture.root.path().join("config.toml").display()
            );
            if !config.is_empty() {
                args.pop();
                args.extend([
                    "-v",
                    &config_mount,
                    &image,
                    "--config",
                    "/config.toml",
                    "serve",
                ]);
            }
            if let Err(error) = docker(&args).await {
                return Err(fixture.startup_error(error).await);
            }
            if let Err(error) = fixture.refresh_port().await {
                return Err(fixture.startup_error(error).await);
            }
        } else {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
            fixture.base = format!("http://127.0.0.1:{}", listener.local_addr()?.port());
            drop(listener);
            fixture.spawn_native()?;
        }
        if let Err(error) = fixture.ready().await {
            return Err(fixture.startup_error(error).await);
        }
        Ok(fixture)
    }

    async fn startup_error(
        &self,
        error: Box<dyn std::error::Error + Send + Sync>,
    ) -> Box<dyn std::error::Error + Send + Sync> {
        let mut diagnostics = String::new();
        if let Some(name) = &self.container {
            if let Ok(result) = output(
                Command::new("docker").args([
                    "inspect",
                    "--format",
                    "exit={{.State.ExitCode}} running={{.State.Running}} oom={{.State.OOMKilled}}",
                    name,
                ]),
                5,
            )
            .await
            {
                diagnostics.push_str(&String::from_utf8_lossy(&result.stdout));
            }
            if let Ok(result) = output(
                Command::new("docker").args(["logs", "--tail", "50", name]),
                5,
            )
            .await
            {
                diagnostics.push_str(&String::from_utf8_lossy(&result.stdout));
                diagnostics.push_str(&String::from_utf8_lossy(&result.stderr));
            }
        } else if let Ok(mut file) = std::fs::File::open(self.root.path().join("server.log")) {
            use std::io::{Read, Seek, SeekFrom};
            if let Ok(size) = file.metadata().map(|m| m.len()) {
                let _ = file.seek(SeekFrom::Start(size.saturating_sub(8192)));
                let _ = file.read_to_string(&mut diagnostics);
            }
        }
        for token in [&self.ingest_token, &self.read_token, &self.admin_token] {
            diagnostics = diagnostics.replace(token.as_str(), "[redacted]");
        }
        let diagnostic = redacted_diagnostic(&diagnostics);
        format!("{error}; fixture startup diagnostic: {}", diagnostic.trim()).into()
    }

    fn native_command(&self, local: bool) -> Command {
        let mut command = Command::new(&self.binary);
        if local && self.root.path().join("config.toml").exists() {
            command.args([
                "--config",
                self.root.path().join("config.toml").to_str().unwrap(),
            ]);
        }
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("LOGBROOK_") {
                command.env_remove(key);
            }
        }
        command
            .env("LOGBROOK_INGEST_TOKEN", &self.ingest_token)
            .env("LOGBROOK_READ_TOKEN", &self.read_token)
            .env("LOGBROOK_ADMIN_TOKEN", &self.admin_token)
            .env("LOGBROOK_BIND", self.base.trim_start_matches("http://"))
            .env("LOGBROOK_DATA_DIR", self.root.path().join("data"))
            .env("LOGBROOK_SOURCE", "smoke");
        command
    }

    fn spawn_native(&mut self) -> Result<()> {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.path().join("server.log"))?;
        self.child = Some(
            self.native_command(true)
                .arg("serve")
                .stdout(log.try_clone()?)
                .stderr(log)
                .kill_on_drop(true)
                .spawn()?,
        );
        Ok(())
    }

    async fn refresh_port(&mut self) -> Result<()> {
        let port = docker(&["port", self.container.as_deref().unwrap(), "3100/tcp"]).await?;
        self.base = format!(
            "http://127.0.0.1:{}",
            port.rsplit(':').next().ok_or("missing container port")?
        );
        Ok(())
    }

    pub async fn request(
        &self,
        path: &str,
        token: Option<&str>,
        body: Option<&Value>,
    ) -> Result<(u16, Value)> {
        let mut request = if body.is_some() {
            self.client.post(format!("{}{path}", self.base))
        } else {
            self.client.get(format!("{}{path}", self.base))
        };
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await?;
        let status = response.status().as_u16();
        let text = response.text().await?;
        Ok((
            status,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        ))
    }

    pub async fn ready(&mut self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if matches!(
                tokio::time::timeout_at(deadline, self.request("/ready", None, None)).await,
                Ok(Ok((200, _)))
            ) {
                return Ok(());
            }
            if let Some(child) = &mut self.child
                && child.try_wait()?.is_some()
            {
                return Err("native fixture exited before readiness".into());
            }
            if let Some(name) = &self.container {
                let running = tokio::time::timeout_at(
                    deadline,
                    docker(&["inspect", "--format", "{{.State.Running}}", name]),
                )
                .await??;
                if running == "false" {
                    return Err("container fixture exited before readiness".into());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("fixture readiness deadline exceeded".into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn command(&self, args: &[&str]) -> Result<String> {
        let mut command = if let Some(name) = &self.container {
            let mut c = Command::new("docker");
            c.args(["exec", name, "/usr/local/bin/logbrook"]);
            c
        } else {
            self.native_command(true)
        };
        let result = output(command.args(args), 60).await?;
        if !result.status.success() {
            return Err("fixture CLI command failed".into());
        }
        Ok(String::from_utf8(result.stdout)?)
    }

    pub async fn cli(&self, args: &[&str], json: bool, success: bool) -> Result<Value> {
        let mut command = if let Some(name) = &self.container {
            let mut c = Command::new("docker");
            c.args(["exec", name, "/usr/local/bin/logbrook"]);
            c
        } else {
            let mut c = self.native_command(false);
            c.args(["--url", &self.base]);
            c
        };
        if json {
            command.arg("--json");
        }
        let result = output(command.args(args), 60).await?;
        if result.status.success() != success {
            return Err(format!(
                "unexpected CLI status for {}",
                args.last().unwrap_or(&"command")
            )
            .into());
        }
        let stdout = String::from_utf8(result.stdout)?;
        let stderr = String::from_utf8(result.stderr)?;
        if !success {
            if !stdout.is_empty()
                || [&self.ingest_token, &self.read_token, &self.admin_token]
                    .iter()
                    .any(|token| stderr.contains(token.as_str()))
            {
                return Err("CLI failure output violated credential/stdout constraints".into());
            }
            return Ok(Value::String(stderr));
        }
        if json {
            Ok(serde_json::from_str(&stdout)?)
        } else {
            Ok(Value::String(stdout))
        }
    }

    pub async fn stop(&mut self) -> Result<()> {
        if let Some(name) = &self.container {
            docker(&["stop", "--time", "35", name]).await?;
            if docker(&["inspect", "--format", "{{.State.ExitCode}}", name]).await? != "0" {
                return Err("container shutdown was not clean".into());
            }
        }
        if let Some(mut child) = self.child.take() {
            let pid = child.id().ok_or("native fixture missing PID")?.to_string();
            output(Command::new("kill").args(["-TERM", &pid]), 5).await?;
            let status = match tokio::time::timeout(Duration::from_secs(35), child.wait()).await {
                Ok(result) => result?,
                Err(_) => {
                    child.start_kill()?;
                    tokio::time::timeout(Duration::from_secs(5), child.wait()).await??;
                    return Err("native shutdown deadline exceeded".into());
                }
            };
            if !status.success() {
                return Err("native fixture shutdown was not clean".into());
            }
        }
        Ok(())
    }

    pub async fn start_again(&mut self) -> Result<()> {
        if let Some(name) = &self.container {
            docker(&["start", name]).await?;
            self.refresh_port().await?;
        } else {
            self.spawn_native()?;
        }
        self.ready().await
    }

    pub async fn restart(&mut self) -> Result<()> {
        self.stop().await?;
        self.start_again().await
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        let stopped = self.stop().await;
        let cleaned = if let Some(resource) = &mut self.resource {
            resource.cleanup().await
        } else {
            Ok(())
        };
        finish(stopped, cleaned)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child
            && matches!(child.try_wait(), Ok(None))
            && let Some(pid) = child.id()
        {
            let _ = std::process::Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            let deadline = std::time::Instant::now() + Duration::from_secs(35);
            loop {
                match child.try_wait() {
                    Ok(Some(_)) | Err(_) => break,
                    Ok(None) if std::time::Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(50))
                    }
                    _ => {
                        let _ = child.start_kill();
                        let reap_deadline = std::time::Instant::now() + Duration::from_secs(5);
                        while matches!(child.try_wait(), Ok(None))
                            && std::time::Instant::now() < reap_deadline
                        {
                            std::thread::sleep(Duration::from_millis(20));
                        }
                        break;
                    }
                }
            }
        }
        // Remove Docker mounts before the temporary configuration directory.
        drop(self.resource.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires a built application binary"]
    async fn native_fixture_drop_cleans_up_after_worker_failure() -> Result<()> {
        let binary = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/debug/logbrook");
        let fixture = Fixture::start(binary, None).await?;
        let pid = fixture
            .child
            .as_ref()
            .and_then(Child::id)
            .ok_or("missing PID")?
            .to_string();
        let root = fixture.root.path().to_path_buf();
        let result: Result<()> = {
            let _owned = fixture;
            Err("synthetic workload failure".into())
        };
        assert!(result.is_err());
        assert!(!root.exists());
        let probe = output(Command::new("kill").args(["-0", &pid]), 5).await?;
        assert!(
            !probe.status.success(),
            "native child survived failed workload"
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires Docker and the LOGBROOK_TEST_IMAGE image"]
    async fn failed_container_start_preserves_redacted_diagnostic() -> Result<()> {
        let image =
            std::env::var("LOGBROOK_TEST_IMAGE").unwrap_or_else(|_| "logbrook:rust-tools".into());
        let result = Fixture::start_config(
            PathBuf::from("unused"),
            Some(image),
            "[storage]\nmemory_limit=\"invalid\"\n",
        )
        .await;
        let diagnostic = match result {
            Ok(_) => return Err("invalid configuration unexpectedly started".into()),
            Err(error) => error.to_string(),
        };
        assert!(
            diagnostic.contains("invalid storage budgets"),
            "{diagnostic}"
        );
        assert!(diagnostic.contains("exit="), "{diagnostic}");
        for token in [INGEST_TOKEN, READ_TOKEN, ADMIN_TOKEN] {
            assert!(!diagnostic.contains(token));
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires Docker and the LOGBROOK_TEST_IMAGE image"]
    async fn configured_container_fixture_starts_server_and_is_cleansed() -> Result<()> {
        let image =
            std::env::var("LOGBROOK_TEST_IMAGE").unwrap_or_else(|_| "logbrook:rust-tools".into());
        let mut fixture = Fixture::start_config(
            PathBuf::from("unused"),
            Some(image),
            "[indexes.payments]\nretention_days=7\nmax_size_gb=20\n",
        )
        .await?;
        let name = fixture.container.clone().ok_or("missing container")?;
        let volume = fixture.volume.clone().ok_or("missing volume")?;
        let indexes = fixture.cli(&["indexes", "list"], true, true).await?;
        assert!(
            indexes["indexes"]
                .as_array()
                .is_some_and(|items| items.iter().any(|item| item["name"] == "payments"))
        );
        fixture.shutdown().await?;
        assert!(docker(&["inspect", &name]).await.is_err());
        assert!(docker(&["volume", "inspect", &volume]).await.is_err());
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires Docker and the LOGBROOK_TEST_IMAGE image"]
    async fn container_fixture_drop_cleans_up_after_worker_failure() -> Result<()> {
        let image =
            std::env::var("LOGBROOK_TEST_IMAGE").unwrap_or_else(|_| "logbrook:rust-tools".into());
        let fixture = Fixture::start(PathBuf::from("unused"), Some(image)).await?;
        let name = fixture.container.clone().ok_or("missing container")?;
        let volume = fixture.volume.clone().ok_or("missing volume")?;
        let root = fixture.root.path().to_path_buf();
        let result: Result<()> = {
            let _owned = fixture;
            Err("synthetic workload failure".into())
        };
        assert!(result.is_err());
        assert!(!root.exists());
        assert!(
            docker(&["inspect", &name]).await.is_err(),
            "container survived failed workload"
        );
        assert!(
            docker(&["volume", "inspect", &volume]).await.is_err(),
            "volume survived failed workload"
        );
        Ok(())
    }
}
