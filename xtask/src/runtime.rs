//! Disposable fixtures with isolated synthetic credentials and bounded operations.
use crate::Result;
use serde_json::Value;
use std::{
    error::Error,
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
    process::{ExitStatus, Output, Stdio},
    time::{Duration, Instant},
};
use tokio::{
    process::{Child, Command},
    time::timeout,
};

/// Program that every Docker call in this module runs.
const DOCKER: &str = "docker";

/// Program that delivers SIGTERM to a native fixture.
const KILL: &str = "kill";

/// Largest diagnostic copied into an error, counted in characters after redaction.
const DIAGNOSTIC_MAX_CHARS: usize = 8192;

/// Bytes at the end of the native server log that are copied into a startup diagnostic.
const SERVER_LOG_TAIL_BYTES: u64 = 8192;

/// Lines of container log copied into a startup diagnostic.
const LOG_TAIL_LINES: &str = "50";

/// Default bound, in seconds, for one Docker or CLI command.
const COMMAND_TIMEOUT_SECS: u64 = 60;

/// Bound, in seconds, for a quick probe or signal delivery.
const SHORT_COMMAND_TIMEOUT_SECS: u64 = 5;

/// Bound, in seconds, for copying or restoring a complete data directory.
const SNAPSHOT_TIMEOUT_SECS: u64 = 600;

/// Time a server has to stop after SIGTERM or `docker stop` before it is killed.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(35);

/// Time allowed for a killed process to be reaped.
const KILL_REAP_TIMEOUT: Duration = Duration::from_secs(5);

/// Time limit for one HTTP request made by a fixture.
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Time a fixture has to answer its readiness probe.
const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Delay between readiness probes.
const READY_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Delay between exit checks in synchronous cleanup.
const EXIT_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Delay between reap checks after a process has been killed.
const REAP_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Time a `Drop` impl waits for a Docker removal before killing it.
const SYNC_CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);

/// Buffer size used when hashing snapshot contents.
const HASH_BUFFER_BYTES: usize = 64 * 1024;

/// Smallest memory limit, in MiB, that a fixture may request.
const MIN_MEMORY_MIB: usize = 128;

/// Largest memory limit, in MiB, that a fixture may request: 64 GiB.
const MAX_MEMORY_MIB: usize = 65_536;

/// Largest CPU count that a fixture may request.
const MAX_CPUS: usize = 64;

/// Permissions that let a container's user read the fixture root.
const FIXTURE_ROOT_MODE: u32 = 0o755;

/// HTTP status that a fixture returns from `/ready` once it can serve requests.
const HTTP_OK: u16 = 200;

/// Combines workload and cleanup outcomes, retaining both failures when both occur.
///
/// # Errors
///
/// Returns the workload error, or the cleanup error when only cleanup failed.
pub fn finish(work: Result<()>, cleanup: Result<()>) -> Result<()> {
    match (work, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(a), Err(b)) => Err(format!("{a}; cleanup: {b}").into()),
        (Err(error), _) | (_, Err(error)) => Err(error),
    }
}

/// Synthetic ingest credential; never a real secret.
pub const INGEST_TOKEN: &str = "synthetic-cli-ingest-token";

/// Synthetic read credential; never a real secret.
pub const READ_TOKEN: &str = "synthetic-cli-reader-token";

/// Synthetic admin credential; never a real secret.
pub const ADMIN_TOKEN: &str = "synthetic-cli-admin-token";

/// Hardening flags for every fixture container, as `docker` arguments.
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

/// Replaces every synthetic credential in `text` and bounds its length.
fn redacted_diagnostic(text: &str) -> String {
    let mut text = text.to_owned();
    for token in [INGEST_TOKEN, READ_TOKEN, ADMIN_TOKEN] {
        text = text.replace(token, "[redacted]");
    }

    text.chars().take(DIAGNOSTIC_MAX_CHARS).collect()
}

/// Runs `command` to completion and kills it if it takes longer than `seconds`.
///
/// # Errors
///
/// Returns an error when the command cannot run, its output cannot be read, or it exceeds
/// `seconds`.
pub async fn output(command: &mut Command, seconds: u64) -> Result<Output> {
    command.kill_on_drop(true);
    Ok(timeout(Duration::from_secs(seconds), command.output()).await??)
}

// Command::output replaces stdout with a pipe. Snapshot archives must stream to
// disk without buffering the complete DuckDB/Parquet fixture in driver memory.
async fn output_to_file(command: &mut Command, path: &Path, seconds: u64) -> Result<Output> {
    command
        .kill_on_drop(true)
        .stdout(Stdio::from(std::fs::File::create(path)?))
        .stderr(Stdio::piped());
    let child = command.spawn()?;
    Ok(timeout(Duration::from_secs(seconds), child.wait_with_output()).await??)
}

/// A `docker` invocation; every Docker call in this module starts from here.
fn docker_command() -> Command {
    Command::new(DOCKER)
}

/// Runs `docker` with `args` and returns its stdout, failing on a non-zero exit.
///
/// # Errors
///
/// Returns an error when Docker cannot run, exceeds the time limit or exits non-zero.
pub async fn docker_bytes(args: &[&str]) -> Result<Vec<u8>> {
    docker_bytes_with_timeout(args, COMMAND_TIMEOUT_SECS).await
}

/// Like [`docker_bytes`], with an explicit bound in seconds.
///
/// # Errors
///
/// Returns an error when Docker cannot run, exceeds `seconds` or exits non-zero.
pub async fn docker_bytes_with_timeout(args: &[&str], seconds: u64) -> Result<Vec<u8>> {
    let result = output(docker_command().args(args), seconds).await?;
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

/// Runs `docker` with `args` and returns its trimmed stdout as text.
///
/// # Errors
///
/// Returns an error when Docker fails or its output is not UTF-8.
pub async fn docker(args: &[&str]) -> Result<String> {
    Ok(String::from_utf8(docker_bytes(args).await?)?
        .trim()
        .to_owned())
}

/// Like [`docker`], with an explicit bound in seconds.
///
/// # Errors
///
/// Returns an error when Docker fails, exceeds `seconds` or its output is not UTF-8.
pub async fn docker_with_timeout(args: &[&str], seconds: u64) -> Result<String> {
    Ok(
        String::from_utf8(docker_bytes_with_timeout(args, seconds).await?)?
            .trim()
            .to_owned(),
    )
}

/// A named Docker container and optional volume that are removed on cleanup or drop.
pub struct DockerResource {
    /// Container name, unique within this process.
    pub name: String,
    /// Volume to remove together with the container.
    pub volume: Option<String>,
    removed: bool,
}

impl DockerResource {
    /// Creates a resource whose name starts with `logbrook-{prefix}-` and is unique in this
    /// process.
    pub fn new(prefix: &str) -> Self {
        // Keeps names unique when one process creates several resources in the same millisecond.
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

    /// Records that the container was removed by the caller, so drop does not retry it.
    pub fn mark_removed(&mut self) {
        self.removed = true;
    }

    /// Removes the container and volume, reporting every failure.
    ///
    /// # Errors
    ///
    /// Returns the joined messages of every removal that failed.
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
        if !self.removed {
            remove_detached(&["rm", "-f", "-v", &self.name]);
        }

        if let Some(volume) = &self.volume {
            remove_detached(&["volume", "rm", volume]);
        }
    }
}

/// Runs a Docker removal from `Drop`, where no async work is possible, and kills it
/// if it does not finish within `SYNC_CLEANUP_TIMEOUT`.
fn remove_detached(args: &[&str]) {
    let Ok(mut child) = std::process::Command::new(DOCKER)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    else {
        return;
    };
    if !wait_for_exit(SYNC_CLEANUP_TIMEOUT, || child.try_wait()) {
        let _ = child.kill();
        let _ = child.wait();
    }
}

/// Polls `try_wait` until the process reports an exit, returning `true`, or until
/// `grace` has passed, returning `false`. An error from `try_wait` counts as exited.
fn wait_for_exit(
    grace: Duration,
    mut try_wait: impl FnMut() -> io::Result<Option<ExitStatus>>,
) -> bool {
    let deadline = Instant::now() + grace;
    loop {
        match try_wait() {
            Ok(Some(_)) | Err(_) => return true,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(EXIT_POLL_INTERVAL),
            Ok(None) => return false,
        }
    }
}

/// Waits a bounded time for a child that has been sent `start_kill` to be reaped.
fn reap_after_kill(child: &mut Child) {
    let reap_deadline = Instant::now() + KILL_REAP_TIMEOUT;
    while matches!(child.try_wait(), Ok(None)) && Instant::now() < reap_deadline {
        std::thread::sleep(REAP_POLL_INTERVAL);
    }
}

/// Stable content/ownership inventory; tar ordering and access times are excluded.
///
/// # Errors
///
/// Returns an error when the archive cannot be read, has a non-file or escaping entry, repeats a
/// path or has no files.
pub fn snapshot_manifest(path: &Path) -> Result<std::collections::BTreeMap<String, Value>> {
    let mut manifest = std::collections::BTreeMap::new();
    let mut archive = tar::Archive::new(File::open(path)?);
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        let name = path
            .to_str()
            .ok_or("non UTF-8 snapshot entry")?
            .trim_start_matches("./")
            .trim_end_matches('/')
            .to_owned();

        if entry.header().entry_type().is_dir() {
            continue;
        }

        if !entry.header().entry_type().is_file()
            || path.components().any(|part| {
                matches!(
                    part,
                    std::path::Component::ParentDir
                        | std::path::Component::RootDir
                        | std::path::Component::Prefix(_)
                )
            })
        {
            return Err("snapshot contains unsupported non-file or escaping entry".into());
        }

        let digest = sha256_reader(&mut entry)?;
        let info = serde_json::json!({
            "bytes": entry.size(),
            "sha256": digest,
            "mode": entry.header().mode()?,
            "uid": entry.header().uid()?,
            "gid": entry.header().gid()?,
        });
        if manifest.insert(name, info).is_some() {
            return Err("snapshot contains duplicate file paths".into());
        }
    }

    if manifest.is_empty() {
        return Err("snapshot contains no files".into());
    }

    Ok(manifest)
}

/// Hex SHA-256 of everything that `reader` yields.
fn sha256_reader(reader: &mut impl Read) -> io::Result<String> {
    use sha2::{Digest, Sha256};

    let mut hash = Sha256::new();
    let mut buffer = vec![0; HASH_BUFFER_BYTES];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }

    Ok(crate::hex_digest(&hash.finalize()))
}

/// A running fixture server, either a Docker container or a native child process.
pub struct Fixture {
    /// Base URL of the server, without a trailing slash.
    pub base: String,
    /// Temporary directory for config, logs and native data.
    pub root: tempfile::TempDir,
    /// Container name when the fixture runs in Docker.
    pub container: Option<String>,
    /// Data volume when the fixture runs in Docker.
    pub volume: Option<String>,
    /// Ingest credential the server was started with.
    pub ingest_token: String,
    /// Read credential the server was started with.
    pub read_token: String,
    /// Admin credential the server was started with.
    pub admin_token: String,
    binary: PathBuf,
    image: Option<String>,
    resources: Option<ResourceLimits>,
    child: Option<Child>,
    resource: Option<DockerResource>,
    client: reqwest::Client,
}

/// CPU and memory limits applied to a fixture container.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct ResourceLimits {
    /// Number of CPUs.
    pub cpus: usize,
    /// Memory limit in MiB.
    pub memory_mib: usize,
}

impl ResourceLimits {
    /// Whether both limits fall inside the range that fixtures accept.
    fn within_supported_bounds(self) -> bool {
        (1..=MAX_CPUS).contains(&self.cpus)
            && (MIN_MEMORY_MIB..=MAX_MEMORY_MIB).contains(&self.memory_mib)
    }
}

/// [`HARDENED`] with the CPU and memory flags replaced by `resources` when given.
fn hardened_args(resources: Option<ResourceLimits>) -> Vec<String> {
    let mut args: Vec<String> = HARDENED.iter().map(|arg| (*arg).into()).collect();
    if let Some(resources) = resources {
        for index in 0..args.len() - 1 {
            match args[index].as_str() {
                "--cpus" => args[index + 1] = resources.cpus.to_string(),
                "--memory" | "--memory-swap" => {
                    args[index + 1] = format!("{}m", resources.memory_mib)
                }
                _ => {}
            }
        }
    }

    args
}

/// Rejects a start request that cannot be honored, before any Docker or process work.
fn validate_start(
    image: Option<&str>,
    snapshot: Option<&Path>,
    resources: Option<ResourceLimits>,
) -> Result<()> {
    if snapshot.is_some() && image.is_none() {
        return Err("snapshot restore requires a Docker image".into());
    }

    if resources.is_some_and(|limits| !limits.within_supported_bounds()) {
        return Err("fixture resource limits outside supported bounds".into());
    }

    Ok(())
}

/// `docker exec` into the fixture container's CLI.
fn container_exec(name: &str) -> Command {
    let mut command = docker_command();
    command.args(["exec", name, "/usr/local/bin/logbrook"]);
    command
}

/// Error for a failed command, with its stderr redacted and bounded.
fn failure_with_stderr(context: &str, stderr: &[u8]) -> Box<dyn Error + Send + Sync> {
    format!(
        "{context}: {}",
        redacted_diagnostic(&String::from_utf8_lossy(stderr))
    )
    .into()
}

impl Fixture {
    /// Starts a fixture with the default configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the fixture cannot be created or is not ready in time.
    pub async fn start(binary: PathBuf, image: Option<String>) -> Result<Self> {
        Self::start_config(binary, image, "").await
    }

    /// Starts a fixture with `config` written as its `config.toml`.
    ///
    /// # Errors
    ///
    /// Returns an error when the fixture cannot be created or is not ready in time.
    pub async fn start_config(
        binary: PathBuf,
        image: Option<String>,
        config: &str,
    ) -> Result<Self> {
        Self::start_config_with_resources(binary, image, config, None).await
    }

    /// Starts a fixture with `config` and optional CPU and memory limits.
    ///
    /// # Errors
    ///
    /// Returns an error when the limits are unsupported or the fixture is not ready in time.
    pub async fn start_config_with_resources(
        binary: PathBuf,
        image: Option<String>,
        config: &str,
        resources: Option<ResourceLimits>,
    ) -> Result<Self> {
        Self::start_config_from_snapshot(binary, image, config, resources, None).await
    }

    /// Populate a fresh disposable Docker volume before the server is started.
    /// The archive must have been copied from a cleanly stopped fixture.
    ///
    /// # Errors
    ///
    /// Returns an error when the arguments are unsupported, the snapshot does not restore, or the
    /// fixture is not ready in time.
    pub async fn start_config_from_snapshot(
        binary: PathBuf,
        image: Option<String>,
        config: &str,
        resources: Option<ResourceLimits>,
        snapshot: Option<&Path>,
    ) -> Result<Self> {
        validate_start(image.as_deref(), snapshot, resources)?;

        let mut fixture = Self {
            base: String::new(),
            root: tempfile::tempdir()?,
            container: None,
            volume: None,
            ingest_token: INGEST_TOKEN.into(),
            read_token: READ_TOKEN.into(),
            admin_token: ADMIN_TOKEN.into(),
            binary: std::fs::canonicalize(&binary).unwrap_or(binary),
            image: image.clone(),
            resources,
            child: None,
            resource: None,
            client: reqwest::Client::builder().timeout(HTTP_TIMEOUT).build()?,
        };
        if !config.is_empty() {
            std::fs::write(fixture.root.path().join("config.toml"), config)?;
        }

        match image {
            Some(image) => {
                fixture
                    .start_container(&image, config, snapshot, resources)
                    .await?
            }
            None => fixture.start_native()?,
        }

        if let Err(error) = fixture.ready().await {
            return Err(fixture.startup_error(error).await);
        }

        Ok(fixture)
    }

    /// Creates the volume and container, restores `snapshot` into it when given,
    /// starts it and records the published port.
    async fn start_container(
        &mut self,
        image: &str,
        config: &str,
        snapshot: Option<&Path>,
        resources: Option<ResourceLimits>,
    ) -> Result<()> {
        let mut resource = DockerResource::new("fixture");
        let name = resource.name.clone();
        let volume = format!("{name}-data");
        resource.volume = Some(volume.clone());
        docker(&["volume", "create", &volume]).await?;
        self.container = Some(name.clone());
        self.volume = Some(volume.clone());
        self.resource = Some(resource);

        let mut args = if snapshot.is_some() {
            vec!["create", "--name", name.as_str()]
        } else {
            vec!["run", "-d", "--name", name.as_str()]
        };
        let hardened = hardened_args(resources);
        args.extend(hardened.iter().map(String::as_str));
        let mount = format!("{volume}:/data");
        let config_mount = format!(
            "{}:/config.toml:ro",
            self.root.path().join("config.toml").display()
        );
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
        ]);

        if config.is_empty() {
            args.push(image);
        } else {
            args.extend([
                "-v",
                &config_mount,
                image,
                "--config",
                "/config.toml",
                "serve",
            ]);
        }

        if let Err(error) = docker(&args).await {
            return Err(self.startup_error(error).await);
        }

        if let Some(snapshot) = snapshot {
            self.restore_snapshot(snapshot).await?;
            docker(&["start", &name]).await?;
        }

        if let Err(error) = self.refresh_port().await {
            return Err(self.startup_error(error).await);
        }

        Ok(())
    }

    /// Binds a free loopback port, records its URL and starts the native server on it.
    fn start_native(&mut self) -> Result<()> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        self.base = format!("http://127.0.0.1:{}", listener.local_addr()?.port());
        drop(listener);
        self.spawn_native()
    }

    /// Archive a complete Docker data directory only after clean shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error when the container is not cleanly stopped or the copy fails.
    pub async fn snapshot(&self, path: &Path) -> Result<()> {
        let name = self
            .container
            .as_deref()
            .ok_or("snapshot requires Docker")?;
        let state: Value = serde_json::from_str(
            &docker(&["inspect", "--format", "{{json .State}}", name]).await?,
        )?;
        if state["Running"] != false || state["ExitCode"] != 0 {
            return Err("snapshot requires a cleanly stopped container".into());
        }

        let result = output_to_file(
            docker_command().args(["cp", &format!("{name}:/data/."), "-"]),
            path,
            SNAPSHOT_TIMEOUT_SECS,
        )
        .await?;
        if !result.status.success() {
            return Err(failure_with_stderr("snapshot copy failed", &result.stderr));
        }

        Ok(())
    }

    /// Copies `path` into the container's data directory, then checks that the restored
    /// files match the archive.
    async fn restore_snapshot(&self, path: &Path) -> Result<()> {
        let name = self.container.as_deref().ok_or("restore requires Docker")?;
        let source = File::open(path)?;
        let result = output(
            docker_command()
                .args(["cp", "-a", "-", &format!("{name}:/data")])
                .stdin(Stdio::from(source)),
            SNAPSHOT_TIMEOUT_SECS,
        )
        .await?;
        if !result.status.success() {
            return Err(failure_with_stderr(
                "snapshot restore failed",
                &result.stderr,
            ));
        }

        // Verify physical contents and ownership before any server can open DuckDB.
        let verification = tempfile::NamedTempFile::new()?;
        self.snapshot(verification.path()).await?;

        if snapshot_manifest(path)? != snapshot_manifest(verification.path())? {
            return Err("restored data differs from canonical snapshot".into());
        }

        Ok(())
    }

    /// Adds the container state and recent logs, or the tail of the native server log,
    /// to `error`, with credentials redacted.
    async fn startup_error(
        &self,
        error: Box<dyn Error + Send + Sync>,
    ) -> Box<dyn Error + Send + Sync> {
        let mut diagnostics = self.startup_logs().await;
        for token in [&self.ingest_token, &self.read_token, &self.admin_token] {
            diagnostics = diagnostics.replace(token.as_str(), "[redacted]");
        }

        let diagnostic = redacted_diagnostic(&diagnostics);
        format!("{error}; fixture startup diagnostic: {}", diagnostic.trim()).into()
    }

    /// Collects container state and logs, or the tail of the native server log.
    async fn startup_logs(&self) -> String {
        let mut diagnostics = String::new();
        if let Some(name) = &self.container {
            if let Ok(result) = output(
                docker_command().args([
                    "inspect",
                    "--format",
                    "exit={{.State.ExitCode}} running={{.State.Running}} oom={{.State.OOMKilled}}",
                    name,
                ]),
                SHORT_COMMAND_TIMEOUT_SECS,
            )
            .await
            {
                diagnostics.push_str(&String::from_utf8_lossy(&result.stdout));
            }

            if let Ok(result) = output(
                docker_command().args(["logs", "--tail", LOG_TAIL_LINES, name]),
                SHORT_COMMAND_TIMEOUT_SECS,
            )
            .await
            {
                diagnostics.push_str(&String::from_utf8_lossy(&result.stdout));
                diagnostics.push_str(&String::from_utf8_lossy(&result.stderr));
            }
        } else if let Ok(mut file) = File::open(self.root.path().join("server.log")) {
            use std::io::Seek;

            if let Ok(size) = file.metadata().map(|m| m.len()) {
                let tail = size.saturating_sub(SERVER_LOG_TAIL_BYTES);
                let _ = file.seek(io::SeekFrom::Start(tail));
                let _ = file.read_to_string(&mut diagnostics);
            }
        }

        diagnostics
    }

    /// The command that runs the server binary against this fixture's config and data.
    fn native_command(&self, local: bool) -> Command {
        let mut command = Command::new(&self.binary);
        let config = self.root.path().join("config.toml");
        if local && config.exists() {
            command.args([
                "--config",
                config.to_str().expect("temporary config path is UTF-8"),
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

    /// Starts the native server, appending its output to `server.log`.
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

    /// Reads the published host port of the container and updates `base`.
    async fn refresh_port(&mut self) -> Result<()> {
        let name = self
            .container
            .as_deref()
            .expect("container fixture has a container name");
        let port = docker(&["port", name, "3100/tcp"]).await?;
        self.base = format!(
            "http://127.0.0.1:{}",
            port.rsplit(':').next().ok_or("missing container port")?
        );
        Ok(())
    }

    /// Sends one HTTP request to the fixture and returns its status and JSON body,
    /// or the raw text when the body is not JSON.
    ///
    /// # Errors
    ///
    /// Returns an error when the connection fails or the response body cannot be read.
    pub async fn request(
        &self,
        path: &str,
        token: Option<&str>,
        body: Option<&Value>,
    ) -> Result<(u16, Value)> {
        let url = format!("{}{path}", self.base);
        let mut request = if body.is_some() {
            self.client.post(url)
        } else {
            self.client.get(url)
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

    /// Waits until `/ready` answers 200, failing early if the server or container exits.
    ///
    /// # Errors
    ///
    /// Returns an error when the process exits, the container stops, or
    /// `READY_TIMEOUT` passes first.
    pub async fn ready(&mut self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        loop {
            if matches!(
                tokio::time::timeout_at(deadline, self.request("/ready", None, None)).await,
                Ok(Ok((HTTP_OK, _)))
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

            tokio::time::sleep(READY_POLL_INTERVAL).await;
        }
    }

    /// Runs the CLI with `args` and returns its stdout, failing on a non-zero exit.
    ///
    /// # Errors
    ///
    /// Returns an error when the CLI cannot run, exceeds the time limit or exits non-zero.
    pub async fn command(&self, args: &[&str]) -> Result<String> {
        self.command_with_timeout(args, COMMAND_TIMEOUT_SECS).await
    }

    /// Like [`Fixture::command`], with an explicit bound in seconds.
    ///
    /// # Errors
    ///
    /// Returns an error when the CLI cannot run, exceeds `seconds` or exits non-zero.
    pub async fn command_with_timeout(&self, args: &[&str], seconds: u64) -> Result<String> {
        let mut command = match &self.container {
            Some(name) => container_exec(name),
            None => self.native_command(true),
        };
        let result = output(command.args(args), seconds).await?;
        if !result.status.success() {
            return Err(failure_with_stderr(
                "fixture CLI command failed",
                &result.stderr,
            ));
        }

        Ok(String::from_utf8(result.stdout)?)
    }

    /// Run while the server is stopped. Container fixtures share only their
    /// disposable data volume/config and expose the temporary root at /fixture.
    ///
    /// # Errors
    ///
    /// Returns an error when the command cannot run, exceeds `seconds` or exits non-zero.
    pub async fn offline_command(&self, args: &[&str], seconds: u64) -> Result<String> {
        let Some(image) = &self.image else {
            return self.command_with_timeout(args, seconds).await;
        };

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                self.root.path(),
                std::fs::Permissions::from_mode(FIXTURE_ROOT_MODE),
            )?;
        }

        // The guard removes a still-running container if timeout/cancellation
        // drops the Docker client before --rm can complete.
        let mut resource = DockerResource::new("offline");
        let mut command = self.offline_run(image, &resource.name)?;
        let result = output(command.args(args), seconds).await?;
        resource.mark_removed();
        if !result.status.success() {
            return Err(failure_with_stderr(
                "offline fixture CLI command failed",
                &result.stderr,
            ));
        }

        Ok(String::from_utf8(result.stdout)?)
    }

    /// `docker run --rm` for a one-off CLI command against the stopped fixture's data.
    fn offline_run(&self, image: &str, name: &str) -> Result<Command> {
        let mut command = docker_command();
        command.args(["run", "--rm", "--name", name]);
        command.args(hardened_args(self.resources));
        command.args([
            "-v",
            &format!(
                "{}:/data",
                self.volume
                    .as_deref()
                    .ok_or("missing disposable data volume")?
            ),
        ]);
        command.args(["-v", &format!("{}:/fixture:ro", self.root.path().display())]);

        command.args([
            "-e",
            "LOGBROOK_DATA_DIR=/data",
            "-e",
            "LOGBROOK_SOURCE=smoke",
            "-e",
            "LOGBROOK_INGEST_TOKEN=synthetic-cli-ingest-token",
            "-e",
            "LOGBROOK_READ_TOKEN=synthetic-cli-reader-token",
            "-e",
            "LOGBROOK_ADMIN_TOKEN=synthetic-cli-admin-token",
        ]);
        command.arg(image);

        if self.root.path().join("config.toml").exists() {
            command.args(["--config", "/fixture/config.toml"]);
        }

        Ok(command)
    }

    /// Runs the CLI as a client of this fixture.
    ///
    /// `json` adds `--json` and parses stdout as JSON. `success` says whether the
    /// command must exit zero; a failing command must produce empty stdout and no
    /// credential in stderr, and its stderr is returned as a string.
    ///
    /// # Errors
    ///
    /// Returns an error when the exit status differs from `success`, failure output
    /// carries a credential or stdout, or JSON cannot be parsed.
    pub async fn cli(&self, args: &[&str], json: bool, success: bool) -> Result<Value> {
        let mut command = self.cli_command(json);
        let result = output(command.args(args), COMMAND_TIMEOUT_SECS).await?;
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
            self.check_failure_output(&stdout, &stderr)?;
            return Ok(Value::String(stderr));
        }

        if json {
            Ok(serde_json::from_str(&stdout)?)
        } else {
            Ok(Value::String(stdout))
        }
    }

    /// The CLI invocation for this fixture, with `--json` when `json` is set.
    fn cli_command(&self, json: bool) -> Command {
        let mut command = match &self.container {
            Some(name) => container_exec(name),
            None => {
                let mut command = self.native_command(false);
                command.args(["--url", &self.base]);
                command
            }
        };
        if json {
            command.arg("--json");
        }
        command
    }

    /// A failing CLI command must print nothing to stdout and must not echo a credential.
    fn check_failure_output(&self, stdout: &str, stderr: &str) -> Result<()> {
        let echoes_credential = [&self.ingest_token, &self.read_token, &self.admin_token]
            .iter()
            .any(|token| stderr.contains(token.as_str()));
        if !stdout.is_empty() || echoes_credential {
            return Err("CLI failure output violated credential/stdout constraints".into());
        }

        Ok(())
    }

    /// Stops the server gracefully and checks that it exited cleanly.
    ///
    /// # Errors
    ///
    /// Returns an error when the container or process does not exit with status zero
    /// within `SHUTDOWN_GRACE`.
    pub async fn stop(&mut self) -> Result<()> {
        if let Some(name) = &self.container {
            let grace = SHUTDOWN_GRACE.as_secs().to_string();
            docker(&["stop", "--time", &grace, name]).await?;

            if docker(&["inspect", "--format", "{{.State.ExitCode}}", name]).await? != "0" {
                return Err("container shutdown was not clean".into());
            }
        }

        if let Some(mut child) = self.child.take() {
            let pid = child.id().ok_or("native fixture missing PID")?.to_string();
            output(
                Command::new(KILL).args(["-TERM", &pid]),
                SHORT_COMMAND_TIMEOUT_SECS,
            )
            .await?;

            let status = match timeout(SHUTDOWN_GRACE, child.wait()).await {
                Ok(result) => result?,
                Err(_) => {
                    child.start_kill()?;
                    timeout(KILL_REAP_TIMEOUT, child.wait()).await??;
                    return Err("native shutdown deadline exceeded".into());
                }
            };
            if !status.success() {
                return Err("native fixture shutdown was not clean".into());
            }
        }

        Ok(())
    }

    /// Starts a stopped fixture again on the same data and waits for readiness.
    ///
    /// # Errors
    ///
    /// Returns an error when the fixture cannot start or is not ready in time.
    pub async fn start_again(&mut self) -> Result<()> {
        if let Some(name) = &self.container {
            docker(&["start", name]).await?;
            self.refresh_port().await?;
        } else {
            self.spawn_native()?;
        }

        self.ready().await
    }

    /// Stops the fixture and starts it again.
    ///
    /// # Errors
    ///
    /// Returns an error when the fixture cannot stop or start again.
    pub async fn restart(&mut self) -> Result<()> {
        self.stop().await?;
        self.start_again().await
    }

    /// Stops the server and removes its Docker resources, reporting both failures.
    ///
    /// # Errors
    ///
    /// Returns the stop error, or the cleanup error when only cleanup failed.
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
            let _ = std::process::Command::new(KILL)
                .args(["-TERM", &pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();

            if !wait_for_exit(SHUTDOWN_GRACE, || child.try_wait()) {
                let _ = child.start_kill();
                reap_after_kill(child);
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
    async fn snapshot_output_streams_to_file_instead_of_capturing_stdout() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let result = output_to_file(
            Command::new(std::env::current_exe()?).arg("--list"),
            file.path(),
            10,
        )
        .await?;
        assert!(result.status.success());
        assert!(result.stdout.is_empty());

        let data = std::fs::read_to_string(file.path())?;
        assert!(data.contains("snapshot_output_streams_to_file_instead_of_capturing_stdout"));
        Ok(())
    }

    #[test]
    fn snapshot_manifest_tracks_physical_content_and_ownership() -> Result<()> {
        let archive = |content: &[u8], uid| -> Result<tempfile::NamedTempFile> {
            let mut file = tempfile::NamedTempFile::new()?;
            let mut tar = tar::Builder::new(file.as_file_mut());
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_uid(uid);
            header.set_gid(10001);
            header.set_mode(0o600);
            header.set_cksum();

            tar.append_data(&mut header, "indexes/default/data.duckdb", content)?;
            tar.finish()?;
            drop(tar);
            Ok(file)
        };
        let original = archive(b"physical fixture", 10001)?;
        let identical = archive(b"physical fixture", 10001)?;
        let changed = archive(b"changed fixture", 10001)?;
        let wrong_owner = archive(b"physical fixture", 501)?;
        assert_eq!(
            snapshot_manifest(original.path())?,
            snapshot_manifest(identical.path())?
        );

        assert_ne!(
            snapshot_manifest(original.path())?,
            snapshot_manifest(changed.path())?
        );

        assert_ne!(
            snapshot_manifest(original.path())?,
            snapshot_manifest(wrong_owner.path())?
        );
        Ok(())
    }

    #[test]
    fn explicit_fixture_resources_preserve_other_hardening_defaults() {
        assert_eq!(hardened_args(None), HARDENED);

        let args = hardened_args(Some(ResourceLimits {
            cpus: 4,
            memory_mib: 1024,
        }));
        for (flag, value) in [
            ("--cpus", "4"),
            ("--memory", "1024m"),
            ("--memory-swap", "1024m"),
        ] {
            let index = args.iter().position(|arg| arg == flag).unwrap();
            assert_eq!(args[index + 1], value);
        }

        assert!(args.iter().any(|arg| arg == "no-new-privileges:true"));
        assert!(args.iter().any(|arg| arg == "ALL"));
    }

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
