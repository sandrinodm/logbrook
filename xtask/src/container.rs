//! Packaging checks for the container image and the CLI, run against disposable fixtures.
use crate::{
    Result, emit_json,
    runtime::{self, DockerResource, Fixture},
};
use clap::Args;
use serde_json::{Value, json};
use std::{
    io::Read,
    path::{Path, PathBuf},
};

/// Image size budget, in MiB, used when `--max-image-mib` is not given.
const DEFAULT_MAX_IMAGE_MIB: f64 = 110.0;

/// Bytes in one mebibyte.
const BYTES_PER_MIB: f64 = 1_048_576.0;

/// Longest container name or ID that the cgroup observer accepts.
const MAX_CONTAINER_NAME_BYTES: usize = 128;

/// Bound, in seconds, for the cgroup observer container.
const OBSERVER_TIMEOUT_SECS: u64 = 30;

/// Image that runs the cgroup observer, pinned by digest.
const OBSERVER_IMAGE: &str =
    "debian:trixie-slim@sha256:f6b08b09a05725f9431502d3b5714f5c4576e82c1a84fa1debe0c24a7e0c3ec4";

/// Shell script that prints each requested counter, NUL-terminated.
const OBSERVER_SCRIPT: &str =
    "set -eu; for name do cat \"/proc/1/root/sys/fs/cgroup/$name\"; printf '\\000'; done";

/// Cgroup counters that the observer may read.
const OBSERVER_COUNTERS: [&str; 4] = ["memory.current", "memory.peak", "memory.events", "cpu.stat"];

/// Unprivileged user that the image and the observer run as.
const RUNTIME_USER: &str = "10001:10001";

/// `NanoCpus` that the smoke container must report: two CPUs.
const EXPECTED_NANO_CPUS: i64 = 2_000_000_000;

/// `Memory` in bytes that the smoke container must report: two GiB.
const EXPECTED_MEMORY_BYTES: i64 = 2 * 1024 * 1024 * 1024;

/// HTTP status of a successful request.
const STATUS_OK: u16 = 200;

/// HTTP status of a request without a valid credential.
const STATUS_UNAUTHORIZED: u16 = 401;

/// HTTP status of a route that does not exist.
const STATUS_NOT_FOUND: u16 = 404;

/// Log message that the smoke test ingests and then searches for.
const SMOKE_MESSAGE: &str = "container parquet smoke";

/// Half-width, in milliseconds, of the time window that the CLI checks search.
const QUERY_WINDOW_MS: i64 = 1000;

/// Options for `container-smoke`.
#[derive(Args)]
pub struct SmokeArgs {
    /// Image to test.
    #[arg(long, default_value = "logbrook:local")]
    pub image: String,
}

/// Options for `image-inspect`.
#[derive(Args)]
pub struct InspectArgs {
    /// Image to inspect.
    #[arg(long)]
    pub image: String,
    /// Largest accepted image size, in MiB.
    #[arg(long, default_value_t = DEFAULT_MAX_IMAGE_MIB)]
    pub max_image_mib: f64,
}

/// Options for `container-probe`.
#[derive(Args)]
pub struct ProbeArgs {
    /// Name or ID of the running container to observe.
    pub container: String,
    /// Cgroup counters to read; all of them when empty.
    pub counters: Vec<String>,
}

/// Options for `check-cli`.
#[derive(Args)]
pub struct CliArgs {
    /// Binary to test when no image is given.
    #[arg(
        long,
        default_value = "target/debug/logbrook",
        conflicts_with = "image"
    )]
    pub binary: PathBuf,
    /// Image to test instead of a local binary.
    #[arg(long)]
    pub image: Option<String>,
    /// File that receives the JSON report.
    #[arg(long)]
    pub output: Option<PathBuf>,
}

/// Returns `message` as an error unless `ok` holds.
fn ensure(ok: bool, message: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(message.to_owned().into())
    }
}

/// Reads cgroup counters from the container `container`, through an unprivileged observer
/// that shares its PID namespace. An empty `counters` reads all of them.
///
/// # Errors
///
/// Returns an error when the name is invalid, a counter is not in the fixed list, the
/// observer fails, or its output does not match the requested counters.
pub async fn probe(container: &str, counters: &[String]) -> Result<Value> {
    ensure(
        is_valid_container_name(container),
        "invalid Docker container name or ID",
    )?;
    let names = requested_counters(counters)?;

    let mut resource = DockerResource::new("observer");
    let pid = format!("--pid=container:{container}");
    let mut args = observer_args(&resource.name, &pid);
    args.extend(&names);
    let result = runtime::output(
        tokio::process::Command::new("docker").args(&args),
        OBSERVER_TIMEOUT_SECS,
    )
    .await?;
    // --rm already removed a successful observer; Drop still cleans failed operations.
    resource.mark_removed();
    ensure(
        result.status.success(),
        "unprivileged cgroup observer failed",
    )?;

    let text = String::from_utf8(result.stdout)?;
    let values: Vec<_> = text.split('\0').collect();
    ensure(
        values.len() == names.len() + 1 && values.last() == Some(&""),
        "invalid observer response",
    )?;

    Ok(Value::Object(
        names
            .iter()
            .zip(values)
            .map(|(name, value)| (name.to_string(), json!(value)))
            .collect(),
    ))
}

/// Whether `name` is a plausible container name or ID that is safe to pass to `docker`.
fn is_valid_container_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_CONTAINER_NAME_BYTES
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
}

/// The requested counters, or all of them when none are requested, checked against
/// [`OBSERVER_COUNTERS`].
fn requested_counters(counters: &[String]) -> Result<Vec<&str>> {
    let names: Vec<&str> = if counters.is_empty() {
        OBSERVER_COUNTERS.to_vec()
    } else {
        counters.iter().map(String::as_str).collect()
    };
    ensure(
        names.iter().all(|name| OBSERVER_COUNTERS.contains(name)),
        "only fixed supported cgroup counters may be read",
    )?;
    Ok(names)
}

/// `docker run` arguments for the observer, which has no network, a read-only root and
/// no capabilities.
fn observer_args<'a>(name: &'a str, pid: &'a str) -> Vec<&'a str> {
    vec![
        "run",
        "--rm",
        "--name",
        name,
        pid,
        "--user",
        RUNTIME_USER,
        "--network",
        "none",
        "--cap-drop",
        "ALL",
        "--read-only",
        "--security-opt",
        "no-new-privileges:true",
        "--pids-limit",
        "32",
        "--memory",
        "32m",
        "--cpus",
        "0.25",
        OBSERVER_IMAGE,
        "/bin/sh",
        "-c",
        OBSERVER_SCRIPT,
        "cgroup-observer",
    ]
}

/// Checks the image's size budget, runtime user, license, notices and executable, then
/// prints its facts.
///
/// # Errors
///
/// Returns an error when the budget is invalid, the image cannot be inspected, or any check
/// fails.
pub async fn inspect(args: InspectArgs) -> Result<()> {
    ensure(
        args.max_image_mib.is_finite() && args.max_image_mib > 0.0,
        "image budget must be positive and finite",
    )?;

    let metadata: Value =
        serde_json::from_str(&runtime::docker(&["image", "inspect", &args.image]).await?)?;
    let info = &metadata[0];
    let mut resource = DockerResource::new("inspect");
    runtime::docker(&["create", "--name", &resource.name, &args.image]).await?;

    let executable_bytes = executable_size(&resource.name).await?;
    check_license(&resource.name, info).await?;
    check_notices(&resource.name).await?;

    let size = info["Size"].as_u64().ok_or("image size missing")?;
    emit_json(&json!({
        "image": args.image,
        "id": info["Id"],
        "architecture": info["Architecture"],
        "image_bytes": size,
        "image_mib": (size as f64 / BYTES_PER_MIB * 100.0).round() / 100.0,
        "executable_bytes": executable_bytes.ok_or("image executable missing")?,
        "user": info["Config"]["User"],
    }))?;

    let checks = ensure(
        size as f64 <= args.max_image_mib * BYTES_PER_MIB,
        "image exceeds size budget",
    )
    .and(ensure(
        info["Config"]["User"] == RUNTIME_USER,
        "unexpected runtime user",
    ));
    let cleanup = resource.cleanup().await;
    runtime::finish(checks, cleanup)
}

/// Size in bytes of the first regular file in the `logbrook` binary copied from `name`.
async fn executable_size(name: &str) -> Result<Option<u64>> {
    let wire =
        runtime::docker_bytes(&["cp", &format!("{name}:/usr/local/bin/logbrook"), "-"]).await?;
    let mut archive = tar::Archive::new(wire.as_slice());
    for entry in archive.entries()? {
        let entry = entry?;
        if entry.header().entry_type().is_file() {
            return Ok(Some(entry.size()));
        }
    }

    Ok(None)
}

/// Checks that the image carries the MIT license text and its label.
async fn check_license(name: &str, info: &Value) -> Result<()> {
    let license_wire = runtime::docker_bytes(&[
        "cp",
        &format!("{name}:/usr/share/licenses/logbrook/LICENSE"),
        "-",
    ])
    .await?;
    let mut license_archive = tar::Archive::new(license_wire.as_slice());
    let mut license_valid = false;
    for entry in license_archive.entries()? {
        let mut entry = entry?;
        if entry.header().entry_type().is_file() {
            let mut text = String::new();
            entry.read_to_string(&mut text)?;
            license_valid =
                text.contains("MIT License") && text.contains("Permission is hereby granted");
        }
    }

    ensure(
        license_valid && info["Config"]["Labels"]["org.opencontainers.image.licenses"] == "MIT",
        "MIT license packaging missing",
    )
}

/// Checks that the image carries the same third-party notices as the repository.
async fn check_notices(name: &str) -> Result<()> {
    let notices_wire = runtime::docker_bytes(&[
        "cp",
        &format!("{name}:/usr/share/licenses/logbrook/THIRD_PARTY_NOTICES.txt"),
        "-",
    ])
    .await?;
    let expected = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../THIRD_PARTY_NOTICES.txt"),
    )?;
    let mut notices_archive = tar::Archive::new(notices_wire.as_slice());
    let mut notices_valid = false;
    for entry in notices_archive.entries()? {
        let mut entry = entry?;
        if entry.header().entry_type().is_file() {
            let mut text = String::new();
            entry.read_to_string(&mut text)?;
            notices_valid = text == expected;
        }
    }

    ensure(notices_valid, "third-party notices missing or out of date")
}

/// Starts the image as a smoke fixture, runs the container checks and shuts it down.
///
/// # Errors
///
/// Returns the first failing check, or the shutdown error when only shutdown failed.
pub async fn smoke(args: SmokeArgs) -> Result<()> {
    let mut fixture = Fixture::start(
        PathBuf::from("target/debug/logbrook"),
        Some(args.image.clone()),
    )
    .await?;
    let result = smoke_checks(&mut fixture, &args.image).await;
    let cleanup = fixture.shutdown().await;
    runtime::finish(result, cleanup)
}

/// The container checks in order: settings, HTTP surface, durable ingest, offline archive,
/// and search after a restart.
async fn smoke_checks(f: &mut Fixture, image: &str) -> Result<()> {
    let name = f.container.clone().ok_or("missing container")?;
    let info: Value = serde_json::from_str(&runtime::docker(&["inspect", &name]).await?)?;
    let info = &info[0];
    check_container_settings(info)?;

    ensure(
        f.command(&["healthcheck"]).await?.trim().is_empty(),
        "CLI healthcheck output unexpected",
    )?;

    let version = f.command(&["--version"]).await?;
    ensure(
        info["Config"]["Labels"]["org.opencontainers.image.version"]
            == version.split_whitespace().last().unwrap_or(""),
        "version label mismatch",
    )?;

    check_http_surface(f).await?;

    let time = crate::now_ms();
    let query = verify_durable_ingest(f, time).await?;

    f.stop().await?;
    verify_archive(f, &name, image, time).await?;
    f.start_again().await?;
    verify_restart(f, &query).await?;

    let architecture =
        runtime::docker(&["image", "inspect", "--format", "{{.Architecture}}", image]).await?;
    emit_json(&json!({
        "image": image,
        "architecture": architecture,
        "version": version.trim(),
        "uid": 10001,
        "cpu_limit": 2,
        "memory_limit_gib": 2,
        "verified": [
            "health",
            "CLI healthcheck",
            "read-only root",
            "dropped capabilities",
            "no-new-privileges",
            "readiness",
            "authentication",
            "JSON route fallback",
            "OpenAPI",
            "durable ingest/search",
            "offline Parquet archive",
            "restart/search",
            "graceful shutdown",
        ],
    }))
}

/// Checks the hardening flags and resource limits that the container was created with.
fn check_container_settings(info: &Value) -> Result<()> {
    ensure(
        info["Config"]["User"] == RUNTIME_USER
            && info["HostConfig"]["ReadonlyRootfs"] == true
            && info["HostConfig"]["CapDrop"] == json!(["ALL"])
            && info["HostConfig"]["SecurityOpt"]
                .as_array()
                .is_some_and(|a| a.contains(&json!("no-new-privileges:true"))),
        "container hardening mismatch",
    )?;
    ensure(
        info["HostConfig"]["NanoCpus"] == EXPECTED_NANO_CPUS
            && info["HostConfig"]["Memory"] == EXPECTED_MEMORY_BYTES,
        "container resource limits mismatch",
    )
}

/// Checks health, authentication, the JSON fallback for unknown routes and the OpenAPI document.
async fn check_http_surface(f: &Fixture) -> Result<()> {
    ensure(
        f.request("/health", None, None).await?.0 == STATUS_OK
            && f.request("/logs", None, None).await?.0 == STATUS_UNAUTHORIZED,
        "health/auth check failed",
    )?;

    for path in ["/", "/assets/old-browser-client.js", "/unknown-route"] {
        let (status, value) = f.request(path, None, None).await?;
        ensure(
            status == STATUS_NOT_FOUND && value["error"].is_string(),
            "JSON route fallback failed",
        )?;
    }

    let (status, value) = f.request("/openapi.json", None, None).await?;
    ensure(
        status == STATUS_OK
            && value["openapi"]
                .as_str()
                .is_some_and(|v| v.starts_with("3.")),
        "OpenAPI check failed",
    )
}

/// Ingests one smoke event at `time` and checks that it is searchable. Returns the search
/// query for that event's time window.
async fn verify_durable_ingest(f: &Fixture, time: i64) -> Result<String> {
    let events = json!([{
        "time": time,
        "level": 30,
        "msg": SMOKE_MESSAGE,
        "service": "packaging",
        "details": {"ok": true},
    }]);
    let (status, value) = f
        .request("/logs/ingest", Some(&f.ingest_token), Some(&events))
        .await?;
    ensure(
        status == STATUS_OK && value["accepted"] == 1,
        "ingestion failed",
    )?;

    let query = format!("?from={}&to={}&source=smoke", time - 1, time + 2);
    let (status, value) = f
        .request(&format!("/logs{query}"), Some(&f.read_token), None)
        .await?;
    ensure(
        status == STATUS_OK && value["events"][0]["message"] == SMOKE_MESSAGE,
        "durable query failed",
    )?;
    Ok(query)
}

/// Archives the events before `time + 1` with a hardened one-off container, then checks that
/// the archive holds Parquet files with valid magic bytes.
async fn verify_archive(f: &Fixture, name: &str, image: &str, time: i64) -> Result<()> {
    let volume = f.volume.as_deref().ok_or("missing data volume")?;
    let mount = format!("{volume}:/data");
    let before = (time + 1).to_string();
    let mut archive_resource = DockerResource::new("archive");
    let mut args = vec!["run", "--rm", "--name", &archive_resource.name];
    args.extend(runtime::HARDENED);
    args.extend([
        "--network",
        "none",
        "-v",
        &mount,
        "-e",
        "LOGBROOK_INGEST_TOKEN=synthetic-cli-ingest-token",
        "-e",
        "LOGBROOK_READ_TOKEN=synthetic-cli-reader-token",
        "-e",
        "LOGBROOK_SOURCE=smoke",
        image,
        "archive",
        "--before",
        &before,
    ]);

    let archived = runtime::docker(&args).await?;
    archive_resource.mark_removed();
    let value: Value =
        serde_json::from_str(archived.lines().last().ok_or("archive output missing")?)?;
    ensure(value["archived"] == 1, "archive count mismatch")?;

    let wire =
        runtime::docker_bytes(&["cp", &format!("{name}:/data/indexes/default/archives"), "-"])
            .await?;
    let mut archive = tar::Archive::new(wire.as_slice());
    let mut count = 0;
    for entry in archive.entries()? {
        let mut entry = entry?;
        if entry.header().entry_type().is_file()
            && entry.path()?.extension().is_some_and(|p| p == "parquet")
        {
            let mut data = Vec::new();
            entry.read_to_end(&mut data)?;
            ensure(
                data.starts_with(b"PAR1") && data.ends_with(b"PAR1"),
                "invalid Parquet magic",
            )?;
            count += 1;
        }
    }

    ensure(count > 0, "no Parquet file persisted")
}

/// After a restart, checks that the smoke event is still counted and keeps its attributes.
async fn verify_restart(f: &Fixture, query: &str) -> Result<()> {
    let (status, value) = f
        .request(&format!("/logs/count{query}"), Some(&f.read_token), None)
        .await?;
    ensure(
        status == STATUS_OK && value["count"] == 1,
        "restart count failed",
    )?;

    let (status, value) = f
        .request(&format!("/logs{query}"), Some(&f.read_token), None)
        .await?;
    ensure(
        status == STATUS_OK && value["events"][0]["attributes"]["details"]["ok"] == true,
        "restart attributes failed",
    )
}

/// Runs the CLI checks against a fixture and prints the report, also writing it to
/// `--output` when given.
///
/// # Errors
///
/// Returns the first failing check, or the shutdown error when only shutdown failed, or
/// the report cannot be written.
pub async fn check_cli(args: CliArgs) -> Result<()> {
    let mode = if args.image.is_some() {
        "container"
    } else {
        "native"
    };

    let mut fixture = Fixture::start(args.binary, args.image).await?;
    let checked = cli_checks(&mut fixture).await;
    let cleanup = fixture.shutdown().await;
    runtime::finish(checked, cleanup)?;

    let report = json!({
        "passed": true,
        "mode": mode,
        "checks": [
            "idempotent create",
            "list/show physical bytes",
            "human table",
            "filtered query",
            "count",
            "cursor pagination",
            "delete confirmation",
            "role isolation",
            "protected default",
            "delete/recreate",
            "sibling isolation",
            "restart",
        ],
    });

    if let Some(path) = args.output {
        std::fs::write(
            path,
            format!("{}\n", serde_json::to_string_pretty(&report)?),
        )?;
    }

    emit_json(&report)
}

/// Runs a CLI command that must succeed and returns its JSON output.
async fn cli_json(f: &Fixture, args: &[&str]) -> Result<Value> {
    f.cli(args, true, true).await
}

/// Runs a CLI command that must succeed and returns its plain-text output.
async fn cli_text(f: &Fixture, args: &[&str]) -> Result<Value> {
    f.cli(args, false, true).await
}

/// Runs a CLI command that must fail and discards its output.
async fn cli_rejected(f: &Fixture, args: &[&str]) -> Result<()> {
    f.cli(args, true, false).await?;
    Ok(())
}

/// `count` arguments for `index` over the time window `bounds`.
fn count_args<'a>(index: &'a str, bounds: [&'a str; 4]) -> Vec<&'a str> {
    let mut args = vec!["count", index];
    args.extend(bounds);
    args
}

/// `query` arguments for `index` over the time window `bounds`.
fn query_args<'a>(index: &'a str, bounds: [&'a str; 4]) -> Vec<&'a str> {
    let mut args = vec!["query", index];
    args.extend(bounds);
    args
}

/// Runs every CLI check in order against a running fixture, restarting it once near the end.
async fn cli_checks(f: &mut Fixture) -> Result<()> {
    for name in ["payments", "payments", "audit"] {
        ensure(
            cli_json(f, &["indexes", "create", name]).await?["name"] == name,
            "idempotent create failed",
        )?;
    }

    let now = crate::now_ms();
    let message = "checkout / ? & café";
    ingest_index_events(f, now, message).await?;
    check_index_storage(f).await?;

    let from = (now - QUERY_WINDOW_MS).to_string();
    let to = (now + QUERY_WINDOW_MS).to_string();
    let bounds = ["--from", &from, "--to", &to];
    check_count_and_query(f, bounds, message).await?;
    check_roles_and_delete(f, bounds).await?;
    check_recreate_and_restart(f, bounds).await
}

/// Ingests the checkout events into `payments` and one sibling event into `audit`.
async fn ingest_index_events(f: &Fixture, now: i64, message: &str) -> Result<()> {
    let events = json!([
        {"time": now, "level": 40, "msg": message, "service": "checkout"},
        {"time": now + 1, "level": 30, "msg": "other", "service": "worker"},
    ]);
    let (status, _) = f
        .request(
            "/indexes/payments/logs/ingest",
            Some(&f.ingest_token),
            Some(&events),
        )
        .await?;
    ensure(status == STATUS_OK, "payments ingestion failed")?;

    let (status, _) = f
        .request(
            "/indexes/audit/logs/ingest",
            Some(&f.ingest_token),
            Some(&json!([{"time":now,"level":30,"msg":"sibling"}])),
        )
        .await?;
    ensure(status == STATUS_OK, "audit ingestion failed")
}

/// Checks the index listing, its physical size and readiness, and the human-readable table.
async fn check_index_storage(f: &Fixture) -> Result<()> {
    let listing = cli_json(f, &["indexes", "list"]).await?;
    let info = listing["indexes"]
        .as_array()
        .and_then(|list| list.iter().find(|item| item["name"] == "payments"))
        .ok_or("payments index missing")?;
    ensure(
        info["size_bytes"].as_u64().is_some_and(|v| v > 0) && info["ready"] == true,
        "index physical bytes/readiness failed",
    )?;

    ensure(
        cli_json(f, &["indexes", "show", "payments"]).await?["size_bytes"]
            .as_u64()
            .is_some_and(|v| v > 0),
        "index show failed",
    )?;

    ensure(
        cli_text(f, &["indexes", "list"])
            .await?
            .as_str()
            .is_some_and(|v| v.contains("payments")),
        "human table failed",
    )
}

/// Checks counting, filtered search and cursor pagination over the time window.
async fn check_count_and_query(f: &Fixture, bounds: [&str; 4], message: &str) -> Result<()> {
    ensure(
        cli_json(f, &count_args("payments", bounds)).await?["count"] == 2,
        "CLI count failed",
    )?;

    let query = query_args("payments", bounds);
    let mut filtered = query.clone();
    filtered.extend(["--message", message]);
    let page = cli_json(f, &filtered).await?;
    ensure(
        page["events"].as_array().is_some_and(|a| a.len() == 1)
            && page["events"][0]["message"] == message,
        "filtered query failed",
    )?;

    let mut first_args = query.clone();
    first_args.extend(["--limit", "1"]);
    let first = cli_json(f, &first_args).await?;
    let cursor = first["next_cursor"]
        .as_str()
        .filter(|v| !v.is_empty())
        .ok_or("pagination cursor missing")?;
    let mut second_args = first_args.clone();
    second_args.extend(["--cursor", cursor]);
    let second = cli_json(f, &second_args).await?;
    ensure(
        second["events"].as_array().is_some_and(|a| a.len() == 1)
            && first["events"][0]["id"] != second["events"][0]["id"],
        "cursor pagination failed",
    )
}

/// Checks that deletes are refused without the admin role and that the default index is
/// protected, then deletes `payments` and checks that `audit` is untouched.
async fn check_roles_and_delete(f: &Fixture, bounds: [&str; 4]) -> Result<()> {
    cli_rejected(f, &["indexes", "delete", "payments"]).await?;
    cli_rejected(
        f,
        &[
            "--token",
            &f.read_token,
            "indexes",
            "delete",
            "payments",
            "--yes",
        ],
    )
    .await?;

    let mut admin = vec!["--token", &f.admin_token];
    admin.extend(&query_args("payments", bounds));
    cli_rejected(f, &admin).await?;

    ensure(
        cli_json(f, &count_args("payments", bounds)).await?["count"] == 2,
        "role isolation modified index",
    )?;

    cli_rejected(f, &["indexes", "delete", "default", "--yes"]).await?;
    ensure(
        cli_json(f, &["indexes", "delete", "payments", "--yes"]).await?
            == json!({"deleted":"payments"}),
        "index deletion failed",
    )?;

    cli_rejected(f, &["indexes", "show", "payments"]).await?;
    ensure(
        cli_json(f, &count_args("audit", bounds)).await?["count"] == 1,
        "sibling isolation failed",
    )
}

/// Recreates `payments` empty, restarts the fixture and checks that the deletion and the
/// sibling index both survived.
async fn check_recreate_and_restart(f: &mut Fixture, bounds: [&str; 4]) -> Result<()> {
    ensure(
        cli_json(f, &["indexes", "create", "payments"]).await?["name"] == "payments",
        "index recreation failed",
    )?;
    ensure(
        cli_json(f, &count_args("payments", bounds)).await?["count"] == 0,
        "recreated index retained events",
    )?;

    cli_json(f, &["indexes", "delete", "payments", "--yes"]).await?;
    f.restart().await?;

    ensure(
        cli_json(f, &["indexes", "list"]).await?["indexes"]
            .as_array()
            .is_some_and(|a| a.iter().all(|i| i["name"] != "payments")),
        "deleted index reappeared after restart",
    )?;

    ensure(
        cli_json(f, &count_args("audit", bounds)).await?["count"] == 1,
        "sibling durability failed",
    )
}

/// Reads cgroup counters for `container`; see [`probe`].
///
/// # Errors
///
/// Returns the same errors as [`probe`].
pub async fn read_cgroup(container: &str, counters: &[String]) -> Result<Value> {
    probe(container, counters).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn observer_rejects_namespace_and_counter_injection_before_docker() {
        for name in [
            "",
            "--privileged",
            "target/../../host",
            "target;id",
            "target\nname",
        ] {
            assert!(probe(name, &[]).await.is_err());
        }

        for counter in ["../memory.current", "cgroup.procs", "cpu.stat;id"] {
            assert!(probe("valid-container", &[counter.into()]).await.is_err());
        }
    }
}
