use crate::{
    Result, emit_json,
    runtime::{self, DockerResource, Fixture},
};
use clap::Args;
use serde_json::{Value, json};
use std::{io::Read, path::PathBuf};

#[derive(Args)]
pub struct SmokeArgs {
    #[arg(long, default_value = "logbrook:local")]
    pub image: String,
}

#[derive(Args)]
pub struct InspectArgs {
    #[arg(long)]
    pub image: String,
    #[arg(long, default_value_t = 110.0)]
    pub max_image_mib: f64,
}

#[derive(Args)]
pub struct ProbeArgs {
    pub container: String,
    pub counters: Vec<String>,
}

#[derive(Args)]
pub struct CliArgs {
    #[arg(
        long,
        default_value = "target/debug/logbrook",
        conflicts_with = "image"
    )]
    pub binary: PathBuf,
    #[arg(long)]
    pub image: Option<String>,
    #[arg(long)]
    pub output: Option<PathBuf>,
}

fn ensure(ok: bool, message: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(message.to_owned().into())
    }
}

pub async fn probe(container: &str, counters: &[String]) -> Result<Value> {
    ensure(
        !container.is_empty()
            && container.len() <= 128
            && container.as_bytes()[0].is_ascii_alphanumeric()
            && container
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c)),
        "invalid Docker container name or ID",
    )?;
    let defaults = ["memory.current", "memory.peak", "memory.events", "cpu.stat"];
    let names: Vec<&str> = if counters.is_empty() {
        defaults.to_vec()
    } else {
        counters.iter().map(String::as_str).collect()
    };
    ensure(
        names.iter().all(|name| defaults.contains(name)),
        "only fixed supported cgroup counters may be read",
    )?;
    let mut resource = DockerResource::new("observer");
    let pid = format!("--pid=container:{container}");
    let mut args = vec![
        "run",
        "--rm",
        "--name",
        &resource.name,
        &pid,
        "--user",
        "10001:10001",
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
        "debian:trixie-slim@sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f",
        "/bin/sh",
        "-c",
        "set -eu; for name do cat \"/proc/1/root/sys/fs/cgroup/$name\"; printf '\\000'; done",
        "cgroup-observer",
    ];
    args.extend(&names);
    let result = runtime::output(tokio::process::Command::new("docker").args(&args), 30).await?;
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
    let wire = runtime::docker_bytes(&[
        "cp",
        &format!("{}:/usr/local/bin/logbrook", resource.name),
        "-",
    ])
    .await?;
    let mut archive = tar::Archive::new(wire.as_slice());
    let mut executable_bytes = None;
    for entry in archive.entries()? {
        let entry = entry?;
        if entry.header().entry_type().is_file() {
            executable_bytes = Some(entry.size());
            break;
        }
    }
    let license_wire = runtime::docker_bytes(&[
        "cp",
        &format!("{}:/usr/share/licenses/logbrook/LICENSE", resource.name),
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
    )?;

    let notices_wire = runtime::docker_bytes(&[
        "cp",
        &format!(
            "{}:/usr/share/licenses/logbrook/THIRD_PARTY_NOTICES.txt",
            resource.name
        ),
        "-",
    ])
    .await?;
    let expected = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../THIRD_PARTY_NOTICES.txt"),
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
    ensure(notices_valid, "third-party notices missing or out of date")?;

    let size = info["Size"].as_u64().ok_or("image size missing")?;
    emit_json(&json!({
        "image": args.image,
        "id": info["Id"],
        "architecture": info["Architecture"],
        "image_bytes": size,
        "image_mib": (size as f64 / 1048576.0 * 100.0).round() / 100.0,
        "executable_bytes": executable_bytes.ok_or("image executable missing")?,
        "user": info["Config"]["User"],
    }))?;
    let checks = ensure(
        size as f64 <= args.max_image_mib * 1048576.0,
        "image exceeds size budget",
    )
    .and(ensure(
        info["Config"]["User"] == "10001:10001",
        "unexpected runtime user",
    ));
    let cleanup = resource.cleanup().await;
    runtime::finish(checks, cleanup)
}

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

async fn smoke_checks(f: &mut Fixture, image: &str) -> Result<()> {
    let name = f.container.clone().ok_or("missing container")?;
    let info: Value = serde_json::from_str(&runtime::docker(&["inspect", &name]).await?)?;
    let info = &info[0];
    ensure(
        info["Config"]["User"] == "10001:10001"
            && info["HostConfig"]["ReadonlyRootfs"] == true
            && info["HostConfig"]["CapDrop"] == json!(["ALL"])
            && info["HostConfig"]["SecurityOpt"]
                .as_array()
                .is_some_and(|a| a.contains(&json!("no-new-privileges:true"))),
        "container hardening mismatch",
    )?;
    ensure(
        info["HostConfig"]["NanoCpus"] == 2_000_000_000_i64
            && info["HostConfig"]["Memory"] == 2_147_483_648_i64,
        "container resource limits mismatch",
    )?;
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
    ensure(
        f.request("/health", None, None).await?.0 == 200
            && f.request("/logs", None, None).await?.0 == 401,
        "health/auth check failed",
    )?;
    for path in ["/", "/assets/old-browser-client.js", "/unknown-route"] {
        let (status, value) = f.request(path, None, None).await?;
        ensure(
            status == 404 && value["error"].is_string(),
            "JSON route fallback failed",
        )?;
    }
    let (status, value) = f.request("/openapi.json", None, None).await?;
    ensure(
        status == 200
            && value["openapi"]
                .as_str()
                .is_some_and(|v| v.starts_with("3.")),
        "OpenAPI check failed",
    )?;
    let time = crate::now_ms();
    let events = json!([{"time":time,"level":30,"msg":"container parquet smoke","service":"packaging","details":{"ok":true}}]);
    let (status, value) = f
        .request("/logs/ingest", Some(&f.ingest_token), Some(&events))
        .await?;
    ensure(status == 200 && value["accepted"] == 1, "ingestion failed")?;
    let query = format!("?from={}&to={}&source=smoke", time - 1, time + 2);
    let (status, value) = f
        .request(&format!("/logs{query}"), Some(&f.read_token), None)
        .await?;
    ensure(
        status == 200 && value["events"][0]["message"] == "container parquet smoke",
        "durable query failed",
    )?;
    f.stop().await?;
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
    ensure(count > 0, "no Parquet file persisted")?;
    f.start_again().await?;
    let (status, value) = f
        .request(&format!("/logs/count{query}"), Some(&f.read_token), None)
        .await?;
    ensure(status == 200 && value["count"] == 1, "restart count failed")?;
    let (status, value) = f
        .request(&format!("/logs{query}"), Some(&f.read_token), None)
        .await?;
    ensure(
        status == 200 && value["events"][0]["attributes"]["details"]["ok"] == true,
        "restart attributes failed",
    )?;
    emit_json(&json!({
        "image": image,
        "architecture": runtime::docker(&["image", "inspect", "--format", "{{.Architecture}}", image]).await?,
        "version": version.trim(),
        "uid": 10001,
        "cpu_limit": 2,
        "memory_limit_gib": 2,
        "verified": [
            "health", "CLI healthcheck", "read-only root", "dropped capabilities",
            "no-new-privileges", "readiness", "authentication", "JSON route fallback",
            "OpenAPI", "durable ingest/search", "offline Parquet archive",
            "restart/search", "graceful shutdown",
        ],
    }))
}

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
            "idempotent create", "list/show physical bytes", "human table", "filtered query",
            "count", "cursor pagination", "delete confirmation", "role isolation",
            "protected default", "delete/recreate", "sibling isolation", "restart",
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

async fn cli_checks(f: &mut Fixture) -> Result<()> {
    for name in ["payments", "payments", "audit"] {
        ensure(
            f.cli(&["indexes", "create", name], true, true).await?["name"] == name,
            "idempotent create failed",
        )?;
    }
    let now = crate::now_ms();
    let message = "checkout / ? & café";
    let events = json!([{"time":now,"level":40,"msg":message,"service":"checkout"},{"time":now+1,"level":30,"msg":"other","service":"worker"}]);
    let (status, _) = f
        .request(
            "/indexes/payments/logs/ingest",
            Some(&f.ingest_token),
            Some(&events),
        )
        .await?;
    ensure(status == 200, "payments ingestion failed")?;
    let (status, _) = f
        .request(
            "/indexes/audit/logs/ingest",
            Some(&f.ingest_token),
            Some(&json!([{"time":now,"level":30,"msg":"sibling"}])),
        )
        .await?;
    ensure(status == 200, "audit ingestion failed")?;
    let listing = f.cli(&["indexes", "list"], true, true).await?;
    let info = listing["indexes"]
        .as_array()
        .and_then(|list| list.iter().find(|item| item["name"] == "payments"))
        .ok_or("payments index missing")?;
    ensure(
        info["size_bytes"].as_u64().is_some_and(|v| v > 0) && info["ready"] == true,
        "index physical bytes/readiness failed",
    )?;
    ensure(
        f.cli(&["indexes", "show", "payments"], true, true).await?["size_bytes"]
            .as_u64()
            .is_some_and(|v| v > 0),
        "index show failed",
    )?;
    ensure(
        f.cli(&["indexes", "list"], false, true)
            .await?
            .as_str()
            .is_some_and(|v| v.contains("payments")),
        "human table failed",
    )?;
    let from = (now - 1000).to_string();
    let to = (now + 1000).to_string();
    let bounds = ["--from", &from, "--to", &to];
    let mut count = vec!["count", "payments"];
    count.extend(bounds);
    ensure(
        f.cli(&count, true, true).await?["count"] == 2,
        "CLI count failed",
    )?;
    let mut query = vec!["query", "payments"];
    query.extend(bounds);
    let mut filtered = query.clone();
    filtered.extend(["--message", message]);
    let page = f.cli(&filtered, true, true).await?;
    ensure(
        page["events"].as_array().is_some_and(|a| a.len() == 1)
            && page["events"][0]["message"] == message,
        "filtered query failed",
    )?;
    let mut first_args = query.clone();
    first_args.extend(["--limit", "1"]);
    let first = f.cli(&first_args, true, true).await?;
    let cursor = first["next_cursor"]
        .as_str()
        .filter(|v| !v.is_empty())
        .ok_or("pagination cursor missing")?;
    let mut second_args = first_args.clone();
    second_args.extend(["--cursor", cursor]);
    let second = f.cli(&second_args, true, true).await?;
    ensure(
        second["events"].as_array().is_some_and(|a| a.len() == 1)
            && first["events"][0]["id"] != second["events"][0]["id"],
        "cursor pagination failed",
    )?;
    f.cli(&["indexes", "delete", "payments"], true, false)
        .await?;
    f.cli(
        &[
            "--token",
            &f.read_token,
            "indexes",
            "delete",
            "payments",
            "--yes",
        ],
        true,
        false,
    )
    .await?;
    let mut admin = vec!["--token", &f.admin_token];
    admin.extend(&query);
    f.cli(&admin, true, false).await?;
    ensure(
        f.cli(&count, true, true).await?["count"] == 2,
        "role isolation modified index",
    )?;
    f.cli(&["indexes", "delete", "default", "--yes"], true, false)
        .await?;
    ensure(
        f.cli(&["indexes", "delete", "payments", "--yes"], true, true)
            .await?
            == json!({"deleted":"payments"}),
        "index deletion failed",
    )?;
    f.cli(&["indexes", "show", "payments"], true, false).await?;
    let mut audit = vec!["count", "audit"];
    audit.extend(bounds);
    ensure(
        f.cli(&audit, true, true).await?["count"] == 1,
        "sibling isolation failed",
    )?;
    ensure(
        f.cli(&["indexes", "create", "payments"], true, true)
            .await?["name"]
            == "payments",
        "index recreation failed",
    )?;
    ensure(
        f.cli(&count, true, true).await?["count"] == 0,
        "recreated index retained events",
    )?;
    f.cli(&["indexes", "delete", "payments", "--yes"], true, true)
        .await?;
    f.restart().await?;
    ensure(
        f.cli(&["indexes", "list"], true, true).await?["indexes"]
            .as_array()
            .is_some_and(|a| a.iter().all(|i| i["name"] != "payments")),
        "deleted index reappeared after restart",
    )?;
    ensure(
        f.cli(&audit, true, true).await?["count"] == 1,
        "sibling durability failed",
    )
}

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
