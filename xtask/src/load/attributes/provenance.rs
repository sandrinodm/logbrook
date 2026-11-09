//! Validated hardware evidence and recorded host/build provenance.
use super::{
    Args,
    constants::{
        CONTAINER_MEMORY_BYTES, CONTAINER_NANO_CPUS, MAX_HARDWARE_PROVENANCE_BYTES,
        PROBE_TIMEOUT_SECS,
    },
};
use crate::{Result, runtime};
use serde_json::{Value, json};
use std::{io::Read, path::Path};
use tokio::process::Command;

/// Host facts that an operator measured and recorded outside the tool.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::load) struct HardwareProvenance {
    pub(super) host_cpu: String,
    pub(super) host_memory_bytes: u64,
    pub(super) storage_description: String,
    pub(super) local_ssd: bool,
    /// Explicit evidence that unrelated host activity was absent during measurement.
    #[serde(default)]
    pub(super) quiet_host: bool,
    pub(super) evidence: String,
}

impl HardwareProvenance {
    /// Trims the text fields and rejects empty ones or a zero memory size.
    pub(super) fn normalize(mut self) -> Result<Self> {
        self.host_cpu = self.host_cpu.trim().to_owned();
        self.storage_description = self.storage_description.trim().to_owned();
        self.evidence = self.evidence.trim().to_owned();

        let text_missing = self.host_cpu.is_empty()
            || self.storage_description.is_empty()
            || self.evidence.is_empty();
        if text_missing || self.host_memory_bytes == 0 {
            return Err(concat!(
                "hardware provenance requires nonempty host_cpu, storage_description, evidence, ",
                "and positive host_memory_bytes"
            )
            .into());
        }

        Ok(self)
    }

    /// Reads a provenance file of at most 64 KiB and validates it.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, is too large, is not the expected JSON
    /// object, or has an empty or zero field.
    pub(in crate::load) fn read(path: &Path) -> Result<Self> {
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take(MAX_HARDWARE_PROVENANCE_BYTES + 1)
            .read_to_end(&mut bytes)?;
        let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if length > MAX_HARDWARE_PROVENANCE_BYTES {
            return Err("hardware provenance exceeds 65536 bytes".into());
        }

        serde_json::from_slice::<Self>(&bytes)?.normalize()
    }
}

/// Whether a run's recorded container inspect output shows the target CPU and memory budget.
pub(super) fn container_budget_verified(run: &Value) -> bool {
    run["resources"]["inspect"]
        .as_str()
        .and_then(|text| serde_json::from_str::<Value>(text).ok())
        .is_some_and(|config| {
            config["NanoCpus"].as_u64() == Some(CONTAINER_NANO_CPUS)
                && config["Memory"].as_u64() == Some(CONTAINER_MEMORY_BYTES)
        })
}

/// Records where and how the run was produced, including the source and binary hashes.
pub(super) async fn provenance(args: &Args) -> Result<Value> {
    let binary_sha256 = if args.image.is_none() {
        Some(crate::load::file_sha256(&args.binary)?)
    } else {
        None
    };

    let commit = runtime::output(
        Command::new("git").args(["rev-parse", "HEAD"]),
        PROBE_TIMEOUT_SECS,
    )
    .await
    .ok()
    .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned());
    Ok(json!({
        "os": std::env::consts::OS,
        "architecture": std::env::consts::ARCH,
        "generator_source_sha256": crate::load::source_sha256(),
        "binary_sha256": binary_sha256,
        "commit": commit,
        "cache": concat!(
            "100 warmups by default after fixture process start; ",
            "OS page cache is not evicted"
        ),
        "storage": concat!(
            "disposable local filesystem/volume; ",
            "hardware storage type must be recorded externally"
        )
    }))
}

/// Records the host facts of the machine that ran the workload.
pub(super) async fn record_host_facts(report: &mut Value) {
    report["provenance"]["host_cpu_parallelism"] =
        json!(std::thread::available_parallelism().ok().map(usize::from));
    report["provenance"]["host_uname"] = json!(
        runtime::output(Command::new("uname").arg("-a"), PROBE_TIMEOUT_SECS)
            .await
            .ok()
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
    );
    report["provenance"]["host_physical_memory_bytes"] = json!(
        runtime::output(
            Command::new("sysctl").args(["-n", "hw.memsize"]),
            PROBE_TIMEOUT_SECS
        )
        .await
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
    );
    report["provenance"]["duckdb_driver_lock_version"] =
        json!("1.20000.1 (bundled engine version must be verified with the release build)");
}
