//! Same physical fixture, sequential release-image query comparison.
use super::{
    attributes::{
        self, CONTAINER_CPUS, CONTAINER_MEMORY_BYTES, CONTAINER_MEMORY_MIB, CONTAINER_NANO_CPUS,
    },
    attributes_schedule::{self as schedule, OPERATIONS},
    http::{self, Http},
    semantics::{DAY_MS, HTTP_OK},
};
use crate::{
    Result,
    runtime::{self, Fixture, ResourceLimits},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::{self, Write},
    path::{Path, PathBuf},
    time::Instant,
};

/// Order of the process blocks: the baseline and the candidate each run twice, in an ABBA
/// arrangement, so that drift over time affects both images equally.
const ORDER: [&str; 4] = ["baseline", "candidate", "candidate", "baseline"];

/// The two images compared, in report order.
const VARIANTS: [&str; 2] = ["baseline", "candidate"];

/// Default `--warmup`, which is smaller than the reference's because each block is a process.
const DEFAULT_WARMUP: u64 = 20;

/// Default `--samples` per cell.
const DEFAULT_SAMPLES: u64 = 50;

/// Length of an image ID: the `sha256:` prefix and 64 hex digits.
const IMAGE_ID_LEN: usize = 71;

/// Timeout of copying the binary out of the container, in seconds.
const COPY_TIMEOUT_SECS: u64 = 120;

/// Largest error text kept per request sample, in characters.
const SAMPLE_ERROR_CHARS: usize = 1000;

/// Retention of the canonical fixture; data older than this has been removed, so the fixture
/// cannot be compared.
const RETENTION_MS: i64 = 604_800_000;

/// Margin before the one-day window that the canonical fixture starts at, in milliseconds.
const CANONICAL_LEAD_MS: i64 = 60_000;

/// Message of an absent container, which the compare path always requires.
const MISSING_CONTAINER: &str = "missing container";

// Options of the same-fixture query comparison.
#[derive(clap::Args, Clone, Debug, serde::Serialize)]
pub struct Args {
    // Image of the baseline build.
    #[arg(long, default_value = "logbrook:attributes-local")]
    baseline_image: String,
    // Image of the candidate build.
    #[arg(long, default_value = "logbrook:attributes-optimized")]
    candidate_image: String,
    // Rows in the fixture.
    #[arg(long, default_value_t = attributes::REFERENCE_ROWS)]
    rows: u64,
    // Runs in the fixture.
    #[arg(long, default_value_t = attributes::REFERENCE_RUNS)]
    runs: u64,
    // Warmup requests per cell.
    #[arg(long, default_value_t = DEFAULT_WARMUP)]
    warmup: u64,
    // Measured requests per cell.
    #[arg(long, default_value_t = DEFAULT_SAMPLES)]
    samples: u64,
    /// Collect optional whole-container CPU deltas around ten-request cell batches.
    #[arg(long)]
    cpu_diagnostics: bool,
    /// Promote a top-level string key in the canonical fixture (repeatable).
    #[arg(long = "promoted-string-key")]
    promoted_string_keys: Vec<String>,
    #[arg(long)]
    hardware_provenance: Option<PathBuf>,
    // Report path.
    #[arg(long, default_value = "artifacts/attribute-compare.json")]
    output: PathBuf,
}

/// The attribute workload settings that a comparison run uses for one image.
fn workload(args: &Args, image: String) -> attributes::Args {
    attributes::Args {
        binary: PathBuf::from("unused"),
        image: Some(image),
        hardware_provenance: None,
        rows: args.rows,
        runs: args.runs,
        samples: args.samples,
        warmup: args.warmup,
        cpu_diagnostics: args.cpu_diagnostics,
        promoted_string_keys: args.promoted_string_keys.clone(),
        metadata_bytes: attributes::REFERENCE_METADATA_BYTES,
        archive_percent: attributes::REFERENCE_ARCHIVE_PERCENT,
        profile: "queries".into(),
        duration: attributes::REFERENCE_DURATION_SECONDS,
        rate: attributes::REFERENCE_RATE,
        query_rate: attributes::REFERENCE_QUERY_RATE,
        live_warmup_seconds: attributes::REFERENCE_LIVE_WARMUP_SECONDS,
        paired_repetitions: attributes::REFERENCE_PAIRED_REPETITIONS,
        output: args.output.clone(),
    }
}

/// Inspects an image and checks that its ID is an immutable `sha256:` identity.
async fn image_identity(reference: &str) -> Result<Value> {
    let inspected: Value = serde_json::from_str(
        &runtime::docker(&["image", "inspect", "--format", "{{json .}}", reference]).await?,
    )?;
    let id = inspected["Id"]
        .as_str()
        .ok_or("image inspection omitted ID")?;
    if !id.starts_with("sha256:") || id.len() != IMAGE_ID_LEN {
        return Err("image ID is not an immutable sha256 identity".into());
    }

    Ok(json!({"reference": reference, "id": id, "inspect": inspected}))
}

/// Hash of the logbrook binary inside the started container.
async fn binary_hash(fixture: &Fixture) -> Result<String> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("logbrook");
    let container = fixture.container.as_deref().ok_or(MISSING_CONTAINER)?;
    runtime::docker_with_timeout(
        &[
            "cp",
            &format!("{container}:/usr/local/bin/logbrook"),
            path.to_str().ok_or("non UTF-8 binary copy path")?,
        ],
        COPY_TIMEOUT_SECS,
    )
    .await?;

    super::file_sha256(&path)
}

/// Which request population a sample belongs to.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Warmup,
    Measured,
}

impl Phase {
    /// Whether a recorded sample is a warmup request.
    fn is_warmup(self) -> bool {
        matches!(self, Self::Warmup)
    }
}

/// Nearest-rank fraction of the median latency that the comparison reports.
const P50: f64 = 0.50;

/// Nearest-rank fraction of the 95th-percentile latency that the comparison reports.
const P95: f64 = 0.95;

/// Nearest-rank fraction of the 99th-percentile latency that the comparison reports.
const P99: f64 = 0.99;

/// Exact nearest-rank latency summary of the samples in one phase.
fn latency(raw: &[Value], phase: Phase) -> Value {
    let mut values: Vec<_> = raw
        .iter()
        .filter(|sample| sample["warmup"] == phase.is_warmup())
        .map(|sample| sample["elapsed_ms"].as_f64().expect("recorded latency"))
        .collect();
    values.sort_unstable_by(f64::total_cmp);

    let percentile = |p: f64| {
        values
            .get(((values.len() as f64 * p).ceil() as usize).saturating_sub(1))
            .copied()
    };
    json!({
        "count": values.len(),
        "method": "exact nearest rank; includes failed requests",
        "min": values.first(),
        "p50": percentile(P50),
        "p95": percentile(P95),
        "p99": percentile(P99),
        "max": values.last()
    })
}

/// Summary of one cell: latency, statuses, oracle failures and the reference p95 verdict.
fn cell_report(selection: &str, operation: &str, raw: Vec<Value>) -> Value {
    let mut warmup_statuses = BTreeMap::new();
    let mut measured_statuses = BTreeMap::new();
    let mut failures = 0_u64;
    for sample in &raw {
        let statuses = if sample["warmup"] == true {
            &mut warmup_statuses
        } else {
            &mut measured_statuses
        };
        *statuses
            .entry(sample["status"].as_u64().expect("recorded status"))
            .or_insert(0_u64) += 1;
        failures += u64::from(sample["oracle_passed"] != true);
    }

    let measured = latency(&raw, Phase::Measured);
    let limit = attributes::reference_p95_limit_ms(operation);
    json!({
        "selection": selection,
        "operation": operation,
        "warmup_latency_ms": latency(&raw, Phase::Warmup),
        "latency_ms": measured,
        "coverage": {
            "warmup": schedule::coverage(&raw, true),
            "measured": schedule::coverage(&raw, false)
        },
        "warmup_statuses": warmup_statuses,
        "measured_statuses": measured_statuses,
        "correctness_failures": failures,
        "reference_p95_limit_ms": limit,
        "reference_latency_met": measured["p95"].as_f64().is_some_and(|p95| p95 <= limit),
        "passed": failures == 0,
        "raw": raw
    })
}

/// Sends one comparison request and returns its sample record.
async fn send_query(
    http: &Http,
    args: &Args,
    seed: &attributes::Seed,
    batch: &schedule::Batch,
    offset: u64,
) -> Value {
    let (selection, operation) = schedule::cell(batch.cell);
    let ordinal = batch.start + offset;
    let (run, actor) = schedule::query(ordinal, args.runs, selection == "run_actor");
    let path = attributes::request_path(seed, operation, run, actor);

    let started = Instant::now();
    let (status, payload) = http.request(&path, None, false).await;
    let elapsed_ms = attributes::elapsed_ms(started);
    let checked = if status == HTTP_OK {
        attributes::verify_response(seed, operation, run, actor, &payload)
    } else {
        Err(attributes::http_error(status, &payload).into())
    };
    json!({
        "iteration": ordinal,
        "warmup": batch.warmup,
        "batch_id": batch.id,
        "schedule_position": batch.position + offset,
        "run": run,
        "actor": actor,
        "status": status,
        "elapsed_ms": elapsed_ms,
        "oracle_passed": checked.is_ok(),
        "error": attributes::error_text(&checked, SAMPLE_ERROR_CHARS)
    })
}

/// Runs the eight cells in their rotated batch order and summarizes each one.
async fn queries(fixture: &Fixture, args: &Args, seed: &attributes::Seed) -> Result<Value> {
    let http = Http::new(&fixture.base, &fixture.read_token, &fixture.ingest_token)?;
    let mut raw: Vec<Vec<Value>> = vec![Vec::new(); schedule::CELLS];
    let mut cpu: Vec<Vec<Value>> = vec![Vec::new(); schedule::CELLS];
    for batch in schedule::batches(args.warmup, args.samples) {
        let before = schedule::observe_cpu(fixture, args.cpu_diagnostics).await;
        for offset in 0..batch.len {
            raw[batch.cell].push(send_query(&http, args, seed, &batch, offset).await);
        }

        let after = schedule::observe_cpu(fixture, args.cpu_diagnostics).await;
        cpu[batch.cell].push(schedule::cpu_batch(batch, before, after));
    }

    let mut cells = Vec::new();
    for (index, raw) in raw.into_iter().enumerate() {
        let (selection, operation) = schedule::cell(index);
        let mut cell = cell_report(selection, operation, raw);
        cell["cpu"] = schedule::cpu_report(std::mem::take(&mut cpu[index]), args.cpu_diagnostics);
        let _ = writeln!(
            io::stderr(),
            "attribute-compare selection={selection} operation={operation} p95_ms={} failures={}",
            cell["latency_ms"]["p95"],
            cell["correctness_failures"]
        );
        cells.push(cell);
    }

    Ok(json!({
        "schedule": schedule::metadata(args.runs),
        "passed": cells.iter().all(|cell| cell["passed"] == true),
        "reference_latency_met": cells.iter().all(|cell| cell["reference_latency_met"] == true),
        "cells": cells
    }))
}

/// Stops the fixture, verifies the projection status offline, and starts it again. Used only
/// when the comparison promotes string keys.
async fn verified_projection_status(
    fixture: &mut Fixture,
    args: &Args,
    image: &str,
) -> Result<Value> {
    fixture.stop().await?;
    let status: Value = serde_json::from_str(
        &fixture
            .offline_command(&["projection-status"], attributes::OFFLINE_COMMAND_SECS)
            .await?,
    )?;
    attributes::verify_projection_status(&workload(args, image.into()), &status, args.rows)?;
    fixture.start_again().await?;
    Ok(status)
}

/// Collects the container's state, inspect, stats and cgroup counters for one block.
///
/// Observer errors become `collection_error` so that completed request samples are kept.
async fn observe_block(name: &str, host_config: &Value) -> Value {
    match collect_block(name, host_config).await {
        Ok(resources) => resources,
        Err(error) => json!({"collection_error": error.to_string()}),
    }
}

/// Reads the state, inspect, stats and cgroup counters of the block's container.
async fn collect_block(name: &str, host_config: &Value) -> Result<Value> {
    let state: Value = serde_json::from_str(
        &runtime::docker(&["inspect", "--format", "{{json .State}}", name]).await?,
    )?;
    Ok(json!({
        "state": state,
        "inspect": host_config,
        "stats": runtime::docker(&["stats", "--no-stream", "--format", "{{json .}}", name]).await?,
        "cgroup": crate::container::probe(
            name,
            &["memory.peak".into(), "memory.events".into(), "cpu.stat".into()]
        )
        .await?
    }))
}

/// Whether a block's container ran without restarts, OOM kills, or queued readers, and the
/// server answered its metrics.
fn block_resources_passed(
    resources: &Value,
    metrics_status: u16,
    metrics: &BTreeMap<String, f64>,
) -> bool {
    let memory_events: BTreeMap<_, _> = resources["cgroup"]["memory.events"]
        .as_str()
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.split_once(' '))
        .collect();
    resources["state"]["Running"] == true
        && resources["state"]["OOMKilled"] == false
        && metrics_status == HTTP_OK
        && metrics.get("logbrook_storage_readers_busy") == Some(&0.0)
        && metrics.get("logbrook_storage_reader_queue_used") == Some(&0.0)
        && memory_events.get("oom") == Some(&"0")
        && memory_events.get("oom_kill") == Some(&"0")
}

/// Runs one process block: starts the image on the snapshot, measures the queries, and records
/// the container's resources.
async fn block(
    args: &Args,
    seed: &attributes::Seed,
    snapshot: &Path,
    image: &str,
    expected_binary: &mut Option<String>,
) -> Result<Value> {
    // A reused canonical fixture must never drift outside retention during a long run.
    if seed.from <= crate::now_ms() - RETENTION_MS {
        return Err("canonical fixture has crossed retention; comparison cannot continue".into());
    }

    let mut fixture = Fixture::start_config_from_snapshot(
        PathBuf::from("unused"),
        Some(image.into()),
        &workload(args, image.into()).server_config(),
        Some(ResourceLimits {
            cpus: CONTAINER_CPUS,
            memory_mib: CONTAINER_MEMORY_MIB,
        }),
        Some(snapshot),
    )
    .await?;
    let work = measure_block(&mut fixture, args, seed, image, expected_binary).await;
    let cleanup = fixture.shutdown().await;
    finish_block(work, cleanup)
}

/// The measured part of a process block, after the fixture has started.
async fn measure_block(
    fixture: &mut Fixture,
    args: &Args,
    seed: &attributes::Seed,
    image: &str,
    expected_binary: &mut Option<String>,
) -> Result<Value> {
    let projection_status = if args.promoted_string_keys.is_empty() {
        attributes::unrequested_projection_status()
    } else {
        verified_projection_status(fixture, args, image).await?
    };

    let hash = binary_hash(fixture).await?;
    if expected_binary
        .as_ref()
        .is_some_and(|expected| expected != &hash)
    {
        return Err("binary identity changed between process blocks".into());
    }

    *expected_binary = Some(hash.clone());
    let name = fixture.container.as_deref().ok_or(MISSING_CONTAINER)?;
    let host_config: Value = serde_json::from_str(
        &runtime::docker(&["inspect", "--format", attributes::HOST_CONFIG_FORMAT, name]).await?,
    )?;
    if host_config["NanoCpus"] != CONTAINER_NANO_CPUS
        || host_config["Memory"] != CONTAINER_MEMORY_BYTES
        || host_config["MemorySwap"] != CONTAINER_MEMORY_BYTES
    {
        return Err("container CPU/memory limits differ from comparison budget".into());
    }

    let http = Http::new(&fixture.base, &fixture.read_token, &fixture.ingest_token)?;
    let before = http.request("/metrics", None, true).await;
    let result = queries(fixture, args, seed).await?;
    let after = http.request("/metrics", None, true).await;

    let resources = observe_block(name, &host_config).await;
    let metrics = http::metrics(&after.1);
    let resources_passed = block_resources_passed(&resources, after.0, &metrics);
    Ok(json!({
        "resources_passed": resources_passed,
        "image_id": image,
        "binary_sha256": hash,
        "restored_manifest_verified_before_start": true,
        "projection_status": projection_status,
        "result": result,
        "resources": resources,
        "metrics_before": {"status": before.0, "text": before.1},
        "metrics_after": {"status": after.0, "text": after.1}
    }))
}

/// Combines a block's result with its cleanup result, keeping the result either way.
fn finish_block(work: Result<Value>, cleanup: Result<()>) -> Result<Value> {
    match (work, cleanup) {
        (Ok(mut report), cleanup) => {
            report["cleanup_passed"] = json!(cleanup.is_ok());
            if let Err(error) = cleanup {
                report["cleanup_error"] = json!(error.to_string());
            }
            Ok(report)
        }
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup)) => Err(format!("{error}; cleanup: {cleanup}").into()),
    }
}

/// Pools the samples of each cell over both variants' process blocks.
fn summarize(blocks: &[Value]) -> Value {
    let mut summary = Vec::new();
    for selection in ["run", "run_actor"] {
        for operation in OPERATIONS {
            let mut variants = BTreeMap::new();
            for variant in VARIANTS {
                let raw: Vec<Value> = blocks
                    .iter()
                    .filter(|block| block["variant"] == variant)
                    .flat_map(|block| block["result"]["cells"].as_array().into_iter().flatten())
                    .filter(|cell| cell["selection"] == selection && cell["operation"] == operation)
                    .flat_map(|cell| cell["raw"].as_array().into_iter().flatten().cloned())
                    .collect();
                variants.insert(variant, cell_report(selection, operation, raw));
            }

            let baseline = variants["baseline"]["latency_ms"]["p50"].as_f64();
            let candidate = variants["candidate"]["latency_ms"]["p50"].as_f64();
            summary.push(json!({
                "selection": selection,
                "operation": operation,
                "variants": variants,
                "candidate_over_baseline_p50": baseline
                    .zip(candidate)
                    .filter(|(b, _)| *b > 0.0)
                    .map(|(b, c)| c / b)
            }));
        }
    }

    // Raw samples live once, in process blocks; pooled summaries retain all failures.
    for cell in &mut summary {
        for variant in VARIANTS {
            cell["variants"][variant]
                .as_object_mut()
                .expect("cell object")
                .remove("raw");
        }
    }

    json!(summary)
}

/// Runs the comparison end to end and writes its report.
///
/// # Errors
///
/// Returns an error when the images are not immutable identities, the workload arguments are
/// out of bounds, a block fails, the report cannot be written, or any correctness, resource or
/// cleanup check fails. The report is written in each case.
pub async fn run(args: Args) -> Result<()> {
    workload(&args, args.baseline_image.clone()).validate()?;

    let baseline = image_identity(&args.baseline_image).await?;
    let candidate = image_identity(&args.candidate_image).await?;
    // The candidate creates the snapshot: an old baseline must explicitly prove
    // it can read the candidate's schema before any timed comparison is reported.
    let workload = workload(&args, candidate["id"].as_str().expect("image ID").into());
    workload.validate()?;

    let hardware = args
        .hardware_provenance
        .as_deref()
        .map(attributes::HardwareProvenance::read)
        .transpose()?;

    let mut report = json!({
        "scenario": "attribute-same-fixture-abba-v2",
        "settings": args,
        "order": ORDER,
        "images": {"baseline": baseline, "candidate": candidate},
        "hardware": hardware,
        "server_config": workload.server_config(),
        "generator_source_sha256": super::source_sha256(),
        "comparison_source_sha256": crate::hex_digest(&Sha256::digest(
            include_bytes!("attributes_compare.rs")
        )),
        "driver_binary_sha256": super::file_sha256(&std::env::current_exe()?)?,
        "cache": "Independent process warmups; OS/Docker cache not evicted. All blocks sequential.",
        "reference_qualified": false,
        "qualification_note": concat!(
            "Query comparison only; reduced pilot or even full query samples cannot qualify tail, ",
            "paired ingestion, host isolation, or full reference gates."
        ),
        "blocks": [],
        "complete": false
    });
    super::write_report(&args.output, &report)?;

    let work = run_blocks(&args, &workload, &mut report).await;
    let passed = record_outcome(&args, &work, &mut report)?;
    work?;

    if !passed {
        return Err(concat!(
            "attribute comparison failed correctness, resource, or cleanup checks; ",
            "see persisted raw samples"
        )
        .into());
    }
    Ok(())
}

/// Creates the canonical fixture and its snapshot, then runs the four process blocks in order.
async fn run_blocks(args: &Args, workload: &attributes::Args, report: &mut Value) -> Result<()> {
    let from = crate::now_ms() - DAY_MS - CANONICAL_LEAD_MS;
    let attributes::Prepared {
        fixture: mut canonical,
        seed,
    } = attributes::prepare_stopped(workload, from).await?;
    let snapshot = tempfile::NamedTempFile::new()?;
    canonical.snapshot(snapshot.path()).await?;

    report["fixture"] = seed.report.clone();
    report["fixture"]["ndjson_sha256"] = json!(super::file_sha256(
        &canonical.root.path().join("attributes.ndjson")
    )?);
    report["fixture"]["snapshot_tar_sha256"] = json!(super::file_sha256(snapshot.path())?);
    report["fixture"]["snapshot_manifest"] = json!(runtime::snapshot_manifest(snapshot.path())?);
    report["fixture"]["canonical_binary_sha256"] = json!(binary_hash(&canonical).await?);
    canonical.shutdown().await?;

    // Release the generation file and initial volume before timed process blocks.
    drop(canonical);

    super::write_report(&args.output, report)?;

    let mut hashes = BTreeMap::from([("baseline", None), ("candidate", None)]);
    for (position, variant) in ORDER.iter().enumerate() {
        let image = report["images"][variant]["id"]
            .as_str()
            .expect("image ID")
            .to_owned();
        let _ = writeln!(
            io::stderr(),
            "attribute-compare block={} variant={variant} image={image}",
            position + 1
        );
        let mut result = block(
            args,
            &seed,
            snapshot.path(),
            &image,
            hashes.get_mut(variant).expect("variant"),
        )
        .await?;
        result["position"] = json!(position);
        result["variant"] = json!(variant);
        report["blocks"]
            .as_array_mut()
            .expect("blocks")
            .push(result);

        super::write_report(&args.output, report)?;
    }

    report["complete"] = json!(true);
    Ok(())
}

/// Records the schema compatibility verdict and the overall pass, writes the report, and returns
/// the pass.
///
/// # Errors
///
/// Returns an error when the report cannot be written.
fn record_outcome(args: &Args, work: &Result<()>, report: &mut Value) -> Result<bool> {
    if let Err(error) = work {
        report["fatal_error"] = json!(error.to_string());
        report["schema_layout_compatibility"] = json!({
            "passed": false,
            "snapshot_created_by": "candidate",
            "configured_keys": args.promoted_string_keys,
            "error": error.to_string(),
            "interpretation": concat!(
                "A v3 executable cannot restore a v4 snapshot. No comparison is valid until both ",
                "executables support the same durable schema and layout."
            )
        });
    } else {
        report["schema_layout_compatibility"] = json!({
            "passed": true,
            "snapshot_created_by": "candidate",
            "configured_keys": args.promoted_string_keys,
            "all_blocks_restored_same_snapshot": true
        });
    }

    let blocks = report["blocks"].as_array().expect("blocks");
    let passed = work.is_ok()
        && blocks.len() == ORDER.len()
        && blocks.iter().all(|block| {
            block["result"]["passed"] == true
                && block["resources_passed"] == true
                && block["cleanup_passed"] == true
        });
    report["summary"] = summarize(blocks);

    report["passed"] = json!(passed);
    super::write_report(&args.output, report)?;
    Ok(passed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_failure_retains_the_completed_block_and_error() -> Result<()> {
        let report = json!({"result":{"cells":[{"raw":[{"status":504,"elapsed_ms":5000.0}]}]}});
        let finished = finish_block(Ok(report.clone()), Err("synthetic cleanup failure".into()))?;
        assert_eq!(finished["result"], report["result"]);
        assert_eq!(finished["cleanup_passed"], false);
        assert_eq!(finished["cleanup_error"], "synthetic cleanup failure");
        Ok(())
    }

    #[test]
    fn abba_keeps_failures_in_measured_population_and_warmups_separate() {
        assert_eq!(ORDER, ["baseline", "candidate", "candidate", "baseline"]);

        let cell = cell_report(
            "run",
            "search",
            vec![
                json!({"warmup":true,"elapsed_ms":9000.0,"status":504,"oracle_passed":false}),
                json!({"warmup":false,"elapsed_ms":100.0,"status":200,"oracle_passed":true}),
                json!({"warmup":false,"elapsed_ms":5000.0,"status":504,"oracle_passed":false}),
            ],
        );
        assert_eq!(cell["latency_ms"]["count"], 2);
        assert_eq!(cell["latency_ms"]["p95"], 5000.0);
        assert_eq!(cell["warmup_latency_ms"]["count"], 1);
        assert_eq!(cell["correctness_failures"], 2);
        assert_eq!(cell["passed"], false);
        assert_eq!(cell["measured_statuses"]["504"], 1);
    }

    #[test]
    fn pooled_summary_retains_both_process_blocks_and_all_errors() {
        let make = |variant, ms, passed| {
            json!({"variant":variant,"result":{"cells":[cell_report("run","search",vec![json!({
                "warmup":false,
                "elapsed_ms":ms,
                "status":if passed{200}else{504},
                "oracle_passed":passed
            })])]}})
        };
        let summary = summarize(&[
            make("baseline", 100.0, true),
            make("candidate", 50.0, true),
            make("candidate", 5000.0, false),
            make("baseline", 200.0, true),
        ]);
        assert_eq!(
            summary[0]["variants"]["candidate"]["latency_ms"]["count"],
            2
        );
        assert_eq!(
            summary[0]["variants"]["candidate"]["correctness_failures"],
            1
        );
        assert_eq!(
            summary[0]["variants"]["candidate"]["latency_ms"]["p95"],
            5000.0
        );
        assert!(summary[0]["variants"]["candidate"].get("raw").is_none());
    }
}
