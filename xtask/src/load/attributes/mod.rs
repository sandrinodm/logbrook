//! Reproducible attribute filtering qualification on disposable data only.

use crate::{
    Result,
    load::semantics::{DAY_MS, MILLISECONDS_PER_SECOND},
    runtime,
};
use constants::{FIXTURE_LEAD_MS, FIXTURE_SEED};
use execution::one;
use provenance::{provenance, record_host_facts};
use report::outcome;
use serde_json::{Value, json};
use std::{path::Path, time::Instant};

mod constants;
mod data;
mod execution;
mod fixture;
mod options;
mod provenance;
mod queries;
mod report;

pub(super) use constants::{
    CONTAINER_CPUS, CONTAINER_MEMORY_BYTES, CONTAINER_MEMORY_MIB, CONTAINER_NANO_CPUS,
    HOST_CONFIG_FORMAT, OFFLINE_COMMAND_SECS, REFERENCE_ARCHIVE_PERCENT,
    REFERENCE_DURATION_SECONDS, REFERENCE_LIVE_WARMUP_SECONDS, REFERENCE_METADATA_BYTES,
    REFERENCE_PAIRED_REPETITIONS, REFERENCE_QUERY_RATE, REFERENCE_RATE, REFERENCE_ROWS,
    REFERENCE_RUNS, REPORT_RUNS, USERS_PER_RUN,
};
pub use constants::{SCENARIO, SERVER_CONFIG};
pub(super) use data::{Operation, len_u64};
pub use data::{
    Seed, make_event, query_params, selection, verify_response, verify_unfiltered_response,
};
pub(super) use fixture::{
    Prepared, prepare_stopped, unrequested_projection_status, verify_projection_status,
};
pub use options::Args;
pub(super) use provenance::HardwareProvenance;
pub(super) use queries::{error_text, operation_path, reference_p95_limit_ms, request_path};

#[cfg(test)]
mod tests;

/// The message for a response whose status is not the expected one, with its payload.
pub(in crate::load) fn http_error(status: u16, payload: &Value) -> String {
    format!("HTTP {status}: {payload}")
}

/// Milliseconds elapsed since `started`, as a float.
pub(in crate::load) fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * MILLISECONDS_PER_SECOND
}

/// Appends a finished run to the report and writes the report before anything else runs.
///
/// # Errors
///
/// Returns an error when the report cannot be written, or when the run's resource collection or
/// cleanup failed; the run is persisted first in both cases.
fn persist_run(report: &mut Value, run: Value, output: &Path) -> Result<()> {
    let lifecycle_passed = run["resources_passed"] == true && run["cleanup_passed"] == true;
    report["runs"].as_array_mut().expect(REPORT_RUNS).push(run);
    super::write_report(output, report)?;
    if !lifecycle_passed {
        return Err(concat!(
            "attribute workload resource collection or cleanup failed; ",
            "completed run persisted before stopping"
        )
        .into());
    }

    Ok(())
}

/// The profiles requested by `--profile`, in the order they run.
fn profiles(args: &Args) -> Vec<&str> {
    if args.profile == "all" {
        vec!["queries", "tail", "paired"]
    } else {
        vec![args.profile.as_str()]
    }
}

/// The order of the paired arms in one repetition, alternating so neither build always runs
/// first. Each entry is the `comparison` flag of the run.
fn arm_order(repetition: usize) -> [bool; 2] {
    if repetition.is_multiple_of(2) {
        [false, true]
    } else {
        [true, false]
    }
}

/// Runs every requested profile in order, persisting each run as soon as it completes.
async fn run_profiles(args: &Args, from: i64, report: &mut Value) -> Result<()> {
    for profile in profiles(args) {
        if profile == "paired" {
            for repetition in 0..args.paired_repetitions {
                for comparison in arm_order(repetition) {
                    let mut run = one(args, profile, comparison, from).await?;
                    run["repetition"] = json!(repetition);
                    persist_run(report, run, &args.output)?;
                }
            }
        } else {
            let run = one(args, profile, true, from).await?;
            persist_run(report, run, &args.output)?;
        }
    }

    Ok(())
}

/// Runs the attribute workload end to end and writes its report.
///
/// # Errors
///
/// Returns an error when the arguments are out of bounds, a profile fails, the report cannot be
/// written, or any correctness, accounting, resource or applicable reference gate fails. The
/// report is written in each case.
pub async fn run(args: Args) -> Result<()> {
    args.validate()?;

    let hardware = args
        .hardware_provenance
        .as_deref()
        .map(HardwareProvenance::read)
        .transpose()?;

    // Every repetition imports byte-identical historical events and timestamps.
    let from = crate::now_ms() - DAY_MS - FIXTURE_LEAD_MS;
    let mut report = json!({
        "scenario": SCENARIO,
        "settings": args,
        "fixture_seed": FIXTURE_SEED,
        "server_config": args.server_config(),
        "provenance": provenance(&args).await?,
        "runs": [],
        "reference_qualified": false
    });
    report["provenance"]["hardware"] = serde_json::to_value(hardware)?;

    if let Some(image) = &args.image {
        report["provenance"]["image_inspect"] =
            json!(runtime::docker(&["image", "inspect", "--format", "{{json .}}", image]).await?);
    }

    record_host_facts(&mut report).await;
    let work = run_profiles(&args, from, &mut report).await;
    if let Err(error) = &work {
        report["fatal_error"] = json!(error.to_string());
    }

    report["qualification_note"] = json!(concat!(
        "Results are measurements only. Full reference qualification additionally requires ",
        "the frozen hardware/storage budget, full query/tail/three paired profiles, every ",
        "correctness/resource check, latency targets, and median committed-throughput ratio ",
        ">=0.8. Scale or reduced settings do not qualify the reference."
    ));
    let passed = outcome(&args, &mut report);
    super::write_report(&args.output, &report)?;
    work?;
    if !passed {
        return Err(concat!(
            "attribute workload failed correctness, accounting, resource, or applicable ",
            "reference targets; see persisted report"
        )
        .into());
    }

    Ok(())
}
