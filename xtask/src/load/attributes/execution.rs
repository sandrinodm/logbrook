//! Profile execution, resource evidence, and unconditional fixture cleanup.
use super::{
    Args,
    constants::{DEDICATED_TAIL_RATE, HOST_CONFIG_FORMAT},
    data::Seed,
    fixture::{Prepared, prepare},
    queries::queries,
};
use crate::{
    Result,
    load::http::Http,
    runtime::{self, Fixture},
};
use serde_json::{Value, json};

/// Runs one profile against a fresh fixture and returns its run report.
pub(super) async fn one(args: &Args, profile: &str, comparison: bool, from: i64) -> Result<Value> {
    let Prepared { mut fixture, seed } = prepare(args, from).await?;
    let work = profile_report(&fixture, args, &seed, profile, comparison).await;
    let cleanup = fixture.shutdown().await;
    finish_run(work, cleanup)
}

/// Measures one profile on a started fixture and assembles the run report around it.
async fn profile_report(
    fixture: &Fixture,
    args: &Args,
    seed: &Seed,
    profile: &str,
    comparison: bool,
) -> Result<Value> {
    let http = Http::new(&fixture.base, &fixture.read_token, &fixture.ingest_token)?;
    let before = http.request("/metrics", None, true).await;

    let result = measure(fixture, args, seed, profile, comparison).await?;
    let after = http.request("/metrics", None, true).await;

    let resources = observe_resources(fixture).await;
    Ok(json!({
        "profile": profile,
        "comparison": comparison,
        "seed": seed.report,
        "result": result,
        "resources": resources,
        "metrics_before": {"status": before.0, "text": before.1},
        "metrics_after": {"status": after.0, "text": after.1}
    }))
}

/// Dispatches to the query cells or to the live workload for `profile`.
async fn measure(
    fixture: &Fixture,
    args: &Args,
    seed: &Seed,
    profile: &str,
    comparison: bool,
) -> Result<Value> {
    if profile == "queries" {
        return queries(fixture, args, seed).await;
    }

    let mut profile_args = args.clone();
    profile_args.profile = profile.into();

    let tail_rate = if profile == "tail" {
        DEDICATED_TAIL_RATE
    } else {
        args.rate
    };
    crate::load::attributes_live::run_live(fixture, &profile_args, seed, comparison, tail_rate)
        .await
}

/// Collects the container's resource evidence, or the reason it is unavailable.
///
/// Observer errors become a `collection_error` field so that completed samples are kept.
async fn observe_resources(fixture: &Fixture) -> Value {
    match collect_resources(fixture).await {
        Ok(resources) => resources,
        Err(error) => json!({"collection_error": error.to_string()}),
    }
}

/// Reads the container's inspect, stats and cgroup counters.
async fn collect_resources(fixture: &Fixture) -> Result<Value> {
    let Some(name) = &fixture.container else {
        return Ok(json!({
            "limits": concat!(
                "native process is not constrained by the fixture; ",
                "no reference qualification claimed"
            )
        }));
    };
    Ok(json!({
        "inspect": runtime::docker(&["inspect", "--format", HOST_CONFIG_FORMAT, name]).await?,
        "stats": runtime::docker(&["stats", "--no-stream", "--format", "{{json .}}", name]).await?,
        "cgroup": crate::container::probe(
            name,
            &["memory.peak".into(), "memory.events".into(), "cpu.stat".into()]
        )
        .await?
    }))
}

/// Combines a run's measurement with its cleanup result, keeping the measurement either way.
pub(super) fn finish_run(work: Result<Value>, cleanup: Result<()>) -> Result<Value> {
    match (work, cleanup) {
        (Ok(mut report), cleanup) => {
            report["resources_passed"] =
                json!(report["resources"].get("collection_error").is_none());
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
