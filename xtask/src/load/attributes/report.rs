//! Reference qualification gates and paired-run summaries.

use super::{
    Args,
    constants::{
        DEDICATED_TAIL_RATE, MIN_THROUGHPUT_RATIO, REFERENCE_ARCHIVE_PERCENT,
        REFERENCE_DURATION_SECONDS, REFERENCE_LIVE_WARMUP_SECONDS, REFERENCE_METADATA_BYTES,
        REFERENCE_PAIRED_REPETITIONS, REFERENCE_QUERY_RATE, REFERENCE_RATE, REFERENCE_ROWS,
        REFERENCE_RUNS, REFERENCE_SAMPLES, REFERENCE_WARMUP, REPORT_RUNS, TAIL_P95_LIMIT_MS,
    },
    provenance::{HardwareProvenance, container_budget_verified},
};
use crate::load::attributes_schedule as schedule;
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// Hardware provenance from the report, if it was requested and is valid.
fn verified_hardware(args: &Args, report: &Value) -> Option<HardwareProvenance> {
    args.hardware_provenance.as_ref().and_then(|_| {
        report["provenance"]
            .get("hardware")
            .cloned()
            .and_then(|value| serde_json::from_value::<HardwareProvenance>(value).ok())
            .and_then(|hardware| hardware.normalize().ok())
    })
}

/// Whether the recorded image inspection shows a real image identity.
fn image_identity_recorded(report: &Value) -> bool {
    report["provenance"]["image_inspect"]
        .as_str()
        .is_some_and(|text| {
            serde_json::from_str::<Value>(text)
                .ok()
                .is_some_and(|image| image["Id"].as_str().is_some_and(|id| !id.is_empty()))
        })
}

/// Whether the container image and the target budget were both verified.
fn container_verified(args: &Args, report: &Value, runs: &[Value]) -> bool {
    args.image.is_some()
        && image_identity_recorded(report)
        && !runs.is_empty()
        && runs.iter().all(container_budget_verified)
}

/// Whether every run had correct results.
fn correctness(runs: &[Value]) -> bool {
    !runs.is_empty()
        && runs.iter().all(|run| {
            if run["profile"] == "queries" {
                run["result"]["passed"] == true
            } else {
                run["result"]["correct"] == true
            }
        })
}

/// The runs of `profile`, in report order.
fn runs_of<'a>(runs: &'a [Value], profile: &str) -> Vec<&'a Value> {
    runs.iter()
        .filter(|run| run["profile"] == profile)
        .collect()
}

/// Whether every query run met the reference p95 limits, with complete samples.
fn query_latency_gate(args: &Args, runs: &[Value]) -> Option<bool> {
    let query_runs = runs_of(runs, "queries");
    (!query_runs.is_empty()).then(|| {
        query_runs.iter().all(|run| {
            run["result"]["reference_latency_met"] == true
                && run["result"]["cells"].as_array().is_some_and(|cells| {
                    cells.len() == schedule::CELLS
                        && cells.iter().all(|cell| {
                            cell["latency_ms"]["samples"]
                                .as_u64()
                                .is_some_and(|samples| samples > 0 && samples == args.samples)
                        })
                })
        })
    })
}

/// Whether every dedicated tail run delivered every offered event at the reference settings.
fn tail_delivery_gate(args: &Args, runs: &[Value]) -> Option<bool> {
    let tail_runs = runs_of(runs, "tail");
    (!tail_runs.is_empty()).then(|| {
        tail_runs.iter().all(|run| {
            crate::load::attributes_live::tail_delivery_complete(&run["result"])
                && run["result"]["offered_rate"].as_f64() == Some(DEDICATED_TAIL_RATE)
                && run["result"]["warmup_seconds"].as_f64() == Some(args.live_warmup_seconds)
                && run["result"]["measured_seconds"].as_f64() == Some(args.duration)
        })
    })
}

/// Whether every tail run meets the acknowledgement-to-observation p95 limit, or observed every
/// matching event before its acknowledgement.
fn tail_latency_gate(runs: &[Value]) -> Option<bool> {
    let tail_runs = runs_of(runs, "tail");
    (!tail_runs.is_empty()).then(|| {
        tail_runs.iter().all(|run| {
            let latency = &run["result"]["tail"]["durable_acknowledgement_to_observation_ms"];
            let within_limit = latency["samples"]
                .as_u64()
                .is_some_and(|samples| samples > 0)
                && latency["p95"]
                    .as_f64()
                    .is_some_and(|p95| (0.0..=TAIL_P95_LIMIT_MS).contains(&p95));
            let all_before_acknowledgement = latency["samples"] == 0
                && crate::load::attributes_live::tail_delivery_complete(&run["result"])
                && run["result"]["tail"]["observed_before_acknowledgement"]
                    .as_u64()
                    .zip(run["result"]["expected_measured_matching_offers"].as_u64())
                    .is_some_and(|(negative, expected)| expected > 0 && negative == expected);
            within_limit || all_before_acknowledgement
        })
    })
}

/// Committed throughput of the baseline and comparison builds in one repetition.
#[derive(Default)]
struct Throughput {
    baseline: Option<f64>,
    comparison: Option<f64>,
}

/// Paired throughput across repetitions: per-repetition details, the median ratio, and how many
/// repetitions produced a ratio.
struct PairedSummary {
    details: Vec<Value>,
    median: Option<f64>,
    ratio_count: usize,
}

/// Pairs the baseline and comparison runs by repetition and computes the median ratio.
fn paired_summary(runs: &[Value]) -> PairedSummary {
    let mut paired: BTreeMap<u64, Throughput> = BTreeMap::new();
    for run in runs.iter().filter(|run| run["profile"] == "paired") {
        let entry = paired
            .entry(run["repetition"].as_u64().unwrap_or(0))
            .or_default();
        if run["comparison"] == true {
            entry.comparison = run["result"]["committed_per_second"].as_f64();
        } else {
            entry.baseline = run["result"]["committed_per_second"].as_f64();
        }
    }

    let mut ratios = Vec::new();
    let mut details = Vec::new();
    for (repetition, throughput) in paired {
        let ratio = throughput
            .baseline
            .filter(|value| *value > 0.0)
            .zip(throughput.comparison)
            .map(|(baseline, comparison)| comparison / baseline);
        if let Some(ratio) = ratio {
            ratios.push(ratio);
        }

        details.push(json!({
            "repetition": repetition,
            "baseline_committed_per_second": throughput.baseline,
            "comparison_committed_per_second": throughput.comparison,
            "throughput_ratio": ratio
        }));
    }

    ratios.sort_by(f64::total_cmp);
    PairedSummary {
        details,
        median: median(&ratios),
        ratio_count: ratios.len(),
    }
}

/// Median of sorted values; the mean of the two middle values for an even count.
fn median(sorted: &[f64]) -> Option<f64> {
    let middle = sorted.len() / 2;
    match sorted.len() {
        0 => None,
        len if len % 2 == 0 => Some((sorted[middle - 1] + sorted[middle]) / 2.0),
        _ => Some(sorted[middle]),
    }
}

/// Whether the run uses the reference query shape, so the query latency targets apply.
fn reference_query_shape(args: &Args) -> bool {
    args.rows == REFERENCE_ROWS
        && args.runs == REFERENCE_RUNS
        && args.metadata_bytes == REFERENCE_METADATA_BYTES
        && args.archive_percent == REFERENCE_ARCHIVE_PERCENT
        && args.image.is_some()
        && args.samples == REFERENCE_SAMPLES
        && args.warmup == REFERENCE_WARMUP
}

/// Whether the run uses the reference live shape, so the live targets apply.
fn reference_live_shape(args: &Args) -> bool {
    args.rows == REFERENCE_ROWS
        && args.image.is_some()
        && args.duration == REFERENCE_DURATION_SECONDS
        && args.live_warmup_seconds == REFERENCE_LIVE_WARMUP_SECONDS
}

/// Each reference dimension, true only when the run matches it.
fn reference_dimensions(
    args: &Args,
    container_verified: bool,
    hardware_verified: bool,
    quiet_host_verified: bool,
    paired: &PairedSummary,
) -> Value {
    json!({
        "one_million_rows": args.rows == REFERENCE_ROWS,
        "one_thousand_runs": args.runs == REFERENCE_RUNS,
        "one_kib_metadata": args.metadata_bytes == REFERENCE_METADATA_BYTES,
        "ninety_percent_archived": args.archive_percent == REFERENCE_ARCHIVE_PERCENT,
        "one_thousand_samples_each_cell": args.samples == REFERENCE_SAMPLES,
        "one_hundred_query_warmups": args.warmup == REFERENCE_WARMUP,
        "four_cpu_one_gib_container": container_verified,
        "all_profiles": args.profile == "all",
        "ten_minute_live_measurements": args.duration == REFERENCE_DURATION_SECONDS,
        "one_minute_live_warmup": args.live_warmup_seconds == REFERENCE_LIVE_WARMUP_SECONDS,
        "one_thousand_offered_events_per_second": args.rate == REFERENCE_RATE,
        "two_queries_per_second": args.query_rate == REFERENCE_QUERY_RATE,
        "three_complete_paired_repetitions": paired.details.len() == REFERENCE_PAIRED_REPETITIONS
            && paired.ratio_count == REFERENCE_PAIRED_REPETITIONS,
        "hardware_and_local_ssd_provenance_verified": hardware_verified,
        "quiet_host_provenance_verified": quiet_host_verified
    })
}

/// Evaluates every gate over the persisted runs, records the summary in `report`, and returns
/// whether the workload passed.
///
/// Latency and throughput targets are qualification gates for the frozen reference. Reduced and
/// scale probes still fail on any correctness or resource error.
pub(super) fn outcome(args: &Args, report: &mut Value) -> bool {
    let runs = report["runs"].as_array().expect(REPORT_RUNS);
    let hardware = verified_hardware(args, report);
    let hardware_verified = hardware.as_ref().is_some_and(|hardware| hardware.local_ssd);
    let quiet_host_verified = hardware
        .as_ref()
        .is_some_and(|hardware| hardware.quiet_host);
    let container_verified = container_verified(args, report, runs);
    let resources_and_cleanup = !runs.is_empty()
        && runs
            .iter()
            .all(|run| run["resources_passed"] == true && run["cleanup_passed"] == true);
    let correctness = correctness(runs);
    let query_latency = query_latency_gate(args, runs);
    let tail_delivery = tail_delivery_gate(args, runs);
    let tail_latency = tail_latency_gate(runs);
    let paired_query_load = runs_of(runs, "paired")
        .into_iter()
        .all(|run| crate::load::attributes_live::normal_query_load_achieved(&run["result"]));
    let paired = paired_summary(runs);
    let median = paired.median;
    let throughput = median.map(|ratio| ratio >= MIN_THROUGHPUT_RATIO);
    let dimensions = reference_dimensions(
        args,
        container_verified,
        hardware_verified,
        quiet_host_verified,
        &paired,
    );
    let dimensions_met = dimensions
        .as_object()
        .is_some_and(|map| map.values().all(|value| value == true));

    let reference_query_dimensions = reference_query_shape(args);
    let reference_live_dimensions = reference_live_shape(args);
    let query_gate = !reference_query_dimensions || query_latency != Some(false);
    let live_gate = !reference_live_dimensions
        || (tail_latency != Some(false)
            && tail_delivery != Some(false)
            && paired_query_load
            && throughput != Some(false));
    let passed = report.get("fatal_error").is_none()
        && correctness
        && resources_and_cleanup
        && tail_delivery != Some(false)
        && paired_query_load
        && query_gate
        && live_gate;
    let reference_qualified = dimensions_met
        && passed
        && query_latency == Some(true)
        && tail_latency == Some(true)
        && tail_delivery == Some(true)
        && paired_query_load
        && throughput == Some(true);

    report["paired_summary"] = json!({
        "repetitions": paired.details,
        "median_throughput_ratio": median,
        "minimum_reference_ratio": MIN_THROUGHPUT_RATIO
    });

    report["reference_dimensions"] = dimensions;
    report["reference_dimensions_met"] = json!(dimensions_met);
    report["gates"] = json!({
        "correctness_and_accounting": correctness,
        "resource_collection_and_cleanup": resources_and_cleanup,
        "query_p95_targets": query_latency,
        "tail_p95_target": tail_latency,
        "tail_full_delivery": tail_delivery,
        "normal_paired_query_load_achieved": paired_query_load,
        "paired_median_throughput_target": throughput,
        "hardware_storage_provenance": hardware_verified,
        "quiet_host_provenance": quiet_host_verified
    });
    report["reference_qualified"] = json!(reference_qualified);
    report["passed"] = json!(passed);
    passed
}
