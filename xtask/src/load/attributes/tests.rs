use super::{
    Args, SERVER_CONFIG,
    data::{make_event, selection, verify_response, verify_unfiltered_response, write_seed},
    execution::finish_run,
    fixture::{unrequested_projection_status, verify_projection_status},
    persist_run,
    provenance::HardwareProvenance,
    queries::request_path,
    report::outcome,
};
use crate::{Result, load::semantics::DAY_MS};
use clap::Parser;
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn args() -> Args {
    #[derive(Parser)]
    struct Defaults {
        #[command(flatten)]
        args: Args,
    }

    Defaults::parse_from(["attribute-load"]).args
}

#[test]
fn empty_projection_configuration_is_byte_identical_to_legacy_config() {
    let args = args();
    assert!(args.promoted_string_keys.is_empty());
    assert_eq!(args.server_config(), SERVER_CONFIG);

    let status = unrequested_projection_status();
    assert_eq!(status["status"], "not_requested");
    assert_eq!(status["durable_layout_verified"], false);
    assert_eq!(status["coverage_verified"], false);
    assert!(status.get("hot_covered_rows").is_none());
}

#[test]
fn promoted_fixture_configuration_preserves_reference_budgets_and_requires_coverage() {
    let mut args = args();
    args.promoted_string_keys = vec!["runId".into(), "userId".into()];
    args.validate().unwrap();

    let config = args.server_config();
    assert!(config.starts_with(SERVER_CONFIG));
    assert!(config.contains("promoted_string_keys=[\"runId\",\"userId\"]"));

    let status = json!({"schema_version":4, "promoted_string_keys":["runId", "userId"],
        "hot_rows":10, "hot_covered_rows":10, "archived_rows":90, "archived_covered_rows":90,
        "legacy_archive_files":0});
    verify_projection_status(&args, &status, 100).unwrap();

    for (key, value) in [
        ("hot_covered_rows", json!(0)),
        ("archived_covered_rows", json!(0)),
        ("schema_version", json!(3)),
        ("legacy_archive_files", json!(1)),
        ("promoted_string_keys", json!([])),
    ] {
        let mut invalid = status.clone();
        invalid[key] = value;
        assert!(
            verify_projection_status(&args, &invalid, 100).is_err(),
            "{key}"
        );
    }

    args.promoted_string_keys.push("runId".into());
    assert!(args.validate().is_err());
}

#[test]
fn absent_user_facets_require_all_empty_dimensions() {
    let mut args = args();
    args.rows = 10;
    args.runs = 10;
    let root = tempfile::tempdir().unwrap();
    let seed = write_seed(&args, root.path(), 0).unwrap();

    let empty = json!({"services":[],"loggers":[],"hosts":[],"levels":[],"sources":[]});
    assert!(verify_response(&seed, "facets", 0, Some(9), &empty).is_ok());

    let mut wrong = empty;
    wrong["services"] = json!([{"value":"api","count":1}]);
    assert!(verify_response(&seed, "facets", 0, Some(9), &wrong).is_err());
    assert!(verify_response(&seed, "facets", 0, Some(9), &json!({})).is_err());
}

#[test]
fn reference_fixture_has_ten_percent_errors_for_every_user() {
    let args = args();
    let mut users: BTreeMap<String, (u64, u64, std::collections::BTreeSet<String>)> =
        BTreeMap::new();
    for ordinal in 0..1000 {
        let event = make_event(&args, ordinal * 1000 + 17, 0);
        assert_eq!(event["runId"], "run-0017");
        let entry = users
            .entry(event["userId"].as_str().unwrap().into())
            .or_default();
        entry.0 += 1;
        if event["level"] == 50 {
            entry.1 += 1;
        }
        entry.2.insert(event["sessionId"].as_str().unwrap().into());
    }

    assert_eq!(users.len(), 10);

    for (total, errors, sessions) in users.values() {
        assert_eq!((*total, *errors, sessions.len()), (100, 10, 10));
    }
}

#[test]
fn oracle_and_transport_use_exact_fixture_values() {
    let mut args = args();
    args.rows = 1000;
    args.runs = 10;
    let root = tempfile::tempdir().unwrap();
    let seed = write_seed(&args, root.path(), crate::now_ms() - DAY_MS - 1000).unwrap();
    assert_eq!(seed.report["metadata_bytes"]["min"], 1024);
    assert_eq!(seed.report["metadata_bytes"]["max"], 1024);
    assert!(verify_response(&seed, "count", 0, None, &json!({"count":100})).is_ok());
    assert!(verify_response(&seed, "count", 0, Some(0), &json!({"count":10})).is_ok());
    assert!(verify_response(&seed, "count", 0, None, &json!({"count":99})).is_err());
    assert!(verify_unfiltered_response(&seed, "count", &json!({"count":1000})).is_ok());

    let path = request_path(&seed, "search", 0, Some(0));
    let url = reqwest::Url::parse(&format!("http://localhost{path}")).unwrap();
    let params: BTreeMap<_, _> = url
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    assert_eq!(params["filter"], selection(0, Some(0)));
    assert_eq!(params["source"], "attributes");
}

#[test]
fn report_preserves_failed_repetitions_and_computes_median_ratio() {
    let mut args = args();
    args.profile = "paired".into();
    let mut runs = Vec::new();
    for (repetition, ratio) in [0.7, 0.9, 1.1].into_iter().enumerate() {
        for comparison in [false, true] {
            runs.push(
                json!({"profile":"paired","repetition":repetition,"comparison":comparison,
                "result":{"correct":repetition!=0,
                    "committed_per_second":if comparison{1000.0*ratio}else{1000.0}}}),
            );
        }
    }

    let mut report = json!({"runs":runs});
    assert!(!outcome(&args, &mut report));
    assert_eq!(report["passed"], false);
    assert_eq!(
        report["paired_summary"]["repetitions"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(report["paired_summary"]["median_throughput_ratio"], 0.9);
    assert_eq!(report["gates"]["correctness_and_accounting"], false);
    assert_eq!(report["reference_qualified"], false);
}

fn hardware() -> Value {
    json!({"host_cpu":" Recorded CPU ","host_memory_bytes":34_359_738_368_u64,
        "storage_description":" Local SSD ", "local_ssd":true, "quiet_host":true,
            "evidence":" Verified externally "})
}

fn qualified_report() -> (Args, Value) {
    let mut args = args();
    args.profile = "all".into();
    args.image = Some("synthetic-image".into());
    args.hardware_provenance = Some("synthetic-hardware.json".into());

    let resources = json!({"inspect":json!({"NanoCpus":4_000_000_000_u64,
        "Memory":1_073_741_824_u64}).to_string()});

    let live = json!({"correct":true,"committed_per_second":1000.0,
        "expected_offered_ingest":660000,"expected_measured_matching_offers":6000,
        "expected_offered_query":1320,"expected_measured_queries":1200,
        "counts":{"offered_ingest":660000, "accepted_ingest":660000, "offered_query":1320,
            "sent_query":1320, "completed_query":1320},
        "service_latency_ms":{"measured_search":{"samples":480},
            "measured_count":{"samples":240}, "measured_histogram":{"samples":240},
            "measured_facets":{"samples":240}},
        "tail":{"expected_matching_committed":6000, "missing_matching_committed":0,
            "warmup_missing_matching_committed":0,
            "scheduled_offer_to_observation_ms":{"samples":6000},
                "observed_before_acknowledgement":0,
            "durable_acknowledgement_to_observation_ms":{"samples":6000,"p95":10.0}}});
    let mut dedicated_tail = live.clone();
    dedicated_tail["offered_rate"] = json!(100.0);
    dedicated_tail["warmup_seconds"] = json!(60.0);
    dedicated_tail["measured_seconds"] = json!(600.0);
    dedicated_tail["expected_offered_ingest"] = json!(66000);
    dedicated_tail["counts"]["offered_ingest"] = json!(66000);
    dedicated_tail["counts"]["accepted_ingest"] = json!(66000);
    dedicated_tail["expected_measured_matching_offers"] = json!(600);
    dedicated_tail["tail"]["expected_matching_committed"] = json!(600);
    dedicated_tail["tail"]["scheduled_offer_to_observation_ms"]["samples"] = json!(600);
    dedicated_tail["tail"]["durable_acknowledgement_to_observation_ms"]["samples"] = json!(600);

    let mut runs = vec![
        json!({"profile":"queries", "resources":resources, "result":{"passed":true,
            "reference_latency_met":true,
            "cells":vec![json!({"latency_ms":{"samples":1000}});8]}}),
        json!({"profile":"tail","resources":resources,"result":dedicated_tail}),
    ];
    for repetition in 0..3 {
        for comparison in [false, true] {
            runs.push(json!({"profile":"paired", "repetition":repetition,
                "comparison":comparison, "resources":resources, "result":live}));
        }
    }

    let runs: Vec<_> = runs
        .into_iter()
        .map(|run| finish_run(Ok(run), Ok(())).unwrap())
        .collect();
    (
        args,
        json!({"runs":runs, "provenance":{"hardware":hardware(),
            "image_inspect":json!({"Id":"sha256:synthetic"}).to_string()}}),
    )
}

#[test]
fn lifecycle_failure_is_persisted_before_stopping_the_run_sequence() {
    let directory = tempfile::tempdir().unwrap();
    for field in ["resources_passed", "cleanup_passed"] {
        let output = directory.path().join(format!("{field}.json"));
        let mut report = json!({"runs":[]});
        let mut launched = 0;
        let result: Result<()> = (0..3).try_for_each(|position| {
            launched += 1;
            let mut run = json!({"position":position, "resources_passed":true,
                "cleanup_passed":true,
                "result":{"raw":[{"elapsed_ms":12.0}],"cpu":{"usage_usec_delta":42}}});
            if position == 1 {
                run[field] = json!(false);
            }
            persist_run(&mut report, run, &output)
        });
        assert!(result.is_err());
        assert_eq!(launched, 2, "must stop before launching the next fixture");

        let saved: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
        assert_eq!(saved, report);
        assert_eq!(saved["runs"].as_array().unwrap().len(), 2);
        assert_eq!(saved["runs"][1][field], false);
        assert_eq!(saved["runs"][1]["result"]["raw"][0]["elapsed_ms"], 12.0);
        assert_eq!(saved["runs"][1]["result"]["cpu"]["usage_usec_delta"], 42);
    }
}

#[test]
fn resource_and_cleanup_failures_retain_samples_and_fail_outcome() {
    let (args, original) = qualified_report();
    for (resource_failure, cleanup_failure) in [(true, false), (false, true), (true, true)] {
        let mut report = original.clone();
        let mut completed = report["runs"][0].clone();
        completed["result"]["cells"][0]["raw"] = json!([{"status":200,"elapsed_ms":12.0}]);
        completed["result"]["cells"][0]["cpu"] =
            json!({"measured_usage_usec_delta":42,"batches":[{"delta":{"usage_usec":42}}]});
        let expected = completed["result"].clone();

        if resource_failure {
            completed["resources"] = json!({"collection_error":"synthetic observer failure"});
        }

        let cleanup = if cleanup_failure {
            Err("synthetic cleanup failure".into())
        } else {
            Ok(())
        };
        let finished = finish_run(Ok(completed), cleanup).unwrap();
        assert_eq!(finished["result"], expected);
        assert_eq!(finished["resources_passed"], !resource_failure);
        assert_eq!(finished["cleanup_passed"], !cleanup_failure);

        if resource_failure {
            assert_eq!(
                finished["resources"]["collection_error"],
                "synthetic observer failure"
            );
        }

        if cleanup_failure {
            assert_eq!(finished["cleanup_error"], "synthetic cleanup failure");
        }

        report["runs"][0] = finished;
        assert!(!outcome(&args, &mut report));
        assert_eq!(report["gates"]["resource_collection_and_cleanup"], false);
        assert_eq!(report["passed"], false);
        assert_eq!(report["reference_qualified"], false);
    }

    assert!(finish_run(Err("work failed".into()), Ok(())).is_err());
    assert_eq!(
        finish_run(Err("work failed".into()), Err("cleanup failed".into()))
            .unwrap_err()
            .to_string(),
        "work failed; cleanup: cleanup failed"
    );
}

#[test]
fn explicit_hardware_evidence_allows_full_reference_qualification() {
    let (mut args, mut report) = qualified_report();
    assert!(outcome(&args, &mut report));
    assert_eq!(report["reference_qualified"], true);

    args.hardware_provenance = None;
    assert!(outcome(&args, &mut report));
    assert_eq!(report["reference_qualified"], false);

    args.hardware_provenance = Some("synthetic-hardware.json".into());
    args.image = None;
    assert!(outcome(&args, &mut report));
    assert_eq!(report["reference_qualified"], false);
}

#[test]
fn shared_or_unspecified_host_cannot_qualify_but_retains_passing_measured_targets() {
    for quiet_host in [Some(false), None] {
        let (args, mut report) = qualified_report();
        if let Some(quiet_host) = quiet_host {
            report["provenance"]["hardware"]["quiet_host"] = json!(quiet_host);
        } else {
            report["provenance"]["hardware"]
                .as_object_mut()
                .unwrap()
                .remove("quiet_host");
        }

        assert!(outcome(&args, &mut report));
        assert_eq!(report["passed"], true);
        assert_eq!(report["reference_qualified"], false);
        assert_eq!(report["gates"]["hardware_storage_provenance"], true);
        assert_eq!(report["gates"]["quiet_host_provenance"], false);
        assert_eq!(report["gates"]["query_p95_targets"], true);
        assert_eq!(report["gates"]["tail_p95_target"], true);
        assert_eq!(report["gates"]["paired_median_throughput_target"], true);
    }
}

#[test]
fn hardware_provenance_is_strict_and_normalized() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("hardware.json");
    std::fs::write(&path, hardware().to_string()).unwrap();

    let parsed = HardwareProvenance::read(&path).unwrap();
    assert_eq!(parsed.host_cpu, "Recorded CPU");
    assert_eq!(parsed.storage_description, "Local SSD");
    assert_eq!(parsed.evidence, "Verified externally");
    assert!(parsed.quiet_host);

    let mut old_evidence = hardware();
    old_evidence.as_object_mut().unwrap().remove("quiet_host");
    std::fs::write(&path, old_evidence.to_string()).unwrap();
    assert!(!HardwareProvenance::read(&path).unwrap().quiet_host);

    for (field, value) in [
        ("host_cpu", json!(" ")),
        ("host_memory_bytes", json!(0)),
        ("host_memory_bytes", json!("34359738368")),
        ("storage_description", json!("")),
        ("local_ssd", json!("true")),
        ("quiet_host", json!("true")),
        ("evidence", json!("\n")),
        ("unknown", json!(1)),
    ] {
        let mut value_object = hardware();
        value_object[field] = value;
        std::fs::write(&path, value_object.to_string()).unwrap();
        assert!(HardwareProvenance::read(&path).is_err(), "{field}");
    }

    for field in [
        "host_cpu",
        "host_memory_bytes",
        "storage_description",
        "local_ssd",
        "evidence",
    ] {
        let mut missing = hardware();
        missing.as_object_mut().unwrap().remove(field);
        std::fs::write(&path, missing.to_string()).unwrap();
        assert!(HardwareProvenance::read(&path).is_err(), "missing {field}");
    }

    let duplicate = hardware()
        .to_string()
        .replacen('{', "{\"local_ssd\":true,", 1);
    std::fs::write(&path, duplicate).unwrap();
    assert!(HardwareProvenance::read(&path).is_err());

    std::fs::write(&path, " ".repeat(65_537)).unwrap();
    assert!(HardwareProvenance::read(&path).is_err());
}

#[test]
fn one_fast_tail_sample_cannot_hide_rejected_reference_traffic() {
    let (args, mut report) = qualified_report();
    let tail = &mut report["runs"][1]["result"];
    tail["counts"]["accepted_ingest"] = json!(10);
    tail["counts"]["rejected_ingest"] = json!(65990);
    tail["tail"]["expected_matching_committed"] = json!(1);
    tail["tail"]["scheduled_offer_to_observation_ms"]["samples"] = json!(1);
    tail["tail"]["durable_acknowledgement_to_observation_ms"]["samples"] = json!(1);

    assert!(!outcome(&args, &mut report));
    assert_eq!(report["gates"]["tail_p95_target"], true);
    assert_eq!(report["gates"]["tail_full_delivery"], false);
    assert_eq!(report["reference_qualified"], false);
}

#[test]
fn qualification_requires_actual_container_limits_and_latency_samples() {
    let (args, report) = qualified_report();
    for mutate in [0, 1, 2, 3, 4] {
        let mut report = report.clone();
        match mutate {
            0 => report["runs"][0]["resources"]["inspect"] = json!("{}"),
            1 => report["provenance"]["hardware"]["local_ssd"] = json!(false),
            2 => {
                report["runs"][1]["result"]["tail"]["durable_acknowledgement_to_observation_ms"]["samples"] =
                    json!(0)
            }
            3 => report["runs"][0]["result"]["cells"][0]["latency_ms"]["samples"] = json!(0),
            _ => report["runs"][2]["result"]["counts"]["client_dropped_query"] = json!(1),
        }

        outcome(&args, &mut report);
        assert_eq!(report["reference_qualified"], false, "mutation {mutate}");
    }
}

#[test]
fn all_negative_acknowledgement_races_require_the_complete_population() {
    let (args, mut report) = qualified_report();
    let tail = &mut report["runs"][1]["result"]["tail"];
    tail["durable_acknowledgement_to_observation_ms"] = json!({"samples":0,"p95":null});
    tail["observed_before_acknowledgement"] = json!(600);
    tail["negative_observation_magnitude_ms"] = json!({"samples":600,"p95":2.0});

    assert!(outcome(&args, &mut report));
    assert_eq!(report["gates"]["tail_p95_target"], true);
    assert_eq!(report["gates"]["tail_full_delivery"], true);
    assert_eq!(report["reference_qualified"], true);

    report["runs"][1]["result"]["tail"]["observed_before_acknowledgement"] = json!(599);
    assert!(!outcome(&args, &mut report));
    assert_eq!(report["gates"]["tail_p95_target"], false);
    assert_eq!(report["gates"]["tail_full_delivery"], false);
    assert_eq!(report["reference_qualified"], false);
}
