//! Query cells, bounded stress probes, and timeout recovery verification.
use super::{
    Args,
    constants::{
        FAILURE_EXAMPLE_LIMIT, GATEWAY_TIMEOUT_STATUS, HISTOGRAM_INTERVAL_MS,
        MAX_BOUNDED_DEADLINE_MS, MAX_LATE_PAGES, MAX_ROWS, MAXIMUM_FILTER_KEYS, OTHER_P95_LIMIT_MS,
        QUERY_ERROR_CHARS, RECOVERY_DEADLINE, RECOVERY_POLL, SEARCH_P95_LIMIT_MS,
        SEARCH_PAGE_LIMIT,
    },
    data::{Operation, Position, Scope, Seed, make_event, query_params, verify_response},
    elapsed_ms, http_error, len_u64,
};
use crate::{
    Result,
    load::{
        attributes_schedule as schedule,
        http::{self, Http},
        semantics::{HTTP_OK, Samples},
    },
    runtime::Fixture,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Write},
    time::Instant,
};

/// Builds the request path for `operation` over one run or run-and-actor selection.
pub(in crate::load) fn request_path(
    seed: &Seed,
    operation: &str,
    run: u64,
    actor: Option<u64>,
) -> String {
    let operation = Operation::parse(operation).expect("known attribute operation");
    operation_path(operation, query_params(seed, run, actor))
}

/// Adds the operation's own query parameter to `params` and returns its request path.
pub(in crate::load) fn operation_path(
    operation: Operation,
    mut params: BTreeMap<String, String>,
) -> String {
    match operation {
        Operation::Search => {
            params.insert("limit".into(), SEARCH_PAGE_LIMIT.to_string());
        }
        Operation::Histogram => {
            params.insert("interval_ms".into(), HISTOGRAM_INTERVAL_MS.to_string());
        }
        Operation::Count | Operation::Facets => {}
    }

    http::path(operation.endpoint(), &params)
}

/// Whether a deadline response from a bounded envelope counts as a measured limit rather than a
/// failure: only at the largest fixture or the maximum filter shape, with a server deadline
/// status and a bounded elapsed time.
fn allowed_stress_timeout(rows: u64, maximum_filter: bool, status: u16, elapsed_ms: f64) -> bool {
    // A real server deadline response is an envelope measurement at scale or
    // maximum filter shape. Transport timeouts and unbounded responses fail.
    (rows >= MAX_ROWS || maximum_filter)
        && status == GATEWAY_TIMEOUT_STATUS
        && (0.0..=MAX_BOUNDED_DEADLINE_MS).contains(&elapsed_ms)
}

/// Text of a query error, truncated to `limit` characters, or `None` for success.
pub(in crate::load) fn error_text(result: &Result<()>, limit: usize) -> Option<String> {
    result
        .as_ref()
        .err()
        .map(|error| error.to_string().chars().take(limit).collect::<String>())
}

/// Checks that a narrow count succeeds again after a deadline, and that readers go idle.
async fn verify_timeout_recovery(http: &Http, args: &Args, seed: &Seed) -> Value {
    let first = make_event(args, 0, seed.from);
    let time = first["time"].as_i64().expect("fixture timestamp");
    let mut params = query_params(seed, 0, None);
    params.insert("from".into(), time.to_string());

    params.insert("to".into(), (time + 1).to_string());
    params.insert(
        "filter".into(),
        json!({"attributes":[{"key":"sequence","op":"eq","value":0}]}).to_string(),
    );
    let started = Instant::now();
    let (status, payload) = http
        .request(&http::path("/logs/count", &params), None, false)
        .await;
    let count_correct = status == HTTP_OK && payload["count"] == 1;
    let (metric_status, busy) = await_idle_readers(http).await;
    json!({
        "narrow_count_status": status,
        "narrow_count": payload["count"],
        "expected_count": 1,
        "metrics_status": metric_status,
        "readers_busy": busy,
        "elapsed_ms": elapsed_ms(started),
        "passed": count_correct && metric_status == HTTP_OK && busy == Some(0.0)
    })
}

/// Polls `/metrics` until no reader is busy or the recovery deadline passes.
///
/// Returns the last metrics status and busy-reader count; status 0 means the poll itself timed
/// out.
async fn await_idle_readers(http: &Http) -> (u16, Option<f64>) {
    let deadline = tokio::time::Instant::now() + RECOVERY_DEADLINE;
    loop {
        let Ok((status, payload)) =
            tokio::time::timeout_at(deadline, http.request("/metrics", None, true)).await
        else {
            return (0, None);
        };
        let busy = http::metrics(&payload)
            .get("logbrook_storage_readers_busy")
            .copied();
        if status == HTTP_OK && busy == Some(0.0) || tokio::time::Instant::now() >= deadline {
            return (status, busy);
        }

        tokio::time::sleep(RECOVERY_POLL).await;
    }
}

/// Outcome of one measured query: its sample record and, after a bounded deadline, its
/// timeout record.
async fn send_query(
    http: &Http,
    args: &Args,
    seed: &Seed,
    batch: &schedule::Batch,
    offset: u64,
) -> (Value, Option<Value>) {
    let (selection, operation) = schedule::cell(batch.cell);
    let ordinal = batch.start + offset;
    let (run, actor) = schedule::query(ordinal, args.runs, selection == "run_actor");
    let path = request_path(seed, operation, run, actor);
    let started = Instant::now();
    let (status, payload) = http.request(&path, None, false).await;
    let elapsed = elapsed_ms(started);
    // Validation, diagnostics and deadline recovery are outside HTTP timing.
    let mut checked = if status == HTTP_OK {
        verify_response(seed, operation, run, actor, &payload)
    } else {
        Err(http_error(status, &payload).into())
    };
    let mut timeout = None;
    if allowed_stress_timeout(args.rows, false, status, elapsed) {
        let recovery = verify_timeout_recovery(http, args, seed).await;
        if recovery["passed"] == true {
            checked = Ok(());
        }

        timeout = Some(json!({
            "iteration": ordinal,
            "warmup": batch.warmup,
            "status": status,
            "elapsed_ms": elapsed,
            "response": payload,
            "recovery": recovery
        }));
    }

    let sample = json!({
        "iteration": ordinal,
        "warmup": batch.warmup,
        "batch_id": batch.id,
        "schedule_position": batch.position + offset,
        "run": run,
        "actor": actor,
        "status": status,
        "elapsed_ms": elapsed,
        "oracle_passed": checked.is_ok(),
        "error": error_text(&checked, QUERY_ERROR_CHARS)
    });
    (sample, timeout)
}

/// Runs the eight reference latency cells in their rotated batch order.
pub(super) async fn queries(fixture: &Fixture, args: &Args, seed: &Seed) -> Result<Value> {
    let http = Http::new(&fixture.base, &fixture.read_token, &fixture.ingest_token)?;
    let mut raw: Vec<Vec<Value>> = vec![Vec::new(); schedule::CELLS];
    let mut cpu: Vec<Vec<Value>> = vec![Vec::new(); schedule::CELLS];
    let mut timeouts: Vec<Vec<Value>> = vec![Vec::new(); schedule::CELLS];
    for batch in schedule::batches(args.warmup, args.samples) {
        let before = schedule::observe_cpu(fixture, args.cpu_diagnostics).await;

        for offset in 0..batch.len {
            let (sample, timeout) = send_query(&http, args, seed, &batch, offset).await;
            raw[batch.cell].push(sample);
            timeouts[batch.cell].extend(timeout);
        }

        let after = schedule::observe_cpu(fixture, args.cpu_diagnostics).await;
        cpu[batch.cell].push(schedule::cpu_batch(batch, before, after));
    }

    let mut cells = Vec::new();
    for (index, samples) in raw.into_iter().enumerate() {
        cells.push(query_cell(
            args,
            index,
            samples,
            std::mem::take(&mut cpu[index]),
            std::mem::take(&mut timeouts[index]),
        ));
    }

    let extended = extended_queries(&http, args, seed).await;
    let passed = cells.iter().all(|cell| cell["passed"] == true) && extended["passed"] == true;
    let reference_latency_met = cells
        .iter()
        .all(|cell| cell["reference_latency_met"] == true);
    Ok(json!({
        "schedule": schedule::metadata(args.runs),
        "cells": cells,
        "extended": extended,
        "passed": passed,
        "reference_latency_met": reference_latency_met
    }))
}

/// Summarizes one reference cell: latency, statuses, oracle failures, timeouts and CPU.
fn query_cell(
    args: &Args,
    index: usize,
    raw: Vec<Value>,
    cpu: Vec<Value>,
    timeouts: Vec<Value>,
) -> Value {
    let (selection, operation) = schedule::cell(index);
    let mut samples = Samples::default();
    let mut statuses = BTreeMap::new();
    for sample in raw.iter().filter(|sample| sample["warmup"] == false) {
        samples.add(sample["elapsed_ms"].as_f64().expect("recorded latency"));
        *statuses
            .entry(sample["status"].as_u64().expect("recorded status"))
            .or_insert(0_u64) += 1;
    }

    let failures: Vec<_> = raw
        .iter()
        .filter(|sample| sample["oracle_passed"] != true)
        .collect();
    let latency = samples.report();
    let target = reference_p95_limit_ms(operation);
    let _ = writeln!(
        io::stderr(),
        concat!(
            "attribute-load cell selection={} operation={} ",
            "measured={} failures={} bounded_timeouts={} p95_ms={}"
        ),
        selection,
        operation,
        args.samples,
        failures.len(),
        timeouts.len(),
        latency["p95"]
    );

    json!({
        "selection": selection,
        "operation": operation,
        "warmup_requests": args.warmup,
        "measured_requests": args.samples,
        "coverage": {
            "warmup": schedule::coverage(&raw, true),
            "measured": schedule::coverage(&raw, false)
        },
        "cpu": schedule::cpu_report(cpu, args.cpu_diagnostics),
        "statuses": statuses,
        "latency_ms": latency,
        "correctness_failures": failures.len(),
        "failure_examples": failures.iter().take(FAILURE_EXAMPLE_LIMIT).collect::<Vec<_>>(),
        "bounded_scale_timeouts": timeouts,
        "reference_p95_limit_ms": target,
        "reference_latency_met": latency["p95"].as_f64().is_some_and(|value| value <= target),
        "passed": failures.is_empty(),
        "raw": raw
    })
}

/// Reference p95 limit for `operation`, in milliseconds.
pub(in crate::load) fn reference_p95_limit_ms(operation: &str) -> f64 {
    if operation == "search" {
        SEARCH_P95_LIMIT_MS
    } else {
        OTHER_P95_LIMIT_MS
    }
}

/// Unfiltered and maximum-shape count probes, then the late-page cursor walk.
async fn extended_queries(http: &Http, args: &Args, seed: &Seed) -> Value {
    let mut checks = Vec::new();
    for case in [
        ExtendedCase::NoMatch,
        ExtendedCase::Singleton,
        ExtendedCase::MaximumConditions,
    ] {
        checks.push(count_case(http, args, seed, case).await);
    }

    let (late_pages, bounded_timeout) = late_page_probe(http, args, seed).await;
    checks.push(late_pages);
    let interpretation = if bounded_timeout {
        concat!(
            "bounded ten-million-row/maximum-filter deadline outcomes ",
            "are measured envelope limits; narrow query and idle-reader recovery are required"
        )
    } else {
        concat!(
            "single correctness/stress probes, ",
            "separate from the eight isolated reference latency cells"
        )
    };
    json!({
        "checks": checks,
        "passed": checks.iter().all(|check| check["passed"] == true),
        "interpretation": interpretation
    })
}

/// The single-count probes that run outside the reference latency cells.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ExtendedCase {
    NoMatch,
    Singleton,
    MaximumConditions,
}

impl ExtendedCase {
    /// Name of the case in the report.
    fn name(self) -> &'static str {
        match self {
            Self::NoMatch => "no_match",
            Self::Singleton => "singleton",
            Self::MaximumConditions => "maximum_16_conditions",
        }
    }

    /// Attribute conditions that the case sends.
    fn conditions(self, args: &Args, seed: &Seed) -> Value {
        match self {
            Self::NoMatch => json!([{"key":"runId","op":"eq","value":"absent-run"}]),
            Self::Singleton => json!([{"key":"sequence","op":"eq","value":0}]),
            Self::MaximumConditions => {
                let event = make_event(args, 0, seed.from);
                let conditions: Vec<_> = MAXIMUM_FILTER_KEYS
                    .iter()
                    .map(|key| json!({"key": key, "op": "eq", "value": event[*key]}))
                    .collect();
                json!(conditions)
            }
        }
    }
}

/// Sends one extended count probe and checks its total, or its bounded timeout recovery.
async fn count_case(http: &Http, args: &Args, seed: &Seed, case: ExtendedCase) -> Value {
    let mut params = query_params(seed, 0, None);
    params.insert(
        "filter".into(),
        json!({"attributes": case.conditions(args, seed)}).to_string(),
    );

    let started = Instant::now();
    let (status, payload) = http
        .request(&http::path("/logs/count", &params), None, false)
        .await;
    let elapsed = elapsed_ms(started);
    let expected = u64::from(case != ExtendedCase::NoMatch);
    let limitation = allowed_stress_timeout(
        args.rows,
        case == ExtendedCase::MaximumConditions,
        status,
        elapsed,
    );
    let recovery = if limitation {
        Some(verify_timeout_recovery(http, args, seed).await)
    } else {
        None
    };
    let passed = (status == HTTP_OK && payload["count"].as_u64() == Some(expected))
        || recovery
            .as_ref()
            .is_some_and(|recovery| recovery["passed"] == true);
    json!({
        "case": case.name(),
        "status": status,
        "expected_count": expected,
        "actual_count": payload["count"],
        "response": payload,
        "elapsed_ms": elapsed,
        "bounded_envelope_timeout": limitation,
        "recovery": recovery,
        "passed": passed
    })
}

/// Progress of the late-page walk over run 0.
#[derive(Default)]
struct PageWalk {
    pages: u64,
    actual: BTreeSet<u64>,
    previous: Option<Position>,
    error: Option<String>,
}

/// Checks one page of events in order, recording each ID; returns the first failure.
fn accept_page(walk: &mut PageWalk, events: &[Value], seed: &Seed) -> Option<&'static str> {
    for event in events {
        let id = event["id"]
            .as_str()
            .and_then(|value| value.parse::<u64>().ok());
        let time = event["event_time"].as_i64();
        let (Some(id), Some(time)) = (id, time) else {
            return Some("late-page malformed event");
        };
        let position = Position { time, id };
        if id == 0
            || id > seed.rows
            || (id - 1) % seed.runs != 0
            || !walk.actual.insert(id)
            || walk.previous.is_some_and(|previous| position >= previous)
        {
            return Some("late-page IDs or ordering differ from independent fixture oracle");
        }

        walk.previous = Some(position);
    }

    None
}

/// Walks every cursor page of run 0 and checks each ID against the oracle.
///
/// Returns the report check and whether a bounded envelope timeout ended the walk early.
async fn late_page_probe(http: &Http, args: &Args, seed: &Seed) -> (Value, bool) {
    let started = Instant::now();
    let mut params = query_params(seed, 0, None);
    params.insert("limit".into(), SEARCH_PAGE_LIMIT.to_string());

    let mut walk = PageWalk::default();
    loop {
        if walk.pages >= MAX_LATE_PAGES {
            walk.error = Some("bounded late-page probe exceeded 10000 pages".to_owned());
            break;
        }

        let page_started = Instant::now();
        let (status, payload) = http
            .request(&http::path("/logs", &params), None, false)
            .await;
        walk.pages += 1;
        if allowed_stress_timeout(args.rows, false, status, elapsed_ms(page_started)) {
            let recovery = verify_timeout_recovery(http, args, seed).await;
            let check = json!({
                "case": "all_later_cursor_pages_one_run",
                "status": status,
                "bounded_envelope_timeout": true,
                "pages": walk.pages,
                "partial_actual_count": walk.actual.len(),
                "expected_count": seed.expected[&Scope::new(0, None)].count,
                "response": payload,
                "recovery": recovery,
                "passed": recovery["passed"] == true
            });
            return (check, true);
        }

        if status != 200 {
            walk.error = Some(format!("late-page HTTP {status}: {payload}"));
            break;
        }

        let Some(events) = payload["events"].as_array() else {
            walk.error = Some("late-page missing events".into());
            break;
        };
        if let Some(message) = accept_page(&mut walk, events, seed) {
            walk.error = Some(message.into());
            break;
        }

        let Some(cursor) = payload["next_cursor"].as_str() else {
            break;
        };
        params.insert("cursor".into(), cursor.into());
    }

    let expected = seed.expected[&Scope::new(0, None)].count;
    let check = json!({
        "case": "all_later_cursor_pages_one_run",
        "pages": walk.pages,
        "expected_count": expected,
        "actual_count": walk.actual.len(),
        "elapsed_ms": elapsed_ms(started),
        "error": walk.error,
        "passed": walk.error.is_none() && len_u64(walk.actual.len()) == expected
    });
    (check, false)
}

#[cfg(test)]
mod stress_timeout_tests {
    use super::allowed_stress_timeout;

    #[test]
    fn only_bounded_scale_or_maximum_shape_server_deadlines_are_limitations() {
        assert!(allowed_stress_timeout(10_000_000, false, 504, 5001.0));
        assert!(allowed_stress_timeout(1_000_000, true, 504, 5001.0));
        assert!(!allowed_stress_timeout(1_000_000, false, 504, 5001.0));
        assert!(!allowed_stress_timeout(10_000_000, false, 0, 15000.0));
        assert!(!allowed_stress_timeout(10_000_000, false, 503, 5000.0));
        assert!(!allowed_stress_timeout(10_000_000, false, 504, 7001.0));
        assert!(!allowed_stress_timeout(10_000_000, false, 504, f64::NAN));
    }
}
