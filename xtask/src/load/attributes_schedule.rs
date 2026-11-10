//! Shared deterministic query ordering and optional whole-container CPU diagnostics.
use super::semantics::MILLISECONDS_PER_SECOND;
use crate::{Result, runtime::Fixture};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

/// Read operations in cell order. Cell `index` measures `OPERATIONS[index % 4]`.
pub const OPERATIONS: [&str; 4] = ["search", "count", "histogram", "facets"];

/// Number of cells: every operation over the run selection, then over the run-and-user one.
pub const CELLS: usize = 8;

/// Requests per batch. Every cell gets one batch per cycle of `CELLS` batches.
pub const BATCH_SIZE: u64 = 10;

/// Seed of the deterministic cell rotation and run permutation.
const SCHEDULE_SEED: u64 = 123;

/// Stride the run permutation starts searching from before it is made coprime with the runs.
const RUN_STRIDE_START: u64 = 37;

/// Cell rotation advances by this many cells for every batch cycle.
const ROTATION_STEP: u64 = 3;

/// Distinct users per run; a query's user is taken modulo this.
const USERS: u64 = 10;

/// Runs in the reference fixture, which the reference coverage check expects.
const REFERENCE_RUN_COUNT: usize = 1000;

/// Requests per user that the reference coverage check expects.
const REQUESTS_PER_USER: u64 = 100;

/// Microseconds in one second, for converting CPU counters.
const MICROS_PER_SECOND: f64 = 1_000_000.0;

/// Message of the invariant behind a CPU delta sum: a phase is summed only when collected.
const USAGE_COUNTER: &str = "validated usage counter";

/// One batch of requests: which cell it measures, where it sits, and how many requests it has.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Batch {
    /// Position of the batch in the whole schedule.
    pub id: usize,
    /// Index of the cell that the batch measures.
    pub cell: usize,
    /// Whether the batch belongs to the warmup phase.
    pub warmup: bool,
    /// Ordinal of the batch's first request within its phase.
    pub start: u64,
    /// Requests in the batch.
    pub len: u64,
    /// Number of requests scheduled before this batch, across all phases.
    pub position: u64,
}

/// The selection and operation that cell `index` measures.
pub fn cell(index: usize) -> (&'static str, &'static str) {
    let selection = if index < OPERATIONS.len() {
        "run"
    } else {
        "run_user"
    };
    (selection, OPERATIONS[index % OPERATIONS.len()])
}

/// Builds the batch schedule: warmup batches first, then measured batches, each phase rotating
/// through the cells one batch per cell per cycle.
pub fn batches(warmup: u64, samples: u64) -> Vec<Batch> {
    let mut result = Vec::new();
    let mut position = 0;
    for (is_warmup, total) in [(true, warmup), (false, samples)] {
        for cycle in 0..total.div_ceil(BATCH_SIZE) {
            let start = cycle * BATCH_SIZE;
            for slot in 0..CELLS {
                let cell = (slot + rotation(cycle)) % CELLS;
                let len = BATCH_SIZE.min(total - start);
                result.push(Batch {
                    id: result.len(),
                    cell,
                    warmup: is_warmup,
                    start,
                    len,
                    position,
                });
                position += len;
            }
        }
    }

    result
}

/// How far the cell rotation is shifted in batch cycle `cycle`.
fn rotation(cycle: u64) -> usize {
    let cells = u64::try_from(CELLS).expect("cell count fits in u64");
    usize::try_from((SCHEDULE_SEED + cycle * ROTATION_STEP) % cells)
        .expect("rotation is below the cell count")
}

/// Greatest common divisor.
fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

/// The smallest stride at or above the starting point that is coprime with `runs`, so that the
/// run permutation visits every run exactly once.
fn run_stride(runs: u64) -> u64 {
    assert!(runs > 0, "validated nonzero run count");
    let mut stride = RUN_STRIDE_START % runs;
    while gcd(stride, runs) != 1 {
        stride += 1;
    }
    stride
}

/// Maps a request's ordinal within its phase onto `(run, user)`. The user is `None` when the
/// selection is run-only.
pub fn query(ordinal: u64, runs: u64, with_user: bool) -> (u64, Option<u64>) {
    let stride = run_stride(runs);
    // Modulo before multiplication also bounds arithmetic for arbitrary ordinals.
    let run = ((ordinal % runs) * stride + SCHEDULE_SEED % runs) % runs;
    (
        run,
        with_user.then_some((ordinal % runs + ordinal / runs) % USERS),
    )
}

/// Report section that describes the schedule's order and selection rules.
pub fn metadata(runs: u64) -> Value {
    json!({
        "version": 2,
        "seed": SCHEDULE_SEED,
        "run_stride": run_stride(runs),
        "runs": runs,
        "batch_size": BATCH_SIZE,
        "order": concat!(
            "eight cells, rotated round robin per ten-request cycle; warmup then measurement, ",
            "phase ordinal resets"
        ),
        "query_selection": concat!(
            "coprime run permutation; user = (phase ordinal modulo runs + phase ordinal / runs) ",
            "modulo 10; identical in every process block"
        )
    })
}

/// Request coverage of one phase: counts per run and per user, and whether the reference
/// coverage was reached (every run seen, and every user seen at least 100 times).
pub fn coverage(raw: &[Value], warmup: bool) -> Value {
    let mut runs = BTreeMap::<u64, u64>::new();
    let mut users = BTreeMap::<u64, u64>::new();
    let mut pairs = BTreeSet::new();
    let mut count = 0;
    for sample in raw.iter().filter(|sample| sample["warmup"] == warmup) {
        let Some(run) = sample["run"].as_u64() else {
            continue;
        };
        let user = sample["user"].as_u64();
        count += 1;
        *runs.entry(run).or_default() += 1;
        if let Some(user) = user {
            *users.entry(user).or_default() += 1;
        }

        pairs.insert((run, user));
    }

    let all_runs = runs.len() == REFERENCE_RUN_COUNT;
    let users_balanced =
        users.len() as u64 == USERS && users.values().all(|count| *count >= REQUESTS_PER_USER);
    json!({
        "requests": count,
        "distinct_runs": runs.len(),
        "distinct_users": users.len(),
        "distinct_run_user_pairs": pairs.len(),
        "run_requests": runs,
        "user_requests": users,
        "reference_coverage_achieved": all_runs && (users.is_empty() || users_balanced),
        "reference_run_coverage_achieved": all_runs,
        "reference_user_coverage_achieved": if users.is_empty() {
            None
        } else {
            Some(users_balanced)
        }
    })
}

/// One CPU counter reading of the target container, or why there is none.
#[derive(Clone, Debug)]
pub struct CpuObservation {
    status: &'static str,
    counters: Option<BTreeMap<String, u64>>,
    error: Option<String>,
    elapsed_ms: f64,
}

/// Parses the `cpu.stat` text into counters, requiring `usage_usec` and no duplicates.
fn parse_cpu(text: &str) -> Result<BTreeMap<String, u64>> {
    let mut counters = BTreeMap::new();
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        let key = fields.next().ok_or("empty cpu.stat line")?;
        // The parse error only says that the digits were malformed, which the message states.
        let value = fields
            .next()
            .ok_or("missing cpu.stat value")?
            .parse::<u64>()
            .map_err(|_| "invalid cpu.stat value")?;
        if fields.next().is_some() || counters.insert(key.to_owned(), value).is_some() {
            return Err("duplicate or malformed cpu.stat counter".into());
        }
    }

    if !counters.contains_key("usage_usec") {
        return Err("cpu.stat omitted usage_usec".into());
    }

    Ok(counters)
}

impl CpuObservation {
    /// Reads the `cpu.stat` text out of an observer result; a failed read or parse is recorded.
    fn from_probe(result: Result<Value>, elapsed_ms: f64) -> Self {
        let parsed = result.and_then(|value| {
            parse_cpu(
                value["cpu.stat"]
                    .as_str()
                    .ok_or("observer omitted cpu.stat")?,
            )
        });
        match parsed {
            Ok(counters) => Self {
                status: "collected",
                counters: Some(counters),
                error: None,
                elapsed_ms,
            },
            Err(error) => Self {
                status: "failed",
                counters: None,
                error: Some(error.to_string()),
                elapsed_ms,
            },
        }
    }

    /// An observation that was not attempted, with the reason as its status.
    fn unavailable(status: &'static str) -> Self {
        Self {
            status,
            counters: None,
            error: None,
            elapsed_ms: 0.0,
        }
    }

    /// The observation as the report records it.
    fn report(&self) -> Value {
        json!({
            "status": self.status,
            "counters": self.counters,
            "error": self.error,
            "observer_elapsed_ms": self.elapsed_ms
        })
    }
}

/// Reads the target container's `cpu.stat` when diagnostics are enabled and the fixture is a
/// container. Otherwise returns an observation that says why it was not read.
pub async fn observe_cpu(fixture: &Fixture, enabled: bool) -> CpuObservation {
    if !enabled {
        return CpuObservation::unavailable("disabled");
    }
    let Some(name) = &fixture.container else {
        return CpuObservation::unavailable("unavailable_native");
    };
    let started = Instant::now();
    let result = crate::container::probe(name, &["cpu.stat".into()]).await;
    CpuObservation::from_probe(
        result,
        started.elapsed().as_secs_f64() * MILLISECONDS_PER_SECOND,
    )
}

/// The counter deltas of one batch, from the observations before and after it.
pub fn cpu_batch(batch: Batch, before: CpuObservation, after: CpuObservation) -> Value {
    let delta = before
        .counters
        .as_ref()
        .zip(after.counters.as_ref())
        .and_then(|(before, after)| {
            before
                .iter()
                .map(|(key, start)| {
                    after
                        .get(key)
                        .and_then(|end| end.checked_sub(*start))
                        .map(|delta| (key.clone(), delta))
                })
                .collect::<Option<BTreeMap<_, _>>>()
        });
    let status = if delta.is_some() {
        "collected"
    } else if before.status == "collected" && after.status == "collected" {
        "counter_reset_or_missing"
    } else if before.status == "failed" || after.status == "failed" {
        "failed"
    } else {
        before.status
    };
    json!({
        "batch_id": batch.id,
        "warmup": batch.warmup,
        "phase_ordinal_start": batch.start,
        "requests": batch.len,
        "schedule_position": batch.position,
        "status": status,
        "delta": delta,
        "before": before.report(),
        "after": after.report()
    })
}

/// Sum of the `usage_usec` deltas of batches that were all collected.
fn usage_usec_total<'a>(batches: impl Iterator<Item = &'a Value>) -> u64 {
    batches
        .map(|batch| batch["delta"]["usage_usec"].as_u64().expect(USAGE_COUNTER))
        .sum()
}

/// CPU summary of one phase of batches.
fn cpu_phase(batches: &[Value], warmup: bool) -> Value {
    let phase: Vec<_> = batches
        .iter()
        .filter(|batch| batch["warmup"] == warmup)
        .collect();
    let requests: u64 = phase
        .iter()
        .filter_map(|batch| batch["requests"].as_u64())
        .sum();

    let complete = !phase.is_empty() && phase.iter().all(|batch| batch["status"] == "collected");
    let delta = complete.then(|| usage_usec_total(phase.iter().copied()));
    json!({
        "requests_including_failures": requests,
        "complete": complete,
        "usage_usec_delta": delta,
        // Lossy only for counters far beyond any measurement; the report is a rate.
        "cpu_seconds_per_request": delta
            .filter(|_| requests > 0)
            .map(|delta| delta as f64 / MICROS_PER_SECOND / requests as f64)
    })
}

/// CPU report of the whole run: its status, its measured phase, and every batch.
pub fn cpu_report(batches: Vec<Value>, enabled: bool) -> Value {
    let measured: Vec<_> = batches
        .iter()
        .filter(|batch| batch["warmup"] == false)
        .collect();
    let complete =
        !measured.is_empty() && measured.iter().all(|batch| batch["status"] == "collected");
    let delta = complete.then(|| usage_usec_total(measured.iter().copied()));
    let status = if complete {
        "collected"
    } else if !enabled {
        "disabled"
    } else if measured
        .iter()
        .all(|batch| batch["status"] == "unavailable_native")
    {
        "unavailable_native"
    } else {
        "incomplete"
    };
    json!({
        "status": status,
        "measured_batches": measured.len(),
        "collected_measured_batches": measured
            .iter()
            .filter(|batch| batch["status"] == "collected")
            .count(),
        "measured_usage_usec_delta": delta,
        "warmup": cpu_phase(&batches, true),
        "measured": cpu_phase(&batches, false),
        "scope": concat!(
            "whole target-container CPU during cell batches; includes server background and ",
            "recovery work, not query-only CPU; separate observer containers add host load"
        ),
        "limitations": concat!(
            "cgroup values are lifetime counters; only paired differences are attributed to a ",
            "batch. Interleaved cells have nonoverlapping request windows, but background work ",
            "may overlap. Observer launch/read overhead is outside HTTP timing and can disturb ",
            "caches/host scheduling. Native runs have no cgroup attribution. Diagnostic ",
            "availability is not a performance gate."
        ),
        "batches": batches
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_measurement_covers_every_run_and_user_balanced() {
        let pairs: Vec<_> = (0..1000).map(|n| query(n, 1000, true)).collect();
        assert_eq!(
            pairs
                .iter()
                .map(|(run, _)| run)
                .collect::<BTreeSet<_>>()
                .len(),
            1000
        );

        for user in 0..10 {
            assert_eq!(
                pairs
                    .iter()
                    .filter(|(_, value)| *value == Some(user))
                    .count(),
                100
            );
        }

        assert_ne!(query(0, 1000, true).1, query(1000, 1000, true).1);

        for runs in 1..=1000 {
            assert_eq!(
                (0..runs)
                    .map(|n| query(n, runs, false).0)
                    .collect::<BTreeSet<_>>()
                    .len() as u64,
                runs
            );
        }
    }

    #[test]
    fn warmup_and_measurement_are_independent_repeatable_and_fair() {
        let schedule = batches(13, 23);
        assert_eq!(schedule, batches(13, 23));
        let mut counts = [0_u64; CELLS];
        let mut measured = vec![Vec::new(); CELLS];
        for batch in &schedule {
            assert_eq!(batch.position, counts.iter().sum::<u64>());
            counts[batch.cell] += batch.len;
            assert!(counts.iter().max().unwrap() - counts.iter().min().unwrap() <= BATCH_SIZE);
            if !batch.warmup {
                measured[batch.cell].extend(batch.start..batch.start + batch.len);
            }
        }

        for ordinals in measured {
            assert_eq!(ordinals, (0..23).collect::<Vec<_>>());
        }

        let without_warmup: Vec<_> = batches(0, 23)
            .iter()
            .map(|batch| (batch.cell, batch.start, batch.len))
            .collect();
        assert_eq!(
            schedule
                .iter()
                .filter(|batch| !batch.warmup)
                .map(|batch| (batch.cell, batch.start, batch.len))
                .collect::<Vec<_>>(),
            without_warmup
        );

        for cell in 0..CELLS {
            let positions: BTreeSet<_> = batches(0, 80)
                .chunks(CELLS)
                .map(|cycle| cycle.iter().position(|batch| batch.cell == cell).unwrap())
                .collect();
            assert_eq!(positions.len(), CELLS);
        }
    }

    #[test]
    fn coverage_and_cpu_populations_include_failed_requests() {
        let raw = vec![
            json!({"warmup":true,"run":99,"user":9,"oracle_passed":false}),
            json!({"warmup":false,"run":1,"user":1,"oracle_passed":false}),
            json!({"warmup":false,"run":2,"user":2,"oracle_passed":true}),
        ];
        let covered = coverage(&raw, false);
        assert_eq!(covered["requests"], 2);
        assert_eq!(covered["distinct_runs"], 2);
        assert_eq!(covered["user_requests"]["1"], 1);

        let batch = batches(0, 2)[0];
        let before = CpuObservation::from_probe(Ok(json!({"cpu.stat":"usage_usec 100\n"})), 1.0);
        let after = CpuObservation::from_probe(Ok(json!({"cpu.stat":"usage_usec 300\n"})), 1.0);
        let report = cpu_report(vec![cpu_batch(batch, before, after)], true);
        assert_eq!(report["measured"]["requests_including_failures"], 2);
        assert_eq!(report["measured"]["cpu_seconds_per_request"], 0.0001);

        let failed = cpu_batch(
            batch,
            CpuObservation::unavailable("unavailable_native"),
            CpuObservation::unavailable("unavailable_native"),
        );
        let native = cpu_report(vec![failed], true);
        assert_eq!(native["status"], "unavailable_native");
        assert!(native["measured"]["cpu_seconds_per_request"].is_null());
    }

    #[test]
    fn cpu_counter_deltas_and_failed_observations_never_invent_zero() {
        let observation = |text| CpuObservation::from_probe(Ok(json!({"cpu.stat":text})), 1.0);
        let batch = batches(0, 1)[0];
        let good = cpu_batch(
            batch,
            observation("usage_usec 100\nuser_usec 80\n"),
            observation("usage_usec 140\nuser_usec 110\n"),
        );
        assert_eq!(good["delta"]["usage_usec"], 40);
        assert_eq!(good["delta"]["user_usec"], 30);

        for bad in [
            observation("usage_usec nope\n"),
            observation("user_usec 1\n"),
            CpuObservation::from_probe(Err("observer failed".into()), 2.0),
        ] {
            let failed = cpu_batch(batch, observation("usage_usec 100\n"), bad);
            assert_eq!(failed["status"], "failed");
            assert!(failed["delta"].is_null());
            assert!(cpu_report(vec![failed], true)["measured_usage_usec_delta"].is_null());
        }

        assert!(
            cpu_batch(
                batch,
                observation("usage_usec 100\n"),
                observation("usage_usec 99\n")
            )["delta"]
                .is_null()
        );

        assert!(parse_cpu("usage_usec 1\nusage_usec 2\n").is_err());
        assert!(parse_cpu("usage_usec 1 trailing\n").is_err());

        assert_eq!(
            cpu_report(vec![good], true)["measured_usage_usec_delta"],
            40
        );
    }
}
