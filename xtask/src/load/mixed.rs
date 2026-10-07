use super::{
    engine::{self, Accounting, Completion, Schedule, Work},
    http::{Http, metrics},
    semantics::{self, DAY_MS},
    tail,
};
use crate::Result;
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::watch;

#[derive(clap::Args, Clone, Debug)]
pub struct Args {
    #[arg(long, default_value = "http://127.0.0.1:3100")]
    pub url: String,
    #[arg(long, default_value_t = 10000)]
    pub seed_events: u64,
    #[arg(long, default_value_t = 7.0)]
    pub history_days: f64,
    #[arg(long, default_value_t = 0.0)]
    pub seed_pause_ms: f64,
    #[arg(long, default_value_t = 30.0)]
    pub duration: f64,
    #[arg(long, default_value_t = 1000.0)]
    pub rate: f64,
    #[arg(long, default_value_t = 100)]
    pub batch_size: u64,
    #[arg(long, default_value_t = 4)]
    pub ingest_clients: usize,
    #[arg(long, default_value_t = 4)]
    pub query_clients: usize,
    #[arg(long, default_value_t = 20.0)]
    pub query_rate: f64,
    #[arg(long, default_value_t = 64)]
    pub client_queue_capacity: usize,
    #[arg(long,default_value="json",value_parser=["json","ndjson","chunked","mixed"])]
    pub request_shape: String,
    #[arg(long, default_value_t = 0)]
    pub body_bytes: usize,
    #[arg(long, default_value_t = 820)]
    pub event_padding_bytes: usize,
    #[arg(long, default_value_t = 0.05)]
    pub late_fraction: f64,
    #[arg(long, default_value_t = 3600000)]
    pub late_delay_ms: i64,
    #[arg(long, default_value_t = 0.0)]
    pub burst_rate: f64,
    #[arg(long, default_value_t = 10.0)]
    pub burst_start: f64,
    #[arg(long, default_value_t = 5.0)]
    pub burst_duration: f64,
    #[arg(long, default_value_t = 0)]
    pub tail_subscribers: usize,
    #[arg(long, default_value_t = 2.0)]
    pub tail_drain_seconds: f64,
    #[arg(long, default_value_t = 10.0)]
    pub metrics_interval: f64,
    #[arg(long)]
    pub server_config: Option<PathBuf>,
    #[arg(long, default_value = "unspecified")]
    pub server_resources: String,
    #[arg(long)]
    pub retention_ms: Option<i64>,
    #[arg(long, default_value_t = 0)]
    pub require_min_archives: u64,
    #[arg(long)]
    pub require_qualified: bool,
    #[arg(long, default_value = "artifacts/benchmark.json")]
    pub output: PathBuf,
}
impl Default for Args {
    fn default() -> Self {
        use clap::Parser;
        #[derive(clap::Parser)]
        struct Defaults {
            #[command(flatten)]
            args: Args,
        }
        Defaults::parse_from(["mixed-load"]).args
    }
}
impl Args {
    fn validate(&self) -> Result<()> {
        if self.duration > 86400.0
            || self.history_days > 36500.0
            || self.seed_events > 1_000_000_000
            || self.rate > 1_000_000.0
            || self.query_rate > 100000.0
            || self.client_queue_capacity > 65536
            || self.event_padding_bytes > 4096
            || self.body_bytes > 64 * 1024 * 1024
            || self.metrics_interval > 86400.0
            || [
                self.burst_start,
                self.burst_duration,
                self.tail_drain_seconds,
                self.seed_pause_ms,
            ]
            .iter()
            .any(|v| *v > 86400.0)
            || self.burst_rate > 1_000_000.0
            || !self.duration.is_finite()
            || self.duration <= 0.0
            || !self.rate.is_finite()
            || self.rate <= 0.0
            || !self.history_days.is_finite()
            || self.history_days <= 0.0
            || !(1..=1000).contains(&self.batch_size)
            || !(1..=32).contains(&self.ingest_clients)
            || self.query_clients > 32
            || !self.query_rate.is_finite()
            || self.query_rate < 0.0
            || self.client_queue_capacity < 1
            || self.tail_subscribers > 8
            || !(0.0..=1.0).contains(&self.late_fraction)
            || !(0..=36500 * DAY_MS).contains(&self.late_delay_ms)
            || !self.metrics_interval.is_finite()
            || self.metrics_interval <= 0.0
            || [
                self.burst_rate,
                self.burst_start,
                self.burst_duration,
                self.tail_drain_seconds,
                self.seed_pause_ms,
            ]
            .iter()
            .any(|v| !v.is_finite() || *v < 0.0)
            || self.retention_ms.is_some_and(|v| v <= 0)
            || !["json", "ndjson", "chunked", "mixed"].contains(&self.request_shape.as_str())
        {
            return Err("invalid workload limits".into());
        }
        Ok(())
    }
}

pub struct Seed {
    pub marker: String,
    pub seeded: u64,
    pub start_ms: i64,
    pub accepted_seconds: BTreeMap<i64, u64>,
    pub seed_latency: Value,
    pub seed_requests: u64,
}
pub fn make_events(
    args: &Args,
    marker: &str,
    start: u64,
    count: u64,
    history: bool,
    now: i64,
) -> Vec<Value> {
    (start..start + count).map(|index| {
        let late = index * 997 % 10000 < (args.late_fraction * 10000.0).round() as u64;
        let age = if history {
            // Wide multiplication keeps large historical seeds from overflowing
            // before the division brings the age back within the history span.
            (index as i128 * (args.history_days * DAY_MS as f64) as i128
                / args.seed_events.max(1) as i128) as i64
        } else if late {
            args.late_delay_ms
        } else {
            0
        };

        json!({
            "time": now - age,
            "level": if index % 29 == 0 {50} else {30},
            "service": if index % 10 < 8 {"checkout"} else {"worker"},
            "name": "benchmark",
            "hostname": format!("host-{}", index % 8),
            "msg": format!("{marker} common event {}{index}", if index % 5000 == 0 {"rare-match "} else {""}),
            "payload": "x".repeat(args.event_padding_bytes),
            "nested": {"request": index, "late": late && !history, "benchmark_run": marker},
        })
    }).collect()
}

pub async fn seed(args: &Args, marker: &str, read: &str, write: &str) -> Result<Seed> {
    args.validate()?;
    let http = Http::new(&args.url, read, write)?;
    let mut seed = Seed {
        marker: marker.into(),
        seeded: 0,
        start_ms: crate::now_ms(),
        accepted_seconds: BTreeMap::new(),
        seed_latency: Value::Null,
        seed_requests: 0,
    };
    let mut seed_latency = semantics::Samples::default();
    if args.body_bytes > 0 {
        for shape in ["json", "ndjson"] {
            let mut events = make_events(
                args,
                marker,
                args.seed_events,
                args.batch_size,
                false,
                seed.start_ms,
            );
            semantics::encode_events(&mut events, shape, args.body_bytes)?;
        }
    }

    while seed.seeded < args.seed_events {
        let count = args.batch_size.min(args.seed_events - seed.seeded);
        let mut events = make_events(args, marker, seed.seeded, count, true, seed.start_ms);
        let (body, kind) = semantics::encode_events(&mut events, "json", args.body_bytes)?;
        let started = tokio::time::Instant::now();
        let (status, data) = http
            .request("/logs/ingest", Some((body, kind, false)), false)
            .await;
        seed_latency.add(started.elapsed().as_secs_f64() * 1000.0);
        seed.seed_requests += 1;
        if status != 200 || data["accepted"].as_u64() != Some(count) {
            return Err(format!("seed ingest failed or ambiguous: {status} {data}").into());
        }
        for event in events {
            *seed
                .accepted_seconds
                .entry(event["time"].as_i64().expect("event time").div_euclid(1000))
                .or_default() += 1;
        }
        seed.seeded += count;
        tokio::time::sleep(Duration::from_secs_f64(args.seed_pause_ms / 1000.0)).await;
    }
    seed.seed_latency = seed_latency.report();
    Ok(seed)
}

pub async fn report(args: &Args, read: &str, write: &str) -> Result<Value> {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let marker = format!(
        "bench-{}-{}-{}",
        crate::now_ms(),
        std::process::id(),
        SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let seeded = seed(args, &marker, read, write).await?;
    report_seeded(args, read, write, seeded).await
}

fn provenance(args: &Args) -> Result<Value> {
    let config = if let Some(path) = &args.server_config {
        let text = std::fs::read_to_string(path)?;
        if path.extension().is_some_and(|s| s == "toml") {
            serde_json::to_value(toml::from_str::<toml::Value>(&text)?)?
        } else {
            serde_json::from_str(&text)?
        }
    } else {
        json!({})
    };
    let mut limits = json!({});
    for key in [
        "retention_days",
        "max_size_gb",
        "max_indexes",
        "archive_after_ms",
        "retention_ms",
        "maintenance_interval_secs",
        "max_tail_buffer_bytes",
        "max_tail_subscribers",
        "tail_poll_ms",
        "max_inflight_ingest",
        "max_inflight_requests",
        "max_body_bytes",
        "max_decoded_bytes",
    ] {
        if let Some(value) = config.get(key) {
            limits[key] = value.clone();
        }
    }
    limits["storage"] = json!({});
    for key in [
        "max_size_bytes",
        "reader_threads",
        "duckdb_threads",
        "memory_limit",
        "temp_limit",
        "queue_capacity",
        "reader_queue_capacity",
        "query_timeout_ms",
        "max_result_bytes",
        "max_query_bytes",
        "max_archive_rows",
        "archive_batch_rows",
        "archive_partition_ms",
        "queue_bytes",
    ] {
        if let Some(value) = config["storage"].get(key) {
            limits["storage"][key] = value.clone();
        }
    }
    if let Some(retention) = args.retention_ms {
        limits["retention_ms"] = json!(retention);
    }
    Ok(
        json!({"config_file":args.server_config,"limits":limits,"server_resources":args.server_resources,"verified_by_client":false}),
    )
}
async fn sample(http: &Http) -> Value {
    // Metrics intervals can exceed the server idle-header deadline; scrapes use
    // fresh read-only connections and never share the ingest pool.
    let fresh = Http::new(&http.base, &http.read, &http.write);
    let (status, text) = match fresh {
        Ok(fresh) => fresh.request("/metrics", None, true).await,
        Err(error) => (0, json!({"error":error.to_string()})),
    };
    json!({"wall_time_ms":crate::now_ms(),"status":status,"metrics":if status==200{metrics(&text)}else{BTreeMap::new()}})
}

fn check_tails(accounting: &mut Accounting, tails: &[Value]) {
    for tail in tails {
        for (field, problem) in [
            ("monotonic", "tail IDs not strictly increasing"),
            ("filter_correct", "tail filter mismatch"),
        ] {
            if tail[field] == false {
                accounting.problems.push(problem.into());
                *accounting
                    .counts
                    .entry("correctness_failures".into())
                    .or_default() += 1;
            }
        }
    }
}

pub async fn report_seeded(args: &Args, read: &str, write: &str, seed: Seed) -> Result<Value> {
    args.validate()?;
    let http = Http::new(&args.url, read, write)?;
    let provenance = provenance(args)?;
    let first = sample(&http).await;
    let archives = first["metrics"]["logbrook_storage_archive_files"]
        .as_f64()
        .or_else(|| first["metrics"]["logbrook_archive_files"].as_f64());
    if args.require_min_archives > 0
        && archives.is_none_or(|n| n < args.require_min_archives as f64)
    {
        return Err(format!("qualification requires {} archives; metrics report {archives:?}. Prepare a steady-state fixture first",args.require_min_archives).into());
    }
    let (stop, receiver) = watch::channel(false);
    let mut tails = Vec::new();
    for n in 0..args.tail_subscribers {
        let params = BTreeMap::from([
            ("message".into(), seed.marker.clone()),
            (
                "service".into(),
                if n % 2 == 0 {
                    "worker".into()
                } else {
                    "checkout".into()
                },
            ),
            ("min_level".into(), "30".into()),
        ]);
        tails.push(tail::spawn(
            http.clone(),
            "/logs/tail".into(),
            params,
            n,
            None,
            receiver.clone(),
        ));
    }
    let metrics_http = http.clone();
    let interval = args.metrics_interval;
    let mut metrics_stop = receiver.clone();
    let observer = tokio::spawn(async move {
        let mut samples = Vec::new();
        loop {
            tokio::select! {
                biased;
                _ = metrics_stop.changed() => break,
                _ = tokio::time::sleep(Duration::from_secs_f64(interval)) => samples.push(sample(&metrics_http).await),
            }
        }
        samples
    });
    let options = args.clone();
    let marker = seed.marker.clone();
    let seeded = seed.seeded;
    let work: Work = Arc::new(move |job| {
        let http = http.clone();
        let args = options.clone();
        let marker = marker.clone();
        Box::pin(async move {
            if job.queue == 0 {
                let mut events = make_events(
                    &args,
                    &marker,
                    seeded + job.sequence * args.batch_size,
                    job.count,
                    false,
                    crate::now_ms(),
                );
                let shape = if args.request_shape == "mixed" {
                    ["json", "ndjson", "chunked"][job.sequence as usize % 3]
                } else {
                    args.request_shape.as_str()
                };
                let (body, kind) = semantics::encode_events(
                    &mut events,
                    if shape == "ndjson" { "ndjson" } else { "json" },
                    args.body_bytes,
                )?;
                let bytes = body.len();
                let (status, data) = http
                    .request(
                        "/logs/ingest",
                        Some((body, kind, shape == "chunked")),
                        false,
                    )
                    .await;
                let mut c = Completion::new("ingest", status);
                c.counters.insert("sent_ingest".into(), job.count);
                c.counters.insert("sent_body_bytes".into(), bytes as u64);
                c.counters.insert(format!("shape_{shape}_requests"), 1);
                if status == 200 && data["accepted"].as_u64() == Some(job.count) {
                    c.counters.insert("accepted_ingest".into(), job.count);
                    c.counters.insert(
                        "late_events_accepted".into(),
                        events
                            .iter()
                            .filter(|e| e["nested"]["late"] == true)
                            .count() as u64,
                    );
                    for e in events {
                        *c.accepted_seconds
                            .entry(e["time"].as_i64().expect("event time").div_euclid(1000))
                            .or_default() += 1;
                    }
                } else if status == 0 {
                    c.counters.insert("ambiguous_ingest".into(), job.count);
                } else {
                    c.counters.insert("rejected_ingest".into(), job.count);
                    if status == 200 {
                        c.problem =
                            Some("ingest accepted count does not match submitted batch".into());
                    }
                }
                Ok(c)
            } else {
                let (operation, path, params) = semantics::query_shape(
                    job.sequence,
                    &marker,
                    args.history_days,
                    crate::now_ms() + 1,
                );
                let (status, data) = http.request(&path, None, false).await;
                let mut c = Completion::new(&operation, status);
                c.counters.insert("sent_query".into(), 1);
                c.counters
                    .insert("query_errors".into(), u64::from(status != 200));
                if status == 200 {
                    c.problem = semantics::validate_query(
                        path.split('?').next().expect("query path"),
                        &params,
                        &data,
                    );
                }
                Ok(c)
            }
        })
    });
    let mut schedules = vec![Schedule {
        duration: args.duration,
        rate: args.rate,
        batch: args.batch_size,
        burst_rate: args.burst_rate,
        burst_start: args.burst_start,
        burst_duration: args.burst_duration,
        queue: 0,
        total: None,
    }];
    if args.query_clients > 0 && args.query_rate > 0.0 {
        schedules.push(Schedule {
            duration: args.duration,
            rate: args.query_rate,
            batch: 1,
            burst_rate: 0.0,
            burst_start: 0.0,
            burst_duration: 0.0,
            queue: 1,
            total: None,
        });
    }
    let (mut accounting, elapsed, drain) = engine::execute(
        schedules,
        vec![args.ingest_clients, args.query_clients],
        vec![args.client_queue_capacity; 2],
        vec!["ingest".into(), "query".into()],
        work,
        Duration::from_secs(30),
    )
    .await;
    for name in [
        "offered_ingest",
        "offered_query",
        "accepted_ingest",
        "sent_ingest",
        "sent_query",
        "client_dropped_ingest",
        "client_dropped_query",
        "rejected_ingest",
        "ambiguous_ingest",
        "query_errors",
        "correctness_failures",
    ] {
        accounting.counts.entry(name.into()).or_default();
    }

    tokio::time::sleep(Duration::from_secs_f64(args.tail_drain_seconds)).await;
    let _ = stop.send(true);
    let tails = tail::finish(tails).await;
    check_tails(&mut accounting, &tails);
    let mut samples = vec![first.clone()];
    match observer.await {
        Ok(more) => samples.extend(more),
        Err(error) => accounting
            .problems
            .push(format!("metrics worker failed: {error}")),
    }
    let http = Http::new(&args.url, read, write)?;
    let before = sample(&http).await;
    samples.push(before.clone());
    let final_time = crate::now_ms() + 1;
    let params = BTreeMap::from([
        (
            "from".into(),
            (seed.start_ms
                - (args.history_days * DAY_MS as f64) as i64
                - args.late_delay_ms
                - 1000)
                .to_string(),
        ),
        ("to".into(), final_time.to_string()),
        ("message".into(), seed.marker.clone()),
    ]);
    let (code, count) = http
        .request(&super::http::path("/logs/count", &params), None, false)
        .await;
    let after = sample(&http).await;
    samples.push(after.clone());
    for (second, count) in seed.accepted_seconds {
        *accounting.seconds.entry(second).or_default() += count;
    }
    let expected = semantics::final_visibility(
        &accounting.seconds,
        seed.seeded + accounting.count("accepted_ingest"),
        accounting.count("ambiguous_ingest"),
        before["metrics"]["logbrook_storage_retention_before_ms"]
            .as_f64()
            .map(|n| n as i64),
        after["metrics"]["logbrook_storage_retention_before_ms"]
            .as_f64()
            .map(|n| n as i64),
        provenance["limits"]["retention_ms"].as_i64(),
        final_time,
        if code == 200 {
            count["count"].as_u64()
        } else {
            None
        },
    );
    if code == 200
        && accounting.count("ambiguous_ingest") == 0
        && expected["observed_within_bounds"] != true
    {
        accounting.problems.push(format!(
            "final count {count} outside accepted visibility bounds {expected}"
        ));
    }
    let telemetry = samples.iter().all(|s| s["status"] == 200);
    let passed = telemetry
        && accounting.problems.is_empty()
        && accounting.worker_errors.is_empty()
        && [
            "client_dropped_ingest",
            "client_dropped_query",
            "rejected_ingest",
            "ambiguous_ingest",
            "query_errors",
        ]
        .iter()
        .all(|k| accounting.count(k) == 0)
        && tails.iter().all(|t| {
            t["status"] == 200
                && t["errors"] == 0
                && t["gaps"] == 0
                && t["monotonic"] == true
                && t["filter_correct"] == true
        })
        && expected["correctness_verified"] == true
        && code == 200;
    let latency = |position: usize| {
        let mut reports = accounting
            .latency
            .iter()
            .map(|(name, samples)| (name.clone(), samples[position].report()))
            .collect::<BTreeMap<_, _>>();
        if seed.seeded > 0 {
            reports.insert(
                "seed".into(),
                if position == 2 {
                    let mut zero = semantics::Samples::default();
                    for _ in 0..seed.seed_requests {
                        zero.add(0.0);
                    }
                    zero.report()
                } else {
                    seed.seed_latency.clone()
                },
            );
        }
        reports
    };
    if seed.seeded > 0 {
        accounting
            .statuses
            .insert("seed:200".into(), seed.seed_requests);
    }
    Ok(json!({
        "client_platform":std::env::consts::OS,
        "server_url":args.url,
        "run_marker":seed.marker,
        "server_provenance":provenance,
        "seeded_events":seed.seeded,
        "seed_history_days":args.history_days,
        "target_rate":args.rate,
        "duration_seconds":elapsed,
        "offered_horizon_seconds":args.duration,
        "drain_seconds":drain,
        "query_clients":args.query_clients,
        "query_rate":args.query_rate,
        "ingest_clients":args.ingest_clients,
        "batch_size":args.batch_size,
        "request_shape":args.request_shape,
        "body_target_bytes":args.body_bytes,
        "client_queue_capacity_per_kind":args.client_queue_capacity,
        "burst":{"rate":args.burst_rate,"start":args.burst_start,"duration":args.burst_duration},
        "late_arrivals":{"fraction":args.late_fraction,"delay_ms":args.late_delay_ms},
        "offered_events":accounting.count("offered_ingest"),
        "accepted_events":accounting.count("accepted_ingest"),
        "accepted_events_per_second":accounting.count("accepted_ingest") as f64/elapsed.max(0.001),
        "accounting":accounting.counts,
        "matching_events_after":if code==200{Some(count)}else{None},
        "final_count_status":code,
        "expected_visibility":expected,
        "http_status_counts":accounting.statuses,
        "latency_ms":latency(0),
        "service_latency_ms":latency(1),
        "client_queue_latency_ms":latency(2),
        "scheduler_lag_ms":accounting.lag.iter().map(|(name,samples)|(name.clone(),samples.report())).collect::<BTreeMap<_,
        _>>(),
        "tails":tails,
        "metrics_samples":samples,
        "archive_files_before":archives,
        "metrics_before":first,
        "metrics_before_final_count":before,
        "metrics_after":after,
        "correctness_problems":accounting.problems,
        "worker_errors":accounting.worker_errors,
        "qualification":{"telemetry_complete":telemetry,"passed":passed,"scope":"this offered workload and observed fixture only; accepted count verified within committed cutoff bounds, not a sustained capacity claim"},
        "limits":"Bounded queues report drops. No ingestion retries. Scheduled, service and queue latencies are separate. Unique marker isolates checks. Tail counts are observed, not a delivery proof. Operator-supplied resources and limits are unverified."
    }))
}

pub async fn run(args: Args) -> Result<()> {
    let read = std::env::var("LOGBROOK_READ_TOKEN")?;
    let write = std::env::var("LOGBROOK_INGEST_TOKEN")?;
    let report = report(&args, &read, &write).await?;
    super::write_report(&args.output, &report)?;
    if !report["correctness_problems"]
        .as_array()
        .is_some_and(Vec::is_empty)
        || !report["worker_errors"]
            .as_array()
            .is_some_and(Vec::is_empty)
        || (args.require_qualified && report["qualification"]["passed"] != true)
    {
        return Err("mixed workload failed qualification".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tail_correctness_failures_fail_the_default_cli_contract() {
        let mut stats = Accounting::default();
        check_tails(
            &mut stats,
            &[json!({"monotonic":false,"filter_correct":false})],
        );
        assert_eq!(
            stats.problems,
            vec!["tail IDs not strictly increasing", "tail filter mismatch"]
        );
        assert_eq!(stats.count("correctness_failures"), 2);
        // run() fails on correctness_problems without requiring --require-qualified.
        assert!(!stats.problems.is_empty());
    }

    #[test]
    fn live_time_and_late_delay() {
        let args = Args {
            late_fraction: 1.0,
            late_delay_ms: 30000,
            ..Args::default()
        };
        let first = make_events(&args, "run", 0, 2, false, 100000);
        let next = make_events(&args, "run", 2, 2, false, 120000);
        assert_eq!(first[0]["time"], 70000);
        assert_eq!(next[0]["time"], 90000);
        assert!(
            first
                .iter()
                .chain(&next)
                .all(|e| e["nested"]["late"] == true)
        );
    }
    #[test]
    fn exact_padding_distributed_bounded() {
        let args = Args::default();
        for count in [100, 1000] {
            for shape in ["json", "ndjson"] {
                let mut events =
                    make_events(&args, "bench-123456789abc", 10000, count, false, 100000);
                let (body, _) = semantics::encode_events(&mut events, shape, 1048576).unwrap();
                assert_eq!(body.len(), 1048576);
                for event in events {
                    for (key, value) in event.as_object().unwrap() {
                        if key == "payload" || key.starts_with("benchmark_padding_") {
                            assert!(value.as_str().unwrap().len() <= 4096);
                        }
                    }
                    assert!(serde_json::to_vec(&event).unwrap().len() < 13000);
                }
            }
        }
        let mut events = make_events(&args, "run", 0, 1, false, 100000);
        assert!(
            semantics::encode_events(&mut events, "json", 1048576)
                .unwrap_err()
                .to_string()
                .contains("increase --batch-size")
        );
        let mut events = make_events(&args, "run", 0, 100, false, 100000);
        assert!(
            semantics::encode_events(&mut events, "json", 10)
                .unwrap_err()
                .to_string()
                .contains("metadata")
        );
    }
}
