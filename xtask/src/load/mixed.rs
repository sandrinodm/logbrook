//! The mixed workload: a seeded store, concurrent ingest and query traffic, and checks on the
//! results and the server's telemetry.
use super::{
    engine::{self, Accounting, Completion, Job, Schedule, Work},
    http::{Http, metrics},
    semantics::{self, ACCEPTED_COUNT_MISMATCH, DAY_MS, HTTP_OK, INGEST_OPERATION},
    tail,
};
use crate::Result;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle, time::sleep};

/// Milliseconds per second, for bucketing integer event times by second.
const MILLIS_PER_SEC: i64 = 1000;

/// The count query starts this long before the seeded history, to include its first events.
const FROM_MARGIN_MS: i64 = 1000;

/// Milliseconds per second, for floating-point conversions.
const MILLIS_PER_SEC_F64: f64 = 1000.0;

/// Shortest elapsed time used as a divisor, so a near-zero run cannot divide by zero.
const MIN_ELAPSED_SECS: f64 = 0.001;

/// Longest wait for the tail subscribers to report that they are subscribed.
const TAIL_READY_TIMEOUT: Duration = Duration::from_secs(15);

/// Longest wait for the engine's in-flight requests to finish after the schedule ends.
const ENGINE_DRAIN: Duration = Duration::from_secs(30);

// Engine queue indices for ingest batches and queries.
const INGEST_QUEUE: usize = 0;
const QUERY_QUEUE: usize = 1;

/// The `--request-shape` values accepted.
const REQUEST_SHAPES: [&str; 4] = ["json", "ndjson", "chunked", "mixed"];

/// The shapes that a `mixed` request shape rotates through.
const MIXED_SHAPES: [&str; 3] = ["json", "ndjson", "chunked"];

/// Top-level server config keys copied into the report's provenance, when the config sets them.
const TOP_LEVEL_LIMITS: [&str; 13] = [
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
];

/// `[storage]` server config keys copied into the report's provenance, when the config sets them.
const STORAGE_LIMITS: [&str; 14] = [
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
];

/// Largest value accepted for `--duration`, `--metrics-interval`, `--burst-start`,
/// `--burst-duration` and `--tail-drain-seconds`: one day in seconds.
const MAX_SECONDS: f64 = 86_400.0;

/// Largest `--seed-pause-ms` accepted: the same one day, in milliseconds.
const MAX_SEED_PAUSE_MS: f64 = MAX_SECONDS * MILLIS_PER_SEC_F64;

/// Largest `--history-days` accepted, about a century.
const MAX_HISTORY_DAYS: f64 = 36_500.0;

/// Largest `--seed-events` accepted.
const MAX_SEED_EVENTS: u64 = 1_000_000_000;

/// Largest `--rate` and `--burst-rate` accepted, in events per second.
const MAX_RATE: f64 = 1_000_000.0;

/// Largest `--query-rate` accepted, in queries per second.
const MAX_QUERY_RATE: f64 = 100_000.0;

/// Largest `--client-queue-capacity` accepted, in batches per engine queue.
const MAX_QUEUE_CAPACITY: usize = 65_536;

/// Largest `--event-padding-bytes` accepted, per event.
const MAX_EVENT_PADDING_BYTES: usize = 4096;

/// Largest `--body-bytes` accepted, the target size of each request body.
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

/// Largest `--batch-size` accepted, in events per request.
const MAX_BATCH_SIZE: u64 = 1000;

/// Largest `--ingest-clients` and `--query-clients` accepted.
const MAX_CLIENTS: usize = 32;

/// Largest `--tail-subscribers` accepted.
const MAX_TAIL_SUBSCRIBERS: usize = 8;

/// Largest `--late-delay-ms` accepted, the same century as `MAX_HISTORY_DAYS`.
const MAX_LATE_DELAY_MS: i64 = 36_500 * DAY_MS;

/// Events whose index is a multiple of this carry the rare-match text in their message.
const RARE_MATCH_EVERY: u64 = 5000;

/// Events whose index is a multiple of this are at error level.
const ERROR_LEVEL_EVERY: u64 = 29;

/// Error level in the pino numbering.
const ERROR_LEVEL: i64 = 50;

/// Info level in the pino numbering, used for every event that is not at error level.
const INFO_LEVEL: i64 = 30;

/// Out of every `SERVICE_CYCLE` events, the first `CHECKOUT_PER_CYCLE` are from checkout.
const SERVICE_CYCLE: u64 = 10;

/// Checkout events in each `SERVICE_CYCLE`; the rest are from the worker service.
const CHECKOUT_PER_CYCLE: u64 = 8;

/// Events rotate through this many `host-N` hostnames by index.
const HOSTS: u64 = 8;

/// Late events are chosen by scattering the index: `index * LATE_SCATTER % LATE_SCATTER_MODULUS`
/// falls below the late fraction of the modulus.
const LATE_SCATTER: u64 = 997;

/// Modulus of the late-event scatter, so `--late-fraction` takes effect in steps of 1/10,000.
const LATE_SCATTER_MODULUS: u64 = 10_000;

// Options for `mixed-load`: seeded history, then timed ingest and query traffic.
// Clap uses `///` docs as help text, so this stays a plain comment to keep `--help` unchanged.
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
    #[arg(long, default_value = "json", value_parser = ["json", "ndjson", "chunked", "mixed"])]
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
    /// Checks the options against their bounds. Any failing group fails the run with one error.
    fn validate(&self) -> Result<()> {
        if self.upper_bounds_exceeded() || self.ranges_invalid() || self.options_invalid() {
            return Err("invalid workload limits".into());
        }

        Ok(())
    }

    /// Durations, rates and sizes that are above their maximum.
    fn upper_bounds_exceeded(&self) -> bool {
        self.duration > MAX_SECONDS
            || self.history_days > MAX_HISTORY_DAYS
            || self.seed_events > MAX_SEED_EVENTS
            || self.rate > MAX_RATE
            || self.query_rate > MAX_QUERY_RATE
            || self.client_queue_capacity > MAX_QUEUE_CAPACITY
            || self.event_padding_bytes > MAX_EVENT_PADDING_BYTES
            || self.body_bytes > MAX_BODY_BYTES
            || self.metrics_interval > MAX_SECONDS
            || [
                self.burst_start,
                self.burst_duration,
                self.tail_drain_seconds,
            ]
            .iter()
            .any(|v| *v > MAX_SECONDS)
            || self.seed_pause_ms > MAX_SEED_PAUSE_MS
            || self.burst_rate > MAX_RATE
    }

    /// Finite, positive and in-range values, and the counts and fractions with their bounds.
    fn ranges_invalid(&self) -> bool {
        !self.duration.is_finite()
            || self.duration <= 0.0
            || !self.rate.is_finite()
            || self.rate <= 0.0
            || !self.history_days.is_finite()
            || self.history_days <= 0.0
            || !(1..=MAX_BATCH_SIZE).contains(&self.batch_size)
            || !(1..=MAX_CLIENTS).contains(&self.ingest_clients)
            || self.query_clients > MAX_CLIENTS
            || !self.query_rate.is_finite()
            || self.query_rate < 0.0
            || self.client_queue_capacity < 1
            || self.tail_subscribers > MAX_TAIL_SUBSCRIBERS
            || !(0.0..=1.0).contains(&self.late_fraction)
            || !(0..=MAX_LATE_DELAY_MS).contains(&self.late_delay_ms)
    }

    /// The metrics interval, the timing options that cannot be negative, the retention and the
    /// request shape.
    fn options_invalid(&self) -> bool {
        !self.metrics_interval.is_finite()
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
            || !REQUEST_SHAPES.contains(&self.request_shape.as_str())
    }
}

/// The state of a seeded run: its marker, how many events it wrote, and how the seed traffic
/// performed.
pub struct Seed {
    pub marker: String,
    pub seeded: u64,
    pub start_ms: i64,
    pub accepted_seconds: BTreeMap<i64, u64>,
    pub seed_latency: Value,
    pub seed_requests: u64,
}

/// Builds `count` events from `start`, timed at `now` or, for history, spread back over the
/// history span. `history` selects that spread; it is `false` for live traffic.
pub fn make_events(
    args: &Args,
    marker: &str,
    start: u64,
    count: u64,
    history: bool,
    now: i64,
) -> Vec<Value> {
    (start..start + count)
        .map(|index| make_event(args, marker, index, history, now))
        .collect()
}

/// One benchmark event with the given index.
fn make_event(args: &Args, marker: &str, index: u64, history: bool, now: i64) -> Value {
    let late = index * LATE_SCATTER % LATE_SCATTER_MODULUS
        < (args.late_fraction * LATE_SCATTER_MODULUS as f64).round() as u64;
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

    let level = if index.is_multiple_of(ERROR_LEVEL_EVERY) {
        ERROR_LEVEL
    } else {
        INFO_LEVEL
    };
    let service = if index % SERVICE_CYCLE < CHECKOUT_PER_CYCLE {
        "checkout"
    } else {
        "worker"
    };
    let rare = if index.is_multiple_of(RARE_MATCH_EVERY) {
        "rare-match "
    } else {
        ""
    };

    json!({
        "time": now - age,
        "level": level,
        "service": service,
        "name": "benchmark",
        "hostname": format!("host-{}", index % HOSTS),
        "msg": format!("{marker} common event {rare}{index}"),
        "payload": "x".repeat(args.event_padding_bytes),
        "nested": {"request": index, "late": late && !history, "benchmark_run": marker},
    })
}

/// Seeds the history: ingests `seed_events` events in batches, pausing between them, and
/// returns the seed's state.
///
/// # Errors
///
/// Returns an error when the options are invalid, a batch is rejected or its outcome is
/// ambiguous, or the events cannot be encoded.
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
        prime_encodings(args, marker, seed.start_ms)?;
    }

    while seed.seeded < args.seed_events {
        ingest_seed_batch(&http, args, marker, &mut seed, &mut seed_latency).await?;
    }

    seed.seed_latency = seed_latency.report();
    Ok(seed)
}

/// Encodes one batch in each shape up front, so that an encoding that cannot fit the body
/// budget fails before any seed traffic is sent.
fn prime_encodings(args: &Args, marker: &str, start_ms: i64) -> Result<()> {
    for shape in ["json", "ndjson"] {
        let mut events = make_events(
            args,
            marker,
            args.seed_events,
            args.batch_size,
            false,
            start_ms,
        );
        semantics::encode_events(&mut events, shape, args.body_bytes)?;
    }
    Ok(())
}

/// Ingests the next seed batch, records its latency and accepted seconds, and pauses.
async fn ingest_seed_batch(
    http: &Http,
    args: &Args,
    marker: &str,
    seed: &mut Seed,
    seed_latency: &mut semantics::Samples,
) -> Result<()> {
    let count = args.batch_size.min(args.seed_events - seed.seeded);
    let mut events = make_events(args, marker, seed.seeded, count, true, seed.start_ms);
    let (body, kind) = semantics::encode_events(&mut events, "json", args.body_bytes)?;
    let started = tokio::time::Instant::now();
    let (status, data) = http
        .request("/logs/ingest", Some((body, kind, false)), false)
        .await;
    seed_latency.add(started.elapsed().as_secs_f64() * MILLIS_PER_SEC_F64);
    seed.seed_requests += 1;

    if status != HTTP_OK || data["accepted"].as_u64() != Some(count) {
        return Err(format!("seed ingest failed or ambiguous: {status} {data}").into());
    }

    for event in events {
        *seed
            .accepted_seconds
            .entry(
                event["time"]
                    .as_i64()
                    .expect("event time")
                    .div_euclid(MILLIS_PER_SEC),
            )
            .or_default() += 1;
    }

    seed.seeded += count;

    sleep(Duration::from_secs_f64(
        args.seed_pause_ms / MILLIS_PER_SEC_F64,
    ))
    .await;
    Ok(())
}

/// Seeds a fresh history under a unique marker and then runs the workload against it.
///
/// # Errors
///
/// Returns an error when seeding or the workload fails.
pub async fn report(args: &Args, read: &str, write: &str) -> Result<Value> {
    // Keeps markers unique when two reports start in the same process and millisecond.
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let marker = format!(
        "bench-{}-{}-{}",
        crate::now_ms(),
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let seeded = seed(args, &marker, read, write).await?;
    report_seeded(args, read, write, seeded).await
}

/// The server's configuration as the report records it: the limits that the report names, and
/// the retention given on the command line.
fn provenance(args: &Args) -> Result<Value> {
    let config = server_config(args)?;
    let mut limits = json!({});
    for key in TOP_LEVEL_LIMITS {
        if let Some(value) = config.get(key) {
            limits[key] = value.clone();
        }
    }

    limits["storage"] = json!({});

    for key in STORAGE_LIMITS {
        if let Some(value) = config["storage"].get(key) {
            limits["storage"][key] = value.clone();
        }
    }

    if let Some(retention) = args.retention_ms {
        limits["retention_ms"] = json!(retention);
    }

    Ok(json!({
        "config_file": args.server_config,
        "limits": limits,
        "server_resources": args.server_resources,
        "verified_by_client": false
    }))
}

/// Reads the server configuration file, as JSON or TOML, or an empty object when none is given.
fn server_config(args: &Args) -> Result<Value> {
    let Some(path) = &args.server_config else {
        return Ok(json!({}));
    };
    let text = std::fs::read_to_string(path)?;
    if path.extension().is_some_and(|s| s == "toml") {
        Ok(serde_json::to_value(toml::from_str::<toml::Value>(&text)?)?)
    } else {
        Ok(serde_json::from_str(&text)?)
    }
}

/// One telemetry sample. It uses a fresh connection, because metrics intervals can exceed the
/// server's idle-header deadline and scrapes must not share the ingest pool.
async fn sample(http: &Http) -> Value {
    let fresh = Http::new(&http.base, &http.read, &http.write);
    let (status, text) = match fresh {
        Ok(fresh) => fresh.request("/metrics", None, true).await,
        Err(error) => (0, json!({"error": error.to_string()})),
    };
    json!({
        "wall_time_ms": crate::now_ms(),
        "status": status,
        "metrics": if status == HTTP_OK { metrics(&text) } else { BTreeMap::new() }
    })
}

/// Records the tail subscribers' order and filter failures as correctness problems.
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

/// Starts the tail subscribers, waiting until each one is subscribed.
async fn spawn_tails(
    http: &Http,
    args: &Args,
    seed: &Seed,
    stop: &watch::Receiver<bool>,
) -> Result<Vec<JoinHandle<Value>>> {
    let mut tails = Vec::new();
    let mut ready = Vec::new();
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
        let (task, started) = tail::spawn_count_ready(
            http.clone(),
            "/logs/tail".into(),
            params,
            n,
            None,
            stop.clone(),
            Arc::new(AtomicU64::new(0)),
        );
        tails.push(task);
        ready.push(started);
    }

    for started in ready {
        tokio::time::timeout(TAIL_READY_TIMEOUT, started).await??;
    }

    Ok(tails)
}

/// Samples the server's telemetry every `interval` seconds until `stopped` changes.
fn spawn_observer(
    http: Http,
    interval: f64,
    mut stopped: watch::Receiver<bool>,
) -> JoinHandle<Vec<Value>> {
    tokio::spawn(async move {
        let mut samples = Vec::new();
        loop {
            tokio::select! {
                biased;
                _ = stopped.changed() => break,
                _ = sleep(Duration::from_secs_f64(interval)) => samples.push(sample(&http).await),
            }
        }
        samples
    })
}

/// The ingest and query schedules: queries run only when they are enabled.
fn schedules(args: &Args) -> Vec<Schedule> {
    let mut schedules = vec![Schedule {
        duration: args.duration,
        rate: args.rate,
        batch: args.batch_size,
        burst_rate: args.burst_rate,
        burst_start: args.burst_start,
        burst_duration: args.burst_duration,
        queue: INGEST_QUEUE,
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
            queue: QUERY_QUEUE,
            total: None,
        });
    }

    schedules
}

/// The work function for the engine: an ingest batch or a query, as the job's queue says.
fn work_for(http: Http, args: &Args, marker: &str, seeded: u64) -> Work {
    let options = args.clone();
    let marker = marker.to_owned();
    Arc::new(move |job| {
        let http = http.clone();
        let args = options.clone();
        let marker = marker.clone();
        Box::pin(async move {
            if job.queue == INGEST_QUEUE {
                ingest(&http, &args, &marker, seeded, &job).await
            } else {
                query(&http, &args, &marker, &job).await
            }
        })
    })
}

/// Sends one ingest batch in the request shape that the job asks for.
async fn ingest(
    http: &Http,
    args: &Args,
    marker: &str,
    seeded: u64,
    job: &Job,
) -> Result<Completion> {
    let mut events = make_events(
        args,
        marker,
        seeded + job.sequence * args.batch_size,
        job.count,
        false,
        crate::now_ms(),
    );

    let shape = if args.request_shape == "mixed" {
        MIXED_SHAPES[job.sequence as usize % MIXED_SHAPES.len()]
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
    let mut c = Completion::new(INGEST_OPERATION, status);
    c.counters.insert("sent_ingest".into(), job.count);
    c.counters.insert("sent_body_bytes".into(), bytes as u64);
    c.counters.insert(format!("shape_{shape}_requests"), 1);

    if status == HTTP_OK && data["accepted"].as_u64() == Some(job.count) {
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
                .entry(
                    e["time"]
                        .as_i64()
                        .expect("event time")
                        .div_euclid(MILLIS_PER_SEC),
                )
                .or_default() += 1;
        }
    } else if status == 0 {
        c.counters.insert("ambiguous_ingest".into(), job.count);
    } else {
        c.counters.insert("rejected_ingest".into(), job.count);
        if status == HTTP_OK {
            c.problem = Some(ACCEPTED_COUNT_MISMATCH.into());
        }
    }

    Ok(c)
}

/// Runs one query from the rotation and checks its response against the request.
async fn query(http: &Http, args: &Args, marker: &str, job: &Job) -> Result<Completion> {
    let (operation, path, params) =
        semantics::query_shape(job.sequence, marker, args.history_days, crate::now_ms() + 1);
    let (status, data) = http.request(&path, None, false).await;
    let mut c = Completion::new(&operation, status);
    c.counters.insert("sent_query".into(), 1);
    c.counters
        .insert("query_errors".into(), u64::from(status != HTTP_OK));

    if status == HTTP_OK {
        c.problem =
            semantics::validate_query(path.split('?').next().expect("query path"), &params, &data);
    }

    Ok(c)
}

/// What the engine run measured: its accounting, the elapsed seconds, and the drain seconds.
struct EngineRun {
    accounting: Accounting,
    elapsed: f64,
    drain: f64,
}

/// Runs the schedules through the engine.
async fn run_load(http: &Http, args: &Args, seed: &Seed) -> EngineRun {
    let (accounting, elapsed, drain) = engine::execute(
        schedules(args),
        vec![args.ingest_clients, args.query_clients],
        vec![args.client_queue_capacity; 2],
        vec!["ingest".into(), "query".into()],
        work_for(http.clone(), args, &seed.marker, seed.seeded),
        ENGINE_DRAIN,
    )
    .await;
    EngineRun {
        accounting,
        elapsed,
        drain,
    }
}

/// The final count of the seeded marker, from the start of its history to `final_time`.
async fn final_count(http: &Http, args: &Args, seed: &Seed, final_time: i64) -> (u16, Value) {
    let history_ms = (args.history_days * DAY_MS as f64) as i64;
    let params = BTreeMap::from([
        (
            "from".into(),
            (seed.start_ms - history_ms - args.late_delay_ms - FROM_MARGIN_MS).to_string(),
        ),
        ("to".into(), final_time.to_string()),
        ("message".into(), seed.marker.clone()),
    ]);
    http.request(&super::http::path("/logs/count", &params), None, false)
        .await
}

/// What the engine run measured, and what it observed around the run.
struct Outcome {
    accounting: Accounting,
    elapsed: f64,
    drain: f64,
    tails: Vec<Value>,
    samples: Vec<Value>,
    first: Value,
    before: Value,
    after: Value,
    provenance: Value,
    archives: Option<f64>,
    expected: Value,
    code: u16,
    count: Value,
    telemetry: bool,
    passed: bool,
}

/// The latency reports at one position (0 scheduled, 1 service, 2 client queue) for each
/// operation, plus the seed's own latency.
fn latency_reports(
    accounting: &Accounting,
    seed: &Seed,
    position: usize,
) -> BTreeMap<String, Value> {
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
}

/// Whether the tail subscribers saw every event in order, with the right filter, and no gaps.
fn tails_passed(tails: &[Value]) -> bool {
    tails.iter().all(|t| {
        t["status"] == HTTP_OK
            && t["errors"] == 0
            && t["gaps"] == 0
            && t["monotonic"] == true
            && t["filter_correct"] == true
    })
}

/// Gives every counter that the report names a zero entry, so that it appears in the report even
/// when no event reached it.
fn register_counts(accounting: &mut Accounting) {
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
}

/// What the run observed once the store had settled: the telemetry around the final count, the
/// count itself, and the visibility bounds the accepted events must fall within.
struct Settled {
    before: Value,
    after: Value,
    code: u16,
    count: Value,
    expected: Value,
}

/// Samples the telemetry, runs the final count, and checks it against the visibility bounds.
async fn settle(
    http: &Http,
    args: &Args,
    seed: &Seed,
    provenance: &Value,
    accounting: &mut Accounting,
) -> Settled {
    let before = sample(http).await;
    let final_time = crate::now_ms() + 1;
    let (code, count) = final_count(http, args, seed, final_time).await;
    let after = sample(http).await;
    for (second, count) in &seed.accepted_seconds {
        *accounting.seconds.entry(*second).or_default() += count;
    }

    let expected = semantics::final_visibility(
        &accounting.seconds,
        semantics::VisibilityObservations {
            acknowledged: seed.seeded + accounting.count("accepted_ingest"),
            ambiguous: accounting.count("ambiguous_ingest"),
            before: before["metrics"]["logbrook_storage_retention_before_ms"]
                .as_f64()
                .map(|n| n as i64),
            after: after["metrics"]["logbrook_storage_retention_before_ms"]
                .as_f64()
                .map(|n| n as i64),
            retention: provenance["limits"]["retention_ms"].as_i64(),
            time: final_time,
            observed: if code == HTTP_OK {
                count["count"].as_u64()
            } else {
                None
            },
        },
    );
    if code == HTTP_OK
        && accounting.count("ambiguous_ingest") == 0
        && expected["observed_within_bounds"] != true
    {
        accounting.problems.push(format!(
            "final count {count} outside accepted visibility bounds {expected}"
        ));
    }

    Settled {
        before,
        after,
        code,
        count,
        expected,
    }
}

/// Whether every telemetry sample was read successfully.
fn telemetry_complete(samples: &[Value]) -> bool {
    samples.iter().all(|s| s["status"] == HTTP_OK)
}

/// Whether the run qualifies: telemetry complete, no problems or drops, tails correct, and the
/// final count within its bounds.
fn qualified(
    accounting: &Accounting,
    tails: &[Value],
    expected: &Value,
    code: u16,
    samples: &[Value],
) -> bool {
    telemetry_complete(samples)
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
        && tails_passed(tails)
        && expected["correctness_verified"] == true
        && code == HTTP_OK
}

/// Seeds nothing: runs the timed workload against a history that `seed` already wrote, then
/// checks the results and the telemetry.
///
/// # Errors
///
/// Returns an error when the options are invalid, a required archive count is not met, the
/// tail subscribers do not start, or the final HTTP client cannot be built.
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
        return Err(format!(
            concat!(
                "qualification requires {} archives; ",
                "metrics report {:?}. ",
                "Prepare a steady-state fixture first"
            ),
            args.require_min_archives, archives
        )
        .into());
    }

    let (stop, receiver) = watch::channel(false);
    let tails = spawn_tails(&http, args, &seed, &receiver).await?;
    let observer = spawn_observer(http.clone(), args.metrics_interval, receiver.clone());
    let EngineRun {
        mut accounting,
        elapsed,
        drain,
    } = run_load(&http, args, &seed).await;
    register_counts(&mut accounting);

    sleep(Duration::from_secs_f64(args.tail_drain_seconds)).await;
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
    let settled = settle(&http, args, &seed, &provenance, &mut accounting).await;
    samples.push(settled.before.clone());
    samples.push(settled.after.clone());

    let telemetry = telemetry_complete(&samples);
    let passed = qualified(
        &accounting,
        &tails,
        &settled.expected,
        settled.code,
        &samples,
    );

    if seed.seeded > 0 {
        accounting
            .statuses
            .insert("seed:200".into(), seed.seed_requests);
    }

    Ok(report_json(
        args,
        &seed,
        Outcome {
            accounting,
            elapsed,
            drain,
            tails,
            samples,
            first,
            before: settled.before,
            after: settled.after,
            provenance,
            archives,
            expected: settled.expected,
            code: settled.code,
            count: settled.count,
            telemetry,
            passed,
        },
    ))
}

/// The report object for a finished run.
fn report_json(args: &Args, seed: &Seed, outcome: Outcome) -> Value {
    let Outcome {
        accounting,
        elapsed,
        drain,
        tails,
        samples,
        first,
        before,
        after,
        provenance,
        archives,
        expected,
        code,
        count,
        telemetry,
        passed,
    } = outcome;
    json!({
        "client_platform": std::env::consts::OS,
        "server_url": args.url,
        "run_marker": seed.marker,
        "server_provenance": provenance,
        "seeded_events": seed.seeded,
        "seed_history_days": args.history_days,
        "target_rate": args.rate,
        "duration_seconds": elapsed,
        "offered_horizon_seconds": args.duration,
        "drain_seconds": drain,
        "query_clients": args.query_clients,
        "query_rate": args.query_rate,
        "ingest_clients": args.ingest_clients,
        "batch_size": args.batch_size,
        "request_shape": args.request_shape,
        "body_target_bytes": args.body_bytes,
        "client_queue_capacity_per_kind": args.client_queue_capacity,
        "burst": {
            "rate": args.burst_rate,
            "start": args.burst_start,
            "duration": args.burst_duration
        },
        "late_arrivals": {"fraction": args.late_fraction, "delay_ms": args.late_delay_ms},
        "offered_events": accounting.count("offered_ingest"),
        "accepted_events": accounting.count("accepted_ingest"),
        "accepted_events_per_second":
            accounting.count("accepted_ingest") as f64 / elapsed.max(MIN_ELAPSED_SECS),
        "accounting": accounting.counts,
        "matching_events_after": if code == HTTP_OK { Some(&count) } else { None },
        "final_count_status": code,
        "expected_visibility": expected,
        "http_status_counts": accounting.statuses,
        "latency_ms": latency_reports(&accounting, seed, 0),
        "service_latency_ms": latency_reports(&accounting, seed, 1),
        "client_queue_latency_ms": latency_reports(&accounting, seed, 2),
        "scheduler_lag_ms": accounting
            .lag
            .iter()
            .map(|(name, samples)| (name.clone(), samples.report()))
            .collect::<BTreeMap<_, _>>(),
        "tails": tails,
        "metrics_samples": samples,
        "archive_files_before": archives,
        "metrics_before": first,
        "metrics_before_final_count": before,
        "metrics_after": after,
        "correctness_problems": accounting.problems,
        "worker_errors": accounting.worker_errors,
        "qualification": {
            "telemetry_complete": telemetry,
            "passed": passed,
            "scope": concat!(
                "this offered workload and observed fixture only; ",
                "accepted count verified within committed cutoff bounds, ",
                "not a sustained capacity claim"
            )
        },
        "limits": concat!(
            "Bounded queues report drops. No ingestion retries. ",
            "Scheduled, service and queue latencies are separate. ",
            "Unique marker isolates checks. ",
            "Tail counts are observed, not a delivery proof. ",
            "Operator-supplied resources and limits are unverified."
        )
    })
}

/// Runs the workload from the environment's tokens, writes the report, and fails when the
/// report has correctness problems or worker errors, or when qualification is required and
/// did not pass.
///
/// # Errors
///
/// Returns an error when a token is missing, the workload fails, the report cannot be written,
/// or the run does not qualify.
pub async fn run(args: Args) -> Result<()> {
    let read = std::env::var("LOGBROOK_READ_TOKEN")?;
    let write = std::env::var("LOGBROOK_INGEST_TOKEN")?;
    let report = report(&args, &read, &write).await?;
    super::write_report(&args.output, &report)?;

    let correct = report["correctness_problems"]
        .as_array()
        .is_some_and(Vec::is_empty);
    let no_worker_errors = report["worker_errors"]
        .as_array()
        .is_some_and(Vec::is_empty);
    let qualified = !args.require_qualified || report["qualification"]["passed"] == true;
    if !correct || !no_worker_errors || !qualified {
        return Err("mixed workload failed qualification".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_pause_is_bounded_in_milliseconds() {
        let one_day = Args {
            seed_pause_ms: MAX_SEED_PAUSE_MS,
            ..Args::default()
        };
        let longer = Args {
            seed_pause_ms: MAX_SEED_PAUSE_MS + 1.0,
            ..Args::default()
        };
        // 100 seconds is above MAX_SECONDS as a number, but well within one day of milliseconds.
        let past_seconds_bound = Args {
            seed_pause_ms: 100_000.0,
            ..Args::default()
        };

        assert!(one_day.validate().is_ok());
        assert!(past_seconds_bound.validate().is_ok());
        assert!(longer.validate().is_err());
    }

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
