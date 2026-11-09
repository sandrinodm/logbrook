//! The index workload: concurrent ingest and count and event queries against two indexes, with
//! checks that each index sees only its own events.
use super::{
    engine::{self, Accounting, Completion, Job, Schedule, Work},
    http::Http,
    semantics::{ACCEPTED_COUNT_MISMATCH, HTTP_OK, INGEST_OPERATION, QUERY_OPERATION},
    tail,
};
use crate::{
    Result,
    runtime::{self, Fixture},
};
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
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};

/// The two indexes the workload writes and queries; ingest queue `q` feeds `INDICES[q]`.
const INDICES: [&str; 2] = ["payments", "orders"];

/// Queue indices: one ingest queue per index, then the query queue.
const QUERY_QUEUE: usize = 2;

/// Engine workers per ingest queue, so each index's batches are sent one at a time.
const INGEST_CLIENTS: usize = 1;

/// Engine workers for the query queue.
const QUERY_CLIENTS: usize = 4;

/// Capacity of each ingest queue, in batches; a full queue drops the batch.
const QUEUE_CAPACITY: usize = 64;

/// Capacity of the query queue, in queries.
const QUERY_QUEUE_CAPACITY: usize = 256;

/// Each index gets this many tail subscribers.
const TAILS_PER_INDEX: usize = 2;

/// Every third query is a count query; the rest fetch events.
const COUNT_QUERY_EVERY: u64 = 3;

/// The status that marks a query whose response was malformed or contained another index's events.
const CONTAMINATION_STATUS: u16 = 599;

/// Statuses that a server returns when it refuses work because it is overloaded.
const REFUSAL_STATUSES: [u16; 3] = [429, 503, 507];

/// Level of the synthetic log records; 30 is info in the pino numbering.
const INFO_LEVEL: i64 = 30;

/// Longest response or request payload copied into a failure example, in characters.
const FAILURE_PAYLOAD_CHARS: usize = 500;

/// The query window starts this long before the run, to include the first events.
const LOOKBACK_MS: i64 = 1000;

/// The query window ends this long after the offered load, to cover the drain and stragglers.
const WINDOW_TAIL_SECS: f64 = 120.0;

/// Milliseconds per second, for floating-point conversions.
const MILLIS_PER_SEC_F64: f64 = 1000.0;

/// Longest `--duration` accepted, in seconds.
const MAX_DURATION_SECS: f64 = 3600.0;

/// Largest `--rate` accepted, in events per second across both indexes.
const MAX_RATE: f64 = 10_000.0;

/// Largest `--query-rate` accepted, in queries per second.
const MAX_QUERY_RATE: f64 = 1000.0;

/// Largest `--batch-size` accepted, in events per request.
const MAX_BATCH_SIZE: u64 = 1000;

/// Largest `--padding-bytes` accepted, per event.
const MAX_PADDING_BYTES: usize = 4096;

/// Longest wait for the engine's in-flight requests after the schedule ends; workers still
/// running then are aborted.
const ENGINE_DRAIN: Duration = Duration::from_secs(30);

/// Longest wait for the tail subscribers to report that they are subscribed.
const TAIL_READY_TIMEOUT: Duration = Duration::from_secs(15);

/// Longest wait for every tail subscriber to see the events accepted for its index.
const TAIL_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// How often the server's metrics, and a container's resources, are sampled. It also sets the
/// minimum number of metrics samples the report expects.
const OBSERVER_INTERVAL: Duration = Duration::from_secs(10);

/// The observer gets this long to stop before it is aborted and the timeout recorded as an error.
const OBSERVER_STOP_TIMEOUT: Duration = Duration::from_secs(20);

/// Delay between checks while waiting for the tail subscribers to catch up.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// CPUs recorded in the report for a container run, matching `--cpus` in `runtime::HARDENED`.
const CONTAINER_CPUS: u64 = 2;

/// Memory limit recorded in the report for a container run, matching `--memory` in
/// `runtime::HARDENED`.
const CONTAINER_MEMORY_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// The most recent characters of a native server's log that are kept in the report.
const SERVER_LOG_TAIL_CHARS: usize = 12_000;

/// The receiver configuration: two indexes with different retention.
const RECEIVER_CONFIG: &str = concat!(
    "retention_days=7\n",
    "max_size_gb=20\n",
    "[indexes.payments]\n",
    "retention_days=7\n",
    "max_size_gb=20\n",
    "[indexes.orders]\n",
    "retention_days=14\n",
    "max_size_gb=20\n",
);

// Options for `index-load`: a fresh two-index workload with correctness, isolation and latency
// checks. Clap uses `///` docs as help text, so this stays a plain comment.
#[derive(clap::Args, Clone, Debug, serde::Serialize)]
pub struct Args {
    #[arg(
        long,
        default_value = "target/debug/logbrook",
        conflicts_with = "image"
    )]
    pub binary: PathBuf,
    #[arg(long)]
    pub image: Option<String>,
    #[arg(long, default_value_t = 60.0)]
    pub duration: f64,
    #[arg(long, default_value_t = 1000.0)]
    pub rate: f64,
    #[arg(long, default_value_t = 20.0)]
    pub query_rate: f64,
    #[arg(long, default_value_t = 100)]
    pub batch_size: u64,
    #[arg(long, default_value_t = 900)]
    pub padding_bytes: usize,
    #[arg(long)]
    pub output: PathBuf,
}

impl Args {
    /// Checks the timing and volume options. Either failing group fails the run with one error.
    fn validate(&self) -> Result<()> {
        if self.timing_invalid() || self.volume_invalid() {
            return Err("duration/rate/batch/padding outside bounded probe limits".into());
        }
        Ok(())
    }

    /// Duration and rates: finite, positive and within their maximums.
    fn timing_invalid(&self) -> bool {
        !self.duration.is_finite()
            || !(0.0..=MAX_DURATION_SECS).contains(&self.duration)
            || self.duration == 0.0
            || !self.rate.is_finite()
            || !(0.0..=MAX_RATE).contains(&self.rate)
            || self.rate == 0.0
    }

    /// The query rate, batch size and padding.
    fn volume_invalid(&self) -> bool {
        !self.query_rate.is_finite()
            || !(0.0..=MAX_QUERY_RATE).contains(&self.query_rate)
            || self.query_rate == 0.0
            || !(1..=MAX_BATCH_SIZE).contains(&self.batch_size)
            || self.padding_bytes > MAX_PADDING_BYTES
    }
}

/// One metrics sample, with the seconds since the run started.
async fn metric(http: &Http, start: Instant) -> Value {
    let (status, text) = http.request("/metrics", None, true).await;
    json!({
        "offset_seconds": start.elapsed().as_secs_f64(),
        "status": status,
        "text": text
    })
}

/// The index a job works on: each ingest queue has one index, and queries alternate.
fn index_for(job: &Job) -> &'static str {
    if job.queue < QUERY_QUEUE {
        INDICES[job.queue]
    } else {
        INDICES[job.sequence as usize % INDICES.len()]
    }
}

/// The schedules for both ingest queues (one per index) and the query queue.
fn schedules(args: &Args, planned: u64) -> Vec<Schedule> {
    let mut schedules = Vec::new();
    for queue in 0..QUERY_QUEUE {
        schedules.push(Schedule {
            duration: args.duration,
            rate: args.rate / INDICES.len() as f64,
            batch: args.batch_size,
            burst_rate: 0.0,
            burst_start: 0.0,
            burst_duration: 0.0,
            queue,
            total: Some(planned),
        });
    }

    schedules.push(Schedule {
        duration: args.duration,
        rate: args.query_rate,
        batch: 1,
        burst_rate: 0.0,
        burst_start: 0.0,
        burst_duration: 0.0,
        queue: QUERY_QUEUE,
        total: Some((args.query_rate * args.duration) as u64),
    });
    schedules
}

/// Starts the tail subscribers, `TAILS_PER_INDEX` for each index, and waits until each one is
/// subscribed. Returns the tasks and each subscriber's observed-event counter.
async fn start_tails(
    http: &Http,
    stop: &watch::Receiver<bool>,
) -> Result<(Vec<JoinHandle<Value>>, Vec<Arc<AtomicU64>>)> {
    let mut tails = Vec::new();
    let mut observations = Vec::new();
    let mut ready = Vec::new();

    for n in 0..INDICES.len() * TAILS_PER_INDEX {
        let index = INDICES[n / TAILS_PER_INDEX];
        let observed = Arc::new(AtomicU64::new(0));
        let (task, started) = tail::spawn_count_ready(
            http.clone(),
            format!("/indexes/{index}/logs/tail"),
            BTreeMap::new(),
            n,
            Some(index.into()),
            stop.clone(),
            observed.clone(),
        );
        tails.push(task);
        ready.push(started);
        observations.push(observed);
    }

    for started in ready {
        timeout(TAIL_READY_TIMEOUT, started).await??;
    }

    Ok((tails, observations))
}

/// What the metrics observer collected: the metrics samples, the container's resource samples,
/// and the errors it hit.
#[derive(Debug)]
struct Observed {
    metrics: Vec<Value>,
    resources: Vec<Value>,
    errors: Vec<Value>,
}

/// The container's resource use as reported by `docker stats`.
async fn docker_stats(container: &str) -> Result<Value> {
    let text =
        runtime::docker(&["stats", "--no-stream", "--format", "{{json .}}", container]).await?;
    Ok(serde_json::from_str(&text)?)
}

/// Samples the server's metrics every `OBSERVER_INTERVAL`, and the container's resources when
/// there is one, until `stopped` changes.
async fn observe(
    http: Http,
    start: Instant,
    container: Option<String>,
    mut stopped: watch::Receiver<bool>,
) -> Observed {
    let mut observed = Observed {
        metrics: Vec::new(),
        resources: Vec::new(),
        errors: Vec::new(),
    };
    let mut ticker = tokio::time::interval(OBSERVER_INTERVAL);
    loop {
        tokio::select! {
            biased;
            _ = stopped.changed() => break,
            _ = ticker.tick() => {
                observed.metrics.push(metric(&http, start).await);
                if let Some(container) = &container {
                    match docker_stats(container).await {
                        Ok(value) => observed.resources.push(value),
                        Err(error) => observed.errors.push(json!({
                            "type": "ObserverError",
                            "message": error.to_string()
                        })),
                    }
                }
            }
        }
    }

    observed
}

/// The engine's work function: an ingest batch or a query, against the job's index.
fn work_for(http: Http, args: Args, lower: i64, upper: i64) -> Work {
    Arc::new(move |job| {
        let http = http.clone();
        let args = args.clone();
        Box::pin(async move {
            let index = index_for(&job);

            let mut c = if job.queue < QUERY_QUEUE {
                ingest_job(&http, &args, index, &job).await?
            } else {
                query_job(&http, index, lower, upper, &job).await
            };
            c.counters
                .insert(format!("{}_status_{}", c.operation, c.status), 1);

            Ok(c)
        })
    })
}

/// Ingests one batch of events into the job's index.
async fn ingest_job(http: &Http, args: &Args, index: &str, job: &Job) -> Result<Completion> {
    let now = crate::now_ms();
    let events: Vec<_> = (0..job.count)
        .map(|n| {
            json!({
                "time": now,
                "level": INFO_LEVEL,
                "msg": format!("{index} {}", "x".repeat(args.padding_bytes)),
                "sequence": job.sequence * args.batch_size + n,
                "test_index": index
            })
        })
        .collect();

    let body = serde_json::to_vec(&events)?;
    let bytes = body.len();
    let (status, payload) = http
        .request(
            &format!("/indexes/{index}/logs/ingest"),
            Some((body, "application/json", false)),
            false,
        )
        .await;
    let mut c = Completion::new(INGEST_OPERATION, status);
    c.counters.insert("wire_ingest_bytes".into(), bytes as u64);
    if status == HTTP_OK && payload["accepted"].as_u64() == Some(job.count) {
        c.counters.insert(format!("accepted_{index}"), job.count);
    } else {
        c.counters.insert("ingest_failed_units".into(), job.count);
        c.failure = Some(json!({
            "kind": "ingest",
            "index": index,
            "status": status,
            "payload": payload.to_string().chars().take(FAILURE_PAYLOAD_CHARS).collect::<String>()
        }));
        if status == 0 {
            c.counters
                .insert("ambiguous_ingest_events".into(), job.count);
        }

        if status == HTTP_OK {
            c.problem = Some(ACCEPTED_COUNT_MISMATCH.into());
        }
    }

    Ok(c)
}

/// Runs one query against the job's index: every third is a count, the rest fetch events. Each
/// fetched event must belong to the index, and a 200 with the wrong shape is contamination.
async fn query_job(http: &Http, index: &str, lower: i64, upper: i64, job: &Job) -> Completion {
    let count = job.sequence.is_multiple_of(COUNT_QUERY_EVERY);
    let endpoint = if count { "/count" } else { "" };
    let (status, payload) = http
        .request(
            &format!("/indexes/{index}/logs{endpoint}?from={lower}&to={upper}"),
            None,
            false,
        )
        .await;
    let mut c = Completion::new(QUERY_OPERATION, status);
    let valid = status == HTTP_OK
        && if count {
            payload["count"].as_u64().is_some()
        } else {
            payload["events"].as_array().is_some_and(|rows| {
                rows.iter().all(|row| {
                    row["message"]
                        .as_str()
                        .is_some_and(|s| s.starts_with(&format!("{index} ")))
                })
            })
        };
    if valid {
        c.counters.insert("successful_queries".into(), 1);
    } else {
        c.counters.insert("query_failed_units".into(), 1);

        c.failure = Some(json!({
            "kind": "query",
            "index": index,
            "status": status,
            "payload": payload.to_string().chars().take(FAILURE_PAYLOAD_CHARS).collect::<String>()
        }));
        if status == HTTP_OK {
            c.status = CONTAMINATION_STATUS;
            c.problem = Some("cross-index query contamination or malformed response".into());
        }
    }

    c
}

/// Moves the client-side drop counts into the names that the report uses.
fn record_client_drops(stats: &mut Accounting) {
    let dropped_ingest =
        stats.count("client_dropped_payments") + stats.count("client_dropped_orders");
    let dropped_queries = stats.count("client_dropped_queries");
    stats
        .counts
        .insert("client_dropped_ingest".into(), dropped_ingest);
    stats
        .counts
        .insert("client_dropped_query".into(), dropped_queries);
}

/// Waits, up to `TAIL_DRAIN_TIMEOUT`, until each tail subscriber has seen the events accepted for
/// its index. Returns the seconds that it waited.
async fn drain_tails(stats: &Accounting, observations: &[Arc<AtomicU64>]) -> f64 {
    let tail_start = Instant::now();
    let tail_deadline = tail_start + TAIL_DRAIN_TIMEOUT;
    while stats.worker_errors.is_empty()
        && Instant::now() < tail_deadline
        && observations.iter().enumerate().any(|(n, c)| {
            c.load(Ordering::Relaxed)
                < stats.count(&format!("accepted_{}", INDICES[n / TAILS_PER_INDEX]))
        })
    {
        sleep(POLL_INTERVAL).await;
    }

    tail_start.elapsed().as_secs_f64()
}

/// Stops the observer and collects what it sampled. An observer that does not finish in time is
/// aborted, and the timeout is recorded as an error.
async fn finish_observer(mut observer: JoinHandle<Observed>) -> Observed {
    // Cancellation must also bound an in-flight Docker resource sampler.
    match timeout(OBSERVER_STOP_TIMEOUT, &mut observer).await {
        Ok(Ok(observed)) => observed,
        other => {
            observer.abort();
            let _ = observer.await;
            Observed {
                metrics: Vec::new(),
                resources: Vec::new(),
                errors: vec![json!({
                    "type": "Timeout",
                    "message": format!("observer shutdown: {other:?}")
                })],
            }
        }
    }
}

/// The per-index counts, the isolation results, and the errors from verifying both.
struct IndexChecks {
    counts: Value,
    isolation: Value,
    errors: Vec<Value>,
}

/// Counts each index, and checks that a query for one index finds none of the other's events.
async fn verify_indexes(http: &Http, lower: i64, upper: i64) -> IndexChecks {
    let mut counts = json!({});
    let mut isolation = json!({});
    let mut errors = Vec::new();
    for (n, index) in INDICES.iter().enumerate() {
        let path = format!("/indexes/{index}/logs/count?from={lower}&to={upper}");
        let (code, value) = http.request(&path, None, false).await;
        if code != HTTP_OK || value["count"].as_u64().is_none() {
            errors.push(json!({
                "path": path,
                "type": "VerificationError",
                "status": code,
                "payload": value
            }));
        }

        counts[index] = json!({"status": code, "count": value["count"]});

        // The other index's name as a message filter must match none of this index's events.
        let path = format!("{path}&message={}", INDICES[1 - n]);
        let (code, value) = http.request(&path, None, false).await;
        if code != HTTP_OK || value["count"].as_u64().is_none() {
            errors.push(json!({
                "path": path,
                "type": "VerificationError",
                "status": code,
                "payload": value
            }));
        }

        isolation[index] = json!(code == HTTP_OK && value["count"] == 0);
    }

    IndexChecks {
        counts,
        isolation,
        errors,
    }
}

/// Whether every sample contains a metrics series labelled with each index.
fn index_labels_present(metrics: &[Value]) -> bool {
    INDICES.iter().all(|index| {
        metrics.iter().any(|sample| {
            sample["status"] == HTTP_OK
                && sample["text"]
                    .as_str()
                    .is_some_and(|text| text.contains(&format!("{{index=\"{index}\"}}")))
        })
    })
}

/// The container's peak memory and memory events, or `None` when there is no container or it
/// cannot be probed (the failure is recorded in `errors`).
async fn cgroup_memory(container: Option<&str>, errors: &mut Vec<Value>) -> Option<Value> {
    let container = container?;
    let counters: [String; 2] = ["memory.peak".into(), "memory.events".into()];
    match crate::container::probe(container, &counters).await {
        Ok(v) => Some(Value::String(format!(
            "{}{}",
            v["memory.peak"].as_str().unwrap_or_default(),
            v["memory.events"].as_str().unwrap_or_default()
        ))),
        Err(error) => {
            errors.push(json!({"type": "CgroupError", "message": error.to_string()}));
            None
        }
    }
}

/// Replaces each tail's error count with the list of its errors: empty when it had none,
/// otherwise its message.
fn normalize_tail_errors(tails: &mut [Value]) {
    for tail in tails {
        let errors = if tail["errors"] == 0 {
            Vec::new()
        } else {
            vec![tail["error"].as_str().unwrap_or("tail failure").to_owned()]
        };
        tail["errors"] = json!(errors);
    }
}

/// Whether each index was ingested in full, read back in full, and isolated from the other.
fn indexes_qualify(counts: &Value, isolation: &Value, stats: &Accounting, planned: u64) -> bool {
    INDICES.iter().all(|index| {
        counts[index]["status"] == HTTP_OK
            && counts[index]["count"].as_u64() == Some(planned)
            && stats.count(&format!("accepted_{index}")) == planned
            && isolation[index] == true
    })
}

/// Whether no client-side drop and no failed unit was recorded.
fn counters_clean(stats: &Accounting) -> bool {
    [
        "client_dropped_ingest",
        "client_dropped_query",
        "ingest_failed_units",
        "query_failed_units",
    ]
    .iter()
    .all(|k| stats.count(k) == 0)
}

/// Whether every tail subscriber saw all the planned events of its index, in order, with no gap
/// or error.
fn tails_qualify(tails: &[Value], planned: u64) -> bool {
    tails.iter().all(|t| {
        t["status"] == HTTP_OK
            && t["observed"].as_u64() == Some(planned)
            && t["gaps"] == 0
            && t["errors"] == 0
            && t["isolated"] == true
            && t["monotonic"] == true
    })
}

/// The count of each service-refusal status for each kind of request.
fn service_refusals(stats: &Accounting) -> BTreeMap<String, u64> {
    ["ingest", "query"]
        .into_iter()
        .flat_map(|kind| {
            REFUSAL_STATUSES
                .into_iter()
                .map(move |status| (kind, status))
        })
        .map(|(kind, status)| {
            (
                format!("{kind}_{status}"),
                stats.count(&format!("{kind}_status_{status}")),
            )
        })
        .collect()
}

/// The latency report for one operation at one position: 0 scheduled, 1 service. An operation
/// that never ran reports an empty distribution.
fn latency_report(stats: &Accounting, operation: &str, position: usize) -> Value {
    stats
        .latency
        .get(operation)
        .map(|s| s[position].report())
        .unwrap_or_else(|| super::semantics::Samples::default().report())
}

/// Runs the two-index workload against the receiver at `base`, and returns the qualification
/// report.
///
/// # Errors
///
/// Returns an error when the options are out of bounds, the HTTP client cannot be built, a tail
/// subscriber does not start, or the clock is invalid.
pub async fn workload(
    base: &str,
    read: &str,
    write: &str,
    container: Option<&str>,
    args: &Args,
) -> Result<Value> {
    args.validate()?;
    let http = Http::new(base, read, write)?;
    let start = Instant::now();
    let started_at = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(crate::now_ms())
        .ok_or("invalid system timestamp")?
        .to_rfc3339();
    let lower = crate::now_ms() - LOOKBACK_MS;
    let upper = lower + ((args.duration + WINDOW_TAIL_SECS) * MILLIS_PER_SEC_F64) as i64;
    let planned = (args.rate * args.duration / INDICES.len() as f64) as u64;

    let (stop, receiver) = watch::channel(false);
    let (tails, observations) = start_tails(&http, &receiver).await?;
    let container = container.map(str::to_owned);
    let observer = tokio::spawn(observe(
        http.clone(),
        start,
        container.clone(),
        receiver.clone(),
    ));

    let (mut stats, _, request_drain) = engine::execute(
        schedules(args, planned),
        vec![INGEST_CLIENTS, INGEST_CLIENTS, QUERY_CLIENTS],
        vec![QUEUE_CAPACITY, QUEUE_CAPACITY, QUERY_QUEUE_CAPACITY],
        vec!["payments".into(), "orders".into(), "queries".into()],
        work_for(http.clone(), args.clone(), lower, upper),
        ENGINE_DRAIN,
    )
    .await;
    record_client_drops(&mut stats);
    let tail_drain = drain_tails(&stats, &observations).await;
    let _ = stop.send(true);
    let mut tails = tail::finish(tails).await;

    let Observed {
        mut metrics,
        resources,
        errors: mut observer_errors,
    } = finish_observer(observer).await;

    let IndexChecks {
        counts,
        isolation,
        errors: verification_errors,
    } = verify_indexes(&http, lower, upper).await;

    metrics.push(metric(&http, start).await);
    let labels = index_labels_present(&metrics);
    let cgroup = cgroup_memory(container.as_deref(), &mut observer_errors).await;
    let minimum = (args.duration / OBSERVER_INTERVAL.as_secs_f64()).ceil() as usize + 1;
    let passed = indexes_qualify(&counts, &isolation, &stats, planned)
        && counters_clean(&stats)
        && labels
        && stats.worker_errors.is_empty()
        && stats.problems.is_empty()
        && observer_errors.is_empty()
        && verification_errors.is_empty()
        && metrics.len() >= minimum
        && metrics.iter().all(|s| s["status"] == HTTP_OK)
        && tails_qualify(&tails, planned);

    normalize_tail_errors(&mut tails);

    let latency_ms = |operation: &str, position: usize| latency_report(&stats, operation, position);
    Ok(json!({
        "started_at": started_at,
        "started_at_ms": lower + LOOKBACK_MS,
        "planned_per_index": planned,
        "counts": stats.counts,
        "final_counts": counts,
        "isolated_queries": isolation,
        "tails": tails,
        "request_drain_seconds": request_drain,
        "tail_drain_seconds": tail_drain,
        "ingest_ms": latency_ms("ingest", 1),
        "query_ms": latency_ms("query", 1),
        "ingest_scheduled_ms": latency_ms("ingest", 0),
        "query_scheduled_ms": latency_ms("query", 0),
        "scheduler_lag_ms": stats
            .lag
            .iter()
            .map(|(n, s)| (n, s.report()))
            .collect::<BTreeMap<_, _>>(),
        "failure_examples": stats.failures,
        "correctness_problems": stats.problems,
        "service_refusals": service_refusals(&stats),
        "metrics": metrics,
        "index_metric_labels": labels,
        "worker_errors": stats.worker_errors,
        "observer_errors": observer_errors,
        "verification_errors": verification_errors,
        "expected_minimum_metrics_samples": minimum,
        "docker_stats": resources,
        "cgroup_memory": cgroup,
        "passed": passed
    }))
}

/// Records what the run needs before the workload starts: the binary's digest for a native run,
/// and the container's limits and image for a container run.
async fn prepare_fixture(fixture: &Fixture, args: &Args, lifecycle: &mut Value) -> Result<()> {
    if args.image.is_none() {
        lifecycle["binary_sha256"] = json!(super::file_sha256(&args.binary)?);
        lifecycle["resource_limits"] = json!("native process, no cgroup constraint");
    }

    if let Some(container) = &fixture.container {
        lifecycle["container"] = json!(container);
        lifecycle["cpus"] = json!(CONTAINER_CPUS);
        lifecycle["memory_bytes"] = json!(CONTAINER_MEMORY_BYTES);
        lifecycle["image_inspect"] = serde_json::from_str(
            &runtime::docker(&["image", "inspect", args.image.as_deref().expect("image")]).await?,
        )?;
    }

    Ok(())
}

/// Records how the server stopped: the container's exit state and log tail, or the native
/// process's exit code and the tail of its log.
async fn record_exit(fixture: &Fixture, stopped: &Result<()>, lifecycle: &mut Value) {
    if let Some(container) = &fixture.container {
        match runtime::docker(&["inspect", container])
            .await
            .and_then(|text| Ok(serde_json::from_str::<Value>(&text)?))
        {
            Ok(inspect) => {
                lifecycle["exit_code"] = inspect[0]["State"]["ExitCode"].clone();
                lifecycle["oom_killed"] = inspect[0]["State"]["OOMKilled"].clone();
            }
            Err(error) => lifecycle["inspection_error"] = json!(error.to_string()),
        }

        if let Ok(logs) = runtime::docker(&["logs", "--tail", "40", container]).await {
            lifecycle["server_logs"] = json!(logs);
        }
    } else {
        lifecycle["exit_code"] = if stopped.is_ok() {
            json!(0)
        } else {
            Value::Null
        };

        let path = fixture.root.path().join("server.log");
        if let Ok(logs) = tokio::fs::read_to_string(path).await {
            let tail = logs
                .chars()
                .rev()
                .take(SERVER_LOG_TAIL_CHARS)
                .collect::<String>()
                .chars()
                .rev()
                .collect::<String>();
            lifecycle["server_logs"] = json!(tail);
        }
    }
}

/// Stops the receiver, records how it stopped, and removes its disposable resources. Returns
/// whether the stop succeeded. The inspection happens before removal, so that a clean removal
/// cannot conceal an OOM or abnormal exit.
async fn shut_down(fixture: &mut Fixture, lifecycle: &mut Value) -> bool {
    let stopped = fixture.stop().await;
    record_exit(fixture, &stopped, lifecycle).await;
    let stop_succeeded = stopped.is_ok();
    if let Err(error) = &stopped {
        lifecycle["stop_error"] = json!(error.to_string());
    }

    let cleanup = fixture.shutdown().await;
    lifecycle["cleanup_succeeded"] = json!(cleanup.is_ok());
    lifecycle["container_and_volume_removed"] = json!(cleanup.is_ok());
    if let Err(error) = cleanup {
        lifecycle["cleanup_error"] = json!(error.to_string());
    }
    stop_succeeded
}

/// Runs the index workload on a fresh two-index receiver, writes the JSON report, and fails when
/// the qualification does not pass.
///
/// # Errors
///
/// Returns an error when the options are out of bounds, the receiver cannot start, the report
/// cannot be written, or the qualification fails.
pub async fn run(args: Args) -> Result<()> {
    args.validate()?;

    let mut lifecycle = json!({
        "mode": if args.image.is_some() { "docker" } else { "native" },
        "binary": args.binary,
        "image": args.image
    });

    let config = RECEIVER_CONFIG;
    let mut fixture =
        match Fixture::start_config(args.binary.clone(), args.image.clone(), config).await {
            Ok(fixture) => fixture,
            Err(error) => {
                let report = json!({
                    "version": 1,
                    "parameters": args,
                    "lifecycle": lifecycle,
                    "workload": {"passed": false},
                    "passed": false,
                    "error": error.to_string()
                });
                super::write_report(&args.output, &report)?;
                return Err(error);
            }
        };
    let setup = prepare_fixture(&fixture, &args, &mut lifecycle).await;
    let workload = match setup {
        Ok(()) => {
            workload(
                &fixture.base,
                &fixture.read_token,
                &fixture.ingest_token,
                fixture.container.as_deref(),
                &args,
            )
            .await
        }
        Err(error) => Err(error),
    };
    let stop_succeeded = shut_down(&mut fixture, &mut lifecycle).await;
    let workload = match workload {
        Ok(report) => report,
        Err(error) => json!({"passed": false, "error": error.to_string()}),
    };
    let passed = workload["passed"] == true
        && lifecycle["cleanup_succeeded"] == true
        && stop_succeeded
        && lifecycle["exit_code"] == 0
        && lifecycle["oom_killed"] != true
        && lifecycle.get("inspection_error").is_none();
    let report = json!({
        "version": 1,
        "parameters": args,
        "lifecycle": lifecycle,
        "scope": concat!(
            "fresh two-index workload correctness and measured latency, ",
            "not sustained capacity"
        ),
        "source_sha256": super::source_sha256(),
        "tool_binary_sha256": super::file_sha256(&std::env::current_exe()?)?,
        "workload": workload,
        "passed": passed,
    });
    super::write_report(&args.output, &report)?;
    if !passed {
        return Err("index workload failed qualification".into());
    }

    Ok(())
}
