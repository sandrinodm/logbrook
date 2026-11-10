//! Live attribute qualification: fixed offered schedules, bounded queues, and one clock.
use super::{
    attributes::{self, Args, Operation, Seed, len_u64},
    attributes_schedule as schedule,
    engine::{self, Accounting, Completion, Job, Schedule, Work},
    http::{self, Http},
    semantics::{HTTP_OK, INGEST_OPERATION, MILLISECONDS_PER_SECOND, Samples},
};
use crate::{Result, runtime::Fixture};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    ops::Range,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
    time::Instant,
};

/// Events per ingest request.
const BATCH: u64 = schedule::BATCH_SIZE;

/// Client queue capacity of the ingest and query workers.
const QUEUE_CAPACITY: usize = 64;

/// Concurrent ingest clients.
const INGEST_WORKERS: usize = 4;

/// Largest matching-observation ledger the run may track.
const MAX_TRACKED_MATCHES: f64 = 1_000_000.0;

/// Logger that marks the matching events the tail must observe.
const MATCH_LOGGER: &str = "attribute-live-match";

/// Logger of the events that do not match.
const OTHER_LOGGER: &str = "attribute-live-other";

/// One event in this many is a matching event.
const MATCHING_EVERY: u64 = 100;

/// Delay between the readiness check and the first scheduled offer.
const START_DELAY: Duration = Duration::from_millis(100);

/// Longest the tail may take to report its first checkpoint.
const READY_TIMEOUT: Duration = Duration::from_secs(15);

/// Longest the workload may drain after its last offer.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(15);

/// Connect timeout of the tail stream.
const TAIL_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Longest the tail may take to stop after it is asked to.
const TAIL_STOP_TIMEOUT: Duration = Duration::from_secs(2);

/// Longest the run waits for the tail to observe every committed matching event.
const OBSERVATION_GRACE: Duration = Duration::from_secs(2);

/// Pause between checks of the observation ledger.
const OBSERVATION_POLL: Duration = Duration::from_millis(10);

/// Largest tail frame or unparsed buffer the client keeps, in bytes.
const MAX_TAIL_BUFFER_BYTES: usize = 2 * 1024 * 1024;

/// Query operations in the order the query worker cycles through them.
const LIVE_QUERY_MIX: [Operation; 5] = [
    Operation::Search,
    Operation::Search,
    Operation::Count,
    Operation::Histogram,
    Operation::Facets,
];

/// Length of `LIVE_QUERY_MIX`, as the schedule counts it.
const LIVE_QUERY_MIX_LEN: u64 = 5;

/// Measured queries needed before every operation must have a sample.
const MIN_QUERIES_FOR_ALL_OPERATIONS: u64 = 5;

/// What the tail observed for one matching event, and when.
struct Observation {
    scheduled: Instant,
    acknowledged: Option<Instant>,
    observed: Option<Instant>,
    accepted: bool,
    measured: bool,
}

/// Matching events keyed by live sequence, shared by the ingest workers and the tail.
type Ledger = Arc<Mutex<BTreeMap<u64, Observation>>>;

/// Locks the observation ledger.
fn lock_ledger(ledger: &Ledger) -> MutexGuard<'_, BTreeMap<u64, Observation>> {
    ledger.lock().expect("live observation ledger")
}

/// Running totals of what the tail stream delivered.
#[derive(Default)]
struct TailSummary {
    events: u64,
    duplicates: u64,
    unknown: u64,
    wrong_selection: u64,
    gaps: u64,
    errors: Vec<String>,
    last_id: u64,
    monotonic: bool,
}

impl TailSummary {
    /// Applies one server-sent event frame to the summary and the ledger.
    fn frame(
        &mut self,
        frame: &str,
        ledger: &Ledger,
        ready: &mut Option<oneshot::Sender<()>>,
    ) -> Result<()> {
        let fields: BTreeMap<_, _> = frame
            .lines()
            .filter_map(|line| {
                line.split_once(':')
                    .map(|(key, value)| (key, value.trim_start()))
            })
            .collect();
        match fields.get("event").copied() {
            Some("cursor") => {
                if fields.get("id").is_none_or(|token| token.is_empty()) {
                    return Err("tail checkpoint omitted its resume token".into());
                }
                if let Some(ready) = ready.take() {
                    let _ = ready.send(());
                }
            }
            Some("gap") => self.gaps += 1,
            Some("error") => return Err("tail returned an error frame".into()),
            Some("log") => self.log_frame(&fields, ledger)?,
            _ => {}
        }

        Ok(())
    }

    /// Applies one `log` frame: ordering, selection, resume token and ledger match.
    fn log_frame(&mut self, fields: &BTreeMap<&str, &str>, ledger: &Ledger) -> Result<()> {
        let now = Instant::now();
        let event: Value =
            serde_json::from_str(fields.get("data").ok_or("tail log omitted data")?)?;
        let id = event["id"]
            .as_str()
            .ok_or("tail log omitted payload ID")?
            .parse::<u64>()?;
        if id <= self.last_id {
            self.monotonic = false;
        }
        self.last_id = id;
        self.events += 1;

        if fields.get("id").is_none_or(|token| token.is_empty()) {
            return Err("tail log omitted resume token".into());
        }

        let sequence = event["attributes"]["liveSequence"]
            .as_u64()
            .ok_or("tail event omitted live sequence")?;
        if event["attributes"]["liveRun"] != MATCH_LOGGER
            || !sequence.is_multiple_of(MATCHING_EVERY)
        {
            self.wrong_selection += 1;
        }

        let mut observations = lock_ledger(ledger);
        if let Some(entry) = observations.get_mut(&sequence) {
            if entry.observed.is_some() {
                self.duplicates += 1;
            } else {
                entry.observed = Some(now);
            }
        } else {
            self.unknown += 1;
        }

        Ok(())
    }
}

/// Message of a tail frame or buffer that exceeds the client bound.
fn oversized_tail_frame() -> &'static str {
    "tail frame exceeded bounded client buffer"
}

/// The request that opens the tail stream: the client that sends it, its URL and the read token.
struct TailRequest {
    client: reqwest::Client,
    url: String,
    token: String,
}

impl TailRequest {
    /// Sends the request with the read token.
    async fn open(self) -> reqwest::Result<reqwest::Response> {
        self.client
            .get(self.url)
            .bearer_auth(self.token)
            .send()
            .await
    }
}

/// Opens the tail stream and reads frames until stopped, the stream ends, or an error occurs.
async fn read_tail(
    stop: &mut watch::Receiver<bool>,
    request: TailRequest,
    ledger: &Ledger,
    summary: &mut TailSummary,
    ready: &mut Option<oneshot::Sender<()>>,
) -> Result<()> {
    let mut response = tokio::select! {
        _ = stop.changed() => return Ok(()),
        response = request.open() => response?,
    };
    if response.status() != HTTP_OK {
        return Err(format!("tail returned HTTP {}", response.status()).into());
    }

    let mut buffered = Vec::new();
    loop {
        let chunk = tokio::select! {
            biased;
            _ = stop.changed() => return Ok(()),
            chunk = response.chunk() => chunk?,
        };
        let Some(chunk) = chunk else {
            return Err("tail disconnected before workload completion".into());
        };
        buffered.extend_from_slice(&chunk);

        while let Some(end) = buffered.windows(2).position(|bytes| bytes == b"\n\n") {
            if end + 2 > MAX_TAIL_BUFFER_BYTES {
                return Err(oversized_tail_frame().into());
            }

            let frame = String::from_utf8(buffered.drain(..end + 2).collect())?;
            summary.frame(&frame, ledger, ready)?;
        }

        if buffered.len() > MAX_TAIL_BUFFER_BYTES {
            return Err(oversized_tail_frame().into());
        }
    }
}

/// Starts the tail task. Its oneshot fires at the first checkpoint.
///
/// # Errors
///
/// Returns an error when the HTTP client cannot be built.
fn spawn_tail(
    http: &Http,
    comparison: bool,
    ledger: Ledger,
    mut stop: watch::Receiver<bool>,
) -> Result<(JoinHandle<TailSummary>, oneshot::Receiver<()>)> {
    let client = reqwest::Client::builder()
        .connect_timeout(TAIL_CONNECT_TIMEOUT)
        .retry(reqwest::retry::never())
        .redirect(reqwest::redirect::Policy::none())
        .build()?;

    let params = if comparison {
        BTreeMap::from([(
            "filter".into(),
            json!({"attributes":[{"key":"liveRun","op":"eq","value":MATCH_LOGGER}]}).to_string(),
        )])
    } else {
        BTreeMap::from([("logger".into(), MATCH_LOGGER.into())])
    };
    let request = TailRequest {
        client,
        url: format!("{}{}", http.base, http::path("/logs/tail", &params)),
        token: http.read.clone(),
    };
    let (ready, started) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut summary = TailSummary {
            monotonic: true,
            ..TailSummary::default()
        };
        let mut ready = Some(ready);
        let result = read_tail(&mut stop, request, &ledger, &mut summary, &mut ready).await;
        if let Err(error) = result {
            summary.errors.push(error.to_string());
        }

        summary
    });
    Ok((task, started))
}

/// A fixed-rate schedule with no bursts.
fn schedule(duration: f64, rate: f64, batch: u64, queue: usize) -> Schedule {
    Schedule {
        duration,
        rate,
        batch,
        burst_rate: 0.0,
        burst_start: 0.0,
        burst_duration: 0.0,
        queue,
        total: None,
    }
}

/// Builds live event `sequence`, stamped with the current time.
fn live_event(args: &Args, sequence: u64) -> Value {
    let mut event = attributes::make_event(args, sequence, crate::now_ms());
    // Live events cannot change fixed preloaded-window query results.
    event["time"] = json!(crate::now_ms());
    let logger = if sequence.is_multiple_of(MATCHING_EVERY) {
        MATCH_LOGGER
    } else {
        OTHER_LOGGER
    };
    event["name"] = json!(logger);
    event["liveRun"] = json!(logger);
    event["liveSequence"] = json!(sequence);
    event
}

/// Milliseconds in `duration`, as a float.
fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * MILLISECONDS_PER_SECOND
}

/// Summarizes the matching events and the tail stream into the report's tail section.
fn observation_report(ledger: &BTreeMap<u64, Observation>, tail: &TailSummary) -> Value {
    let mut acknowledgement = Samples::default();
    let mut scheduled = Samples::default();
    let mut negative = Samples::default();
    let mut missing = 0;
    let mut accepted = 0;
    let mut warmup_accepted = 0;
    let mut warmup_observed = 0;
    let mut warmup_missing = 0;
    let mut negative_count = 0;
    for entry in ledger.values() {
        if !entry.measured {
            warmup_accepted += u64::from(entry.accepted);
            warmup_observed += u64::from(entry.observed.is_some());
            warmup_missing += u64::from(entry.accepted && entry.observed.is_none());
            continue;
        }
        if !entry.accepted {
            continue;
        }

        accepted += 1;
        let Some(observed) = entry.observed else {
            missing += 1;
            continue;
        };
        scheduled.add(millis(observed.saturating_duration_since(entry.scheduled)));
        let acknowledged = entry.acknowledged.expect("accepted event acknowledgement");
        if let Some(duration) = observed.checked_duration_since(acknowledged) {
            acknowledgement.add(millis(duration));
        } else {
            negative_count += 1;
            negative.add(millis(acknowledged.duration_since(observed)));
        }
    }

    json!({
        "events": tail.events,
        "monotonic": tail.monotonic,
        "duplicates": tail.duplicates,
        "unknown_sequences": tail.unknown,
        "wrong_selection": tail.wrong_selection,
        "gaps": tail.gaps,
        "errors": tail.errors,
        "expected_matching_committed": accepted,
        "missing_matching_committed": missing,
        "warmup_matching_committed": warmup_accepted,
        "warmup_matching_observed": warmup_observed,
        "warmup_missing_matching_committed": warmup_missing,
        "durable_acknowledgement_to_observation_ms": acknowledgement.report(),
        "scheduled_offer_to_observation_ms": scheduled.report(),
        "observed_before_acknowledgement": negative_count,
        "negative_observation_magnitude_ms": negative.report(),
        "clock": concat!(
            "one driver monotonic Instant epoch; ",
            "negative acknowledgement races reported separately"
        )
    })
}

/// The counter `key` of a live result, or zero when it is absent.
fn count(result: &Value, key: &str) -> u64 {
    result["counts"][key].as_u64().unwrap_or(0)
}

/// Whether a dedicated tail result offered and observed every event it promised.
pub(super) fn tail_delivery_complete(result: &Value) -> bool {
    let Some(expected) = result["expected_offered_ingest"]
        .as_u64()
        .filter(|value| *value > 0)
    else {
        return false;
    };
    let Some(matches) = result["expected_measured_matching_offers"]
        .as_u64()
        .filter(|value| *value > 0)
    else {
        return false;
    };

    let tail = &result["tail"];
    count(result, "offered_ingest") == expected
        && count(result, "accepted_ingest") == expected
        && [
            "rejected_ingest",
            "ambiguous_ingest",
            "client_dropped_ingest",
        ]
        .iter()
        .all(|key| count(result, key) == 0)
        && tail["expected_matching_committed"].as_u64() == Some(matches)
        && tail["scheduled_offer_to_observation_ms"]["samples"].as_u64() == Some(matches)
        && tail["durable_acknowledgement_to_observation_ms"]["samples"]
            .as_u64()
            .zip(tail["observed_before_acknowledgement"].as_u64())
            .is_some_and(|(samples, negative)| samples.checked_add(negative) == Some(matches))
        && tail["missing_matching_committed"] == 0
        && tail["warmup_missing_matching_committed"] == 0
}

/// Whether a live result sent and completed every query it offered, with a sample for each
/// measured operation once enough queries ran.
pub(super) fn normal_query_load_achieved(result: &Value) -> bool {
    let Some(expected) = result["expected_offered_query"]
        .as_u64()
        .filter(|value| *value > 0)
    else {
        return false;
    };
    let Some(measured) = result["expected_measured_queries"]
        .as_u64()
        .filter(|value| *value > 0)
    else {
        return false;
    };

    let samples = result["service_latency_ms"]
        .as_object()
        .map_or(0, |operations| {
            operations
                .iter()
                .filter(|(operation, _)| operation.starts_with("measured_"))
                .map(|(_, latency)| latency["samples"].as_u64().unwrap_or(0))
                .sum::<u64>()
        });
    count(result, "offered_query") == expected
        && count(result, "sent_query") == expected
        && count(result, "completed_query") == expected
        && count(result, "failed_query") == 0
        && count(result, "client_dropped_query") == 0
        && samples == measured
        && (measured < MIN_QUERIES_FOR_ALL_OPERATIONS
            || schedule::OPERATIONS.iter().all(|operation| {
                result["service_latency_ms"][format!("measured_{operation}")]["samples"]
                    .as_u64()
                    .is_some_and(|samples| samples > 0)
            }))
}

/// Everything an ingest or query job needs; one copy is shared by every job of a run.
struct LiveContext {
    args: Args,
    seed: Seed,
    comparison: bool,
    http: Http,
    ledger: Ledger,
    start: Instant,
    measured_start: Instant,
    measured_end: Instant,
}

/// Sends one ingest batch, records its matching events in the ledger, and counts the outcome.
async fn ingest_job(context: &LiveContext, job: Job) -> Result<Completion> {
    let measured = job.offset >= context.args.live_warmup_seconds;
    let first = job.sequence * BATCH;
    let matching = first..first + job.count;
    let events: Vec<_> = matching
        .clone()
        .map(|sequence| live_event(&context.args, sequence))
        .collect();
    let scheduled = context.start + Duration::from_secs_f64(job.offset);
    register_matching_offers(context, matching.clone(), scheduled, measured);

    let body = serde_json::to_vec(&events)?;
    let bytes = len_u64(body.len());
    let (status, response) = context
        .http
        .request(
            "/logs/ingest",
            Some((body, "application/json", false)),
            false,
        )
        .await;
    let acknowledged = Instant::now();
    let accepted = status == HTTP_OK && response["accepted"].as_u64() == Some(job.count);
    record_acknowledgements(context, matching, accepted, acknowledged);

    let reply = IngestReply {
        status,
        response,
        bytes,
        acknowledged,
        accepted,
    };
    Ok(ingest_completion(context, &job, measured, reply))
}

/// What the server said about one ingest batch, and when the client got it.
struct IngestReply {
    status: u16,
    response: Value,
    bytes: u64,
    acknowledged: Instant,
    accepted: bool,
}

/// Records every matching event of a batch in the ledger before the batch is sent.
fn register_matching_offers(
    context: &LiveContext,
    matching: Range<u64>,
    scheduled: Instant,
    measured: bool,
) {
    let mut ledger = lock_ledger(&context.ledger);
    for sequence in matching.filter(|sequence| sequence.is_multiple_of(MATCHING_EVERY)) {
        ledger.insert(
            sequence,
            Observation {
                scheduled,
                acknowledged: None,
                observed: None,
                accepted: false,
                measured,
            },
        );
    }
}

/// Stores the outcome and acknowledgement time of a batch's matching events in the ledger.
fn record_acknowledgements(
    context: &LiveContext,
    matching: Range<u64>,
    accepted: bool,
    acknowledged: Instant,
) {
    let mut ledger = lock_ledger(&context.ledger);
    for sequence in matching.filter(|sequence| sequence.is_multiple_of(MATCHING_EVERY)) {
        let entry = ledger.get_mut(&sequence).expect("inserted matching offer");
        entry.accepted = accepted;
        entry.acknowledged = Some(acknowledged);
    }
}

/// The completion of one ingest batch: what it offered, how the server answered, and the
/// problems and failure record that follow from the answer.
fn ingest_completion(
    context: &LiveContext,
    job: &Job,
    measured: bool,
    reply: IngestReply,
) -> Completion {
    let IngestReply {
        status,
        response,
        bytes,
        acknowledged,
        accepted,
    } = reply;
    let mut result = Completion::new(INGEST_OPERATION, status);
    result.counters.insert("sent_ingest".into(), job.count);
    result.counters.insert("sent_body_bytes".into(), bytes);

    let outcome = if accepted {
        "accepted"
    } else if status == 0 || status == HTTP_OK {
        "ambiguous"
    } else {
        "rejected"
    };
    result
        .counters
        .insert(format!("{outcome}_ingest"), job.count);

    if measured {
        result
            .counters
            .insert(format!("measured_{outcome}_ingest"), job.count);
    }

    if accepted && acknowledged >= context.measured_start {
        let counter = if acknowledged <= context.measured_end {
            "committed_in_scheduled_horizon"
        } else {
            "committed_during_drain"
        };
        result.counters.insert(counter.into(), job.count);
    }

    if status == HTTP_OK && !accepted {
        result.problem = Some("ingest accepted count mismatch".into());
    }

    if !accepted {
        result.failure =
            Some(json!({"sequence": job.sequence, "status": status, "response": response}));
    }

    result
}

/// Sends one query from the operation mix and checks it against the oracle when it succeeds.
async fn query_job(context: &LiveContext, job: Job) -> Result<Completion> {
    let measured = job.offset >= context.args.live_warmup_seconds;
    let seed = &context.seed;
    let slot =
        usize::try_from(job.sequence % LIVE_QUERY_MIX_LEN).expect("query mix slot fits in usize");
    let operation = LIVE_QUERY_MIX[slot];
    let run = job.sequence % seed.runs;
    let user = Some(job.sequence / seed.runs % attributes::USERS_PER_RUN);
    let mut params = attributes::query_params(seed, run, user);
    if !context.comparison {
        params.remove("filter");
    }

    let path = attributes::operation_path(operation, params);
    let (status, response) = context.http.request(&path, None, false).await;

    let phase = if measured { "measured" } else { "warmup" };
    let mut result = Completion::new(&format!("{phase}_{}", operation.name()), status);
    result.counters.insert("sent_query".into(), 1);

    if status == HTTP_OK {
        result.counters.insert("completed_query".into(), 1);
        let correct = if context.comparison {
            attributes::verify_response(seed, operation.name(), run, user, &response)
        } else {
            attributes::verify_unfiltered_response(seed, operation.name(), &response)
        };
        if let Err(error) = correct {
            result.problem = Some(error.to_string());
        }
    } else {
        result.counters.insert("failed_query".into(), 1);
        result.failure = Some(json!({
            "operation": operation.name(),
            "status": status,
            "response": response
        }));
    }

    Ok(result)
}

/// The schedules, worker counts, queue capacities and names of one live run.
struct Plan {
    schedules: Vec<Schedule>,
    workers: Vec<usize>,
    capacities: Vec<usize>,
    names: Vec<String>,
}

/// The ingest schedule, plus the query schedule when the profile includes queries.
fn live_plan(args: &Args, duration: f64, tail_rate: f64, with_queries: bool) -> Plan {
    let mut plan = Plan {
        schedules: vec![schedule(duration, tail_rate, BATCH, 0)],
        workers: vec![INGEST_WORKERS],
        capacities: vec![QUEUE_CAPACITY],
        names: vec!["ingest".into()],
    };
    if with_queries {
        plan.schedules
            .push(schedule(duration, args.query_rate, 1, 1));
        plan.workers.push(1);
        plan.capacities.push(QUEUE_CAPACITY);
        plan.names.push("query".into());
    }
    plan
}

/// The counts that the fixed schedules promise before any request is sent.
struct Promised {
    offered_ingest: u64,
    matching_measured: u64,
    offered_query: u64,
    measured_query: u64,
}

/// Counts the events and queries that the plan offers, and the matching ones in the window.
fn promised_counts(plan: &Plan, args: &Args) -> Promised {
    let ingest = &plan.schedules[0];
    let matching_in = |first: u64, count: u64| {
        u64::try_from(
            (first..first + count)
                .filter(|sequence| sequence.is_multiple_of(MATCHING_EVERY))
                .count(),
        )
        .expect("matching count fits in u64")
    };
    let measured_jobs = |schedule: &Schedule| {
        schedule
            .jobs()
            .filter(|job| job.offset >= args.live_warmup_seconds)
            .collect::<Vec<_>>()
    };
    let offered_ingest = ingest.jobs().map(|job| job.count).sum();
    let matching_measured = measured_jobs(ingest)
        .iter()
        .map(|job| matching_in(job.sequence * BATCH, job.count))
        .sum();

    let (offered_query, measured_query) = plan.schedules.get(1).map_or((0, 0), |queries| {
        (
            queries.jobs().count() as u64,
            measured_jobs(queries).len() as u64,
        )
    });
    Promised {
        offered_ingest,
        matching_measured,
        offered_query,
        measured_query,
    }
}

/// Waits for the tail's first checkpoint, or stops the tail and fails.
///
/// # Errors
///
/// Returns an error when the checkpoint does not arrive within the ready timeout.
async fn await_tail_ready(
    ready: oneshot::Receiver<()>,
    stop: &watch::Sender<bool>,
    tail_task: &mut JoinHandle<TailSummary>,
) -> Result<()> {
    match tokio::time::timeout(READY_TIMEOUT, ready).await {
        Ok(Ok(())) => Ok(()),
        result => {
            let _ = stop.send(true);
            tail_task.abort();
            let _ = tail_task.await;
            Err(format!("tail checkpoint readiness failed: {result:?}").into())
        }
    }
}

/// Waits until every accepted matching event is observed, the tail ends, or the grace expires.
async fn await_observations(ledger: &Ledger, tail_task: &JoinHandle<TailSummary>) {
    let deadline = Instant::now() + OBSERVATION_GRACE;
    loop {
        let complete = lock_ledger(ledger)
            .values()
            .all(|entry| !entry.accepted || entry.observed.is_some());
        if complete || Instant::now() >= deadline || tail_task.is_finished() {
            break;
        }

        tokio::time::sleep(OBSERVATION_POLL).await;
    }
}

/// Stops the tail and returns its summary; a tail that will not stop is recorded as an error.
async fn stop_tail(
    stop: &watch::Sender<bool>,
    tail_task: &mut JoinHandle<TailSummary>,
) -> Result<TailSummary> {
    let _ = stop.send(true);
    match tokio::time::timeout(TAIL_STOP_TIMEOUT, &mut *tail_task).await {
        Ok(result) => Ok(result?),
        Err(_) => {
            tail_task.abort();
            let _ = tail_task.await;
            Ok(TailSummary {
                monotonic: true,
                errors: vec!["tail failed to stop within two seconds".into()],
                ..TailSummary::default()
            })
        }
    }
}

/// Runs one live measurement against a fresh fixture and returns its report.
///
/// The caller supplies a fresh identically seeded fixture for every paired run.
///
/// # Errors
///
/// Returns an error when the ledger would exceed its bound, the tail cannot be connected or is
/// not ready, or the HTTP client cannot be built.
pub async fn run_live(
    fixture: &Fixture,
    args: &Args,
    seed: &Seed,
    comparison: bool,
    tail_rate: f64,
) -> Result<Value> {
    let duration = args.duration + args.live_warmup_seconds;
    if duration * tail_rate / (MATCHING_EVERY as f64) > MAX_TRACKED_MATCHES {
        return Err("live workload exceeds bounded matching-observation ledger".into());
    }

    let http = Http::new(&fixture.base, &fixture.read_token, &fixture.ingest_token)?;
    let before = http.request("/metrics", None, true).await;
    let ledger: Ledger = Arc::new(Mutex::new(BTreeMap::new()));
    let (stop, mut tail_task) = start_tail(&http, comparison, &ledger).await?;

    let with_queries = args.profile != "tail";
    let plan = live_plan(args, duration, tail_rate, with_queries);
    let promised = promised_counts(&plan, args);
    let start = Instant::now() + START_DELAY;
    let measured_start = start + Duration::from_secs_f64(args.live_warmup_seconds);
    let measured_end = measured_start + Duration::from_secs_f64(args.duration);
    let context = LiveContext {
        args: args.clone(),
        seed: seed.clone(),
        comparison,
        http: http.clone(),
        ledger: ledger.clone(),
        start,
        measured_start,
        measured_end,
    };
    let (accounting, elapsed, drain) = offer(plan, context).await;

    // Give the stream a bounded opportunity to account for every durable matching ID.
    await_observations(&ledger, &tail_task).await;
    let tail = stop_tail(&stop, &mut tail_task).await?;
    let tail = observation_report(&lock_ledger(&ledger), &tail);

    let after = http.request("/metrics", None, true).await;
    let final_metrics = http::metrics(&after.1);
    let resources_released = after.0 == HTTP_OK
        && final_metrics.get("logbrook_tail_active_subscribers") == Some(&0.0)
        && final_metrics.get("logbrook_storage_readers_busy") == Some(&0.0);

    let all_accounted = all_offers_accounted(&accounting);
    let correct = all_accounted
        && resources_released
        && before.0 == HTTP_OK
        && live_run_clean(&accounting, &tail);

    let offered = accounting.count("offered_ingest");
    let mut report = json!({
        "expected_offered_ingest": promised.offered_ingest,
        "expected_measured_matching_offers": promised.matching_measured,
        "expected_offered_query": promised.offered_query,
        "expected_measured_queries": promised.measured_query,
        "comparison": comparison,
        "correct": correct,
        "accounting_complete": all_accounted,
        "resources_released": resources_released,
        "warmup_seconds": args.live_warmup_seconds,
        "measured_seconds": args.duration,
        "offered_rate": tail_rate,
        "query_rate": if with_queries { args.query_rate } else { 0.0 },
        "matching_offered": offered.div_ceil(MATCHING_EVERY),
        "matching_fraction_offered": offered.div_ceil(MATCHING_EVERY) as f64
            / offered.max(1) as f64,
        "ingest_clients": INGEST_WORKERS,
        "query_clients": u64::from(with_queries),
        "client_queue_capacity": QUEUE_CAPACITY,
        "batch_size": BATCH,
        "elapsed_seconds": elapsed,
        "drain_seconds": drain,
        "counts": accounting.counts,
        "statuses": accounting.statuses,
        "latency_ms": latency_by_operation(&accounting, 0),
        "service_latency_ms": latency_by_operation(&accounting, 1),
        "client_queue_latency_ms": latency_by_operation(&accounting, 2),
        "scheduler_lag_ms": accounting
            .lag
            .iter()
            .map(|(name, samples)| (name.clone(), samples.report()))
            .collect::<BTreeMap<_, _>>(),
        "committed_per_second": accounting.count("committed_in_scheduled_horizon") as f64
            / args.duration,
        "problems": accounting.problems,
        "worker_errors": accounting.worker_errors,
        "failures": accounting.failures,
        "tail": tail,
        "metrics_before": {"status": before.0, "metrics": http::metrics(&before.1)},
        "metrics_after": {"status": after.0, "metrics": final_metrics},
    });
    mark_load_achieved(&mut report, with_queries);
    Ok(report)
}

/// Starts the tail stream for a run and waits for its first checkpoint. Returns the stop signal
/// and the task that owns the stream.
async fn start_tail(
    http: &Http,
    comparison: bool,
    ledger: &Ledger,
) -> Result<(watch::Sender<bool>, JoinHandle<TailSummary>)> {
    let (stop, stopped) = watch::channel(false);
    let (mut tail_task, ready) = spawn_tail(http, comparison, ledger.clone(), stopped)?;
    await_tail_ready(ready, &stop, &mut tail_task).await?;
    Ok((stop, tail_task))
}

/// Offers every planned schedule at its fixed time, and returns the accounting with the elapsed
/// and drain times.
async fn offer(plan: Plan, context: LiveContext) -> (Accounting, f64, f64) {
    let start = context.start;
    let context = Arc::new(context);
    let work: Work = Arc::new(move |job| {
        let context = context.clone();
        Box::pin(async move {
            if job.queue == 0 {
                ingest_job(&context, job).await
            } else {
                query_job(&context, job).await
            }
        })
    });
    engine::execute_at(
        plan.schedules,
        plan.workers,
        plan.capacities,
        plan.names,
        work,
        DRAIN_TIMEOUT,
        start,
    )
    .await
}

/// Whether every offered ingest was accepted, rejected, ambiguous or dropped by the client.
fn all_offers_accounted(accounting: &Accounting) -> bool {
    accounting.count("offered_ingest")
        == accounting.count("accepted_ingest")
            + accounting.count("rejected_ingest")
            + accounting.count("ambiguous_ingest")
            + accounting.count("client_dropped_ingest")
}

/// Whether a live run had no problems, worker errors or failed queries, and its tail observed
/// every matching event in the order and selection it was promised.
fn live_run_clean(accounting: &Accounting, tail: &Value) -> bool {
    accounting.problems.is_empty()
        && accounting.worker_errors.is_empty()
        && accounting.count("failed_query") == 0
        && accounting.count("ambiguous_ingest") == 0
        && tail["missing_matching_committed"] == 0
        && tail["warmup_missing_matching_committed"] == 0
        && tail["duplicates"] == 0
        && tail["unknown_sequences"] == 0
        && tail["wrong_selection"] == 0
        && tail["gaps"] == 0
        && tail["monotonic"] == true
        && tail["errors"].as_array().is_some_and(Vec::is_empty)
}

/// The latency samples of one measurement position (`0` end to end, `1` service, `2` queue) for
/// every operation.
fn latency_by_operation(accounting: &Accounting, position: usize) -> BTreeMap<String, Value> {
    accounting
        .latency
        .iter()
        .map(|(operation, samples)| (operation.clone(), samples[position].report()))
        .collect()
}

/// Adds the two achievement flags: whether the tail delivered everything it promised, and
/// whether the queries were sent and answered at the normal rate when queries are part of the
/// profile.
fn mark_load_achieved(report: &mut Value, with_queries: bool) {
    report["tail_delivery_complete"] = json!(tail_delivery_complete(report));
    report["normal_query_load_achieved"] = if with_queries {
        json!(normal_query_load_achieved(report))
    } else {
        Value::Null
    };
    report["load_achieved"] = json!(
        tail_delivery_complete(report) && (!with_queries || normal_query_load_achieved(report))
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn checkpoint_readiness_and_observation_use_driver_clock() {
        let now = Instant::now();
        let ledger = Arc::new(Mutex::new(BTreeMap::from([(
            0,
            Observation {
                scheduled: now - Duration::from_millis(20),
                acknowledged: None,
                observed: None,
                accepted: true,
                measured: true,
            },
        )])));
        let (ready, started) = oneshot::channel();
        let mut ready = Some(ready);
        let mut summary = TailSummary {
            monotonic: true,
            ..TailSummary::default()
        };
        summary
            .frame(
                "event: cursor\nid: opaque-token\ndata: caught up\n\n",
                &ledger,
                &mut ready,
            )
            .unwrap();
        started.await.unwrap();
        let frame = format!(
            "event: log\nid: another-opaque-token\ndata: {}\n\n",
            json!({"id":"12","attributes":{"liveSequence":0,"liveRun":MATCH_LOGGER}})
        );
        summary.frame(&frame, &ledger, &mut ready).unwrap();

        // Durable ACK can lose the race to the stream; retain its negative sign.
        ledger.lock().unwrap().get_mut(&0).unwrap().acknowledged =
            Some(Instant::now() + Duration::from_millis(10));

        let report = observation_report(&ledger.lock().unwrap(), &summary);
        assert_eq!(report["observed_before_acknowledgement"], 1);
        assert_eq!(report["missing_matching_committed"], 0);
        assert_eq!(report["scheduled_offer_to_observation_ms"]["samples"], 1);
        assert_eq!(
            report["durable_acknowledgement_to_observation_ms"]["samples"],
            0
        );

        summary.frame(&frame, &ledger, &mut ready).unwrap();
        assert_eq!(summary.duplicates, 1);
        assert!(!summary.monotonic);
    }

    #[test]
    fn missing_commits_and_warmup_are_accounted_separately() {
        let now = Instant::now();
        let ledger = BTreeMap::from([
            (
                0,
                Observation {
                    scheduled: now,
                    acknowledged: Some(now),
                    observed: None,
                    accepted: true,
                    measured: true,
                },
            ),
            (
                100,
                Observation {
                    scheduled: now,
                    acknowledged: Some(now),
                    observed: Some(now),
                    accepted: true,
                    measured: false,
                },
            ),
            (
                200,
                Observation {
                    scheduled: now,
                    acknowledged: Some(now),
                    observed: None,
                    accepted: false,
                    measured: true,
                },
            ),
        ]);
        let report = observation_report(&ledger, &TailSummary::default());
        assert_eq!(report["expected_matching_committed"], 1);
        assert_eq!(report["missing_matching_committed"], 1);
        assert_eq!(report["warmup_matching_committed"], 1);
        assert_eq!(report["warmup_matching_observed"], 1);
    }

    #[test]
    fn fixed_schedule_gives_one_percent_matching_live_sequences() {
        let jobs: Vec<_> = schedule(10.0, 100.0, BATCH, 0).jobs().collect();
        assert_eq!(jobs.iter().map(|job| job.count).sum::<u64>(), 1000);

        let matching = jobs
            .iter()
            .flat_map(|job| job.sequence * BATCH..job.sequence * BATCH + job.count)
            .filter(|sequence| sequence.is_multiple_of(100))
            .count();
        assert_eq!(matching, 10);
    }
}
