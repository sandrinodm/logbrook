//! Open-loop load engine: schedules jobs on a clock, runs them on bounded queues and accounts
//! for every offered, dropped, completed and failed job.
use super::semantics::Samples;
use crate::Result;
use futures_util::{FutureExt, future::BoxFuture};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
    time::Instant,
};

/// Delay between the epoch of an execution and the first scheduled job.
const START_DELAY: Duration = Duration::from_millis(100);

/// Tolerance, in seconds, for comparing a schedule's end with its horizon.
const HORIZON_EPSILON: f64 = 1e-9;

/// Milliseconds in one second, for reporting latencies in milliseconds.
const MILLIS_PER_SEC: f64 = 1000.0;

/// Most failures and problems kept in a report; later ones are counted but not stored.
const MAX_RETAINED_ENTRIES: usize = 50;

/// Message for the accounting lock, which is only poisoned by a panic while it is held.
const ACCOUNTING_LOCK: &str = "accounting lock";

/// One batch of jobs that a schedule offers at `offset` seconds after the epoch.
#[derive(Clone, Debug)]
pub struct Job {
    /// Seconds after the epoch at which the batch is due.
    pub offset: f64,
    /// Position of the batch within its schedule.
    pub sequence: u64,
    /// Number of operations in the batch.
    pub count: u64,
    /// Index of the queue that runs the batch.
    pub queue: usize,
}

/// An open-loop arrival process: batches at a steady rate, optionally at a burst rate for a
/// window, for a fixed horizon.
#[derive(Clone)]
pub struct Schedule {
    /// Horizon in seconds; no batch is offered at or after it.
    pub duration: f64,
    /// Steady offered rate, in operations per second.
    pub rate: f64,
    /// Operations per batch.
    pub batch: u64,
    /// Rate during the burst window, in operations per second; zero disables the burst.
    pub burst_rate: f64,
    /// Start of the burst window, in seconds after the epoch.
    pub burst_start: f64,
    /// Length of the burst window, in seconds.
    pub burst_duration: f64,
    /// Index of the queue that receives this schedule's batches.
    pub queue: usize,
    /// Total operations to offer, when the schedule is bounded by count rather than time.
    pub total: Option<u64>,
}

impl Schedule {
    /// The batches in order, stopping at the horizon, at the total or when the rate is zero.
    pub fn jobs(&self) -> impl Iterator<Item = Job> + '_ {
        let mut elapsed = 0.0;
        let mut sequence = 0;
        let mut units = 0;
        std::iter::from_fn(move || {
            if elapsed >= self.duration - HORIZON_EPSILON
                || self.total.is_some_and(|total| units >= total)
                || self.rate <= 0.0
            {
                return None;
            }

            let count = self
                .total
                .map(|n| self.batch.min(n - units))
                .unwrap_or(self.batch);
            let job = Job {
                offset: elapsed,
                sequence,
                count,
                queue: self.queue,
            };

            let rate = if self.burst_rate > 0.0
                && elapsed >= self.burst_start
                && elapsed < self.burst_start + self.burst_duration
            {
                self.burst_rate
            } else {
                self.rate
            };
            elapsed += self.batch as f64 / rate;
            sequence += 1;
            units += count;
            Some(job)
        })
    }
}

/// What one completed operation contributes to the accounting.
pub struct Completion {
    /// Operation name, used in latency and status keys.
    pub operation: String,
    /// HTTP status, or another code the operation reports.
    pub status: u16,
    /// Counters to add to the accounting.
    pub counters: BTreeMap<String, u64>,
    /// Accepted operations per wall-clock second.
    pub accepted_seconds: BTreeMap<i64, u64>,
    /// A correctness problem to report, if the operation observed one.
    pub problem: Option<String>,
    /// A failure record to keep for the report, if the operation failed.
    pub failure: Option<Value>,
}

impl Completion {
    /// A completion with no counters, seconds, problem or failure.
    pub fn new(operation: &str, status: u16) -> Self {
        Self {
            operation: operation.into(),
            status,
            counters: BTreeMap::new(),
            accepted_seconds: BTreeMap::new(),
            problem: None,
            failure: None,
        }
    }
}

/// Everything that one execution counts, sampled and reports, shared between its tasks.
#[derive(Default)]
pub struct Accounting {
    /// Named operation counters.
    pub counts: BTreeMap<String, u64>,
    /// Accepted operations per wall-clock second.
    pub seconds: BTreeMap<i64, u64>,
    /// Completions per `operation:status`.
    pub statuses: BTreeMap<String, u64>,
    /// Per operation: scheduled-to-end, start-to-end and scheduled-to-start latency samples.
    pub latency: BTreeMap<String, [Samples; 3]>,
    /// Scheduler lag per queue, in milliseconds.
    pub lag: BTreeMap<String, Samples>,
    /// Correctness problems, capped at `MAX_RETAINED_ENTRIES`.
    pub problems: Vec<String>,
    /// Worker failures, panics and timeouts.
    pub worker_errors: Vec<Value>,
    /// Failure records, capped at `MAX_RETAINED_ENTRIES`.
    pub failures: Vec<Value>,
    closed: bool,
}

impl Accounting {
    /// Adds `count` to the counter `name`, unless the accounting has been closed.
    pub fn add(&mut self, name: &str, count: u64) {
        if !self.closed {
            *self.counts.entry(name.into()).or_default() += count;
        }
    }

    /// Records a batch that was offered, its scheduler lag, and whether the queue dropped it.
    fn offer(&mut self, name: &str, count: u64, dropped: bool, lag: f64) {
        self.add(&format!("offered_{name}"), count);
        self.add(&format!("scheduled_{name}_jobs"), 1);
        if dropped {
            self.add(&format!("client_dropped_{name}"), count);
        }

        self.lag.entry(name.into()).or_default().add(lag);
    }

    /// The value of the counter `name`, or zero when it was never added.
    pub fn count(&self, name: &str) -> u64 {
        self.counts.get(name).copied().unwrap_or_default()
    }

    /// Accounts for one finished operation, unless the accounting has been closed.
    fn complete(&mut self, completion: Completion, scheduled: Instant, started: Instant) {
        if self.closed {
            return;
        }

        let end = Instant::now();
        let samples = self
            .latency
            .entry(completion.operation.clone())
            .or_default();
        samples[0].add(end.saturating_duration_since(scheduled).as_secs_f64() * MILLIS_PER_SEC);
        samples[1].add((end - started).as_secs_f64() * MILLIS_PER_SEC);
        samples[2].add(started.saturating_duration_since(scheduled).as_secs_f64() * MILLIS_PER_SEC);

        *self
            .statuses
            .entry(format!("{}:{}", completion.operation, completion.status))
            .or_default() += 1;

        for (name, count) in completion.counters {
            self.add(&name, count);
        }

        for (second, count) in completion.accepted_seconds {
            *self.seconds.entry(second).or_default() += count;
        }

        if let Some(failure) = completion.failure
            && self.failures.len() < MAX_RETAINED_ENTRIES
        {
            self.failures.push(failure);
        }

        if let Some(problem) = completion.problem {
            self.add("correctness_failures", 1);
            if self.problems.len() < MAX_RETAINED_ENTRIES {
                self.problems.push(problem);
            }
        }
    }
}

/// Runs one job and returns what it contributes to the accounting.
pub type Work = Arc<dyn Fn(Job) -> BoxFuture<'static, Result<Completion>> + Send + Sync>;

/// What every worker and scheduler of one execution shares.
struct Run {
    state: Arc<Mutex<Accounting>>,
    abort: watch::Sender<bool>,
    start: Instant,
    work: Work,
}

/// Runs `schedules` against the queues and returns the accounting, the measured elapsed
/// seconds and the seconds spent past the horizon.
///
/// The epoch is `START_DELAY` from now. See [`execute_at`].
pub async fn execute(
    schedules: Vec<Schedule>,
    workers: Vec<usize>,
    capacities: Vec<usize>,
    names: Vec<String>,
    work: Work,
    drain: Duration,
) -> (Accounting, f64, f64) {
    execute_at(
        schedules,
        workers,
        capacities,
        names,
        work,
        drain,
        Instant::now() + START_DELAY,
    )
    .await
}

/// Use one caller-selected monotonic epoch for scheduling and external observations.
///
/// `workers[q]` tasks share the bounded queue `q` of capacity `capacities[q]`. Schedulers
/// offer each batch at its offset from `start`; a full queue drops the batch and counts it.
/// Workers still running `drain` after the horizon are aborted and reported.
pub async fn execute_at(
    schedules: Vec<Schedule>,
    workers: Vec<usize>,
    capacities: Vec<usize>,
    names: Vec<String>,
    work: Work,
    drain: Duration,
    start: Instant,
) -> (Accounting, f64, f64) {
    let (abort, mut interrupted) = watch::channel(false);
    let run = Run {
        state: Arc::new(Mutex::new(Accounting::default())),
        abort,
        start,
        work,
    };

    let mut tasks = JoinSet::new();
    let senders = spawn_workers(&mut tasks, workers, capacities, &run);

    let horizon = schedules.iter().map(|s| s.duration).fold(0.0, f64::max);
    let mut schedulers = JoinSet::new();
    spawn_schedulers(&mut schedulers, schedules, &senders, &names, &run);
    drop(senders);
    while schedulers.join_next().await.is_some() {}

    wait_for_horizon(&mut interrupted, start, horizon).await;
    drain_workers(&mut tasks, &run.state, drain).await;

    let end = Instant::now();
    let mut accounting = Arc::try_unwrap(run.state)
        .ok()
        .expect("all workers joined")
        .into_inner()
        .expect(ACCOUNTING_LOCK);
    accounting.closed = true;
    (
        accounting,
        end.saturating_duration_since(start).as_secs_f64(),
        end.saturating_duration_since(start + Duration::from_secs_f64(horizon))
            .as_secs_f64(),
    )
}

/// Spawns the workers of every queue and returns the sender of each queue. The workers of one
/// queue share a receiver behind an async mutex.
fn spawn_workers(
    tasks: &mut JoinSet<()>,
    workers: Vec<usize>,
    capacities: Vec<usize>,
    run: &Run,
) -> Vec<mpsc::Sender<Job>> {
    let mut senders = Vec::new();
    for (queue, number) in workers.into_iter().enumerate() {
        let (tx, rx) = mpsc::channel::<Job>(capacities[queue]);
        senders.push(tx);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        for worker in 0..number {
            let rx = rx.clone();
            let work = run.work.clone();
            let state = run.state.clone();
            let abort = run.abort.clone();
            let mut stop = abort.subscribe();
            let start = run.start;
            tasks.spawn(async move {
                let result =
                    AssertUnwindSafe(work_until_stopped(&rx, &work, &state, start, &mut stop))
                        .catch_unwind()
                        .await;
                let failure = match result {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(("WorkerError", error.to_string())),
                    Err(_) => Some(("Panic", "unexpected worker panic".into())),
                };
                if let Some((kind, message)) = failure {
                    let label = format!("queue-{queue}-{worker}");
                    record_worker_failure(&state, &abort, label, kind, message);
                }
            });
        }
    }
    senders
}

/// Takes jobs from the shared queue receiver and runs them until the queue closes or `stop`
/// is signalled.
async fn work_until_stopped(
    rx: &tokio::sync::Mutex<mpsc::Receiver<Job>>,
    work: &Work,
    state: &Mutex<Accounting>,
    start: Instant,
    stop: &mut watch::Receiver<bool>,
) -> Result<()> {
    loop {
        let job = tokio::select! {
            biased;
            _ = stop.changed() => break,
            job = async { rx.lock().await.recv().await } => match job {
                Some(job) => job,
                None => break,
            },
        };
        let scheduled = start + Duration::from_secs_f64(job.offset);
        let begun = Instant::now();
        let completion = work(job).await?;
        state
            .lock()
            .expect(ACCOUNTING_LOCK)
            .complete(completion, scheduled, begun);
    }

    Ok(())
}

/// Records a worker failure and signals every other task to stop.
fn record_worker_failure(
    state: &Mutex<Accounting>,
    abort: &watch::Sender<bool>,
    worker: String,
    kind: &str,
    message: String,
) {
    state
        .lock()
        .expect(ACCOUNTING_LOCK)
        .worker_errors
        .push(json!({
            "worker": worker,
            "type": kind,
            "message": message,
        }));
    let _ = abort.send(true);
}

/// Spawns one task per schedule. Each task offers its batches on the schedule's queue at their
/// due times, and counts the ones that the queue refuses as dropped.
fn spawn_schedulers(
    schedulers: &mut JoinSet<()>,
    schedules: Vec<Schedule>,
    senders: &[mpsc::Sender<Job>],
    names: &[String],
    run: &Run,
) {
    for schedule in schedules {
        let tx = senders[schedule.queue].clone();
        let state = run.state.clone();
        let name = names[schedule.queue].clone();
        let mut stop = run.abort.subscribe();
        let start = run.start;
        schedulers.spawn(async move {
            for job in schedule.jobs() {
                let scheduled = start + Duration::from_secs_f64(job.offset);
                tokio::select! {
                    biased;
                    _ = stop.changed() => break,
                    _ = tokio::time::sleep_until(scheduled) => {},
                }

                let lag = Instant::now()
                    .saturating_duration_since(scheduled)
                    .as_secs_f64()
                    * MILLIS_PER_SEC;
                let count = job.count;
                let dropped = tx.try_send(job).is_err();

                let mut stats = state.lock().expect(ACCOUNTING_LOCK);
                stats.offer(&name, count, dropped, lag);
            }
        });
    }
}

/// Returns once `interrupted` is set, or once the epoch plus `horizon` seconds has passed.
async fn wait_for_horizon(interrupted: &mut watch::Receiver<bool>, start: Instant, horizon: f64) {
    if !*interrupted.borrow() {
        tokio::select! {
            _ = interrupted.changed() => {},
            _ = tokio::time::sleep_until(start + Duration::from_secs_f64(horizon)) => {},
        }
    }
}

/// Waits up to `drain` for every worker. Workers still running at the deadline are aborted
/// and joined, and the timeout is recorded as a worker error.
async fn drain_workers(tasks: &mut JoinSet<()>, state: &Mutex<Accounting>, drain: Duration) {
    // One deadline covers all workers. Aborting and joining ensures that returned
    // reports cannot change after a timeout or an unexpected worker failure.
    let deadline = Instant::now() + drain;
    if tokio::time::timeout_at(deadline, async {
        while tasks.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        state
            .lock()
            .expect(ACCOUNTING_LOCK)
            .worker_errors
            .push(json!({
                "type": "Timeout",
                "workers": tasks.len(),
                "message": "shared worker drain deadline exceeded",
            }));

        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schedule(duration: f64) -> Schedule {
        Schedule {
            duration,
            rate: 200.0,
            batch: 1,
            burst_rate: 0.0,
            burst_start: 0.0,
            burst_duration: 0.0,
            queue: 0,
            total: None,
        }
    }

    #[test]
    fn overdue_offers_preserve_scheduler_lag_and_drops() {
        let mut stats = Accounting::default();
        stats.offer("ingest", 100, false, 2000.0);
        stats.offer("ingest", 100, true, 1000.0);
        assert_eq!(stats.count("offered_ingest"), 200);
        assert_eq!(stats.count("client_dropped_ingest"), 100);
        assert_eq!(stats.lag["ingest"].report()["max"], 2000.0);
        assert_eq!(stats.lag["ingest"].report()["samples"], 2);
    }

    #[test]
    fn arrivals_preserve_horizon_and_burst() {
        let mut base = schedule(4.0);
        base.rate = 1000.0;
        base.batch = 100;
        let jobs: Vec<_> = base.jobs().collect();
        assert!((jobs[9].offset - 0.9).abs() < 1e-6);

        base.burst_rate = 5000.0;
        base.burst_start = 1.0;
        base.burst_duration = 2.0;

        let burst: Vec<_> = base.jobs().collect();
        assert!(burst.len() > jobs.len() * 2);
        assert!(burst.windows(2).all(|w| w[0].offset < w[1].offset));
    }

    #[tokio::test]
    async fn external_epoch_is_shared_with_offer_observation() {
        let start = Instant::now() + Duration::from_millis(50);
        let work: Work = Arc::new(move |job| {
            Box::pin(async move {
                assert!(Instant::now() >= start + Duration::from_secs_f64(job.offset));
                Ok(Completion::new("ingest", 200))
            })
        });
        let (accounting, elapsed, _) = execute_at(
            vec![schedule(0.01)],
            vec![1],
            vec![4],
            vec!["ingest".into()],
            work,
            Duration::from_secs(1),
            start,
        )
        .await;
        assert_eq!(accounting.count("offered_ingest"), 2);
        assert!(accounting.worker_errors.is_empty());
        assert!(elapsed >= 0.01);
    }

    #[tokio::test]
    async fn full_queue_drops_are_explicit_and_drain_shared() {
        let work: Work = Arc::new(|_| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok(Completion::new("ingest", 200))
            })
        });
        let now = Instant::now();
        let (stats, _, _) = execute(
            vec![schedule(0.05)],
            vec![4],
            vec![2],
            vec!["ingest".into()],
            work,
            Duration::from_millis(50),
        )
        .await;
        assert!(now.elapsed() < Duration::from_secs(1));
        assert_eq!(stats.count("offered_ingest"), 10);
        assert!(stats.count("client_dropped_ingest") > 0);
        assert_eq!(stats.worker_errors[0]["type"], "Timeout");
    }

    #[tokio::test]
    async fn unexpected_worker_error_stops_long_schedule() {
        let work: Work =
            Arc::new(|_| Box::pin(async { Err("injected programming failure".into()) }));
        let now = Instant::now();
        let (stats, _, _) = execute(
            vec![schedule(30.0)],
            vec![1],
            vec![64],
            vec!["ingest".into()],
            work,
            Duration::from_secs(1),
        )
        .await;
        assert!(now.elapsed() < Duration::from_secs(1));
        assert!(stats.count("offered_ingest") < 6000);
        assert_eq!(stats.worker_errors.len(), 1);
        assert!(stats.statuses.is_empty());
    }

    #[tokio::test]
    async fn normal_work_drains_before_snapshot() {
        let work: Work = Arc::new(|job| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let mut c = Completion::new("ingest", 200);
                c.counters.insert("accepted_ingest".into(), job.count);
                Ok(c)
            })
        });
        let (stats, _, _) = execute(
            vec![schedule(0.03)],
            vec![1],
            vec![64],
            vec!["ingest".into()],
            work,
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(stats.count("accepted_ingest"), 6);
        assert!(stats.worker_errors.is_empty());
    }
}
