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

#[derive(Clone, Debug)]
pub struct Job {
    pub offset: f64,
    pub sequence: u64,
    pub count: u64,
    pub queue: usize,
}
#[derive(Clone)]
pub struct Schedule {
    pub duration: f64,
    pub rate: f64,
    pub batch: u64,
    pub burst_rate: f64,
    pub burst_start: f64,
    pub burst_duration: f64,
    pub queue: usize,
    pub total: Option<u64>,
}
impl Schedule {
    pub fn jobs(&self) -> impl Iterator<Item = Job> + '_ {
        let mut elapsed = 0.0;
        let mut sequence = 0;
        let mut units = 0;
        std::iter::from_fn(move || {
            if elapsed >= self.duration - 1e-9
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

pub struct Completion {
    pub operation: String,
    pub status: u16,
    pub counters: BTreeMap<String, u64>,
    pub accepted_seconds: BTreeMap<i64, u64>,
    pub problem: Option<String>,
    pub failure: Option<Value>,
}
impl Completion {
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

#[derive(Default)]
pub struct Accounting {
    pub counts: BTreeMap<String, u64>,
    pub seconds: BTreeMap<i64, u64>,
    pub statuses: BTreeMap<String, u64>,
    pub latency: BTreeMap<String, [Samples; 3]>,
    pub lag: BTreeMap<String, Samples>,
    pub problems: Vec<String>,
    pub worker_errors: Vec<Value>,
    pub failures: Vec<Value>,
    closed: bool,
}
impl Accounting {
    pub fn add(&mut self, name: &str, count: u64) {
        if !self.closed {
            *self.counts.entry(name.into()).or_default() += count;
        }
    }
    fn offer(&mut self, name: &str, count: u64, dropped: bool, lag: f64) {
        self.add(&format!("offered_{name}"), count);
        self.add(&format!("scheduled_{name}_jobs"), 1);
        if dropped {
            self.add(&format!("client_dropped_{name}"), count);
        }
        self.lag.entry(name.into()).or_default().add(lag);
    }

    pub fn count(&self, name: &str) -> u64 {
        self.counts.get(name).copied().unwrap_or_default()
    }
    fn complete(&mut self, completion: Completion, scheduled: Instant, started: Instant) {
        if self.closed {
            return;
        }
        let end = Instant::now();
        let samples = self
            .latency
            .entry(completion.operation.clone())
            .or_default();
        samples[0].add(end.saturating_duration_since(scheduled).as_secs_f64() * 1000.0);
        samples[1].add((end - started).as_secs_f64() * 1000.0);
        samples[2].add(started.saturating_duration_since(scheduled).as_secs_f64() * 1000.0);
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
            && self.failures.len() < 50
        {
            self.failures.push(failure);
        }
        if let Some(problem) = completion.problem {
            self.add("correctness_failures", 1);
            if self.problems.len() < 50 {
                self.problems.push(problem);
            }
        }
    }
}
pub type Work = Arc<dyn Fn(Job) -> BoxFuture<'static, Result<Completion>> + Send + Sync>;

pub async fn execute(
    schedules: Vec<Schedule>,
    workers: Vec<usize>,
    capacities: Vec<usize>,
    names: Vec<String>,
    work: Work,
    drain: Duration,
) -> (Accounting, f64, f64) {
    let state = Arc::new(Mutex::new(Accounting::default()));
    let (abort, mut interrupted) = watch::channel(false);
    let start = Instant::now() + Duration::from_millis(100);
    let mut senders = Vec::new();
    let mut tasks = JoinSet::new();
    for (queue, number) in workers.into_iter().enumerate() {
        let (tx, rx) = mpsc::channel::<Job>(capacities[queue]);
        senders.push(tx);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        for worker in 0..number {
            let rx = rx.clone();
            let work = work.clone();
            let state = state.clone();
            let abort = abort.clone();
            let mut stop = abort.subscribe();
            tasks.spawn(async move {
                let result = AssertUnwindSafe(async {
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
                            .expect("accounting lock")
                            .complete(completion, scheduled, begun);
                    }
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                })
                .catch_unwind()
                .await;

                let error = match result {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(("WorkerError", error.to_string())),
                    Err(_) => Some(("Panic", "unexpected worker panic".into())),
                };
                if let Some((kind, message)) = error {
                    state
                        .lock()
                        .expect("accounting lock")
                        .worker_errors
                        .push(json!({
                            "worker": format!("queue-{queue}-{worker}"),
                            "type": kind,
                            "message": message,
                        }));
                    let _ = abort.send(true);
                }
            });
        }
    }
    let mut schedulers = JoinSet::new();
    let horizon = schedules.iter().map(|s| s.duration).fold(0.0, f64::max);
    for schedule in schedules {
        let tx = senders[schedule.queue].clone();
        let state = state.clone();
        let name = names[schedule.queue].clone();
        let mut stop = abort.subscribe();
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
                    * 1000.0;
                let count = job.count;
                let dropped = tx.try_send(job).is_err();
                let mut stats = state.lock().expect("accounting lock");
                stats.offer(&name, count, dropped, lag);
            }
        });
    }
    drop(senders);
    while schedulers.join_next().await.is_some() {}
    if !*interrupted.borrow() {
        tokio::select! {_=interrupted.changed()=>{},_=tokio::time::sleep_until(start+Duration::from_secs_f64(horizon))=>{}}
    }
    // One deadline covers all workers. Aborting and joining ensures that returned
    // reports cannot change after a timeout or an unexpected worker failure.
    let deadline = Instant::now() + drain;
    if tokio::time::timeout_at(deadline, async {
        while tasks.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        state.lock().expect("accounting lock").worker_errors.push(json!({"type":"Timeout","workers":tasks.len(),"message":"shared worker drain deadline exceeded"}));
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    let end = Instant::now();
    let mut accounting = Arc::try_unwrap(state)
        .ok()
        .expect("all workers joined")
        .into_inner()
        .expect("accounting lock");
    accounting.closed = true;
    (
        accounting,
        end.saturating_duration_since(start).as_secs_f64(),
        end.saturating_duration_since(start + Duration::from_secs_f64(horizon))
            .as_secs_f64(),
    )
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
