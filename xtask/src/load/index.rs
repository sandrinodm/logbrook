use super::{
    engine::{self, Completion, Schedule, Work},
    http::Http,
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
use tokio::{sync::watch, time::Instant};
const INDICES: [&str; 2] = ["payments", "orders"];

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
    fn validate(&self) -> Result<()> {
        if !self.duration.is_finite()
            || !(0.0..=3600.0).contains(&self.duration)
            || self.duration == 0.0
            || !self.rate.is_finite()
            || !(0.0..=10000.0).contains(&self.rate)
            || self.rate == 0.0
            || !self.query_rate.is_finite()
            || !(0.0..=1000.0).contains(&self.query_rate)
            || self.query_rate == 0.0
            || !(1..=1000).contains(&self.batch_size)
            || self.padding_bytes > 4096
        {
            return Err("duration/rate/batch/padding outside bounded probe limits".into());
        }
        Ok(())
    }
}

async fn metric(http: &Http, start: Instant) -> Value {
    let (status, text) = http.request("/metrics", None, true).await;
    json!({"offset_seconds":start.elapsed().as_secs_f64(),"status":status,"text":text})
}

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
    let lower = crate::now_ms() - 1000;
    let upper = lower + ((args.duration + 120.0) * 1000.0) as i64;
    let planned = (args.rate * args.duration / 2.0) as u64;
    let (stop, receiver) = watch::channel(false);
    let mut tails = Vec::new();
    let mut observations = Vec::new();
    for n in 0..4 {
        let index = INDICES[n / 2];
        let observed = Arc::new(AtomicU64::new(0));
        tails.push(tail::spawn_count(
            http.clone(),
            format!("/indexes/{index}/logs/tail"),
            BTreeMap::new(),
            n,
            Some(index.into()),
            receiver.clone(),
            observed.clone(),
        ));
        observations.push(observed);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let observer_http = http.clone();
    let container = container.map(str::to_owned);
    let observer_container = container.clone();
    let mut observing = receiver.clone();
    let observer = tokio::spawn(async move {
        let mut metrics = Vec::new();
        let mut resources = Vec::new();
        let mut errors = Vec::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                biased;
                _ = observing.changed() => break,
                _ = ticker.tick() => {
                    metrics.push(metric(&observer_http, start).await);
                    if let Some(container) = &observer_container {
                        let result = runtime::docker(&["stats", "--no-stream", "--format", "{{json .}}", container])
                            .await.and_then(|s| Ok(serde_json::from_str::<Value>(&s)?));
                        match result {
                            Ok(value) => resources.push(value),
                            Err(error) => errors.push(json!({"type": "ObserverError", "message": error.to_string()})),
                        }
                    }
                }
            }
        }
        (metrics, resources, errors)
    });
    let worker_http = http.clone();
    let options = args.clone();
    let work: Work = Arc::new(move |job| {
        let http = worker_http.clone();
        let args = options.clone();
        Box::pin(async move {
            let index = INDICES[if job.queue < 2 {
                job.queue
            } else {
                job.sequence as usize % 2
            }];
            let mut c = if job.queue < 2 {
                let now = crate::now_ms();
                let events: Vec<_> = (0..job.count)
                    .map(|n| {
                        json!({
                            "time": now,
                            "level": 30,
                            "msg": format!("{index} {}", "x".repeat(args.padding_bytes)),
                            "sequence": job.sequence * args.batch_size + n,
                            "test_index": index,
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
                let mut c = Completion::new("ingest", status);
                c.counters.insert("wire_ingest_bytes".into(), bytes as u64);
                if status == 200 && payload["accepted"].as_u64() == Some(job.count) {
                    c.counters.insert(format!("accepted_{index}"), job.count);
                } else {
                    c.counters.insert("ingest_failed_units".into(), job.count);
                    c.failure = Some(
                        json!({"kind":"ingest","index":index,"status":status,"payload":payload.to_string().chars().take(500).collect::<String>()}),
                    );
                    if status == 0 {
                        c.counters
                            .insert("ambiguous_ingest_events".into(), job.count);
                    }
                    if status == 200 {
                        c.problem =
                            Some("ingest accepted count does not match submitted batch".into());
                    }
                }
                c
            } else {
                let count = job.sequence % 3 == 0;
                let endpoint = if count { "/count" } else { "" };
                let (status, payload) = http
                    .request(
                        &format!("/indexes/{index}/logs{endpoint}?from={lower}&to={upper}"),
                        None,
                        false,
                    )
                    .await;
                let mut c = Completion::new("query", status);
                let valid = status == 200
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
                    c.failure = Some(
                        json!({"kind":"query","index":index,"status":status,"payload":payload.to_string().chars().take(500).collect::<String>()}),
                    );
                    if status == 200 {
                        c.status = 599;
                        c.problem =
                            Some("cross-index query contamination or malformed response".into());
                    }
                }
                c
            };
            c.counters
                .insert(format!("{}_status_{}", c.operation, c.status), 1);
            Ok(c)
        })
    });
    let mut schedules = Vec::new();
    for queue in 0..2 {
        schedules.push(Schedule {
            duration: args.duration,
            rate: args.rate / 2.0,
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
        queue: 2,
        total: Some((args.query_rate * args.duration) as u64),
    });
    let (mut stats, _, request_drain) = engine::execute(
        schedules,
        vec![1, 1, 4],
        vec![64, 64, 256],
        vec!["payments".into(), "orders".into(), "queries".into()],
        work,
        Duration::from_secs(30),
    )
    .await;
    let dropped_ingest =
        stats.count("client_dropped_payments") + stats.count("client_dropped_orders");
    let dropped_queries = stats.count("client_dropped_queries");
    stats
        .counts
        .insert("client_dropped_ingest".into(), dropped_ingest);
    stats
        .counts
        .insert("client_dropped_query".into(), dropped_queries);
    let tail_start = Instant::now();
    let tail_deadline = tail_start + Duration::from_secs(10);
    while stats.worker_errors.is_empty()
        && Instant::now() < tail_deadline
        && observations.iter().enumerate().any(|(n, c)| {
            c.load(Ordering::Relaxed) < stats.count(&format!("accepted_{}", INDICES[n / 2]))
        })
    {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let tail_drain = tail_start.elapsed().as_secs_f64();
    let _ = stop.send(true);
    let mut tails = tail::finish(tails).await;
    // Cancellation must also bound an in-flight Docker resource sampler.
    let mut observer = observer;
    let (mut metrics, resources, mut observer_errors) =
        match tokio::time::timeout(Duration::from_secs(20), &mut observer).await {
            Ok(Ok(values)) => values,
            other => {
                observer.abort();
                let _ = observer.await;
                (
                    Vec::new(),
                    Vec::new(),
                    vec![
                        json!({"type":"Timeout","message":format!("observer shutdown: {other:?}")}),
                    ],
                )
            }
        };
    let mut counts = json!({});
    let mut isolation = json!({});
    let mut verification_errors = Vec::new();
    for (n, index) in INDICES.iter().enumerate() {
        let path = format!("/indexes/{index}/logs/count?from={lower}&to={upper}");
        let (code, value) = http.request(&path, None, false).await;
        if code != 200 || value["count"].as_u64().is_none() {
            verification_errors.push(
                json!({"path":path,"type":"VerificationError","status":code,"payload":value}),
            );
        }
        counts[index] = json!({"status":code,"count":value["count"]});
        let path = format!("{path}&message={}", INDICES[1 - n]);
        let (code, value) = http.request(&path, None, false).await;
        if code != 200 || value["count"].as_u64().is_none() {
            verification_errors.push(
                json!({"path":path,"type":"VerificationError","status":code,"payload":value}),
            );
        }
        isolation[index] = json!(code == 200 && value["count"] == 0);
    }
    metrics.push(metric(&http, start).await);
    let labels = INDICES.iter().all(|index| {
        metrics.iter().any(|sample| {
            sample["status"] == 200
                && sample["text"]
                    .as_str()
                    .is_some_and(|text| text.contains(&format!("{{index=\"{index}\"}}")))
        })
    });
    let cgroup = if let Some(container) = &container {
        match crate::container::probe(container, &["memory.peak".into(), "memory.events".into()])
            .await
        {
            Ok(v) => Some(Value::String(format!(
                "{}{}",
                v["memory.peak"].as_str().unwrap_or_default(),
                v["memory.events"].as_str().unwrap_or_default()
            ))),
            Err(error) => {
                observer_errors.push(json!({"type":"CgroupError","message":error.to_string()}));
                None
            }
        }
    } else {
        None
    };
    let minimum = (args.duration / 10.0).ceil() as usize + 1;
    let passed = INDICES.iter().all(|index| {
        counts[index]["status"] == 200
            && counts[index]["count"].as_u64() == Some(planned)
            && stats.count(&format!("accepted_{index}")) == planned
            && isolation[index] == true
    }) && [
        "client_dropped_ingest",
        "client_dropped_query",
        "ingest_failed_units",
        "query_failed_units",
    ]
    .iter()
    .all(|k| stats.count(k) == 0)
        && labels
        && stats.worker_errors.is_empty()
        && stats.problems.is_empty()
        && observer_errors.is_empty()
        && verification_errors.is_empty()
        && metrics.len() >= minimum
        && metrics.iter().all(|s| s["status"] == 200)
        && tails.iter().all(|t| {
            t["status"] == 200
                && t["observed"].as_u64() == Some(planned)
                && t["gaps"] == 0
                && t["errors"] == 0
                && t["isolated"] == true
                && t["monotonic"] == true
        });
    for tail in &mut tails {
        let errors = if tail["errors"] == 0 {
            Vec::new()
        } else {
            vec![tail["error"].as_str().unwrap_or("tail failure").to_owned()]
        };
        tail["errors"] = json!(errors);
    }

    let service_refusals: BTreeMap<_, _> = ["ingest", "query"]
        .into_iter()
        .flat_map(|kind| {
            [429, 503, 507]
                .into_iter()
                .map(move |status| (kind, status))
        })
        .map(|(kind, status)| {
            (
                format!("{kind}_{status}"),
                stats.count(&format!("{kind}_status_{status}")),
            )
        })
        .collect();
    let latency = |operation: &str, position: usize| {
        stats
            .latency
            .get(operation)
            .map(|s| s[position].report())
            .unwrap_or_else(|| super::semantics::Samples::default().report())
    };
    Ok(json!({
        "started_at":started_at,
        "started_at_ms":lower+1000,
        "planned_per_index":planned,
        "counts":stats.counts,
        "final_counts":counts,
        "isolated_queries":isolation,
        "tails":tails,
        "request_drain_seconds":request_drain,
        "tail_drain_seconds":tail_drain,
        "ingest_ms":latency("ingest",1),
        "query_ms":latency("query",1),
        "ingest_scheduled_ms":latency("ingest",0),
        "query_scheduled_ms":latency("query",0),
        "scheduler_lag_ms":stats.lag.iter().map(|(n,s)|(n,s.report())).collect::<BTreeMap<_,
        _>>(),
        "failure_examples":stats.failures,
        "correctness_problems":stats.problems,
        "service_refusals":service_refusals,
        "metrics":metrics,
        "index_metric_labels":labels,
        "worker_errors":stats.worker_errors,
        "observer_errors":observer_errors,
        "verification_errors":verification_errors,
        "expected_minimum_metrics_samples":minimum,
        "docker_stats":resources,
        "cgroup_memory":cgroup,
        "passed":passed
    }))
}

pub async fn run(args: Args) -> Result<()> {
    args.validate()?;
    let mut lifecycle = json!({"mode":if args.image.is_some(){"docker"}else{"native"},"binary":args.binary,"image":args.image});
    let config = "retention_days=7\nmax_size_gb=20\n[indexes.payments]\nretention_days=7\nmax_size_gb=20\n[indexes.orders]\nretention_days=14\nmax_size_gb=20\n";
    let mut fixture = match Fixture::start_config(args.binary.clone(), args.image.clone(), config)
        .await
    {
        Ok(fixture) => fixture,
        Err(error) => {
            let report = json!({"version":1,"parameters":args,"lifecycle":lifecycle,"workload":{"passed":false},"passed":false,"error":error.to_string()});
            super::write_report(&args.output, &report)?;
            return Err(error);
        }
    };
    let setup: Result<()> = async {
        if args.image.is_none() {
            lifecycle["binary_sha256"] = json!(super::file_sha256(&args.binary)?);
            lifecycle["resource_limits"] = json!("native process, no cgroup constraint");
        }

        if let Some(container) = &fixture.container {
            lifecycle["container"] = json!(container);
            lifecycle["cpus"] = json!(2);
            lifecycle["memory_bytes"] = json!(2147483648_u64);
            lifecycle["image_inspect"] = serde_json::from_str(
                &runtime::docker(&["image", "inspect", args.image.as_deref().expect("image")])
                    .await?,
            )?;
        }
        Ok(())
    }
    .await;
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
    // Inspect the stopped server before removing its disposable resources, so
    // a clean resource removal cannot conceal an OOM or abnormal server exit.
    let stopped = fixture.stop().await;
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
        if let Ok(logs) = std::fs::read_to_string(fixture.root.path().join("server.log")) {
            let tail = logs
                .chars()
                .rev()
                .take(12000)
                .collect::<String>()
                .chars()
                .rev()
                .collect::<String>();
            lifecycle["server_logs"] = json!(tail);
        }
    }
    let stop_succeeded = stopped.is_ok();
    if let Err(error) = stopped {
        lifecycle["stop_error"] = json!(error.to_string());
    }
    let cleanup = fixture.shutdown().await;
    lifecycle["cleanup_succeeded"] = json!(cleanup.is_ok());
    lifecycle["container_and_volume_removed"] = json!(cleanup.is_ok());

    if let Err(error) = cleanup {
        lifecycle["cleanup_error"] = json!(error.to_string());
    }
    let workload = match workload {
        Ok(report) => report,
        Err(error) => json!({"passed":false,"error":error.to_string()}),
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
        "scope": "fresh two-index workload correctness and measured latency, not sustained capacity",
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
