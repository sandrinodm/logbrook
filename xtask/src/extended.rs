//! Long container qualification with a historical Parquet fixture prepared offline.

use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use clap::Args;
use serde_json::{Value, json};
use tokio::{process::Command, sync::watch, task::JoinHandle};

use crate::{Result, load::mixed, now_ms, runtime};

const WRITE_TOKEN: &str = "extended-load-write-token";
const READ_TOKEN: &str = "extended-load-read-token";
const DAY_MS: i64 = 86_400_000;
const CONFIG: &str = r#"bind = "0.0.0.0:3100"
retention_ms = 604800000
archive_after_ms = 86400000
maintenance_interval_secs = 1
max_indexes = 1

[ingest_tokens]
extended-load-write-token = "benchmark"

[read_tokens]
extended-load-read-token = []

[storage]
data_dir = "/data"
reader_threads = 2
duckdb_threads = 2
memory_limit = "512MB"
temp_limit = "2GB"
queue_capacity = 16
queue_bytes = 33554432
query_timeout_ms = 5000
max_query_bytes = 8388608
archive_batch_rows = 100000
archive_partition_ms = 3600000
compact_max_files = 16
compact_max_bytes = 134217728
maintenance_timeout_ms = 30000
"#;

#[derive(Args, Clone, Debug)]
pub struct Options {
    #[arg(long)]
    pub image: String,
    #[arg(long, default_value = "artifacts/load-retention")]
    pub output_dir: PathBuf,
    #[arg(long, default_value_t = 600)]
    pub duration: u64,
    #[arg(long, default_value_t = 1_000_000)]
    pub seed_events: u64,
    #[arg(long, default_value_t = 100)]
    pub require_min_archives: usize,
}

fn write_json(path: impl AsRef<Path>, value: &impl serde::Serialize) -> Result<()> {
    let mut file = File::create(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    writeln!(file)?;
    Ok(())
}

async fn docker(args: &[String], timeout: u64) -> Result<Vec<u8>> {
    let output = runtime::output(Command::new("docker").args(args), timeout).await?;
    if !output.status.success() {
        return Err(format!(
            "docker {} failed: {}",
            args.first().map_or("operation", String::as_str),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }

    Ok(output.stdout)
}

async fn state(name: &str) -> Result<Value> {
    Ok(serde_json::from_str::<Value>(&runtime::docker(&["inspect", name]).await?)?[0].clone())
}

async fn ready(name: &str) -> Result<String> {
    let port = runtime::docker(&["port", name, "3100/tcp"]).await?;
    let base = format!(
        "http://127.0.0.1:{}",
        port.rsplit(':').next().ok_or("missing published port")?
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;

    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if let Ok(response) = client.get(format!("{base}/ready")).send().await
                && response.status().is_success()
            {
                return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
            }
            if state(name).await?["State"]["Running"] != true {
                return Err("container stopped before readiness".into());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await??;

    Ok(base)
}

struct ArchiveCopy(Arc<Mutex<Child>>);

impl Drop for ArchiveCopy {
    fn drop(&mut self) {
        let mut child = self.0.lock().expect("archive copy process lock");
        if !matches!(child.try_wait(), Ok(Some(_))) {
            let _ = child.kill();
        }
        let _ = child.wait();
    }
}

async fn archive_count(name: &str) -> Result<usize> {
    let errors = tempfile::NamedTempFile::new()?;
    let mut child = std::process::Command::new("docker")
        .args(["cp", &format!("{name}:/data/indexes/default/archives"), "-"])
        .stdout(Stdio::piped())
        .stderr(errors.reopen()?)
        .spawn()?;
    let mut stdout = child.stdout.take().ok_or("archive copy stdout missing")?;
    let process = ArchiveCopy(Arc::new(Mutex::new(child)));

    // Stream the corpus without materializing it in client memory. Killing the
    // producer on timeout also unblocks the parser's pipe reads.
    let mut reader = tokio::task::spawn_blocking(move || -> Result<usize> {
        let mut archive = tar::Archive::new(&mut stdout);
        let mut count = 0;
        for entry in archive.entries()? {
            let entry = entry?;
            if entry.header().entry_type().is_file()
                && entry
                    .path()?
                    .extension()
                    .is_some_and(|value| value == "parquet")
            {
                count += 1;
            }
        }

        // Drain trailing padding before waiting for the producer to exit.
        std::io::copy(&mut stdout, &mut std::io::sink())?;
        Ok(count)
    });
    let count = match tokio::time::timeout(Duration::from_secs(120), &mut reader).await {
        Ok(result) => result??,
        Err(error) => {
            drop(process);
            let _ = reader.await;
            return Err(error.into());
        }
    };

    let status = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = process
                .0
                .lock()
                .expect("archive copy process lock")
                .try_wait()?
            {
                return Ok::<_, std::io::Error>(status);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await??;
    if !status.success() {
        return Err(format!(
            "archive copy failed: {}",
            fs::read_to_string(errors.path())?
        )
        .into());
    }
    Ok(count)
}

struct Sampler(Option<JoinHandle<Result<()>>>);

impl Drop for Sampler {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

impl Sampler {
    fn start(
        name: String,
        output: PathBuf,
        phase: watch::Receiver<&'static str>,
        mut stop: watch::Receiver<bool>,
    ) -> Self {
        Self(Some(tokio::spawn(async move {
            let mut file = File::create(output.join("docker-stats.jsonl"))?;
            loop {
                tokio::select! {
                    _ = stop.changed() => break,
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                }

                let current_phase = *phase.borrow();
                let sample = runtime::docker_with_timeout(
                    &["stats", "--no-stream", "--format", "{{json .}}", &name],
                    15,
                )
                .await;
                let row = match sample {
                    Ok(text) => json!({
                        "wall_time_ms": now_ms(), "phase": current_phase,
                        "docker": serde_json::from_str::<Value>(&text)?,
                    }),
                    Err(error) => json!({"phase": current_phase, "error": error.to_string()}),
                };
                serde_json::to_writer(&mut file, &row)?;
                writeln!(file)?;
                file.flush()?;
            }
            Ok(())
        })))
    }

    async fn finish(&mut self) -> Result<()> {
        if let Some(mut task) = self.0.take() {
            match tokio::time::timeout(Duration::from_secs(20), &mut task).await {
                Ok(result) => result??,
                Err(error) => {
                    task.abort();
                    let _ = task.await;
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }
}

struct Progress {
    output: PathBuf,
    events: Vec<Value>,
    phase: watch::Sender<&'static str>,
}

impl Progress {
    fn record(&mut self, phase: &'static str, message: &str, details: Value) -> Result<()> {
        self.phase.send_replace(phase);
        let event = json!({
            "wall_time_ms": now_ms(), "phase": phase, "message": message, "details": details,
        });
        writeln!(std::io::stderr().lock(), "{event}")?;
        self.events.push(event);
        write_json(self.output.join("lifecycle.json"), &self.events)
    }
}

pub async fn run(options: Options) -> Result<()> {
    if !(1..=3600).contains(&options.duration) || options.seed_events == 0 {
        return Err("duration must be 1..3600 seconds and seed-events must be positive".into());
    }

    fs::create_dir_all(&options.output_dir)?;
    let output = options.output_dir.canonicalize()?;
    let config_path = output.join("server.toml");
    fs::write(&config_path, CONFIG)?;
    let config: toml::Value = toml::from_str(CONFIG)?;
    write_json(output.join("server-config.json"), &config)?;

    let mut resource = runtime::DockerResource::new("extended");
    let name = resource.name.clone();
    let volume = format!("{name}-data");
    resource.volume = Some(volume.clone());
    let archive_resource = runtime::DockerResource::new("extended-archive");

    let common = [
        "--cpus".to_owned(),
        "2".into(),
        "--memory".into(),
        "2g".into(),
        "--memory-swap".into(),
        "2g".into(),
        "--read-only".into(),
        "--tmpfs".into(),
        "/tmp:rw,noexec,nosuid,size=256m,mode=1777".into(),
        "--cap-drop".into(),
        "ALL".into(),
        "--security-opt".into(),
        "no-new-privileges:true".into(),
        "-v".into(),
        format!("{volume}:/data"),
        "-v".into(),
        format!("{}:/config.toml:ro", config_path.display()),
    ];

    let (phase, phase_reader) = watch::channel("setup");
    let (stop, stopped) = watch::channel(false);
    let mut progress = Progress {
        output: output.clone(),
        events: Vec::new(),
        phase,
    };
    let mut sampler = Sampler(None);
    let started = Instant::now();

    let outcome = async {
        runtime::docker(&["volume", "create", &volume]).await?;
        let mut args: Vec<String> = [
            "run", "-d", "--name", &name, "--log-opt", "max-size=20m", "--log-opt",
            "max-file=2", "-p", "127.0.0.1::3100",
        ].into_iter().map(str::to_owned).collect();
        args.extend(common.clone());
        args.extend([options.image.clone(), "--config".into(), "/config.toml".into(), "serve".into()]);
        docker(&args, 120).await?;
        let base = ready(&name).await?;

        write_json(output.join("provenance.json"), &json!({
            "image": serde_json::from_str::<Value>(&runtime::docker(&["image", "inspect", &options.image]).await?)?[0],
            "container": state(&name).await?, "cpu_limit": 2,
            "memory_limit_bytes": 2_u64 * 1024 * 1024 * 1024, "swap_disabled": true,
            "config": config,
        }))?;
        sampler = Sampler::start(name.clone(), output.clone(), phase_reader, stopped);
        progress.record("seed", "seeding historical corpus", json!({"seed_events": options.seed_events, "history_days": 6}))?;

        let mut workload = mixed::Args {
            url: base,
            seed_events: options.seed_events,
            history_days: 6.0,
            duration: options.duration as f64,
            rate: 1000.0,
            batch_size: 100,
            ingest_clients: 4,
            query_clients: 4,
            query_rate: 20.0,
            client_queue_capacity: 64,
            request_shape: "mixed".into(),
            body_bytes: 102400,
            late_fraction: 0.05,
            late_delay_ms: 3_600_000,
            burst_rate: 5000.0,
            burst_start: 240.0,
            burst_duration: 60.0,
            tail_subscribers: 8,
            tail_drain_seconds: 10.0,
            metrics_interval: 10.0,
            server_config: Some(output.join("server-config.json")),
            server_resources: "Docker enforced 2CPU/2GiB RAM/no swap; see provenance.json for architecture".into(),
            retention_ms: Some(604_800_000),
            require_min_archives: options.require_min_archives as u64,
            require_qualified: true,
            ..Default::default()
        };
        let marker = format!("bench-{}-{}", std::process::id(), now_ms());
        let seed = mixed::seed(&workload, &marker, READ_TOKEN, WRITE_TOKEN).await?;
        progress.record(
            "offline-archive",
            "preparing hourly archive fixture",
            json!({"seeded": seed.seeded}),
        )?;

        docker(&["stop".into(), "--time".into(), "90".into(), name.clone()], 100).await?;
        if state(&name).await?["State"]["ExitCode"] != 0 {
            return Err("server failed to shut down before offline archival".into());
        }
        let cutoff = now_ms() - DAY_MS;
        let mut args = vec![
            "run".into(),
            "--rm".into(),
            "--name".into(),
            archive_resource.name.clone(),
            "--network".into(),
            "none".into(),
        ];
        args.extend(common.clone());
        args.extend([
            options.image.clone(),
            "--config".into(),
            "/config.toml".into(),
            "archive".into(),
            "--before".into(),
            cutoff.to_string(),
        ]);
        fs::write(output.join("offline-archive.log"), docker(&args, 600).await?)?;

        let archives = archive_count(&name).await?;
        progress.record(
            "offline-archive",
            "hourly archive fixture prepared",
            json!({"archive_files": archives, "cutoff_ms": cutoff}),
        )?;
        if archives < options.require_min_archives {
            return Err(format!("only {archives} initial archives; need {}", options.require_min_archives).into());
        }

        runtime::docker(&["start", &name]).await?;
        workload.url = ready(&name).await?;
        write_json(output.join("cgroup-before-load.json"), &crate::container::read_cgroup(&name, &[]).await?)?;
        progress.record(
            "load",
            "starting offered horizon",
            json!({
                "duration_seconds": options.duration,
                "baseline_events_per_second": 1000,
                "burst_events_per_second": 5000,
            }),
        )?;

        let mut report = mixed::report_seeded(&workload, READ_TOKEN, WRITE_TOKEN, seed).await?;
        report["qualification_wall_seconds_including_seed_and_fixture"] = json!(started.elapsed().as_secs_f64());
        write_json(output.join("mixed-load.json"), &report)?;
        write_json(output.join("cgroup-after-load.json"), &crate::container::read_cgroup(&name, &[]).await?)?;
        fs::write(output.join("logs.jsonl"), runtime::docker(&["logs", &name]).await?)?;
        progress.record(
            "complete",
            "offered workload finished",
            json!({"qualification": report["qualification"], "accounting": report["accounting"]}),
        )?;
        if report["qualification"]["passed"] != true {
            return Err("extended workload did not qualify; inspect mixed-load.json".into());
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }.await;

    // Stop the observer before disposing resources, and still clean up on every failure.
    stop.send_replace(true);
    let sampler_result = sampler.finish().await;
    if let Err(error) = &outcome {
        fs::write(output.join("failure.txt"), format!("{error}\n"))?;
        progress.record(
            "failure",
            "qualification interrupted",
            json!({"error": error.to_string()}),
        )?;
    }
    if let Ok(logs) = runtime::docker(&["logs", &name]).await {
        fs::write(output.join("final-logs.jsonl"), logs)?;
    }
    let stop_result = docker(
        &["stop".into(), "--time".into(), "90".into(), name.clone()],
        100,
    )
    .await
    .map(|_| ());
    let final_state_result = match state(&name).await {
        Ok(final_state) => {
            write_json(output.join("final-state.json"), &final_state)?;
            if final_state["State"]["ExitCode"] == 0 && final_state["State"]["OOMKilled"] == false {
                Ok(())
            } else {
                Err("loaded container did not shut down cleanly".into())
            }
        }
        Err(error) => Err(error),
    };
    let cleanup_result = resource.cleanup().await;
    progress.record(
        "cleanup",
        "disposable resource cleanup finished",
        json!({"container": name, "volume": volume, "success": cleanup_result.is_ok()}),
    )?;

    let shutdown = runtime::finish(stop_result, final_state_result);
    let cleanup = runtime::finish(runtime::finish(sampler_result, shutdown), cleanup_result);
    runtime::finish(outcome, cleanup)
}
