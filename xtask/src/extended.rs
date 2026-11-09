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

/// Ingest token of the qualification server.
const WRITE_TOKEN: &str = "extended-load-write-token";

/// Read token of the qualification server.
const READ_TOKEN: &str = "extended-load-read-token";

/// Milliseconds in one day.
const DAY_MS: i64 = 86_400_000;

/// Longest accepted offered horizon, in seconds.
const MAX_DURATION_SECS: u64 = 3600;

/// Grace period, in seconds, that the server gets on `docker stop`.
const STOP_GRACE_SECS: &str = "90";

/// Bound, in seconds, for `docker stop` and other quick Docker operations.
const STOP_TIMEOUT_SECS: u64 = 100;

/// Bound, in seconds, for starting the server container.
const START_TIMEOUT_SECS: u64 = 120;

/// Bound, in seconds, for the offline archive run.
const ARCHIVE_TIMEOUT_SECS: u64 = 600;

/// Bound, in seconds, for `docker stats` in one sample.
const STATS_TIMEOUT_SECS: u64 = 15;

/// Time between resource samples.
const STATS_INTERVAL: Duration = Duration::from_secs(5);

/// Time the sampler gets to finish after it is stopped.
const SAMPLER_FINISH_TIMEOUT: Duration = Duration::from_secs(20);

/// Time allowed for one `/ready` probe to answer.
const READY_CLIENT_TIMEOUT: Duration = Duration::from_secs(2);

/// Time allowed for the server to become ready.
const READY_TIMEOUT: Duration = Duration::from_secs(60);

/// Delay between readiness probes.
const READY_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Time allowed for the archive copy to finish streaming.
const ARCHIVE_COPY_TIMEOUT: Duration = Duration::from_secs(120);

/// Time allowed for the archive copy process to exit after its stream ends.
const ARCHIVE_EXIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Delay between checks of the archive copy process.
const ARCHIVE_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Memory limit of the server container, in bytes: two GiB.
const MEMORY_LIMIT_BYTES: u64 = 2 * 1024 * 1024 * 1024;

// Shape of the offered load, handed to the mixed workload by `workload`.

/// Span of the seeded history, in days: seed event ages are spread evenly across it.
const HISTORY_DAYS: u32 = 6;

/// Ingest rate outside the burst, in events per second.
const BASELINE_RATE: u32 = 1000;

/// Ingest rate during the burst, in events per second.
const BURST_RATE: u32 = 5000;

/// Seconds into the load phase at which ingest switches to `BURST_RATE`.
const BURST_START_SECS: f64 = 240.0;

/// Length of the ingest burst, in seconds.
const BURST_DURATION_SECS: f64 = 60.0;

/// Events per ingest request.
const BATCH_SIZE: u64 = 100;

/// Concurrent workers sending ingest requests.
const INGEST_CLIENTS: usize = 4;

/// Concurrent workers sending queries.
const QUERY_CLIENTS: usize = 4;

/// Offered query rate during the load phase, in queries per second.
const QUERY_RATE: f64 = 20.0;

/// Pending requests queued for the ingest workers, and separately for the query workers;
/// a request offered to a full queue is dropped and counted.
const CLIENT_QUEUE_CAPACITY: usize = 64;

/// Size of every ingest request body, in bytes (100 KiB): batches are padded or trimmed to it.
const BODY_BYTES: usize = 102_400;

/// Share of live events timestamped `LATE_DELAY_MS` in the past, to exercise late arrivals.
const LATE_FRACTION: f64 = 0.05;

/// How far in the past late events are timestamped, in milliseconds: one hour.
const LATE_DELAY_MS: i64 = 3_600_000;

/// Live tail subscriptions held open while the load runs.
const TAIL_SUBSCRIBERS: usize = 8;

/// Wait after the load phase before the tail subscriptions are stopped, in seconds, so they
/// can receive the last events.
const TAIL_DRAIN_SECS: f64 = 10.0;

/// Interval between `/metrics` samples taken during the load, in seconds.
const METRICS_INTERVAL_SECS: f64 = 10.0;

/// Retention of the server, in milliseconds: seven days.
const RETENTION_MS: i64 = 604_800_000;

/// Message for a lock that is only poisoned by a panic in this process.
const ARCHIVE_LOCK_POISONED: &str = "archive copy process lock";

/// File name of the server configuration inside the output directory.
const SERVER_CONFIG_FILE: &str = "server.toml";

/// Server configuration of the qualification run, written to `server.toml`.
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

/// Options for `cargo xtask extended-load`.
#[derive(Args, Clone, Debug)]
pub struct Options {
    /// Image under test.
    #[arg(long)]
    pub image: String,
    /// Directory that receives the qualification artifacts.
    #[arg(long, default_value = "artifacts/load-retention")]
    pub output_dir: PathBuf,
    /// Offered horizon in seconds, from 1 to 3600.
    #[arg(long, default_value_t = 600)]
    pub duration: u64,
    /// Number of events in the historical corpus.
    #[arg(long, default_value_t = 1_000_000)]
    pub seed_events: u64,
    /// Fewest hourly archive files that the offline fixture must produce.
    #[arg(long, default_value_t = 100)]
    pub require_min_archives: usize,
}

/// Writes `value` to `path` as pretty JSON with a trailing newline.
fn write_json(path: impl AsRef<Path>, value: &impl serde::Serialize) -> Result<()> {
    let mut file = File::create(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    writeln!(file)?;
    Ok(())
}

/// Runs `docker` with `args` and returns its stdout, failing on a non-zero exit.
async fn docker(args: &[String], timeout_secs: u64) -> Result<Vec<u8>> {
    let output = runtime::output(Command::new("docker").args(args), timeout_secs).await?;
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

/// Inspects the container `name` and returns the first element of Docker's state array.
async fn state(name: &str) -> Result<Value> {
    Ok(serde_json::from_str::<Value>(&runtime::docker(&["inspect", name]).await?)?[0].clone())
}

/// Waits until the container's `/ready` endpoint answers, and returns its base URL.
async fn ready(name: &str) -> Result<String> {
    let port = runtime::docker(&["port", name, "3100/tcp"]).await?;
    let base = format!(
        "http://127.0.0.1:{}",
        port.rsplit(':').next().ok_or("missing published port")?
    );

    let client = reqwest::Client::builder()
        .timeout(READY_CLIENT_TIMEOUT)
        .build()?;

    tokio::time::timeout(READY_TIMEOUT, async {
        loop {
            if let Ok(response) = client.get(format!("{base}/ready")).send().await
                && response.status().is_success()
            {
                return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
            }

            if state(name).await?["State"]["Running"] != true {
                return Err("container stopped before readiness".into());
            }

            tokio::time::sleep(READY_POLL_INTERVAL).await;
        }
    })
    .await??;

    Ok(base)
}

/// The `docker cp` process that streams the archives, killed when dropped.
struct ArchiveCopy(Arc<Mutex<Child>>);

impl Drop for ArchiveCopy {
    fn drop(&mut self) {
        let mut child = self.0.lock().expect(ARCHIVE_LOCK_POISONED);
        if !matches!(child.try_wait(), Ok(Some(_))) {
            let _ = child.kill();
        }
        let _ = child.wait();
    }
}

/// Counts the Parquet files in the container's archive directory without buffering them.
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
    let count = match tokio::time::timeout(ARCHIVE_COPY_TIMEOUT, &mut reader).await {
        Ok(result) => result??,
        Err(error) => {
            drop(process);
            let _ = reader.await;
            return Err(error.into());
        }
    };

    let status = wait_for_archive_copy(&process).await?;
    if !status.success() {
        return Err(format!(
            "archive copy failed: {}",
            fs::read_to_string(errors.path())?
        )
        .into());
    }

    Ok(count)
}

/// Polls the archive copy process until it exits, within `ARCHIVE_EXIT_TIMEOUT`.
async fn wait_for_archive_copy(process: &ArchiveCopy) -> Result<std::process::ExitStatus> {
    let status = tokio::time::timeout(ARCHIVE_EXIT_TIMEOUT, async {
        loop {
            if let Some(status) = process.0.lock().expect(ARCHIVE_LOCK_POISONED).try_wait()? {
                return Ok::<_, std::io::Error>(status);
            }
            tokio::time::sleep(ARCHIVE_POLL_INTERVAL).await;
        }
    })
    .await??;
    Ok(status)
}

/// The background task that writes `docker stats` samples to `docker-stats.jsonl`.
struct Sampler(Option<JoinHandle<Result<()>>>);

impl Drop for Sampler {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

impl Sampler {
    /// Starts sampling the container `name` into `output` until `stop` is set, tagging each
    /// sample with the current `phase`.
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
                    _ = tokio::time::sleep(STATS_INTERVAL) => {}
                }

                let current_phase = *phase.borrow();
                let sample = runtime::docker_with_timeout(
                    &["stats", "--no-stream", "--format", "{{json .}}", &name],
                    STATS_TIMEOUT_SECS,
                )
                .await;
                let row = match sample {
                    Ok(text) => json!({
                        "wall_time_ms": now_ms(),
                        "phase": current_phase,
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

    /// Waits for the sampler to stop, aborting it when it does not finish in time.
    ///
    /// # Errors
    ///
    /// Returns the sampler's error, or the timeout when it does not finish within
    /// `SAMPLER_FINISH_TIMEOUT`.
    async fn finish(&mut self) -> Result<()> {
        if let Some(mut task) = self.0.take() {
            match tokio::time::timeout(SAMPLER_FINISH_TIMEOUT, &mut task).await {
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

/// The lifecycle log: each phase change is printed to stderr and kept in `lifecycle.json`.
struct Progress {
    output: PathBuf,
    events: Vec<Value>,
    phase: watch::Sender<&'static str>,
}

impl Progress {
    /// Records an event in `phase` and rewrites `lifecycle.json`.
    fn record(&mut self, phase: &'static str, message: &str, details: Value) -> Result<()> {
        self.phase.send_replace(phase);
        let event = json!({
            "wall_time_ms": now_ms(),
            "phase": phase,
            "message": message,
            "details": details,
        });
        writeln!(std::io::stderr().lock(), "{event}")?;
        self.events.push(event);
        write_json(self.output.join("lifecycle.json"), &self.events)
    }
}

/// Runs the qualification: seeds the server, archives history offline, then runs the mixed
/// workload and checks that it qualifies.
///
/// # Errors
///
/// Returns an error when the options are out of range, any step fails, or the workload does
/// not qualify. Cleanup runs on every failure, and its errors are reported with the first one.
pub async fn run(options: Options) -> Result<()> {
    validate_options(&options)?;
    let (output, config) = prepare_output(&options.output_dir)?;
    let config_path = output.join(SERVER_CONFIG_FILE);

    let mut resource = runtime::DockerResource::new("extended");
    let name = resource.name.clone();
    let volume = format!("{name}-data");
    resource.volume = Some(volume.clone());

    // Declared after `resource` so it drops before it; its drop removes any leftover container.
    let archive_resource = runtime::DockerResource::new("extended-archive");
    let common = common_flags(&volume, &config_path);

    let (phase, phase_reader) = watch::channel("setup");
    let (stop, stopped) = watch::channel(false);
    let mut qualification = Qualification {
        progress: Progress {
            output: output.clone(),
            events: Vec::new(),
            phase,
        },
        output,
        config,
        name,
        volume,
        common,
        archive_name: archive_resource.name.clone(),
        sampler: Sampler(None),
        started: Instant::now(),
    };
    let outcome = qualification.execute(&options, phase_reader, stopped).await;
    qualification.teardown(stop, outcome, &mut resource).await
}

/// Rejects a horizon outside `1..=MAX_DURATION_SECS` and an empty corpus before anything starts.
fn validate_options(options: &Options) -> Result<()> {
    if !(1..=MAX_DURATION_SECS).contains(&options.duration) || options.seed_events == 0 {
        return Err(format!(
            "duration must be 1..{MAX_DURATION_SECS} seconds and seed-events must be positive"
        )
        .into());
    }

    Ok(())
}

/// Creates the output directory and writes the server configuration into it. Returns the
/// canonical output directory and the parsed configuration.
fn prepare_output(output_dir: &Path) -> Result<(PathBuf, toml::Value)> {
    fs::create_dir_all(output_dir)?;
    let output = output_dir.canonicalize()?;
    fs::write(output.join(SERVER_CONFIG_FILE), CONFIG)?;

    let config: toml::Value = toml::from_str(CONFIG)?;
    write_json(output.join("server-config.json"), &config)?;
    Ok((output, config))
}

/// The state of one qualification run, shared by its phases.
struct Qualification {
    output: PathBuf,
    config: toml::Value,
    name: String,
    volume: String,
    archive_name: String,
    common: Vec<String>,
    progress: Progress,
    sampler: Sampler,
    started: Instant,
}

impl Qualification {
    /// Seeds the server, archives its history offline, then runs the offered load against it.
    async fn execute(
        &mut self,
        options: &Options,
        phase: watch::Receiver<&'static str>,
        stopped: watch::Receiver<bool>,
    ) -> Result<()> {
        runtime::docker(&["volume", "create", &self.volume]).await?;
        let base = start_server(&self.name, &options.image, &self.common).await?;

        write_provenance(&self.output, &options.image, &self.name, &self.config).await?;
        self.sampler = Sampler::start(self.name.clone(), self.output.clone(), phase, stopped);
        self.progress.record(
            "seed",
            "seeding historical corpus",
            json!({"seed_events": options.seed_events, "history_days": HISTORY_DAYS}),
        )?;

        let mut workload = workload(base, options, &self.output);
        let marker = format!("bench-{}-{}", std::process::id(), now_ms());
        let seed = mixed::seed(&workload, &marker, READ_TOKEN, WRITE_TOKEN).await?;
        self.progress.record(
            "offline-archive",
            "preparing hourly archive fixture",
            json!({"seeded": seed.seeded}),
        )?;

        self.archive_history(options).await?;
        workload.url = self.restart_for_load().await?;
        self.measure_load(&workload, options.duration, seed).await
    }

    /// Stops the server, archives everything older than a day offline, and checks that enough
    /// archives were produced.
    async fn archive_history(&mut self, options: &Options) -> Result<()> {
        stop_container(&self.name).await?;
        if state(&self.name).await?["State"]["ExitCode"] != 0 {
            return Err("server failed to shut down before offline archival".into());
        }

        let cutoff = now_ms() - DAY_MS;
        let archive_args = archive_args(&self.archive_name, &self.common, &options.image, cutoff);
        fs::write(
            self.output.join("offline-archive.log"),
            docker(&archive_args, ARCHIVE_TIMEOUT_SECS).await?,
        )?;

        let archives = archive_count(&self.name).await?;
        self.progress.record(
            "offline-archive",
            "hourly archive fixture prepared",
            json!({"archive_files": archives, "cutoff_ms": cutoff}),
        )?;

        if archives < options.require_min_archives {
            return Err(format!(
                "only {archives} initial archives; need {}",
                options.require_min_archives
            )
            .into());
        }

        Ok(())
    }

    /// Starts the archived server again, waits until it answers, and records its cgroup limits.
    /// Returns its base URL.
    async fn restart_for_load(&mut self) -> Result<String> {
        runtime::docker(&["start", &self.name]).await?;
        let url = ready(&self.name).await?;

        write_json(
            self.output.join("cgroup-before-load.json"),
            &crate::container::read_cgroup(&self.name, &[]).await?,
        )?;

        Ok(url)
    }

    /// Runs the offered horizon and checks its report, which must qualify.
    async fn measure_load(
        &mut self,
        workload: &mixed::Args,
        duration: u64,
        seed: mixed::Seed,
    ) -> Result<()> {
        self.progress.record(
            "load",
            "starting offered horizon",
            json!({
                "duration_seconds": duration,
                "baseline_events_per_second": BASELINE_RATE,
                "burst_events_per_second": BURST_RATE,
            }),
        )?;

        let mut report = mixed::report_seeded(workload, READ_TOKEN, WRITE_TOKEN, seed).await?;
        report["qualification_wall_seconds_including_seed_and_fixture"] =
            json!(self.started.elapsed().as_secs_f64());
        write_json(self.output.join("mixed-load.json"), &report)?;

        write_json(
            self.output.join("cgroup-after-load.json"),
            &crate::container::read_cgroup(&self.name, &[]).await?,
        )?;

        fs::write(
            self.output.join("logs.jsonl"),
            runtime::docker(&["logs", &self.name]).await?,
        )?;

        self.progress.record(
            "complete",
            "offered workload finished",
            json!({"qualification": report["qualification"], "accounting": report["accounting"]}),
        )?;

        if report["qualification"]["passed"] != true {
            return Err("extended workload did not qualify; inspect mixed-load.json".into());
        }

        Ok(())
    }

    /// Stops the observer, records any failure, stops the server and checks that it shut down
    /// cleanly, then removes the disposable resources. Cleanup runs on every failure, and its
    /// errors are reported with the first one.
    async fn teardown(
        &mut self,
        stop: watch::Sender<bool>,
        outcome: Result<()>,
        resource: &mut runtime::DockerResource,
    ) -> Result<()> {
        // Stop the observer before disposing resources, and still clean up on every failure.
        stop.send_replace(true);
        let sampler_result = self.sampler.finish().await;

        if let Err(error) = &outcome {
            fs::write(self.output.join("failure.txt"), format!("{error}\n"))?;
            self.progress.record(
                "failure",
                "qualification interrupted",
                json!({"error": error.to_string()}),
            )?;
        }

        if let Ok(logs) = runtime::docker(&["logs", &self.name]).await {
            fs::write(self.output.join("final-logs.jsonl"), logs)?;
        }

        let stop_result = stop_container(&self.name).await;

        let final_state_result = match state(&self.name).await {
            Ok(final_state) => {
                write_json(self.output.join("final-state.json"), &final_state)?;
                if final_state["State"]["ExitCode"] == 0
                    && final_state["State"]["OOMKilled"] == false
                {
                    Ok(())
                } else {
                    Err("loaded container did not shut down cleanly".into())
                }
            }
            Err(error) => Err(error),
        };

        let cleanup_result = resource.cleanup().await;
        self.progress.record(
            "cleanup",
            "disposable resource cleanup finished",
            json!({
                "container": self.name,
                "volume": self.volume,
                "success": cleanup_result.is_ok(),
            }),
        )?;

        let shutdown = runtime::finish(stop_result, final_state_result);
        let cleanup = runtime::finish(runtime::finish(sampler_result, shutdown), cleanup_result);
        runtime::finish(outcome, cleanup)
    }
}

/// Flags shared by the server and the offline archive run: CPU and memory limits, a read-only
/// root with a private `/tmp`, dropped capabilities, the data volume and the config file.
fn common_flags(volume: &str, config_path: &Path) -> Vec<String> {
    let mut flags: Vec<String> = [
        "--cpus",
        "2",
        "--memory",
        "2g",
        "--memory-swap",
        "2g",
        "--read-only",
        "--tmpfs",
        "/tmp:rw,noexec,nosuid,size=256m,mode=1777",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges:true",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    flags.extend([
        "-v".to_owned(),
        format!("{volume}:/data"),
        "-v".to_owned(),
        format!("{}:/config.toml:ro", config_path.display()),
    ]);
    flags
}

/// Starts the server container `name` in the background, waits for readiness and returns its
/// base URL.
async fn start_server(name: &str, image: &str, common: &[String]) -> Result<String> {
    let mut args: Vec<String> = [
        "run",
        "-d",
        "--name",
        name,
        "--log-opt",
        "max-size=20m",
        "--log-opt",
        "max-file=2",
        "-p",
        "127.0.0.1::3100",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    args.extend(common.iter().cloned());
    args.extend([
        image.to_owned(),
        "--config".into(),
        "/config.toml".into(),
        "serve".into(),
    ]);

    docker(&args, START_TIMEOUT_SECS).await?;

    ready(name).await
}

/// Writes `provenance.json`: the image and container facts, the resource limits and the
/// server configuration that the run used.
async fn write_provenance(
    output: &Path,
    image: &str,
    name: &str,
    config: &toml::Value,
) -> Result<()> {
    let image_state: Value =
        serde_json::from_str(&runtime::docker(&["image", "inspect", image]).await?)?;
    let container_state = state(name).await?;
    write_json(
        output.join("provenance.json"),
        &json!({
            "image": image_state[0],
            "container": container_state,
            "cpu_limit": 2,
            "memory_limit_bytes": MEMORY_LIMIT_BYTES,
            "swap_disabled": true,
            "config": config,
        }),
    )
}

/// Stops the container `name` with the standard grace period.
async fn stop_container(name: &str) -> Result<()> {
    docker(
        &[
            "stop".into(),
            "--time".into(),
            STOP_GRACE_SECS.into(),
            name.into(),
        ],
        STOP_TIMEOUT_SECS,
    )
    .await
    .map(|_| ())
}

/// `docker run` arguments for the offline archive: a one-off, networkless container that
/// archives everything before `cutoff`.
fn archive_args(name: &str, common: &[String], image: &str, cutoff: i64) -> Vec<String> {
    let mut args = vec![
        "run".into(),
        "--rm".into(),
        "--name".into(),
        name.into(),
        "--network".into(),
        "none".into(),
    ];
    args.extend(common.iter().cloned());
    args.extend([
        image.to_owned(),
        "--config".into(),
        "/config.toml".into(),
        "archive".into(),
        "--before".into(),
        cutoff.to_string(),
    ]);
    args
}

/// The mixed workload for the qualification: the fixed shape of the offered load, plus the
/// server URL, sizes and files that this run owns.
fn workload(base: String, options: &Options, output: &Path) -> mixed::Args {
    mixed::Args {
        url: base,
        seed_events: options.seed_events,
        history_days: f64::from(HISTORY_DAYS),
        duration: options.duration as f64,
        rate: f64::from(BASELINE_RATE),
        batch_size: BATCH_SIZE,
        ingest_clients: INGEST_CLIENTS,
        query_clients: QUERY_CLIENTS,
        query_rate: QUERY_RATE,
        client_queue_capacity: CLIENT_QUEUE_CAPACITY,
        request_shape: "mixed".into(),
        body_bytes: BODY_BYTES,
        late_fraction: LATE_FRACTION,
        late_delay_ms: LATE_DELAY_MS,
        burst_rate: f64::from(BURST_RATE),
        burst_start: BURST_START_SECS,
        burst_duration: BURST_DURATION_SECS,
        tail_subscribers: TAIL_SUBSCRIBERS,
        tail_drain_seconds: TAIL_DRAIN_SECS,
        metrics_interval: METRICS_INTERVAL_SECS,
        server_config: Some(output.join("server-config.json")),
        server_resources:
            "Docker enforced 2CPU/2GiB RAM/no swap; see provenance.json for architecture".into(),
        retention_ms: Some(RETENTION_MS),
        require_min_archives: options.require_min_archives as u64,
        require_qualified: true,
        ..Default::default()
    }
}
