//! Dedicated connection owners. Acknowledgements follow explicit appender flush and commit.

#[cfg(test)]
use crate::model::Event;
use crate::{
    config::StorageConfig,
    model::{Error, ErrorKind, Facets, HistogramBucket, NormalizedEvent, Query, SearchResult},
};
use duckdb::{Connection, ProgressHandle};
#[cfg(test)]
use duckdb::{params, params_from_iter, types::Value};
#[cfg(test)]
use maintenance::WriterScope;
use std::{
    fs::create_dir_all,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};

mod admission;
mod archive;
mod interrupt;
mod leases;
mod maintenance;
mod projection;
mod projection_integrity;
mod query;
mod quota;
mod readers;
mod writer;

pub(crate) use admission::{AppendLane, IngressAdmission, serialized_length};

use admission::{AppendOptions, forward_byte_share};
use archive::{migrate, recover};
use interrupt::build_maintenance_interrupt;
use leases::ArchiveLeases;
#[cfg(test)]
use leases::cleanup_archives;
use maintenance::{Maintainer, Maintenance, MaintenanceBudget, spawn_maintenance_side};
use quota::DiskBudget;
use readers::{Answer, Operation, Read, ReaderShared, spawn_reader_side};
use writer::{RETENTION_CUTOFF_SQL, Write, Writer, spawn_writer_side};

#[cfg(test)]
use archive::{ArchiveWork, compact_export, export, export_bounded, publish, retain};
#[cfg(test)]
use interrupt::ConnectionOwner;
#[cfg(test)]
use maintenance::{maintenance_outcome, maintenance_reply, maintenance_send};
#[cfg(test)]
use query::dataset;
#[cfg(test)]
use writer::append;

type Result<T> = std::result::Result<T, Error>;

fn db_error(e: impl std::error::Error + Send + Sync + 'static) -> Error {
    let message = e.to_string();

    if let Some(error) = (&e as &dyn std::any::Any).downcast_ref::<duckdb::Error>()
        && matches!(
            error,
            duckdb::Error::FromSqlConversionFailure(..)
                | duckdb::Error::InvalidColumnType(..)
                | duckdb::Error::IntegralValueOutOfRange(..)
                | duckdb::Error::UnsignedIntegralValueOutOfRange(..)
        )
    {
        return Error::new(ErrorKind::Integrity, message).with_source(e);
    }

    if let Ok(details) = serde_json::from_str::<serde_json::Value>(&message) {
        let kind = details.get("exception_type").and_then(|v| v.as_str());
        if matches!(kind, Some("Fatal" | "Internal" | "Serialization")) {
            return Error::new(ErrorKind::Integrity, message).with_source(e);
        }
    }

    Error::unavailable(message).with_source(e)
}

fn queue_error<T>(error: mpsc::error::TrySendError<T>) -> Error {
    match error {
        mpsc::error::TrySendError::Full(_) => {
            Error::new(ErrorKind::Overloaded, "storage queue full")
        }
        mpsc::error::TrySendError::Closed(_) => Error::unavailable("storage worker closed"),
    }
}

/// A cloneable handle to bounded ingestion, read, and maintenance workers.
#[derive(Clone)]
pub struct Storage {
    inner: Arc<Inner>,
}

/// A fresh file-byte snapshot, including unpublished and unmanaged files.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct DiskUsage {
    /// Total bytes across all files in the index directory.
    pub size_bytes: u64,
    /// Bytes occupied by the primary `DuckDB` file.
    pub database_bytes: u64,
    /// Bytes occupied by the write-ahead log.
    pub wal_bytes: u64,
    /// Bytes occupied by archive files, including unpublished output.
    pub archive_bytes: u64,
    /// Bytes occupied by temporary files and engine spill files.
    pub temp_bytes: u64,
    /// Bytes occupied by files outside the managed storage categories.
    pub other_bytes: u64,
}

/// Observed durable layout and actual row coverage, for offline qualification.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ProjectionStatus {
    /// Durable hot-table schema version.
    pub schema_version: i32,
    /// Canonical attribute keys stored in typed string columns.
    pub promoted_string_keys: Vec<String>,
    /// Visible rows in the hot table.
    pub hot_rows: u64,
    /// Visible hot rows with validated promoted columns.
    pub hot_covered_rows: u64,
    /// Visible rows stored in archive files.
    pub archived_rows: u64,
    /// Visible archived rows with validated promoted columns.
    pub archived_covered_rows: u64,
    /// Archive files that predate promoted columns.
    pub legacy_archive_files: u64,
}

/// Enables native estimates without `DuckDB`'s terminal progress output.
fn track_query_progress(connection: &Connection) -> Result<ProgressHandle> {
    connection
        .set_query_progress_tracking(true)
        .map_err(db_error)?;
    Ok(connection.progress_handle())
}

/// Emits per-connection gauges. `NaN` means unavailable, including idle/closed
/// connections and plans DuckDB cannot estimate; it must never mean 0% complete.
fn push_query_progress(
    output: &mut String,
    readers: &[ProgressHandle],
    maintenance: &ProgressHandle,
) {
    for (name, help) in [
        (
            "available",
            "Whether DuckDB reports an estimate for the current query.",
        ),
        (
            "fraction",
            "Estimated completed fraction of the current query, or NaN if unavailable.",
        ),
        (
            "rows_processed",
            "Estimated rows processed by the current query, or NaN if unavailable.",
        ),
        (
            "rows_total",
            "Estimated total rows of the current query, or NaN if unavailable.",
        ),
    ] {
        output.push_str(&format!(
            "# HELP logbrook_storage_query_progress_{name} {help}\n\
             # TYPE logbrook_storage_query_progress_{name} gauge\n"
        ));
    }

    for (role, worker, handle) in readers
        .iter()
        .enumerate()
        .map(|(worker, handle)| ("reader", worker, handle))
        .chain(std::iter::once(("maintenance", 0, maintenance)))
    {
        let snapshot = handle.query_progress();
        let percentage = snapshot.percentage();
        let available = u8::from(percentage.is_some());
        let fraction = percentage.map_or(f64::NAN, |value| value / PERCENTAGE_SCALE);
        let rows = percentage.map_or(f64::NAN, |_| snapshot.rows_processed() as f64);
        let total = percentage.map_or(f64::NAN, |_| snapshot.total_rows_to_process() as f64);
        for (name, value) in [
            ("available", f64::from(available)),
            ("fraction", fraction),
            ("rows_processed", rows),
            ("rows_total", total),
        ] {
            output.push_str(&format!(
                "logbrook_storage_query_progress_{name}\
                 {{role=\"{role}\",worker=\"{worker}\"}} {value}\n"
            ));
        }
    }
}

/// Adds metric descriptions and types to the fixed storage samples.
fn describe_metric_samples(samples: &str) -> String {
    samples
        .lines()
        .map(|line| {
            let name = line.split_whitespace().next().unwrap();
            let kind = if name.ends_with("_total") || name.ends_with("_seconds_sum") {
                "counter"
            } else {
                "gauge"
            };
            format!(
                "# HELP {name} Storage {}.\n# TYPE {name} {kind}\n{line}\n",
                name.trim_start_matches("logbrook_storage_")
                    .replace('_', " ")
            )
        })
        .collect()
}

struct Inner {
    projection: Option<projection::ProjectionLayout>,
    writer: mpsc::Sender<Write>,
    maintenance: mpsc::Sender<Maintenance>,
    readers: Vec<mpsc::Sender<Read>>,
    interrupts: Vec<Arc<dyn Fn(u64) + Send + Sync>>,
    query_progress: Vec<ProgressHandle>,
    maintenance_progress: ProgressHandle,
    next: AtomicU64,
    committed: Arc<AtomicU64>,
    ready: Arc<AtomicBool>,
    admission: Arc<IngressAdmission>,
    accepting: Mutex<bool>,
    bytes: Arc<Semaphore>,
    max_queue_bytes: usize,
    forward_bytes: Arc<Semaphore>,
    forward_slots: Arc<Semaphore>,
    timeout: Duration,
    commits: watch::Sender<u64>,
    data_dir: PathBuf,
    max_result_bytes: usize,
    metrics: Arc<Metrics>,
    archive_leases: Arc<Mutex<ArchiveLeases>>,
    quota: Arc<DiskBudget>,
    maintenance_cancel: Arc<AtomicBool>,
    maintenance_interrupt: Arc<dyn Fn() + Send + Sync>,
    joins: Mutex<Vec<thread::JoinHandle<()>>>,
    shutdown: Arc<tokio::sync::Mutex<Option<Result<()>>>>,
}

#[derive(Default)]
struct Metrics {
    retention_before_ms: AtomicI64,
    busy_readers: AtomicU64,
    timeouts: AtomicU64,
    maintenance_failures: AtomicU64,
    commit_ns: AtomicU64,
    commits: AtomicU64,
    group_requests: AtomicU64,
    group_events: AtomicU64,
    group_bytes: AtomicU64,
}

impl Metrics {
    fn record_group(&self, elapsed: Duration, requests: usize, events: usize, bytes: usize) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.commit_ns.fetch_add(nanos, Ordering::Relaxed);
        self.commits.fetch_add(1, Ordering::Relaxed);
        self.group_requests
            .fetch_add(widen(requests), Ordering::Relaxed);
        self.group_events
            .fetch_add(widen(events), Ordering::Relaxed);
        self.group_bytes.fetch_add(widen(bytes), Ordering::Relaxed);
    }
}

/// Retention privilege is independent of how memory was reserved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngestionPolicy {
    /// Live events must respect the durable retention cutoff.
    Live,
    /// Historical imports may precede the durable retention cutoff.
    Import,
}

/// Message for a poisoned ingress admission lock.
const INGRESS_ADMISSION_LOCK: &str = "ingress admission lock";

/// Message for a poisoned storage admission lock.
const STORAGE_ADMISSION_LOCK: &str = "storage admission lock";

/// Nanoseconds in one second, for converting commit time to the metric unit.
const NANOS_PER_SECOND: f64 = 1e9;

/// Percentage points in one complete query, for the unit-interval progress gauge.
const PERCENTAGE_SCALE: f64 = 100.0;

struct WorkerHealth(Arc<AtomicBool>);

impl Drop for WorkerHealth {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Converts a count or byte total to the metric width. The conversion cannot
/// saturate on supported targets, where `usize` is no wider than `u64`.
fn widen(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn storage_unavailable() -> Error {
    Error::unavailable("storage unavailable")
}

fn writer_closed() -> Error {
    Error::unavailable("writer closed")
}

fn validate_archive_name(path: &str) -> Result<()> {
    let is_literal_filename = !path.is_empty()
        && Path::new(path).components().count() == 1
        && !path.contains(['*', '?', '[', ']', '{', '}']);
    let is_relative_directory = path == "." || path == "..";
    if !is_literal_filename || is_relative_directory {
        return Err(Error::invalid("archive path must be a literal filename"));
    }

    Ok(())
}

/// Creates the data layout and opens the primary connection with bounded engine settings.
fn prepare_config(config: StorageConfig) -> Result<StorageConfig> {
    let mut config = config;
    crate::config::validate_promoted_string_keys(&config.promoted_string_keys)?;

    if config.data_dir.is_relative() {
        config.data_dir = std::env::current_dir()
            .map_err(db_error)?
            .join(&config.data_dir);
    }

    crate::config::validate_literal_path(&config.data_dir)?;

    let invalid_archive_window = config.archive_batch_rows == 0 || config.archive_partition_ms <= 0;
    let invalid_compaction = config.compact_max_files < 2 || config.compact_max_bytes == 0;
    if invalid_archive_window || invalid_compaction || config.maintenance_timeout_ms == 0 {
        return Err(Error::invalid("invalid archive maintenance budgets"));
    }

    Ok(config)
}

/// Returns the primary connection and the archive directory, both prepared under the data dir.
fn open_database(config: &StorageConfig) -> Result<(Connection, PathBuf)> {
    create_dir_all(&config.data_dir).map_err(db_error)?;
    let temp = config.data_dir.join("temp");
    create_dir_all(&temp).map_err(db_error)?;
    let archives = config.data_dir.join("archives");
    create_dir_all(&archives).map_err(db_error)?;

    let conn = Connection::open(config.data_dir.join("events.duckdb")).map_err(db_error)?;
    conn.execute_batch(&format!(
        "SET memory_limit='{}'; \
         SET max_temp_directory_size='{}'; \
         SET threads={}; \
         SET temp_directory='{}'; \
         SET errors_as_json=true; \
         SET autoinstall_known_extensions=false; \
         SET autoload_known_extensions=false;",
        quote(&config.memory_limit),
        quote(&config.temp_limit),
        config.duckdb_threads,
        quote(&temp.to_string_lossy())
    ))
    .map_err(db_error)?;

    Ok((conn, archives))
}

/// The primary connection and the durable state it was validated against.
struct ValidatedDatabase {
    conn: Connection,
    archives: PathBuf,
    projection: Option<projection::ProjectionLayout>,
}

/// Opens the primary connection and validates durable state before any worker starts.
fn open_validated(config: &StorageConfig) -> Result<ValidatedDatabase> {
    let (mut conn, archives) = open_database(config)?;
    migrate(&mut conn)?;
    let projection = projection::activate(&mut conn, &config.promoted_string_keys)?;
    recover(&conn, &archives)?;

    Ok(ValidatedDatabase {
        conn,
        archives,
        projection,
    })
}

/// Clones the primary connection once per reader, so no reader starts if one clone fails.
fn clone_connections(conn: &Connection, count: usize) -> Result<Vec<Connection>> {
    (0..count)
        .map(|_| conn.try_clone().map_err(db_error))
        .collect()
}

fn quote(s: &str) -> String {
    s.replace('\'', "''")
}

impl Storage {
    /// Opens or creates the index under `config.data_dir`. Durable state is validated
    /// before any worker accepts ingestion or queries.
    ///
    /// # Errors
    /// Fails on invalid budgets, on durable state that cannot be migrated or recovered,
    /// and when the engine cannot start.
    pub fn open(config: StorageConfig) -> Result<Self> {
        Self::open_with_ingested_counter(
            config,
            Arc::new(AtomicU64::new(0)),
            Arc::new(IngressAdmission::new()),
        )
    }

    pub(crate) fn open_with_ingested_counter(
        config: StorageConfig,
        ingested: Arc<AtomicU64>,
        admission: Arc<IngressAdmission>,
    ) -> Result<Self> {
        let config = prepare_config(config)?;
        let ValidatedDatabase {
            conn,
            archives,
            projection,
        } = open_validated(&config)?;

        let ready = Arc::new(AtomicBool::new(true));
        let retention_before: i64 = conn
            .query_row(RETENTION_CUTOFF_SQL, [], |row| row.get(0))
            .map_err(db_error)?;
        let metrics = Arc::new(Metrics {
            retention_before_ms: AtomicI64::new(retention_before),
            ..Metrics::default()
        });

        let gate = Arc::new(Mutex::new(ArchiveLeases::default()));
        let reader_connections = clone_connections(&conn, config.reader_threads.max(1))?;
        let maintenance_connection = conn.try_clone().map_err(db_error)?;
        // Progress is diagnostic only. Each connection must opt in separately;
        // polling never changes deadlines, cancellation, or reader occupancy.
        let query_progress = reader_connections
            .iter()
            .map(track_query_progress)
            .collect::<Result<Vec<_>>>()?;
        let maintenance_progress = track_query_progress(&maintenance_connection)?;

        let (writer_tx, writer_rx) = mpsc::channel(config.queue_capacity.max(1));
        let committed = Arc::new(AtomicU64::new(0));
        let (commits, _) = watch::channel(0);
        let disk_budget = DiskBudget::new(config.data_dir.clone(), config.max_size_bytes)?;

        // Nothing below can fail, so every thread started from here on is owned by the
        // handle this function returns.
        let maintenance_cancel = Arc::new(AtomicBool::new(false));
        let writer_active = Arc::new(Mutex::new(None::<MaintenanceBudget>));
        let writer_interrupt = conn.interrupt_handle();
        let maintenance_deadline = Arc::new(Mutex::new(None::<Instant>));
        let native_interrupt = maintenance_connection.interrupt_handle();
        let maintenance_interrupt = build_maintenance_interrupt(
            Arc::clone(&maintenance_deadline),
            Arc::clone(&native_interrupt),
            Arc::clone(&writer_active),
            Arc::clone(&writer_interrupt),
        );

        let writer = Writer {
            ready: Arc::clone(&ready),
            ingested,
            committed: Arc::clone(&committed),
            commits: commits.clone(),
            metrics: Arc::clone(&metrics),
            disk: Arc::clone(&disk_budget),
            gate: Arc::clone(&gate),
            active: writer_active,
            projection: projection.clone(),
            retention_rows: config.archive_batch_rows,
        };

        let mut joins = Vec::new();
        spawn_writer_side(conn, writer_rx, writer, writer_interrupt, &mut joins);

        let (maintenance, maintenance_rx) = mpsc::channel(config.queue_capacity.max(1));
        let maintainer = Maintainer {
            ready: Arc::clone(&ready),
            metrics: Arc::clone(&metrics),
            gate: Arc::clone(&gate),
            disk: Arc::clone(&disk_budget),
            cancel: Arc::clone(&maintenance_cancel),
            deadline: maintenance_deadline,
            watchdog_alive: Arc::new(AtomicBool::new(true)),
            archives: archives.clone(),
            publication: writer_tx.clone(),
            timeout: Duration::from_millis(config.maintenance_timeout_ms),
            export_rows: config.archive_batch_rows,
            partition_ms: config.archive_partition_ms,
            compact_files: config.compact_max_files,
            compact_bytes: config.compact_max_bytes,
        };
        spawn_maintenance_side(
            maintenance_connection,
            native_interrupt,
            maintainer,
            maintenance_rx,
            &mut joins,
        );

        let shared = ReaderShared {
            ready: Arc::clone(&ready),
            metrics: Arc::clone(&metrics),
            gate: Arc::clone(&gate),
            archives: archives.clone(),
            max_result_bytes: config.max_query_bytes,
        };
        let (readers, interrupts) = spawn_reader_side(
            reader_connections,
            config.queue_capacity.max(1),
            &shared,
            &mut joins,
        );

        Ok(Self {
            inner: Arc::new(Inner {
                projection,
                writer: writer_tx,
                maintenance,
                readers,
                interrupts,
                query_progress,
                maintenance_progress,
                next: AtomicU64::new(0),
                committed,
                ready,
                admission,
                accepting: Mutex::new(true),
                bytes: Arc::new(Semaphore::new(config.queue_bytes)),
                max_queue_bytes: config.queue_bytes,
                forward_bytes: Arc::new(Semaphore::new(forward_byte_share(config.queue_bytes))),
                forward_slots: Arc::new(Semaphore::new((config.queue_capacity / 2).max(1))),
                timeout: Duration::from_millis(config.query_timeout_ms),
                commits,
                max_result_bytes: config.max_query_bytes,
                data_dir: config.data_dir,
                metrics,
                archive_leases: gate,
                quota: disk_budget,
                maintenance_cancel,
                maintenance_interrupt,
                joins: Mutex::new(joins),
                shutdown: Arc::new(tokio::sync::Mutex::new(None)),
            }),
        })
    }

    /// Receives the cumulative committed-event count after each commit.
    pub fn subscribe_commits(&self) -> watch::Receiver<u64> {
        self.inner.commits.subscribe()
    }

    /// Filesystem work: callers should run this in their bounded blocking pool.
    ///
    /// # Errors
    /// Fails if storage admission is closed or the filesystem cannot be inspected.
    pub fn disk_usage(&self) -> Result<DiskUsage> {
        if !self.is_ready() {
            return Err(storage_unavailable());
        }

        let footprint = quota::footprint(&self.inner.data_dir)?;

        Ok(DiskUsage {
            size_bytes: footprint.total(),
            database_bytes: footprint.database,
            wal_bytes: footprint.wal,
            archive_bytes: footprint.archives,
            temp_bytes: footprint.temporary,
            other_bytes: footprint.other,
        })
    }

    /// Renders the storage metrics in the Prometheus text exposition format.
    ///
    /// # Panics
    /// Panics if the archive-lease lock is poisoned.
    pub fn metrics_prometheus(&self) -> String {
        let db =
            std::fs::metadata(self.inner.data_dir.join("events.duckdb")).map_or(0, |m| m.len());
        let wal =
            std::fs::metadata(self.inner.data_dir.join("events.duckdb.wal")).map_or(0, |m| m.len());
        let mut files = 0;
        let mut archived_bytes = 0;

        if let Ok(entries) = std::fs::read_dir(self.inner.data_dir.join("archives")) {
            for entry in entries.flatten() {
                if entry.path().extension().is_some_and(|ext| ext == "parquet") {
                    files += 1;
                    archived_bytes += entry.metadata().map_or(0, |m| m.len());
                }
            }
        }

        let temp_bytes = std::fs::read_dir(self.inner.data_dir.join("temp")).map_or(0, |entries| {
            entries
                .flatten()
                .map(|entry| entry.metadata().map_or(0, |m| m.len()))
                .sum::<u64>()
        });

        let deferred = self.inner.archive_leases.lock().unwrap().pending_count();
        let m = &self.inner.metrics;
        let retention_before = m.retention_before_ms.load(Ordering::Acquire);
        let size_target = self.inner.quota.limit.unwrap_or(0);
        let disk_bytes = if self.inner.quota.limit.is_some() {
            self.inner.quota.measured.load(Ordering::Acquire)
        } else {
            db + wal + archived_bytes + temp_bytes
        };

        let size_paused = u8::from(self.inner.quota.blocked.load(Ordering::Acquire));
        let reserved_disk = self.inner.quota.reserved.load(Ordering::Acquire);
        let samples = format!(
            "logbrook_storage_size_target_bytes {size_target}\n\
                logbrook_storage_size_bytes {disk_bytes}\n\
                logbrook_storage_size_paused {size_paused}\n\
                logbrook_storage_disk_reserved_bytes {reserved_disk}\n\
                logbrook_storage_retention_before_ms {retention_before}\n\
                logbrook_storage_queue_used {}\n\
                logbrook_storage_reader_queue_used {}\n\
                logbrook_storage_readers_busy {}\n\
                logbrook_storage_query_timeouts_total {}\n\
                logbrook_storage_maintenance_failures_total {}\n\
                logbrook_storage_commit_seconds_sum {}\n\
                logbrook_storage_commits_total {}\n\
                logbrook_storage_group_requests_total {}\n\
                logbrook_storage_group_events_total {}\n\
                logbrook_storage_group_bytes_total {}\n\
                logbrook_storage_archive_files {files}\n\
                logbrook_storage_deferred_unlinks {deferred}\n\
                logbrook_storage_archive_bytes {archived_bytes}\n\
                logbrook_storage_database_bytes {db}\n\
                logbrook_storage_wal_bytes {wal}\n\
                logbrook_storage_temp_bytes {temp_bytes}\n",
            self.inner.writer.max_capacity() - self.inner.writer.capacity(),
            self.inner.readers[0].max_capacity() - self.inner.readers[0].capacity(),
            m.busy_readers.load(Ordering::Relaxed),
            m.timeouts.load(Ordering::Relaxed),
            m.maintenance_failures.load(Ordering::Relaxed),
            m.commit_ns.load(Ordering::Relaxed) as f64 / NANOS_PER_SECOND,
            m.commits.load(Ordering::Relaxed),
            m.group_requests.load(Ordering::Relaxed),
            m.group_events.load(Ordering::Relaxed),
            m.group_bytes.load(Ordering::Relaxed)
        );
        let mut output = describe_metric_samples(&samples);
        push_query_progress(
            &mut output,
            &self.inner.query_progress,
            &self.inner.maintenance_progress,
        );
        output
    }

    /// Returns the next page of matching events after an optional cursor.
    ///
    /// # Errors
    /// Fails for invalid or expired cursors, unavailable storage, query deadlines,
    /// integrity failures, or results that exceed the configured byte budget.
    pub async fn tail_filtered(&self, after_id: Option<String>, q: Query) -> Result<SearchResult> {
        let after = after_id
            .map(|token| crate::query_cursor::decode_tail(&q, &token))
            .transpose()?;
        match self.query(q, Operation::Tail(after)).await? {
            Answer::Search(v) => Ok(v),
            _ => unreachable!("query answer matches its operation"),
        }
    }

    /// Like [`Storage::tail_filtered`], with a response byte budget.
    ///
    /// # Errors
    /// Fails for a zero byte budget, invalid or expired cursors, unavailable storage,
    /// query deadlines, integrity failures, or results that exceed the byte budget.
    pub async fn tail_filtered_bounded(
        &self,
        after_id: Option<String>,
        q: Query,
        max_bytes: usize,
    ) -> Result<SearchResult> {
        if max_bytes == 0 {
            return Err(Error::invalid("tail byte budget must be positive"));
        }

        let after = after_id
            .map(|token| crate::query_cursor::decode_tail(&q, &token))
            .transpose()?;
        match self
            .query_bounded(q, Operation::Tail(after), Some(max_bytes))
            .await?
        {
            Answer::Search(value) => Ok(value),
            _ => unreachable!("query answer matches its operation"),
        }
    }

    /// Events committed since this process opened the index.
    pub fn committed_events(&self) -> u64 {
        self.inner.committed.load(Ordering::Relaxed)
    }

    /// Cancels the running maintenance command and interrupts its native work.
    pub fn cancel_maintenance(&self) {
        self.inner.maintenance_cancel.store(true, Ordering::Release);
        (self.inner.maintenance_interrupt)();
    }

    /// Stops accepting new appends and cancels running maintenance.
    ///
    /// # Panics
    /// Panics if an admission or maintenance interruption-state lock is poisoned.
    pub fn stop_admission(&self) {
        *self.inner.accepting.lock().expect(STORAGE_ADMISSION_LOCK) = false;
        self.cancel_maintenance();
    }

    /// Whether the index accepts work: admission is open and every worker remains healthy.
    ///
    /// # Panics
    /// Panics if an ingress or storage admission lock is poisoned.
    pub fn is_ready(&self) -> bool {
        self.inner.ready.load(Ordering::Acquire)
            && *self
                .inner
                .admission
                .accepting
                .lock()
                .expect(INGRESS_ADMISSION_LOCK)
            && *self.inner.accepting.lock().expect(STORAGE_ADMISSION_LOCK)
    }

    /// Historical imports intentionally bypass the live retention cutoff.
    ///
    /// # Errors
    /// Fails when admission or byte budgets are exhausted, storage is unavailable,
    /// or preparation or commit cannot complete.
    pub async fn append(&self, source: String, events: Vec<NormalizedEvent>) -> Result<usize> {
        self.append_with_policy(source, events, IngestionPolicy::Import, None)
            .await
    }

    /// Appends live events, charged to the caller's admission permit.
    ///
    /// # Errors
    /// Fails for events older than retention, exhausted admission or byte budgets,
    /// unavailable storage, or preparation or commit failures.
    pub async fn append_with_permit(
        &self,
        source: String,
        events: Vec<NormalizedEvent>,
        permit: OwnedSemaphorePermit,
    ) -> Result<usize> {
        self.append_with_policy(source, events, IngestionPolicy::Live, Some(permit))
            .await
    }

    /// Appends events under `policy`, with an optional admission permit.
    ///
    /// # Errors
    /// Fails when the policy rejects an event, admission or byte budgets are exhausted,
    /// storage is unavailable, or preparation or commit cannot complete.
    pub async fn append_with_policy(
        &self,
        source: String,
        events: Vec<NormalizedEvent>,
        policy: IngestionPolicy,
        request_permit: Option<OwnedSemaphorePermit>,
    ) -> Result<usize> {
        self.append_admitted(
            source,
            events,
            request_permit,
            AppendOptions {
                policy,
                lane: AppendLane::Direct,
                validation: None,
                preparation_wait: None,
            },
        )
        .await
    }

    pub(crate) async fn append_validated_live(
        &self,
        source: String,
        events: Vec<NormalizedEvent>,
        permit: OwnedSemaphorePermit,
        validation: crate::ingestion::LiveValidation,
        lane: AppendLane,
        preparation_wait: Option<Duration>,
    ) -> Result<usize> {
        self.append_admitted(
            source,
            events,
            Some(permit),
            AppendOptions {
                policy: IngestionPolicy::Live,
                lane,
                validation: Some(validation),
                preparation_wait,
            },
        )
        .await
    }

    #[cfg(test)]
    async fn append_forward(
        &self,
        source: String,
        events: Vec<NormalizedEvent>,
        permit: OwnedSemaphorePermit,
    ) -> Result<usize> {
        self.append_admitted(
            source,
            events,
            Some(permit),
            AppendOptions {
                policy: IngestionPolicy::Live,
                lane: AppendLane::Forward,
                validation: None,
                preparation_wait: None,
            },
        )
        .await
    }

    /// Returns up to `limit` events from `sources` after an optional cursor.
    ///
    /// # Errors
    /// Fails for invalid or expired cursors, unavailable storage, query deadlines,
    /// integrity failures, or results that exceed the configured byte budget.
    pub async fn tail(
        &self,
        after_id: Option<String>,
        sources: Vec<String>,
        limit: usize,
    ) -> Result<SearchResult> {
        let q = Query {
            from: i64::MIN,
            to: i64::MAX,
            sources,
            limit,
            ..Query::default()
        };
        self.tail_filtered(after_id, q).await
    }

    /// Inspect actual coverage through the bounded reader and archive integrity path.
    ///
    /// # Errors
    /// Fails if storage is unavailable, the read exceeds its deadline or byte budget,
    /// or durable data fails integrity checks.
    pub async fn projection_status(&self) -> Result<ProjectionStatus> {
        let query = Query {
            from: i64::MIN,
            to: i64::MAX,
            limit: 1,
            ..Query::default()
        };
        match self.query(query, Operation::ProjectionStatus).await? {
            Answer::ProjectionStatus(status) => Ok(status),
            _ => unreachable!("query answer matches its operation"),
        }
    }

    /// Returns one page of events matching `q`.
    ///
    /// # Errors
    /// Fails for invalid queries or cursors, unavailable storage, query deadlines,
    /// integrity failures, or results that exceed the configured byte budget.
    pub async fn search(&self, q: Query) -> Result<SearchResult> {
        match self.query(q, Operation::Search).await? {
            Answer::Search(v) => Ok(v),
            _ => unreachable!("query answer matches its operation"),
        }
    }

    /// Counts the events matching `q`.
    ///
    /// # Errors
    /// Fails for invalid queries, unavailable storage, query deadlines, integrity
    /// failures, or results that exceed the configured byte budget.
    pub async fn count(&self, q: Query) -> Result<u64> {
        match self.query(q, Operation::Count).await? {
            Answer::Count(v) => Ok(v),
            _ => unreachable!("query answer matches its operation"),
        }
    }

    /// Returns facet counts for the events matching `q`.
    ///
    /// # Errors
    /// Fails for invalid queries, unavailable storage, query deadlines, integrity
    /// failures, or results that exceed the configured byte budget.
    pub async fn facets(&self, q: Query) -> Result<Facets> {
        match self.query(q, Operation::Facets).await? {
            Answer::Facets(v) => Ok(v),
            _ => unreachable!("query answer matches its operation"),
        }
    }

    /// Returns event counts per `interval_ms` bucket, which must be positive.
    ///
    /// # Errors
    /// Fails for invalid queries or bucket intervals, unavailable storage, query
    /// deadlines, integrity failures, or results that exceed the configured byte budget.
    pub async fn histogram(&self, q: Query, interval_ms: i64) -> Result<Vec<HistogramBucket>> {
        if interval_ms <= 0 {
            return Err(Error::invalid("interval must be positive"));
        }

        match self.query(q, Operation::Histogram(interval_ms)).await? {
            Answer::Histogram(v) => Ok(v),
            _ => unreachable!("query answer matches its operation"),
        }
    }

    /// Exports events before `before` (Unix milliseconds) and returns the exported count.
    ///
    /// # Errors
    /// Fails if storage or its maintenance queue is unavailable, the operation exceeds
    /// its deadline or disk budget, or archive export or publication fails.
    pub async fn archive(&self, before: i64) -> Result<usize> {
        if !self.is_ready() {
            return Err(storage_unavailable());
        }

        let (tx, rx) = oneshot::channel();
        self.inner
            .maintenance
            .try_send(Maintenance::Archive(before, tx))
            .map_err(queue_error)?;
        rx.await.map_err(db_error)?
    }

    /// Expires the oldest data until the index is back under its size target.
    ///
    /// # Errors
    /// Fails if storage or its maintenance queue is unavailable, the operation exceeds
    /// its deadline, or no safe disk reclaim can meet the size target.
    pub async fn enforce_size_limit(&self) -> Result<usize> {
        if !self.is_ready() {
            return Err(storage_unavailable());
        }

        let (reply, answer) = oneshot::channel();
        self.inner
            .maintenance
            .try_send(Maintenance::EnforceSize(reply))
            .map_err(queue_error)?;
        answer.await.map_err(db_error)?
    }

    /// Merges small archive files and returns the merged row count.
    ///
    /// # Errors
    /// Fails if storage or its maintenance queue is unavailable, the operation exceeds
    /// its deadline or disk budget, or archive validation or publication fails.
    pub async fn compact(&self) -> Result<usize> {
        if !self.is_ready() {
            return Err(storage_unavailable());
        }

        let (tx, rx) = oneshot::channel();
        self.inner
            .maintenance
            .try_send(Maintenance::Compact(tx))
            .map_err(queue_error)?;
        rx.await.map_err(db_error)?
    }

    /// Removes hot events before `before` (Unix milliseconds) and returns the removed count.
    ///
    /// # Errors
    /// Fails if storage or its maintenance queue is unavailable, the operation exceeds
    /// its deadline, or retained data cannot be validated or deleted.
    pub async fn retain(&self, before: i64) -> Result<usize> {
        if !self.is_ready() {
            return Err(storage_unavailable());
        }

        let (tx, rx) = oneshot::channel();
        self.inner
            .maintenance
            .try_send(Maintenance::Retain(before, tx))
            .map_err(queue_error)?;
        rx.await.map_err(db_error)?
    }

    /// Old cloned handles may call shutdown during index deletion. Serialize the
    /// owned drain and cache its outcome so cancellation cannot orphan workers.
    ///
    /// # Errors
    /// Fails when a worker closes unexpectedly, checkpointing fails, or an owned
    /// worker or shutdown task panics.
    ///
    /// # Panics
    /// Panics if an admission or maintenance interruption-state lock is poisoned.
    pub async fn shutdown(&self) -> Result<()> {
        self.stop_admission();

        let mut shutdown = Arc::clone(&self.inner.shutdown).lock_owned().await;
        let storage = self.clone();
        tokio::spawn(async move {
            if shutdown.is_none() {
                *shutdown = Some(storage.shutdown_workers().await);
            }

            shutdown
                .as_ref()
                .expect("completed storage shutdown")
                .as_ref()
                .copied()
                .map_err(Clone::clone)
        })
        .await
        .map_err(|error| Error::unavailable("storage shutdown worker failed").with_source(error))?
    }

    async fn shutdown_workers(&self) -> Result<()> {
        self.stop_admission();
        self.inner.maintenance_cancel.store(true, Ordering::Release);
        (self.inner.maintenance_interrupt)();

        for interrupt in &self.inner.interrupts {
            interrupt(u64::MAX);
        }

        let mut failure = self.stop_maintenance().await;
        if let Some(error) = self.stop_readers().await {
            failure = Some(error);
        }

        // Reader stop replies are sent only after their last transaction has completed.
        for interrupt in &self.inner.interrupts {
            interrupt(u64::MAX);
        }

        if let Some(error) = self.stop_writer().await {
            failure = Some(error);
        }

        let joins = std::mem::take(&mut *self.inner.joins.lock().unwrap());
        tokio::task::spawn_blocking(move || {
            for join in joins {
                if join.join().is_err() {
                    failure = Some(Error::unavailable("database worker panicked"));
                }
            }

            failure.map_or(Ok(()), Err)
        })
        .await
        .map_err(db_error)?
    }

    /// Stops the maintenance worker after its queued commands. Returns the failure, if any.
    async fn stop_maintenance(&self) -> Option<Error> {
        let (tx, rx) = oneshot::channel();
        if self
            .inner
            .maintenance
            .send(Maintenance::Stop(tx))
            .await
            .is_ok()
        {
            rx.await.err().map(db_error)
        } else {
            Some(Error::unavailable("maintenance worker closed"))
        }
    }

    /// Stops every reader, waiting for each stop reply. Returns the last failure, if any.
    async fn stop_readers(&self) -> Option<Error> {
        let mut failure = None;
        for reader in &self.inner.readers {
            let (tx, stopped) = oneshot::channel();
            if reader
                .send(Read {
                    id: 0,
                    query: None,
                    operation: Operation::Stop,
                    reply: tx,
                    cancelled: Arc::new(AtomicBool::new(false)),
                    deadline: Instant::now() + self.inner.timeout,
                    max_bytes: self.inner.max_result_bytes,
                })
                .await
                .is_err()
            {
                failure = Some(Error::unavailable("reader closed"));
            } else {
                let _ = stopped.await;
            }
        }

        failure
    }

    /// Stops the writer after its queued commands and checkpoint. Returns the failure, if any.
    async fn stop_writer(&self) -> Option<Error> {
        let (tx, rx) = oneshot::channel();
        if self.inner.writer.send(Write::Stop(tx)).await.is_ok() {
            match rx.await {
                Ok(Ok(())) => None,
                Ok(Err(e)) => Some(e),
                Err(e) => Some(db_error(e)),
            }
        } else {
            Some(writer_closed())
        }
    }
}

#[cfg(test)]
mod tests;
