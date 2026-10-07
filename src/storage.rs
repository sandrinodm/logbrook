//! Dedicated connection owners. Acknowledgements follow explicit appender flush and commit.
use crate::{
    config::StorageConfig,
    model::{
        Error, ErrorKind, Event, FacetValue, Facets, HistogramBucket, NormalizedEvent, Query,
        SearchResult,
    },
};
use duckdb::{Connection, params, params_from_iter, types::Value};
use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};

mod archive;
mod leases;
mod query;
mod quota;

#[cfg(test)]
use archive::export;
use archive::{ArchiveWork, compact_export, expired_id, export_bounded, migrate, recover};
#[cfg(test)]
use archive::{publish, retain};
use leases::{ArchiveLeases, cleanup_archives};
use query::{QueryDeadline, ResultBudget, dataset, run_query};
use quota::DiskBudget;

type Result<T> = std::result::Result<T, Error>;

fn db_error(e: impl std::fmt::Display + std::any::Any) -> Error {
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
        return Error::new(ErrorKind::Integrity, message);
    }

    if let Ok(details) = serde_json::from_str::<serde_json::Value>(&message) {
        let kind = details.get("exception_type").and_then(|v| v.as_str());

        if matches!(kind, Some("Fatal" | "Internal" | "Serialization")) {
            return Error::new(ErrorKind::Integrity, message);
        }
    }

    Error::unavailable(message)
}

fn queue_error<T>(error: mpsc::error::TrySendError<T>) -> Error {
    match error {
        mpsc::error::TrySendError::Full(_) => {
            Error::new(ErrorKind::Overloaded, "storage queue full")
        }
        mpsc::error::TrySendError::Closed(_) => Error::unavailable("storage worker closed"),
    }
}

#[derive(Clone)]
pub struct Storage {
    inner: Arc<Inner>,
}

/// A fresh file-byte snapshot, including unpublished and unmanaged files.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct DiskUsage {
    pub size_bytes: u64,
    pub database_bytes: u64,
    pub wal_bytes: u64,
    pub archive_bytes: u64,
    pub temp_bytes: u64,
    pub other_bytes: u64,
}

struct Inner {
    writer: mpsc::Sender<Write>,
    maintenance: mpsc::Sender<Maintenance>,
    readers: Vec<mpsc::Sender<Read>>,
    interrupts: Vec<Arc<dyn Fn(u64) + Send + Sync>>,
    next: AtomicU64,
    committed: Arc<AtomicU64>,
    ready: Arc<AtomicBool>,
    bytes: Arc<Semaphore>,
    timeout: Duration,
    commits: watch::Sender<u64>,
    data_dir: std::path::PathBuf,
    max_result_bytes: usize,
    metrics: Arc<Metrics>,
    archive_leases: Arc<Mutex<ArchiveLeases>>,
    quota: Arc<DiskBudget>,
    maintenance_cancel: Arc<AtomicBool>,
    maintenance_interrupt: Arc<dyn Fn() + Send + Sync>,
    joins: Mutex<Vec<thread::JoinHandle<()>>>,
    shutdown: Arc<tokio::sync::Mutex<Option<Result<()>>>>,
}

struct QueuedEvent {
    event: NormalizedEvent,
    attributes: String,
}

#[derive(Default)]
struct Metrics {
    retention_before_ms: AtomicI64,
    busy_readers: AtomicU64,
    timeouts: AtomicU64,
    maintenance_failures: AtomicU64,
    commit_ns: AtomicU64,
    commits: AtomicU64,
}

struct Published {
    path: String,
    min: i64,
    max: i64,
    count: i64,
    before: i64,
    high: i64,
    artifact: archive::ArtifactGuard,
    reservation: Option<quota::Reservation>,
    replaced: Vec<String>,
    min_id: i64,
    bytes: u64,
    lower: i64,
}

enum Maintenance {
    Archive(i64, oneshot::Sender<Result<usize>>),
    Compact(oneshot::Sender<Result<usize>>),
    EnforceSize(oneshot::Sender<Result<usize>>),
    Retain(i64, oneshot::Sender<Result<usize>>),
    Stop(oneshot::Sender<()>),
}

#[cfg(test)]
type WriterTest = Box<dyn FnOnce(&mut Connection, &Arc<Mutex<Option<MaintenanceBudget>>>) + Send>;

enum Write {
    #[cfg(test)]
    Test(WriterTest),
    Append(
        String,
        Vec<QueuedEvent>,
        oneshot::Sender<Result<usize>>,
        tokio::sync::OwnedSemaphorePermit,
        Option<tokio::sync::OwnedSemaphorePermit>,
    ),
    Publish(Published, MaintenanceBudget, oneshot::Sender<Result<usize>>),
    Retain(i64, i64, MaintenanceBudget, oneshot::Sender<Result<usize>>),
    Checkpoint(MaintenanceBudget, oneshot::Sender<Result<()>>),
    Stop(oneshot::Sender<Result<()>>),
}

enum Operation {
    Search,
    Count,
    Facets,
    Histogram(i64),
    #[cfg(test)]
    Expensive,
    Tail(Option<i64>, usize),
    Stop,
}

#[derive(serde::Serialize)]
enum Answer {
    Search(SearchResult),
    Count(u64),
    Facets(Facets),
    Histogram(Vec<HistogramBucket>),
}

// The crate disconnects before clearing InterruptHandle. Own destruction behind
// the same state mutex all interrupt paths hold, including unwinding on panic.
struct ConnectionOwner<T> {
    connection: Option<Connection>,
    state: Arc<Mutex<Option<T>>>,
    alive: Arc<AtomicBool>,
}

impl<T> Drop for ConnectionOwner<T> {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *state = None;

        drop(self.connection.take());
    }
}

struct WorkerHealth(Arc<AtomicBool>);

impl Drop for WorkerHealth {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

struct Cancellation {
    cancelled: Arc<AtomicBool>,
    interrupt: Arc<dyn Fn(u64) + Send + Sync>,
    id: u64,
}

impl Drop for Cancellation {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        (self.interrupt)(self.id);
    }
}

struct Read {
    id: u64,
    query: Option<Query>,
    operation: Operation,
    reply: oneshot::Sender<Result<Answer>>,
    cancelled: Arc<AtomicBool>,
    deadline: Instant,
    max_bytes: usize,
}

fn check_cancelled(cancelled: &AtomicBool, deadline: Instant) -> Result<()> {
    if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
        return Err(Error::new(ErrorKind::Timeout, "query deadline exceeded"));
    }

    Ok(())
}

#[derive(Clone)]
struct MaintenanceBudget {
    cancelled: Arc<AtomicBool>,
    deadline: Instant,
}

impl MaintenanceBudget {
    fn check(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Acquire) || Instant::now() >= self.deadline {
            Err(Error::new(
                ErrorKind::Timeout,
                "maintenance deadline exceeded",
            ))
        } else {
            Ok(())
        }
    }

    fn error(&self, error: Error) -> Error {
        if error.kind == ErrorKind::Integrity {
            error
        } else {
            self.check().err().unwrap_or(error)
        }
    }
}

/// Only maintenance SQL can arm the writer interrupt. The shared mutex makes
/// disarming atomic with respect to watchdog and external cancellation calls.
struct WriterScope {
    budget: MaintenanceBudget,
    active: Arc<Mutex<Option<MaintenanceBudget>>>,
}

impl WriterScope {
    fn start(
        budget: MaintenanceBudget,
        active: &Arc<Mutex<Option<MaintenanceBudget>>>,
    ) -> Result<Self> {
        budget.check()?;
        *active.lock().unwrap() = Some(budget.clone());

        Ok(Self {
            budget,
            active: active.clone(),
        })
    }

    fn check(&self) -> Result<()> {
        let result = self.budget.check();

        if result.is_err() {
            self.disarm();
        }

        result
    }

    fn error(&self, error: duckdb::Error) -> Error {
        self.disarm();
        self.budget.error(db_error(error))
    }

    fn disarm(&self) {
        *self.active.lock().unwrap() = None;
    }

    fn finish(&self) -> Result<()> {
        let mut active = self.active.lock().unwrap();
        let result = self.budget.check();
        *active = None;

        result
    }
}

impl Drop for WriterScope {
    fn drop(&mut self) {
        self.disarm();
    }
}

fn maintenance_outcome<T>(result: &Result<T>, metrics: &Metrics, ready: &AtomicBool) {
    if let Err(error) = result {
        metrics.maintenance_failures.fetch_add(1, Ordering::Relaxed);

        if error.kind == ErrorKind::Integrity {
            ready.store(false, Ordering::Release);
        }
    }
}

fn finish_archive_publication(
    exported: Result<Option<Published>>,
    writer: &mpsc::Sender<Write>,
    budget: &MaintenanceBudget,
    metrics: &Metrics,
    ready: &AtomicBool,
    reply: oneshot::Sender<Result<usize>>,
) {
    // Keep cleanup ownership on maintenance until the reply is sent. Writer
    // errors must not unlink output while holding the snapshot gate.
    let _ownership = match &exported {
        Ok(Some(file)) => Some((file.reservation.clone(), file.artifact.clone())),
        _ => None,
    };

    let result = match exported {
        Ok(Some(file)) => {
            let (sender, receiver) = oneshot::channel();
            maintenance_send(writer, Write::Publish(file, budget.clone(), sender), budget)
                .and_then(|_| maintenance_reply(receiver, budget))
        }
        Ok(None) => budget.check().map(|_| 0),
        Err(error) => Err(budget.error(error)),
    };

    maintenance_outcome(&result, metrics, ready);
    let _ = reply.send(result);
}

fn maintenance_send(
    writer: &mpsc::Sender<Write>,
    mut command: Write,
    budget: &MaintenanceBudget,
) -> Result<()> {
    loop {
        budget.check()?;

        match writer.try_send(command) {
            Ok(()) => return Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(Error::unavailable("writer closed"));
            }

            Err(mpsc::error::TrySendError::Full(returned)) => command = returned,
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn maintenance_reply<T>(
    mut receiver: oneshot::Receiver<Result<T>>,
    budget: &MaintenanceBudget,
) -> Result<T> {
    loop {
        match receiver.try_recv() {
            Ok(result) => return result,
            Err(oneshot::error::TryRecvError::Closed) => {
                return Err(Error::unavailable("writer closed"));
            }

            Err(oneshot::error::TryRecvError::Empty) => budget.check()?,
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn maintenance_gate<'a>(
    gate: &'a Mutex<ArchiveLeases>,
    budget: &MaintenanceBudget,
) -> Result<std::sync::MutexGuard<'a, ArchiveLeases>> {
    loop {
        budget.check()?;

        match gate.try_lock() {
            Ok(lease) => return Ok(lease),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(Error::new(
                    ErrorKind::Integrity,
                    "archive snapshot gate poisoned",
                ));
            }

            Err(std::sync::TryLockError::WouldBlock) => thread::sleep(Duration::from_millis(1)),
        }
    }
}

fn validate_archive_name(path: &str) -> Result<()> {
    if path.is_empty()
        || Path::new(path).components().count() != 1
        || path.contains(['*', '?', '[', ']', '{', '}'])
        || path == "."
        || path == ".."
    {
        return Err(Error::invalid("archive path must be a literal filename"));
    }

    Ok(())
}

impl Storage {
    pub fn open(config: StorageConfig) -> Result<Self> {
        Self::open_with_ingested_counter(config, Arc::new(AtomicU64::new(0)))
    }

    pub(crate) fn open_with_ingested_counter(
        config: StorageConfig,
        ingested: Arc<AtomicU64>,
    ) -> Result<Self> {
        let mut config = config;

        if config.data_dir.is_relative() {
            config.data_dir = std::env::current_dir()
                .map_err(db_error)?
                .join(&config.data_dir);
        }
        crate::config::validate_literal_path(&config.data_dir)?;

        if config.archive_batch_rows == 0
            || config.archive_partition_ms <= 0
            || config.compact_max_files < 2
            || config.compact_max_bytes == 0
            || config.maintenance_timeout_ms == 0
        {
            return Err(Error::invalid("invalid archive maintenance budgets"));
        }

        std::fs::create_dir_all(&config.data_dir).map_err(db_error)?;
        let temp = config.data_dir.join("temp");
        std::fs::create_dir_all(&temp).map_err(db_error)?;
        let archives = config.data_dir.join("archives");
        std::fs::create_dir_all(&archives).map_err(db_error)?;

        let mut conn = Connection::open(config.data_dir.join("events.duckdb")).map_err(db_error)?;
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

        // Validate durable state before workers can accept ingestion or queries.
        migrate(&mut conn)?;
        recover(&conn, &archives)?;
        let ready = Arc::new(AtomicBool::new(true));
        let retention_before: i64 = conn
            .query_row(
                "SELECT before_time FROM retention_state WHERE singleton=1",
                [],
                |row| row.get(0),
            )
            .map_err(db_error)?;
        let metrics = Arc::new(Metrics {
            retention_before_ms: AtomicI64::new(retention_before),
            ..Metrics::default()
        });

        let writer_metrics = metrics.clone();
        let gate = Arc::new(Mutex::new(ArchiveLeases::default()));
        let mut reader_connections = Vec::new();

        for _ in 0..config.reader_threads.max(1) {
            reader_connections.push(conn.try_clone().map_err(db_error)?);
        }

        let maintenance_connection = conn.try_clone().map_err(db_error)?;
        let (writer, mut rx) = mpsc::channel(config.queue_capacity.max(1));
        let reader_archives = archives.clone();
        let writer_ready = ready.clone();

        let committed = Arc::new(AtomicU64::new(0));
        let writer_committed = committed.clone();
        let (commits, _) = watch::channel(0);
        let writer_commits = commits.clone();

        let disk_budget = DiskBudget::new(config.data_dir.clone(), config.max_size_bytes)?;
        let writer_budget = disk_budget.clone();
        let writer_gate = gate.clone();
        let retention_rows = config.archive_batch_rows;

        let maintenance_cancel = Arc::new(AtomicBool::new(false));
        let writer_active = Arc::new(Mutex::new(None::<MaintenanceBudget>));
        let writer_alive = Arc::new(AtomicBool::new(true));
        let writer_interrupt = conn.interrupt_handle();
        let writer_watch_active = writer_active.clone();
        let writer_watch_alive = writer_alive.clone();
        let writer_watch_interrupt = writer_interrupt.clone();

        // Each native connection has one worker owner. Watchdogs only interrupt
        // under its state mutex, which also protects connection destruction.
        let mut joins = vec![thread::spawn(move || {
            let _health = WorkerHealth(writer_ready.clone());
            let mut owner = ConnectionOwner {
                connection: Some(conn),
                state: writer_active.clone(),
                alive: writer_alive,
            };

            while let Some(cmd) = rx.blocking_recv() {
                let conn = owner.connection.as_mut().unwrap();

                match cmd {
                    #[cfg(test)]
                    Write::Test(task) => task(conn, &writer_active),
                    Write::Append(source, events, reply, _permit, _request_permit) => {
                        let started = Instant::now();
                        let mut result = writer_budget.check_write().and_then(|_| {
                            append_queued(conn, &source, &events, _request_permit.is_some())
                        });

                        if result
                            .as_ref()
                            .is_err_and(|error| error.kind != ErrorKind::SizeLimit)
                        {
                            let _ = conn.execute_batch("ROLLBACK");

                            if conn
                                .query_row(
                                    "SELECT next_id FROM ingest_state WHERE singleton=1",
                                    [],
                                    |r| r.get::<_, i64>(0),
                                )
                                .is_err()
                            {
                                result = Err(Error::new(
                                    ErrorKind::Integrity,
                                    "writer connection recovery failed",
                                ));
                            }
                        }

                        writer_metrics
                            .commit_ns
                            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        writer_metrics.commits.fetch_add(1, Ordering::Relaxed);

                        if let Ok(count) = &result {
                            ingested.fetch_add(*count as u64, Ordering::Relaxed);
                            let _ = writer_budget.refresh_native();
                            let total = writer_committed
                                .fetch_add(*count as u64, Ordering::Relaxed)
                                + *count as u64;
                            writer_commits.send_replace(total);
                        }

                        if result
                            .as_ref()
                            .is_err_and(|e| matches!(e.kind, ErrorKind::Integrity))
                        {
                            writer_ready.store(false, Ordering::Release);
                        }

                        let _ = reply.send(result);
                    }
                    Write::Publish(file, budget, reply) => {
                        // The bounded maintenance waiter may already have returned.
                        // Retain cleanup ownership until after releasing the gate.
                        let _cleanup = file.artifact.clone();
                        let _reservation = file.reservation.clone();
                        let replaced = file.replaced.clone();
                        let file_bytes = file.bytes;
                        let result =
                            maintenance_gate(&writer_gate, &budget).and_then(|mut lease| {
                                let scope = WriterScope::start(budget, &writer_active)?;
                                let result = archive::publish_bounded(conn, file, &scope);

                                drop(scope);

                                if result.is_ok() {
                                    lease.retire(replaced);
                                    writer_budget.published(file_bytes);
                                }

                                result
                            });

                        if result
                            .as_ref()
                            .is_err_and(|e| matches!(e.kind, ErrorKind::Integrity))
                        {
                            writer_ready.store(false, Ordering::Release);
                        }

                        let _ = reply.send(result);
                    }
                    Write::Retain(before, expired, budget, reply) => {
                        let result =
                            maintenance_gate(&writer_gate, &budget).and_then(|mut lease| {
                                let scope = WriterScope::start(budget, &writer_active)?;
                                archive::retain_bounded(
                                    conn,
                                    before,
                                    expired,
                                    &mut lease,
                                    retention_rows,
                                    &scope,
                                )
                            });

                        if result.is_ok() {
                            writer_metrics
                                .retention_before_ms
                                .fetch_max(before, Ordering::Release);
                            writer_commits.send_replace(writer_committed.load(Ordering::Relaxed));
                        }

                        if result
                            .as_ref()
                            .is_err_and(|e| matches!(e.kind, ErrorKind::Integrity))
                        {
                            writer_ready.store(false, Ordering::Release);
                        }

                        let _ = reply.send(result);
                    }
                    Write::Checkpoint(budget, reply) => {
                        let result = WriterScope::start(budget, &writer_active).and_then(|scope| {
                            conn.execute_batch("CHECKPOINT")
                                .map_err(|error| scope.error(error))?;
                            scope.finish()
                        });

                        let _ = writer_budget.refresh_native();

                        if result
                            .as_ref()
                            .is_err_and(|error| error.kind == ErrorKind::Integrity)
                        {
                            writer_ready.store(false, Ordering::Release);
                        }

                        let _ = reply.send(result);
                    }
                    Write::Stop(reply) => {
                        rx.close();

                        while let Some(cmd) = rx.blocking_recv() {
                            if let Write::Append(source, events, reply, _permit, _request_permit) =
                                cmd
                            {
                                let result = writer_budget.check_write().and_then(|_| {
                                    append_queued(conn, &source, &events, _request_permit.is_some())
                                });

                                if let Ok(count) = &result {
                                    ingested.fetch_add(*count as u64, Ordering::Relaxed);
                                    let _ = writer_budget.refresh_native();
                                    let total = writer_committed
                                        .fetch_add(*count as u64, Ordering::Relaxed)
                                        + *count as u64;
                                    writer_commits.send_replace(total);
                                }

                                if result
                                    .as_ref()
                                    .is_err_and(|e| matches!(e.kind, ErrorKind::Integrity))
                                {
                                    writer_ready.store(false, Ordering::Release);
                                }

                                let _ = reply.send(result);
                            }
                        }

                        let result = conn.execute_batch("CHECKPOINT").map_err(db_error);

                        if result
                            .as_ref()
                            .is_err_and(|e| matches!(e.kind, ErrorKind::Integrity))
                        {
                            writer_ready.store(false, Ordering::Release);
                        }

                        let _ = reply.send(result);
                        break;
                    }
                }
            }

            writer_ready.store(false, Ordering::Release);
        })];

        let writer_for_external = writer_watch_active.clone();
        joins.push(thread::spawn(move || {
            while writer_watch_alive.load(Ordering::Acquire) {
                {
                    let active = writer_watch_active.lock().unwrap();

                    if active
                        .as_ref()
                        .is_some_and(|budget| budget.check().is_err())
                    {
                        writer_watch_interrupt.interrupt();
                    }
                }
                thread::sleep(Duration::from_millis(5));
            }
        }));

        let (maintenance, mut maintenance_rx) = mpsc::channel(config.queue_capacity.max(1));
        let publication = writer.clone();
        let maintenance_ready = ready.clone();
        let maintenance_metrics = metrics.clone();
        let maintenance_gate = gate.clone();
        let maintenance_budget = disk_budget.clone();
        let cancel_export = maintenance_cancel.clone();
        let native_interrupt = maintenance_connection.interrupt_handle();
        let maintenance_deadline = Arc::new(Mutex::new(None::<Instant>));
        let interrupt_state = maintenance_deadline.clone();
        let native_for_external = native_interrupt.clone();
        let maintenance_interrupt: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let active = interrupt_state.lock().unwrap();

            if active.is_some() {
                native_for_external.interrupt();
            }

            let active = writer_for_external.lock().unwrap();

            if active.is_some() {
                writer_interrupt.interrupt();
            }
        });

        let maintenance_watchdog_alive = Arc::new(AtomicBool::new(true));
        let deadline_watch = maintenance_deadline.clone();
        let alive_watch = maintenance_watchdog_alive.clone();
        let interrupt_watch = native_interrupt;
        let cancel_watch = maintenance_cancel.clone();
        joins.push(thread::spawn(move || {
            while alive_watch.load(Ordering::Acquire) {
                {
                    let active = deadline_watch.lock().unwrap();

                    if active.is_some_and(|deadline| {
                        Instant::now() >= deadline || cancel_watch.load(Ordering::Acquire)
                    }) {
                        interrupt_watch.interrupt();
                    }
                }
                thread::sleep(Duration::from_millis(5));
            }
        }));

        let maintenance_timeout = Duration::from_millis(config.maintenance_timeout_ms);
        let export_rows = config.archive_batch_rows;
        let partition_ms = config.archive_partition_ms;
        let compact_files = config.compact_max_files;
        let compact_bytes = config.compact_max_bytes;
        joins.push(thread::spawn(move || {
            let _health = WorkerHealth(maintenance_ready.clone());
            let mut owner = ConnectionOwner {
                connection: Some(maintenance_connection),
                state: maintenance_deadline.clone(),
                alive: maintenance_watchdog_alive.clone(),
            };

            while let Some(command) = maintenance_rx.blocking_recv() {
                if let Maintenance::Stop(reply) = command {
                    maintenance_watchdog_alive.store(false, Ordering::Release);
                    let _ = reply.send(());
                    break;
                }

                let deadline = Instant::now() + maintenance_timeout;
                let budget = MaintenanceBudget {
                    cancelled: cancel_export.clone(),
                    deadline,
                };
                *maintenance_deadline.lock().unwrap() = Some(deadline);

                cleanup_archives(&maintenance_gate, &archives);
                let _ = maintenance_budget.audit();

                match command {
                    Maintenance::Archive(before, reply) => {
                        let result = export_bounded(
                            owner.connection.as_mut().unwrap(),
                            &archives,
                            before,
                            export_rows,
                            partition_ms,
                            ArchiveWork {
                                cancelled: &cancel_export,
                                deadline,
                                quota: maintenance_budget.limit.map(|_| &maintenance_budget),
                            },
                        );

                        finish_archive_publication(
                            result,
                            &publication,
                            &budget,
                            &maintenance_metrics,
                            &maintenance_ready,
                            reply,
                        );
                    }
                    Maintenance::EnforceSize(reply) => {
                        let result = quota::enforce_size(
                            owner.connection.as_mut().unwrap(),
                            &archives,
                            &maintenance_budget,
                            &publication,
                            quota::SizeWork {
                                gate: &maintenance_gate,
                                rows: export_rows,
                                budget: &budget,
                            },
                        )
                        .map_err(|error| budget.error(error));

                        if let Err(error) = &result {
                            maintenance_metrics
                                .maintenance_failures
                                .fetch_add(1, Ordering::Relaxed);

                            if error.kind == ErrorKind::Integrity {
                                maintenance_ready.store(false, Ordering::Release);
                            }
                        }

                        let _ = reply.send(result);
                    }
                    Maintenance::Retain(before, reply) => {
                        let result = expired_id(
                            owner.connection.as_mut().unwrap(),
                            &archives,
                            before,
                            &cancel_export,
                            deadline,
                        );

                        match result {
                            Ok(expired) => {
                                let (tx, rx) = oneshot::channel();
                                let result = maintenance_send(
                                    &publication,
                                    Write::Retain(before, expired, budget.clone(), tx),
                                    &budget,
                                )
                                .and_then(|_| maintenance_reply(rx, &budget));
                                maintenance_outcome(
                                    &result,
                                    &maintenance_metrics,
                                    &maintenance_ready,
                                );
                                let _ = reply.send(result);
                            }

                            Err(e) => {
                                let e = budget.error(e);
                                maintenance_metrics
                                    .maintenance_failures
                                    .fetch_add(1, Ordering::Relaxed);

                                if matches!(e.kind, ErrorKind::Integrity) {
                                    maintenance_ready.store(false, Ordering::Release);
                                }

                                let _ = reply.send(Err(e));
                            }
                        }
                    }
                    Maintenance::Compact(reply) => {
                        let result = compact_export(
                            owner.connection.as_mut().unwrap(),
                            &archives,
                            compact_files,
                            compact_bytes,
                            partition_ms,
                            ArchiveWork {
                                cancelled: &cancel_export,
                                deadline,
                                quota: maintenance_budget.limit.map(|_| &maintenance_budget),
                            },
                        );

                        finish_archive_publication(
                            result,
                            &publication,
                            &budget,
                            &maintenance_metrics,
                            &maintenance_ready,
                            reply,
                        );
                    }
                    Maintenance::Stop(_) => unreachable!(),
                }
                *maintenance_deadline.lock().unwrap() = None;
                let _ = owner.connection.as_mut().unwrap().execute_batch("ROLLBACK");

                cleanup_archives(&maintenance_gate, &archives);
                let _ = maintenance_budget.audit();
            }

            maintenance_watchdog_alive.store(false, Ordering::Release);

            drop(owner);
        }));

        // Readers compete for one shared queue so an idle connection can take
        // the next request while another connection is still running SQL.
        let (reader_tx, reader_rx) = mpsc::channel::<Read>(config.queue_capacity.max(1));
        let reader_rx = Arc::new(Mutex::new(reader_rx));
        let mut readers = Vec::new();
        let mut interrupts: Vec<Arc<dyn Fn(u64) + Send + Sync>> = Vec::new();

        for connection in reader_connections {
            readers.push(reader_tx.clone());
            let shared_rx = reader_rx.clone();
            let gate = gate.clone();
            let reader_archives = reader_archives.clone();
            let ready = ready.clone();
            let max_result_bytes = config.max_query_bytes;
            let reader_metrics = metrics.clone();
            let running = Arc::new(Mutex::new(None::<(u64, Instant, Arc<AtomicBool>)>));
            let watchdog_alive = Arc::new(AtomicBool::new(true));
            let watchdog_running = running.clone();
            let watchdog_stop = watchdog_alive.clone();
            let watchdog_interrupt = connection.interrupt_handle();
            joins.push(thread::spawn(move || {
                while watchdog_stop.load(Ordering::Acquire) {
                    {
                        let active = watchdog_running.lock().unwrap();

                        if let Some((_, deadline, cancelled)) = active.as_ref()
                            && (cancelled.load(Ordering::Acquire) || Instant::now() >= *deadline)
                        {
                            watchdog_interrupt.interrupt();
                        }
                    }
                    thread::sleep(Duration::from_millis(5));
                }
            }));
            let interrupt = connection.interrupt_handle();
            let current = running.clone();
            interrupts.push(Arc::new(move |id| {
                let active = current.lock().unwrap();

                if active
                    .as_ref()
                    .is_some_and(|(current, _, _)| id == u64::MAX || *current == id)
                {
                    interrupt.interrupt();
                }
            }));
            joins.push(thread::spawn(move || {
                let _health = WorkerHealth(ready.clone());
                let mut owner = ConnectionOwner {
                    connection: Some(connection),
                    state: running.clone(),
                    alive: watchdog_alive.clone(),
                };

                loop {
                    let Some(req) = shared_rx.lock().unwrap().blocking_recv() else {
                        break;
                    };

                    if matches!(req.operation, Operation::Stop) {
                        let _ = req.reply.send(Ok(Answer::Count(0)));
                        break;
                    }

                    if req.cancelled.load(Ordering::Acquire) {
                        continue;
                    }

                    {
                        let mut active = running.lock().unwrap();

                        if req.cancelled.load(Ordering::Acquire) {
                            continue;
                        }
                        *active = Some((req.id, req.deadline, req.cancelled.clone()));
                    }

                    reader_metrics.busy_readers.fetch_add(1, Ordering::Relaxed);
                    let answer = run_query(
                        owner.connection.as_mut().unwrap(),
                        &reader_archives,
                        req.query.as_ref().unwrap(),
                        req.operation,
                        req.max_bytes.min(max_result_bytes),
                        &gate,
                        QueryDeadline {
                            cancelled: &req.cancelled,
                            at: req.deadline,
                        },
                    );
                    // Clear interruption state only after native work returns;
                    // a timed-out waiter does not make this connection available.
                    *running.lock().unwrap() = None;
                    reader_metrics.busy_readers.fetch_sub(1, Ordering::Relaxed);

                    if answer.is_err() {
                        let _ = owner.connection.as_mut().unwrap().execute_batch("ROLLBACK");
                    }

                    let answer = if req.cancelled.load(Ordering::Acquire)
                        || Instant::now() >= req.deadline
                    {
                        Err(Error::new(ErrorKind::Timeout, "query deadline exceeded"))
                    } else {
                        answer
                    };

                    if answer
                        .as_ref()
                        .is_err_and(|e| matches!(e.kind, ErrorKind::Integrity))
                    {
                        ready.store(false, Ordering::Release);
                    }

                    let _ = req.reply.send(answer);
                }

                watchdog_alive.store(false, Ordering::Release);

                drop(owner);
                ready.store(false, Ordering::Release);
            }));
        }

        Ok(Self {
            inner: Arc::new(Inner {
                writer,
                maintenance,
                readers,
                interrupts,
                next: AtomicU64::new(0),
                committed,
                ready,
                bytes: Arc::new(Semaphore::new(config.queue_bytes)),
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

    pub fn subscribe_commits(&self) -> watch::Receiver<u64> {
        self.inner.commits.subscribe()
    }

    /// Filesystem work: callers should run this in their bounded blocking pool.
    pub fn disk_usage(&self) -> Result<DiskUsage> {
        if !self.is_ready() {
            return Err(Error::unavailable("storage unavailable"));
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
            m.commit_ns.load(Ordering::Relaxed) as f64 / 1e9,
            m.commits.load(Ordering::Relaxed)
        );
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

    pub async fn tail_filtered(&self, after_id: Option<String>, q: Query) -> Result<SearchResult> {
        let after = after_id
            .map(|s| {
                s.parse::<i64>()
                    .map_err(|_| Error::invalid("invalid tail cursor"))
            })
            .transpose()?;

        match self.query(q, Operation::Tail(after, 0)).await? {
            Answer::Search(v) => Ok(v),
            _ => unreachable!(),
        }
    }

    pub async fn tail_filtered_bounded(
        &self,
        after_id: Option<String>,
        q: Query,
        max_bytes: usize,
    ) -> Result<SearchResult> {
        self.tail_filtered_bounded_with_overhead(after_id, q, max_bytes, 0)
            .await
    }

    pub async fn tail_filtered_bounded_with_overhead(
        &self,
        after_id: Option<String>,
        q: Query,
        max_bytes: usize,
        extra_frame_bytes: usize,
    ) -> Result<SearchResult> {
        if max_bytes == 0 {
            return Err(Error::invalid("tail byte budget must be positive"));
        }

        let after = after_id
            .map(|s| {
                s.parse::<i64>()
                    .map_err(|_| Error::invalid("invalid tail cursor"))
            })
            .transpose()?;

        match self
            .query_bounded(
                q,
                Operation::Tail(after, extra_frame_bytes),
                Some(max_bytes),
            )
            .await?
        {
            Answer::Search(value) => Ok(value),
            _ => unreachable!(),
        }
    }

    pub fn committed_events(&self) -> u64 {
        self.inner.committed.load(Ordering::Relaxed)
    }

    pub fn cancel_maintenance(&self) {
        self.inner.maintenance_cancel.store(true, Ordering::Release);
        (self.inner.maintenance_interrupt)();
    }

    pub fn stop_admission(&self) {
        self.cancel_maintenance();
        self.inner.ready.store(false, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.inner.ready.load(Ordering::Acquire)
    }

    pub async fn append(&self, source: String, events: Vec<NormalizedEvent>) -> Result<usize> {
        self.append_admitted(source, events, None).await
    }

    pub async fn append_with_permit(
        &self,
        source: String,
        events: Vec<NormalizedEvent>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<usize> {
        self.append_admitted(source, events, Some(permit)).await
    }

    async fn append_admitted(
        &self,
        source: String,
        events: Vec<NormalizedEvent>,
        request_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<usize> {
        if !self.is_ready() {
            return Err(Error::unavailable("storage unavailable"));
        }

        if self.inner.quota.blocked.load(Ordering::Acquire) {
            return Err(quota::size_error());
        }

        // Queue one serialized attributes value instead of retaining both the
        // JSON tree and the string consumed by the native appender.
        let events: Vec<_> = events
            .into_iter()
            .map(|mut event| {
                let attributes = event.attributes.to_string();
                event.attributes = serde_json::Value::Null;
                QueuedEvent { event, attributes }
            })
            .collect();
        let size = events
            .iter()
            .map(|e| {
                e.event.message.len()
                    + e.attributes.len()
                    + e.event.service.as_ref().map_or(0, String::len)
                    + e.event.logger.as_ref().map_or(0, String::len)
                    + e.event.host.as_ref().map_or(0, String::len)
                    + 256
            })
            .sum::<usize>();

        let permits = u32::try_from(size.max(1))
            .map_err(|_| Error::new(ErrorKind::TooLarge, "batch too large"))?;
        let permit = self
            .inner
            .bytes
            .clone()
            .try_acquire_many_owned(permits)
            .map_err(|_| Error::new(ErrorKind::Overloaded, "storage byte budget exhausted"))?;
        let (tx, rx) = oneshot::channel();
        self.inner
            .writer
            .try_send(Write::Append(source, events, tx, permit, request_permit))
            .map_err(queue_error)?;
        rx.await.map_err(db_error)?
    }

    async fn query(&self, query: Query, operation: Operation) -> Result<Answer> {
        self.query_bounded(query, operation, None).await
    }

    async fn query_bounded(
        &self,
        query: Query,
        operation: Operation,
        max_bytes: Option<usize>,
    ) -> Result<Answer> {
        if !self.is_ready() {
            return Err(Error::unavailable("storage unavailable"));
        }

        let id = self.inner.next.fetch_add(1, Ordering::Relaxed);
        let index = self
            .inner
            .readers
            .iter()
            .enumerate()
            .max_by_key(|(_, reader)| reader.capacity())
            .map(|(index, _)| index)
            .unwrap();
        let (tx, rx) = oneshot::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        self.inner.readers[index]
            .try_send(Read {
                id,
                query: Some(query),
                operation,
                reply: tx,
                cancelled: cancelled.clone(),
                deadline: Instant::now() + self.inner.timeout,
                max_bytes: max_bytes
                    .unwrap_or(self.inner.max_result_bytes)
                    .min(self.inner.max_result_bytes),
            })
            .map_err(queue_error)?;

        // Dropping the caller's future also cancels its queued or active query.
        let _cancellation = Cancellation {
            cancelled: cancelled.clone(),
            interrupt: {
                let interrupts = self.inner.interrupts.clone();
                Arc::new(move |id| {
                    for interrupt in &interrupts {
                        interrupt(id);
                    }
                })
            },
            id,
        };

        match tokio::time::timeout(self.inner.timeout, rx).await {
            Ok(reply) => {
                let answer = reply.map_err(db_error)??;
                let mut budget = ResultBudget {
                    remaining: self.inner.max_result_bytes,
                    deadline: None,
                    cancelled: None,
                };
                serde_json::to_writer(&mut budget, &answer).map_err(|_| {
                    Error::new(ErrorKind::TooLarge, "query response exceeds byte budget")
                })?;

                Ok(answer)
            }

            Err(_) => {
                self.inner.metrics.timeouts.fetch_add(1, Ordering::Relaxed);
                cancelled.store(true, Ordering::Release);

                for interrupt in &self.inner.interrupts {
                    interrupt(id);
                }

                Err(Error::new(
                    ErrorKind::Timeout,
                    "query deadline exceeded; reader remains occupied until completion",
                ))
            }
        }
    }

    pub async fn tail(
        &self,
        after_id: Option<String>,
        sources: Vec<String>,
        limit: usize,
    ) -> Result<SearchResult> {
        let after = after_id
            .map(|s| {
                s.parse::<i64>()
                    .map_err(|_| Error::invalid("invalid tail cursor"))
            })
            .transpose()?;
        let q = Query {
            from: i64::MIN,
            to: i64::MAX,
            sources,
            limit,
            ..Query::default()
        };

        match self.query(q, Operation::Tail(after, 0)).await? {
            Answer::Search(v) => Ok(v),
            _ => unreachable!(),
        }
    }

    pub async fn search(&self, q: Query) -> Result<SearchResult> {
        match self.query(q, Operation::Search).await? {
            Answer::Search(v) => Ok(v),
            _ => unreachable!(),
        }
    }

    pub async fn count(&self, q: Query) -> Result<u64> {
        match self.query(q, Operation::Count).await? {
            Answer::Count(v) => Ok(v),
            _ => unreachable!(),
        }
    }

    pub async fn facets(&self, q: Query) -> Result<Facets> {
        match self.query(q, Operation::Facets).await? {
            Answer::Facets(v) => Ok(v),
            _ => unreachable!(),
        }
    }

    pub async fn histogram(&self, q: Query, interval_ms: i64) -> Result<Vec<HistogramBucket>> {
        if interval_ms <= 0 {
            return Err(Error::invalid("interval must be positive"));
        }

        match self.query(q, Operation::Histogram(interval_ms)).await? {
            Answer::Histogram(v) => Ok(v),
            _ => unreachable!(),
        }
    }

    pub async fn archive(&self, before: i64) -> Result<usize> {
        if !self.is_ready() {
            return Err(Error::unavailable("storage unavailable"));
        }

        let (tx, rx) = oneshot::channel();
        self.inner
            .maintenance
            .try_send(Maintenance::Archive(before, tx))
            .map_err(queue_error)?;
        rx.await.map_err(db_error)?
    }

    pub async fn enforce_size_limit(&self) -> Result<usize> {
        if !self.is_ready() {
            return Err(Error::unavailable("storage unavailable"));
        }

        let (reply, answer) = oneshot::channel();
        self.inner
            .maintenance
            .try_send(Maintenance::EnforceSize(reply))
            .map_err(queue_error)?;
        answer.await.map_err(db_error)?
    }

    pub async fn compact(&self) -> Result<usize> {
        if !self.is_ready() {
            return Err(Error::unavailable("storage unavailable"));
        }

        let (tx, rx) = oneshot::channel();
        self.inner
            .maintenance
            .try_send(Maintenance::Compact(tx))
            .map_err(queue_error)?;
        rx.await.map_err(db_error)?
    }

    pub async fn retain(&self, before: i64) -> Result<usize> {
        if !self.is_ready() {
            return Err(Error::unavailable("storage unavailable"));
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
    pub async fn shutdown(&self) -> Result<()> {
        self.stop_admission();
        let mut shutdown = self.inner.shutdown.clone().lock_owned().await;
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
                .map_err(|error| Error::new(error.kind, error.message.clone()))
        })
        .await
        .map_err(|_| Error::unavailable("storage shutdown worker failed"))?
    }

    async fn shutdown_workers(&self) -> Result<()> {
        self.stop_admission();
        self.inner.maintenance_cancel.store(true, Ordering::Release);
        (self.inner.maintenance_interrupt)();

        for interrupt in &self.inner.interrupts {
            interrupt(u64::MAX);
        }

        let mut failure = None;
        let (tx, rx) = oneshot::channel();

        if self
            .inner
            .maintenance
            .send(Maintenance::Stop(tx))
            .await
            .is_ok()
        {
            if let Err(e) = rx.await {
                failure = Some(db_error(e));
            }
        } else {
            failure = Some(Error::unavailable("maintenance worker closed"));
        }

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

        // Reader stop replies are sent only after their last transaction has completed.
        for interrupt in &self.inner.interrupts {
            interrupt(u64::MAX);
        }

        let (tx, rx) = oneshot::channel();

        if self.inner.writer.send(Write::Stop(tx)).await.is_ok() {
            match rx.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => failure = Some(e),
                Err(e) => failure = Some(db_error(e)),
            }
        } else {
            failure = Some(Error::unavailable("writer closed"));
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
}

fn quote(s: &str) -> String {
    s.replace('\'', "''")
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
fn append(conn: &mut Connection, source: &str, events: &[NormalizedEvent]) -> Result<usize> {
    let queued: Vec<_> = events
        .iter()
        .cloned()
        .map(|event| QueuedEvent {
            attributes: event.attributes.to_string(),
            event,
        })
        .collect();
    append_queued(conn, source, &queued, false)
}

fn append_queued(
    conn: &mut Connection,
    source: &str,
    events: &[QueuedEvent],
    live: bool,
) -> Result<usize> {
    let tx = conn.transaction().map_err(db_error)?;

    if live {
        let cutoff: i64 = tx
            .query_row(
                "SELECT before_time FROM retention_state WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .map_err(db_error)?;

        if events.iter().any(|event| event.event.event_time < cutoff) {
            return Err(Error::invalid(
                "event timestamp expired by current retention",
            ));
        }
    }

    let next: i64 = tx
        .query_row(
            "SELECT next_id FROM ingest_state WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .map_err(db_error)?;

    // Flush and drop the appender before updating the sequence and committing;
    // acknowledgements and counters are published only after this succeeds.
    {
        let mut app = tx.appender("events").map_err(db_error)?;
        let received = now();

        for (offset, queued) in events.iter().enumerate() {
            let event = &queued.event;
            app.append_row(params![
                next + offset as i64,
                source,
                event.event_time,
                received,
                event.level,
                event.service,
                event.logger,
                event.host,
                event.pid,
                event.message,
                queued.attributes
            ])
            .map_err(db_error)?;
        }

        app.flush().map_err(db_error)?;
    }

    tx.execute(
        "UPDATE ingest_state SET next_id=? WHERE singleton=1",
        params![next + events.len() as i64],
    )
    .map_err(db_error)?;

    tx.commit().map_err(db_error)?;

    Ok(events.len())
}

#[cfg(test)]
mod tests;
