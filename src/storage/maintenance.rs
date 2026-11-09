//! Maintenance worker: archive export, compaction, retention and size enforcement, with deadlines.
use super::{
    Metrics, Result, WorkerHealth,
    archive::{
        self, ArchiveWork, CompactionRequest, ExportRequest, compact_export, expired_id,
        export_bounded,
    },
    db_error,
    interrupt::{ConnectionOwner, spawn_deadline_watchdog},
    leases::{ArchiveLeases, cleanup_archives},
    quota::{self, DiskBudget},
    storage_unavailable,
    writer::Write,
    writer_closed,
};
use crate::model::{Error, ErrorKind};
use duckdb::Connection;
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};

pub(super) struct Published {
    pub(super) archive_schema: i32,
    pub(super) projection_sha256: Option<String>,
    pub(super) path: String,
    pub(super) min: i64,
    pub(super) max: i64,
    pub(super) count: i64,
    pub(super) before: i64,
    pub(super) high: i64,
    pub(super) artifact: archive::ArtifactGuard,
    pub(super) reservation: Option<quota::Reservation>,
    pub(super) replaced: Vec<String>,
    pub(super) min_id: i64,
    pub(super) bytes: u64,
    pub(super) lower: i64,
}

pub(super) enum Maintenance {
    Archive(i64, oneshot::Sender<Result<usize>>),
    Compact(oneshot::Sender<Result<usize>>),
    EnforceSize(oneshot::Sender<Result<usize>>),
    Retain(i64, oneshot::Sender<Result<usize>>),
    Stop(oneshot::Sender<()>),
}

/// Poll interval while a maintenance caller waits for a full writer queue or reply.
const MAINTENANCE_POLL: Duration = Duration::from_millis(1);

#[derive(Clone)]
pub(super) struct MaintenanceBudget {
    pub(super) cancelled: Arc<AtomicBool>,
    pub(super) deadline: Instant,
}

impl MaintenanceBudget {
    pub(super) fn check(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Acquire) || Instant::now() >= self.deadline {
            Err(Error::new(
                ErrorKind::Timeout,
                "maintenance deadline exceeded",
            ))
        } else {
            Ok(())
        }
    }

    pub(super) fn error(&self, error: Error) -> Error {
        if error.kind == ErrorKind::Integrity {
            error
        } else {
            self.check().err().unwrap_or(error)
        }
    }
}

/// Only maintenance SQL can arm the writer interrupt. The shared mutex makes
/// disarming atomic with respect to watchdog and external cancellation calls.
pub(super) struct WriterScope {
    pub(super) budget: MaintenanceBudget,
    pub(super) active: Arc<Mutex<Option<MaintenanceBudget>>>,
}

impl WriterScope {
    pub(super) fn start(
        budget: MaintenanceBudget,
        active: &Arc<Mutex<Option<MaintenanceBudget>>>,
    ) -> Result<Self> {
        budget.check()?;
        *active.lock().unwrap() = Some(budget.clone());

        Ok(Self {
            budget,
            active: Arc::clone(active),
        })
    }

    pub(super) fn check(&self) -> Result<()> {
        let result = self.budget.check();
        if result.is_err() {
            self.disarm();
        }

        result
    }

    pub(super) fn error(&self, error: duckdb::Error) -> Error {
        self.disarm();
        self.budget.error(db_error(error))
    }

    pub(super) fn disarm(&self) {
        *self.active.lock().unwrap() = None;
    }

    pub(super) fn finish(&self) -> Result<()> {
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

pub(super) fn maintenance_outcome<T>(result: &Result<T>, metrics: &Metrics, ready: &AtomicBool) {
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

pub(super) fn maintenance_send(
    writer: &mpsc::Sender<Write>,
    mut command: Write,
    budget: &MaintenanceBudget,
) -> Result<()> {
    loop {
        budget.check()?;

        match writer.try_send(command) {
            Ok(()) => return Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(writer_closed());
            }

            Err(mpsc::error::TrySendError::Full(returned)) => command = returned,
        }

        thread::sleep(MAINTENANCE_POLL);
    }
}

pub(super) fn maintenance_reply<T>(
    mut receiver: oneshot::Receiver<Result<T>>,
    budget: &MaintenanceBudget,
) -> Result<T> {
    loop {
        match receiver.try_recv() {
            Ok(result) => return result,
            Err(oneshot::error::TryRecvError::Closed) => {
                return Err(writer_closed());
            }

            Err(oneshot::error::TryRecvError::Empty) => budget.check()?,
        }

        thread::sleep(MAINTENANCE_POLL);
    }
}

pub(super) fn maintenance_gate<'a>(
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

            Err(std::sync::TryLockError::WouldBlock) => thread::sleep(MAINTENANCE_POLL),
        }
    }
}

/// Starts the maintenance thread and the watchdog that interrupts its native work at the deadline.
pub(super) fn spawn_maintenance_side(
    connection: Connection,
    native_interrupt: Arc<duckdb::InterruptHandle>,
    maintainer: Maintainer,
    rx: mpsc::Receiver<Maintenance>,
    joins: &mut Vec<thread::JoinHandle<()>>,
) {
    joins.push(spawn_deadline_watchdog(
        Arc::clone(&maintainer.deadline),
        Arc::clone(&maintainer.cancel),
        native_interrupt,
        Arc::clone(&maintainer.watchdog_alive),
    ));
    joins.push(thread::spawn(move || maintainer.run(connection, rx)));
}

/// The shared state of the maintenance worker, apart from its own connection.
pub(super) struct Maintainer {
    pub(super) ready: Arc<AtomicBool>,
    pub(super) metrics: Arc<Metrics>,
    pub(super) gate: Arc<Mutex<ArchiveLeases>>,
    pub(super) disk: Arc<DiskBudget>,
    pub(super) cancel: Arc<AtomicBool>,
    pub(super) deadline: Arc<Mutex<Option<Instant>>>,
    pub(super) watchdog_alive: Arc<AtomicBool>,
    pub(super) archives: PathBuf,
    pub(super) publication: mpsc::Sender<Write>,
    pub(super) timeout: Duration,
    pub(super) export_rows: usize,
    pub(super) partition_ms: i64,
    pub(super) compact_files: usize,
    pub(super) compact_bytes: u64,
}

impl Maintainer {
    pub(super) fn run(self, connection: Connection, mut rx: mpsc::Receiver<Maintenance>) {
        let _health = WorkerHealth(Arc::clone(&self.ready));
        let mut owner = ConnectionOwner {
            connection: Some(connection),
            state: Arc::clone(&self.deadline),
            alive: Arc::clone(&self.watchdog_alive),
        };

        while let Some(command) = rx.blocking_recv() {
            if let Maintenance::Stop(reply) = command {
                rx.close();
                while let Some(queued) = rx.blocking_recv() {
                    match queued {
                        Maintenance::Archive(_, reply)
                        | Maintenance::Compact(reply)
                        | Maintenance::EnforceSize(reply)
                        | Maintenance::Retain(_, reply) => {
                            let _ = reply.send(Err(storage_unavailable()));
                        }
                        Maintenance::Stop(reply) => {
                            let _ = reply.send(());
                        }
                    }
                }

                self.watchdog_alive.store(false, Ordering::Release);
                let _ = reply.send(());
                break;
            }

            self.run_command(owner.connection.as_mut().unwrap(), command);
        }

        self.watchdog_alive.store(false, Ordering::Release);

        drop(owner);
    }

    /// Runs one maintenance command under its own deadline, with archive cleanup on both sides.
    fn run_command(&self, conn: &mut Connection, command: Maintenance) {
        let deadline = Instant::now() + self.timeout;
        let budget = MaintenanceBudget {
            cancelled: Arc::clone(&self.cancel),
            deadline,
        };
        *self.deadline.lock().unwrap() = Some(deadline);

        cleanup_archives(&self.gate, &self.archives);
        let _ = self.disk.audit();

        match command {
            Maintenance::Archive(before, reply) => self.run_archive(conn, before, &budget, reply),
            Maintenance::EnforceSize(reply) => self.run_enforce_size(conn, &budget, reply),
            Maintenance::Retain(before, reply) => self.run_retain(conn, before, &budget, reply),
            Maintenance::Compact(reply) => self.run_compact(conn, &budget, reply),
            Maintenance::Stop(_) => unreachable!("stop is handled by the maintenance loop"),
        }

        *self.deadline.lock().unwrap() = None;
        let _ = conn.execute_batch("ROLLBACK");

        cleanup_archives(&self.gate, &self.archives);
        let _ = self.disk.audit();
    }

    /// Exports events before `before` to archive files, then publishes them through the writer.
    fn run_archive(
        &self,
        conn: &mut Connection,
        before: i64,
        budget: &MaintenanceBudget,
        reply: oneshot::Sender<Result<usize>>,
    ) {
        let result = export_bounded(
            conn,
            ExportRequest {
                dir: &self.archives,
                before,
                batch_rows: self.export_rows,
                partition_ms: self.partition_ms,
                work: self.work(budget.deadline),
            },
        );
        finish_archive_publication(
            result,
            &self.publication,
            budget,
            &self.metrics,
            &self.ready,
            reply,
        );
    }

    /// Expires the oldest data until the index is back under its size target.
    fn run_enforce_size(
        &self,
        conn: &mut Connection,
        budget: &MaintenanceBudget,
        reply: oneshot::Sender<Result<usize>>,
    ) {
        let result = quota::enforce_size(
            conn,
            &self.archives,
            &self.disk,
            &self.publication,
            quota::SizeWork {
                gate: &self.gate,
                rows: self.export_rows,
                budget,
            },
        )
        .map_err(|error| budget.error(error));
        maintenance_outcome(&result, &self.metrics, &self.ready);
        let _ = reply.send(result);
    }

    /// Chooses the expired archive ids for `before`, then removes them from the hot store.
    fn run_retain(
        &self,
        conn: &mut Connection,
        before: i64,
        budget: &MaintenanceBudget,
        reply: oneshot::Sender<Result<usize>>,
    ) {
        let result = match expired_id(conn, &self.archives, before, &self.cancel, budget.deadline) {
            Ok(expired) => {
                let (tx, rx) = oneshot::channel();
                maintenance_send(
                    &self.publication,
                    Write::Retain(before, expired, budget.clone(), tx),
                    budget,
                )
                .and_then(|_| maintenance_reply(rx, budget))
            }
            Err(error) => Err(budget.error(error)),
        };
        maintenance_outcome(&result, &self.metrics, &self.ready);
        let _ = reply.send(result);
    }

    /// Merges small archive files, then publishes the merged file through the writer.
    fn run_compact(
        &self,
        conn: &mut Connection,
        budget: &MaintenanceBudget,
        reply: oneshot::Sender<Result<usize>>,
    ) {
        let result = compact_export(
            conn,
            CompactionRequest {
                dir: &self.archives,
                max_files: self.compact_files,
                max_bytes: self.compact_bytes,
                partition_ms: self.partition_ms,
                work: self.work(budget.deadline),
            },
        );
        finish_archive_publication(
            result,
            &self.publication,
            budget,
            &self.metrics,
            &self.ready,
            reply,
        );
    }

    fn work(&self, deadline: Instant) -> ArchiveWork<'_> {
        ArchiveWork {
            cancelled: &self.cancel,
            deadline,
            quota: self.disk.limit.map(|_| &self.disk),
        }
    }
}
