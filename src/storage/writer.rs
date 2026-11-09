//! Writer thread: adjacent appends share one transaction, maintenance runs as barriers.

use super::{
    IngestionPolicy, Metrics, Result, WorkerHealth,
    admission::{ForwardPermits, QueuedEvent},
    archive, db_error,
    interrupt::{ConnectionOwner, spawn_writer_watchdog},
    leases::ArchiveLeases,
    maintenance::{MaintenanceBudget, Published, WriterScope, maintenance_gate},
    projection,
    quota::DiskBudget,
    widen, writer_closed,
};
#[cfg(test)]
use crate::model::NormalizedEvent;
use crate::model::{Error, ErrorKind};
use duckdb::{Connection, params};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot, watch};

#[cfg(test)]
pub(super) type WriterTest =
    Box<dyn FnOnce(&mut Connection, &Arc<Mutex<Option<MaintenanceBudget>>>) + Send>;

/// Largest number of appends one writer transaction may group.
const GROUP_MAX_REQUESTS: usize = 32;

/// Largest number of events one writer transaction may group.
const GROUP_MAX_EVENTS: usize = 10_000;

/// Largest serialized size one writer transaction may group.
const GROUP_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Eleven original columns, one coverage flag, and at most eight projected slots.
const PROJECTED_ROW_CAPACITY: usize = 20;

/// Number of original (non-projected) columns in an events row.
const ORIGINAL_COLUMNS: usize = 11;

/// The retention cutoff recorded by the last retention pass.
pub(super) const RETENTION_CUTOFF_SQL: &str =
    "SELECT before_time FROM retention_state WHERE singleton=1";

/// The next event id to assign, read inside the commit transaction.
const NEXT_ID_SQL: &str = "SELECT next_id FROM ingest_state WHERE singleton=1";

/// Live events older than the retention cutoff are rejected with this message.
const EXPIRED_TIMESTAMP_MESSAGE: &str = "event timestamp expired by current retention";

pub(super) struct AppendRequest {
    pub(super) source: String,
    pub(super) events: Vec<QueuedEvent>,
    pub(super) reply: oneshot::Sender<Result<usize>>,
    pub(super) _permit: OwnedSemaphorePermit,
    pub(super) policy: IngestionPolicy,
    pub(super) _forward_permits: Option<ForwardPermits>,
    pub(super) bytes: usize,
}

impl AppendRequest {
    pub(super) fn finish(self, result: Result<usize>) {
        let Self {
            source,
            events,
            reply,
            _permit,
            _forward_permits,
            ..
        } = self;
        drop(events);
        drop(source);
        drop(_permit);
        drop(_forward_permits);
        let _ = reply.send(result);
    }

    pub(super) fn rows(&self) -> GroupRows<'_> {
        GroupRows {
            source: &self.source,
            events: self.events.as_slice(),
            policy: self.policy,
        }
    }
}

/// One request's rows, borrowed for a single appender transaction.
#[derive(Clone, Copy)]
pub(super) struct GroupRows<'a> {
    source: &'a str,
    events: &'a [QueuedEvent],
    policy: IngestionPolicy,
}

/// Adjacent append requests that share one writer transaction.
struct AppendGroup {
    requests: Vec<AppendRequest>,
    events: usize,
    bytes: usize,
}

impl AppendGroup {
    fn start(first: AppendRequest) -> Self {
        Self {
            events: first.events.len(),
            bytes: first.bytes,
            requests: vec![first],
        }
    }

    fn is_full(&self) -> bool {
        self.requests.len() >= GROUP_MAX_REQUESTS
            || self.events >= GROUP_MAX_EVENTS
            || self.bytes >= GROUP_MAX_BYTES
    }

    fn accepts(&self, request: &AppendRequest) -> bool {
        self.events.saturating_add(request.events.len()) <= GROUP_MAX_EVENTS
            && self.bytes.saturating_add(request.bytes) <= GROUP_MAX_BYTES
    }

    fn push(&mut self, request: AppendRequest) {
        self.events += request.events.len();
        self.bytes += request.bytes;
        self.requests.push(request);
    }
}

/// Collects the append requests that follow `first` while the group has room.
/// The first command that is not an admissible append stays queued as `pending`,
/// so maintenance and stop commands keep their place in the writer order.
fn collect_group(
    first: AppendRequest,
    rx: &mut mpsc::Receiver<Write>,
    pending: &mut Option<Write>,
) -> AppendGroup {
    let mut group = AppendGroup::start(first);
    while !group.is_full() {
        match rx.try_recv() {
            Ok(Write::Append(request)) if group.accepts(&request) => group.push(request),
            Ok(command) => {
                *pending = Some(command);
                break;
            }
            Err(_) => break,
        }
    }

    group
}

/// Writer-thread state: the commit path, its counters, and the maintenance barriers.
pub(super) struct Writer {
    pub(super) ready: Arc<AtomicBool>,
    pub(super) ingested: Arc<AtomicU64>,
    pub(super) committed: Arc<AtomicU64>,
    pub(super) commits: watch::Sender<u64>,
    pub(super) metrics: Arc<Metrics>,
    pub(super) disk: Arc<DiskBudget>,
    pub(super) gate: Arc<Mutex<ArchiveLeases>>,
    pub(super) active: Arc<Mutex<Option<MaintenanceBudget>>>,
    pub(super) projection: Option<projection::ProjectionLayout>,
    pub(super) retention_rows: usize,
}

impl Writer {
    /// Commits one group in one transaction and answers every request in it.
    fn commit_group(&self, conn: &mut Connection, group: AppendGroup) {
        let cutoff = retention_cutoff(conn, &group.requests);
        let requests = reject_expired(group.requests, &cutoff);
        if requests.is_empty() {
            return;
        }

        let (events, bytes) = group_totals(&requests);
        let started = Instant::now();
        let mut result = self.write_requests(conn, &requests, cutoff.map(|_| ()));
        if result
            .as_ref()
            .is_err_and(|error| error.kind != ErrorKind::SizeLimit)
        {
            let _ = conn.execute_batch("ROLLBACK");

            if conn
                .query_row(NEXT_ID_SQL, [], |r| r.get::<_, i64>(0))
                .is_err()
            {
                result = Err(Error::new(
                    ErrorKind::Integrity,
                    "writer connection recovery failed",
                ));
            }
        }

        self.metrics
            .record_group(started.elapsed(), requests.len(), events, bytes);
        self.publish_commit(&result);
        finish_requests(requests, &result);
    }

    /// Runs the admission outcome and then the appender transaction for `requests`.
    fn write_requests(
        &self,
        conn: &mut Connection,
        requests: &[AppendRequest],
        admitted: Result<()>,
    ) -> Result<usize> {
        let groups: Vec<GroupRows<'_>> = requests.iter().map(AppendRequest::rows).collect();
        admitted
            .and_then(|_| self.disk.check_write())
            .and_then(|_| append_groups_with_layout(conn, &groups, self.projection.as_ref()))
    }

    /// Publishes a successful commit to the counters and commit watchers, then
    /// stops accepting work if the engine reported corruption.
    fn publish_commit(&self, result: &Result<usize>) {
        if let Ok(count) = result {
            let count = widen(*count);
            self.ingested.fetch_add(count, Ordering::Relaxed);
            let _ = self.disk.refresh_native();
            let total = self.committed.fetch_add(count, Ordering::Relaxed) + count;
            self.commits.send_replace(total);
        }

        self.disable_on_integrity(result);
    }

    fn disable_on_integrity<T>(&self, result: &Result<T>) {
        if result
            .as_ref()
            .is_err_and(|error| error.kind == ErrorKind::Integrity)
        {
            self.ready.store(false, Ordering::Release);
        }
    }

    fn publish(
        &self,
        conn: &mut Connection,
        file: Published,
        budget: MaintenanceBudget,
        reply: oneshot::Sender<Result<usize>>,
    ) {
        // The bounded maintenance waiter may already have returned.
        // Retain cleanup ownership until after releasing the gate.
        let _cleanup = file.artifact.clone();
        let _reservation = file.reservation.clone();
        let replaced = file.replaced.clone();
        let file_bytes = file.bytes;
        let result = maintenance_gate(&self.gate, &budget).and_then(|mut lease| {
            let scope = WriterScope::start(budget, &self.active)?;
            let result = archive::publish_bounded(conn, file, &scope);
            drop(scope);

            if result.is_ok() {
                lease.retire(replaced);
                self.disk.published(file_bytes);
            }

            result
        });
        self.disable_on_integrity(&result);
        let _ = reply.send(result);
    }

    fn retain(
        &self,
        conn: &mut Connection,
        before: i64,
        expired: i64,
        budget: MaintenanceBudget,
        reply: oneshot::Sender<Result<usize>>,
    ) {
        let result = maintenance_gate(&self.gate, &budget).and_then(|mut lease| {
            let scope = WriterScope::start(budget, &self.active)?;
            archive::retain_bounded(
                conn,
                before,
                expired,
                &mut lease,
                self.retention_rows,
                &scope,
            )
        });
        if result.is_ok() {
            self.metrics
                .retention_before_ms
                .fetch_max(before, Ordering::Release);
            self.commits
                .send_replace(self.committed.load(Ordering::Relaxed));
        }

        self.disable_on_integrity(&result);
        let _ = reply.send(result);
    }

    fn checkpoint(
        &self,
        conn: &mut Connection,
        budget: MaintenanceBudget,
        reply: oneshot::Sender<Result<()>>,
    ) {
        let result = WriterScope::start(budget, &self.active).and_then(|scope| {
            conn.execute_batch("CHECKPOINT")
                .map_err(|error| scope.error(error))?;
            scope.finish()
        });

        let _ = self.disk.refresh_native();
        self.disable_on_integrity(&result);
        let _ = reply.send(result);
    }

    /// Stop admits nothing new. Appends already queued are committed one by one,
    /// other queued commands receive a closed-writer error, and the connection is
    /// checkpointed.
    fn drain_and_stop(
        &self,
        conn: &mut Connection,
        rx: &mut mpsc::Receiver<Write>,
        reply: oneshot::Sender<Result<()>>,
    ) {
        rx.close();
        while let Some(command) = rx.blocking_recv() {
            match command {
                #[cfg(test)]
                Write::Test(task) => task(conn, &self.active),
                Write::Append(request) => self.commit_group(conn, AppendGroup::start(request)),
                Write::Publish(_, _, reply) | Write::Retain(_, _, _, reply) => {
                    let _ = reply.send(Err(writer_closed()));
                }
                Write::Checkpoint(_, reply) | Write::Stop(reply) => {
                    let _ = reply.send(Err(writer_closed()));
                }
            }
        }

        let result = conn.execute_batch("CHECKPOINT").map_err(db_error);
        self.disable_on_integrity(&result);
        let _ = reply.send(result);
    }
}

/// Answers each request with the group's result. Every request keeps its own
/// count on success, and a failure is cloned to each request in the group.
fn finish_requests(requests: impl IntoIterator<Item = AppendRequest>, result: &Result<usize>) {
    for request in requests {
        let answer = match result {
            Ok(_) => Ok(request.events.len()),
            Err(error) => Err(error.clone()),
        };
        request.finish(answer);
    }
}

/// The live retention cutoff for a group, read once. A failed lookup is kept and
/// fails the group's commit, while groups without live requests skip the lookup.
fn retention_cutoff(conn: &Connection, requests: &[AppendRequest]) -> Result<i64> {
    if requests
        .iter()
        .any(|request| request.policy == IngestionPolicy::Live)
    {
        conn.query_row(RETENTION_CUTOFF_SQL, [], |row| row.get::<_, i64>(0))
            .map_err(db_error)
    } else {
        Ok(i64::MIN)
    }
}

/// Fails each live request holding an event older than the cutoff. Other
/// requests in the same group keep going, so one stale batch cannot poison them.
fn reject_expired(requests: Vec<AppendRequest>, cutoff: &Result<i64>) -> Vec<AppendRequest> {
    requests
        .into_iter()
        .filter_map(|request| {
            if cutoff.as_ref().is_ok_and(|cutoff| {
                request.policy == IngestionPolicy::Live
                    && request
                        .events
                        .iter()
                        .any(|event| event.event.event_time < *cutoff)
            }) {
                request.finish(Err(expired_timestamp()));
                None
            } else {
                Some(request)
            }
        })
        .collect()
}

fn group_totals(requests: &[AppendRequest]) -> (usize, usize) {
    requests.iter().fold((0, 0), |(events, bytes), request| {
        (events + request.events.len(), bytes + request.bytes)
    })
}

fn expired_timestamp() -> Error {
    Error::invalid(EXPIRED_TIMESTAMP_MESSAGE)
}

pub(super) enum Write {
    #[cfg(test)]
    Test(WriterTest),
    Append(AppendRequest),
    Publish(Published, MaintenanceBudget, oneshot::Sender<Result<usize>>),
    Retain(i64, i64, MaintenanceBudget, oneshot::Sender<Result<usize>>),
    Checkpoint(MaintenanceBudget, oneshot::Sender<Result<()>>),
    Stop(oneshot::Sender<Result<()>>),
}

/// Starts the writer thread and the watchdog that interrupts its maintenance barriers.
pub(super) fn spawn_writer_side(
    conn: Connection,
    rx: mpsc::Receiver<Write>,
    writer: Writer,
    interrupt: Arc<duckdb::InterruptHandle>,
    joins: &mut Vec<thread::JoinHandle<()>>,
) {
    let alive = Arc::new(AtomicBool::new(true));
    let active = Arc::clone(&writer.active);
    joins.push(spawn_writer(conn, rx, writer, Arc::clone(&alive)));
    joins.push(spawn_writer_watchdog(active, alive, interrupt));
}

/// Runs the writer thread, which owns the primary connection and every append and
/// write-side maintenance command.
pub(super) fn spawn_writer(
    conn: Connection,
    mut rx: mpsc::Receiver<Write>,
    writer: Writer,
    alive: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let _health = WorkerHealth(Arc::clone(&writer.ready));
        let mut owner = ConnectionOwner {
            connection: Some(conn),
            state: Arc::clone(&writer.active),
            alive,
        };

        let mut pending = None;
        while let Some(cmd) = pending.take().or_else(|| rx.blocking_recv()) {
            let conn = owner.connection.as_mut().unwrap();
            match cmd {
                #[cfg(test)]
                Write::Test(task) => task(conn, &writer.active),
                Write::Append(first) => {
                    // Only adjacent appends are grouped. Maintenance and stop
                    // commands remain barriers in the original writer order.
                    let group = collect_group(first, &mut rx, &mut pending);
                    writer.commit_group(conn, group);
                }
                Write::Publish(file, budget, reply) => writer.publish(conn, file, budget, reply),
                Write::Retain(before, expired, budget, reply) => {
                    writer.retain(conn, before, expired, budget, reply);
                }
                Write::Checkpoint(budget, reply) => writer.checkpoint(conn, budget, reply),
                Write::Stop(reply) => {
                    writer.drain_and_stop(conn, &mut rx, reply);
                    break;
                }
            }
        }

        writer.ready.store(false, Ordering::Release);
    })
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
pub(super) fn append(
    conn: &mut Connection,
    source: &str,
    events: &[NormalizedEvent],
) -> Result<usize> {
    let layout = projection::ProjectionLayout::load(conn)?;
    let queued: Vec<_> = events
        .iter()
        .cloned()
        .map(|mut event| {
            let attributes = event.attributes.to_string();
            let projected = layout
                .as_ref()
                .map_or_else(Vec::new, |layout| layout.take_values(&mut event.attributes));
            QueuedEvent {
                attributes,
                event,
                projected,
            }
        })
        .collect();
    append_queued(conn, source, &queued, IngestionPolicy::Import)
}

#[cfg(test)]
fn append_queued(
    conn: &mut Connection,
    source: &str,
    events: &[QueuedEvent],
    policy: IngestionPolicy,
) -> Result<usize> {
    append_groups(
        conn,
        &[GroupRows {
            source,
            events,
            policy,
        }],
    )
}

#[cfg(test)]
fn append_groups(conn: &mut Connection, groups: &[GroupRows<'_>]) -> Result<usize> {
    let layout = projection::ProjectionLayout::load(conn)?;
    append_groups_with_layout(conn, groups, layout.as_ref())
}

fn append_groups_with_layout(
    conn: &mut Connection,
    groups: &[GroupRows<'_>],
    layout: Option<&projection::ProjectionLayout>,
) -> Result<usize> {
    let tx = conn.transaction().map_err(db_error)?;
    check_retention(&tx, groups)?;

    let next: i64 = tx
        .query_row(NEXT_ID_SQL, [], |r| r.get(0))
        .map_err(db_error)?;
    let count = groups.iter().try_fold(0usize, |count, group| {
        count
            .checked_add(group.events.len())
            .ok_or_else(|| Error::invalid("event count overflow"))
    })?;
    let end = i64::try_from(count)
        .ok()
        .and_then(|count| next.checked_add(count))
        .ok_or_else(|| Error::new(ErrorKind::Integrity, "event sequence exhausted"))?;

    append_rows(&tx, groups, next, layout)?;

    tx.execute(
        "UPDATE ingest_state SET next_id=? WHERE singleton=1",
        params![end],
    )
    .map_err(db_error)?;
    tx.commit().map_err(db_error)?;

    Ok(count)
}

/// Fails the whole transaction when any live event is older than the retention cutoff.
fn check_retention(tx: &duckdb::Transaction<'_>, groups: &[GroupRows<'_>]) -> Result<()> {
    if !groups
        .iter()
        .any(|group| group.policy == IngestionPolicy::Live)
    {
        return Ok(());
    }

    let cutoff: i64 = tx
        .query_row(RETENTION_CUTOFF_SQL, [], |r| r.get(0))
        .map_err(db_error)?;
    if groups.iter().any(|group| {
        group.policy == IngestionPolicy::Live
            && group
                .events
                .iter()
                .any(|event| event.event.event_time < cutoff)
    }) {
        return Err(expired_timestamp());
    }

    Ok(())
}

/// Appends every event row through one appender, numbering them from `next`.
fn append_rows(
    tx: &duckdb::Transaction<'_>,
    groups: &[GroupRows<'_>],
    next: i64,
    layout: Option<&projection::ProjectionLayout>,
) -> Result<()> {
    // Every original request keeps its source and remains atomic. Commit receipts
    // are published together only after appender flush and transaction commit.
    let mut app = tx.appender("events").map_err(db_error)?;
    let received = now();
    let mut id = next;

    for group in groups {
        for queued in group.events {
            let event = &queued.event;
            let original = params![
                id,
                group.source,
                event.event_time,
                received,
                event.level,
                event.service,
                event.logger,
                event.host,
                event.pid,
                event.message,
                queued.attributes
            ];
            append_event_row(&mut app, original, queued, layout)?;
            id += 1;
        }
    }

    app.flush().map_err(db_error)
}

/// Appends one event. Its projected values follow the original columns when the
/// index has a projection layout.
fn append_event_row(
    app: &mut duckdb::Appender<'_>,
    original: &[&dyn duckdb::ToSql],
    queued: &QueuedEvent,
    layout: Option<&projection::ProjectionLayout>,
) -> Result<()> {
    let Some(layout) = layout else {
        return app.append_row(original).map_err(db_error);
    };
    if queued.projected.len() != layout.keys().len() {
        return Err(Error::new(
            ErrorKind::Integrity,
            "queued projection layout mismatch",
        ));
    }

    let covered = true;
    // References keep the per-row appender representation on the stack.
    let mut row: [&dyn duckdb::ToSql; PROJECTED_ROW_CAPACITY] = [&covered; PROJECTED_ROW_CAPACITY];
    row[..ORIGINAL_COLUMNS].copy_from_slice(original);
    row[ORIGINAL_COLUMNS] = &covered;

    for (slot, value) in queued.projected.iter().enumerate() {
        row[ORIGINAL_COLUMNS + 1 + slot] = value;
    }

    app.append_row(&row[..ORIGINAL_COLUMNS + 1 + queued.projected.len()])
        .map_err(db_error)
}
