//! Reader workers: each owns one read-only connection and serves queries from a shared queue.

use super::{
    Metrics, ProjectionStatus, Result, Storage, WorkerHealth, db_error,
    interrupt::{Cancellation, ConnectionOwner, reader_interrupt, spawn_reader_watchdog},
    leases::ArchiveLeases,
    query::{QueryDeadline, ResultBudget, response_too_large, run_query},
    queue_error, storage_unavailable,
};
use crate::model::{Error, ErrorKind, Facets, HistogramBucket, Query, SearchResult};
use duckdb::Connection;
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Instant,
};
use tokio::sync::{mpsc, oneshot};

/// Six fixed layout/catalog statements plus retention and ingest high-water lookups.
const METADATA_STATEMENT_CACHE_CAPACITY: usize = 8;

/// Message for a query that outlived its deadline or was cancelled.
const QUERY_DEADLINE_MESSAGE: &str = "query deadline exceeded";

/// Interrupts a reader for the query with the given id, or for every query at `u64::MAX`.
pub(super) type ReaderInterrupt = Arc<dyn Fn(u64) + Send + Sync>;

/// The query a reader is running, as far as its watchdog and interrupt hook need to know.
pub(super) struct ActiveQuery {
    pub(super) id: u64,
    pub(super) deadline: Instant,
    pub(super) cancelled: Arc<AtomicBool>,
}

/// Shared handle to the query each reader is running, or `None` while it is idle.
pub(super) type RunningQuery = Arc<Mutex<Option<ActiveQuery>>>;

pub(super) enum Operation {
    ProjectionStatus,
    Search,
    Count,
    Facets,
    Histogram(i64),
    #[cfg(test)]
    ObserveNative(NativeCompletionProbe),
    Tail(Option<i64>),
    Stop,
}

/// Observes native cancellation and holds the reader at the native-completion boundary.
#[cfg(test)]
pub(super) struct NativeCompletionProbe {
    pub(super) started: oneshot::Sender<()>,
    pub(super) completed: oneshot::Sender<Option<String>>,
    pub(super) release: std::sync::mpsc::Receiver<()>,
}

#[derive(serde::Serialize)]
pub(super) enum Answer {
    ProjectionStatus(ProjectionStatus),
    Search(SearchResult),
    Count(u64),
    Facets(Facets),
    Histogram(Vec<HistogramBucket>),
}

pub(super) struct Read {
    pub(super) id: u64,
    pub(super) query: Option<Query>,
    pub(super) operation: Operation,
    pub(super) reply: oneshot::Sender<Result<Answer>>,
    pub(super) cancelled: Arc<AtomicBool>,
    pub(super) deadline: Instant,
    pub(super) max_bytes: usize,
}

pub(super) fn check_cancelled(cancelled: &AtomicBool, deadline: Instant) -> Result<()> {
    if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
        return Err(deadline_error());
    }

    Ok(())
}

/// State every reader worker shares, cloned once per worker.
#[derive(Clone)]
pub(super) struct ReaderShared {
    pub(super) ready: Arc<AtomicBool>,
    pub(super) metrics: Arc<Metrics>,
    pub(super) gate: Arc<Mutex<ArchiveLeases>>,
    pub(super) archives: PathBuf,
    pub(super) max_result_bytes: usize,
}

/// Starts the reader queue with one watchdog and one worker per connection. Returns a
/// sender for the queue per reader, and each reader's interrupt hook.
pub(super) fn spawn_reader_side(
    connections: Vec<Connection>,
    capacity: usize,
    shared: &ReaderShared,
    joins: &mut Vec<thread::JoinHandle<()>>,
) -> (Vec<mpsc::Sender<Read>>, Vec<ReaderInterrupt>) {
    // Readers compete for one shared queue so an idle connection can take
    // the next request while another connection is still running SQL.
    let (queue, queue_rx) = mpsc::channel::<Read>(capacity);
    let queue_rx = Arc::new(Mutex::new(queue_rx));
    let senders = connections.iter().map(|_| queue.clone()).collect();
    let interrupts = spawn_readers(connections, &queue_rx, shared, joins);
    (senders, interrupts)
}

/// Starts one watchdog and one worker per connection. The workers take requests from
/// `queue`, and their thread handles go into `joins`. Returns each reader's interrupt hook.
fn spawn_readers(
    connections: Vec<Connection>,
    queue: &Arc<Mutex<mpsc::Receiver<Read>>>,
    shared: &ReaderShared,
    joins: &mut Vec<thread::JoinHandle<()>>,
) -> Vec<Arc<dyn Fn(u64) + Send + Sync>> {
    let mut interrupts = Vec::with_capacity(connections.len());

    for connection in connections {
        // Only small metadata results enter this bounded, connection-local LRU.
        connection.set_prepared_statement_cache_capacity(METADATA_STATEMENT_CACHE_CAPACITY);

        let running: RunningQuery = Arc::new(Mutex::new(None));
        let watchdog_alive = Arc::new(AtomicBool::new(true));
        joins.push(spawn_reader_watchdog(
            Arc::clone(&running),
            Arc::clone(&watchdog_alive),
            connection.interrupt_handle(),
        ));
        interrupts.push(reader_interrupt(
            Arc::clone(&running),
            connection.interrupt_handle(),
        ));

        let worker = ReaderWorker {
            ready: Arc::clone(&shared.ready),
            metrics: Arc::clone(&shared.metrics),
            gate: Arc::clone(&shared.gate),
            archives: shared.archives.clone(),
            max_result_bytes: shared.max_result_bytes,
            running,
            watchdog_alive,
        };
        let shared_rx = Arc::clone(queue);
        joins.push(thread::spawn(move || {
            worker.run(connection, shared_rx);
        }));
    }

    interrupts
}

/// The shared state of one reader worker, apart from its own connection.
struct ReaderWorker {
    ready: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    gate: Arc<Mutex<ArchiveLeases>>,
    archives: PathBuf,
    max_result_bytes: usize,
    running: RunningQuery,
    watchdog_alive: Arc<AtomicBool>,
}

impl ReaderWorker {
    fn run(self, connection: Connection, shared_rx: Arc<Mutex<mpsc::Receiver<Read>>>) {
        let _health = WorkerHealth(Arc::clone(&self.ready));
        let mut owner = ConnectionOwner {
            connection: Some(connection),
            state: Arc::clone(&self.running),
            alive: Arc::clone(&self.watchdog_alive),
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
                let _ = req.reply.send(Err(deadline_error()));
                continue;
            }

            let Some(req) = self.claim(req) else {
                continue;
            };

            self.metrics.busy_readers.fetch_add(1, Ordering::Relaxed);
            let answer = run_query(
                owner.connection.as_mut().unwrap(),
                &self.archives,
                req.query.as_ref().unwrap(),
                req.operation,
                req.max_bytes.min(self.max_result_bytes),
                &self.gate,
                QueryDeadline {
                    cancelled: &req.cancelled,
                    at: req.deadline,
                },
            );

            // Clear interruption state only after native work returns;
            // a timed-out waiter does not make this connection available.
            *self.running.lock().unwrap() = None;
            self.metrics.busy_readers.fetch_sub(1, Ordering::Relaxed);

            if answer.is_err() {
                let connection = owner.connection.as_mut().unwrap();
                let _ = connection.execute_batch("ROLLBACK");
                // Discard interrupted/failed native state before admitting another request.
                connection.flush_prepared_statement_cache();
            }

            let answer = self.settle(&req.cancelled, req.deadline, answer);
            let _ = req.reply.send(answer);
        }

        drain_queue(&shared_rx);

        self.watchdog_alive.store(false, Ordering::Release);

        drop(owner);
        self.ready.store(false, Ordering::Release);
    }

    /// Makes `req` this worker's running query. A request cancelled while it waited is
    /// answered here, while the running slot is still locked, and is not returned.
    fn claim(&self, req: Read) -> Option<Read> {
        let mut active = self.running.lock().unwrap();
        if req.cancelled.load(Ordering::Acquire) {
            let _ = req.reply.send(Err(deadline_error()));
            return None;
        }

        *active = Some(ActiveQuery {
            id: req.id,
            deadline: req.deadline,
            cancelled: Arc::clone(&req.cancelled),
        });
        Some(req)
    }

    /// Turns a finished query into its answer. Work that ended after cancellation or the
    /// deadline is a timeout, and an integrity failure stops this reader pool.
    fn settle(
        &self,
        cancelled: &AtomicBool,
        deadline: Instant,
        answer: Result<Answer>,
    ) -> Result<Answer> {
        let answer = if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            Err(deadline_error())
        } else {
            answer
        };
        if answer
            .as_ref()
            .is_err_and(|e| matches!(e.kind, ErrorKind::Integrity))
        {
            self.ready.store(false, Ordering::Release);
        }

        answer
    }
}

/// Runs once the last worker has left the queue: each request still queued fails,
/// except a stop request, which is acknowledged.
fn drain_queue(shared_rx: &Arc<Mutex<mpsc::Receiver<Read>>>) {
    if Arc::strong_count(shared_rx) == 1 {
        let mut rx = shared_rx.lock().unwrap();
        rx.close();
        while let Ok(req) = rx.try_recv() {
            if matches!(req.operation, Operation::Stop) {
                let _ = req.reply.send(Ok(Answer::Count(0)));
            } else {
                let _ = req.reply.send(Err(storage_unavailable()));
            }
        }
    }
}

/// The error a query reports once its deadline has passed or it was cancelled.
fn deadline_error() -> Error {
    Error::new(ErrorKind::Timeout, QUERY_DEADLINE_MESSAGE)
}

impl Storage {
    pub(super) async fn query(&self, query: Query, operation: Operation) -> Result<Answer> {
        self.query_bounded(query, operation, None).await
    }

    pub(super) async fn query_bounded(
        &self,
        query: Query,
        operation: Operation,
        max_bytes: Option<usize>,
    ) -> Result<Answer> {
        if !self.is_ready() {
            return Err(storage_unavailable());
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
                cancelled: Arc::clone(&cancelled),
                deadline: Instant::now() + self.inner.timeout,
                max_bytes: max_bytes
                    .unwrap_or(self.inner.max_result_bytes)
                    .min(self.inner.max_result_bytes),
            })
            .map_err(queue_error)?;

        self.wait_for_query(id, cancelled, rx, self.inner.timeout)
            .await
    }

    /// Waits for an admitted query's result under the caller's deadline. Cancellation
    /// interrupts native work, but only the reader worker can release its connection.
    pub(super) async fn wait_for_query(
        &self,
        id: u64,
        cancelled: Arc<AtomicBool>,
        rx: oneshot::Receiver<Result<Answer>>,
        timeout: std::time::Duration,
    ) -> Result<Answer> {
        // Dropping the caller's future also cancels its queued or active query.
        let _cancellation = self.cancellation(id, &cancelled);

        match tokio::time::timeout(timeout, rx).await {
            Ok(reply) => {
                let answer = reply.map_err(db_error)??;
                check_response_size(&answer, self.inner.max_result_bytes)?;

                Ok(answer)
            }
            Err(_) => Err(self.abandon(id, &cancelled)),
        }
    }

    /// Cancels query `id` when the returned guard drops, so a caller that stops waiting
    /// also stops its queued or running query.
    fn cancellation(&self, id: u64, cancelled: &Arc<AtomicBool>) -> Cancellation {
        let interrupts = self.inner.interrupts.clone();
        Cancellation {
            cancelled: Arc::clone(cancelled),
            interrupt: Arc::new(move |id| {
                for interrupt in &interrupts {
                    interrupt(id);
                }
            }),
            id,
        }
    }

    /// Records a timeout for query `id` and interrupts its reader. The reader stays
    /// occupied until the native call returns.
    fn abandon(&self, id: u64, cancelled: &AtomicBool) -> Error {
        self.inner.metrics.timeouts.fetch_add(1, Ordering::Relaxed);
        cancelled.store(true, Ordering::Release);

        for interrupt in &self.inner.interrupts {
            interrupt(id);
        }

        Error::new(
            ErrorKind::Timeout,
            "query deadline exceeded; reader remains occupied until completion",
        )
    }
}

/// Serializes `answer` into a budget of `max_bytes`, so an oversized response fails
/// before it is sent.
fn check_response_size(answer: &Answer, max_bytes: usize) -> Result<()> {
    let mut budget = ResultBudget {
        remaining: max_bytes,
        deadline: None,
        cancelled: None,
    };
    serde_json::to_writer(&mut budget, answer)
        .map_err(|error| response_too_large().with_source(error))?;

    Ok(())
}
