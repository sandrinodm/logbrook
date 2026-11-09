//! Native interruption: watchdogs, interrupt hooks, and the guards that stop a running query.
use super::{maintenance::MaintenanceBudget, readers::RunningQuery};
use duckdb::Connection;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

/// Poll interval for the watchdogs that interrupt overdue native work.
const WATCHDOG_POLL: Duration = Duration::from_millis(5);

// The crate disconnects before clearing InterruptHandle. Own destruction behind
// the same state mutex all interrupt paths hold, including unwinding on panic.
pub(super) struct ConnectionOwner<T> {
    pub(super) connection: Option<Connection>,
    pub(super) state: Arc<Mutex<Option<T>>>,
    pub(super) alive: Arc<AtomicBool>,
}

impl<T> Drop for ConnectionOwner<T> {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = None;

        drop(self.connection.take());
    }
}

pub(super) struct Cancellation {
    pub(super) cancelled: Arc<AtomicBool>,
    pub(super) interrupt: Arc<dyn Fn(u64) + Send + Sync>,
    pub(super) id: u64,
}

impl Drop for Cancellation {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        (self.interrupt)(self.id);
    }
}

/// Interrupts the writer while a maintenance budget it is running has expired or been cancelled.
pub(super) fn spawn_writer_watchdog(
    active: Arc<Mutex<Option<MaintenanceBudget>>>,
    alive: Arc<AtomicBool>,
    interrupt: Arc<duckdb::InterruptHandle>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while alive.load(Ordering::Acquire) {
            {
                let active = active.lock().unwrap();
                if active
                    .as_ref()
                    .is_some_and(|budget| budget.check().is_err())
                {
                    interrupt.interrupt();
                }
            }

            thread::sleep(WATCHDOG_POLL);
        }
    })
}

/// Interrupts the maintenance connection while its deadline has passed or it was cancelled.
pub(super) fn spawn_deadline_watchdog(
    deadline: Arc<Mutex<Option<Instant>>>,
    cancel: Arc<AtomicBool>,
    interrupt: Arc<duckdb::InterruptHandle>,
    alive: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while alive.load(Ordering::Acquire) {
            {
                let active = deadline.lock().unwrap();
                if active.is_some_and(|deadline| {
                    Instant::now() >= deadline || cancel.load(Ordering::Acquire)
                }) {
                    interrupt.interrupt();
                }
            }

            thread::sleep(WATCHDOG_POLL);
        }
    })
}

/// Cancels the native work of whichever maintenance or writer command is running.
pub(super) fn build_maintenance_interrupt(
    deadline: Arc<Mutex<Option<Instant>>>,
    native: Arc<duckdb::InterruptHandle>,
    writer_active: Arc<Mutex<Option<MaintenanceBudget>>>,
    writer: Arc<duckdb::InterruptHandle>,
) -> Arc<dyn Fn() + Send + Sync> {
    Arc::new(move || {
        let active = deadline.lock().unwrap();
        if active.is_some() {
            native.interrupt();
        }

        let active = writer_active.lock().unwrap();
        if active.is_some() {
            writer.interrupt();
        }
    })
}

/// Interrupts one reader when its running query matches `id`, or every query for `u64::MAX`.
pub(super) fn reader_interrupt(
    running: RunningQuery,
    interrupt: Arc<duckdb::InterruptHandle>,
) -> Arc<dyn Fn(u64) + Send + Sync> {
    Arc::new(move |id: u64| {
        let active = running.lock().unwrap();
        if active
            .as_ref()
            .is_some_and(|running| id == u64::MAX || running.id == id)
        {
            interrupt.interrupt();
        }
    })
}

/// Interrupts a reader whose query has passed its deadline or was cancelled.
pub(super) fn spawn_reader_watchdog(
    running: RunningQuery,
    alive: Arc<AtomicBool>,
    interrupt: Arc<duckdb::InterruptHandle>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while alive.load(Ordering::Acquire) {
            {
                let active = running.lock().unwrap();
                if let Some(running) = active.as_ref()
                    && (running.cancelled.load(Ordering::Acquire)
                        || Instant::now() >= running.deadline)
                {
                    interrupt.interrupt();
                }
            }

            thread::sleep(WATCHDOG_POLL);
        }
    })
}
