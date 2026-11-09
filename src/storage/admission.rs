//! Append admission: CPU preparation permits, byte budgets, and the queued representation.
use super::{
    INGRESS_ADMISSION_LOCK, IngestionPolicy, Result, STORAGE_ADMISSION_LOCK, Storage, db_error,
    queue_error, quota, storage_unavailable, widen,
    writer::{AppendRequest, Write},
};
use crate::model::{Error, ErrorKind, NormalizedEvent};
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

/// One registry-wide boundary for admission and bounded serialization work.
pub(crate) struct IngressAdmission {
    pub(super) accepting: Mutex<bool>,
    preparations: Arc<Semaphore>,
    pub(super) forward_preparations: Arc<Semaphore>,
}

impl IngressAdmission {
    pub(crate) fn new() -> Self {
        Self {
            accepting: Mutex::new(true),
            preparations: Arc::new(Semaphore::new(PREPARATION_SLOTS)),
            forward_preparations: Arc::new(Semaphore::new(FORWARD_PREPARATION_SLOTS)),
        }
    }

    pub(crate) fn stop(&self) {
        *self.accepting.lock().expect(INGRESS_ADMISSION_LOCK) = false;
    }
}

fn batch_too_large() -> Error {
    Error::new(ErrorKind::TooLarge, "batch too large")
}

struct PreparationCancellation(Arc<AtomicBool>);

impl Drop for PreparationCancellation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

pub(super) struct QueuedEvent {
    pub(super) event: NormalizedEvent,
    pub(super) attributes: String,
    pub(super) projected: Vec<Option<String>>,
}

/// Concurrent CPU preparations for all ingestion.
const PREPARATION_SLOTS: usize = 4;

/// Concurrent CPU preparations reserved for Forward ingestion.
const FORWARD_PREPARATION_SLOTS: usize = 3;

/// Bookkeeping bytes charged per queued event on top of its fields.
const QUEUED_EVENT_OVERHEAD_BYTES: usize = 256;

/// Which admission budget a live append draws from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AppendLane {
    /// Direct producers such as the HTTP ingest route.
    Direct,
    /// Forward receiver requests, which also reserve the Forward budget.
    Forward,
}

impl AppendLane {
    pub(super) fn is_forward(self) -> bool {
        self == AppendLane::Forward
    }
}

#[derive(Clone, Copy)]
pub(super) struct AppendOptions {
    pub(super) policy: IngestionPolicy,
    pub(super) lane: AppendLane,
    pub(super) validation: Option<crate::ingestion::LiveValidation>,
    pub(super) preparation_wait: Option<Duration>,
}

/// CPU admission held from acquisition until the blocking preparation job exits.
struct PreparationPermits {
    _preparation: OwnedSemaphorePermit,
    _forward: Option<OwnedSemaphorePermit>,
}

/// The Forward slot and byte reservations, released together when the request finishes.
pub(super) struct ForwardPermits {
    _slot: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

/// Forward producers may hold at most half of the writer byte budget, so one
/// index cannot starve the others.
pub(super) fn forward_byte_share(queue_bytes: usize) -> usize {
    queue_bytes / 2
}

impl Storage {
    pub(super) async fn append_admitted(
        &self,
        source: String,
        events: Vec<NormalizedEvent>,
        request_permit: Option<OwnedSemaphorePermit>,
        options: AppendOptions,
    ) -> Result<usize> {
        if !self.is_ready() {
            return Err(storage_unavailable());
        }

        let admission_timer = options.lane.is_forward().then(|| {
            crate::forward::metrics::Timer::new(crate::forward::metrics::Stage::Admission)
        });
        // Acquire before spawn_blocking: Tokio's blocking queue is not a bound.
        // The job owns both decoded input and output reservations until it exits.
        let permits = self.acquire_preparation(options).await?;
        let storage = self.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancellation = PreparationCancellation(Arc::clone(&cancelled));
        let receipt = tokio::task::spawn_blocking(move || {
            let _preparation = permits;
            storage.prepare_append(source, events, request_permit, options, &cancelled)
        })
        .await
        .map_err(|error| {
            Error::unavailable("ingestion preparation worker failed").with_source(error)
        })??;
        drop(_cancellation);
        drop(admission_timer);

        let _commit_timer = options
            .lane
            .is_forward()
            .then(|| crate::forward::metrics::Timer::new(crate::forward::metrics::Stage::Commit));
        receipt.await.map_err(db_error)?
    }

    /// Reserves CPU preparation capacity. A caller with a deadline waits for it,
    /// and any other caller fails at once when the capacity is taken.
    async fn acquire_preparation(&self, options: AppendOptions) -> Result<PreparationPermits> {
        let lane = options.lane;
        match options.preparation_wait {
            Some(wait) => self.wait_for_preparation(lane, wait).await,
            None => self.try_preparation(lane),
        }
    }

    async fn wait_for_preparation(
        &self,
        lane: AppendLane,
        wait: Duration,
    ) -> Result<PreparationPermits> {
        // The caller still owns bounded decoded-memory credit while queued.
        // Tokio semaphores serve whole CPU reservations in FIFO order.
        tokio::time::timeout(wait, async {
            let forward_permit = if lane.is_forward() {
                Some(
                    Arc::clone(&self.inner.admission.forward_preparations)
                        .acquire_owned()
                        .await
                        .map_err(|error| {
                            Error::unavailable("Forward preparation admission closed")
                                .with_source(error)
                        })?,
                )
            } else {
                None
            };

            let preparation = Arc::clone(&self.inner.admission.preparations)
                .acquire_owned()
                .await
                .map_err(|error| {
                    Error::unavailable("ingestion preparation admission closed").with_source(error)
                })?;
            Ok::<_, Error>(PreparationPermits {
                _preparation: preparation,
                _forward: forward_permit,
            })
        })
        .await
        .map_err(|error| {
            Error::new(
                ErrorKind::Timeout,
                "ingestion preparation admission deadline exceeded",
            )
            .with_source(error)
        })?
    }

    fn try_preparation(&self, lane: AppendLane) -> Result<PreparationPermits> {
        let forward_permit = if lane.is_forward() {
            Some(
                Arc::clone(&self.inner.admission.forward_preparations)
                    .try_acquire_owned()
                    .map_err(|error| {
                        Error::new(
                            ErrorKind::Overloaded,
                            "Forward preparation capacity exhausted",
                        )
                        .with_source(error)
                    })?,
            )
        } else {
            None
        };

        let preparation = Arc::clone(&self.inner.admission.preparations)
            .try_acquire_owned()
            .map_err(|error| {
                Error::new(
                    ErrorKind::Overloaded,
                    "ingestion preparation capacity exhausted",
                )
                .with_source(error)
            })?;
        Ok(PreparationPermits {
            _preparation: preparation,
            _forward: forward_permit,
        })
    }

    fn prepare_append(
        &self,
        source: String,
        events: Vec<NormalizedEvent>,
        request_permit: Option<OwnedSemaphorePermit>,
        options: AppendOptions,
        cancelled: &AtomicBool,
    ) -> Result<oneshot::Receiver<Result<usize>>> {
        if cancelled.load(Ordering::Acquire) || !self.is_ready() {
            return Err(storage_unavailable());
        }

        if self.inner.quota.blocked.load(Ordering::Acquire) {
            return Err(quota::size_error());
        }

        if let Some(validation) = options.validation {
            validation.validate(&events)?;
        }

        // Count without allocation. Keep the decoded reservation alive while
        // reserving the queued representation, including JSON escaping growth.
        let lengths = events
            .iter()
            .map(|event| serialized_length(&event.attributes))
            .collect::<Result<Vec<_>>>()?;
        let size = self.queued_size(source.capacity(), &events, &lengths)?;
        let forward = options.lane.is_forward();
        self.check_size_limits(size, options.lane)?;

        let permits =
            u32::try_from(size.max(1)).map_err(|error| batch_too_large().with_source(error))?;
        let forward_permits = if forward {
            Some(self.reserve_forward(permits)?)
        } else {
            None
        };
        let permit = Arc::clone(&self.inner.bytes)
            .try_acquire_many_owned(permits)
            .map_err(|error| {
                Error::new(ErrorKind::Overloaded, "storage byte budget exhausted")
                    .with_source(error)
            })?;
        let events = self.queue_events(events, lengths)?;

        // The queued reservation now owns the remaining event strings and output.
        drop(request_permit);
        let event_count = events.len();
        let (tx, rx) = oneshot::channel();
        {
            // This short enqueue boundary is shared by every registry destination.
            // A lookup or serialization reservation is never admission by itself.
            let admission = self
                .inner
                .admission
                .accepting
                .lock()
                .expect(INGRESS_ADMISSION_LOCK);
            let accepting = self.inner.accepting.lock().expect(STORAGE_ADMISSION_LOCK);
            let admission_closed = !*admission || !*accepting;
            if cancelled.load(Ordering::Acquire)
                || admission_closed
                || !self.inner.ready.load(Ordering::Acquire)
            {
                return Err(storage_unavailable());
            }

            self.inner
                .writer
                .try_send(Write::Append(AppendRequest {
                    source,
                    events,
                    reply: tx,
                    _permit: permit,
                    policy: options.policy,
                    _forward_permits: forward_permits,
                    bytes: size,
                }))
                .map_err(queue_error)?;

            if forward {
                crate::forward::metrics::increment(&crate::forward::metrics::ADMITTED);
                crate::forward::metrics::ADMITTED_EVENTS
                    .fetch_add(widen(event_count), Ordering::Relaxed);
            }
        }

        // The writer owns accepted work independently of this caller's lifetime.
        Ok(rx)
    }

    /// Reserved bytes for one batch: the source and decoded input it already holds,
    /// the serialized output, and a fixed allowance per event.
    fn queued_size(
        &self,
        source_capacity: usize,
        events: &[NormalizedEvent],
        lengths: &[usize],
    ) -> Result<usize> {
        let mut size = source_capacity;
        for (event, length) in events.iter().zip(lengths) {
            size = size
                .checked_add(*length)
                .and_then(|n| {
                    n.checked_add(
                        self.inner
                            .projection
                            .as_ref()
                            .map_or(0, |layout| layout.retained_bytes(&event.attributes)),
                    )
                })
                .and_then(|n| n.checked_add(event.message.capacity()))
                .and_then(|n| n.checked_add(event.service.as_ref().map_or(0, String::capacity)))
                .and_then(|n| n.checked_add(event.logger.as_ref().map_or(0, String::capacity)))
                .and_then(|n| n.checked_add(event.host.as_ref().map_or(0, String::capacity)))
                .and_then(|n| n.checked_add(QUEUED_EVENT_OVERHEAD_BYTES))
                .ok_or_else(batch_too_large)?;
        }

        Ok(size)
    }

    fn check_size_limits(&self, size: usize, lane: AppendLane) -> Result<()> {
        if size > self.inner.max_queue_bytes {
            return Err(Error::new(
                ErrorKind::TooLarge,
                "batch exceeds configured writer byte capacity",
            ));
        }

        if lane.is_forward() && size > forward_byte_share(self.inner.max_queue_bytes) {
            return Err(Error::new(
                ErrorKind::TooLarge,
                "batch exceeds configured Forward index byte capacity",
            ));
        }

        Ok(())
    }

    /// A Forward producer cannot consume the entire per-index writer budget.
    /// Whole reservations fail immediately, so large envelopes never block
    /// small requests behind a partially acquired byte reservation.
    fn reserve_forward(&self, permits: u32) -> Result<ForwardPermits> {
        let slot = Arc::clone(&self.inner.forward_slots)
            .try_acquire_owned()
            .map_err(|error| {
                Error::new(ErrorKind::Overloaded, "Forward index admission full").with_source(error)
            })?;
        let bytes = Arc::clone(&self.inner.forward_bytes)
            .try_acquire_many_owned(permits)
            .map_err(|error| {
                Error::new(ErrorKind::Overloaded, "Forward index byte budget exhausted")
                    .with_source(error)
            })?;

        Ok(ForwardPermits {
            _slot: slot,
            _bytes: bytes,
        })
    }

    /// Serializes each event's attributes into a buffer of exactly the measured length.
    /// Projected values move out of the JSON into their own columns.
    fn queue_events(
        &self,
        events: Vec<NormalizedEvent>,
        lengths: Vec<usize>,
    ) -> Result<Vec<QueuedEvent>> {
        events
            .into_iter()
            .zip(lengths)
            .map(|(event, length)| self.queue_event(event, length))
            .collect::<Result<Vec<_>>>()
    }

    /// Queues one event: its attributes become a string of exactly `length` bytes, and
    /// its projected values are taken out of the attribute object.
    fn queue_event(&self, mut event: NormalizedEvent, length: usize) -> Result<QueuedEvent> {
        let mut output = BoundedJson {
            bytes: Vec::with_capacity(length),
            remaining: length,
        };
        serde_json::to_writer(&mut output, &event.attributes).map_err(db_error)?;
        let attributes = String::from_utf8(output.bytes).map_err(db_error)?;
        let projected = self
            .inner
            .projection
            .as_ref()
            .map_or_else(Vec::new, |layout| layout.take_values(&mut event.attributes));
        event.attributes = serde_json::Value::Null;

        Ok(QueuedEvent {
            event,
            attributes,
            projected,
        })
    }
}

pub(crate) fn serialized_length(value: &impl serde::Serialize) -> Result<usize> {
    struct Counter(usize);

    impl io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| io::Error::other("serialized JSON length overflow"))?;
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value).map_err(db_error)?;
    Ok(counter.0)
}

struct BoundedJson {
    bytes: Vec<u8>,
    remaining: usize,
}

impl io::Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(io::Error::other("JSON exceeds reserved output"));
        }

        self.bytes.extend_from_slice(bytes);
        self.remaining -= bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
