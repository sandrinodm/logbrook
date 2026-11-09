//! Fixed-cardinality receiver measurements, rendered on the authenticated endpoint.
use std::{
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

/// Number of timed stages, one duration histogram each.
const STAGE_COUNT: usize = 5;

/// Upper bounds of the duration buckets, in microseconds. The last bucket has no bound.
const BUCKET_BOUNDS_MICROS: [u64; 7] = [
    1_000,
    10_000,
    100_000,
    500_000,
    1_000_000,
    5_000_000,
    u64::MAX,
];

/// The `le` label of each bucket, in seconds, in the same order as `BUCKET_BOUNDS_MICROS`.
const BUCKET_LABELS: [&str; BUCKETS] = ["0.001", "0.01", "0.1", "0.5", "1", "5", "+Inf"];

/// Duration buckets per stage, including the unbounded `+Inf` bucket.
const BUCKETS: usize = BUCKET_BOUNDS_MICROS.len();

/// Slot holding the sum of observed durations, in microseconds, after the bucket counts.
const SUM_SLOT: usize = BUCKETS;

/// Slot holding the number of observations.
const COUNT_SLOT: usize = BUCKETS + 1;

/// Slots per stage: the bucket counts, the sum and the count.
const SLOTS: usize = BUCKETS + 2;

/// Converts the summed microseconds to the seconds of `logbrook_forward_duration_seconds_sum`.
const MICROS_PER_SECOND: f64 = 1_000_000.0;

macro_rules! counters {
    ($($name:ident),+) => {
        $(
            /// Receiver measurement exposed under a fixed-cardinality Prometheus name.
            pub static $name: AtomicU64 = AtomicU64::new(0);
        )+
    };
}

counters!(
    CONNECTIONS,
    ACCEPTED,
    REJECTED,
    ENVELOPES,
    EVENTS,
    ADMITTED,
    ADMITTED_EVENTS,
    COMMITTED,
    EMPTY,
    IGNORED,
    ACK_REQUESTED,
    ACK_SENT,
    ACK_FAILED,
    PROTOCOL_ERRORS,
    AUTH_ERRORS,
    ROUTE_ERRORS,
    OVERLOAD,
    STORAGE_ERRORS,
    RAW_BYTES,
    DECODED_BYTES,
    CPU_JOBS,
    HEARTBEATS,
    HEARTBEAT_FAILED
);

/// Failed session attempts, counted whether or not their diagnostic is logged.
pub static SESSION_ERRORS: AtomicU64 = AtomicU64::new(0);

/// Frames normalized through the generic fallback after a Docker parse failed.
pub static FALLBACKS: AtomicU64 = AtomicU64::new(0);

/// Events whose binary record encoding was preserved as typed values.
pub static BINARY_PRESERVED: AtomicU64 = AtomicU64::new(0);

/// Docker records that carry partial-message fields, stored as separate events without reassembly.
pub static DOCKER_FRAGMENTS: AtomicU64 = AtomicU64::new(0);

/// Per stage: one count per bucket, then the sum of durations and the number of durations.
static DURATIONS: Mutex<[[u64; SLOTS]; STAGE_COUNT]> = Mutex::new([[0; SLOTS]; STAGE_COUNT]);

/// Adds one to `counter`.
pub fn increment(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Widens a byte or item count to the 64-bit counter type. Every supported target has a `usize`
/// no wider than `u64`.
pub(super) fn to_u64(count: usize) -> u64 {
    u64::try_from(count).expect("usize fits in u64")
}

/// Holds `amount` in a gauge until it is dropped, then takes it back out.
pub struct GaugeGuard {
    counter: &'static AtomicU64,
    amount: u64,
}

impl GaugeGuard {
    /// Adds `amount` to `counter` for as long as the returned guard lives.
    pub fn new(counter: &'static AtomicU64, amount: usize) -> Self {
        let amount = to_u64(amount);
        counter.fetch_add(amount, Ordering::Relaxed);
        Self { counter, amount }
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.amount, Ordering::Relaxed);
    }
}

/// Renders every counter, gauge and histogram in the Prometheus text format.
pub fn prometheus() -> String {
    let mut output = String::new();
    for (name, counter, kind) in [
        ("connections_active", &CONNECTIONS, "gauge"),
        ("connections_accepted_total", &ACCEPTED, "counter"),
        ("connections_rejected_total", &REJECTED, "counter"),
        ("envelopes_received_total", &ENVELOPES, "counter"),
        ("events_received_total", &EVENTS, "counter"),
        ("envelopes_admitted_total", &ADMITTED, "counter"),
        ("events_admitted_total", &ADMITTED_EVENTS, "counter"),
        ("events_committed_total", &COMMITTED, "counter"),
        ("empty_envelopes_total", &EMPTY, "counter"),
        ("ignored_values_total", &IGNORED, "counter"),
        ("ack_requested_total", &ACK_REQUESTED, "counter"),
        ("ack_sent_total", &ACK_SENT, "counter"),
        ("ack_failed_total", &ACK_FAILED, "counter"),
        ("protocol_rejected_total", &PROTOCOL_ERRORS, "counter"),
        ("authentication_rejected_total", &AUTH_ERRORS, "counter"),
        ("route_rejected_total", &ROUTE_ERRORS, "counter"),
        ("overload_total", &OVERLOAD, "counter"),
        ("storage_rejected_total", &STORAGE_ERRORS, "counter"),
        ("raw_bytes", &RAW_BYTES, "gauge"),
        ("decoded_bytes", &DECODED_BYTES, "gauge"),
        ("cpu_jobs", &CPU_JOBS, "gauge"),
        ("heartbeats_total", &HEARTBEATS, "counter"),
        ("heartbeat_send_failed_total", &HEARTBEAT_FAILED, "counter"),
    ] {
        push_sample(&mut output, name, kind, counter.load(Ordering::Relaxed));
    }

    output.push_str(&durations_prometheus());

    for (name, counter) in [
        ("session_errors_total", &SESSION_ERRORS),
        ("normalization_fallback_total", &FALLBACKS),
        ("binary_preserved_total", &BINARY_PRESERVED),
        ("docker_fragments_total", &DOCKER_FRAGMENTS),
    ] {
        push_sample(
            &mut output,
            name,
            "counter",
            counter.load(Ordering::Relaxed),
        );
    }

    output
}

fn push_sample(output: &mut String, name: &str, kind: &str, value: u64) {
    output.push_str(&format!(
        "# TYPE logbrook_forward_{name} {kind}\nlogbrook_forward_{name} {value}\n"
    ));
}

/// A timed stage of Forward receipt. Each stage has its own duration histogram.
#[derive(Clone, Copy)]
pub enum Stage {
    /// Decoding a frame on a blocking worker.
    Decode,
    /// Waiting for capacity to append a batch.
    Admission,
    /// Appending a batch to storage.
    Commit,
    /// Writing an acknowledgement.
    Ack,
    /// Waiting for CPU and decode-budget permits before decoding.
    Preparation,
}

impl Stage {
    /// Every stage in histogram index order, which is also the order they are rendered in.
    const ALL: [Self; STAGE_COUNT] = [
        Self::Decode,
        Self::Admission,
        Self::Commit,
        Self::Ack,
        Self::Preparation,
    ];

    /// The `stage` label value.
    fn label(self) -> &'static str {
        match self {
            Self::Decode => "decode",
            Self::Admission => "admission",
            Self::Commit => "commit",
            Self::Ack => "ack",
            Self::Preparation => "preparation",
        }
    }
}

/// Records how long a stage takes, when dropped.
pub struct Timer {
    stage: Stage,
    start: Instant,
}

impl Timer {
    /// Starts timing `stage`.
    pub fn new(stage: Stage) -> Self {
        Self {
            stage,
            start: Instant::now(),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let micros = u64::try_from(self.start.elapsed().as_micros()).unwrap_or(u64::MAX);
        let mut durations = lock_durations();
        // Stage discriminants are the histogram indices, in declaration order.
        let slots = &mut durations[self.stage as usize];
        for (count, bound) in slots.iter_mut().zip(BUCKET_BOUNDS_MICROS) {
            if micros <= bound {
                *count = count.saturating_add(1);
            }
        }

        slots[SUM_SLOT] = slots[SUM_SLOT].saturating_add(micros);
        slots[COUNT_SLOT] = slots[COUNT_SLOT].saturating_add(1);
    }
}

/// Locks the histograms. A panic while the lock is held leaves only counters behind, which stay
/// valid, so a poisoned lock is still used.
fn lock_durations() -> MutexGuard<'static, [[u64; SLOTS]; STAGE_COUNT]> {
    DURATIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn durations_prometheus() -> String {
    let mut output = String::from("# TYPE logbrook_forward_duration_seconds histogram\n");
    let durations = lock_durations();
    for (stage, slots) in Stage::ALL.into_iter().zip(durations.iter()) {
        let label = stage.label();
        for (bound, count) in BUCKET_LABELS.iter().zip(slots) {
            output.push_str(&format!(
                "logbrook_forward_duration_seconds_bucket{{stage=\"{label}\",\
                 le=\"{bound}\"}} {count}\n"
            ));
        }

        // u64 to f64 rounds only beyond 2^53 microseconds, about 285 years of summed time.
        let sum_seconds = slots[SUM_SLOT] as f64 / MICROS_PER_SECOND;
        let observations = slots[COUNT_SLOT];
        output.push_str(&format!(
            "logbrook_forward_duration_seconds_sum{{stage=\"{label}\"}} {sum_seconds}\n"
        ));
        output.push_str(&format!(
            "logbrook_forward_duration_seconds_count{{stage=\"{label}\"}} {observations}\n"
        ));
    }

    output
}
