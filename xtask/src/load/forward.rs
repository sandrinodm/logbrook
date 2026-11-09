//! Open-loop Forward workloads with independently observed committed event IDs.
use super::{
    engine::{self, Accounting, Completion, Job, Schedule, Work},
    http::{self, Http},
    semantics::{HTTP_OK, QUERY_OPERATION, Samples},
};
use crate::{
    Result,
    runtime::{self, Fixture},
};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::Write,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Mutex, Semaphore, watch},
    task::JoinHandle,
    time::{Instant, timeout},
};

// Bits in each event's state word, recording how far its submission got. They combine with `|`.
/// The event's batch was about to be written to a Forward session or sent over HTTP.
const ATTEMPTED: u8 = 1;

/// The whole Forward batch was written to the socket, which alone does not confirm ingestion.
const WRITTEN: u8 = 2;

/// The receiver returned an ACK naming this batch's chunk token.
const ACKNOWLEDGED: u8 = 4;

/// The send failed, so whether the event was committed is ambiguous.
const FAILED: u8 = 8;

/// The verification query found the event in storage.
const COMMITTED: u8 = 16;

/// The HTTP ingest returned 200 with an `accepted` count matching the batch.
const HTTP_CONFIRMED: u8 = 32;

// Engine queue indices for the three kinds of scheduled work.
const FORWARD_QUEUE: usize = 0;
const HTTP_QUEUE: usize = 1;
const QUERY_QUEUE: usize = 2;

/// Engine workers that run the query schedule concurrently.
const QUERY_CLIENTS: usize = 4;

/// Smallest engine queue capacity. The forward queue grows to `--connections`; the HTTP and
/// query queues use exactly this, and a full queue drops the batch.
const MIN_QUEUE_CAPACITY: usize = 64;

/// Shortest `--duration` accepted, in seconds.
const MIN_DURATION_SECS: f64 = 0.01;

/// Longest `--duration` accepted, in seconds.
const MAX_DURATION_SECS: f64 = 3600.0;

/// Largest `--rate` accepted, in Forward events per second.
const MAX_RATE: f64 = 1_000_000.0;

/// Largest `--connections` accepted, in persistent TCP sessions.
const MAX_CONNECTIONS: usize = 10_000;

/// Largest `--batch-size` accepted, in events per batch.
const MAX_BATCH_SIZE: u64 = 10_000;

/// Largest `--padding-bytes` accepted, per event.
const MAX_PADDING_BYTES: usize = 128 * 1024;

/// Largest `--http-rate` accepted, in HTTP events per second.
const MAX_HTTP_RATE: f64 = 100_000.0;

/// Largest `--query-rate` accepted, in queries per second.
const MAX_QUERY_RATE: f64 = 1000.0;

/// Largest `--http-clients` accepted.
const MAX_HTTP_CLIENTS: usize = 1000;

/// Largest `--drain-seconds` accepted.
const MAX_DRAIN_SECONDS: u64 = 120;

/// Largest total offered load, `(--rate + --http-rate) * --duration`, in events. Every offered
/// event gets a one-byte state word up front, so this also bounds that allocation.
const MAX_OFFERED_EVENTS: f64 = 50_000_000.0;

/// Largest `--padding-bytes` accepted when `--http-rate` is nonzero.
const MAX_MIXED_PADDING_BYTES: usize = 3500;

/// Largest `--batch-size` accepted when `--http-rate` is nonzero.
const MAX_MIXED_BATCH_SIZE: u64 = 1000;

/// Largest encoded envelope the generator will send, in bytes.
const MAX_ENVELOPE_BYTES: usize = 8 * 1024 * 1024;

/// Framing and metadata allowance per event, on top of its padding.
const EVENT_OVERHEAD_BYTES: usize = 256;

/// Multiplier applied to a batch's estimated size when reserving memory for it.
const RESERVATION_MULTIPLIER: usize = 3;

/// Client-side memory, in bytes, that batches in flight may reserve together. A batch that
/// cannot reserve its allowance is dropped before sending and reported as a 429.
const MEMORY_BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// Batches at or above this size use the larger normalized envelope cap.
const LARGE_BATCH_SIZE: u64 = 1000;

/// The receiver's `max_normalized_bytes` for batches of at least `LARGE_BATCH_SIZE` events.
const LARGE_NORMALIZED_CAP_BYTES: usize = 64 * 1024 * 1024;

/// The receiver's `max_normalized_bytes` for smaller batches.
const SMALL_NORMALIZED_CAP_BYTES: usize = 8 * 1024 * 1024;

/// Extra receiver connections above the generator's own, so that it can always connect.
const FORWARD_CONNECTION_HEADROOM: usize = 16;

/// Each TCP connect to the receiver gets at most this long.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Most sessions opened at once while setting up the connection pool.
const CONNECT_CONCURRENCY: usize = 64;

/// Most connection error messages kept in the report.
const CONNECTION_ERROR_EXAMPLES: usize = 20;

/// Writing one batch to its session gets at most this long before the send counts as failed.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// With `--ack`, the receiver's ACK for a batch must arrive within this long.
const ACK_TIMEOUT: Duration = Duration::from_secs(10);

/// How often the server's metrics are sampled during the run.
const OBSERVER_INTERVAL: Duration = Duration::from_secs(10);

/// The metrics observer gets this long to stop before it is aborted and its samples dropped.
const OBSERVER_STOP_TIMEOUT: Duration = Duration::from_secs(15);

/// Delay between metrics polls while waiting for the receiver to go idle.
const RECEIVER_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The query window starts this long before the run, to include the first events.
const LOOKBACK_MS: i64 = 1000;

/// The query window extends this long past the offered load and drain, to include stragglers.
const WINDOW_TAIL_SECS: f64 = 120.0;

// Unit conversions.
const MILLIS_PER_SEC: i64 = 1000;
const BYTES_PER_MIB: f64 = 1_048_576.0;

/// Verification pages are sized to hold about this many bytes of events.
const PAGE_BUDGET_BYTES: usize = 1024 * 1024;

/// Per-row allowance added to the padding when sizing a verification page.
const ROW_OVERHEAD_BYTES: usize = 1024;

/// Most rows requested per verification page, however small the events are.
const MAX_PAGE_ROWS: usize = 1000;

/// Longest string the receiver may echo back in an ACK.
const MAX_ACK_STRING_BYTES: usize = 1024;

// MessagePack format markers. The synthetic frames are built here on purpose, independent of
// the receiver's codec: they use only normative primitive encodings and fixed bounded record
// shapes.
const FIXARRAY: u8 = 0x90;
const FIXARRAY_LIMIT: usize = 16;
const FIXMAP: u8 = 0x80;
const FIXMAP_LAST: u8 = FIXMAP | 0x0f;
const FIXMAP_MASK: u8 = 0x0f;
const FIXSTR: u8 = 0xa0;
const FIXSTR_LAST: u8 = FIXSTR | 0x1f;
const FIXSTR_MASK: u8 = 0x1f;
const FIXSTR_LIMIT: usize = 32;
const BIN32: u8 = 0xc6;
const STR8: u8 = 0xd9;
const STR16: u8 = 0xda;
const STR32: u8 = 0xdb;
const ARRAY16: u8 = 0xdc;
const MAP16: u8 = 0xde;
const MAP32: u8 = 0xdf;
const UINT64: u8 = 0xcf;

/// The only key in a Forward ACK map; its value must be the batch's chunk token.
const ACK_KEY: &[u8] = b"ack";

/// Status recorded for a batch dropped before sending because `MEMORY_BUDGET_BYTES` was full.
const HTTP_TOO_MANY_REQUESTS: u16 = 429;

/// Level of the synthetic log records; 30 is info in the pino numbering.
const INFO_LOG_LEVEL: i64 = 30;

/// The wire layout of a batch, parsed from `--mode`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Message,
    Forward,
    Packed,
    Gzip,
}

impl Mode {
    /// Parses a `--mode` value; `None` for any other text.
    fn parse(text: &str) -> Option<Self> {
        match text {
            "message" => Some(Self::Message),
            "forward" => Some(Self::Forward),
            "packed" => Some(Self::Packed),
            "gzip" => Some(Self::Gzip),
            _ => None,
        }
    }
}

// Options for `forward-load`: an open-loop Forward generator with per-event accounting.
// Clap uses `///` docs as help text, so this stays a plain comment to keep `--help` unchanged.
#[derive(clap::Args, Clone, Debug, serde::Serialize)]
pub struct Args {
    #[arg(long, default_value = "target/debug/logbrook")]
    pub binary: PathBuf,
    #[arg(long, default_value_t = 60.0)]
    pub duration: f64,
    /// Offered events per second, independent of response latency.
    #[arg(long, default_value_t = 1000.0)]
    pub rate: f64,
    /// Persistent TCP sessions; explicitly exercise 100, 1000 and 10000.
    #[arg(long, default_value_t = 100)]
    pub connections: usize,
    #[arg(long, default_value_t = 100)]
    pub batch_size: u64,
    #[arg(long, default_value_t = 900)]
    pub padding_bytes: usize,
    #[arg(long, value_parser = ["message", "forward", "packed", "gzip"], default_value = "forward")]
    pub mode: String,
    #[arg(long)]
    pub ack: bool,
    /// Concurrent HTTP events per second for transport fairness measurements.
    #[arg(long, default_value_t = 0.0)]
    pub http_rate: f64,
    #[arg(long, default_value_t = 0.0)]
    pub query_rate: f64,
    #[arg(long, default_value_t = 10)]
    pub http_clients: usize,
    #[arg(long, default_value_t = 30)]
    pub drain_seconds: u64,
    #[arg(long)]
    pub output: PathBuf,
}

impl Args {
    /// The `--mode` value as a `Mode`, if it names one.
    fn parsed_mode(&self) -> Option<Mode> {
        Mode::parse(&self.mode)
    }

    /// Checks generator, HTTP and mode bounds in order, stopping at the first failing group.
    fn validate(&self) -> Result<()> {
        if self.generator_limits_exceeded()
            || self.http_limits_exceeded()
            || self.mode_and_mixed_limits_exceeded()
        {
            return Err(concat!(
                "Forward workload parameters exceed bounded generator limits ",
                "(message mode requires batch-size=1; ",
                "mixed HTTP padding <=3500 and batch-size<=1000)"
            )
            .into());
        }
        Ok(())
    }

    /// Duration, rate, connection, batch and envelope bounds for the generator itself. The
    /// envelope check multiplies by the batch size, so it relies on the batch bound above it.
    fn generator_limits_exceeded(&self) -> bool {
        !self.duration.is_finite()
            || !(MIN_DURATION_SECS..=MAX_DURATION_SECS).contains(&self.duration)
            || !self.rate.is_finite()
            || !(1.0..=MAX_RATE).contains(&self.rate)
            || !(1..=MAX_CONNECTIONS).contains(&self.connections)
            || !(1..=MAX_BATCH_SIZE).contains(&self.batch_size)
            || self.padding_bytes > MAX_PADDING_BYTES
            || self.batch_size * (self.padding_bytes as u64 + EVENT_OVERHEAD_BYTES as u64)
                > MAX_ENVELOPE_BYTES as u64
    }

    /// Bounds for the HTTP and query legs.
    fn http_limits_exceeded(&self) -> bool {
        !self.http_rate.is_finite()
            || !(0.0..=MAX_HTTP_RATE).contains(&self.http_rate)
            || !self.query_rate.is_finite()
            || !(0.0..=MAX_QUERY_RATE).contains(&self.query_rate)
            || !(1..=MAX_HTTP_CLIENTS).contains(&self.http_clients)
            || !(1..=MAX_DRAIN_SECONDS).contains(&self.drain_seconds)
    }

    /// The mode must be one that exists, and message mode sends one record per batch. Then the
    /// total offered load, and the padding and batch bounds for mixed HTTP traffic.
    fn mode_and_mixed_limits_exceeded(&self) -> bool {
        (self.parsed_mode() == Some(Mode::Message) && self.batch_size != 1)
            || self.parsed_mode().is_none()
            || (self.rate + self.http_rate) * self.duration > MAX_OFFERED_EVENTS
            || (self.http_rate > 0.0
                && (self.padding_bytes > MAX_MIXED_PADDING_BYTES
                    || self.batch_size > MAX_MIXED_BATCH_SIZE))
    }
}

/// The schedule for one queue: forward and HTTP batches use `args.batch_size`, queries are one at
/// a time.
fn schedule(args: &Args, rate: f64, queue: usize) -> Schedule {
    Schedule {
        duration: args.duration,
        rate,
        batch: if queue == QUERY_QUEUE {
            1
        } else {
            args.batch_size
        },
        burst_rate: 0.0,
        burst_start: 0.0,
        burst_duration: 0.0,
        queue,
        total: None,
    }
}

/// The number of events a schedule will offer.
fn planned(schedule: &Schedule) -> u64 {
    schedule.jobs().map(|job| job.count).sum()
}

/// Writes a MessagePack array header for `length` elements.
fn array(out: &mut Vec<u8>, length: usize) {
    if length < FIXARRAY_LIMIT {
        out.push(FIXARRAY | field::<u8>(length));
    } else {
        out.push(ARRAY16);
        out.extend_from_slice(&field::<u16>(length).to_be_bytes());
    }
}

/// Writes a MessagePack string, choosing the smallest header that fits its length.
fn string(out: &mut Vec<u8>, text: &str) {
    let length = text.len();
    if length < FIXSTR_LIMIT {
        out.push(FIXSTR | field::<u8>(length));
    } else if length <= usize::from(u8::MAX) {
        out.extend_from_slice(&[STR8, field::<u8>(length)]);
    } else if length <= usize::from(u16::MAX) {
        out.push(STR16);
        out.extend_from_slice(&field::<u16>(length).to_be_bytes());
    } else {
        out.push(STR32);
        out.extend_from_slice(&field::<u32>(length).to_be_bytes());
    }
    out.extend_from_slice(text.as_bytes());
}

/// Writes a MessagePack unsigned 64-bit integer.
fn integer(out: &mut Vec<u8>, value: u64) {
    out.push(UINT64);
    out.extend_from_slice(&value.to_be_bytes());
}

/// Writes one log record: a message, and a padding field when `padding` is not empty.
fn record(out: &mut Vec<u8>, run: &str, id: u64, padding: &str) {
    out.push(if padding.is_empty() {
        FIXMAP | 1
    } else {
        FIXMAP | 2
    });
    string(out, "msg");
    string(out, &format!("forward-load:{run}:{id}"));
    if !padding.is_empty() {
        string(out, "padding");
        string(out, padding);
    }
}

/// Converts a length to the width of the MessagePack field that holds it. Every caller bounds
/// the length by that width before it gets here, so the conversion cannot fail.
fn field<T: TryFrom<usize>>(length: usize) -> T
where
    T::Error: std::fmt::Debug,
{
    T::try_from(length).expect("length fits its MessagePack field")
}

/// Gzip-compresses encoded records at the fastest level.
fn gzip(bytes: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(bytes)?;
    encoder.finish()
}

/// Builds the bytes of one generated batch frame, and the token its ACK must echo.
fn frame(
    args: &Args,
    run: &str,
    job: &Job,
    time: u64,
    padding: &str,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let batch = BatchFrame {
        args,
        run,
        job,
        time,
        padding,
    };
    let token = batch.token();
    Ok((batch.encode(&token)?, token.into_bytes()))
}

/// The inputs that define one synthetic batch frame.
struct BatchFrame<'a> {
    args: &'a Args,
    run: &'a str,
    job: &'a Job,
    time: u64,
    padding: &'a str,
}

impl BatchFrame<'_> {
    /// The chunk token for this batch, which the receiver echoes in its ACK.
    fn token(&self) -> String {
        format!("batch-{}", self.job.sequence)
    }

    /// Whether the envelope carries an options map: a requested ACK or gzip compression.
    fn has_options(&self) -> bool {
        self.args.ack || self.args.parsed_mode() == Some(Mode::Gzip)
    }

    /// The envelope's array length: the tag, then the body (`time` and a record in message
    /// mode, one events array otherwise), then the options map when there is one.
    fn envelope_len(&self) -> usize {
        let body_items = if self.args.parsed_mode() == Some(Mode::Message) {
            2
        } else {
            1
        };
        1 + body_items + usize::from(self.has_options())
    }

    /// Encodes the envelope that the receiver sees for this batch.
    fn encode(&self, token: &str) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        array(&mut out, self.envelope_len());
        string(&mut out, "load.synthetic");
        match self.args.parsed_mode() {
            Some(Mode::Message) => {
                integer(&mut out, self.time);
                record(&mut out, self.run, self.job.sequence, self.padding);
            }
            Some(Mode::Forward) => {
                array(&mut out, self.job.count as usize);
                out.extend_from_slice(&self.events());
            }
            Some(mode @ (Mode::Packed | Mode::Gzip)) => {
                let mut entries = self.events();
                if mode == Mode::Gzip {
                    entries = gzip(&entries)?;
                }

                out.push(BIN32);
                out.extend_from_slice(&field::<u32>(entries.len()).to_be_bytes());
                out.extend_from_slice(&entries);
            }
            None => return Err("unknown Forward mode".into()),
        }

        self.append_options(&mut out, token);
        if out.len() > MAX_ENVELOPE_BYTES {
            return Err("complete encoded Forward envelope exceeds 8 MiB".into());
        }

        Ok(out)
    }

    /// The batch's records, each a `[time, record]` pair, for the forward and packed layouts.
    fn events(&self) -> Vec<u8> {
        let mut entries = Vec::new();
        for offset in 0..self.job.count {
            array(&mut entries, 2);
            integer(&mut entries, self.time);
            record(
                &mut entries,
                self.run,
                self.job.sequence * self.args.batch_size + offset,
                self.padding,
            );
        }
        entries
    }

    /// Appends the options map: the chunk token for an ACK, and the codec for gzip.
    fn append_options(&self, out: &mut Vec<u8>, token: &str) {
        if !self.has_options() {
            return;
        }

        let gzip = self.args.parsed_mode() == Some(Mode::Gzip);
        out.push(FIXMAP | (u8::from(self.args.ack) + u8::from(gzip)));
        if self.args.ack {
            string(out, "chunk");
            string(out, token);
        }

        if gzip {
            string(out, "compressed");
            string(out, "gzip");
        }
    }
}

/// Reads one ACK string: a MessagePack string whose length must stay within the bound.
async fn ack_string(socket: &mut TcpStream) -> Result<Vec<u8>> {
    let marker = socket.read_u8().await?;
    let length = match marker {
        FIXSTR..=FIXSTR_LAST => usize::from(marker & FIXSTR_MASK),
        STR8 => usize::from(socket.read_u8().await?),
        STR16 => usize::from(socket.read_u16().await?),
        STR32 => usize::try_from(socket.read_u32().await?)?,
        _ => return Err("Forward ACK key/token must be protocol strings".into()),
    };
    if length > MAX_ACK_STRING_BYTES {
        return Err("Forward ACK string exceeded its bound".into());
    }

    let mut bytes = vec![0; length];
    socket.read_exact(&mut bytes).await?;
    Ok(bytes)
}

/// Reads the receiver's ACK for one batch and checks that it names `token`.
async fn read_ack(socket: &mut TcpStream, token: &[u8]) -> Result<()> {
    let entries = match socket.read_u8().await? {
        marker @ FIXMAP..=FIXMAP_LAST => usize::from(marker & FIXMAP_MASK),
        MAP16 => usize::from(socket.read_u16().await?),
        MAP32 => usize::try_from(socket.read_u32().await?)?,
        _ => return Err("Forward ACK must be a map".into()),
    };
    let matches =
        entries == 1 && ack_string(socket).await? == ACK_KEY && ack_string(socket).await? == token;
    if !matches {
        return Err("Forward ACK did not match the requested chunk".into());
    }

    Ok(())
}

/// Sets `flag` on every event state in `first..first + count`.
fn mark(states: &[AtomicU8], first: u64, count: u64, flag: u8) {
    for state in &states[first as usize..(first + count) as usize] {
        state.fetch_or(flag, Ordering::Relaxed);
    }
}

/// Records a counter on a completion, replacing any earlier value.
fn set_counter(completion: &mut Completion, name: &str, value: u64) {
    completion.counters.insert(name.into(), value);
}

/// The completion for one HTTP batch, with the status the server returned (0 when none).
fn http_completion(status: u16) -> Completion {
    Completion::new("http", status)
}

/// Opens one TCP session with no-delay set, bounded by `CONNECT_TIMEOUT`.
async fn connect(address: &str) -> Result<TcpStream> {
    let socket = timeout(CONNECT_TIMEOUT, TcpStream::connect(address)).await??;
    socket.set_nodelay(true)?;
    Ok(socket)
}

/// Opens `count` sessions, at most `CONNECT_CONCURRENCY` at a time, and keeps each outcome.
async fn connect_all(address: &str, count: usize) -> Vec<Result<TcpStream>> {
    stream::iter(0..count)
        .map(|_| async { connect(address).await })
        .buffer_unordered(CONNECT_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
}

/// The memory reserved from the shared budget for a batch of `count` events.
fn memory_allowance(padding_bytes: usize, count: u64) -> u32 {
    (count as usize * (padding_bytes + EVENT_OVERHEAD_BYTES) * RESERVATION_MULTIPLIER).max(1) as u32
}

/// The time range that the query and verification steps look at, in epoch milliseconds.
#[derive(Clone, Copy)]
struct Window {
    lower: i64,
    upper: i64,
}

/// What the generator tasks share: the batch inputs, the socket pool and the state table.
struct Workload {
    args: Args,
    address: String,
    run: String,
    padding: String,
    http: Http,
    states: Arc<Vec<AtomicU8>>,
    sessions: Vec<Mutex<Option<TcpStream>>>,
    memory: Arc<Semaphore>,
    /// First event ID of the HTTP jobs. Their IDs follow the forward IDs in the state table.
    http_offset: u64,
}

impl Workload {
    /// The first event ID of a forward job.
    fn forward_first(&self, job: &Job) -> u64 {
        job.sequence * self.args.batch_size
    }

    /// Sends one forward batch and records how far it got. A failure drops the socket, so the
    /// next batch on this session reconnects, and leaves the batch marked as ambiguous.
    async fn forward(&self, job: Job) -> Result<Completion> {
        let mut completion = Completion::new("forward", HTTP_OK);
        let Ok(_memory) = self
            .memory
            .clone()
            .try_acquire_many_owned(memory_allowance(self.args.padding_bytes, job.count))
        else {
            completion.status = HTTP_TOO_MANY_REQUESTS;
            set_counter(
                &mut completion,
                "client_preparation_dropped_forward",
                job.count,
            );
            return Ok(completion);
        };

        let (bytes, expected_ack) = frame(
            &self.args,
            &self.run,
            &job,
            crate::now_ms() as u64 / MILLIS_PER_SEC as u64,
            &self.padding,
        )?;

        let mut session = self.sessions[job.sequence as usize % self.sessions.len()]
            .lock()
            .await;
        let outcome = self
            .send_forward(&mut session, &job, &bytes, &expected_ack, &mut completion)
            .await;
        if let Err(error) = outcome {
            *session = None;
            mark(&self.states, self.forward_first(&job), job.count, FAILED);
            completion.status = 0;
            set_counter(&mut completion, "failed_forward", job.count);
            completion.failure = Some(json!({
                "sequence": job.sequence,
                "error": error.to_string(),
                "commit_ambiguous": true
            }));
        }

        Ok(completion)
    }

    /// Writes one batch on the pooled session, connecting first if it is closed, and waits for
    /// the ACK when one was requested.
    async fn send_forward(
        &self,
        session: &mut Option<TcpStream>,
        job: &Job,
        bytes: &[u8],
        expected_ack: &[u8],
        completion: &mut Completion,
    ) -> Result<()> {
        if session.is_none() {
            *session = Some(connect(&self.address).await?);
        }

        let socket = session.as_mut().expect("connected session");
        let first = self.forward_first(job);
        mark(&self.states, first, job.count, ATTEMPTED);
        set_counter(completion, "transport_attempted_forward", job.count);

        timeout(WRITE_TIMEOUT, socket.write_all(bytes)).await??;

        mark(&self.states, first, job.count, WRITTEN);
        set_counter(completion, "socket_written_forward", job.count);
        set_counter(completion, "wire_bytes_forward", bytes.len() as u64);

        if self.args.ack {
            timeout(ACK_TIMEOUT, read_ack(socket, expected_ack)).await??;
            mark(&self.states, first, job.count, ACKNOWLEDGED);
            set_counter(completion, "ack_observed_forward", job.count);
        }

        Ok(())
    }

    /// Ingests one batch over HTTP. Only a 200 whose `accepted` count matches is confirmed.
    async fn http(&self, job: Job) -> Result<Completion> {
        let first = self.http_offset + job.sequence * self.args.batch_size;
        let Ok(_memory) = self
            .memory
            .clone()
            .try_acquire_many_owned(memory_allowance(self.args.padding_bytes, job.count))
        else {
            let mut completion = http_completion(HTTP_TOO_MANY_REQUESTS);
            set_counter(
                &mut completion,
                "client_preparation_dropped_http",
                job.count,
            );
            return Ok(completion);
        };
        let events = self.http_events(first, job.count);
        mark(&self.states, first, job.count, ATTEMPTED);
        let (status, value) = self
            .http
            .request(
                "/logs/ingest",
                Some((serde_json::to_vec(&events)?, "application/json", false)),
                false,
            )
            .await;
        let mut completion = http_completion(status);
        set_counter(&mut completion, "transport_attempted_http", job.count);
        if status == HTTP_OK && value["accepted"].as_u64() == Some(job.count) {
            mark(&self.states, first, job.count, HTTP_CONFIRMED);
            set_counter(&mut completion, "confirmed_http", job.count);
        } else {
            mark(&self.states, first, job.count, FAILED);
            set_counter(&mut completion, "failed_http", job.count);
            completion.failure = Some(json!({
                "status": status,
                "payload": value
            }));
        }

        Ok(completion)
    }

    /// The JSON events for an HTTP batch, with IDs `first..first + count`.
    fn http_events(&self, first: u64, count: u64) -> Vec<Value> {
        (0..count)
            .map(|offset| {
                json!({
                    "time": crate::now_ms(),
                    "level": INFO_LOG_LEVEL,
                    "msg": format!("forward-load:{}:{}", self.run, first + offset),
                    "sequence": first + offset,
                    "padding": self.padding
                })
            })
            .collect()
    }

    /// Runs one count query over the window, the query leg of the workload.
    async fn count_query(&self, window: Window) -> Result<Completion> {
        let Window { lower, upper } = window;
        let path = format!("/logs/count?from={lower}&to={upper}");
        let (status, payload) = self.http.request(&path, None, false).await;
        let mut completion = Completion::new(QUERY_OPERATION, status);
        if status == HTTP_OK && payload["count"].as_u64().is_some() {
            set_counter(&mut completion, "successful_queries", 1);
        } else {
            set_counter(&mut completion, "failed_queries", 1);
            completion.failure = Some(json!({
                "status": status,
                "payload": payload
            }));
        }

        Ok(completion)
    }
}

/// Builds the engine's work function, which routes each scheduled job to its generator.
fn work_for(state: Arc<Workload>, window: Window) -> Work {
    Arc::new(move |job| {
        let state = state.clone();
        Box::pin(async move {
            match job.queue {
                FORWARD_QUEUE => state.forward(job).await,
                HTTP_QUEUE => state.http(job).await,
                _ => state.count_query(window).await,
            }
        })
    })
}

/// Samples the server's metrics every `OBSERVER_INTERVAL` until `stopped` changes.
async fn observe_metrics(
    http: Http,
    start: Instant,
    mut stopped: watch::Receiver<bool>,
) -> Vec<Value> {
    let mut metrics = Vec::new();
    let mut tick = tokio::time::interval(OBSERVER_INTERVAL);
    loop {
        tokio::select! {
            biased;
            _ = stopped.changed() => break,
            _ = tick.tick() => metrics.push(sample_metrics(&http, start).await),
        }
    }

    metrics
}

/// Reads one metrics response and records the elapsed time after the request completes.
async fn sample_metrics(http: &Http, start: Instant) -> Value {
    let (status, text) = http.request("/metrics", None, true).await;
    json!({
        "seconds": start.elapsed().as_secs_f64(),
        "status": status,
        "text": text
    })
}

/// Signals the metrics observer to stop and returns its samples. An observer that does not
/// finish within `OBSERVER_STOP_TIMEOUT` is aborted and contributes no samples.
async fn stop_observer(
    stop: watch::Sender<bool>,
    observer: &mut JoinHandle<Vec<Value>>,
) -> Result<Vec<Value>> {
    let _ = stop.send(true);
    match timeout(OBSERVER_STOP_TIMEOUT, &mut *observer).await {
        Ok(result) => Ok(result?),
        Err(_) => {
            observer.abort();
            let _ = observer.await;
            Ok(Vec::new())
        }
    }
}

/// Drops every pooled session, so receivers without ACKs see an end of stream.
async fn close_sessions(sessions: &[Mutex<Option<TcpStream>>]) {
    for session in sessions {
        session.lock().await.take();
    }
}

/// Polls the receiver's metrics until its Forward connections and CPU jobs are idle, or until
/// `deadline`. Returns whether it drained, and the last metrics text read.
async fn wait_for_receiver(http: &Http, deadline: Instant) -> (bool, Value) {
    let mut after = Value::Null;
    while Instant::now() < deadline {
        let (status, text) = http.request("/metrics", None, true).await;
        let values = http::metrics(&text);
        after = text;
        if status == HTTP_OK
            && values.get("logbrook_forward_connections_active") == Some(&0.0)
            && values.get("logbrook_forward_cpu_jobs") == Some(&0.0)
        {
            return (true, after);
        }

        tokio::time::sleep(RECEIVER_POLL_INTERVAL).await;
    }

    (false, after)
}

/// Pages through the storage query for this run and checks each stored event against its
/// state word: duplicates and events that were never attempted are counted.
async fn verification(
    http: &Http,
    states: &[AtomicU8],
    run: &str,
    window: Window,
    padding: usize,
) -> Result<Value> {
    let mut cursor = None;
    let mut rows = 0u64;
    let mut duplicates = 0u64;
    let mut unexpected = 0u64;
    let limit = (PAGE_BUDGET_BYTES / (padding + ROW_OVERHEAD_BYTES)).clamp(1, MAX_PAGE_ROWS);

    loop {
        let mut params = BTreeMap::from([
            ("from".into(), window.lower.to_string()),
            ("to".into(), window.upper.to_string()),
            ("message".into(), format!("forward-load:{run}:")),
            ("limit".into(), limit.to_string()),
        ]);
        if let Some(cursor) = cursor.take() {
            params.insert("cursor".into(), cursor);
        }

        let (status, page) = http
            .request(&http::path("/logs", &params), None, false)
            .await;
        if status != HTTP_OK {
            return Err(format!("Forward verification query failed: {status} {page}").into());
        }

        for event in page["events"].as_array().ok_or("missing query events")? {
            rows += 1;
            let Some(state) = event_id(event, run).and_then(|id| states.get(id as usize)) else {
                unexpected += 1;
                continue;
            };
            let previous = state.fetch_or(COMMITTED, Ordering::Relaxed);
            if previous & COMMITTED != 0 {
                duplicates += 1;
            }

            if previous & ATTEMPTED == 0 {
                unexpected += 1;
            }
        }

        cursor = page["next_cursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }

    Ok(json!({
        "queried_rows": rows,
        "duplicated_rows": duplicates,
        "unexpected_rows": unexpected
    }))
}

/// The event ID of a stored row: from its application fields, its top-level field, or the
/// numeric suffix of its message.
fn event_id(event: &Value, run: &str) -> Option<u64> {
    let attributes = &event["attributes"];
    attributes["application"]["sequence"]
        .as_u64()
        .or_else(|| attributes["sequence"].as_u64())
        .or_else(|| {
            event["message"]
                .as_str()
                .and_then(|message| message.strip_prefix(&format!("forward-load:{run}:")))
                .and_then(|id| id.parse().ok())
        })
}

/// Whether every count query was offered, succeeded, and was not dropped by the client.
fn queries_passed(stats: &Accounting, expected: u64) -> bool {
    stats.count("offered_query") == expected
        && stats.count("successful_queries") == expected
        && stats.count("failed_queries") == 0
        && stats.count("client_dropped_query") == 0
}

/// Counts of one kind of event's state words.
#[derive(Default)]
struct StateCounts {
    attempted: u64,
    written: u64,
    acknowledged: u64,
    committed: u64,
    failed: u64,
    confirmed: u64,
    missing_confirmed: u64,
    unresolved: u64,
}

impl StateCounts {
    /// Adds one event's state word to the counts.
    fn add(&mut self, state: u8) {
        self.attempted += u64::from(state & ATTEMPTED != 0);
        self.written += u64::from(state & WRITTEN != 0);
        self.acknowledged += u64::from(state & ACKNOWLEDGED != 0);
        self.committed += u64::from(state & COMMITTED != 0);
        self.failed += u64::from(state & FAILED != 0);
        self.confirmed += u64::from(state & HTTP_CONFIRMED != 0);
        self.missing_confirmed +=
            u64::from(state & (ACKNOWLEDGED | HTTP_CONFIRMED) != 0 && state & COMMITTED == 0);
        self.unresolved += u64::from(state & ATTEMPTED != 0 && state & COMMITTED == 0);
    }
}

/// The accounting object for one kind of event (forward or HTTP) from its state words.
fn event_accounting(states: &[AtomicU8], elapsed: f64, padding_bytes: usize) -> Value {
    let mut counts = StateCounts::default();
    for state in states {
        counts.add(state.load(Ordering::Relaxed));
    }

    json!({
        "planned_events": states.len(),
        "never_submitted": states.len() as u64 - counts.attempted,
        "transport_attempted": counts.attempted,
        "socket_written": counts.written,
        "ack_observed": counts.acknowledged,
        "http_confirmed": counts.confirmed,
        "committed_unique": counts.committed,
        "transport_failed_or_ambiguous": counts.failed,
        "unresolved_after_query": counts.unresolved,
        "confirmed_missing_from_storage": counts.missing_confirmed,
        "committed_events_per_second": counts.committed as f64 / elapsed,
        "committed_payload_mib_per_second":
            counts.committed as f64 * padding_bytes as f64 / elapsed / BYTES_PER_MIB
    })
}

/// The latency report for one operation at one position: 0 scheduled, 1 service, 2 client
/// queue. An operation that never ran reports an empty distribution.
fn latency_report(stats: &Accounting, operation: &str, position: usize) -> Value {
    stats
        .latency
        .get(operation)
        .map(|values| values[position].report())
        .unwrap_or_else(|| Samples::default().report())
}

/// The per-kind accounting objects for forward and HTTP events, from their state words.
fn accounting_by_kind(
    states: &[AtomicU8],
    forward_count: u64,
    elapsed: f64,
    padding_bytes: usize,
) -> Value {
    let split = forward_count as usize;
    json!({
        "forward": event_accounting(&states[..split], elapsed, padding_bytes),
        "http": event_accounting(&states[split..], elapsed, padding_bytes)
    })
}

/// What the run schedules, and the window its queries and verification look at. Decided before
/// anything is sent.
struct Plan {
    window: Window,
    run: String,
    schedules: Vec<Schedule>,
    forward_count: u64,
    http_count: u64,
    query_count: u64,
}

impl Plan {
    fn new(args: &Args) -> Self {
        let lower = crate::now_ms() - LOOKBACK_MS;
        let upper = lower
            + ((args.duration + args.drain_seconds as f64 + WINDOW_TAIL_SECS)
                * MILLIS_PER_SEC as f64) as i64;
        let run = format!("{}-{}", std::process::id(), crate::now_ms());

        let schedules = vec![
            schedule(args, args.rate, FORWARD_QUEUE),
            schedule(args, args.http_rate, HTTP_QUEUE),
            schedule(args, args.query_rate, QUERY_QUEUE),
        ];
        let forward_count = planned(&schedules[FORWARD_QUEUE]);
        let http_count = planned(&schedules[HTTP_QUEUE]);
        let query_count = planned(&schedules[QUERY_QUEUE]);

        Self {
            window: Window { lower, upper },
            run,
            schedules,
            forward_count,
            http_count,
            query_count,
        }
    }
}

/// How many of the persistent sessions opened, the first few errors, and how long it took.
struct Connections {
    opened: usize,
    errors: Vec<String>,
    setup_seconds: f64,
}

/// Opens `count` persistent sessions. The pool keeps the sessions that connected, and `None` for
/// the rest.
async fn open_sessions(
    address: &str,
    count: usize,
) -> (Vec<Mutex<Option<TcpStream>>>, Connections) {
    let started = Instant::now();

    let connections = connect_all(address, count).await;
    let (opened, errors) = summarize_connections(&connections);
    let setup_seconds = started.elapsed().as_secs_f64();
    let sessions = connections
        .into_iter()
        .map(|connection| Mutex::new(connection.ok()))
        .collect();
    (
        sessions,
        Connections {
            opened,
            errors,
            setup_seconds,
        },
    )
}

/// Counts the sessions that opened and keeps the first few connection errors.
fn summarize_connections(connections: &[Result<TcpStream>]) -> (usize, Vec<String>) {
    let opened = connections
        .iter()
        .filter(|connection| connection.is_ok())
        .count();
    let errors = connections
        .iter()
        .filter_map(|connection| connection.as_ref().err().map(ToString::to_string))
        .take(CONNECTION_ERROR_EXAMPLES)
        .collect();
    (opened, errors)
}

/// What the engine measured: its accounting, the elapsed seconds, and the request drain seconds.
struct EngineRun {
    stats: Accounting,
    elapsed: f64,
    request_drain: f64,
}

/// Runs the three schedules through the engine.
async fn run_engine(shared: &Arc<Workload>, schedules: Vec<Schedule>, window: Window) -> EngineRun {
    let args = &shared.args;
    let (stats, elapsed, request_drain) = engine::execute(
        schedules,
        vec![args.connections, args.http_clients, QUERY_CLIENTS],
        vec![
            args.connections.max(MIN_QUEUE_CAPACITY),
            MIN_QUEUE_CAPACITY,
            MIN_QUEUE_CAPACITY,
        ],
        vec!["forward".into(), "http".into(), "query".into()],
        work_for(shared.clone(), window),
        Duration::from_secs(args.drain_seconds),
    )
    .await;

    EngineRun {
        stats,
        elapsed,
        request_drain,
    }
}

/// What happened after the engine stopped: the receiver drain and the storage verification.
struct Drain {
    receiver_drained: bool,
    receiver_drain_seconds: f64,
    after: Value,
    verification: Value,
    verification_seconds: f64,
}

/// Closes the sessions, waits for the receiver to drain, and verifies storage.
async fn drain_receiver(shared: &Workload, window: Window) -> Drain {
    // Closing sessions gives no-ACK runs an observable end-of-stream. Waiting for
    // all receiver sessions to finish also waits for their awaited storage commits.
    close_sessions(&shared.sessions).await;

    let drain_start = Instant::now();
    let (receiver_drained, after) = wait_for_receiver(
        &shared.http,
        drain_start + Duration::from_secs(shared.args.drain_seconds),
    )
    .await;
    let receiver_drain_seconds = drain_start.elapsed().as_secs_f64();

    let verification_start = Instant::now();
    let verification = verification(
        &shared.http,
        &shared.states,
        &shared.run,
        window,
        shared.args.padding_bytes,
    )
    .await;
    let verification_seconds = verification_start.elapsed().as_secs_f64();

    Drain {
        receiver_drained,
        receiver_drain_seconds,
        after,
        verification: match verification {
            Ok(value) => value,
            Err(error) => json!({"error": error.to_string()}),
        },
        verification_seconds,
    }
}

/// Whether the accounting of each kind of event is closed, and whether it is unsaturated: no
/// unresolved, never-submitted or failed event remains.
fn accounting_checks(accounting: &Value, verification: &Value) -> (bool, bool) {
    let accounting_complete = ["forward", "http"]
        .iter()
        .all(|name| accounting[name]["confirmed_missing_from_storage"] == 0)
        && verification["error"].is_null()
        && verification["unexpected_rows"] == 0
        && verification["duplicated_rows"] == 0;
    let unsaturated = ["forward", "http"].iter().all(|name| {
        accounting[name]["unresolved_after_query"] == 0
            && accounting[name]["never_submitted"] == 0
            && accounting[name]["transport_failed_or_ambiguous"] == 0
    });
    (accounting_complete, unsaturated)
}

/// Runs the workload against a receiver at `address` and returns the qualification report.
///
/// # Errors
///
/// Returns an error when the parameters are out of bounds, the HTTP client cannot be built, or the
/// metrics observer cannot be joined.
pub async fn workload(
    base: &str,
    address: &str,
    read: &str,
    write: &str,
    args: &Args,
) -> Result<Value> {
    args.validate()?;
    let http = Http::new(base, read, write)?;
    let Plan {
        window,
        run,
        schedules,
        forward_count,
        http_count,
        query_count,
    } = Plan::new(args);
    let states = Arc::new(
        (0..forward_count + http_count)
            .map(|_| AtomicU8::new(0))
            .collect::<Vec<_>>(),
    );

    let (sessions, connections) = open_sessions(address, args.connections).await;

    let (_, before) = http.request("/metrics", None, true).await;
    let shared = Arc::new(Workload {
        args: args.clone(),
        address: address.into(),
        run: run.clone(),
        padding: "x".repeat(args.padding_bytes),
        http: http.clone(),
        states: states.clone(),
        sessions,
        memory: Arc::new(Semaphore::new(MEMORY_BUDGET_BYTES)),
        http_offset: forward_count,
    });

    let (stop, stopped) = watch::channel(false);
    let observer_start = Instant::now();
    let mut observer = tokio::spawn(observe_metrics(http.clone(), observer_start, stopped));

    let EngineRun {
        stats,
        elapsed,
        request_drain,
    } = run_engine(&shared, schedules, window).await;

    let drain = drain_receiver(&shared, window).await;

    let samples = stop_observer(stop, &mut observer).await?;

    let accounting = accounting_by_kind(&states, forward_count, elapsed, args.padding_bytes);
    Ok(report(
        args,
        Measured {
            run,
            connections,
            elapsed,
            request_drain,
            accounting,
            stats,
            drain,
            before,
            samples,
            query_count,
        },
    ))
}

/// Everything one workload run measured, gathered before the report is assembled.
struct Measured {
    run: String,
    connections: Connections,
    elapsed: f64,
    request_drain: f64,
    accounting: Value,
    stats: Accounting,
    drain: Drain,
    before: Value,
    samples: Vec<Value>,
    query_count: u64,
}

/// The latency reports for every operation that the report names.
fn latency_section(stats: &Accounting) -> Value {
    json!({
        "forward_scheduled": latency_report(stats, "forward", 0),
        "forward_service": latency_report(stats, "forward", 1),
        "forward_client_queue": latency_report(stats, "forward", 2),
        "http_scheduled": latency_report(stats, "http", 0),
        "http_service": latency_report(stats, "http", 1),
        "query_service": latency_report(stats, "query", 1)
    })
}

/// Assembles the qualification report for one workload run.
fn report(args: &Args, measured: Measured) -> Value {
    let Measured {
        run,
        connections,
        elapsed,
        request_drain,
        accounting,
        stats,
        drain,
        before,
        samples,
        query_count,
    } = measured;
    let (accounting_complete, unsaturated) = accounting_checks(&accounting, &drain.verification);
    let query_qualification = queries_passed(&stats, query_count);
    json!({
        "run": run,
        "parameters": args,
        "connections_opened": connections.opened,
        "connection_setup_seconds": connections.setup_seconds,
        "connection_errors": connections.errors,
        "elapsed_seconds": elapsed,
        "request_drain_seconds": request_drain,
        "receiver_drain_seconds": drain.receiver_drain_seconds,
        "receiver_drained": drain.receiver_drained,
        "verification_seconds": drain.verification_seconds,
        "events": accounting,
        "counts": stats.counts,
        "statuses": stats.statuses,
        "verification": drain.verification,
        "latency_ms": latency_section(&stats),
        "scheduler_lag_ms": stats
            .lag
            .iter()
            .map(|(name, samples)| (name, samples.report()))
            .collect::<BTreeMap<_, _>>(),
        "metrics_before": before,
        "metrics_after": drain.after,
        "metrics_samples": samples,
        "worker_errors": stats.worker_errors,
        "failure_examples": stats.failures,
        "correctness_problems": stats.problems,
        "accounting_complete": accounting_complete,
        "unsaturated": unsaturated,
        "queries_passed": query_qualification,
        "passed": accounting_complete
            && unsaturated
            && query_qualification
            && drain.receiver_drained
            && connections.opened == args.connections
            && stats.worker_errors.is_empty(),
        "semantics": concat!(
            "No retries. Socket writes alone do not confirm ingestion. ",
            "Missing attempted IDs remain unresolved; ",
            "transport failures are not asserted to be definite rejection. ",
            "Unique IDs are checked by paginated storage queries after receiver drain."
        )
    })
}

/// The receiver's normalized envelope cap for a batch size.
fn normalized_envelope_cap(batch_size: u64) -> usize {
    if batch_size >= LARGE_BATCH_SIZE {
        LARGE_NORMALIZED_CAP_BYTES
    } else {
        SMALL_NORMALIZED_CAP_BYTES
    }
}

/// The receiver configuration for one run: a single trusted-local listener and route.
fn receiver_config(connections: usize, normalized_cap: usize, address: &str) -> String {
    format!(
        r#"
retention_days=7
max_indexes=1
[storage]
queue_capacity=1024
queue_bytes=134217728
[forward]
enabled=true
max_connections={connections}
max_normalized_bytes={normalized_cap}
[[forward.listeners]]
name="load"
bind="{address}"
security="trusted_local"
principal="load"
keepalive=true
[forward.principals.load]
source="forward-load"
indexes=["default"]
[[forward.routes]]
listener="load"
tag_prefix="load."
index="default"
normalizer="generic"
message_fields=["msg"]
"#
    )
}

/// The lifecycle record for a native run, before the receiver starts.
fn native_lifecycle(args: &Args, normalized_cap: usize) -> Value {
    json!({
        "mode": "native",
        "binary": args.binary,
        "operating_system": std::env::consts::OS,
        "architecture": std::env::consts::ARCH,
        "logical_cpus": std::thread::available_parallelism().map(usize::from).ok(),
        "resource_limits": "native process, no cgroup constraint",
        "normalized_envelope_cap_bytes": normalized_cap,
        "generator_sha256": super::source_sha256()
    })
}

/// Reserves a free loopback port for the receiver and returns its address.
async fn free_local_address() -> Result<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    Ok(listener.local_addr()?.to_string())
}

/// Writes the report for a receiver that never started, and returns its error.
fn fail_to_start(
    args: &Args,
    lifecycle: &Value,
    error: Box<dyn std::error::Error + Send + Sync>,
) -> Result<()> {
    super::write_report(
        &args.output,
        &json!({
            "version": 1,
            "parameters": args,
            "lifecycle": lifecycle,
            "passed": false,
            "error": error.to_string()
        }),
    )?;
    Err(error)
}

/// Runs the Forward workload against a freshly started native receiver, writes the JSON report
/// to `args.output`, and fails when the qualification does not pass.
///
/// # Errors
///
/// Returns an error when the parameters are out of bounds, the receiver cannot start, the report
/// cannot be written, or the qualification fails.
pub async fn run(args: Args) -> Result<()> {
    args.validate()?;

    let address = free_local_address().await?;

    let normalized_cap = normalized_envelope_cap(args.batch_size);
    let config = receiver_config(
        args.connections + FORWARD_CONNECTION_HEADROOM,
        normalized_cap,
        &address,
    );
    let mut lifecycle = native_lifecycle(&args, normalized_cap);
    let mut fixture = match Fixture::start_config(args.binary.clone(), None, &config).await {
        Ok(fixture) => fixture,
        Err(error) => return fail_to_start(&args, &lifecycle, error),
    };
    lifecycle["binary_sha256"] = json!(super::file_sha256(&args.binary)?);

    let result = workload(
        &fixture.base,
        &address,
        &fixture.read_token,
        &fixture.ingest_token,
        &args,
    )
    .await;

    let cleanup = fixture.shutdown().await;
    lifecycle["clean_shutdown"] = json!(cleanup.is_ok());
    if let Err(error) = &cleanup {
        lifecycle["cleanup_error"] = json!(error.to_string());
    }

    let workload = match result {
        Ok(report) => report,
        Err(error) => json!({"passed": false, "error": error.to_string()}),
    };
    let passed = workload["passed"] == true && cleanup.is_ok();
    super::write_report(
        &args.output,
        &json!({
            "version": 1,
            "lifecycle": lifecycle,
            "workload": workload,
            "passed": passed
        }),
    )?;

    runtime::finish(
        if passed {
            Ok(())
        } else {
            Err("Forward qualification failed; inspect the JSON report".into())
        },
        cleanup,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> Args {
        Args {
            binary: "unused".into(),
            duration: 0.1,
            rate: 100.0,
            connections: 100,
            batch_size: 1,
            padding_bytes: 0,
            mode: "forward".into(),
            ack: true,
            http_rate: 0.0,
            query_rate: 0.0,
            http_clients: 1,
            drain_seconds: 1,
            output: "unused".into(),
        }
    }

    #[test]
    fn independent_frame_shapes_have_matching_chunk_and_gzip_options() {
        let mut args = args();
        let job = Job {
            offset: 0.0,
            sequence: 3,
            count: 1,
            queue: 0,
        };
        for mode in ["message", "forward", "packed", "gzip"] {
            args.mode = mode.into();
            let (frame, ack) = frame(&args, "test", &job, 123, "padding").unwrap();
            assert!(frame.windows(7).any(|window| window == b"batch-3"));
            assert_eq!(ack, b"batch-3");
            assert_eq!(frame[0], if mode == "message" { 0x94 } else { 0x93 });
            if mode == "gzip" {
                assert!(frame.windows(2).any(|window| window == [0x1f, 0x8b]));
            }
        }
    }

    #[test]
    fn schedules_and_limits_bound_complete_id_accounting() {
        let mut args = args();
        assert!(args.validate().is_ok());
        assert_eq!(planned(&schedule(&args, 100.0, 0)), 10);

        args.batch_size = 10_000;
        args.padding_bytes = 0;
        assert!(args.validate().is_ok());

        let job = Job {
            offset: 0.0,
            sequence: 0,
            count: 10_000,
            queue: 0,
        };
        assert!(frame(&args, "boundary", &job, 1, "").unwrap().0.len() <= 8 * 1024 * 1024);

        args.batch_size = 10_001;
        assert!(args.validate().is_err());
        args.batch_size = 1;
        args.connections = 10_001;
        assert!(args.validate().is_err());
        args.connections = 100;
        args.padding_bytes = 128 * 1024;
        args.batch_size = 1000;
        assert!(args.validate().is_err());
    }

    #[test]
    fn requested_query_failures_fail_forward_qualification() {
        let mut stats = engine::Accounting::default();
        assert!(queries_passed(&stats, 0));
        stats.add("offered_query", 3);
        stats.add("failed_queries", 3);
        assert!(!queries_passed(&stats, 3));

        let mut healthy = engine::Accounting::default();
        healthy.add("offered_query", 3);
        healthy.add("successful_queries", 3);
        assert!(queries_passed(&healthy, 3));
        healthy.add("client_dropped_query", 1);
        assert!(!queries_passed(&healthy, 3));
    }

    #[tokio::test]
    async fn ack_matches_token_bytes_across_valid_messagepack_header_widths() {
        for marker in [0xa3, 0xd9, 0xda, 0xdb] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut response = vec![0xde, 0, 1];
                for text in [b"ack", b"tok"] {
                    response.push(marker);
                    match marker {
                        0xd9 => response.push(3),
                        0xda => response.extend_from_slice(&3u16.to_be_bytes()),
                        0xdb => response.extend_from_slice(&3u32.to_be_bytes()),
                        _ => {}
                    }

                    response.extend_from_slice(text);
                }

                socket.write_all(&response).await.unwrap();
            });

            let mut socket = TcpStream::connect(address).await.unwrap();
            tokio::time::timeout(Duration::from_secs(1), read_ack(&mut socket, b"tok"))
                .await
                .unwrap()
                .unwrap();
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn lost_ack_is_ambiguous_and_never_retried() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 1024];
            assert!(socket.read(&mut bytes).await.unwrap() > 0);
        });
        let args = args();
        let states = Arc::new(vec![AtomicU8::new(0)]);
        let workload = Workload {
            args,
            address,
            run: "test".into(),
            padding: String::new(),
            http: Http::new("http://localhost", "read", "write").unwrap(),
            states: states.clone(),
            sessions: vec![Mutex::new(None)],
            memory: Arc::new(Semaphore::new(65536)),
            http_offset: 1,
        };
        let completion = workload
            .forward(Job {
                offset: 0.0,
                sequence: 0,
                count: 1,
                queue: 0,
            })
            .await
            .unwrap();
        server.await.unwrap();
        assert_eq!(completion.status, 0);
        assert_eq!(
            states[0].load(Ordering::Relaxed),
            ATTEMPTED | WRITTEN | FAILED
        );
        assert_eq!(completion.counters["transport_attempted_forward"], 1);
        assert!(!completion.counters.contains_key("ack_observed_forward"));
    }
}
