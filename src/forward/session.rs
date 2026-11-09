//! One outstanding envelope per session; commit ownership belongs to storage.
use super::{
    FRAME_PEAK_MULTIPLIER, READ_SCRATCH_BYTES, Step,
    codec::Framer,
    config::{ForwardConfig, ListenerConfig, SecurityMode},
    decode::{decode_frame, decode_value, encode_ack, encode_value},
    metrics::{self, GaugeGuard},
    normalize::normalize_envelope,
    protocol::{ChunkToken, CodecLimits, DecodedFrame, ForwardEnvelope, ProtocolError},
    routing::{self, ResolvedRoute},
    security::{self, Security},
    until_stopped,
};
use crate::{
    indexes::IndexRegistry,
    ingestion::{self, NormalizedLimits},
    model::{Error, NormalizedEvent},
};
use std::{
    sync::{Arc, atomic::Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{OwnedSemaphorePermit, Semaphore, watch},
    time::{Instant, timeout},
};

/// Largest handshake frame, in bytes. The peer is not authenticated until it has been read.
const HANDSHAKE_MAX_FRAME_BYTES: usize = 16 * 1024;

/// Most `MessagePack` values in one handshake frame, in place of the session's `max_values`.
const HANDSHAKE_MAX_VALUES: usize = 512;

/// Most string and binary bytes in one handshake frame, in place of the session's
/// `max_string_bytes`.
const HANDSHAKE_MAX_STRING_BYTES: usize = 8 * 1024;

/// Most bytes charged to decoding one handshake frame, in place of the session's
/// `max_decoded_bytes`.
const HANDSHAKE_MAX_DECODED_BYTES: usize = 64 * 1024;

/// Every decoded value takes at least this many expanded bytes, which bounds the value count by
/// the expansion limit.
const EXPANDED_BYTES_PER_VALUE: usize = 128;

/// An uncompressed frame is assumed to expand by this factor when its decode is reserved.
const DECODE_EXPANSION_ESTIMATE: usize = 160;

/// The smallest decode reservation for an uncompressed frame, in bytes.
const MIN_DECODE_RESERVATION: usize = 4 * 1024;

/// The two bytes that start every gzip member.
const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

/// Non-array values one session may send before it is closed.
const MAX_IGNORED_VALUES: usize = 8;

/// Longest service, logger or host name an event may carry, in bytes.
const MAX_CANONICAL_FIELD_BYTES: usize = 4 * 1024;

/// The normalizer's metadata depth is the configured depth times this, plus the slack.
const METADATA_DEPTH_SCALE: usize = 4;

/// Levels added to the scaled `max_metadata_depth` to give the normalizer's depth limit.
const METADATA_DEPTH_SLACK: usize = 8;

/// Longest protocol reason copied into a diagnostic.
const MAX_PROTOCOL_REASON_BYTES: usize = 64;

// Messages of the errors that close a session. The session log records each one's reason code,
// named below, rather than the message.

/// A read missed its deadline: the session idle timeout between frames, or the frame idle or
/// total timeout within one (reason `frame_deadline`).
const FRAME_DEADLINE: &str = "Forward frame deadline";

/// The peer closed the connection partway through a frame (reason `incomplete_frame`).
const INCOMPLETE_FRAME: &str = "Forward incomplete frame";

/// The session sent `MAX_IGNORED_VALUES` non-array frames other than the `nil` keepalive (reason
/// `repeated_non_array_values`).
const REPEATED_NON_ARRAY_VALUES: &str = "Forward repeated non-array values";

/// An event's service, logger or host exceeds `MAX_CANONICAL_FIELD_BYTES` (reason
/// `canonical_field_limit`).
const CANONICAL_FIELD_EXCEEDS_LIMIT: &str = "Forward canonical field exceeds limit";

/// The read scratch could not be reserved from the raw budget within `admission_timeout_ms`
/// (reason `raw_budget_deadline`).
const RAW_BUDGET_DEADLINE: &str = "Forward raw budget deadline";

/// The raw budget could not cover a session's next read or frame growth without waiting, which is
/// counted as overload (reason `raw_budget_capacity`).
const RAW_GROWTH_BUDGET_EXHAUSTED: &str = "Forward raw growth budget exhausted";

/// A frame's CPU and decode reservations were not admitted within `admission_timeout_ms`, which
/// is counted as overload (reason `preparation_deadline`).
const PREPARATION_DEADLINE: &str = "Forward preparation deadline";

/// A handshake response or acknowledgement was not written and flushed within
/// `ack_write_timeout_ms` (reason `response_deadline`).
const RESPONSE_DEADLINE: &str = "Forward response deadline";

/// Writing or flushing a response failed (reason `response_write_failed`).
const RESPONSE_FAILED: &str = "Forward response failed";

/// Reading from the socket failed (reason `socket_read_failed`).
const SOCKET_READ_FAILED: &str = "Forward socket read failed";

/// A byte stream a session can read from and write to.
pub trait SessionIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> SessionIo for T {}

/// A connection's byte stream, plain or TLS.
pub type BoxIo = Box<dyn SessionIo>;

/// Process-wide admission budgets shared by every session: raw socket bytes, decode working
/// memory, and concurrent decode jobs.
pub struct Budgets {
    /// Budget for socket buffers and frame-copy peaks, one permit per byte.
    pub raw: Arc<Semaphore>,
    /// Budget for decoded and normalized working memory, one permit per byte.
    pub decoded: Arc<Semaphore>,
    /// Budget for concurrently running blocking decode jobs.
    pub cpu: Arc<Semaphore>,
}

impl Budgets {
    /// Creates the budgets that `config` sizes.
    pub fn new(config: &ForwardConfig) -> Self {
        Self {
            raw: Arc::new(Semaphore::new(config.raw_budget_bytes)),
            decoded: Arc::new(Semaphore::new(config.decoded_budget_bytes)),
            cpu: Arc::new(Semaphore::new(config.max_cpu_jobs)),
        }
    }
}

/// What one connection needs from its listener and from the receiver.
pub(super) struct SessionContext {
    /// The listener the connection arrived on.
    pub listener: Arc<ListenerConfig>,
    /// The listener's authentication settings.
    pub security: Arc<Security>,
    /// The receiver's configuration.
    pub config: Arc<ForwardConfig>,
    /// The indexes that routes write to.
    pub registry: IndexRegistry,
    /// Admission budgets shared by all sessions.
    pub budgets: Arc<Budgets>,
    /// Becomes true when the receiver starts shutting down.
    pub stop: watch::Receiver<bool>,
}

/// Raw socket bytes one session holds in the raw budget. The bytes are returned when it drops.
struct RawBudget {
    permit: Option<OwnedSemaphorePermit>,
    bytes: usize,
}

impl RawBudget {
    fn new() -> Self {
        Self {
            permit: None,
            bytes: 0,
        }
    }

    fn add(&mut self, permit: OwnedSemaphorePermit) {
        let bytes = permit.num_permits();
        self.bytes += bytes;
        metrics::RAW_BYTES.fetch_add(metrics::to_u64(bytes), Ordering::Relaxed);

        if let Some(existing) = &mut self.permit {
            existing.merge(permit);
        } else {
            self.permit = Some(permit);
        }
    }

    fn trim(&mut self, amount: usize) {
        if self.bytes > amount {
            drop(self.take(self.bytes - amount));
        }
    }

    /// Splits off `amount` bytes of the reservation into a new budget.
    fn take(&mut self, amount: usize) -> Self {
        let permit = self
            .permit
            .as_mut()
            .and_then(|permit| permit.split(amount))
            .expect("raw budget covers framed bytes");
        self.bytes -= amount;
        Self {
            permit: Some(permit),
            bytes: amount,
        }
    }
}

impl Drop for RawBudget {
    fn drop(&mut self) {
        metrics::RAW_BYTES.fetch_sub(metrics::to_u64(self.bytes), Ordering::Relaxed);
    }
}

/// Whether the reader is accepting an unauthenticated handshake or session traffic.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Handshake,
    Session,
}

/// Reads and frames one session's input, holding the raw bytes it has buffered.
struct Reader {
    framer: Framer,
    raw: RawBudget,
    partial_since: Option<Instant>,
}

impl Reader {
    fn new(config: &ForwardConfig) -> Self {
        Self {
            framer: Framer::new(codec_limits(config)),
            raw: RawBudget::new(),
            partial_since: None,
        }
    }

    /// Returns the next complete frame, reading as needed. Returns `None` when the peer closed
    /// cleanly or shutdown was requested first.
    async fn frame(
        &mut self,
        io: &mut BoxIo,
        config: &ForwardConfig,
        budgets: &Budgets,
        stop: &mut watch::Receiver<bool>,
        phase: Phase,
    ) -> Result<Option<Vec<u8>>, Error> {
        loop {
            if *stop.borrow() {
                return Ok(None);
            }

            if let Some(frame) = self.framer.next_frame().map_err(protocol_error)? {
                self.partial_since = (self.framer.buffered_len() > 0).then(Instant::now);
                return Ok(Some(frame));
            }

            if phase == Phase::Handshake && self.framer.buffered_len() > HANDSHAKE_MAX_FRAME_BYTES {
                return Err(Error::invalid("Forward handshake exceeds limit"));
            }

            let mut buffer = [0u8; READ_SCRATCH_BYTES];
            let read_limit = config
                .max_frame_bytes
                .saturating_sub(self.framer.buffered_len())
                .min(buffer.len());
            if read_limit == 0 {
                return Err(Error::invalid("Forward encoded frame limit"));
            }

            // A fixed scratch reservation precedes each read. Before the frame buffer grows,
            // the predicted capacity, reallocation and frame-copy peak is reserved separately.
            let scratch = match self
                .reserve_scratch(
                    budgets,
                    Duration::from_millis(config.admission_timeout_ms),
                    stop,
                )
                .await?
            {
                Step::Ready(permit) => permit,
                Step::Stopped => return Ok(None),
            };

            let scratch_gauge = GaugeGuard::new(&metrics::RAW_BYTES, READ_SCRATCH_BYTES);
            let deadline = self.read_deadline(config);
            let read = io.read(&mut buffer[..read_limit]);
            let count = match until_stopped(stop, tokio::time::timeout_at(deadline, read)).await {
                Step::Ready(result) => result
                    .map_err(|error| {
                        Error::unavailable(FRAME_DEADLINE)
                            .with_reason("frame_deadline")
                            .with_source(error)
                    })?
                    .map_err(|error| {
                        Error::invalid(SOCKET_READ_FAILED)
                            .with_reason("socket_read_failed")
                            .with_source(error)
                    })?,
                Step::Stopped => return Ok(None),
            };
            if count == 0 {
                return if self.framer.buffered_len() == 0 {
                    Ok(None)
                } else {
                    Err(Error::invalid(INCOMPLETE_FRAME).with_reason("incomplete_frame"))
                };
            }

            self.reserve_growth(budgets, count)?;
            self.partial_since.get_or_insert_with(Instant::now);
            self.framer.push(&buffer[..count]).map_err(protocol_error)?;

            drop(scratch);
            drop(scratch_gauge);
            self.release_surplus();
        }
    }

    /// Reserves the scratch bytes for the next read. A session that already holds raw bytes
    /// takes them without waiting, so it never waits while holding memory.
    async fn reserve_scratch(
        &self,
        budgets: &Budgets,
        deadline: Duration,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<Step<OwnedSemaphorePermit>, Error> {
        if self.raw.bytes > 0 {
            return Arc::clone(&budgets.raw)
                .try_acquire_many_owned(scratch_permits())
                .map(Step::Ready)
                .map_err(|error| growth_exhausted().with_source(error));
        }

        let wait = Arc::clone(&budgets.raw).acquire_many_owned(scratch_permits());
        let admission = timeout(deadline, wait);
        match until_stopped(stop, admission).await {
            Step::Ready(result) => {
                let permit = result
                    .map_err(|error| {
                        Error::unavailable(RAW_BUDGET_DEADLINE)
                            .with_reason("raw_budget_deadline")
                            .with_source(error)
                    })?
                    .map_err(|error| {
                        Error::unavailable("Forward raw budget closed").with_source(error)
                    })?;
                Ok(Step::Ready(permit))
            }
            Step::Stopped => Ok(Step::Stopped),
        }
    }

    /// Grows this session's raw reservation to cover the predicted peak after `count` more bytes.
    fn reserve_growth(&mut self, budgets: &Budgets, count: usize) -> Result<(), Error> {
        let peak = self
            .framer
            .capacity_after_push(count)
            .saturating_mul(FRAME_PEAK_MULTIPLIER);
        if peak > self.raw.bytes {
            let additional = u32::try_from(peak - self.raw.bytes)
                .map_err(|error| growth_exhausted().with_source(error))?;
            let permit = Arc::clone(&budgets.raw)
                .try_acquire_many_owned(additional)
                .map_err(|error| growth_exhausted().with_source(error))?;
            self.raw.add(permit);
        }

        Ok(())
    }

    /// The read deadline: the frame's idle and total limits once a frame has started, otherwise
    /// the session's idle limit.
    fn read_deadline(&self, config: &ForwardConfig) -> Instant {
        match self.partial_since {
            Some(start) => std::cmp::min(
                Instant::now() + Duration::from_millis(config.frame_idle_timeout_ms),
                start + Duration::from_millis(config.frame_total_timeout_ms),
            ),
            None => Instant::now() + Duration::from_millis(config.session_idle_timeout_ms),
        }
    }

    /// Moves the reservation for a frame's bytes into the caller's hands, releasing whatever
    /// the reader does not need.
    fn take_frame_budget(&mut self, frame_len: usize) -> RawBudget {
        let frame = self.raw.take(frame_len);
        self.release_surplus();
        self.completed();
        frame
    }

    /// Releases reserved bytes beyond what the buffered input and its allocation need.
    fn release_surplus(&mut self) {
        let needed = self
            .framer
            .allocated_bytes()
            .saturating_mul(2)
            .saturating_add(self.framer.buffered_len());
        self.raw.trim(needed);
    }

    /// Releases the whole reservation once no input is buffered.
    fn completed(&mut self) {
        if self.framer.buffered_len() == 0 {
            self.raw = RawBudget::new();
        }
    }
}

/// One connection being served: its reader, the principal it authenticated as, and how many
/// non-envelope values it has sent.
struct Session {
    context: SessionContext,
    reader: Reader,
    principal: Option<String>,
    ignored: usize,
}

/// Whether the session reads another frame.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Flow {
    Continue,
    Stop,
}

/// The permits a frame holds while it waits for a decode slot.
struct Admission {
    /// A CPU job slot.
    cpu: OwnedSemaphorePermit,
    /// The decode reservation for the frame.
    decoded: OwnedSemaphorePermit,
}

/// What decoding one frame produced.
enum Prepared {
    /// A nil frame, which clients send as a keepalive. Nothing to commit.
    Keepalive,
    /// A value that is not an envelope. It is consumed without being stored.
    Ignored,
    /// An envelope, routed and normalized, ready to commit.
    Envelope(Box<Batch>),
}

/// A normalized envelope, the route that chose its destination, and the acknowledgement it asked
/// for.
struct Batch {
    route: ResolvedRoute,
    events: Vec<NormalizedEvent>,
    chunk: Option<ChunkToken>,
}

/// What one decode worker owns while it runs.
struct DecodeJob {
    frame: Vec<u8>,
    /// The raw bytes of the frame. Never read: it is held until the job ends.
    _raw: RawBudget,
    /// The CPU admission. Never read: it is held until the job ends.
    _cpu: OwnedSemaphorePermit,
    /// The decode reservation. It is returned with the result and held until the frame is
    /// committed.
    decoded: OwnedSemaphorePermit,
    /// The decoded-bytes gauge, returned with the result for the same reason.
    decoded_bytes: GaugeGuard,
    decode_limit: usize,
    normalized_limit: usize,
    config: Arc<ForwardConfig>,
    registry: IndexRegistry,
    listener: String,
    principal: String,
}

/// The result of a decode worker: the work to do, and the permits that stay alive until the
/// frame is committed.
struct Decoded {
    prepared: Prepared,
    decoded: OwnedSemaphorePermit,
    decoded_bytes: GaugeGuard,
}

impl Session {
    fn new(context: SessionContext, principal: Option<String>) -> Self {
        let reader = Reader::new(&context.config);
        Self {
            context,
            reader,
            principal,
            ignored: 0,
        }
    }

    /// Serves frames until the peer leaves, shutdown is requested, or a frame ends the session.
    async fn serve(mut self, io: &mut BoxIo) -> Result<(), Error> {
        if self.context.security.mode == SecurityMode::SharedKey {
            self.authenticate(io).await?;
            self.reader.completed();
        }

        let principal = self
            .principal
            .take()
            .ok_or_else(|| Error::invalid("Forward identity unavailable"))?;
        while let Some(frame) = self.next_frame(io, Phase::Session).await? {
            if self.handle_frame(io, frame, &principal).await? == Flow::Stop {
                break;
            }
        }

        Ok(())
    }

    async fn next_frame(&mut self, io: &mut BoxIo, phase: Phase) -> Result<Option<Vec<u8>>, Error> {
        self.reader
            .frame(
                io,
                &self.context.config,
                &self.context.budgets,
                &mut self.context.stop,
                phase,
            )
            .await
    }

    /// Runs the shared-key handshake under its deadline and records the identity it grants.
    async fn authenticate(&mut self, io: &mut BoxIo) -> Result<(), Error> {
        let deadline = Duration::from_millis(self.context.config.handshake_timeout_ms);
        let outcome = timeout(deadline, self.shared_key_handshake(io)).await;
        match outcome {
            Ok(Ok(identity)) => {
                self.principal = Some(identity);
                Ok(())
            }
            Ok(Err(error)) => {
                metrics::increment(&metrics::AUTH_ERRORS);
                Err(security::authentication_failed().with_source(error))
            }
            Err(error) => {
                metrics::increment(&metrics::AUTH_ERRORS);
                Err(security::authentication_failed().with_source(error))
            }
        }
    }

    /// Sends the challenge, checks the client's PING and answers with a PONG. Returns the
    /// identity the PING proved.
    async fn shared_key_handshake(&mut self, io: &mut BoxIo) -> Result<String, Error> {
        let challenge = self
            .context
            .security
            .challenge(self.context.listener.keepalive)?;
        let timeout = Duration::from_millis(self.context.config.ack_write_timeout_ms);
        write(io, &encode_value(&challenge.helo()), timeout).await?;

        let frame = self
            .next_frame(io, Phase::Handshake)
            .await?
            .ok_or_else(|| Error::invalid("Forward handshake closed"))?;

        let limits = handshake_limits(&self.context.config);
        let ping = decode_value(&frame, &limits).map_err(protocol_error)?;
        match self.context.security.authenticate(&challenge, &ping) {
            Ok((identity, pong)) => {
                write(io, &encode_value(&pong), timeout).await?;
                Ok(identity)
            }
            Err(error) => {
                drop(write(io, &encode_value(&security::failure()), timeout).await);
                Err(error)
            }
        }
    }

    /// Decodes one frame on a worker, then commits and acknowledges its batch if it has one.
    async fn handle_frame(
        &mut self,
        io: &mut BoxIo,
        frame: Vec<u8>,
        principal: &str,
    ) -> Result<Flow, Error> {
        let frame_raw = self.reader.take_frame_budget(frame.len());

        let PreparationLimits {
            decoded: decode_limit,
            normalized: normalized_limit,
            reservation,
        } = preparation_limits(&self.context.config, &frame);

        let preparation_timer = metrics::Timer::new(metrics::Stage::Preparation);
        let admission = timeout(
            Duration::from_millis(self.context.config.admission_timeout_ms),
            admit_preparation(&self.context.budgets, reservation),
        );
        let Admission { cpu, decoded } =
            match until_stopped(&mut self.context.stop, admission).await {
                Step::Ready(result) => result.map_err(|error| {
                    metrics::increment(&metrics::OVERLOAD);
                    Error::unavailable(PREPARATION_DEADLINE)
                        .with_reason("preparation_deadline")
                        .with_source(error)
                })??,
                Step::Stopped => return Ok(Flow::Stop),
            };
        drop(preparation_timer);

        let job = DecodeJob {
            frame,
            _raw: frame_raw,
            _cpu: cpu,
            decoded,
            decoded_bytes: GaugeGuard::new(&metrics::DECODED_BYTES, reservation),
            decode_limit,
            normalized_limit,
            config: Arc::clone(&self.context.config),
            registry: self.context.registry.clone(),
            listener: self.context.listener.name.clone(),
            principal: principal.to_owned(),
        };
        // A cancelled receiver drops its JoinHandle, but the job keeps every reservation it holds.
        let mut task = tokio::task::spawn_blocking(move || run_decode(job));
        let Decoded {
            prepared,
            decoded,
            decoded_bytes: _decoded_bytes,
        } = match until_stopped(&mut self.context.stop, &mut task).await {
            Step::Ready(joined) => joined.map_err(|error| {
                Error::unavailable("Forward decode worker failed").with_source(error)
            })??,
            Step::Stopped => return Ok(Flow::Stop),
        };

        // No await between this test and entering shared append admission; storage's synchronized
        // gate decides races with stop_admission and index deletion.
        if *self.context.stop.borrow() {
            return Ok(Flow::Stop);
        }

        let flow = match prepared {
            Prepared::Keepalive => Flow::Continue,
            Prepared::Ignored => {
                self.count_ignored()?;
                Flow::Continue
            }
            Prepared::Envelope(batch) => self.deliver(io, *batch, decoded).await?,
        };
        if flow == Flow::Continue {
            self.reader.completed();
        }

        Ok(flow)
    }

    fn count_ignored(&mut self) -> Result<(), Error> {
        metrics::increment(&metrics::IGNORED);
        self.ignored += 1;
        if self.ignored >= MAX_IGNORED_VALUES {
            return Err(
                Error::invalid(REPEATED_NON_ARRAY_VALUES).with_reason("repeated_non_array_values")
            );
        }

        Ok(())
    }

    /// Commits a batch, then acknowledges it if the client asked. Returns `Stop` when the
    /// connection is not kept alive.
    async fn deliver(
        &self,
        io: &mut BoxIo,
        batch: Batch,
        decoded: OwnedSemaphorePermit,
    ) -> Result<Flow, Error> {
        let Batch {
            route,
            events,
            chunk,
        } = batch;
        if chunk.is_some() {
            metrics::increment(&metrics::ACK_REQUESTED);
        }

        self.commit(route, events, decoded).await?;
        if let Some(chunk) = &chunk {
            self.acknowledge(io, chunk).await?;
        }

        Ok(if self.context.listener.keepalive {
            Flow::Continue
        } else {
            Flow::Stop
        })
    }

    /// Appends a batch to its destination. A batch with no events only releases its decode
    /// reservation.
    async fn commit(
        &self,
        route: ResolvedRoute,
        events: Vec<NormalizedEvent>,
        decoded: OwnedSemaphorePermit,
    ) -> Result<(), Error> {
        if events.is_empty() {
            metrics::increment(&metrics::EMPTY);
            drop(decoded);
            return Ok(());
        }

        let count = events.len();
        let limits = self.normalized_limits();
        match ingestion::append_live(&route.destination, route.source, events, decoded, limits)
            .await
        {
            Ok(_) => {
                metrics::COMMITTED.fetch_add(metrics::to_u64(count), Ordering::Relaxed);
                Ok(())
            }
            Err(error) => {
                metrics::increment(&metrics::STORAGE_ERRORS);
                Err(error)
            }
        }
    }

    fn normalized_limits(&self) -> NormalizedLimits {
        let config = &self.context.config;
        NormalizedLimits {
            max_events: config.max_events_per_frame,
            max_message_bytes: config.max_message_bytes,
            max_field_bytes: config.max_normalized_bytes,
            max_metadata_depth: config
                .max_metadata_depth
                .saturating_mul(METADATA_DEPTH_SCALE)
                .saturating_add(METADATA_DEPTH_SLACK),
            max_event_bytes: config.max_normalized_bytes,
        }
    }

    /// Writes the acknowledgement for `chunk`, counting the outcome.
    async fn acknowledge(&self, io: &mut BoxIo, chunk: &ChunkToken) -> Result<(), Error> {
        let _ack_timer = metrics::Timer::new(metrics::Stage::Ack);
        if let Err(error) = write(
            io,
            &encode_ack(chunk),
            Duration::from_millis(self.context.config.ack_write_timeout_ms),
        )
        .await
        {
            metrics::increment(&metrics::ACK_FAILED);
            return Err(error);
        }

        metrics::increment(&metrics::ACK_SENT);
        Ok(())
    }
}

/// Serves one connection after its transport is ready and its principal is known. Closes the
/// transport when the session ends cleanly.
pub(super) async fn run(
    mut io: BoxIo,
    principal: Option<String>,
    context: SessionContext,
) -> Result<(), Error> {
    let close_timeout = Duration::from_millis(context.config.ack_write_timeout_ms);
    let result = Session::new(context, principal).serve(&mut io).await;
    if result.is_ok() {
        // TLS shutdown sends close_notify and flushes it; it does not wait for the peer's
        // reciprocal alert. The supervisor still owns the overall drain.
        close(&mut io, close_timeout).await;
    }

    result
}

/// Waits for a CPU permit and the decode reservation for one frame.
async fn admit_preparation(budgets: &Budgets, reservation: usize) -> Result<Admission, Error> {
    let reservation = u32::try_from(reservation).map_err(decode_budget_closed)?;
    let cpu = Arc::clone(&budgets.cpu)
        .acquire_owned()
        .await
        .map_err(|error| Error::unavailable("Forward CPU admission closed").with_source(error))?;
    let decoded = Arc::clone(&budgets.decoded)
        .acquire_many_owned(reservation)
        .await
        .map_err(decode_budget_closed)?;
    Ok(Admission { cpu, decoded })
}

/// The error for a decode reservation that could not be taken from the decode budget.
fn decode_budget_closed(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::unavailable("Forward decode budget closed").with_source(error)
}

/// Decodes and routes one frame on a blocking worker.
fn run_decode(job: DecodeJob) -> Result<Decoded, Error> {
    let prepared = decode_prepared(&job)?;
    Ok(Decoded {
        prepared,
        decoded: job.decoded,
        decoded_bytes: job.decoded_bytes,
    })
}

fn decode_prepared(job: &DecodeJob) -> Result<Prepared, Error> {
    let _cpu_gauge = GaugeGuard::new(&metrics::CPU_JOBS, 1);
    let _decode_timer = metrics::Timer::new(metrics::Stage::Decode);

    let mut limits = codec_limits(&job.config);
    limits.max_decoded_bytes = job.decode_limit;
    match decode_frame(&job.frame, &limits).map_err(protocol_error)? {
        DecodedFrame::Nil => Ok(Prepared::Keepalive),
        DecodedFrame::Ignored => Ok(Prepared::Ignored),
        DecodedFrame::Envelope(envelope) => {
            prepare_envelope(job, envelope).map(|batch| Prepared::Envelope(Box::new(batch)))
        }
    }
}

/// Routes and normalizes one envelope, and counts what it carries.
fn prepare_envelope(job: &DecodeJob, envelope: ForwardEnvelope) -> Result<Batch, Error> {
    metrics::increment(&metrics::ENVELOPES);
    metrics::EVENTS.fetch_add(metrics::to_u64(envelope.events.len()), Ordering::Relaxed);

    let received_at = unix_millis_now()?;
    let mut route = routing::resolve(
        &job.config,
        &job.registry,
        &job.listener,
        &job.principal,
        &envelope.tag,
        received_at,
    )
    .inspect_err(|_| metrics::increment(&metrics::ROUTE_ERRORS))?;
    route.options.max_normalized_bytes = job.normalized_limit;

    let events = normalize_envelope(&envelope, &route.options).map_err(protocol_error)?;
    count_events(&events)?;

    Ok(Batch {
        route,
        events,
        chunk: envelope.options.chunk,
    })
}

/// Rejects an event whose canonical field is too long, and counts the noteworthy encodings of
/// the events it accepts.
fn count_events(events: &[NormalizedEvent]) -> Result<(), Error> {
    for event in events {
        if [&event.service, &event.logger, &event.host]
            .into_iter()
            .flatten()
            .any(|field| field.len() > MAX_CANONICAL_FIELD_BYTES)
        {
            return Err(
                Error::invalid(CANONICAL_FIELD_EXCEEDS_LIMIT).with_reason("canonical_field_limit")
            );
        }

        if event.attributes["forward"]["record_encoding"] == "msgpack_typed_v1" {
            metrics::increment(&metrics::BINARY_PRESERVED);
        }
        if event.attributes["logbrook"]["docker_status"] == "generic_fallback" {
            metrics::increment(&metrics::FALLBACKS);
        }
        if event.attributes["docker"].get("fragment").is_some() {
            metrics::increment(&metrics::DOCKER_FRAGMENTS);
        }
    }

    Ok(())
}

/// The current time in milliseconds since the Unix epoch, used as the receipt time.
fn unix_millis_now() -> Result<i64, Error> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::invalid("system time before Unix epoch").with_source(error))?;
    // Saturates only after the year 292 million, which no clock reaches.
    Ok(i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
}

async fn close(io: &mut BoxIo, deadline: Duration) {
    drop(timeout(deadline, io.shutdown()).await);
}

/// Writes and flushes `bytes` under one deadline. TLS and other buffered transports may accept
/// plaintext before the ciphertext reaches the socket, so success requires the flush too.
async fn write(io: &mut BoxIo, bytes: &[u8], deadline: Duration) -> Result<(), Error> {
    timeout(deadline, async {
        io.write_all(bytes).await?;
        io.flush().await
    })
    .await
    .map_err(|error| {
        Error::unavailable(RESPONSE_DEADLINE)
            .with_reason("response_deadline")
            .with_source(error)
    })?
    .map_err(|error| {
        Error::unavailable(RESPONSE_FAILED)
            .with_reason("response_write_failed")
            .with_source(error)
    })
}

/// Counts a protocol failure and returns it as a session error whose reason names the failure.
fn protocol_error(error: ProtocolError) -> Error {
    metrics::increment(&metrics::PROTOCOL_ERRORS);

    let (kind, reason) = match error {
        ProtocolError::Incomplete => ("incomplete", "input"),
        ProtocolError::Invalid(reason) => ("invalid", reason),
        ProtocolError::Limit(reason) => ("limit", reason),
    };
    // Protocol reasons are &'static str owned by our parser, never producer content.
    let code = format!("{kind}_{}", reason_token(reason));
    Error::invalid(format!("forward_protocol:{code}"))
        .with_reason(code)
        .with_source(error)
}

/// Lowercases ASCII letters and maps every other byte to `_`, keeping at most
/// `MAX_PROTOCOL_REASON_BYTES` bytes.
fn reason_token(reason: &str) -> String {
    reason
        .bytes()
        .take(MAX_PROTOCOL_REASON_BYTES)
        .map(|byte| {
            if byte.is_ascii_alphanumeric() {
                char::from(byte.to_ascii_lowercase())
            } else {
                '_'
            }
        })
        .collect()
}

/// The error for raw memory that cannot grow. Counted as overload.
fn growth_exhausted() -> Error {
    metrics::increment(&metrics::OVERLOAD);
    Error::unavailable(RAW_GROWTH_BUDGET_EXHAUSTED).with_reason("raw_budget_capacity")
}

/// The read scratch reservation as semaphore permits, one per byte.
fn scratch_permits() -> u32 {
    u32::try_from(READ_SCRATCH_BYTES).expect("read scratch fits in a permit count")
}

/// The decoder limits for a session on this configuration.
pub fn codec_limits(config: &ForwardConfig) -> CodecLimits {
    CodecLimits {
        max_frame_bytes: config.max_frame_bytes,
        max_expanded_bytes: config.max_expanded_bytes,
        max_events: config.max_events_per_frame,
        max_event_bytes: config.max_event_bytes,
        max_depth: config.max_metadata_depth,
        max_values: config.max_expanded_bytes / EXPANDED_BYTES_PER_VALUE,
        max_string_bytes: config.max_expanded_bytes,
        max_decoded_bytes: config.max_expanded_bytes,
        max_tag_bytes: config.max_tag_bytes,
        max_chunk_bytes: config.max_chunk_bytes,
        max_gzip_members: config.max_gzip_members,
        max_expansion_ratio: config.max_expansion_ratio,
        max_decode_time: Duration::from_millis(config.decode_timeout_ms),
        allow_binary_chunk: config.allow_binary_chunk,
    }
}

/// The decoder limits for a handshake frame: far smaller than a session's, since the peer has
/// not authenticated yet.
fn handshake_limits(config: &ForwardConfig) -> CodecLimits {
    let mut limits = codec_limits(config);
    limits.max_frame_bytes = HANDSHAKE_MAX_FRAME_BYTES;
    limits.max_values = HANDSHAKE_MAX_VALUES;
    limits.max_string_bytes = HANDSHAKE_MAX_STRING_BYTES;
    limits.max_decoded_bytes = HANDSHAKE_MAX_DECODED_BYTES;
    limits
}

/// Sizes a frame's decode before it runs: the decoded-output limit, the normalized-output limit,
/// and the decode reservation that covers both together with the expanded payload.
struct PreparationLimits {
    decoded: usize,
    normalized: usize,
    reservation: usize,
}

fn preparation_limits(config: &ForwardConfig, frame: &[u8]) -> PreparationLimits {
    // Every valid gzip member starts with this signature. False positives only reserve more;
    // the decoder's granted allocation cap remains authoritative for every input.
    let gzip = frame
        .windows(GZIP_MAGIC.len())
        .any(|bytes| bytes == GZIP_MAGIC);
    let decoded = if gzip {
        config.max_expanded_bytes
    } else {
        frame
            .len()
            .saturating_mul(DECODE_EXPANSION_ESTIMATE)
            .max(MIN_DECODE_RESERVATION)
            .min(config.max_expanded_bytes)
    };
    let normalized = decoded.saturating_mul(2).min(config.max_normalized_bytes);
    PreparationLimits {
        decoded,
        normalized,
        reservation: decoded.saturating_mul(2).saturating_add(normalized),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn normal_shutdown_is_bounded_when_a_buffered_transport_stalls() {
        let (transport, _unread_peer) = tokio::io::duplex(1);
        let mut io: BoxIo = Box::new(tokio::io::BufWriter::with_capacity(1024, transport));
        io.write_all(b"buffered-close-notification").await.unwrap();

        timeout(
            Duration::from_secs(1),
            close(&mut io, Duration::from_millis(20)),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn response_success_flushes_a_buffered_transport_before_returning() {
        let (transport, mut peer) = tokio::io::duplex(64);
        let mut io: BoxIo = Box::new(tokio::io::BufWriter::with_capacity(1024, transport));
        let response = b"committed-ack";
        write(&mut io, response, Duration::from_millis(1000))
            .await
            .unwrap();

        let mut received = vec![0; response.len()];
        timeout(Duration::from_secs(1), peer.read_exact(&mut received))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(received, response);
    }

    #[tokio::test]
    async fn stalled_transport_flush_observes_the_same_response_deadline() {
        // write_all fits in BufWriter, but its flush stalls once the tiny peer
        // buffer fills. A deadline on write_all alone would incorrectly succeed.
        let (transport, _unread_peer) = tokio::io::duplex(1);
        let mut io: BoxIo = Box::new(tokio::io::BufWriter::with_capacity(1024, transport));
        let error = timeout(
            Duration::from_secs(1),
            write(&mut io, b"committed-ack", Duration::from_millis(20)),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.message, "Forward response deadline");
    }

    #[tokio::test]
    async fn raw_frame_ownership_outlives_session_and_releases_every_permit() {
        let budget = Arc::new(Semaphore::new(3072));
        let mut session = RawBudget::new();
        session.add(budget.clone().acquire_many_owned(3072).await.unwrap());
        let frame = session.take(1024);
        drop(session);
        assert_eq!(budget.available_permits(), 2048);
        drop(frame);
        assert_eq!(budget.available_permits(), 3072);
    }

    fn release_frame(reader: &mut Reader, frame: &[u8]) {
        let owned = reader.raw.take(frame.len());
        reader.raw.trim(
            reader
                .framer
                .allocated_bytes()
                .saturating_mul(2)
                .saturating_add(reader.framer.buffered_len()),
        );
        reader.completed();
        drop(owned);
    }

    #[tokio::test]
    async fn perpetual_partial_next_frame_releases_surplus_and_gets_a_fresh_deadline() {
        let config = ForwardConfig {
            max_frame_bytes: 1024,
            raw_budget_bytes: 4096,
            frame_idle_timeout_ms: 20,
            frame_total_timeout_ms: 30,
            ..ForwardConfig::default()
        };
        let budgets = Budgets::new(&config);
        let mut reader = Reader::new(&config);
        let (mut sender, receiver) = tokio::io::duplex(64);
        let mut io: BoxIo = Box::new(receiver);
        let (_stop, mut stop) = watch::channel(false);
        sender.write_all(&[0xa3]).await.unwrap();
        for _ in 0..100 {
            sender.write_all(&[b'a', b'b', b'c', 0xa3]).await.unwrap();

            let frame = reader
                .frame(&mut io, &config, &budgets, &mut stop, Phase::Session)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(frame, [0xa3, b'a', b'b', b'c']);
            release_frame(&mut reader, &frame);

            assert!(
                budgets.raw.available_permits() > config.raw_budget_bytes - 64,
                "completed prefixes must not accumulate reservations"
            );

            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        drop(reader);
        assert_eq!(budgets.raw.available_permits(), config.raw_budget_bytes);
    }

    #[tokio::test]
    async fn maximum_frame_split_with_next_prefix_fits_minimum_raw_budget() {
        let config = ForwardConfig {
            max_frame_bytes: 1024,
            raw_budget_bytes: 3 * 1024 + 1024,
            ..ForwardConfig::default()
        };
        let budgets = Budgets::new(&config);
        let mut reader = Reader::new(&config);
        let (mut sender, receiver) = tokio::io::duplex(4096);
        let mut io: BoxIo = Box::new(receiver);
        let (_stop, mut stop) = watch::channel(false);
        let mut wire = vec![0xda, 0x03, 0xfd];
        wire.extend(vec![b'x'; 1021]);
        let expected = wire.clone();
        let first = wire[..1020].to_vec();
        let rest = wire[1020..].to_vec();
        let sending = tokio::spawn(async move {
            sender.write_all(&first).await.unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut combined = rest;
            combined.extend([0xa3, b'a', b'b', b'c']);
            sender.write_all(&combined).await.unwrap();
            sender
        });

        let frame = reader
            .frame(&mut io, &config, &budgets, &mut stop, Phase::Session)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame, expected);
        release_frame(&mut reader, &frame);

        let frame = reader
            .frame(&mut io, &config, &budgets, &mut stop, Phase::Session)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame, [0xa3, b'a', b'b', b'c']);
        release_frame(&mut reader, &frame);

        drop(sending.await.unwrap());
        drop(reader);
        assert_eq!(budgets.raw.available_permits(), config.raw_budget_bytes);
    }

    #[test]
    fn preparation_capacity_is_frame_aware_and_gzip_still_bounded() {
        let config = ForwardConfig::default();
        let small = preparation_limits(&config, &[0xc0]).reservation;
        let compressed = preparation_limits(&config, &[0x1f, 0x8b]).reservation;
        assert!(small < 32 * 1024);
        assert_eq!(compressed, config.decode_reservation());
        assert!(compressed <= config.decoded_budget_bytes);

        let small_config = ForwardConfig {
            max_frame_bytes: 1024,
            max_expanded_bytes: 1024,
            max_event_bytes: 1024,
            max_normalized_bytes: 1024,
            ..ForwardConfig::default()
        };
        let PreparationLimits {
            decoded,
            normalized,
            reservation,
        } = preparation_limits(&small_config, &[0xc0]);
        assert_eq!(decoded, 1024);
        assert_eq!(normalized, 1024);
        assert_eq!(reservation, 3072);
    }

    #[tokio::test]
    async fn permit_reservations_above_u32_max_fail_without_truncating() {
        let config = ForwardConfig {
            max_frame_bytes: (u32::MAX as usize) / 2 + 1,
            ..ForwardConfig::default()
        };
        let budgets = Budgets::new(&config);
        let mut reader = Reader::new(&config);
        let err = reader
            .reserve_growth(&budgets, (u32::MAX as usize) / 2 + 1)
            .unwrap_err();
        assert_eq!(err.reason.as_deref(), Some("raw_budget_capacity"));
        assert!(err.source.is_some());

        let err = match admit_preparation(&budgets, (u32::MAX as usize) + 1).await {
            Ok(_) => panic!("expected admit_preparation to fail"),
            Err(err) => err,
        };
        assert_eq!(err.message, "Forward decode budget closed");
        assert!(err.source.is_some());

        assert_eq!(budgets.cpu.available_permits(), config.max_cpu_jobs);
        assert_eq!(
            budgets.decoded.available_permits(),
            config.decoded_budget_bytes
        );
    }

    #[tokio::test]
    async fn cancelled_decode_waiter_does_not_free_a_running_cpu_job() {
        let capacity = Arc::new(Semaphore::new(1));
        let permit = capacity.clone().acquire_owned().await.unwrap();
        let started = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        let job_started = started.clone();
        let job_release = release.clone();
        let job = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            job_started.wait();
            job_release.wait();
        });
        started.wait();
        drop(job);
        assert_eq!(capacity.available_permits(), 0);

        release.wait();

        timeout(Duration::from_secs(1), async {
            while capacity.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
