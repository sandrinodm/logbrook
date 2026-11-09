//! Bounded wire types shared by framing, decoding, and transport.
use std::time::Duration;

/// Bits in one byte, for shifting big-endian bytes into an integer.
pub(crate) const BITS_PER_BYTE: usize = 8;

/// Milliseconds in one second, for converting Forward seconds to Logbrook milliseconds.
pub(crate) const MILLIS_PER_SECOND: i64 = 1_000;

/// Nanoseconds in one millisecond, the precision Logbrook stores event times with.
const NANOS_PER_MILLI: u32 = 1_000_000;

/// Nanoseconds in one second. An `EventTime`'s nanosecond field must be below this.
pub(crate) const NANOS_PER_SECOND: u32 = 1_000_000_000;

/// Per-frame, per-message and per-event budgets enforced while framing and decoding.
#[derive(Clone, Debug)]
pub struct CodecLimits {
    /// Largest encoded frame the framer buffers.
    pub max_frame_bytes: usize,
    /// Largest size of one packed payload after decompression.
    pub max_expanded_bytes: usize,
    /// Most events in one message.
    pub max_events: usize,
    /// Largest encoded event, including its metadata.
    pub max_event_bytes: usize,
    /// Deepest nesting of arrays and maps.
    pub max_depth: usize,
    /// Most `MessagePack` values in one frame.
    pub max_values: usize,
    /// Most string and binary bytes in one frame.
    pub max_string_bytes: usize,
    /// Most bytes charged to decoding one frame, counting decoded copies.
    pub max_decoded_bytes: usize,
    /// Longest accepted tag.
    pub max_tag_bytes: usize,
    /// Longest accepted `chunk` option.
    pub max_chunk_bytes: usize,
    /// Most gzip members in one packed payload.
    pub max_gzip_members: usize,
    /// Largest ratio of expanded to compressed bytes; zero disables the check.
    pub max_expansion_ratio: usize,
    /// Wall-clock budget for decoding one frame.
    pub max_decode_time: Duration,
    /// Whether a `chunk` option may be binary instead of a string.
    pub allow_binary_chunk: bool,
}

impl Default for CodecLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 8 * 1024 * 1024,
            max_expanded_bytes: 32 * 1024 * 1024,
            max_events: 10_000,
            max_event_bytes: 256 * 1024,
            max_depth: 32,
            max_values: 1_000_000,
            max_string_bytes: 32 * 1024 * 1024,
            max_decoded_bytes: 128 * 1024 * 1024,
            max_tag_bytes: 1024,
            max_chunk_bytes: 1024,
            max_gzip_members: 128,
            max_expansion_ratio: 1024,
            max_decode_time: Duration::from_secs(5),
            allow_binary_chunk: false,
        }
    }
}

/// Why a Forward frame was rejected.
///
/// `Incomplete` means more bytes are needed. `Invalid` means the bytes are not Forward, and
/// `Limit` means they are Forward but exceed a configured budget. The payload names the check.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProtocolError {
    /// The input ends before one complete `MessagePack` value.
    #[error("incomplete MessagePack value")]
    Incomplete,
    /// The value violates the Forward protocol; its static reason names the check.
    #[error("invalid Forward protocol: {0}")]
    Invalid(&'static str),
    /// The value exceeds a configured budget; its static reason names the budget.
    #[error("Forward limit exceeded: {0}")]
    Limit(&'static str),
}

impl ProtocolError {
    /// A message holds more `MessagePack` values than `max_values` allows.
    pub(crate) fn values_limit() -> Self {
        Self::Limit("values")
    }

    /// A value nests deeper than `max_depth` allows.
    pub(crate) fn depth_limit() -> Self {
        Self::Limit("depth")
    }

    /// A frame's encoded bytes exceed `max_frame_bytes`.
    pub(crate) fn encoded_envelope_limit() -> Self {
        Self::Limit("encoded envelope")
    }

    /// A rendered message exceeds `max_message_bytes`.
    pub(crate) fn message_limit() -> Self {
        Self::Limit("message")
    }

    /// A timestamp's seconds do not fit the `i64` range Logbrook stores.
    pub(crate) fn timestamp_overflow() -> Self {
        Self::Invalid("timestamp overflow")
    }
}

/// A decoded `MessagePack` value.
///
/// Strings and binaries stay as bytes, because a Forward client may send bytes that are not
/// UTF-8. Maps keep their entries in order, including duplicate keys.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// The `MessagePack` nil value.
    Nil,
    /// A `MessagePack` boolean.
    Boolean(bool),
    /// A signed or unsigned `MessagePack` integer represented without loss.
    Integer(i128),
    /// The IEEE 754 bit pattern, so NaN payloads survive decoding.
    Float32(u32),
    /// The IEEE 754 bit pattern, so NaN payloads survive decoding.
    Float64(u64),
    /// String bytes retained without requiring UTF-8.
    String(Vec<u8>),
    /// Binary bytes retained independently of string values.
    Binary(Vec<u8>),
    /// Extension type and exact payload bytes.
    Extension(i8, Vec<u8>),
    /// Ordered array elements.
    Array(Vec<Value>),
    /// Ordered key/value entries, including duplicate keys from the wire.
    Map(Vec<(Value, Value)>),
}

impl Value {
    /// Builds a `MessagePack` string from any byte-like value.
    pub fn string(value: impl AsRef<[u8]>) -> Self {
        Self::String(value.as_ref().to_vec())
    }

    /// The text of a string value, or `None` for other values and for invalid UTF-8.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(v) => std::str::from_utf8(v).ok(),
            _ => None,
        }
    }

    /// The integer of an integer value, or `None` for other values.
    pub fn as_integer(&self) -> Option<i128> {
        match self {
            Self::Integer(v) => Some(*v),
            _ => None,
        }
    }
}

/// A Forward timestamp: whole seconds plus nanoseconds in `0..NANOS_PER_SECOND`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ForwardTime {
    /// Whole seconds since the Unix epoch.
    pub seconds: i64,
    /// Subsecond nanoseconds, strictly below one billion.
    pub nanoseconds: u32,
}

impl ForwardTime {
    /// Whole milliseconds since the Unix epoch. Sub-millisecond nanoseconds are truncated.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::Invalid`] when the result does not fit in `i64`.
    pub fn milliseconds(self) -> Result<i64, ProtocolError> {
        self.seconds
            .checked_mul(MILLIS_PER_SECOND)
            .and_then(|v| v.checked_add(i64::from(self.nanoseconds / NANOS_PER_MILLI)))
            .ok_or_else(ProtocolError::timestamp_overflow)
    }
}

/// The shape of the Forward message that carried an envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForwardMode {
    /// A single event: `[tag, time, record]`.
    Message,
    /// A list of `[time, record]` entries.
    Forward,
    /// A packed payload of concatenated `[time, record]` entries.
    PackedForward,
    /// A packed payload that is gzip-compressed.
    CompressedPackedForward,
}

/// The acknowledgement token from a `chunk` option, echoed back in the ACK.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChunkToken {
    /// String bytes retained without requiring UTF-8.
    String(Vec<u8>),
    /// Binary bytes retained independently of string values.
    Binary(Vec<u8>),
}

/// The options map from the optional last element of a Forward message.
#[derive(Clone, Debug, Default)]
pub struct ForwardOptions {
    /// Optional acknowledgement token echoed after a successful commit.
    pub chunk: Option<ChunkToken>,
    /// Current Fluent Bit logs metadata extension; never enables metrics/traces.
    pub logs_metadata: bool,
}

/// Metadata that Fluent Bit attaches to a logs event, kept with the bytes it arrived as.
#[derive(Clone, Debug)]
pub struct ForwardMetadata {
    /// Decoded metadata without losing map ordering or wire value types.
    pub value: Value,
    /// Exact encoded metadata bytes retained for lossless replay.
    pub original: Vec<u8>,
}

/// One event of a Forward message.
#[derive(Clone, Debug)]
pub struct ForwardEvent {
    /// Validated producer timestamp.
    pub time: ForwardTime,
    /// Decoded record map, retaining duplicate keys and value types.
    pub record: Value,
    /// The record's exact `MessagePack` bytes, kept for replay and for the stored original.
    pub original_record: Vec<u8>,
    /// Optional declared Fluent Bit logs metadata for this event.
    pub metadata: Option<ForwardMetadata>,
}

/// One decoded Forward message: its tag, shape, events and options.
#[derive(Clone, Debug)]
pub struct ForwardEnvelope {
    /// Validated UTF-8 tag used only for configured route selection.
    pub tag: String,
    /// Envelope representation that carried these events.
    pub mode: ForwardMode,
    /// Events in their original wire order.
    pub events: Vec<ForwardEvent>,
    /// Validated per-envelope options.
    pub options: ForwardOptions,
}

/// The result of decoding one complete frame.
#[derive(Clone, Debug)]
pub enum DecodedFrame {
    /// A `nil` frame, which Forward clients send as a keepalive.
    Nil,
    /// A well-formed value that is not an envelope array. It is consumed without being stored.
    Ignored,
    /// One validated envelope ready for route selection and normalization.
    Envelope(ForwardEnvelope),
}
