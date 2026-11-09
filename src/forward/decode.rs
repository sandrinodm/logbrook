//! Envelope parsing deliberately owns limits rather than deserializing an arbitrary tree.
use super::{
    codec::header,
    protocol::{
        BITS_PER_BYTE, ChunkToken, CodecLimits, DecodedFrame, ForwardEnvelope, ForwardEvent,
        ForwardMetadata, ForwardMode, ForwardOptions, ForwardTime, NANOS_PER_SECOND, ProtocolError,
        Value,
    },
};
use rmp::Marker;
use std::{io::Read, mem::size_of, ops::RangeInclusive, time::Instant};

/// Marker of the `nil` value, which is also a complete frame: a Forward keepalive.
const MARKER_NIL: u8 = 0xc0;

/// Extension type that carries a Fluent Bit `EventTime`.
const EVENT_TIME_EXT_TYPE: i8 = 0;

/// Bytes in an `EventTime` payload: a big-endian seconds word and a big-endian nanoseconds word.
const EVENT_TIME_BYTES: usize = 8;

/// Bytes charged for each decoded value, so a tree of small values is bounded by its count.
const NODE_CHARGE_BYTES: usize = 128;

/// The decode-time limit is checked once per this many values, so the clock is not read for
/// every value.
const DECODE_TIME_CHECK_VALUES: usize = 1024;

/// Gzip output is read through a buffer of this size.
const GZIP_READ_CHUNK_BYTES: usize = 8 * 1024;

/// Bytes charged up front for the inflater's history and header scratch space. These coexist
/// with the output, so they count against the decoded-bytes budget.
const GZIP_STATE_CHARGE_BYTES: usize = 256 * 1024;

/// Largest gzip header, including its optional name, comment and extra fields. Validating the
/// header before the gzip dependency sees it keeps those fields from allocating without a bound.
const MAX_GZIP_HEADER_BYTES: usize = 8 * 1024;

/// The gzip magic bytes followed by the compression method, which must be deflate (RFC 1952).
const GZIP_ID_AND_METHOD: [u8; 3] = [0x1f, 0x8b, 8];

/// Bytes in a gzip header that has none of the optional fields.
const GZIP_FIXED_HEADER_BYTES: usize = 10;

/// Offset of the flags byte, which follows the magic bytes and the compression method.
const GZIP_FLAGS_INDEX: usize = 3;

/// Flag bits the RFC reserves. A header that sets any of them is not gzip this decoder accepts.
const GZIP_RESERVED_FLAGS: u8 = 0xe0;

// FLG bits (RFC 1952, section 2.3.1) for the optional header fields the validator skips over.
const GZIP_FLAG_HCRC: u8 = 2;
const GZIP_FLAG_EXTRA: u8 = 4;
const GZIP_FLAG_NAME: u8 = 8;
const GZIP_FLAG_COMMENT: u8 = 16;

/// Bytes in the length prefix of the gzip extra field.
const GZIP_EXTRA_LENGTH_BYTES: usize = 2;

/// Bytes in the optional header CRC.
const GZIP_HCRC_BYTES: usize = 2;

/// Elements of a `Forward` or packed envelope with options: `[tag, entries, options]`.
const FORWARD_LENGTH_WITH_OPTIONS: usize = 3;

/// Elements of a `Message` envelope without options: `[tag, time, record]`.
const MESSAGE_LENGTH_WITHOUT_OPTIONS: usize = 3;

/// Elements of a `Message` envelope with options: `[tag, time, record, options]`.
const MESSAGE_LENGTH_WITH_OPTIONS: usize = 4;

/// Collections enclosing each value inside an entry's `[time, metadata]` pair.
const TIMED_METADATA_DEPTH: usize = 3;

/// Elements an envelope may have, from a bare `[tag, entries]` to a full `Message`.
const ENVELOPE_LENGTHS: RangeInclusive<usize> = 2..=MESSAGE_LENGTH_WITH_OPTIONS;

/// Whether a packed payload is gzip-compressed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Compression {
    Plain,
    Gzip,
}

/// Whether an entry's time may be a `[time, metadata]` pair.
#[derive(Clone, Copy, PartialEq, Eq)]
enum MetadataPolicy {
    /// Only `[time, record]` entries are valid.
    Rejected,
    /// A `[[time, metadata], record]` entry is valid as well.
    Accepted,
}

/// Decodes exactly one `MessagePack` value and rejects any bytes after it.
///
/// # Errors
///
/// Returns [`ProtocolError`] when the bytes are incomplete, malformed, or over a limit.
pub fn decode_value(bytes: &[u8], limits: &CodecLimits) -> Result<Value, ProtocolError> {
    let mut parser = Parser::new(bytes, limits)?;
    let value = parser.value(0)?;
    parser.ensure_consumed()?;
    Ok(value)
}

/// Decodes one complete frame into an envelope, a `nil` keepalive, or an ignored value.
///
/// # Errors
///
/// Returns [`ProtocolError`] when the frame is malformed, incomplete, or over a limit.
pub fn decode_frame(bytes: &[u8], limits: &CodecLimits) -> Result<DecodedFrame, ProtocolError> {
    if bytes.len() > limits.max_frame_bytes {
        return Err(ProtocolError::encoded_envelope_limit());
    }

    let mut parser = Parser::new(bytes, limits)?;
    if bytes.first() == Some(&MARKER_NIL) && bytes.len() == 1 {
        return Ok(DecodedFrame::Nil);
    }

    let first = *bytes.first().ok_or(ProtocolError::Incomplete)?;
    if !is_array(first) {
        parser.value(0)?;
        parser.ensure_consumed()?;
        return Ok(DecodedFrame::Ignored);
    }

    decode_envelope(&mut parser, limits).map(DecodedFrame::Envelope)
}

/// Decodes an envelope array: `[tag, entries, options?]`, `[tag, packed, options?]`, or
/// `[tag, time, record, options?]`.
fn decode_envelope(
    parser: &mut Parser<'_>,
    limits: &CodecLimits,
) -> Result<ForwardEnvelope, ProtocolError> {
    let count = parser.array_len()?;
    if !ENVELOPE_LENGTHS.contains(&count) {
        return Err(ProtocolError::Invalid("envelope arity"));
    }

    let tag = parse_tag(parser, limits)?;
    let second = parser.peek()?;
    let body = if is_array(second) {
        forward_body(parser, count)?
    } else if is_str_or_bin(second) {
        packed_body(parser, count)?
    } else {
        message_body(parser, count)?
    };
    parser.ensure_consumed()?;

    let ParsedOptions {
        options,
        compression,
    } = parse_options(body.options, limits)?;
    if !options.logs_metadata && body.events.iter().any(|event| event.metadata.is_some()) {
        return Err(ProtocolError::Invalid(
            "metadata timestamps require fluent_signal=0",
        ));
    }

    let mut mode = body.mode;
    let mut events = body.events;
    if let Some(payload) = body.packed {
        if compression == Compression::Gzip {
            mode = ForwardMode::CompressedPackedForward;
        }

        let policy = if options.logs_metadata {
            MetadataPolicy::Accepted
        } else {
            MetadataPolicy::Rejected
        };
        append_packed_events(parser, &payload, compression, policy, &mut events)?;
    } else if compression == Compression::Gzip {
        return Err(ProtocolError::Invalid("compression requires packed mode"));
    }

    Ok(ForwardEnvelope {
        tag,
        mode,
        events,
        options,
    })
}

/// The parts of an envelope between its tag and its options.
struct Body {
    mode: ForwardMode,
    events: Vec<ForwardEvent>,
    /// The packed payload, which is expanded into events after the options are read.
    packed: Option<Vec<u8>>,
    options: Option<Value>,
}

/// Reads the events of a `[tag, [[time, record], ...], options?]` message.
fn forward_body(parser: &mut Parser<'_>, count: usize) -> Result<Body, ProtocolError> {
    if count > FORWARD_LENGTH_WITH_OPTIONS {
        return Err(ProtocolError::Invalid("Forward arity"));
    }

    let entries = parser.array_len()?;
    if entries > parser.limits.max_events {
        return Err(events_limit());
    }

    let mut events = Vec::new();
    for _ in 0..entries {
        events.push(parser.entry(MetadataPolicy::Accepted)?);
    }

    let options = if count == FORWARD_LENGTH_WITH_OPTIONS {
        Some(parser.value(1)?)
    } else {
        None
    };
    Ok(Body {
        mode: ForwardMode::Forward,
        events,
        packed: None,
        options,
    })
}

/// Reads the payload of a `[tag, packed, options?]` message. The events are expanded later.
fn packed_body(parser: &mut Parser<'_>, count: usize) -> Result<Body, ProtocolError> {
    if count > FORWARD_LENGTH_WITH_OPTIONS {
        return Err(ProtocolError::Invalid("packed arity"));
    }

    let (Value::String(payload) | Value::Binary(payload)) = parser.value(1)? else {
        unreachable!("a string or binary marker always decodes to String or Binary");
    };

    let options = if count == FORWARD_LENGTH_WITH_OPTIONS {
        Some(parser.value(1)?)
    } else {
        None
    };
    Ok(Body {
        mode: ForwardMode::PackedForward,
        events: Vec::new(),
        packed: Some(payload),
        options,
    })
}

/// Reads the single event of a `[tag, time, record, options?]` message.
fn message_body(parser: &mut Parser<'_>, count: usize) -> Result<Body, ProtocolError> {
    if !(MESSAGE_LENGTH_WITHOUT_OPTIONS..=MESSAGE_LENGTH_WITH_OPTIONS).contains(&count) {
        return Err(ProtocolError::Invalid("Message arity"));
    }

    let time = parse_time(parser.value(1)?)?;
    let record = parser.record(time)?;
    let options = if count == MESSAGE_LENGTH_WITH_OPTIONS {
        Some(parser.value(1)?)
    } else {
        None
    };
    Ok(Body {
        mode: ForwardMode::Message,
        events: vec![record],
        packed: None,
        options,
    })
}

/// Reads the envelope tag, which must be a UTF-8 string of at most `max_tag_bytes` bytes.
fn parse_tag(parser: &mut Parser<'_>, limits: &CodecLimits) -> Result<String, ProtocolError> {
    let value = parser.value(1)?;
    let tag = value
        .as_str()
        .ok_or(ProtocolError::Invalid("UTF-8 tag required"))?;
    if tag.len() > limits.max_tag_bytes {
        return Err(ProtocolError::Limit("tag"));
    }
    Ok(tag.to_owned())
}

/// Expands a packed payload, gunzipping it first when it is compressed, and appends its events.
fn append_packed_events(
    parser: &mut Parser<'_>,
    payload: &[u8],
    compression: Compression,
    policy: MetadataPolicy,
    events: &mut Vec<ForwardEvent>,
) -> Result<(), ProtocolError> {
    let expanded = if compression == Compression::Gzip {
        parser.charge(GZIP_STATE_CHARGE_BYTES)?;
        let budget = Expansion {
            limits: parser.limits,
            compressed_len: payload.len(),
            allocated: parser.allocated,
            started: parser.started,
        };
        Some(expand_gzip(payload, &budget)?)
    } else {
        None
    };
    let data = expanded.as_deref().unwrap_or(payload);
    if data.len() > parser.limits.max_expanded_bytes {
        return Err(expanded_bytes_limit());
    }

    parser.charge(expanded.as_ref().map_or(data.len(), Vec::capacity))?;

    // The payload's entries are charged to a parser of their own. Its counts are not copied
    // back, because nothing reads the outer parser after the payload is expanded.
    let mut packed = Parser {
        bytes: data,
        pos: 0,
        limits: parser.limits,
        values: parser.values,
        strings: parser.strings,
        allocated: parser.allocated,
        started: parser.started,
    };
    while packed.pos < data.len() {
        if events.len() >= parser.limits.max_events {
            return Err(events_limit());
        }
        events.push(packed.entry(policy)?);
    }

    Ok(())
}

struct ParsedOptions {
    options: ForwardOptions,
    compression: Compression,
}

/// Reads the options map: `chunk`, `fluent_signal` and `compressed`. Unknown keys are ignored.
/// Returns the options and whether the payload is gzip-compressed.
fn parse_options(
    value: Option<Value>,
    limits: &CodecLimits,
) -> Result<ParsedOptions, ProtocolError> {
    let mut options = ForwardOptions::default();
    let mut compression = Compression::Plain;
    let Some(value) = value else {
        return Ok(ParsedOptions {
            options,
            compression,
        });
    };
    let Value::Map(entries) = value else {
        return Err(ProtocolError::Invalid("options must be map"));
    };

    let mut compressed_seen = false;
    let mut signal_seen = false;
    for (key, value) in entries {
        match key.as_str() {
            Some("chunk") => set_chunk(&mut options, value, limits)?,
            Some("fluent_signal") => {
                if signal_seen {
                    return Err(ProtocolError::Invalid("duplicate fluent_signal option"));
                }
                signal_seen = true;
                if value.as_integer() != Some(0) {
                    return Err(ProtocolError::Invalid(
                        "unsupported Fluent signal; logs signal 0 required",
                    ));
                }
                options.logs_metadata = true;
            }
            Some("compressed") => {
                if compressed_seen {
                    return Err(ProtocolError::Invalid("duplicate compression option"));
                }
                compressed_seen = true;
                compression = if value.as_str() == Some("gzip") {
                    Compression::Gzip
                } else {
                    Compression::Plain
                };
            }
            _ => {}
        }
    }

    Ok(ParsedOptions {
        options,
        compression,
    })
}

/// Records the `chunk` option. It may appear once, must be a string (or binary when allowed),
/// and must fit `max_chunk_bytes`.
fn set_chunk(
    options: &mut ForwardOptions,
    value: Value,
    limits: &CodecLimits,
) -> Result<(), ProtocolError> {
    if options.chunk.is_some() {
        return Err(ProtocolError::Invalid("duplicate chunk option"));
    }

    let chunk = match value {
        Value::String(v) => ChunkToken::String(v),
        Value::Binary(v) if limits.allow_binary_chunk => ChunkToken::Binary(v),
        _ => return Err(ProtocolError::Invalid("chunk must be string")),
    };
    let length = match &chunk {
        ChunkToken::String(v) | ChunkToken::Binary(v) => v.len(),
    };
    if length > limits.max_chunk_bytes {
        return Err(ProtocolError::Limit("chunk"));
    }

    options.chunk = Some(chunk);
    Ok(())
}

/// Reads a Forward timestamp: an integer of whole seconds, or an `EventTime` extension.
fn parse_time(value: Value) -> Result<ForwardTime, ProtocolError> {
    match value {
        Value::Integer(v) => Ok(ForwardTime {
            // TryFromIntError says only that the value is out of range, which the error says.
            seconds: i64::try_from(v).map_err(|_| ProtocolError::timestamp_overflow())?,
            nanoseconds: 0,
        }),
        Value::Extension(EVENT_TIME_EXT_TYPE, v) if v.len() == EVENT_TIME_BYTES => {
            let (seconds, nanos) = v.split_at(EVENT_TIME_BYTES / 2);
            let seconds = be_u32(seconds)?;
            let nanoseconds = be_u32(nanos)?;
            if nanoseconds >= NANOS_PER_SECOND {
                return Err(ProtocolError::Invalid("EventTime nanoseconds"));
            }

            Ok(ForwardTime {
                seconds: i64::from(seconds),
                nanoseconds,
            })
        }
        _ => Err(ProtocolError::Invalid(
            "integer or EventTime timestamp required",
        )),
    }
}

/// Reads a big-endian `u32` from exactly four bytes.
fn be_u32(bytes: &[u8]) -> Result<u32, ProtocolError> {
    // TryFromSliceError says only that the length differs, which the error here says.
    let array: [u8; size_of::<u32>()] = bytes
        .try_into()
        .map_err(|_| ProtocolError::Invalid("EventTime"))?;
    Ok(u32::from_be_bytes(array))
}

/// What bounds the output of one packed payload.
struct Expansion<'a> {
    limits: &'a CodecLimits,
    /// Compressed size of the payload, the base of the expansion ratio.
    compressed_len: usize,
    /// Bytes already charged to the frame before the output is added.
    allocated: usize,
    started: Instant,
}

/// Inflates a gzip payload, which may hold several concatenated members.
fn expand_gzip(payload: &[u8], budget: &Expansion<'_>) -> Result<Vec<u8>, ProtocolError> {
    if payload.is_empty() {
        return Err(ProtocolError::Invalid("empty gzip"));
    }

    let mut rest = payload;
    let mut expanded = Vec::new();
    let mut members = 0usize;
    while !rest.is_empty() {
        members += 1;
        if members > budget.limits.max_gzip_members {
            return Err(ProtocolError::Limit("gzip members"));
        }

        validate_gzip_header(rest)?;
        let mut decoder = flate2::bufread::GzDecoder::new(rest);
        inflate_member(&mut decoder, &mut expanded, budget)?;

        let next = decoder.into_inner();
        if next.len() >= rest.len() {
            return Err(ProtocolError::Invalid("gzip made no progress"));
        }

        rest = next;
    }

    Ok(expanded)
}

/// Inflates one gzip member onto the end of `expanded`, checking each chunk against the budget.
fn inflate_member(
    decoder: &mut flate2::bufread::GzDecoder<&[u8]>,
    expanded: &mut Vec<u8>,
    budget: &Expansion<'_>,
) -> Result<(), ProtocolError> {
    let limits = budget.limits;
    let mut chunk = [0u8; GZIP_READ_CHUNK_BYTES];
    loop {
        if budget.started.elapsed() > limits.max_decode_time {
            return Err(decode_time_limit());
        }

        // ProtocolError exposes only the stable rejection reason; changing it to carry the
        // inflater's I/O error would change this public error type.
        let n = decoder
            .read(&mut chunk)
            .map_err(|_| ProtocolError::Invalid("invalid gzip"))?;
        if n == 0 {
            return Ok(());
        }

        let length = expanded
            .len()
            .checked_add(n)
            .ok_or_else(expanded_bytes_limit)?;
        if length > limits.max_expanded_bytes
            || length > limits.max_decoded_bytes.saturating_sub(budget.allocated)
        {
            return Err(expanded_bytes_limit());
        }

        if limits.max_expansion_ratio > 0
            && length
                > budget
                    .compressed_len
                    .saturating_mul(limits.max_expansion_ratio)
        {
            return Err(ProtocolError::Limit("gzip expansion ratio"));
        }

        if length > expanded.capacity() {
            reserve_output(expanded, length, budget);
        }

        expanded.extend_from_slice(&chunk[..n]);
    }
}

/// Grows `expanded` to hold `length` bytes, at least doubling it, but never past the output
/// budget.
fn reserve_output(expanded: &mut Vec<u8>, length: usize, budget: &Expansion<'_>) {
    let output_cap = budget.limits.max_expanded_bytes.min(
        budget
            .limits
            .max_decoded_bytes
            .saturating_sub(budget.allocated),
    );
    let target = length
        .max(expanded.capacity().saturating_mul(2))
        .min(output_cap);
    expanded.reserve_exact(target - expanded.len());
}

/// Checks the gzip header at the start of `bytes`, and that its optional fields fit within
/// `MAX_GZIP_HEADER_BYTES`.
fn validate_gzip_header(bytes: &[u8]) -> Result<(), ProtocolError> {
    if bytes.len() < GZIP_FIXED_HEADER_BYTES
        || bytes[..GZIP_ID_AND_METHOD.len()] != GZIP_ID_AND_METHOD
        || bytes[GZIP_FLAGS_INDEX] & GZIP_RESERVED_FLAGS != 0
    {
        return Err(ProtocolError::Invalid("invalid gzip header"));
    }

    let flags = bytes[GZIP_FLAGS_INDEX];
    let mut pos = GZIP_FIXED_HEADER_BYTES;
    if flags & GZIP_FLAG_EXTRA != 0 {
        let size = bytes
            .get(pos..pos + GZIP_EXTRA_LENGTH_BYTES)
            .ok_or_else(truncated_gzip_header)?;
        let length = usize::from(u16::from_le_bytes([size[0], size[1]]));
        pos += GZIP_EXTRA_LENGTH_BYTES + length;
    }

    for flag in [GZIP_FLAG_NAME, GZIP_FLAG_COMMENT] {
        if flags & flag != 0 {
            skip_zero_terminated(bytes, &mut pos)?;
        }
    }

    if flags & GZIP_FLAG_HCRC != 0 {
        pos += GZIP_HCRC_BYTES;
    }

    if pos > MAX_GZIP_HEADER_BYTES {
        return Err(gzip_header_limit());
    }
    if pos > bytes.len() {
        return Err(truncated_gzip_header());
    }

    Ok(())
}

/// Moves `pos` past a zero-terminated gzip name or comment.
fn skip_zero_terminated(bytes: &[u8], pos: &mut usize) -> Result<(), ProtocolError> {
    loop {
        if *pos >= MAX_GZIP_HEADER_BYTES {
            return Err(gzip_header_limit());
        }
        let byte = *bytes.get(*pos).ok_or_else(truncated_gzip_header)?;
        *pos += 1;
        if byte == 0 {
            return Ok(());
        }
    }
}

fn is_array(byte: u8) -> bool {
    matches!(
        Marker::from_u8(byte),
        Marker::FixArray(_) | Marker::Array16 | Marker::Array32
    )
}

fn is_str_or_bin(byte: u8) -> bool {
    matches!(
        Marker::from_u8(byte),
        Marker::FixStr(_)
            | Marker::Str8
            | Marker::Str16
            | Marker::Str32
            | Marker::Bin8
            | Marker::Bin16
            | Marker::Bin32
    )
}

/// Folds big-endian bytes into an unsigned integer. Callers pass at most eight bytes.
fn big_endian_bits(data: &[u8]) -> u64 {
    data.iter()
        .fold(0u64, |v, b| (v << BITS_PER_BYTE) | u64::from(*b))
}

/// Reads a big-endian two's-complement integer whose width is the payload's length.
fn signed_bits(data: &[u8]) -> i64 {
    let shift = (size_of::<u64>() - data.len()) * BITS_PER_BYTE;
    (big_endian_bits(data) << shift).cast_signed() >> shift
}

/// An entry's time together with the optional metadata carried alongside it.
struct EntryTime {
    time: ForwardTime,
    metadata: Option<ForwardMetadata>,
}

/// Walks the `MessagePack` bytes of one frame, charging each value against the frame's budgets.
struct Parser<'a> {
    bytes: &'a [u8],
    /// Offset of the next unread byte.
    pos: usize,
    limits: &'a CodecLimits,
    /// Values read so far, checked against `max_values`.
    values: usize,
    /// String and binary bytes read so far, checked against `max_string_bytes`.
    strings: usize,
    /// Bytes charged to the decoded tree so far, checked against `max_decoded_bytes`.
    allocated: usize,
    /// When decoding began, for `max_decode_time`.
    started: Instant,
}

impl<'a> Parser<'a> {
    fn new(bytes: &'a [u8], limits: &'a CodecLimits) -> Result<Self, ProtocolError> {
        if bytes.len() > limits.max_decoded_bytes {
            return Err(decoded_allocation_limit());
        }

        Ok(Self {
            bytes,
            pos: 0,
            limits,
            values: 0,
            strings: 0,
            allocated: bytes.len(),
            started: Instant::now(),
        })
    }

    /// Rejects input that continues past the value just read.
    fn ensure_consumed(&self) -> Result<(), ProtocolError> {
        if self.pos != self.bytes.len() {
            return Err(ProtocolError::Invalid("trailing bytes"));
        }
        Ok(())
    }

    /// The byte at the cursor, which starts the next value.
    fn peek(&self) -> Result<u8, ProtocolError> {
        self.bytes
            .get(self.pos)
            .copied()
            .ok_or(ProtocolError::Incomplete)
    }

    /// Charges `n` bytes against the decoded-bytes budget.
    fn charge(&mut self, n: usize) -> Result<(), ProtocolError> {
        self.allocated = self
            .allocated
            .checked_add(n)
            .ok_or_else(decoded_allocation_limit)?;
        if self.allocated > self.limits.max_decoded_bytes {
            return Err(decoded_allocation_limit());
        }
        Ok(())
    }

    /// Counts one value and charges its fixed overhead. Every so often it also checks the
    /// decode-time budget.
    fn node(&mut self) -> Result<(), ProtocolError> {
        self.values += 1;
        if self.values > self.limits.max_values {
            return Err(ProtocolError::values_limit());
        }

        if self.values.is_multiple_of(DECODE_TIME_CHECK_VALUES)
            && self.started.elapsed() > self.limits.max_decode_time
        {
            return Err(decode_time_limit());
        }

        self.charge(NODE_CHARGE_BYTES)
    }

    /// Consumes the next `n` bytes.
    fn take(&mut self, n: usize) -> Result<&'a [u8], ProtocolError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(ProtocolError::Limit("length"))?;
        let data = self
            .bytes
            .get(self.pos..end)
            .ok_or(ProtocolError::Incomplete)?;
        self.pos = end;
        Ok(data)
    }

    /// Rejects a value whose children would push the frame past `max_values`.
    fn check_children(&self, children: usize) -> Result<(), ProtocolError> {
        if children > self.limits.max_values.saturating_sub(self.values) {
            return Err(ProtocolError::values_limit());
        }
        Ok(())
    }

    /// Reads an array header and returns its element count.
    fn array_len(&mut self) -> Result<usize, ProtocolError> {
        self.node()?;
        let h = header(&self.bytes[self.pos..])?;
        if !is_array(self.peek()?) {
            return Err(ProtocolError::Invalid("entry array required"));
        }

        self.check_children(h.children)?;
        self.pos += h.bytes;
        Ok(h.children)
    }

    /// Reads one event entry, `[time, record]`. When `allow_metadata` is set, the time may be
    /// a `[time, metadata]` pair.
    fn entry(&mut self, policy: MetadataPolicy) -> Result<ForwardEvent, ProtocolError> {
        if self.array_len()? != 2 {
            return Err(ProtocolError::Invalid("entry arity"));
        }

        let EntryTime { time, metadata } = if is_array(self.peek()?) {
            self.timed_metadata(policy)?
        } else {
            EntryTime {
                time: parse_time(self.value(2)?)?,
                metadata: None,
            }
        };
        let mut event = self.record(time)?;
        if metadata.as_ref().is_some_and(|metadata| {
            metadata
                .original
                .len()
                .saturating_add(event.original_record.len())
                > self.limits.max_event_bytes
        }) {
            return Err(ProtocolError::Limit("original event including metadata"));
        }

        event.metadata = metadata;
        Ok(event)
    }

    /// Reads the `[time, metadata]` pair of a logs-metadata entry, keeping the metadata's
    /// original bytes.
    fn timed_metadata(&mut self, policy: MetadataPolicy) -> Result<EntryTime, ProtocolError> {
        if policy == MetadataPolicy::Rejected || self.array_len()? != 2 {
            return Err(ProtocolError::Invalid(
                "logs metadata timestamp requires [time, map]",
            ));
        }

        let time = parse_time(self.value(TIMED_METADATA_DEPTH)?)?;
        let start = self.pos;
        let value = self.value(TIMED_METADATA_DEPTH)?;
        if !matches!(value, Value::Map(_)) {
            return Err(ProtocolError::Invalid("logs metadata must be map"));
        }

        let length = self.pos - start;
        if length > self.limits.max_event_bytes {
            return Err(ProtocolError::Limit("original event metadata"));
        }

        self.charge(length)?;
        let original = self.bytes[start..self.pos].to_vec();
        Ok(EntryTime {
            time,
            metadata: Some(ForwardMetadata { value, original }),
        })
    }

    /// Reads an event's record, which must be a map with string keys.
    fn record(&mut self, time: ForwardTime) -> Result<ForwardEvent, ProtocolError> {
        let start = self.pos;
        let record = self.value(2)?;
        let Value::Map(ref fields) = record else {
            return Err(ProtocolError::Invalid("record must be map"));
        };
        if fields.iter().any(|(k, _)| !matches!(k, Value::String(_))) {
            return Err(ProtocolError::Invalid("record keys must be strings"));
        }

        let length = self.pos - start;
        if length > self.limits.max_event_bytes {
            return Err(ProtocolError::Limit("original event"));
        }

        self.charge(length)?;
        Ok(ForwardEvent {
            time,
            record,
            original_record: self.bytes[start..self.pos].to_vec(),
            metadata: None,
        })
    }

    /// Reads the value at the cursor. `depth` counts the collections around it.
    fn value(&mut self, depth: usize) -> Result<Value, ProtocolError> {
        if depth > self.limits.max_depth {
            return Err(ProtocolError::depth_limit());
        }

        self.node()?;
        let marker = Marker::from_u8(self.peek()?);
        let h = header(&self.bytes[self.pos..])?;
        self.check_children(h.children)?;

        self.pos += h.bytes;
        let payload = self.take(h.payload)?;
        Ok(match marker {
            Marker::Null => Value::Nil,
            Marker::True => Value::Boolean(true),
            Marker::False => Value::Boolean(false),
            Marker::FixPos(v) => Value::Integer(i128::from(v)),
            Marker::FixNeg(v) => Value::Integer(i128::from(v)),
            Marker::U8 | Marker::U16 | Marker::U32 | Marker::U64 => {
                Value::Integer(i128::from(big_endian_bits(payload)))
            }
            Marker::I8 | Marker::I16 | Marker::I32 | Marker::I64 => {
                Value::Integer(i128::from(signed_bits(payload)))
            }
            // The header gives F32 exactly four payload bytes, so the bits fit in u32.
            Marker::F32 => Value::Float32(
                u32::try_from(big_endian_bits(payload)).expect("float32 payloads are four bytes"),
            ),
            Marker::F64 => Value::Float64(big_endian_bits(payload)),
            Marker::FixStr(_)
            | Marker::Str8
            | Marker::Str16
            | Marker::Str32
            | Marker::Bin8
            | Marker::Bin16
            | Marker::Bin32 => self.string_or_binary(marker, payload)?,
            Marker::Ext8
            | Marker::Ext16
            | Marker::Ext32
            | Marker::FixExt1
            | Marker::FixExt2
            | Marker::FixExt4
            | Marker::FixExt8
            | Marker::FixExt16 => self.extension(payload)?,
            Marker::FixArray(_) | Marker::Array16 | Marker::Array32 => {
                self.array(h.children, depth)?
            }
            Marker::FixMap(_) | Marker::Map16 | Marker::Map32 => self.map(h.children, depth)?,
            Marker::Reserved => return Err(ProtocolError::Invalid("reserved marker")),
        })
    }

    /// Copies a string or binary payload, charging its bytes.
    fn string_or_binary(&mut self, marker: Marker, payload: &[u8]) -> Result<Value, ProtocolError> {
        self.strings = self
            .strings
            .checked_add(payload.len())
            .ok_or_else(strings_limit)?;
        if self.strings > self.limits.max_string_bytes {
            return Err(strings_limit());
        }

        self.charge(payload.len())?;

        if matches!(marker, Marker::Bin8 | Marker::Bin16 | Marker::Bin32) {
            Ok(Value::Binary(payload.to_vec()))
        } else {
            Ok(Value::String(payload.to_vec()))
        }
    }

    /// Copies an extension payload: its type byte, then its data.
    fn extension(&mut self, payload: &[u8]) -> Result<Value, ProtocolError> {
        self.charge(payload.len())?;
        // The header always gives an extension at least its type byte.
        let (&kind, data) = payload.split_first().ok_or(ProtocolError::Incomplete)?;
        Ok(Value::Extension(kind.cast_signed(), data.to_vec()))
    }

    fn array(&mut self, children: usize, depth: usize) -> Result<Value, ProtocolError> {
        let mut values = Vec::new();
        for _ in 0..children {
            values.push(self.value(depth + 1)?);
        }
        Ok(Value::Array(values))
    }

    /// Reads `children / 2` key-value pairs.
    fn map(&mut self, children: usize, depth: usize) -> Result<Value, ProtocolError> {
        let mut fields = Vec::new();
        for _ in 0..children / 2 {
            fields.push((self.value(depth + 1)?, self.value(depth + 1)?));
        }
        Ok(Value::Map(fields))
    }
}

/// `Limit("events")`: a message holds more events than `max_events` allows.
fn events_limit() -> ProtocolError {
    ProtocolError::Limit("events")
}

/// `Limit("strings")`: string and binary bytes exceed `max_string_bytes`.
fn strings_limit() -> ProtocolError {
    ProtocolError::Limit("strings")
}

/// `Limit("decoded allocation")`: the decoded bytes exceed `max_decoded_bytes`.
fn decoded_allocation_limit() -> ProtocolError {
    ProtocolError::Limit("decoded allocation")
}

/// `Limit("decode time")`: decoding a frame took longer than `max_decode_time`.
fn decode_time_limit() -> ProtocolError {
    ProtocolError::Limit("decode time")
}

/// `Limit("expanded bytes")`: a packed payload expands past its output limits.
fn expanded_bytes_limit() -> ProtocolError {
    ProtocolError::Limit("expanded bytes")
}

/// `Limit("gzip header")`: a gzip header's optional fields exceed `MAX_GZIP_HEADER_BYTES`.
fn gzip_header_limit() -> ProtocolError {
    ProtocolError::Limit("gzip header")
}

/// `Invalid("truncated gzip header")`: the input ends inside a gzip header.
fn truncated_gzip_header() -> ProtocolError {
    ProtocolError::Invalid("truncated gzip header")
}

/// Encodes a value the server generates. Untrusted strings stay byte strings, including in
/// ACKs.
pub fn encode_value(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    put_value(&mut out, value);
    out
}

/// Encodes the acknowledgement for a chunk: a map whose `ack` key holds the chunk token.
pub fn encode_ack(chunk: &ChunkToken) -> Vec<u8> {
    let token = match chunk {
        ChunkToken::String(v) => Value::String(v.clone()),
        ChunkToken::Binary(v) => Value::Binary(v.clone()),
    };
    encode_value(&Value::Map(vec![(Value::string("ack"), token)]))
}

// The MessagePack markers this encoder writes. Lengths always use the 32-bit forms, so each
// length has one encoding.
const MARKER_FALSE: u8 = 0xc2;
const MARKER_TRUE: u8 = 0xc3;
const MARKER_BIN32: u8 = 0xc6;
const MARKER_EXT32: u8 = 0xc9;
const MARKER_FLOAT32: u8 = 0xca;
const MARKER_FLOAT64: u8 = 0xcb;
const MARKER_UINT64: u8 = 0xcf;
const MARKER_INT64: u8 = 0xd3;
const MARKER_STR32: u8 = 0xdb;
const MARKER_ARRAY32: u8 = 0xdd;
const MARKER_MAP32: u8 = 0xdf;

/// Appends the `MessagePack` encoding of `value` to `out`.
fn put_value(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Nil => out.push(MARKER_NIL),
        Value::Boolean(v) => out.push(if *v { MARKER_TRUE } else { MARKER_FALSE }),
        // Decoded integers always fit in 64 bits. A larger integer built by hand wraps here.
        Value::Integer(v) if *v < 0 => {
            out.push(MARKER_INT64);
            out.extend_from_slice(&(*v as i64).to_be_bytes());
        }
        Value::Integer(v) => {
            out.push(MARKER_UINT64);
            out.extend_from_slice(&(*v as u64).to_be_bytes());
        }
        Value::Float32(v) => {
            out.push(MARKER_FLOAT32);
            out.extend_from_slice(&v.to_be_bytes());
        }
        Value::Float64(v) => {
            out.push(MARKER_FLOAT64);
            out.extend_from_slice(&v.to_be_bytes());
        }
        Value::String(v) => put_bytes(out, MARKER_STR32, v),
        Value::Binary(v) => put_bytes(out, MARKER_BIN32, v),
        Value::Extension(kind, v) => {
            put_length(out, MARKER_EXT32, v.len());
            out.push(kind.cast_unsigned());
            out.extend_from_slice(v);
        }
        Value::Array(v) => {
            put_length(out, MARKER_ARRAY32, v.len());
            for child in v {
                put_value(out, child);
            }
        }
        Value::Map(v) => {
            put_length(out, MARKER_MAP32, v.len());
            for (key, child) in v {
                put_value(out, key);
                put_value(out, child);
            }
        }
    }
}

/// Appends a marker, a 32-bit length and the bytes themselves.
fn put_bytes(out: &mut Vec<u8>, marker: u8, bytes: &[u8]) {
    put_length(out, marker, bytes.len());
    out.extend_from_slice(bytes);
}

/// Appends a marker and a 32-bit big-endian length.
fn put_length(out: &mut Vec<u8>, marker: u8, length: usize) {
    out.push(marker);
    // A length above `u32::MAX` wraps. Decoded values never reach that size.
    out.extend_from_slice(&(length as u32).to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forward::codec::Framer;
    // Frames encoded by Ruby's msgpack gem (`tests/fixtures/forward/generate.rb`), one per
    // envelope mode: Message with an `EventTime`, Forward, PackedForward with `bin` and `str`
    // payloads, and CompressedPackedForward with two gzip members.
    const MESSAGE: &[u8] = include_bytes!("../../tests/fixtures/forward/message_eventtime.msgpack");
    const FORWARD: &[u8] = include_bytes!("../../tests/fixtures/forward/forward.msgpack");
    const BIN: &[u8] = include_bytes!("../../tests/fixtures/forward/packed_bin.msgpack");
    const STR: &[u8] = include_bytes!("../../tests/fixtures/forward/packed_str.msgpack");
    const GZIP: &[u8] = include_bytes!("../../tests/fixtures/forward/gzip_multimember.msgpack");

    fn envelope(bytes: &[u8]) -> ForwardEnvelope {
        let DecodedFrame::Envelope(value) = decode_frame(bytes, &CodecLimits::default()).unwrap()
        else {
            panic!("expected envelope")
        };
        value
    }

    #[test]
    fn independent_ruby_fixtures_all_modes_and_every_split() {
        for (wire, mode, count) in [
            (MESSAGE, ForwardMode::Message, 1),
            (FORWARD, ForwardMode::Forward, 2),
            (BIN, ForwardMode::PackedForward, 2),
            (STR, ForwardMode::PackedForward, 2),
            (GZIP, ForwardMode::CompressedPackedForward, 2),
        ] {
            let decoded = envelope(wire);
            assert_eq!(decoded.tag, "fixture.api");
            assert_eq!(decoded.mode, mode);
            assert_eq!(decoded.events.len(), count);
            assert_eq!(
                decoded.events[0].time,
                ForwardTime {
                    seconds: 1_700_000_000,
                    nanoseconds: 123_456_789
                }
            );
            assert_eq!(
                decoded.options.chunk,
                Some(ChunkToken::String(b"fixture-chunk".to_vec()))
            );

            for event in &decoded.events {
                assert_eq!(
                    decode_value(&event.original_record, &CodecLimits::default()).unwrap(),
                    event.record
                );
            }

            for split in 0..=wire.len() {
                let mut framer = Framer::new(CodecLimits::default());
                framer.push(&wire[..split]).unwrap();
                let first = framer.next_frame().unwrap();
                framer.push(&wire[split..]).unwrap();
                let complete = first.or_else(|| framer.next_frame().unwrap()).unwrap();
                assert_eq!(complete, wire);
                assert_eq!(envelope(&complete).events.len(), count);
            }
        }

        let legacy_wire = STR.to_vec();
        assert!(std::str::from_utf8(&legacy_wire).is_err());
    }

    #[test]
    fn captured_fluentbit_logs_metadata_gzip_is_a_declared_extension() {
        let wire = include_bytes!("../../tests/fixtures/forward/fluentbit_logs_metadata.msgpack");
        let decoded = envelope(wire);
        assert_eq!(decoded.mode, ForwardMode::CompressedPackedForward);
        assert!(decoded.options.logs_metadata);
        assert_eq!(decoded.events.len(), 1);
        let metadata = decoded.events[0].metadata.as_ref().unwrap();
        assert_eq!(metadata.value, Value::Map(vec![]));
        assert_eq!(metadata.original, vec![0x80]);

        for split in 0..=wire.len() {
            let mut framer = Framer::new(CodecLimits::default());
            framer.push(&wire[..split]).unwrap();
            let first = framer.next_frame().unwrap();
            framer.push(&wire[split..]).unwrap();
            let complete = first.or_else(|| framer.next_frame().unwrap()).unwrap();
            assert!(envelope(&complete).options.logs_metadata);
        }
    }

    #[test]
    fn logs_metadata_requires_explicit_signal_and_rejects_malformed_or_nonlog_signals() {
        let metadata = Value::Map(vec![(Value::string("trace"), Value::Binary(vec![0, 255]))]);
        let entry = |metadata| {
            Value::Array(vec![
                Value::Array(vec![Value::Integer(1), metadata]),
                Value::Map(vec![]),
            ])
        };
        let wire = |entry, signal: Option<Value>, packed| {
            let entries = if packed {
                Value::Binary(encode_value(&entry))
            } else {
                Value::Array(vec![entry])
            };
            let mut fields = vec![Value::string("metadata"), entries];
            if let Some(signal) = signal {
                fields.push(Value::Map(vec![(Value::string("fluent_signal"), signal)]));
            }
            encode_value(&Value::Array(fields))
        };

        for packed in [false, true] {
            let bytes = wire(entry(metadata.clone()), Some(Value::Integer(0)), packed);
            let decoded = envelope(&bytes);
            assert_eq!(decoded.events[0].metadata.as_ref().unwrap().value, metadata);

            assert!(
                decode_frame(
                    &wire(entry(metadata.clone()), None, packed),
                    &CodecLimits::default()
                )
                .is_err()
            );

            for metadata in [Value::Nil, Value::Array(vec![]), Value::string("invalid")] {
                assert!(
                    decode_frame(
                        &wire(entry(metadata), Some(Value::Integer(0)), packed),
                        &CodecLimits::default()
                    )
                    .is_err()
                );
            }

            for signal in [
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(-1),
                Value::string("0"),
            ] {
                assert!(
                    decode_frame(
                        &wire(entry(Value::Map(vec![])), Some(signal), packed),
                        &CodecLimits::default()
                    )
                    .is_err()
                );
            }
        }

        for malformed_time in [
            Value::Array(vec![Value::Integer(1)]),
            Value::Array(vec![Value::Integer(1), Value::Map(vec![]), Value::Nil]),
        ] {
            let entry = Value::Array(vec![malformed_time, Value::Map(vec![])]);
            assert!(
                decode_frame(
                    &wire(entry, Some(Value::Integer(0)), true),
                    &CodecLimits::default()
                )
                .is_err()
            );
        }

        let duplicate_signal = encode_value(&Value::Array(vec![
            Value::string("metadata"),
            Value::Binary(vec![]),
            Value::Map(vec![
                (Value::string("fluent_signal"), Value::Integer(0)),
                (Value::string("fluent_signal"), Value::Integer(0)),
            ]),
        ]));
        assert!(decode_frame(&duplicate_signal, &CodecLimits::default()).is_err());

        let ordinary = Value::Array(vec![Value::Integer(1), Value::Map(vec![])]);
        assert!(
            envelope(&wire(ordinary, Some(Value::Integer(0)), true)).events[0]
                .metadata
                .is_none()
        );
    }

    #[test]
    fn eventtime_all_header_forms_and_validation() {
        for prefix in [
            &[0xd7, 0][..],
            &[0xc7, 8, 0][..],
            &[0xc8, 0, 8, 0][..],
            &[0xc9, 0, 0, 0, 8, 0][..],
        ] {
            let mut wire = vec![0x93, 0xa1, b't'];
            wire.extend_from_slice(prefix);
            wire.extend_from_slice(&[0, 0, 0, 1, 0x3b, 0x9a, 0xc9, 0xff]);
            wire.push(0x80);

            assert_eq!(envelope(&wire).events[0].time.nanoseconds, 999_999_999);

            let len = wire.len();
            wire[len - 5..len - 1].copy_from_slice(&1_000_000_000u32.to_be_bytes());
            assert!(decode_frame(&wire, &CodecLimits::default()).is_err());
        }

        let mut wire = vec![0x93, 0xa1, b't', 0xcf];
        wire.extend_from_slice(&u64::MAX.to_be_bytes());
        wire.push(0x80);
        assert!(decode_frame(&wire, &CodecLimits::default()).is_err());
        assert!(
            ForwardTime {
                seconds: i64::MAX,
                nanoseconds: 0
            }
            .milliseconds()
            .is_err()
        );
    }

    #[test]
    fn packed_trailing_bytes_and_invalid_last_entry_are_atomic_errors() {
        let mut raw = envelope(BIN);
        assert_eq!(raw.events.len(), 2);
        raw.events.clear();

        let entry = Value::Array(vec![Value::Integer(1), Value::Map(vec![])]);
        let mut payload = encode_value(&entry);
        payload.push(0xc1);
        let wire = encode_value(&Value::Array(vec![
            Value::string("t"),
            Value::Binary(payload),
        ]));
        assert!(decode_frame(&wire, &CodecLimits::default()).is_err());

        let wire = [0x92, 0xa1, b't', 0x92, 0x92, 1, 0x80, 0x92, 1, 0x81, 1, 1];
        assert!(decode_frame(&wire, &CodecLimits::default()).is_err());
    }

    #[test]
    fn gzip_corruption_truncation_trailing_and_resource_limits() {
        for end in 1..GZIP.len() {
            assert!(decode_frame(&GZIP[..end], &CodecLimits::default()).is_err());
        }

        let root = decode_value(GZIP, &CodecLimits::default()).unwrap();
        let Value::Array(mut root) = root else {
            panic!("array")
        };
        let Value::Binary(mut packed) = root[1].clone() else {
            panic!("binary")
        };
        packed[30] ^= 0xff;
        root[1] = Value::Binary(packed);
        assert!(decode_frame(&encode_value(&Value::Array(root)), &CodecLimits::default()).is_err());

        for limits in [
            CodecLimits {
                max_gzip_members: 1,
                ..CodecLimits::default()
            },
            CodecLimits {
                max_expanded_bytes: 32,
                ..CodecLimits::default()
            },
            CodecLimits {
                max_decoded_bytes: 1024,
                ..CodecLimits::default()
            },
            CodecLimits {
                max_expansion_ratio: 1,
                ..CodecLimits::default()
            },
        ] {
            assert!(matches!(
                decode_frame(GZIP, &limits),
                Err(ProtocolError::Limit(_))
            ));
        }
    }

    #[test]
    fn empty_envelopes_unknown_options_and_exact_chunk_encoding() {
        let wire = [
            0x93, 0xa1, b't', 0x90, 0x82, 0xa4, b's', b'i', b'z', b'e', 0xff, 0xa5, b'c', b'h',
            b'u', b'n', b'k', 0xa2, 0xff, 0,
        ];
        let value = envelope(&wire);
        assert!(value.events.is_empty());

        let token = value.options.chunk.unwrap();
        let ack = decode_value(&encode_ack(&token), &CodecLimits::default()).unwrap();
        assert_eq!(
            ack,
            Value::Map(vec![(Value::string("ack"), Value::String(vec![0xff, 0]))])
        );

        let binary = encode_value(&Value::Array(vec![
            Value::string("t"),
            Value::Array(vec![]),
            Value::Map(vec![(Value::string("chunk"), Value::Binary(vec![0, 255]))]),
        ]));
        assert!(decode_frame(&binary, &CodecLimits::default()).is_err());

        let limits = CodecLimits {
            allow_binary_chunk: true,
            ..CodecLimits::default()
        };
        assert!(decode_frame(&binary, &limits).is_ok());

        assert!(matches!(
            decode_frame(&[0xc0], &limits).unwrap(),
            DecodedFrame::Nil
        ));
        assert!(matches!(
            decode_frame(&[1], &limits).unwrap(),
            DecodedFrame::Ignored
        ));
    }

    #[test]
    fn original_noncanonical_integer_and_map_headers_preserved() {
        let wire = [0x93, 0xa1, b't', 1, 0xde, 0, 1, 0xd9, 1, b'x', 0xcc, 1];
        let event = envelope(&wire).events.remove(0);
        assert_eq!(event.original_record, &wire[4..]);
        assert_ne!(encode_value(&event.record), event.original_record);
    }

    #[test]
    fn deterministic_malformed_corpus_has_no_panics() {
        let mut state = 0x12345678u64;
        for len in 0..128 {
            for _ in 0..16 {
                let mut wire = Vec::new();
                for _ in 0..len {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    wire.push(state as u8);
                }

                let _ = decode_frame(&wire, &CodecLimits::default());

                let mut framer = Framer::new(CodecLimits::default());
                for byte in wire {
                    if framer.push(&[byte]).is_err() || framer.next_frame().is_err() {
                        break;
                    }
                }
            }
        }
    }
}
