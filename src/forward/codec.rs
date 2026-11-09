//! Incremental framing. Each header and payload byte is inspected at most once.
pub use super::decode::{decode_frame, decode_value, encode_ack, encode_value};
use super::protocol::{BITS_PER_BYTE, CodecLimits, ProtocolError};
use rmp::Marker;
use std::mem::size_of;

/// Bytes in the marker that starts every `MessagePack` value.
const MARKER_BYTES: usize = 1;

/// Bytes in the type field that follows the length of an extension value.
const EXT_TYPE_BYTES: usize = 1;

/// The shape of one `MessagePack` marker's header, which the marker alone determines.
struct Layout {
    /// Big-endian length bytes that follow the marker.
    length_bytes: usize,
    /// Payload bytes for strings, binaries and extensions, or the element count for collections.
    size: usize,
    /// Child values per unit: one for an array element, two for a map entry, zero otherwise.
    children_per_size: usize,
}

/// The layout of the value that `marker` starts.
fn layout(marker: Marker) -> Result<Layout, ProtocolError> {
    let (length_bytes, size, children_per_size) = match marker {
        Marker::Reserved => return Err(ProtocolError::Invalid("reserved MessagePack marker")),
        Marker::FixStr(n) => (0, usize::from(n), 0),
        Marker::FixArray(n) => (0, usize::from(n), 1),
        Marker::FixMap(n) => (0, usize::from(n), 2),
        Marker::Bin8 | Marker::Str8 => (size_of::<u8>(), 0, 0),
        Marker::Bin16 | Marker::Str16 => (size_of::<u16>(), 0, 0),
        Marker::Bin32 | Marker::Str32 => (size_of::<u32>(), 0, 0),
        Marker::Array16 => (size_of::<u16>(), 0, 1),
        Marker::Array32 => (size_of::<u32>(), 0, 1),
        Marker::Map16 => (size_of::<u16>(), 0, 2),
        Marker::Map32 => (size_of::<u32>(), 0, 2),
        Marker::Ext8 => (size_of::<u8>(), EXT_TYPE_BYTES, 0),
        Marker::Ext16 => (size_of::<u16>(), EXT_TYPE_BYTES, 0),
        Marker::Ext32 => (size_of::<u32>(), EXT_TYPE_BYTES, 0),
        Marker::FixExt1 => (0, EXT_TYPE_BYTES + size_of::<u8>(), 0),
        Marker::FixExt2 => (0, EXT_TYPE_BYTES + size_of::<u16>(), 0),
        Marker::FixExt4 => (0, EXT_TYPE_BYTES + size_of::<u32>(), 0),
        Marker::FixExt8 => (0, EXT_TYPE_BYTES + size_of::<u64>(), 0),
        Marker::FixExt16 => (0, EXT_TYPE_BYTES + size_of::<u128>(), 0),
        Marker::F32 | Marker::U32 | Marker::I32 => (0, size_of::<u32>(), 0),
        Marker::F64 | Marker::U64 | Marker::I64 => (0, size_of::<u64>(), 0),
        Marker::U8 | Marker::I8 => (0, size_of::<u8>(), 0),
        Marker::U16 | Marker::I16 => (0, size_of::<u16>(), 0),
        _ => (0, 0, 0),
    };
    Ok(Layout {
        length_bytes,
        size,
        children_per_size,
    })
}

/// The fixed-size part of one `MessagePack` value, read from its marker.
#[derive(Clone, Copy)]
pub(crate) struct Header {
    /// Marker bytes plus any length bytes that follow the marker.
    pub bytes: usize,
    /// Payload bytes after the header, such as string contents or an extension's data.
    pub payload: usize,
    /// Child values that follow. A map counts each key and each value.
    pub children: usize,
}

/// Reads the header of the `MessagePack` value at the start of `input`.
///
/// # Errors
///
/// Returns [`ProtocolError::Incomplete`] when `input` ends inside the header, and
/// [`ProtocolError::Invalid`] for the reserved marker.
pub(crate) fn header(input: &[u8]) -> Result<Header, ProtocolError> {
    let first = *input.first().ok_or(ProtocolError::Incomplete)?;
    let layout = layout(Marker::from_u8(first))?;
    if input.len() < MARKER_BYTES + layout.length_bytes {
        return Err(ProtocolError::Incomplete);
    }

    let mut payload = layout.size;
    if layout.length_bytes > 0 {
        // Length fields are big-endian.
        let length = input[MARKER_BYTES..MARKER_BYTES + layout.length_bytes]
            .iter()
            .fold(0usize, |n, b| (n << BITS_PER_BYTE) | usize::from(*b));
        payload = payload
            .checked_add(length)
            .ok_or(ProtocolError::Limit("length overflow"))?;
    }

    let children = payload
        .checked_mul(layout.children_per_size)
        .ok_or(ProtocolError::Limit("collection length"))?;
    Ok(Header {
        bytes: MARKER_BYTES + layout.length_bytes,
        payload: if layout.children_per_size == 0 {
            payload
        } else {
            0
        },
        children,
    })
}

/// Splits a byte stream into complete `MessagePack` frames without decoding their contents.
///
/// Bytes are buffered until one whole top-level value is present. Limits are checked from each
/// header as it arrives, so an oversized frame is rejected before its payload is buffered.
pub struct Framer {
    limits: CodecLimits,
    buffer: Vec<u8>,
    /// Offset in `buffer` of the first byte of the frame being assembled.
    start: usize,
    /// Offset in `buffer` of the next byte that has not been walked yet.
    cursor: usize,
    /// Children each open collection still expects, innermost last. A frame starts as `[1]`
    /// for its one top-level value.
    remaining: Vec<usize>,
    /// Payload bytes of the value under the cursor that have not been walked yet.
    pending: usize,
    /// Values counted in the current frame, checked against `max_values`.
    values: usize,
}

impl Framer {
    /// Creates an empty framer that enforces `limits`.
    pub fn new(limits: CodecLimits) -> Self {
        Self {
            limits,
            buffer: Vec::new(),
            start: 0,
            cursor: 0,
            remaining: vec![1],
            pending: 0,
            values: 0,
        }
    }

    /// Bytes received but not yet returned as part of a completed frame.
    pub fn buffered_len(&self) -> usize {
        self.buffer.len() - self.start
    }

    /// Capacity of the internal buffer, in bytes.
    pub fn allocated_bytes(&self) -> usize {
        self.buffer.capacity()
    }

    /// Predicts the buffer capacity after pushing `additional` bytes, so the transport can
    /// reserve budget for a reallocation before it happens.
    pub fn capacity_after_push(&self, additional: usize) -> usize {
        let length = if self.compacts_before(additional) {
            self.buffered_len()
        } else {
            self.buffer.len()
        };
        let needed = length.saturating_add(additional);
        if needed > self.buffer.capacity() {
            needed
                .max(self.buffer.capacity().saturating_mul(2))
                .min(self.limits.max_frame_bytes)
        } else {
            self.buffer.capacity()
        }
    }

    /// Appends received bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::Limit`] when the unreturned bytes plus `bytes` would exceed
    /// `max_frame_bytes`.
    pub fn push(&mut self, bytes: &[u8]) -> Result<(), ProtocolError> {
        if self
            .buffered_len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.limits.max_frame_bytes)
        {
            return Err(ProtocolError::Limit("receive buffer"));
        }

        let capacity = self.capacity_after_push(bytes.len());
        if self.compacts_before(bytes.len()) {
            self.buffer.drain(..self.start);
            self.cursor -= self.start;
            self.start = 0;
        }

        let needed = self.buffer.len() + bytes.len();
        if needed > self.buffer.capacity() {
            self.buffer.reserve_exact(capacity - self.buffer.len());
        }

        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    /// Returns the next complete frame, or `None` when more bytes are needed.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::Invalid`] when the bytes are not `MessagePack`, and
    /// [`ProtocolError::Limit`] when a declared size, value count or nesting depth is over its
    /// limit.
    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>, ProtocolError> {
        loop {
            self.skip_pending();
            if self.pending > 0 {
                return Ok(None);
            }

            self.close_finished_collections();
            if self.remaining.is_empty() {
                return Ok(Some(self.finish_frame()));
            }

            let h = match header(&self.buffer[self.cursor..]) {
                Ok(h) => h,
                Err(ProtocolError::Incomplete) => return Ok(None),
                Err(e) => return Err(e),
            };
            self.admit_value(h)?;
            if let Some(remaining) = self.remaining.last_mut() {
                *remaining -= 1;
            }

            self.cursor += h.bytes;
            self.pending = h.payload;
            if h.children > 0 {
                self.open_collection(h.children)?;
            }
        }
    }

    /// Whether the next `push` moves the unreturned bytes to the front of the buffer first.
    fn compacts_before(&self, additional: usize) -> bool {
        self.start > 0
            && (self.start >= self.buffer.len() / 2
                || self.buffer.len().saturating_add(additional) > self.limits.max_frame_bytes)
    }

    /// Walks the payload bytes that are already buffered.
    fn skip_pending(&mut self) {
        let available = self.buffer.len() - self.cursor;
        let take = self.pending.min(available);
        self.cursor += take;
        self.pending -= take;
    }

    /// Pops collections whose children have all been walked.
    fn close_finished_collections(&mut self) {
        while self.remaining.last() == Some(&0) {
            self.remaining.pop();
        }
    }

    /// Returns the completed frame and resets for the next one, which starts with one value.
    fn finish_frame(&mut self) -> Vec<u8> {
        let frame = self.buffer[self.start..self.cursor].to_vec();
        self.start = self.cursor;
        self.remaining.push(1);
        self.values = 0;
        if self.start == self.buffer.len() {
            self.buffer = Vec::new();
            self.start = 0;
            self.cursor = 0;
        }
        frame
    }

    /// Counts the value described by `h`, after checking the frame size and value budget.
    fn admit_value(&mut self, h: Header) -> Result<(), ProtocolError> {
        let end = (self.cursor - self.start)
            .checked_add(h.bytes)
            .and_then(|n| n.checked_add(h.payload));
        if end.is_none_or(|n| n > self.limits.max_frame_bytes) {
            return Err(ProtocolError::encoded_envelope_limit());
        }

        self.values = self
            .values
            .checked_add(1)
            .ok_or_else(ProtocolError::values_limit)?;
        if self.values > self.limits.max_values
            || h.children > self.limits.max_values.saturating_sub(self.values)
        {
            return Err(ProtocolError::values_limit());
        }

        Ok(())
    }

    /// Opens a collection with `children` values, rejecting nesting deeper than `max_depth`.
    fn open_collection(&mut self, children: usize) -> Result<(), ProtocolError> {
        if self.remaining.len() > self.limits.max_depth {
            return Err(ProtocolError::depth_limit());
        }
        self.remaining.push(children);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_split_and_coalesced() {
        let data = [
            0x93, 0xa1, b't', 1, 0x81, 0xa1, b'x', 0xa3, b'a', b'b', b'c',
        ];
        for split in 0..=data.len() {
            let mut f = Framer::new(CodecLimits::default());
            f.push(&data[..split]).unwrap();
            let first = f.next_frame().unwrap();
            f.push(&data[split..]).unwrap();
            assert_eq!(first.or_else(|| f.next_frame().unwrap()).unwrap(), data);
            assert!(f.next_frame().unwrap().is_none());
        }

        let mut f = Framer::new(CodecLimits::default());
        for byte in data {
            f.push(&[byte]).unwrap();
            if byte != b'c' {
                assert!(f.next_frame().unwrap().is_none());
            }
        }

        assert!(f.next_frame().unwrap().is_some());

        f.push(&[0xc0, 0xc0, 0xc0]).unwrap();
        for _ in 0..3 {
            assert_eq!(f.next_frame().unwrap(), Some(vec![0xc0]));
        }
    }

    #[test]
    fn rejects_declared_lengths_before_payload() {
        let mut f = Framer::new(CodecLimits::default());
        f.push(&[0xdb, 0xff, 0xff, 0xff, 0xff]).unwrap();
        assert!(matches!(f.next_frame(), Err(ProtocolError::Limit(_))));
    }
}
