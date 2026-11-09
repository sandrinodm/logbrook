//! Checks that arbitrary socket fragmentation preserves complete frame boundaries.
#![no_main]
use libfuzzer_sys::fuzz_target;
use logbrook_forward_fuzz::{
    MAX_INPUT_BYTES,
    forward::{codec::Framer, protocol::ProtocolError},
    limits,
};

fn framed(data: &[u8], chunk_size: usize) -> Result<(Vec<Vec<u8>>, usize), ProtocolError> {
    let mut framer = Framer::new(limits());
    let mut frames = Vec::new();
    for chunk in data.chunks(chunk_size) {
        let capacity = framer.capacity_after_push(chunk.len());
        framer.push(chunk)?;
        assert!(framer.allocated_bytes() <= capacity);
        while let Some(frame) = framer.next_frame()? {
            frames.push(frame);
        }
    }

    Ok((frames, framer.buffered_len()))
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    let chunk_size = data.first().map_or(1, |byte| usize::from(*byte) + 1);
    assert_eq!(framed(data, data.len().max(1)), framed(data, chunk_size));
});
