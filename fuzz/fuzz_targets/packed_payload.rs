//! Mutates packed event streams and gzip payloads behind fixed outer framing.
#![no_main]
use libfuzzer_sys::fuzz_target;
use logbrook_forward_fuzz::{MAX_INPUT_BYTES, check_envelope};

// Build only the outer framing by hand; mutate the raw packed stream directly.
// This reaches payload and decompression checks without requiring a fuzzer to
// maintain a valid nested envelope and advertised payload length simultaneously.
fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    for (marker, gzip) in [(0xc6, false), (0xdb, false), (0xc6, true)] {
        let mut wire = vec![0x93, 0xa1, b't', marker];
        wire.extend_from_slice(&(data.len() as u32).to_be_bytes());
        wire.extend_from_slice(data);
        if gzip {
            wire.extend_from_slice(b"\x81\xaacompressed\xa4gzip");
        } else {
            wire.push(0x80);
        }

        check_envelope(&wire);
    }
});
