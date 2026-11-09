//! Mutates option values and timestamp encodings inside otherwise valid envelopes.
#![no_main]
use libfuzzer_sys::fuzz_target;
use logbrook_forward_fuzz::{MAX_INPUT_BYTES, check_envelope};

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    // One valid record, with the entire mutated input as its option value.
    let mut options = b"\x94\xa1t\x01\x80".to_vec();
    options.extend_from_slice(data);
    check_envelope(&options);

    // One valid record, with mutated MessagePack bytes in timestamp position.
    let mut timestamp = b"\x93\xa1t".to_vec();
    timestamp.extend_from_slice(data);
    timestamp.push(0x80);
    check_envelope(&timestamp);
});
