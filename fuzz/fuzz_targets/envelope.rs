//! Checks decoded envelope limits and exact original-record preservation.
#![no_main]
use libfuzzer_sys::fuzz_target;
use logbrook_forward_fuzz::{MAX_INPUT_BYTES, check_envelope};

fuzz_target!(|data: &[u8]| {
    if data.len() <= MAX_INPUT_BYTES {
        check_envelope(data);
    }
});
