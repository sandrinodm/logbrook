//! Checks deterministic Docker normalization across supported JSON profiles.
#![no_main]
use libfuzzer_sys::fuzz_target;
use logbrook_forward_fuzz::{
    MAX_INPUT_BYTES, check_normalization,
    forward::{
        decode::decode_frame,
        normalize::{JsonProfile, NormalizationProfile},
        protocol::DecodedFrame,
    },
    limits,
};

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    if let Ok(DecodedFrame::Envelope(envelope)) = decode_frame(data, &limits()) {
        for profile in [JsonProfile::Text, JsonProfile::Pino, JsonProfile::JsonAuto] {
            check_normalization(&envelope, NormalizationProfile::Docker, profile);
        }
    }
});
