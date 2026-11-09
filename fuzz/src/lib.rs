//! Shared limits and invariants for fuzzing the production Forward decoder and normalizer.
pub mod forward;

#[path = "../../src/model/normalized_event.rs"]
pub mod model;

use base64::{Engine, engine::general_purpose::STANDARD};
use forward::{
    decode::{decode_frame, decode_value},
    normalize::{JsonProfile, NormalizationOptions, NormalizationProfile, normalize_envelope},
    protocol::{CodecLimits, DecodedFrame, ForwardEnvelope},
};
use std::time::Duration;

/// Largest mutated byte slice admitted by any fuzz target.
pub const MAX_INPUT_BYTES: usize = 65_536;

/// Decoder limits shared by every target, bounded independently of the mutated input.
pub fn limits() -> CodecLimits {
    CodecLimits {
        max_frame_bytes: MAX_INPUT_BYTES + 1024,
        max_expanded_bytes: 262_144,
        max_events: 128,
        max_event_bytes: MAX_INPUT_BYTES,
        max_depth: 16,
        max_values: 8192,
        max_string_bytes: 262_144,
        max_decoded_bytes: 2 * 1024 * 1024,
        max_tag_bytes: 1024,
        max_chunk_bytes: 1024,
        max_gzip_members: 8,
        max_expansion_ratio: 512,
        max_decode_time: Duration::from_secs(1),
        allow_binary_chunk: true,
    }
}

/// Checks event counts, original bytes and normalization for any accepted envelope.
pub fn check_envelope(wire: &[u8]) {
    let limits = limits();
    if let Ok(DecodedFrame::Envelope(envelope)) = decode_frame(wire, &limits) {
        assert!(envelope.events.len() <= limits.max_events);
        for event in &envelope.events {
            assert!(event.original_record.len() <= limits.max_event_bytes);
            assert_eq!(
                decode_value(&event.original_record, &limits).unwrap(),
                event.record
            );
        }
        check_normalization(&envelope, NormalizationProfile::Generic, JsonProfile::Text);
    }
}

/// Checks deterministic normalization and exact fallback records for an accepted envelope.
pub fn check_normalization(
    envelope: &ForwardEnvelope,
    profile: NormalizationProfile,
    json_profile: JsonProfile,
) {
    let options = NormalizationOptions {
        profile,
        json_profile,
        received_at: 1_700_000_000_000,
        max_message_bytes: MAX_INPUT_BYTES,
        max_normalized_bytes: 1024 * 1024,
        max_json_depth: 16,
        ..NormalizationOptions::default()
    };
    let first = normalize_envelope(envelope, &options);
    let second = normalize_envelope(envelope, &options);
    match (first, second) {
        (Ok(first), Ok(second)) => {
            assert_eq!(first.len(), envelope.events.len());
            assert_eq!(
                serde_json::to_vec(&first).unwrap(),
                serde_json::to_vec(&second).unwrap()
            );

            for (normalized, original) in first.iter().zip(&envelope.events) {
                assert!(normalized.message.len() <= options.max_message_bytes);
                if let Some(encoded) =
                    normalized.attributes["forward"].get("original_record_msgpack_b64")
                {
                    assert_eq!(
                        STANDARD.decode(encoded.as_str().unwrap()).unwrap(),
                        original.original_record
                    );
                }
            }
        }
        (Err(first), Err(second)) => assert_eq!(first, second),
        _ => panic!("normalization is not deterministic"),
    }
}
