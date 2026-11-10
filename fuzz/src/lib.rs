//! Shared limits and invariants for fuzzing the production Forward decoder and normalizer.
pub mod forward;

#[path = "../../src/model/normalized_event.rs"]
pub mod model;

use base64::{Engine, engine::general_purpose::STANDARD};
use forward::{
    decode::{decode_frame, decode_value},
    normalize::{
        ApplicationAttributes, JsonProfile, NormalizationOptions, NormalizationProfile,
        normalize_envelope,
    },
    protocol::{CodecLimits, DecodedFrame, ForwardEnvelope, ProtocolError},
};
use serde_json::Value as Json;
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
    let top_level = NormalizationOptions {
        application_attributes: ApplicationAttributes::TopLevel,
        ..options.clone()
    };
    let first_top_level = normalize_envelope(envelope, &top_level);
    match (&first_top_level, normalize_envelope(envelope, &top_level)) {
        (Ok(first), Ok(second)) => assert_eq!(
            serde_json::to_vec(first).unwrap(),
            serde_json::to_vec(&second).unwrap()
        ),
        (Err(first), Err(second)) => assert_eq!(first, &second),
        _ => panic!("top_level normalization is not deterministic"),
    }

    let first = normalize_envelope(envelope, &options);
    check_top_level(&first, &first_top_level, profile);
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

/// Top-level attribute keys that Logbrook writes itself.
const RESERVED: [&str; 3] = ["forward", "docker", "logbrook"];

/// Checks that the `top_level` result agrees with the `nested` one for an accepted envelope.
///
/// A lifted event can serialize slightly larger, so `top_level` may fail on the normalized-size
/// limit whatever `nested` returned. Every other difference is a failure.
fn check_top_level(
    nested: &Result<Vec<crate::model::NormalizedEvent>, ProtocolError>,
    top_level: &Result<Vec<crate::model::NormalizedEvent>, ProtocolError>,
    profile: NormalizationProfile,
) {
    if matches!(top_level, Err(ProtocolError::Limit("normalized envelope"))) {
        return;
    }
    let (nested, top_level) = match (nested, top_level) {
        (Ok(nested), Ok(top_level)) => (nested, top_level),
        (Err(nested), Err(top_level)) => {
            assert_eq!(nested, top_level);
            return;
        }
        (Ok(_), Err(error)) => panic!("top_level failed where nested succeeded: {error:?}"),
        (Err(_), Ok(_)) => panic!("top_level succeeded where nested failed"),
    };
    assert_eq!(nested.len(), top_level.len());

    for (nested, lifted) in nested.iter().zip(top_level) {
        assert_eq!(nested.message, lifted.message);
        assert_eq!(nested.level, lifted.level);
        assert_eq!(nested.event_time, lifted.event_time);
        assert_eq!(nested.service, lifted.service);
        assert_eq!(nested.host, lifted.host);
        assert_eq!(nested.logger, lifted.logger);
        assert_eq!(nested.pid, lifted.pid);

        if lifted.attributes["forward"]
            .get("application_attributes")
            .is_none()
        {
            assert_eq!(nested.attributes, lifted.attributes);
            continue;
        }
        assert_eq!(
            lifted.attributes["forward"]["application_attributes"],
            "top_level"
        );

        let Json::Object(application) = &nested.attributes["application"] else {
            panic!("a lifted event had no application object");
        };
        if profile == NormalizationProfile::Docker {
            let status = &nested.attributes["logbrook"]["parse_status"];
            assert!(
                status == "structured" || status == "json_without_message",
                "a Docker event was lifted with parse_status {status}"
            );
        }
        let conflicts = lifted.attributes["logbrook"].get("conflicting_fields");
        for (key, value) in application {
            if RESERVED.contains(&key.as_str()) {
                assert_eq!(conflicts.and_then(|fields| fields.get(key)), Some(value));
            } else {
                assert_eq!(lifted.attributes.get(key), Some(value));
            }
        }

        // The container is gone unless the application itself had an `application` field.
        match application.get("application") {
            Some(inner) => assert_eq!(lifted.attributes.get("application"), Some(inner)),
            None => assert!(lifted.attributes.get("application").is_none()),
        }

        // Logbrook's own objects survive; the lifted ones only add to them.
        for key in RESERVED {
            let Some(Json::Object(kept)) = nested.attributes.get(key) else {
                continue;
            };
            let Json::Object(now) = &lifted.attributes[key] else {
                panic!("{key} is no longer an object");
            };
            for (name, value) in kept {
                assert_eq!(now.get(name), Some(value));
            }
        }
    }
}
