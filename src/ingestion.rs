//! Shared normalized-event validation and live admission for every transport.

use crate::{
    config::Config,
    indexes::IndexHandle,
    ingest::{
        FIELD_BYTES_LIMIT, METADATA_DEPTH_LIMIT, METADATA_FIELD_BYTES_LIMIT,
        METADATA_KEY_BYTES_LIMIT, SUPPORTED_EVENT_TIME_MS, unsupported_event_time,
    },
    model::{Error, ErrorKind, NormalizedEvent},
    storage::{AppendLane, Storage, serialized_length},
};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::OwnedSemaphorePermit;

/// Transport limits supplement the destination's retention and clock policy.
#[derive(Clone, Copy, Debug)]
pub struct NormalizedLimits {
    /// Maximum events accepted from a single transport batch.
    pub max_events: usize,
    /// Maximum UTF-8 bytes in the canonical message.
    pub max_message_bytes: usize,
    /// Maximum bytes in canonical string fields and metadata strings or keys.
    pub max_field_bytes: usize,
    /// Maximum nested metadata depth, with the root at zero.
    pub max_metadata_depth: usize,
    /// Maximum serialized JSON bytes per normalized event.
    pub max_event_bytes: usize,
}

impl NormalizedLimits {
    /// Limits for HTTP ingestion, derived from the configured field and batch sizes.
    pub fn http(config: &Config) -> Self {
        Self {
            max_events: config.max_events,
            max_message_bytes: config.max_field_bytes,
            max_field_bytes: config.max_field_bytes,
            max_metadata_depth: config.max_metadata_depth,
            // The HTTP adapter already enforces its original event-wire limit.
            max_event_bytes: usize::MAX,
        }
    }
}

/// Validates the source and appends live events to the index under `permit`.
///
/// # Errors
/// Fails when the source or an event is invalid, the index is unavailable, or the
/// Forward admission deadline passes.
pub async fn append_live(
    index: &IndexHandle,
    source: String,
    events: Vec<NormalizedEvent>,
    permit: OwnedSemaphorePermit,
    limits: NormalizedLimits,
) -> Result<usize, Error> {
    crate::config::validate_source(&source)?;
    index
        .storage
        .append_validated_live(
            source,
            events,
            permit,
            LiveValidation::new(&index.config, limits),
            AppendLane::Forward,
            Some(std::time::Duration::from_millis(
                index.config.forward.admission_timeout_ms,
            )),
        )
        .await
}

/// A live batch and the decoded-memory reservation that travels with its append.
pub(crate) struct LiveBatch {
    pub(crate) source: String,
    pub(crate) events: Vec<NormalizedEvent>,
    pub(crate) permit: OwnedSemaphorePermit,
    pub(crate) limits: NormalizedLimits,
}

pub(crate) async fn append_live_to_storage(
    storage: &Storage,
    config: &Config,
    batch: LiveBatch,
) -> Result<usize, Error> {
    let LiveBatch {
        source,
        events,
        permit,
        limits,
    } = batch;
    crate::config::validate_source(&source)?;

    storage
        .append_validated_live(
            source,
            events,
            permit,
            LiveValidation::new(config, limits),
            AppendLane::Direct,
            None,
        )
        .await
}

#[derive(Clone, Copy)]
pub(crate) struct LiveValidation {
    retention_ms: i64,
    max_future_skew_ms: i64,
    limits: NormalizedLimits,
}

impl LiveValidation {
    pub(crate) fn new(config: &Config, limits: NormalizedLimits) -> Self {
        Self {
            retention_ms: config.retention_ms,
            max_future_skew_ms: config.max_future_skew_ms,
            limits,
        }
    }

    pub(crate) fn validate(&self, events: &[NormalizedEvent]) -> Result<(), Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| Error::unavailable("system clock precedes epoch").with_source(error))?
            .as_millis();
        let now = i64::try_from(now).unwrap_or(i64::MAX);
        validate_with_policy(events, *self, now)
    }
}

/// Checks live events against the retention window, future skew and size limits.
///
/// # Errors
/// Returns the first event that breaks a limit, named by the limit it broke.
pub fn validate_live(
    events: &[NormalizedEvent],
    config: &Config,
    limits: NormalizedLimits,
) -> Result<(), Error> {
    LiveValidation::new(config, limits).validate(events)
}

#[cfg(test)]
fn validate_at(
    events: &[NormalizedEvent],
    config: &Config,
    limits: NormalizedLimits,
    now: i64,
) -> Result<(), Error> {
    validate_with_policy(events, LiveValidation::new(config, limits), now)
}

fn validate_with_policy(
    events: &[NormalizedEvent],
    policy: LiveValidation,
    now: i64,
) -> Result<(), Error> {
    let limits = policy.limits;
    if events.len() > limits.max_events {
        return Err(large("batch exceeds event count limit"));
    }

    let oldest = now.saturating_sub(policy.retention_ms);
    let newest = now.saturating_add(policy.max_future_skew_ms);

    for event in events {
        if !SUPPORTED_EVENT_TIME_MS.contains(&event.event_time) {
            return Err(unsupported_event_time());
        }

        if event.event_time < oldest || event.event_time > newest {
            return Err(Error::invalid(
                "live event time outside retention/future-skew window",
            ));
        }

        if event.level < 0 || event.pid.is_some_and(|pid| pid < 0) {
            return Err(Error::invalid("level and pid must be nonnegative"));
        }

        if event.message.len() > limits.max_message_bytes {
            return Err(large("message exceeds byte limit"));
        }

        for field in [&event.service, &event.logger, &event.host]
            .into_iter()
            .flatten()
        {
            if field.len() > limits.max_field_bytes {
                return Err(large(FIELD_BYTES_LIMIT));
            }
        }

        validate_metadata(&event.attributes, 0, limits)?;

        if serialized_length(event)? > limits.max_event_bytes {
            return Err(large("normalized event exceeds byte limit"));
        }
    }

    Ok(())
}

fn validate_metadata(value: &Value, depth: usize, limits: NormalizedLimits) -> Result<(), Error> {
    if depth > limits.max_metadata_depth {
        return Err(large(METADATA_DEPTH_LIMIT));
    }

    match value {
        Value::Array(values) => {
            for value in values {
                validate_metadata(value, depth + 1, limits)?;
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                if key.len() > limits.max_field_bytes {
                    return Err(large(METADATA_KEY_BYTES_LIMIT));
                }

                validate_metadata(value, depth + 1, limits)?;
            }
        }
        Value::String(value) if value.len() > limits.max_field_bytes => {
            return Err(large(METADATA_FIELD_BYTES_LIMIT));
        }
        _ => {}
    }

    Ok(())
}

fn large(message: &str) -> Error {
    Error::new(ErrorKind::TooLarge, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event() -> NormalizedEvent {
        NormalizedEvent {
            event_time: 1000,
            level: 30,
            service: None,
            logger: None,
            host: None,
            pid: None,
            message: "message".into(),
            attributes: json!({"application": {"escaped": "\u{0}"}}),
        }
    }

    #[test]
    fn destination_window_and_normalized_escaping_are_enforced_atomically() {
        let config = Config {
            retention_ms: 100,
            max_future_skew_ms: 10,
            ..Config::default()
        };
        let limits = NormalizedLimits::http(&config);
        assert!(validate_at(&[event()], &config, limits, 1050).is_ok());
        assert!(validate_at(&[event()], &config, limits, 1200).is_err());
        assert!(validate_at(&[event()], &config, limits, 900).is_err());

        let size = serialized_length(&event()).unwrap();
        assert!(
            validate_at(
                &[event()],
                &config,
                NormalizedLimits {
                    max_event_bytes: size - 1,
                    ..limits
                },
                1050
            )
            .is_err()
        );

        assert!(
            validate_at(
                &[event()],
                &config,
                NormalizedLimits {
                    max_event_bytes: size,
                    ..limits
                },
                1050
            )
            .is_ok()
        );
    }

    #[test]
    fn empty_forward_envelopes_are_valid_but_invalid_last_event_rejects_batch() {
        let config = Config::default();
        let limits = NormalizedLimits::http(&config);
        assert!(validate_at(&[], &config, limits, 1000).is_ok());
        let mut invalid = event();
        invalid.level = -1;
        assert!(validate_at(&[event(), invalid], &config, limits, 1000).is_err());
    }
}
