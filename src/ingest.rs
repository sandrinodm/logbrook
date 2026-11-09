//! Bounded, atomic wire validation. Canonical identity never comes from producer metadata.

use crate::config::Config;
use crate::model::{Error, ErrorKind, NormalizedEvent};
use serde::de::{DeserializeSeed, SeqAccess, Visitor};
use serde_json::{Map, Value, value::RawValue};
use std::{cell::Cell, ops::RangeInclusive, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Event times are Unix milliseconds, from the epoch through 9999-12-31T23:59:59.999Z.
pub(crate) const SUPPORTED_EVENT_TIME_MS: RangeInclusive<i64> = 0..=253_402_300_799_999;

/// `TooLarge` message for a wire body with more than `max_events` events, or an event longer
/// than `max_event_bytes`.
pub(crate) const BATCH_EVENT_LIMIT: &str = "batch exceeds event limits";

/// `TooLarge` message for a wire body whose decoded-allocation estimate overflows or does not
/// fit one `u32` permit request.
pub(crate) const DECODED_BATCH_LIMIT: &str = "decoded batch exceeds budget";

/// `TooLarge` message for metadata nested past the depth limit, in wire and normalized
/// validation alike.
pub(crate) const METADATA_DEPTH_LIMIT: &str = "metadata exceeds depth limit";

/// `TooLarge` message for a string event field over the field byte limit, in wire and
/// normalized validation alike.
pub(crate) const FIELD_BYTES_LIMIT: &str = "field exceeds byte limit";

/// `TooLarge` message for a metadata key over the field byte limit, in wire and normalized
/// validation alike.
pub(crate) const METADATA_KEY_BYTES_LIMIT: &str = "metadata key exceeds byte limit";

/// `TooLarge` message for a metadata string value over the field byte limit, in wire and
/// normalized validation alike.
pub(crate) const METADATA_FIELD_BYTES_LIMIT: &str = "metadata field exceeds byte limit";

/// Conservative allowance for one allocated JSON node, including its map entry.
const NODE_ALLOCATION_BYTES: usize = 512;

/// Bytes charged per string byte: a string can reserve up to twice its wire size.
const STRING_ALLOCATION_FACTOR: usize = 2;

/// Parses a JSON array or NDJSON body into normalized events, in wire order.
///
/// # Errors
/// Fails when the body is malformed, an event breaks the configured limits, or the
/// batch is empty.
pub fn normalize(
    body: &[u8],
    ndjson: bool,
    config: &Config,
) -> Result<Vec<NormalizedEvent>, Error> {
    normalize_inner(body, BodyFormat::from_ndjson(ndjson), config, None).map(|batch| batch.events)
}

/// Account the allocated JSON tree before constructing it. The raw visitor borrows
/// each event and rejects count/size violations without building a whole Value tree.
///
/// # Errors
/// Fails like [`normalize`] and when the decoded-body budget has no room for the batch.
pub fn normalize_budgeted(
    body: &[u8],
    ndjson: bool,
    config: &Config,
    budget: Arc<Semaphore>,
) -> Result<(Vec<NormalizedEvent>, Option<OwnedSemaphorePermit>), Error> {
    normalize_inner(body, BodyFormat::from_ndjson(ndjson), config, Some(budget))
        .map(|batch| (batch.events, batch.permit))
}

/// The wire representation chosen at the public ingestion boundary.
#[derive(Clone, Copy)]
enum BodyFormat {
    Json,
    Ndjson,
}

impl BodyFormat {
    fn from_ndjson(ndjson: bool) -> Self {
        if ndjson { Self::Ndjson } else { Self::Json }
    }
}

/// Parsed events and the reservation that must outlive their allocated JSON trees.
struct NormalizedBatch {
    events: Vec<NormalizedEvent>,
    permit: Option<OwnedSemaphorePermit>,
}

fn normalize_inner(
    body: &[u8],
    format: BodyFormat,
    config: &Config,
    budget: Option<Arc<Semaphore>>,
) -> Result<NormalizedBatch, Error> {
    let raw = match format {
        BodyFormat::Json => parse_json_array(body, config)?,
        BodyFormat::Ndjson => parse_ndjson(body, config)?,
    };
    if raw.is_empty() {
        return Err(Error::invalid("batch must contain events"));
    }

    let allocated = allocation_estimate(&raw, config)?;
    let permit = reserve_decoded_budget(budget, allocated)?;

    let events = decode_events(raw, config)?;
    Ok(NormalizedBatch { events, permit })
}

/// Splits an NDJSON body into one borrowed raw event per non-blank line.
fn parse_ndjson<'a>(body: &'a [u8], config: &Config) -> Result<Vec<&'a RawValue>, Error> {
    let text = std::str::from_utf8(body)
        .map_err(|error| Error::invalid("body must be UTF-8").with_source(error))?;
    let mut raw = Vec::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        if raw.len() >= config.max_events || line.len() > config.max_event_bytes {
            return Err(large(BATCH_EVENT_LIMIT));
        }

        raw.push(
            serde_json::from_str::<&RawValue>(line)
                .map_err(|e| Error::invalid(format!("invalid NDJSON: {e}")).with_source(e))?,
        );
    }

    Ok(raw)
}

/// Reads a JSON array of events as borrowed raw values, failing at the first event
/// that breaks the count or byte limit.
fn parse_json_array<'a>(body: &'a [u8], config: &Config) -> Result<Vec<&'a RawValue>, Error> {
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let limit_hit = Cell::new(false);
    let raw = Batch {
        config,
        limit_hit: &limit_hit,
    }
    .deserialize(&mut deserializer)
    .map_err(|e| {
        if limit_hit.get() {
            large(BATCH_EVENT_LIMIT).with_source(e)
        } else {
            invalid_json(e)
        }
    })?;
    deserializer.end().map_err(invalid_json)?;

    Ok(raw)
}

fn invalid_json(e: serde_json::Error) -> Error {
    Error::invalid(format!("invalid JSON: {e}")).with_source(e)
}

/// Seed for the top-level event array. It keeps each event borrowed as a `RawValue`.
struct Batch<'a> {
    config: &'a Config,
    limit_hit: &'a Cell<bool>,
}

impl<'de> DeserializeSeed<'de> for Batch<'_> {
    type Value = Vec<&'de RawValue>;

    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for Batch<'_> {
    type Value = Vec<&'de RawValue>;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("an event array")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut raw = Vec::new();

        while let Some(value) = seq.next_element::<&'de RawValue>()? {
            if raw.len() >= self.config.max_events
                || value.get().len() > self.config.max_event_bytes
            {
                self.limit_hit.set(true);
                return Err(serde::de::Error::custom(
                    "batch exceeds event count or byte limit",
                ));
            }

            raw.push(value);
        }

        Ok(raw)
    }
}

/// Conservative allocation allowance for the decoded batch, computed before any `Value`
/// tree exists. It is not an allocator or RSS measurement.
fn allocation_estimate(raw: &[&RawValue], config: &Config) -> Result<usize, Error> {
    let mut allocated = raw
        .len()
        .checked_mul(NODE_ALLOCATION_BYTES)
        .ok_or_else(|| large(DECODED_BATCH_LIMIT))?;

    for value in raw {
        allocated = allocated
            .checked_add(event_allocation(value.get(), config)?)
            .ok_or_else(|| large(DECODED_BATCH_LIMIT))?;
    }

    Ok(allocated)
}

/// Estimates one event's allocation by scanning its wire bytes. A node includes the
/// `Value` plus worst-case map entry and node overhead, one separately allocated node
/// per key. Strings reserve at most twice their wire bytes, and vector capacity
/// rounding is covered by two nodes per token.
fn event_allocation(wire: &str, config: &Config) -> Result<usize, Error> {
    let mut quoted = false;
    let mut escaped = false;
    let mut nodes = 1usize;
    let mut nesting = 0usize;

    for byte in wire.bytes() {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
        } else if byte == b'"' {
            quoted = true;
        } else if matches!(byte, b'{' | b'[') {
            nesting += 1;
            nodes += 1;

            if nesting > config.max_metadata_depth + 1 {
                return Err(large(METADATA_DEPTH_LIMIT));
            }
        } else if matches!(byte, b'}' | b']') {
            nesting = nesting.saturating_sub(1);
        } else if matches!(byte, b',' | b':') {
            nodes += 1;
        }
    }

    nodes
        .checked_mul(NODE_ALLOCATION_BYTES)
        .and_then(|bytes| bytes.checked_add(wire.len().checked_mul(STRING_ALLOCATION_FACTOR)?))
        .ok_or_else(|| large(DECODED_BATCH_LIMIT))
}

/// Takes the decoded-body permit when the caller supplied a budget.
fn reserve_decoded_budget(
    budget: Option<Arc<Semaphore>>,
    allocated: usize,
) -> Result<Option<OwnedSemaphorePermit>, Error> {
    let Some(budget) = budget else {
        return Ok(None);
    };

    let permits =
        u32::try_from(allocated).map_err(|error| large(DECODED_BATCH_LIMIT).with_source(error))?;
    budget
        .try_acquire_many_owned(permits)
        .map(Some)
        .map_err(|error| {
            Error::new(ErrorKind::Overloaded, "decoded body budget exhausted").with_source(error)
        })
}

fn decode_events(raw: Vec<&RawValue>, config: &Config) -> Result<Vec<NormalizedEvent>, Error> {
    raw.into_iter()
        .enumerate()
        .map(|(index, raw)| {
            let value = serde_json::from_str(raw.get())
                .map_err(|e| Error::invalid(format!("invalid event: {e}")).with_source(e))?;
            event(value, config).map_err(|e| {
                Error::new(e.kind, format!("event {index}: {}", e.message)).with_source(e)
            })
        })
        .collect::<Result<Vec<_>, _>>()
}

/// The error for an event time outside [`SUPPORTED_EVENT_TIME_MS`].
pub(crate) fn unsupported_event_time() -> Error {
    Error::invalid("time outside supported UTC range")
}

fn large(message: &str) -> Error {
    Error::new(ErrorKind::TooLarge, message)
}

fn event(value: Value, config: &Config) -> Result<NormalizedEvent, Error> {
    // RawValue/NDJSON validation has already checked the event's wire bytes.
    depth(&value, 0, config.max_metadata_depth, config.max_field_bytes)?;

    let Value::Object(mut object) = value else {
        return Err(Error::invalid("event must be an object"));
    };
    let event_time = object
        .remove("time")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| Error::invalid("time must be integer Unix milliseconds"))?;
    if !SUPPORTED_EVENT_TIME_MS.contains(&event_time) {
        return Err(unsupported_event_time());
    }

    let level = object
        .remove("level")
        .and_then(|v| v.as_i64())
        .and_then(|n| i32::try_from(n).ok())
        .ok_or_else(|| Error::invalid("level must be a nonnegative integer"))?;
    if level < 0 {
        return Err(Error::invalid("level must be nonnegative"));
    }

    let message =
        string(&mut object, "msg", config)?.ok_or_else(|| Error::invalid("msg is required"))?;
    let service = string(&mut object, "service", config)?;
    let logger = string(&mut object, "name", config)?;
    let host = string(&mut object, "hostname", config)?;
    let pid = match object.remove("pid") {
        None | Some(Value::Null) => None,
        Some(value) => Some(
            value
                .as_i64()
                .filter(|n| *n >= 0)
                .ok_or_else(|| Error::invalid("pid must be nonnegative integer"))?,
        ),
    };

    Ok(NormalizedEvent {
        event_time,
        level,
        service,
        logger,
        host,
        pid,
        message,
        attributes: Value::Object(object),
    })
}

fn string(
    object: &mut Map<String, Value>,
    key: &str,
    config: &Config,
) -> Result<Option<String>, Error> {
    match object.remove(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.len() <= config.max_field_bytes => Ok(Some(value)),
        Some(Value::String(_)) => Err(large(FIELD_BYTES_LIMIT)),
        Some(_) => Err(Error::invalid(format!("{key} must be string or null"))),
    }
}

fn depth(value: &Value, current: usize, maximum: usize, field_limit: usize) -> Result<(), Error> {
    if current > maximum {
        return Err(large(METADATA_DEPTH_LIMIT));
    }

    match value {
        Value::Array(items) => {
            for item in items {
                depth(item, current + 1, maximum, field_limit)?;
            }
        }

        Value::Object(items) => {
            for (key, item) in items {
                if key.len() > field_limit {
                    return Err(large(METADATA_KEY_BYTES_LIMIT));
                }

                depth(item, current + 1, maximum, field_limit)?;
            }
        }

        Value::String(value) if value.len() > field_limit => {
            return Err(large(METADATA_FIELD_BYTES_LIMIT));
        }

        _ => {}
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    #[test]
    fn collisions_remain_attributes_and_imports_accept_history() {
        let config = Config::default();
        let body = br#"[{
            "time": 1,
            "level": 30,
            "msg": "ok",
            "id": "producer",
            "source": "producer",
            "host": "producer",
            "received_at": 42,
            "event_time": 43,
            "message": "producer",
            "logger": "producer",
            "attributes": {"keep": true}
        }]"#;
        let events = normalize(body, false, &config).unwrap();
        let attributes = &events[0].attributes;

        for key in [
            "id",
            "source",
            "host",
            "received_at",
            "event_time",
            "message",
            "logger",
            "attributes",
        ] {
            assert!(attributes.get(key).is_some(), "lost {key}");
        }

        assert_eq!(events[0].event_time, 1);
        assert_eq!(events[0].message, "ok");
        assert_eq!(attributes["attributes"]["keep"], true);
    }

    #[test]
    fn adversarial_node_budget_rejects_before_value_tree_and_releases_permits() {
        let config = Config::default();

        // Small wire representation, many allocated Value/map entries.
        let metadata = (0..1000)
            .map(|i| format!("\"k{i}\":[]"))
            .collect::<Vec<_>>()
            .join(",");
        let wire = format!("[{{\"time\":1,\"level\":30,\"msg\":\"ok\",\"meta\":{{{metadata}}}}}]");
        let budget = Arc::new(Semaphore::new(64 * 1024));
        let error =
            normalize_budgeted(wire.as_bytes(), false, &config, budget.clone()).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Overloaded);
        assert_eq!(budget.available_permits(), 64 * 1024);

        let (events, permit) = normalize_budgeted(
            br#"[{"time":1,"level":30,"msg":"ok"}]"#,
            false,
            &config,
            budget.clone(),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        assert!(budget.available_permits() < 64 * 1024);
        drop(permit);
        assert_eq!(budget.available_permits(), 64 * 1024);
    }

    #[test]
    fn count_size_depth_and_trailing_input_are_bounded() {
        let mut config = Config {
            max_events: 1,
            ..Config::default()
        };
        assert_eq!(
            normalize(
                br#"[{"time":1,"level":30,"msg":"ok"},{"time":1,"level":30,"msg":"ok"}]"#,
                false,
                &config
            )
            .unwrap_err()
            .kind,
            ErrorKind::TooLarge
        );

        config.max_metadata_depth = 2;
        assert_eq!(
            normalize(
                br#"[{"time":1,"level":30,"msg":"ok","meta":[[[[]]]]}]"#,
                false,
                &config
            )
            .unwrap_err()
            .kind,
            ErrorKind::TooLarge
        );

        assert!(
            normalize(
                br#"[{"time":1,"level":30,"msg":"ok"}] true"#,
                false,
                &config
            )
            .is_err()
        );

        config.max_event_bytes = 16;
        assert_eq!(
            normalize(br#"[{"time":1,"level":30,"msg":"ok"}]"#, false, &config)
                .unwrap_err()
                .kind,
            ErrorKind::TooLarge
        );
    }
}
