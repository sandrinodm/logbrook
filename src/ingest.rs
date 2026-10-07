//! Bounded, atomic wire validation. Canonical identity never comes from producer metadata.

use crate::config::Config;
use crate::model::{Error, ErrorKind, NormalizedEvent};
use serde_json::{Map, Value, value::RawValue};
use std::{cell::Cell, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub fn normalize(
    body: &[u8],
    ndjson: bool,
    config: &Config,
) -> Result<Vec<NormalizedEvent>, Error> {
    normalize_inner(body, ndjson, config, None).map(|(events, _)| events)
}

/// Account the allocated JSON tree before constructing it. The raw visitor borrows
/// each event and rejects count/size violations without building a whole Value tree.
pub fn normalize_budgeted(
    body: &[u8],
    ndjson: bool,
    config: &Config,
    budget: Arc<Semaphore>,
) -> Result<(Vec<NormalizedEvent>, Option<OwnedSemaphorePermit>), Error> {
    normalize_inner(body, ndjson, config, Some(budget))
}

fn normalize_inner(
    body: &[u8],
    ndjson: bool,
    config: &Config,
    budget: Option<Arc<Semaphore>>,
) -> Result<(Vec<NormalizedEvent>, Option<OwnedSemaphorePermit>), Error> {
    use serde::de::{DeserializeSeed, SeqAccess, Visitor};

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

    let raw = if ndjson {
        let text = std::str::from_utf8(body).map_err(|_| Error::invalid("body must be UTF-8"))?;
        let mut raw = Vec::new();

        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            if raw.len() >= config.max_events || line.len() > config.max_event_bytes {
                return Err(large("batch exceeds event limits"));
            }

            raw.push(
                serde_json::from_str::<&RawValue>(line)
                    .map_err(|e| Error::invalid(format!("invalid NDJSON: {e}")))?,
            );
        }

        raw
    } else {
        let mut deserializer = serde_json::Deserializer::from_slice(body);
        let limit_hit = Cell::new(false);
        let raw = Batch {
            config,
            limit_hit: &limit_hit,
        }
        .deserialize(&mut deserializer)
        .map_err(|e| {
            if limit_hit.get() {
                large("batch exceeds event limits")
            } else {
                Error::invalid(format!("invalid JSON: {e}"))
            }
        })?;

        deserializer
            .end()
            .map_err(|e| Error::invalid(format!("invalid JSON: {e}")))?;

        raw
    };

    if raw.is_empty() {
        return Err(Error::invalid("batch must contain events"));
    }

    // Conservative allocation allowance, not an allocator/RSS measurement.
    // A node includes Value plus worst-case BTreeMap entry/node overhead (one
    // separately allocated node per key). Strings reserve at most twice their
    // wire bytes; vector capacity rounding is covered by two nodes per token.
    let mut allocated = raw
        .len()
        .checked_mul(512)
        .ok_or_else(|| large("decoded batch exceeds budget"))?;

    for value in &raw {
        let mut quoted = false;
        let mut escaped = false;
        let mut nodes = 1usize;
        let mut nesting = 0usize;

        for byte in value.get().bytes() {
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
                    return Err(large("metadata exceeds depth limit"));
                }
            } else if matches!(byte, b'}' | b']') {
                nesting = nesting.saturating_sub(1);
            } else if matches!(byte, b',' | b':') {
                nodes += 1;
            }
        }

        allocated = allocated
            .checked_add(
                nodes
                    .checked_mul(512)
                    .ok_or_else(|| large("decoded batch exceeds budget"))?,
            )
            .and_then(|n| n.checked_add(value.get().len().checked_mul(2)?))
            .ok_or_else(|| large("decoded batch exceeds budget"))?;
    }

    let permit = match budget {
        Some(budget) => Some(
            budget
                .try_acquire_many_owned(
                    u32::try_from(allocated).map_err(|_| large("decoded batch exceeds budget"))?,
                )
                .map_err(|_| Error::new(ErrorKind::Overloaded, "decoded body budget exhausted"))?,
        ),
        None => None,
    };

    let events = raw
        .into_iter()
        .enumerate()
        .map(|(index, raw)| {
            let value = serde_json::from_str(raw.get())
                .map_err(|e| Error::invalid(format!("invalid event: {e}")))?;

            event(value, config)
                .map_err(|e| Error::new(e.kind, format!("event {index}: {}", e.message)))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok((events, permit))
}

fn large(message: &str) -> Error {
    Error::new(ErrorKind::TooLarge, message)
}

fn event(value: Value, config: &Config) -> Result<NormalizedEvent, Error> {
    // RawValue/NDJSON validation has already checked the event's wire bytes.
    depth(&value, 0, config.max_metadata_depth, config.max_field_bytes)?;

    let mut object = match value {
        Value::Object(object) => object,
        _ => return Err(Error::invalid("event must be an object")),
    };

    let event_time = object
        .remove("time")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| Error::invalid("time must be integer Unix milliseconds"))?;

    if !(0..=253_402_300_799_999).contains(&event_time) {
        return Err(Error::invalid("time outside supported UTC range"));
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
        Some(Value::String(_)) => Err(large("field exceeds byte limit")),
        Some(_) => Err(Error::invalid(format!("{key} must be string or null"))),
    }
}

fn depth(value: &Value, current: usize, maximum: usize, field_limit: usize) -> Result<(), Error> {
    if current > maximum {
        return Err(large("metadata exceeds depth limit"));
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
                    return Err(large("metadata key exceeds byte limit"));
                }

                depth(item, current + 1, maximum, field_limit)?;
            }
        }

        Value::String(value) if value.len() > field_limit => {
            return Err(large("metadata field exceeds byte limit"));
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
