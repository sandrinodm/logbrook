//! Docker is a route-selected adapter; arbitrary Forward records are never auto-detected.
use super::{
    normalize::{
        JsonProfile, NormalizationOptions, apply_application_time, natural_projection,
        typed_projection, unique_field, value_text,
    },
    protocol::{ForwardEvent, ProtocolError, Value},
};
use crate::model::NormalizedEvent;
use serde_json::{Map, Value as Json, json};

/// Fields every Docker log record must carry, each exactly once and as a string.
const MANDATORY_FIELDS: [&str; 4] = ["log", "container_id", "container_name", "source"];

/// Fields that Docker adds to the records of a message split into fragments.
const FRAGMENT_FIELDS: [&str; 4] = [
    "partial_message",
    "partial_id",
    "partial_ordinal",
    "partial_last",
];

// Level numbers on the Bunyan and pino scale, which a structured level name maps to.
const LEVEL_TRACE: i32 = 10;
const LEVEL_DEBUG: i32 = 20;
const LEVEL_INFO: i32 = 30;
const LEVEL_WARN: i32 = 40;
const LEVEL_ERROR: i32 = 50;
const LEVEL_FATAL: i32 = 60;

/// Whether `record` has every mandatory Docker field exactly once, each holding a string.
pub(crate) fn is_docker_record(record: &Value) -> bool {
    MANDATORY_FIELDS
        .iter()
        .all(|key| unique_field(record, key).is_some_and(|value| matches!(value, Value::String(_))))
}

/// Adds Docker's fields to a normalized event: its service, its message, and its `docker`
/// attributes. A record that is not a Docker record keeps its generic form and says why.
///
/// # Errors
///
/// Returns [`ProtocolError`] when the record is not a map, or when its log line is over
/// `max_message_bytes`.
pub(crate) fn normalize_docker(
    event: &ForwardEvent,
    options: &NormalizationOptions,
    output: &mut NormalizedEvent,
) -> Result<(), ProtocolError> {
    let Value::Map(fields) = &event.record else {
        return Err(ProtocolError::Invalid("record map"));
    };
    if !is_docker_record(&event.record) {
        output.attributes["logbrook"] = json!({
            "docker_status": "generic_fallback",
            "reason": "missing_invalid_or_duplicate_docker_fields",
        });
        return Ok(());
    }

    if output.service.is_none() {
        output.service = compose_service(&event.record, options);
    }

    let original_log = text_field(&event.record, "log");
    if original_log.len() > options.max_message_bytes {
        return Err(ProtocolError::message_limit());
    }

    let container_name = text_field(&event.record, "container_name");
    let mut docker = json!({
        "container_id": text_field(&event.record, "container_id"),
        "container_name": container_name,
        "stream": text_field(&event.record, "source"),
    });
    docker["extra"] = Json::Object(extra_fields(fields));

    output.message = original_log.clone();
    if has_fragment_field(fields) {
        docker["fragment"] = fragment(&event.record);
        output.attributes["logbrook"] = json!({"parse_status": "fragment"});
    } else if options.json_profile != JsonProfile::Text {
        apply_json_line(&original_log, options, output, &mut docker);
    }

    // Keep the original line whenever parsing changed what is displayed.
    if output.message != original_log {
        docker["log"] = Json::String(original_log);
    }

    let name = container_name.trim_start_matches('/');
    if output.service.is_none() && !name.is_empty() {
        output.service = Some(name.to_owned());
    }

    if options.application_time_field.is_some()
        && output.attributes["logbrook"]
            .get("application_time_status")
            .is_none()
    {
        apply_application_time(None, options, output);
    }

    output.attributes["docker"] = docker;
    Ok(())
}

/// The text of a record field, or an empty string when the field is missing or not a string.
fn text_field(record: &Value, name: &str) -> String {
    unique_field(record, name)
        .and_then(value_text)
        .unwrap_or_default()
}

/// The Compose service named by the configured label. The label is a top-level field, or an
/// entry of a `labels` map.
fn compose_service(record: &Value, options: &NormalizationOptions) -> Option<String> {
    // Moby forwards selected labels at the record's top level. Collector-produced envelopes can
    // instead nest them under an explicitly named `labels` map.
    options
        .compose_service_label
        .as_deref()
        .and_then(|label| {
            unique_field(record, label).or_else(|| {
                unique_field(record, "labels").and_then(|labels| unique_field(labels, label))
            })
        })
        .and_then(value_text)
}

/// The fields that are neither mandatory nor fragment fields, each in its projected form.
fn extra_fields(fields: &[(Value, Value)]) -> Map<String, Json> {
    let mut extra = Map::new();
    for (key, value) in fields {
        if let Some(name) = key.as_str()
            && !MANDATORY_FIELDS.contains(&name)
            && !FRAGMENT_FIELDS.contains(&name)
        {
            extra.insert(name.to_owned(), projection(value));
        }
    }

    extra
}

/// Whether any field, counting duplicates, is named like a fragment field.
fn has_fragment_field(fields: &[(Value, Value)]) -> bool {
    fields
        .iter()
        .any(|(key, _)| key.as_str().is_some_and(|k| FRAGMENT_FIELDS.contains(&k)))
}

/// A value in its natural JSON form when it has one, and in its typed form otherwise.
fn projection(value: &Value) -> Json {
    natural_projection(value).unwrap_or_else(|| typed_projection(value))
}

/// The `fragment` attribute: the fragment fields that are present, and whether they are well
/// formed.
fn fragment(record: &Value) -> Json {
    let mut metadata = Map::new();
    for name in FRAGMENT_FIELDS {
        if let Some(value) = unique_field(record, name) {
            metadata.insert(name.to_owned(), projection(value));
        }
    }

    json!({"fields": metadata, "valid": fragment_is_valid(record)})
}

/// Whether a fragment's `partial_id`, `partial_ordinal` and `partial_last` are well formed.
fn fragment_is_valid(record: &Value) -> bool {
    let id = unique_field(record, "partial_id").is_some_and(|v| v.as_str().is_some());
    let ordinal = unique_field(record, "partial_ordinal").is_some_and(valid_ordinal);
    let last = unique_field(record, "partial_last").is_some_and(is_boolean_flag);
    id && ordinal && last
}

/// A fragment's `partial_last` is a boolean, or the text `true` or `false`.
fn is_boolean_flag(value: &Value) -> bool {
    match value {
        Value::Boolean(_) => true,
        Value::String(text) => text == b"true" || text == b"false",
        _ => false,
    }
}

/// A fragment ordinal is a non-negative integer, or a string of decimal digits that fits `u64`.
fn valid_ordinal(value: &Value) -> bool {
    match value {
        Value::Integer(n) => *n >= 0,
        Value::String(s) => std::str::from_utf8(s).ok().is_some_and(|s| {
            !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) && s.parse::<u64>().is_ok()
        }),
        _ => false,
    }
}

/// Reads a log line as JSON when the profile asks for it, and applies what it finds.
fn apply_json_line(
    line: &str,
    options: &NormalizationOptions,
    output: &mut NormalizedEvent,
    docker: &mut Json,
) {
    if !json_depth_ok(line.as_bytes(), options.max_json_depth) {
        output.attributes["logbrook"] = json!({"parse_status": "json_depth_limit"});
        return;
    }

    let Ok(parsed @ Json::Object(_)) = serde_json::from_str::<Json>(line) else {
        output.attributes["logbrook"] = json!({"parse_status": "text"});
        return;
    };
    apply_structured(parsed, options, output, docker);
}

/// Applies a parsed JSON object to `output`: its message, level, logger, pid, service and
/// application time. The object is kept as `application`, or as `docker.parsed_application`
/// when the normalizer already recorded the raw event.
fn apply_structured(
    parsed: Json,
    options: &NormalizationOptions,
    output: &mut NormalizedEvent,
    docker: &mut Json,
) {
    let application_time = application_time_of(&parsed, options);
    if let Some(message) = structured_message(&parsed, options) {
        output.message = message.to_owned();
        output.level = structured_level(&parsed, options);
        output.logger = ["logger", "name"]
            .iter()
            .find_map(|name| parsed.get(name).and_then(Json::as_str))
            .map(str::to_owned);
        output.pid = parsed
            .get("pid")
            .and_then(Json::as_i64)
            .filter(|pid| *pid >= 0);

        if output.service.is_none() {
            output.service = parsed
                .get("service")
                .and_then(Json::as_str)
                .map(str::to_owned);
        }

        output.attributes["logbrook"] = json!({"parse_status": "structured"});
    } else {
        output.attributes["logbrook"] = json!({"parse_status": "json_without_message"});
    }

    apply_application_time(application_time, options, output);

    if output.attributes["forward"]
        .get("record_encoding")
        .is_some()
    {
        docker["parsed_application"] = parsed;
    } else {
        output.attributes["application"] = parsed;
    }
}

/// The application time in the configured field, as an integer whether it is signed or not.
fn application_time_of(parsed: &Json, options: &NormalizationOptions) -> Option<i128> {
    let value = parsed.get(options.application_time_field.as_deref()?)?;
    value
        .as_i64()
        .map(i128::from)
        .or_else(|| value.as_u64().map(i128::from))
}

/// The message of a parsed object: `msg` for pino, otherwise the first configured message field.
fn structured_message<'a>(parsed: &'a Json, options: &NormalizationOptions) -> Option<&'a str> {
    if options.json_profile == JsonProfile::Pino {
        return parsed.get("msg").and_then(Json::as_str);
    }

    options
        .message_fields
        .iter()
        .find_map(|field| parsed.get(field).and_then(Json::as_str))
}

/// The level of a parsed object. Pino gives a number. Other objects give a name in `level` or
/// `severity`, and an unknown name keeps the default level.
fn structured_level(object: &Json, options: &NormalizationOptions) -> i32 {
    if options.json_profile == JsonProfile::Pino {
        return object
            .get("level")
            .and_then(Json::as_i64)
            .and_then(|n| i32::try_from(n).ok())
            .filter(|n| *n >= 0)
            .unwrap_or(options.default_level);
    }

    let value = object
        .get("level")
        .or_else(|| object.get("severity"))
        .and_then(Json::as_str);
    match value.map(str::to_ascii_lowercase).as_deref() {
        Some("trace") => LEVEL_TRACE,
        Some("debug") => LEVEL_DEBUG,
        Some("info") => LEVEL_INFO,
        Some("warn" | "warning") => LEVEL_WARN,
        Some("error") => LEVEL_ERROR,
        Some("fatal" | "critical") => LEVEL_FATAL,
        _ => options.default_level,
    }
}

/// Reject excessive nesting before serde allocates a structured tree. Quotes and escapes
/// are handled so braces in stack traces and JSON strings do not affect depth.
fn json_depth_ok(bytes: &[u8], max: usize) -> bool {
    let mut depth = 0usize;
    let mut quoted = false;
    let mut escape = false;
    for byte in bytes {
        if quoted {
            if escape {
                escape = false;
            } else if *byte == b'\\' {
                escape = true;
            } else if *byte == b'"' {
                quoted = false;
            }
        } else {
            match byte {
                b'"' => quoted = true,
                b'{' | b'[' => {
                    depth += 1;
                    if depth > max {
                        return false;
                    }
                }
                b'}' | b']' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
    }
    true
}
