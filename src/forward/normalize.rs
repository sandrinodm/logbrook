//! Pure normalization with a pre-allocation output bound for the whole envelope.
use super::protocol::{
    ForwardEnvelope, ForwardEvent, ForwardMetadata, MILLIS_PER_SECOND, ProtocolError, Value,
};
use crate::model::NormalizedEvent;
use base64::Engine;
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json, json};
use std::collections::HashSet;

/// Worst-case JSON size of one byte of text: a control byte escapes to `\u00XX`, six bytes.
const ESCAPED_TEXT_FACTOR: usize = 6;

/// Multiple of a projected value's size that covers its natural and typed trees, its JSON parse,
/// its display text, and the copy a Docker record retains.
const PROJECTION_FACTOR: usize = 5;

/// Fixed bytes charged for each node of a projected tree.
const PROJECTION_NODE_BYTES: usize = 96;

/// Multiple of the metadata's original bytes, which are also written out as Base64 text.
const METADATA_ORIGINAL_FACTOR: usize = 2;

/// Bytes reserved for the fields that normalization generates for every event.
const GENERATED_FIELDS_BYTES: usize = 2048;

/// Slack added to the unpadded Base64 length, which covers padding and the JSON quotes.
const BASE64_SLACK_BYTES: usize = 4;

/// Base64 writes every three input bytes as four characters.
const BASE64_BYTES_PER_GROUP: usize = 3;

/// Characters Base64 writes for each group of `BASE64_BYTES_PER_GROUP` input bytes.
const BASE64_CHARS_PER_GROUP: usize = 4;

/// Reuses CPU-feature detection while retaining canonical padded standard Base64.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
fn encode_base64(bytes: &[u8]) -> String {
    // Built on first use and shared, so the engine's CPU-feature detection runs once.
    static ENGINE: std::sync::LazyLock<base64::engine::Simd> = std::sync::LazyLock::new(|| {
        base64::engine::Simd::standard(base64::engine::general_purpose::PAD)
    });
    ENGINE.encode(bytes)
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
fn encode_base64(bytes: &[u8]) -> String {
    STANDARD.encode(bytes)
}

/// Which normalizer shapes an event's message and attributes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NormalizationProfile {
    /// Any record. The message comes from the first configured message field.
    #[default]
    Generic,
    /// Docker log records, with `log`, `source`, `container_id` and `container_name` fields.
    Docker,
}

/// How the Docker profile reads a log line that may be JSON.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonProfile {
    /// Keep the line as text.
    #[default]
    Text,
    /// Read pino output: `msg` is the message and `level` is numeric.
    Pino,
    /// Read any JSON object and take the message from the first configured message field.
    JsonAuto,
}

/// Where the application's own fields are stored in `attributes`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationAttributes {
    /// Keep the fields together under `attributes.application`.
    #[default]
    Nested,
    /// Store the fields directly under `attributes`, where attribute filters can match them.
    TopLevel,
}

/// Settings that shape normalized events for one route.
#[derive(Clone, Debug)]
pub struct NormalizationOptions {
    /// Adapter that shapes the canonical message and namespaced attributes.
    pub profile: NormalizationProfile,
    /// How the Docker adapter interprets structured application log lines.
    pub json_profile: JsonProfile,
    /// Whether the application's fields stay under `attributes.application` or move to the top
    /// level of `attributes`.
    pub application_attributes: ApplicationAttributes,
    /// Receipt time in milliseconds, used in place of a zero event time.
    pub received_at: i64,
    /// Whether an event whose time is zero takes `received_at`.
    pub zero_time_to_receipt: bool,
    /// Service name stamped on each event. Docker records use it in place of the Compose label
    /// or the container name.
    pub service: Option<String>,
    /// Optional host name stamped on every normalized event.
    pub host: Option<String>,
    /// Record field, or `labels` entry, that names the Compose service of a Docker record.
    pub compose_service_label: Option<String>,
    /// Record field that holds the application's own timestamp.
    pub application_time_field: Option<String>,
    /// Unit of `application_time_field`: `unix_ms` or `unix_seconds`.
    pub application_time_format: Option<String>,
    /// Earliest application time, in milliseconds, that is applied.
    pub oldest_time: i64,
    /// Latest application time, in milliseconds, that is applied.
    pub newest_time: i64,
    /// Level used when a record does not give one. 30 is info in the Bunyan and pino scale.
    pub default_level: i32,
    /// Largest message, in bytes.
    pub max_message_bytes: usize,
    /// Total bytes across this envelope, including generated fields and JSON escaping.
    pub max_normalized_bytes: usize,
    /// Deepest JSON nesting that a Docker log line may have before it is parsed.
    pub max_json_depth: usize,
    /// Record fields tried in order for the message text.
    pub message_fields: Vec<String>,
}

impl Default for NormalizationOptions {
    fn default() -> Self {
        Self {
            profile: NormalizationProfile::Generic,
            json_profile: JsonProfile::JsonAuto,
            application_attributes: ApplicationAttributes::Nested,
            received_at: 0,
            zero_time_to_receipt: true,
            service: None,
            host: None,
            compose_service_label: None,
            application_time_field: None,
            application_time_format: None,
            oldest_time: i64::MIN,
            newest_time: i64::MAX,
            default_level: 30,
            max_message_bytes: 256 * 1024,
            max_normalized_bytes: 32 * 1024 * 1024,
            max_json_depth: 32,
            message_fields: vec!["msg".into(), "message".into(), "log".into()],
        }
    }
}

/// Normalizes every event of an envelope into the rows Logbrook stores.
///
/// The envelope's output is bounded before any event is rendered. Each event's estimate is summed
/// and checked against `max_normalized_bytes`. Then each rendered event is counted as it is
/// serialized, against what remains of the same limit.
///
/// # Errors
///
/// Returns [`ProtocolError::Limit`] when a message or the envelope is over its byte limit, and
/// [`ProtocolError::Invalid`] when an event time does not fit in `i64` milliseconds.
pub fn normalize_envelope(
    envelope: &ForwardEnvelope,
    options: &NormalizationOptions,
) -> Result<Vec<NormalizedEvent>, ProtocolError> {
    check_estimated_size(envelope, options)?;

    let mut result = Vec::new();
    let mut actual = 0usize;
    for event in &envelope.events {
        let normalized = normalize_event(envelope, event, options)?;
        let remaining = options.max_normalized_bytes.saturating_sub(actual);
        let written = serialized_len(&normalized, remaining)?;
        actual = actual
            .checked_add(written)
            .ok_or(normalized_envelope_limit())?;
        result.push(normalized);
    }

    Ok(result)
}

/// Rejects an envelope whose summed per-event estimates exceed `max_normalized_bytes`.
fn check_estimated_size(
    envelope: &ForwardEnvelope,
    options: &NormalizationOptions,
) -> Result<(), ProtocolError> {
    let mut estimated = 0usize;
    for event in &envelope.events {
        let bound = event_bound(envelope, event, options)?;
        estimated = estimated
            .checked_add(bound)
            .ok_or(normalized_envelope_limit())?;
        if estimated > options.max_normalized_bytes {
            return Err(normalized_envelope_limit());
        }
    }

    Ok(())
}

/// Upper bound on the output bytes that one event can produce.
fn event_bound(
    envelope: &ForwardEnvelope,
    event: &ForwardEvent,
    options: &NormalizationOptions,
) -> Result<usize, ProtocolError> {
    let projection = projection_bound(&event.record)?;
    let original = base64_bound(event.original_record.len()).ok_or(normalization_arithmetic())?;
    let defaults = options
        .service
        .as_ref()
        .map_or(0, String::len)
        .saturating_add(options.host.as_ref().map_or(0, String::len));
    let bound = projection
        .checked_mul(PROJECTION_FACTOR)
        .and_then(|n| n.checked_add(original))
        .and_then(|n| n.checked_add(envelope.tag.len().saturating_mul(ESCAPED_TEXT_FACTOR)))
        .and_then(|n| n.checked_add(defaults.saturating_mul(ESCAPED_TEXT_FACTOR)))
        .and_then(|n| n.checked_add(GENERATED_FIELDS_BYTES))
        .ok_or(normalization_arithmetic())?;

    let metadata = metadata_bound(event)?;
    bound
        .checked_add(metadata)
        .ok_or(normalization_arithmetic())
}

/// Upper bound on the output of an event's metadata, which is projected and Base64-encoded.
fn metadata_bound(event: &ForwardEvent) -> Result<usize, ProtocolError> {
    let Some(metadata) = &event.metadata else {
        return Ok(0);
    };
    projection_bound(&metadata.value)?
        .checked_mul(PROJECTION_FACTOR)
        .and_then(|n| {
            metadata
                .original
                .len()
                .checked_mul(METADATA_ORIGINAL_FACTOR)
                .and_then(|original| n.checked_add(original))
        })
        .ok_or(normalization_arithmetic())
}

/// Upper bound on the Base64 text of `len` bytes. The unpadded length is `ceil(4 * len / 3)`,
/// which `(4 * len + 2) / 3` computes, and the slack covers padding.
fn base64_bound(len: usize) -> Option<usize> {
    len.checked_mul(BASE64_CHARS_PER_GROUP)
        .and_then(|n| n.checked_add(BASE64_BYTES_PER_GROUP - 1))
        .map(|n| n / BASE64_BYTES_PER_GROUP + BASE64_SLACK_BYTES)
}

/// Upper bound on the bytes of `value`'s projections, from its shape and text lengths.
fn projection_bound(value: &Value) -> Result<usize, ProtocolError> {
    let mut total = PROJECTION_NODE_BYTES;
    match value {
        Value::String(v) | Value::Binary(v) | Value::Extension(_, v) => {
            let text = v
                .len()
                .checked_mul(ESCAPED_TEXT_FACTOR)
                .ok_or_else(projection_limit)?;
            total = total.checked_add(text).ok_or_else(projection_limit)?;
        }
        Value::Array(values) => {
            for v in values {
                total = total
                    .checked_add(projection_bound(v)?)
                    .ok_or_else(projection_limit)?;
            }
        }
        Value::Map(entries) => {
            for (key, child) in entries {
                total = total
                    .checked_add(projection_bound(key)?)
                    .and_then(|n| n.checked_add(projection_bound(child).ok()?))
                    .ok_or_else(projection_limit)?;
            }
        }
        _ => {}
    }

    Ok(total)
}

/// Normalizes one event, then applies the Docker profile and the application time, and last
/// lifts the application fields to the top level when the route asks for `top_level`.
fn normalize_event(
    envelope: &ForwardEnvelope,
    event: &ForwardEvent,
    options: &NormalizationOptions,
) -> Result<NormalizedEvent, ProtocolError> {
    let mut normalized = normalize_generic(envelope, event, options)?;
    if options.profile == NormalizationProfile::Docker {
        super::docker::normalize_docker(event, options, &mut normalized)?;
    } else if let Some(field) = &options.application_time_field {
        apply_application_time(
            unique_field(&event.record, field).and_then(Value::as_integer),
            options,
            &mut normalized,
        );
    }

    // Last, so that nothing later overwrites or reinterprets the moved keys.
    if options.application_attributes == ApplicationAttributes::TopLevel {
        lift_application_attributes(options.profile, &mut normalized);
    }

    if normalized.message.len() > options.max_message_bytes {
        return Err(ProtocolError::message_limit());
    }

    Ok(normalized)
}

/// Top-level attribute keys that Logbrook writes itself. An application field with one of these
/// names goes to `logbrook.conflicting_fields` instead.
const RESERVED_TOP_LEVEL: [&str; 3] = ["forward", "docker", "logbrook"];

/// Moves the fields of `attributes.application` to the top level of `attributes`.
///
/// Events are left as they are when the application object is not a plain JSON projection: a
/// typed projection (`record_encoding` is set), a value that is not an object, or, on the Docker
/// profile, a raw driver record, which is what `application` holds unless the log line was
/// parsed as a JSON object. They are also left as they are when the attributes do not have the
/// shape the move needs: `forward` is not an object, or `logbrook` or `logbrook.conflicting_fields`
/// exists but is not an object. Every check runs before anything moves, so an event is never
/// left half-moved.
fn lift_application_attributes(profile: NormalizationProfile, output: &mut NormalizedEvent) {
    let Some(attributes) = output.attributes.as_object_mut() else {
        return;
    };
    let Some(Json::Object(forward)) = attributes.get("forward") else {
        return;
    };
    if forward.contains_key("record_encoding") {
        return;
    }
    if !attributes.get("application").is_some_and(Json::is_object) {
        return;
    }
    let logbrook = attributes.get("logbrook");
    if profile == NormalizationProfile::Docker {
        // The text profile writes no `parse_status`.
        let parsed_as_object = logbrook
            .and_then(|logbrook| logbrook.get("parse_status"))
            .and_then(Json::as_str)
            .is_some_and(|status| matches!(status, "structured" | "json_without_message"));
        if !parsed_as_object {
            return;
        }
    }
    let logbrook_is_usable = match logbrook {
        None => true,
        Some(Json::Object(logbrook)) => match logbrook.get("conflicting_fields") {
            None | Some(Json::Object(_)) => true,
            Some(_) => false,
        },
        Some(_) => false,
    };
    if !logbrook_is_usable {
        return;
    }

    let Some(Json::Object(application)) = attributes.remove("application") else {
        return;
    };
    for (key, value) in application {
        if RESERVED_TOP_LEVEL.contains(&key.as_str()) {
            // An existing `logbrook` object keeps its other keys.
            let logbrook = attributes.entry("logbrook").or_insert_with(|| json!({}));
            if let Json::Object(logbrook) = logbrook
                && let Json::Object(conflicting) = logbrook
                    .entry("conflicting_fields")
                    .or_insert_with(|| json!({}))
            {
                conflicting.insert(key, value);
            }
        } else {
            attributes.insert(key, value);
        }
    }

    if let Some(Json::Object(forward)) = attributes.get_mut("forward") {
        forward.insert("application_attributes".into(), json!("top_level"));
    }
}

/// Counts how many bytes `event` serializes to, failing once the count passes `max`.
fn serialized_len(event: &NormalizedEvent, max: usize) -> Result<usize, ProtocolError> {
    let mut counter = CountWriter { count: 0, max };
    // Serialization can only fail here by the counter refusing bytes past `max`, which is the
    // limit this error names.
    serde_json::to_writer(&mut counter, event).map_err(|_| normalized_envelope_limit())?;
    Ok(counter.count)
}

/// A writer that counts the bytes written to it and fails once they pass `max`.
struct CountWriter {
    count: usize,
    max: usize,
}

impl std::io::Write for CountWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.count = self
            .count
            .checked_add(bytes.len())
            .filter(|n| *n <= self.max)
            .ok_or_else(|| std::io::Error::other("normalized envelope limit"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Builds the normalized event, with its `forward` and `application` attributes, before any
/// profile adjusts it.
fn normalize_generic(
    envelope: &ForwardEnvelope,
    event: &ForwardEvent,
    options: &NormalizationOptions,
) -> Result<NormalizedEvent, ProtocolError> {
    let natural = natural_projection(&event.record);
    let fallback = natural.is_none();
    let application = natural.unwrap_or_else(|| typed_projection(&event.record));

    let mut forward = forward_attributes(envelope, event);
    if envelope.options.logs_metadata {
        forward["fluent_signal"] = json!(0);
    }

    if let Some(metadata) = &event.metadata {
        add_metadata_attributes(&mut forward, metadata);
    }

    if fallback {
        forward["record_encoding"] = json!("msgpack_typed_v1");
        forward["original_record_msgpack_b64"] = json!(encode_base64(&event.original_record));
    }

    let zero =
        event.time.seconds == 0 && event.time.nanoseconds == 0 && options.zero_time_to_receipt;
    if zero {
        forward["zero_time_substituted"] = Json::Bool(true);
    }

    let message = generic_message(event, options, &application);
    if message.len() > options.max_message_bytes {
        return Err(ProtocolError::message_limit());
    }

    Ok(NormalizedEvent {
        event_time: if zero {
            options.received_at
        } else {
            event.time.milliseconds()?
        },
        level: options.default_level,
        service: options.service.clone(),
        host: options.host.clone(),
        logger: None,
        pid: None,
        message,
        attributes: json!({"forward": forward, "application": application}),
    })
}

/// The `forward` attributes that every event carries: its tag, its time and its mode.
fn forward_attributes(envelope: &ForwardEnvelope, event: &ForwardEvent) -> Json {
    json!({
        "tag": envelope.tag,
        "seconds": event.time.seconds,
        "nanoseconds": event.time.nanoseconds,
        "normalization_version": 1,
        "mode": format!("{:?}", envelope.mode),
    })
}

/// Adds the metadata's projection, which is natural JSON when it can be, and its original bytes.
fn add_metadata_attributes(forward: &mut Json, metadata: &ForwardMetadata) {
    let natural = natural_projection(&metadata.value);
    forward["metadata_encoding"] = json!(if natural.is_some() {
        "json"
    } else {
        "msgpack_typed_v1"
    });
    forward["metadata"] = natural.unwrap_or_else(|| typed_projection(&metadata.value));
    forward["original_metadata_msgpack_b64"] = json!(encode_base64(&metadata.original));
}

/// The message of a generic event: the first configured message field, or the JSON of the
/// application projection when no field holds text.
fn generic_message(
    event: &ForwardEvent,
    options: &NormalizationOptions,
    application: &Json,
) -> String {
    // A valid Docker adapter supplies its own canonical message. Serializing a throwaway
    // generic display would both allocate an unused escaped copy and incorrectly apply the
    // message cap to that copy instead of the log line.
    if options.profile == NormalizationProfile::Docker
        && super::docker::is_docker_record(&event.record)
    {
        return String::new();
    }

    options
        .message_fields
        .iter()
        .find_map(|field| unique_field(&event.record, field).and_then(value_text))
        .unwrap_or_else(|| serde_json::to_string(application).expect("JSON value serializes"))
}

/// Applies the configured application timestamp to `output`, and records in
/// `logbrook.application_time_status` whether it was used.
///
/// The unit is mandatory: the receiver never guesses it from the value's magnitude.
pub(crate) fn apply_application_time(
    value: Option<i128>,
    options: &NormalizationOptions,
    output: &mut NormalizedEvent,
) {
    if options.application_time_field.is_none() {
        return;
    }

    let candidate = value.and_then(|v| match options.application_time_format.as_deref() {
        Some("unix_ms") => i64::try_from(v).ok(),
        Some("unix_seconds") => v
            .checked_mul(i128::from(MILLIS_PER_SECOND))
            .and_then(|v| i64::try_from(v).ok()),
        _ => None,
    });
    match candidate {
        Some(value) if (options.oldest_time..=options.newest_time).contains(&value) => {
            output.event_time = value;
            output.attributes["logbrook"]["application_time_status"] = json!("applied");
            output.attributes["forward"]["application_time_format"] =
                json!(options.application_time_format);
        }
        Some(_) => set_application_time_status(output, "fallback_out_of_policy"),
        None => set_application_time_status(output, "fallback_invalid_or_missing"),
    }
}

/// Records `status` as the application time status of `output`.
fn set_application_time_status(output: &mut NormalizedEvent, status: &str) {
    output.attributes["logbrook"]["application_time_status"] = json!(status);
}

/// The value of `name` when the record has exactly one field with that name. A repeated name is
/// ambiguous, so it returns `None`.
pub(crate) fn unique_field<'a>(record: &'a Value, name: &str) -> Option<&'a Value> {
    let Value::Map(fields) = record else {
        return None;
    };
    let mut matches = fields.iter().filter(|(key, _)| key.as_str() == Some(name));
    let value = &matches.next()?.1;
    if matches.next().is_some() {
        None
    } else {
        Some(value)
    }
}

/// The text of a string value, with invalid UTF-8 replaced. Other values have no text.
pub(crate) fn value_text(value: &Value) -> Option<String> {
    match value {
        Value::String(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
        _ => None,
    }
}

/// The natural JSON form of a value. `None` means JSON would lose information: the value's type,
/// the order of a map with duplicate keys, its bytes, or a non-finite float.
pub(crate) fn natural_projection(value: &Value) -> Option<Json> {
    Some(match value {
        Value::Nil => Json::Null,
        Value::Boolean(v) => Json::Bool(*v),
        Value::Integer(v) => {
            if *v < 0 {
                json!(i64::try_from(*v).ok()?)
            } else {
                json!(u64::try_from(*v).ok()?)
            }
        }
        Value::Float32(bits) => Json::Number(serde_json::Number::from_f64(f64::from(
            f32::from_bits(*bits),
        ))?),
        Value::Float64(bits) => Json::Number(serde_json::Number::from_f64(f64::from_bits(*bits))?),
        Value::String(bytes) => Json::String(std::str::from_utf8(bytes).ok()?.to_owned()),
        Value::Array(values) => Json::Array(
            values
                .iter()
                .map(natural_projection)
                .collect::<Option<Vec<_>>>()?,
        ),
        Value::Map(values) => {
            let mut names = HashSet::new();
            let mut object = Map::new();
            for (key, value) in values {
                let name = key.as_str()?;
                if !names.insert(name) {
                    return None;
                }
                object.insert(name.to_owned(), natural_projection(value)?);
            }
            Json::Object(object)
        }
        Value::Binary(_) | Value::Extension(_, _) => return None,
    })
}

/// A lossless `[kind, payload]` JSON form of any value, used when [`natural_projection`] cannot
/// represent it. Integers are written in decimal, floats as hex bit patterns, and non-UTF-8
/// strings as Base64.
pub fn typed_projection(value: &Value) -> Json {
    match value {
        Value::Nil => json!(["nil"]),
        Value::Boolean(v) => json!(["boolean", v]),
        Value::Integer(v) => json!(["integer", v.to_string()]),
        Value::Float32(bits) => json!(["float32", format!("{bits:08x}")]),
        Value::Float64(bits) => json!(["float64", format!("{bits:016x}")]),
        Value::String(v) => match std::str::from_utf8(v) {
            Ok(text) => json!(["string", text]),
            Err(_) => json!(["string_bytes", encode_base64(v)]),
        },
        Value::Binary(v) => json!(["binary", encode_base64(v)]),
        Value::Extension(kind, v) => json!(["extension", kind, encode_base64(v)]),
        Value::Array(v) => json!(["array", v.iter().map(typed_projection).collect::<Vec<_>>()]),
        Value::Map(v) => json!([
            "map",
            v.iter()
                .map(|(k, v)| json!([typed_projection(k), typed_projection(v)]))
                .collect::<Vec<_>>()
        ]),
    }
}

/// `Limit("normalization arithmetic")`: a size bound overflowed `usize`.
fn normalization_arithmetic() -> ProtocolError {
    ProtocolError::Limit("normalization arithmetic")
}

/// `Limit("normalized envelope")`: the envelope's output exceeds `max_normalized_bytes`.
fn normalized_envelope_limit() -> ProtocolError {
    ProtocolError::Limit("normalized envelope")
}

/// `Limit("projection")`: a projected value's size bound overflowed `usize`.
fn projection_limit() -> ProtocolError {
    ProtocolError::Limit("projection")
}

#[cfg(test)]
mod tests {
    #[test]
    fn base64_matches_scalar_for_all_vector_boundaries_and_padding() {
        use base64::Engine;
        let bytes: Vec<u8> = (0..=255).cycle().take(8195).collect();
        for len in 0..bytes.len() {
            assert_eq!(
                super::encode_base64(&bytes[..len]),
                STANDARD.encode(&bytes[..len])
            );
        }

        assert_eq!(super::encode_base64(b""), "");
        assert_eq!(super::encode_base64(b"f"), "Zg==");
        assert_eq!(super::encode_base64(b"fo"), "Zm8=");
        assert_eq!(super::encode_base64(&[0xfb, 0xff, 0xff]), "+///");
    }

    use super::*;
    use crate::forward::decode::{decode_frame, encode_value};
    use crate::forward::protocol::{
        CodecLimits, DecodedFrame, ForwardMode, ForwardOptions, ForwardTime,
    };
    use base64::engine::general_purpose::STANDARD;

    fn envelope(record: Value) -> ForwardEnvelope {
        ForwardEnvelope {
            tag: "configured.tag".into(),
            mode: ForwardMode::Message,
            events: vec![ForwardEvent {
                time: ForwardTime {
                    seconds: 1,
                    nanoseconds: 123_456_789,
                },
                original_record: encode_value(&record),
                metadata: None,
                record,
            }],
            options: ForwardOptions::default(),
        }
    }

    fn fields(values: &[(&str, Value)]) -> Value {
        Value::Map(
            values
                .iter()
                .map(|(key, value)| (Value::string(key), value.clone()))
                .collect(),
        )
    }

    fn docker(log: &str) -> Value {
        fields(&[
            ("container_id", Value::string("abc")),
            ("container_name", Value::string("/worker")),
            ("source", Value::string("stderr")),
            ("log", Value::string(log)),
        ])
    }

    #[test]
    fn typed_golden_preserves_duplicates_binary_floats_and_nested_nonstring_keys() {
        // Hand-authored wire uses uint8 for 1 and fixmap: original bytes cannot be
        // reconstructed from a canonical re-encoding of its logical value.
        let wire = [
            0x93, 0xa1, b't', 1, 0x84, 0xa1, b'x', 0xcc, 1, 0xa1, b'x', 0xc4, 2, 0, 255, 0xa1,
            b'z', 0xca, 0x7f, 0xc0, 0, 1, 0xa1, b'f', 0x81, 1, 0xa1, b'q',
        ];
        let DecodedFrame::Envelope(envelope) =
            decode_frame(&wire, &CodecLimits::default()).unwrap()
        else {
            panic!("envelope")
        };
        let event = normalize_envelope(&envelope, &NormalizationOptions::default())
            .unwrap()
            .remove(0);

        let golden: Json =
            serde_json::from_str(include_str!("../../tests/fixtures/forward/typed-v1.json"))
                .unwrap();
        assert_eq!(event.attributes["application"], golden["application"]);
        assert_eq!(
            event.attributes["forward"]["record_encoding"],
            golden["encoding"]
        );
        assert_eq!(
            STANDARD
                .decode(
                    event.attributes["forward"]["original_record_msgpack_b64"]
                        .as_str()
                        .unwrap()
                )
                .unwrap(),
            &wire[4..]
        );

        assert_eq!(
            normalize_envelope(&envelope, &NormalizationOptions::default()).unwrap()[0].attributes,
            event.attributes
        );
    }

    #[test]
    fn logs_metadata_is_namespaced_typed_and_exact_bytes_are_retained() {
        let mut envelope = envelope(fields(&[("msg", Value::string("application"))]));
        envelope.options.logs_metadata = true;
        // A noncanonical map16 header plus duplicate keys and binary values.
        let original = vec![0xde, 0, 2, 0xa1, b'k', 0xc4, 1, 255, 0xa1, b'k', 1];
        let metadata =
            crate::forward::decode::decode_value(&original, &CodecLimits::default()).unwrap();
        envelope.events[0].metadata = Some(ForwardMetadata {
            value: metadata,
            original: original.clone(),
        });

        let output = normalize_envelope(&envelope, &NormalizationOptions::default()).unwrap();
        let forward = &output[0].attributes["forward"];
        assert_eq!(forward["fluent_signal"], 0);
        assert_eq!(forward["metadata_encoding"], "msgpack_typed_v1");
        assert_eq!(
            forward["metadata"],
            json!([
                "map",
                [
                    [["string", "k"], ["binary", "/w=="]],
                    [["string", "k"], ["integer", "1"]]
                ]
            ])
        );

        assert_eq!(
            STANDARD
                .decode(forward["original_metadata_msgpack_b64"].as_str().unwrap())
                .unwrap(),
            original
        );
        assert_eq!(output[0].attributes["application"]["msg"], "application");

        assert!(
            normalize_envelope(
                &envelope,
                &NormalizationOptions {
                    max_normalized_bytes: 100,
                    ..NormalizationOptions::default()
                }
            )
            .is_err()
        );
    }

    #[test]
    fn unknown_docker_partial_fields_survive_structured_application_replacement() {
        let input = envelope(fields(&[
            ("container_id", Value::string("id")),
            ("container_name", Value::string("/name")),
            ("source", Value::string("stdout")),
            ("log", Value::string(r#"{"msg":"structured","level":30}"#)),
            ("partial_custom", Value::string("preserve me")),
        ]));
        let output = normalize_envelope(
            &input,
            &NormalizationOptions {
                profile: NormalizationProfile::Docker,
                ..NormalizationOptions::default()
            },
        )
        .unwrap();
        assert_eq!(output[0].message, "structured");
        assert_eq!(
            output[0].attributes["docker"]["extra"]["partial_custom"],
            "preserve me"
        );
        assert_eq!(output[0].attributes["application"]["msg"], "structured");
    }

    #[test]
    fn all_typed_variants_are_unambiguous() {
        let value = Value::Array(vec![
            Value::Nil,
            Value::Boolean(true),
            Value::Integer(-1),
            Value::Float64(f64::NEG_INFINITY.to_bits()),
            Value::Extension(-7, vec![0, 255]),
            Value::String(vec![255]),
            Value::Array(vec![Value::string("nil")]),
        ]);
        assert_eq!(
            typed_projection(&value),
            json!([
                "array",
                [
                    ["nil"],
                    ["boolean", true],
                    ["integer", "-1"],
                    ["float64", "fff0000000000000"],
                    ["extension", -7, "AP8="],
                    ["string_bytes", "/w=="],
                    ["array", [["string", "nil"]]]
                ]
            ])
        );
        assert!(natural_projection(&value).is_none());
    }

    #[test]
    fn natural_mapping_is_namespaced_and_zero_time_policy_is_explicit() {
        let mut input = envelope(fields(&[
            ("msg", Value::string("hello\n世界")),
            ("forward", Value::string("producer")),
            ("source", Value::string("untrusted")),
        ]));
        input.events[0].time = ForwardTime {
            seconds: 0,
            nanoseconds: 0,
        };

        let options = NormalizationOptions {
            received_at: 123,
            service: Some("fixed".into()),
            ..NormalizationOptions::default()
        };
        let event = normalize_envelope(&input, &options).unwrap().remove(0);
        assert_eq!(event.message, "hello\n世界");
        assert_eq!(event.event_time, 123);
        assert_eq!(event.service.as_deref(), Some("fixed"));
        assert_eq!(event.attributes["application"]["forward"], "producer");
        assert_eq!(event.attributes["forward"]["tag"], "configured.tag");
        assert_eq!(event.attributes["forward"]["zero_time_substituted"], true);

        let strict = NormalizationOptions {
            zero_time_to_receipt: false,
            ..options
        };
        assert_eq!(
            normalize_envelope(&input, &strict).unwrap()[0].event_time,
            0
        );
    }

    #[test]
    fn generic_log_field_does_not_select_docker() {
        let input = envelope(docker("{\"msg\":\"inner\"}"));
        let event = normalize_envelope(&input, &NormalizationOptions::default())
            .unwrap()
            .remove(0);
        assert_eq!(event.message, "{\"msg\":\"inner\"}");
        assert!(event.attributes.get("docker").is_none());
    }

    #[test]
    fn docker_pino_profile_precedence_and_raw_line_preservation() {
        let input = envelope(docker(
            "{\"level\":50,\"msg\":\"failed\",\"pid\":42,\"name\":\"jobs\",\
             \"service\":\"producer\",\"time\":999}",
        ));

        let options = NormalizationOptions {
            profile: NormalizationProfile::Docker,
            json_profile: JsonProfile::Pino,
            service: Some("configured".into()),
            host: Some("machine".into()),
            ..NormalizationOptions::default()
        };
        let event = normalize_envelope(&input, &options).unwrap().remove(0);
        assert_eq!(event.message, "failed");
        assert_eq!(event.level, 50);
        assert_eq!(event.pid, Some(42));
        assert_eq!(event.logger.as_deref(), Some("jobs"));
        assert_eq!(event.service.as_deref(), Some("configured"));
        assert_eq!(event.host.as_deref(), Some("machine"));
        assert_eq!(event.event_time, 1123);
        assert_eq!(event.attributes["docker"]["stream"], "stderr");
        assert!(
            event.attributes["docker"]["log"]
                .as_str()
                .unwrap()
                .contains("failed")
        );
    }

    #[test]
    fn docker_text_json_auto_and_fragment_contract() {
        let options = NormalizationOptions {
            profile: NormalizationProfile::Docker,
            ..NormalizationOptions::default()
        };
        for line in [
            "raw\nstack\ntrace\n",
            "[1,2]",
            "42",
            "{\"unrecognized\":true}",
            "{bad",
        ] {
            let event = normalize_envelope(&envelope(docker(line)), &options)
                .unwrap()
                .remove(0);
            assert_eq!(event.message, line);
            assert_eq!(event.level, 30);
            assert_eq!(event.service.as_deref(), Some("worker"));
        }

        let event = normalize_envelope(
            &envelope(docker("{\"message\":\"bad\",\"level\":\"warning\"}")),
            &options,
        )
        .unwrap()
        .remove(0);
        assert_eq!(event.message, "bad");
        assert_eq!(event.level, 40);

        let mut record = docker("{\"msg\":\"partial");
        let Value::Map(ref mut fields) = record else {
            panic!("map")
        };
        fields.extend(
            [
                ("partial_id", Value::string("id")),
                ("partial_ordinal", Value::string("2")),
                ("partial_last", Value::string("false")),
            ]
            .into_iter()
            .map(|(k, v)| (Value::string(k), v)),
        );

        let event = normalize_envelope(&envelope(record), &options)
            .unwrap()
            .remove(0);
        assert_eq!(event.message, "{\"msg\":\"partial");
        assert_eq!(event.attributes["logbrook"]["parse_status"], "fragment");
        assert_eq!(event.attributes["docker"]["fragment"]["valid"], true);
        assert_eq!(
            event.attributes["docker"]["fragment"]["fields"]["partial_ordinal"],
            "2"
        );
    }

    #[test]
    fn docker_message_cap_applies_to_the_log_line_instead_of_unused_generic_display() {
        let line = "\n\"\\".repeat(43_691);
        let record = docker(&line);
        assert!(encode_value(&record).len() < 256 * 1024);
        assert!(
            serde_json::to_string(&natural_projection(&record).unwrap())
                .unwrap()
                .len()
                > 256 * 1024
        );

        for json_profile in [JsonProfile::Text, JsonProfile::Pino, JsonProfile::JsonAuto] {
            let options = NormalizationOptions {
                profile: NormalizationProfile::Docker,
                json_profile,
                message_fields: vec!["message".into(), "msg".into()],
                max_message_bytes: 256 * 1024,
                max_normalized_bytes: 8 * 1024 * 1024,
                ..NormalizationOptions::default()
            };
            let output = normalize_envelope(&envelope(record.clone()), &options).unwrap();
            assert_eq!(output[0].message, line);

            let mut malformed = record.clone();
            let Value::Map(ref mut fields) = malformed else {
                panic!("map")
            };
            fields.retain(|(key, _)| key.as_str() != Some("container_id"));

            assert!(matches!(
                normalize_envelope(&envelope(malformed), &options),
                Err(ProtocolError::Limit("message"))
            ));
        }
    }

    #[test]
    fn duplicate_mandatory_docker_fields_fall_back_and_preserve_bytes() {
        let mut record = docker("hello");
        let Value::Map(ref mut fields) = record else {
            panic!("map")
        };
        fields.push((Value::string("log"), Value::string("other")));

        let input = envelope(record);
        let options = NormalizationOptions {
            profile: NormalizationProfile::Docker,
            ..NormalizationOptions::default()
        };
        let event = normalize_envelope(&input, &options).unwrap().remove(0);
        assert_eq!(
            event.attributes["logbrook"]["docker_status"],
            "generic_fallback"
        );
        assert_eq!(
            event.attributes["forward"]["record_encoding"],
            "msgpack_typed_v1"
        );
        assert_eq!(
            STANDARD
                .decode(
                    event.attributes["forward"]["original_record_msgpack_b64"]
                        .as_str()
                        .unwrap()
                )
                .unwrap(),
            input.events[0].original_record
        );
    }

    #[test]
    fn compose_label_precedes_application_service_and_application_time_is_explicit() {
        let mut record = docker("{\"msg\":\"event\",\"service\":\"application\",\"at\":12}");
        let Value::Map(ref mut fields) = record else {
            panic!("map")
        };
        fields.push((
            Value::string("com.docker.compose.service"),
            Value::string("compose"),
        ));

        let input = envelope(record);
        let options = NormalizationOptions {
            profile: NormalizationProfile::Docker,
            compose_service_label: Some("com.docker.compose.service".into()),
            application_time_field: Some("at".into()),
            application_time_format: Some("unix_seconds".into()),
            oldest_time: 10_000,
            newest_time: 20_000,
            ..NormalizationOptions::default()
        };
        let event = normalize_envelope(&input, &options).unwrap().remove(0);
        assert_eq!(event.service.as_deref(), Some("compose"));
        assert_eq!(event.event_time, 12_000);
        assert_eq!(event.attributes["forward"]["seconds"], 1);
        assert_eq!(
            event.attributes["logbrook"]["application_time_status"],
            "applied"
        );

        let options = NormalizationOptions {
            service: Some("route".into()),
            ..options
        };
        assert_eq!(
            normalize_envelope(&input, &options).unwrap()[0]
                .service
                .as_deref(),
            Some("route")
        );
    }

    #[test]
    fn generic_application_time_invalid_overflow_and_retention_fallbacks() {
        let options = NormalizationOptions {
            application_time_field: Some("at".into()),
            application_time_format: Some("unix_ms".into()),
            oldest_time: 100,
            newest_time: 200,
            ..NormalizationOptions::default()
        };
        for (value, expected, status) in [
            (Value::Integer(150), 150, "applied"),
            (Value::Integer(99), 1123, "fallback_out_of_policy"),
            (Value::Integer(201), 1123, "fallback_out_of_policy"),
            (
                Value::Integer(i128::MAX),
                1123,
                "fallback_invalid_or_missing",
            ),
            (Value::string("150"), 1123, "fallback_invalid_or_missing"),
        ] {
            let input = envelope(fields(&[("msg", Value::string("event")), ("at", value)]));
            let event = normalize_envelope(&input, &options).unwrap().remove(0);
            assert_eq!(event.event_time, expected);
            assert_eq!(
                event.attributes["logbrook"]["application_time_status"],
                status
            );
        }
    }

    #[test]
    fn message_output_depth_and_whole_envelope_limits() {
        let input = envelope(fields(&[("msg", Value::string("abc"))]));
        let options = NormalizationOptions {
            max_message_bytes: 2,
            ..NormalizationOptions::default()
        };
        assert!(normalize_envelope(&input, &options).is_err());

        let options = NormalizationOptions {
            max_normalized_bytes: 1,
            ..NormalizationOptions::default()
        };
        assert!(normalize_envelope(&input, &options).is_err());

        let options = NormalizationOptions {
            profile: NormalizationProfile::Docker,
            max_json_depth: 1,
            ..NormalizationOptions::default()
        };
        let event = normalize_envelope(
            &envelope(docker("{\"msg\":\"ok\",\"nested\":{}}")),
            &options,
        )
        .unwrap()
        .remove(0);
        assert_eq!(
            event.attributes["logbrook"]["parse_status"],
            "json_depth_limit"
        );

        let mut input = input;
        input.events = vec![input.events[0].clone(); 10];

        let options = NormalizationOptions {
            max_normalized_bytes: 10_000,
            ..NormalizationOptions::default()
        };
        assert!(normalize_envelope(&input, &options).is_err());
    }

    fn top_level(profile: NormalizationProfile, json_profile: JsonProfile) -> NormalizationOptions {
        NormalizationOptions {
            profile,
            json_profile,
            application_attributes: ApplicationAttributes::TopLevel,
            ..NormalizationOptions::default()
        }
    }

    fn nested_of(options: &NormalizationOptions) -> NormalizationOptions {
        NormalizationOptions {
            application_attributes: ApplicationAttributes::Nested,
            ..options.clone()
        }
    }

    /// Asserts that `top_level` leaves the event as `nested` produces it, with no marker.
    fn assert_unlifted(record: Value, options: &NormalizationOptions) -> NormalizedEvent {
        let lifted = normalize_envelope(&envelope(record.clone()), options)
            .unwrap()
            .remove(0);
        let nested = normalize_envelope(&envelope(record), &nested_of(options))
            .unwrap()
            .remove(0);
        assert_eq!(lifted.attributes, nested.attributes);
        assert!(
            lifted.attributes["forward"]
                .get("application_attributes")
                .is_none()
        );
        assert!(lifted.attributes.get("container_id").is_none());
        assert!(lifted.attributes.get("log").is_none());
        lifted
    }

    #[test]
    fn top_level_docker_json_lines_store_application_fields_beside_logbrook_attributes() {
        let options = top_level(NormalizationProfile::Docker, JsonProfile::JsonAuto);
        let record = docker(r#"{"msg":"x","runId":"r1","actorId":"a1"}"#);
        let lifted = normalize_envelope(&envelope(record.clone()), &options)
            .unwrap()
            .remove(0);
        let nested = normalize_envelope(&envelope(record), &nested_of(&options))
            .unwrap()
            .remove(0);

        let attributes = &lifted.attributes;
        assert_eq!(attributes["runId"], "r1");
        assert_eq!(attributes["actorId"], "a1");
        assert_eq!(attributes["msg"], "x");
        assert!(attributes.get("application").is_none());
        assert_eq!(attributes["forward"]["application_attributes"], "top_level");
        assert_eq!(attributes["logbrook"]["parse_status"], "structured");
        assert_eq!(attributes["docker"], nested.attributes["docker"]);
        assert_eq!(lifted.message, nested.message);
        assert_eq!(lifted.level, nested.level);

        // The default keeps the nested layout.
        assert_eq!(nested.attributes["application"]["runId"], "r1");
        assert!(nested.attributes.get("runId").is_none());
        assert!(
            nested.attributes["forward"]
                .get("application_attributes")
                .is_none()
        );
    }

    #[test]
    fn top_level_moves_reserved_keys_to_conflicting_fields_and_keeps_application_field() {
        let options = top_level(NormalizationProfile::Docker, JsonProfile::JsonAuto);
        let line = r#"{"msg":"x","application":"app","docker":"y","logbrook":"z","forward":"w"}"#;
        let event = normalize_envelope(&envelope(docker(line)), &options)
            .unwrap()
            .remove(0);
        let attributes = &event.attributes;

        assert_eq!(attributes["application"], "app");
        assert_eq!(
            attributes["logbrook"]["conflicting_fields"],
            json!({"docker": "y", "logbrook": "z", "forward": "w"})
        );
        assert_eq!(attributes["logbrook"]["parse_status"], "structured");
        assert_eq!(attributes["docker"]["container_id"], "abc");
        assert_eq!(attributes["docker"]["stream"], "stderr");
        assert_eq!(attributes["forward"]["tag"], "configured.tag");
        assert_eq!(attributes["forward"]["application_attributes"], "top_level");
    }

    #[test]
    fn top_level_generic_route_moves_natural_json_fields() {
        let options = top_level(NormalizationProfile::Generic, JsonProfile::Text);
        let record = fields(&[
            ("message", Value::string("hello")),
            ("runId", Value::string("r1")),
            ("logbrook", Value::string("mine")),
        ]);
        let event = normalize_envelope(&envelope(record), &options)
            .unwrap()
            .remove(0);

        assert_eq!(event.message, "hello");
        assert_eq!(event.attributes["runId"], "r1");
        assert_eq!(event.attributes["message"], "hello");
        assert!(event.attributes.get("application").is_none());
        assert_eq!(
            event.attributes["logbrook"],
            json!({"conflicting_fields": {"logbrook": "mine"}})
        );
        assert_eq!(
            event.attributes["forward"]["application_attributes"],
            "top_level"
        );
    }

    #[test]
    fn top_level_keeps_existing_logbrook_object_when_adding_conflicts() {
        let options = NormalizationOptions {
            application_time_field: Some("ts".into()),
            application_time_format: Some("unix_ms".into()),
            ..top_level(NormalizationProfile::Generic, JsonProfile::Text)
        };
        let record = fields(&[
            ("ts", Value::Integer(1_700_000_000_123)),
            ("docker", Value::string("d")),
        ]);
        let event = normalize_envelope(&envelope(record), &options)
            .unwrap()
            .remove(0);

        assert_eq!(event.event_time, 1_700_000_000_123);
        assert_eq!(event.attributes["ts"], 1_700_000_000_123_i64);
        assert_eq!(
            event.attributes["logbrook"]["application_time_status"],
            "applied"
        );
        assert_eq!(
            event.attributes["logbrook"]["conflicting_fields"],
            json!({"docker": "d"})
        );
    }

    #[test]
    fn top_level_leaves_typed_fallback_records_nested() {
        // Duplicate keys need the typed projection.
        let record = Value::Map(vec![
            (Value::string("x"), Value::Integer(1)),
            (Value::string("x"), Value::Integer(2)),
        ]);
        let options = top_level(NormalizationProfile::Generic, JsonProfile::Text);
        let event = assert_unlifted(record, &options);
        assert_eq!(
            event.attributes["forward"]["record_encoding"],
            "msgpack_typed_v1"
        );
        assert!(event.attributes["application"].is_array());
    }

    #[test]
    fn top_level_leaves_docker_raw_records_nested() {
        let options = top_level(NormalizationProfile::Docker, JsonProfile::JsonAuto);

        let event = assert_unlifted(docker("raw\nstack"), &options);
        assert_eq!(event.attributes["logbrook"]["parse_status"], "text");

        let text = top_level(NormalizationProfile::Docker, JsonProfile::Text);
        assert_unlifted(docker(r#"{"msg":"x","runId":"r1"}"#), &text);

        let mut fragment = docker(r#"{"msg":"partial"#);
        let Value::Map(ref mut entries) = fragment else {
            panic!("map")
        };
        entries.extend(
            [
                ("partial_id", Value::string("id")),
                ("partial_ordinal", Value::string("2")),
                ("partial_last", Value::string("false")),
            ]
            .into_iter()
            .map(|(k, v)| (Value::string(k), v)),
        );
        let event = assert_unlifted(fragment, &options);
        assert_eq!(event.attributes["logbrook"]["parse_status"], "fragment");

        let event = assert_unlifted(fields(&[("message", Value::string("m"))]), &options);
        assert_eq!(
            event.attributes["logbrook"]["docker_status"],
            "generic_fallback"
        );

        let shallow = NormalizationOptions {
            max_json_depth: 1,
            ..options
        };
        let event = assert_unlifted(docker(r#"{"msg":"ok","nested":{}}"#), &shallow);
        assert_eq!(
            event.attributes["logbrook"]["parse_status"],
            "json_depth_limit"
        );
    }

    #[test]
    fn top_level_leaves_docker_parsed_line_nested_when_record_is_typed() {
        // A binary field makes the record typed, and the parsed line goes to the docker object.
        let mut record = docker(r#"{"msg":"x","runId":"r1"}"#);
        let Value::Map(ref mut entries) = record else {
            panic!("map")
        };
        entries.push((Value::string("blob"), Value::Binary(vec![0, 255])));
        let options = top_level(NormalizationProfile::Docker, JsonProfile::JsonAuto);
        let event = assert_unlifted(record, &options);
        assert_eq!(
            event.attributes["forward"]["record_encoding"],
            "msgpack_typed_v1"
        );
        assert_eq!(
            event.attributes["docker"]["parsed_application"]["runId"],
            "r1"
        );
        assert!(event.attributes.get("runId").is_none());
    }

    #[test]
    fn top_level_applies_application_time_on_docker_routes() {
        let options = NormalizationOptions {
            application_time_field: Some("ts".into()),
            application_time_format: Some("unix_ms".into()),
            ..top_level(NormalizationProfile::Docker, JsonProfile::JsonAuto)
        };
        let event = normalize_envelope(
            &envelope(docker(r#"{"msg":"x","ts":1700000000123,"runId":"r1"}"#)),
            &options,
        )
        .unwrap()
        .remove(0);

        assert_eq!(event.event_time, 1_700_000_000_123);
        assert_eq!(
            event.attributes["logbrook"]["application_time_status"],
            "applied"
        );
        assert_eq!(event.attributes["runId"], "r1");
        assert_eq!(event.attributes["ts"], 1_700_000_000_123_i64);
    }

    fn event_with(attributes: Json) -> NormalizedEvent {
        NormalizedEvent {
            event_time: 0,
            level: 0,
            service: None,
            logger: None,
            host: None,
            pid: None,
            message: String::new(),
            attributes,
        }
    }

    #[test]
    fn lifting_skips_events_with_unexpected_attribute_shapes_without_panicking() {
        let cases = [
            // `logbrook` is not an object.
            json!({"forward": {}, "logbrook": "text", "application": {"docker": "d", "a": 1}}),
            // `forward` is missing.
            json!({"application": {"docker": "d", "a": 1}}),
            // `forward` is not an object.
            json!({"forward": 7, "application": {"a": 1}}),
            // `conflicting_fields` is not an object.
            json!({
                "forward": {},
                "logbrook": {"conflicting_fields": []},
                "application": {"docker": "d", "a": 1}
            }),
            // The same on the Docker profile, where the `parse_status` check passes first.
            json!({
                "forward": {},
                "logbrook": {"parse_status": "structured", "conflicting_fields": []},
                "application": {"docker": "d", "a": 1}
            }),
        ];
        for attributes in cases {
            for profile in [NormalizationProfile::Generic, NormalizationProfile::Docker] {
                let mut event = event_with(attributes.clone());
                lift_application_attributes(profile, &mut event);
                assert_eq!(event.attributes, attributes);
            }
        }
    }

    #[test]
    fn top_level_docker_json_without_message_lines_are_lifted() {
        let options = top_level(NormalizationProfile::Docker, JsonProfile::JsonAuto);
        let event = normalize_envelope(&envelope(docker(r#"{"runId":"r1"}"#)), &options)
            .unwrap()
            .remove(0);

        assert_eq!(
            event.attributes["logbrook"]["parse_status"],
            "json_without_message"
        );
        assert_eq!(event.attributes["runId"], "r1");
        assert!(event.attributes.get("application").is_none());
        assert_eq!(
            event.attributes["forward"]["application_attributes"],
            "top_level"
        );
    }

    #[test]
    fn top_level_docker_text_profile_with_application_time_field_is_not_lifted() {
        let options = NormalizationOptions {
            application_time_field: Some("ts".into()),
            application_time_format: Some("unix_ms".into()),
            ..top_level(NormalizationProfile::Docker, JsonProfile::Text)
        };
        let event = assert_unlifted(docker(r#"{"ts":1700000000123,"runId":"r1"}"#), &options);

        // A `logbrook` object exists, with the time status only.
        assert!(
            event.attributes["logbrook"]
                .get("application_time_status")
                .is_some()
        );
        assert!(event.attributes["logbrook"].get("parse_status").is_none());
        assert!(event.attributes.get("runId").is_none());
    }

    #[test]
    fn top_level_generic_field_named_application_is_stored_as_application() {
        let options = top_level(NormalizationProfile::Generic, JsonProfile::Text);
        let record = fields(&[
            ("message", Value::string("hello")),
            ("application", Value::string("tv-archive")),
        ]);
        let event = normalize_envelope(&envelope(record), &options)
            .unwrap()
            .remove(0);

        assert_eq!(event.attributes["application"], "tv-archive");
        assert_eq!(
            event.attributes["forward"]["application_attributes"],
            "top_level"
        );
        assert!(event.attributes.get("logbrook").is_none());
    }
}
