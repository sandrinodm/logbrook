//! Validated, canonical equality conditions for literal top-level attributes.
use crate::model::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeStruct};
use serde_json::value::RawValue;

/// Most equality conditions one filter may hold.
pub const MAX_ATTRIBUTE_CONDITIONS: usize = 16;

/// Longest attribute key, in UTF-8 bytes.
pub const MAX_ATTRIBUTE_KEY_BYTES: usize = 256;

/// Longest string value, in UTF-8 bytes.
pub const MAX_ATTRIBUTE_STRING_BYTES: usize = 1_024;

/// Longest numeric token, in bytes of its JSON text.
pub const MAX_ATTRIBUTE_NUMBER_BYTES: usize = 128;

/// Longest whole filter document, in UTF-8 bytes.
pub const MAX_FILTER_BYTES: usize = 4_096;

/// Lowest whole float stored as an integer: -2^63, which `i64::MIN` represents exactly.
const INTEGER_FLOAT_MIN: f64 = -9_223_372_036_854_775_808.0;

/// First float too large to store as an integer: 2^64. `u64::MAX as f64` rounds up to this value,
/// so the bound is written out instead of derived from it.
const INTEGER_FLOAT_END: f64 = 18_446_744_073_709_551_616.0;

/// Fields in a serialized condition: `key`, `op` and `value`.
const CONDITION_FIELD_COUNT: usize = 3;

/// One literal attribute value. Floats are kept as bits so equality and hashing are exact.
///
/// Floats contain finite IEEE-754 bits; integral in-domain floats become Integer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttributeValue {
    /// Exact UTF-8 string equality.
    String(String),
    /// Exact JSON boolean equality.
    Boolean(bool),
    /// Exact whole-number equality within the accepted i64/u64 domains.
    Integer(i128),
    /// Finite IEEE-754 bits for nonintegral or out-of-integer-domain equality.
    Float(u64),
}

/// One `key = value` equality condition on a top-level attribute.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttributeCondition {
    key: String,
    value: AttributeValue,
}

impl AttributeCondition {
    /// The attribute name, exactly as the caller wrote it.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The value the attribute must equal.
    pub fn value(&self) -> &AttributeValue {
        &self.value
    }

    /// The condition's key as a JSON Pointer, with `~` and `/` escaped per RFC 6901.
    pub fn pointer(&self) -> String {
        format!("/{}", self.key.replace('~', "~0").replace('/', "~1"))
    }
}

/// A canonical set of equality conditions: sorted by key, with each key at most once.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AttributeFilters(Vec<AttributeCondition>);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFilter {
    attributes: Vec<RawCondition>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCondition {
    key: String,
    op: String,
    value: Box<RawValue>,
}

impl AttributeFilters {
    /// Parses and canonicalizes a filter document.
    ///
    /// # Errors
    ///
    /// Fails when the document is too long or malformed, has more than the allowed conditions,
    /// uses an operator other than `eq`, holds an unsupported or out-of-range value, or sets two
    /// different values for one key.
    pub fn parse(input: &str, max_field_bytes: usize) -> Result<Self, Error> {
        if input.len() > MAX_FILTER_BYTES {
            return Err(Error::invalid("filter: exceeds 4096 UTF-8 bytes"));
        }

        // Derived struct visitors reject duplicate fields as well as unknown ones.
        // Do not expose Serde's error text: it can contain submitted scalar values.
        let raw: RawFilter = serde_json::from_str(input).map_err(|error| {
            Error::invalid(
                "filter: invalid JSON structure, duplicate/unknown member, or op \
                 (only eq is supported)",
            )
            .with_source(error)
        })?;
        if raw.attributes.len() > MAX_ATTRIBUTE_CONDITIONS {
            return Err(too_many_conditions());
        }

        let conditions = raw
            .attributes
            .into_iter()
            .enumerate()
            .map(|(index, condition)| parse_condition(condition, index, max_field_bytes))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self(normalize(conditions)?))
    }

    /// The conditions in canonical order.
    pub fn conditions(&self) -> &[AttributeCondition] {
        &self.0
    }

    /// Whether the filter has no conditions.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Recheck structural invariants at storage boundaries, independently of HTTP.
    ///
    /// # Errors
    ///
    /// Fails when a condition breaks the same limits `parse` enforces, or the conditions are not
    /// in canonical order with unique keys.
    pub fn validate(&self, max_field_bytes: usize) -> Result<(), Error> {
        if self.0.len() > MAX_ATTRIBUTE_CONDITIONS {
            return Err(too_many_conditions());
        }

        for (index, condition) in self.0.iter().enumerate() {
            validate_key(&condition.key, index, max_field_bytes)?;
            if let AttributeValue::String(value) = &condition.value {
                validate_string(value, index, max_field_bytes)?;
            }

            let previous = index
                .checked_sub(1)
                .and_then(|previous| self.0.get(previous));
            if previous.is_some_and(|previous| previous.key >= condition.key) {
                return Err(Error::invalid(
                    "filter.attributes: conditions are not canonical",
                ));
            }
        }

        Ok(())
    }
}

fn too_many_conditions() -> Error {
    Error::invalid(format!(
        "filter.attributes: exceeds {MAX_ATTRIBUTE_CONDITIONS} conditions"
    ))
}

/// Parses one condition from its raw form. `index` locates it in error messages.
fn parse_condition(
    condition: RawCondition,
    index: usize,
    max_field_bytes: usize,
) -> Result<AttributeCondition, Error> {
    if condition.op != "eq" {
        return Err(field_error(index, "op", "only eq is supported"));
    }

    validate_key(&condition.key, index, max_field_bytes)?;
    let value = parse_scalar(condition.value.get(), index, max_field_bytes)?;

    Ok(AttributeCondition {
        key: condition.key,
        value,
    })
}

/// Sorts by key and merges repeated keys. A key given twice with the same value is one condition;
/// with two different values the filter cannot be satisfied, so it is rejected.
fn normalize(mut conditions: Vec<AttributeCondition>) -> Result<Vec<AttributeCondition>, Error> {
    conditions.sort_by(|a, b| a.key.cmp(&b.key));
    let mut normalized: Vec<AttributeCondition> = Vec::with_capacity(conditions.len());
    for condition in conditions {
        if let Some(previous) = normalized.last()
            && previous.key == condition.key
        {
            if previous.value != condition.value {
                return Err(Error::invalid(
                    "filter.attributes: conflicting values for the same key",
                ));
            }

            continue;
        }

        normalized.push(condition);
    }

    Ok(normalized)
}

fn field_error(index: usize, field: &str, message: &str) -> Error {
    Error::invalid(format!("filter.attributes[{index}].{field}: {message}"))
}

/// The error for a key or string longer than its limit. `limit` is the effective limit in bytes.
fn too_long(index: usize, field: &str, limit: usize) -> Error {
    field_error(index, field, &format!("exceeds {limit} UTF-8 bytes"))
}

fn validate_key(key: &str, index: usize, configured: usize) -> Result<(), Error> {
    if key.is_empty() {
        return Err(field_error(index, "key", "must not be empty"));
    }

    let limit = MAX_ATTRIBUTE_KEY_BYTES.min(configured);
    if key.len() > limit {
        return Err(too_long(index, "key", limit));
    }

    Ok(())
}

fn validate_string(value: &str, index: usize, configured: usize) -> Result<(), Error> {
    let limit = MAX_ATTRIBUTE_STRING_BYTES.min(configured);
    if value.len() > limit {
        return Err(too_long(index, "value", limit));
    }

    Ok(())
}

fn parse_scalar(raw: &str, index: usize, configured: usize) -> Result<AttributeValue, Error> {
    match raw.as_bytes().first() {
        Some(b'"') => {
            let value: String = serde_json::from_str(raw).map_err(|error| {
                field_error(index, "value", "invalid string").with_source(error)
            })?;
            validate_string(&value, index, configured)?;
            Ok(AttributeValue::String(value))
        }
        Some(b't') => Ok(AttributeValue::Boolean(true)),
        Some(b'f') => Ok(AttributeValue::Boolean(false)),
        Some(b'n') => Err(field_error(index, "value", "null is not supported")),
        Some(b'[' | b'{') => Err(field_error(
            index,
            "value",
            "objects and arrays are not supported",
        )),
        _ => parse_number(raw, index),
    }
}

/// A JSON number. Integers stay exact within i64 or u64, and a decimal becomes an integer only
/// when it is whole and in range. Everything else is kept as a float.
fn parse_number(raw: &str, index: usize) -> Result<AttributeValue, Error> {
    if raw.len() > MAX_ATTRIBUTE_NUMBER_BYTES {
        return Err(field_error(
            index,
            "value",
            &format!("exceeds {MAX_ATTRIBUTE_NUMBER_BYTES} numeric token bytes"),
        ));
    }

    if !raw.contains(['.', 'e', 'E']) {
        let integer = if raw.starts_with('-') {
            raw.parse::<i64>().map(i128::from)
        } else {
            raw.parse::<u64>().map(i128::from)
        }
        .map_err(|error| {
            field_error(index, "value", "integer is outside i64/u64 range").with_source(error)
        })?;
        return Ok(AttributeValue::Integer(integer));
    }

    // Canonical ingestion deserializes JSON with serde_json; preserve
    // that parser's precise rounding behavior for decimal tokens.
    let value = serde_json::from_str::<f64>(raw)
        .map_err(|error| field_error(index, "value", "invalid number").with_source(error))?;
    if !value.is_finite() {
        return Err(field_error(index, "value", "number must be finite"));
    }

    if value.fract() == 0.0 && (INTEGER_FLOAT_MIN..INTEGER_FLOAT_END).contains(&value) {
        // The range check makes this cast exact: the value is whole and fits in an i128.
        Ok(AttributeValue::Integer(value as i128))
    } else {
        Ok(AttributeValue::Float(value.to_bits()))
    }
}

impl Serialize for AttributeValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::String(value) => serializer.serialize_str(value),
            Self::Boolean(value) => serializer.serialize_bool(*value),
            Self::Integer(value) => serializer.serialize_i128(*value),
            Self::Float(bits) => serializer.serialize_f64(f64::from_bits(*bits)),
        }
    }
}

impl Serialize for AttributeCondition {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut condition =
            serializer.serialize_struct("AttributeCondition", CONDITION_FIELD_COUNT)?;
        condition.serialize_field("key", &self.key)?;
        condition.serialize_field("op", "eq")?;
        condition.serialize_field("value", &self.value)?;
        condition.end()
    }
}

impl Serialize for AttributeFilters {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut filter = serializer.serialize_struct("AttributeFilters", 1)?;
        filter.serialize_field("attributes", &self.0)?;
        filter.end()
    }
}

impl<'de> Deserialize<'de> for AttributeFilters {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        Self::parse(raw.get(), usize::MAX).map_err(serde::de::Error::custom)
    }
}
