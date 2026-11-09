//! Deterministic, query-bound continuations shared by HTTP and storage.
//!
//! A continuation is a self-describing token: an envelope naming the cursor kind, the position,
//! and a fingerprint of the query it belongs to. It is accepted only for the same query, so a
//! client cannot reuse a page against different filters.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fmt::Write;

use crate::{
    attribute_filter::AttributeValue,
    config::DEFAULT_INDEX_NAME,
    model::{Error, Query},
};

/// Longest accepted continuation token, in bytes, including any index-name prefix.
pub const MAX_CURSOR_BYTES: usize = 1_024;

/// Version of the envelope and of the query identity. A change here invalidates old tokens.
const CURSOR_VERSION: u8 = 1;

/// Marks a token whose payload is the versioned, base64url-encoded envelope.
const VERSION_PREFIX: &str = "v1.";

/// Hex digits in a SHA-256 digest, the length of a fingerprint.
const SHA256_HEX_LEN: usize = 64;

/// Which index a continuation belongs to, which decides how its token is prefixed.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum CursorScope {
    /// Internal calls with no index name: the token has no prefix.
    #[default]
    Internal,
    /// Requests on the unscoped routes: the token has no prefix.
    Legacy,
    /// A request on a named index: the token is prefixed with the index name.
    Named(String),
}

/// The two endpoints that issue continuations, and the ordering each one walks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CursorKind {
    /// Newest first by event time, then id.
    Search,
    /// Oldest first by ingestion id, for the live tail.
    Tail,
}

impl CursorKind {
    /// The kind name stored in the envelope.
    fn name(self) -> &'static str {
        match self {
            Self::Search => "search",
            Self::Tail => "tail",
        }
    }

    /// The ordering label that is part of the query identity.
    fn ordering(self) -> &'static str {
        match self {
            Self::Search => "search:event_time_desc,id_desc",
            Self::Tail => "tail:id_asc",
        }
    }
}

/// Where a search page ended, in the three coordinates the search orders by.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SearchPosition {
    /// The ingestion high-water mark the search was pinned to.
    pub high_water_id: i64,
    /// Event time of the last event on the page.
    pub last_event_time: i64,
    /// Event id of the last event on the page.
    pub last_event_id: i64,
}

/// A query parameter as an identity element: `["some", value]`, or `["none"]` when absent.
fn optional<T: Serialize>(value: Option<&T>) -> Value {
    match value {
        Some(value) => json!(["some", value]),
        None => json!(["none"]),
    }
}

/// The scope element of the identity, so the same filters under two indexes never share a token.
fn scope_identity(scope: &CursorScope) -> Value {
    match scope {
        CursorScope::Internal => json!(["internal"]),
        CursorScope::Legacy => json!(["legacy", DEFAULT_INDEX_NAME]),
        CursorScope::Named(name) => json!(["named", name]),
    }
}

/// The source element of the identity. Sources are sorted and de-duplicated, so their order in
/// the request does not change the token.
fn source_identity(sources: &[String]) -> Value {
    let mut sources = sources.to_vec();
    sources.sort();
    sources.dedup();
    if sources.is_empty() {
        json!(["all"])
    } else {
        json!(["only", sources])
    }
}

/// The attribute conditions as identity elements. A condition's value keeps its type, so `1`,
/// `"1"` and `true` never collide.
fn attribute_identity(query: &Query) -> Vec<Value> {
    query
        .attributes
        .conditions()
        .iter()
        .map(|condition| {
            let value = match condition.value() {
                AttributeValue::String(value) => json!(["string", value]),
                AttributeValue::Boolean(value) => json!(["boolean", value]),
                AttributeValue::Integer(value) => json!(["integer", value.to_string()]),
                AttributeValue::Float(bits) => json!(["float", format!("{bits:016x}")]),
            };
            json!([condition.key(), value])
        })
        .collect()
}

/// The time window and page size, which only search pages depend on.
fn window_identity(query: &Query, kind: CursorKind) -> Value {
    match kind {
        CursorKind::Search => json!([
            query.from.to_string(),
            query.to.to_string(),
            query.limit.to_string()
        ]),
        CursorKind::Tail => json!([]),
    }
}

/// The canonical JSON of everything that defines a query's results, for one cursor kind.
/// Equivalent requests produce the same bytes, and any change that alters results changes them.
fn identity(query: &Query, kind: CursorKind) -> Result<Vec<u8>, Error> {
    query.attributes.validate(usize::MAX)?;

    let selection = json!([
        CURSOR_VERSION,
        kind.ordering(),
        scope_identity(&query.cursor_scope),
        source_identity(&query.sources),
        optional(query.service.as_ref()),
        optional(query.logger.as_ref()),
        optional(query.host.as_ref()),
        optional(query.message.as_ref()),
        optional(query.min_level.as_ref()),
        attribute_identity(query),
        window_identity(query, kind),
    ]);
    serde_json::to_vec(&selection)
        .map_err(|error| Error::invalid("cannot encode cursor query").with_source(error))
}

/// The SHA-256 of a query's identity, as lowercase hex. Envelopes carry it to bind a token to
/// its query.
///
/// # Errors
///
/// Fails when the query's attribute filter is not canonical.
pub fn fingerprint(query: &Query, kind: CursorKind) -> Result<String, Error> {
    let digest = Sha256::digest(identity(query, kind)?);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(output, "{byte:02x}");
    }

    Ok(output)
}

/// The search continuation's payload.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SearchEnvelope {
    v: u8,
    kind: String,
    query: String,
    high: String,
    time: String,
    id: String,
}

/// The tail continuation's payload.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TailEnvelope {
    v: u8,
    kind: String,
    query: String,
    after: String,
}

/// Encodes an envelope, prefixed with the index name when the scope has one.
fn encode(scope: &CursorScope, envelope: &impl Serialize) -> Result<String, Error> {
    let payload = serde_json::to_vec(envelope)
        .map_err(|error| Error::invalid("cannot encode cursor").with_source(error))?;
    let inner = format!("{VERSION_PREFIX}{}", URL_SAFE_NO_PAD.encode(payload));
    let token = match scope {
        CursorScope::Named(name) => format!("{name}:{inner}"),
        CursorScope::Legacy | CursorScope::Internal => inner,
    };
    validate_token_size(&token)?;
    Ok(token)
}

/// Rejects a token longer than `MAX_CURSOR_BYTES`.
///
/// # Errors
///
/// Fails when the token is longer than `MAX_CURSOR_BYTES`.
pub fn validate_token_size(token: &str) -> Result<(), Error> {
    if token.len() > MAX_CURSOR_BYTES {
        return Err(Error::invalid(format!(
            "cursor exceeds {MAX_CURSOR_BYTES} bytes"
        )));
    }

    Ok(())
}

/// Strips the index prefix and the version marker, then base64-decodes the payload.
fn decode(query: &Query, token: &str) -> Result<Vec<u8>, Error> {
    validate_token_size(token)?;

    let inner = match &query.cursor_scope {
        CursorScope::Named(name) => token
            .strip_prefix(&format!("{name}:"))
            .ok_or_else(|| Error::invalid("cursor belongs to another index or route"))?,
        CursorScope::Legacy | CursorScope::Internal => token,
    };
    let Some(encoded) = inner.strip_prefix(VERSION_PREFIX) else {
        return Err(Error::invalid(
            "unsupported or legacy cursor; restart search or reconcile retained history \
             before restarting tail",
        ));
    };
    URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|error| Error::invalid("invalid cursor encoding").with_source(error))
}

/// Checks an envelope's version, kind and query fingerprint against the query it arrived with.
fn validate_envelope(
    query: &Query,
    kind: CursorKind,
    version: u8,
    actual_kind: &str,
    digest: &str,
) -> Result<(), Error> {
    if version != CURSOR_VERSION || actual_kind != kind.name() {
        return Err(Error::invalid("unsupported cursor version or kind"));
    }

    if digest.len() != SHA256_HEX_LEN
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::invalid("invalid cursor query fingerprint"));
    }

    if digest != fingerprint(query, kind)? {
        return Err(Error::invalid("cursor does not match query"));
    }

    Ok(())
}

/// Parses a position that must be the canonical decimal form of an `i64`.
fn decimal(value: &str) -> Result<i64, Error> {
    let number = value
        .parse::<i64>()
        .map_err(|error| Error::invalid("invalid cursor position").with_source(error))?;
    if number.to_string() != value {
        return Err(Error::invalid("noncanonical cursor position"));
    }

    Ok(number)
}

/// Encodes the continuation that resumes a search after `position`.
///
/// # Errors
///
/// Fails when the query's fingerprint cannot be computed or the token exceeds `MAX_CURSOR_BYTES`.
pub fn encode_search(query: &Query, position: SearchPosition) -> Result<String, Error> {
    encode(
        &query.cursor_scope,
        &SearchEnvelope {
            v: CURSOR_VERSION,
            kind: CursorKind::Search.name().to_owned(),
            query: fingerprint(query, CursorKind::Search)?,
            high: position.high_water_id.to_string(),
            time: position.last_event_time.to_string(),
            id: position.last_event_id.to_string(),
        },
    )
}

/// Decodes a search continuation for `query`.
///
/// # Errors
///
/// Fails when the token is malformed, belongs to another index, or was issued for a different
/// query.
pub fn decode_search(query: &Query, token: &str) -> Result<SearchPosition, Error> {
    let envelope: SearchEnvelope = serde_json::from_slice(&decode(query, token)?)
        .map_err(|error| Error::invalid("invalid search cursor envelope").with_source(error))?;
    validate_envelope(
        query,
        CursorKind::Search,
        envelope.v,
        &envelope.kind,
        &envelope.query,
    )?;

    Ok(SearchPosition {
        high_water_id: decimal(&envelope.high)?,
        last_event_time: decimal(&envelope.time)?,
        last_event_id: decimal(&envelope.id)?,
    })
}

/// A tail subscriber can reuse its normalized query identity for every frame.
#[derive(Clone, Debug)]
pub struct TailCursor {
    scope: CursorScope,
    fingerprint: String,
}

impl TailCursor {
    /// Computes the query identity once, so each frame only encodes its position.
    ///
    /// # Errors
    ///
    /// Fails when the query's attribute filter is not canonical.
    pub fn new(query: &Query) -> Result<Self, Error> {
        Ok(Self {
            scope: query.cursor_scope.clone(),
            fingerprint: fingerprint(query, CursorKind::Tail)?,
        })
    }

    /// Encodes the continuation that resumes the tail after `after_ingestion_id`.
    ///
    /// # Errors
    ///
    /// Fails when the token exceeds `MAX_CURSOR_BYTES`.
    pub fn encode(&self, after_ingestion_id: i64) -> Result<String, Error> {
        encode(
            &self.scope,
            &TailEnvelope {
                v: CURSOR_VERSION,
                kind: CursorKind::Tail.name().to_owned(),
                query: self.fingerprint.clone(),
                after: after_ingestion_id.to_string(),
            },
        )
    }

    /// The length in bytes of the token `encode` would produce.
    ///
    /// # Errors
    ///
    /// Fails when the token cannot be encoded.
    pub fn token_len(&self, after_ingestion_id: i64) -> Result<usize, Error> {
        Ok(self.encode(after_ingestion_id)?.len())
    }
}

/// Encodes a tail continuation for `query` in one step. Prefer `TailCursor` in loops.
///
/// # Errors
///
/// Fails when the query's identity cannot be computed or the token is too long.
pub fn encode_tail(query: &Query, after_ingestion_id: i64) -> Result<String, Error> {
    TailCursor::new(query)?.encode(after_ingestion_id)
}

/// Decodes a tail continuation for `query` and returns the ingestion id to resume after.
///
/// # Errors
///
/// Fails when the token is malformed, belongs to another index, or was issued for a different
/// query.
pub fn decode_tail(query: &Query, token: &str) -> Result<i64, Error> {
    let envelope: TailEnvelope = serde_json::from_slice(&decode(query, token)?)
        .map_err(|error| Error::invalid("invalid tail cursor envelope").with_source(error))?;
    validate_envelope(
        query,
        CursorKind::Tail,
        envelope.v,
        &envelope.kind,
        &envelope.query,
    )?;

    decimal(&envelope.after)
}

/// The length in bytes of the tail token `encode_tail` would produce.
///
/// # Errors
///
/// Fails when the token cannot be encoded.
pub fn tail_token_len(query: &Query, after_ingestion_id: i64) -> Result<usize, Error> {
    Ok(encode_tail(query, after_ingestion_id)?.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attribute_filter::AttributeFilters;

    fn query() -> Query {
        Query {
            from: 10,
            to: 100,
            limit: 2,
            ..Query::default()
        }
    }

    fn token(payload: &str) -> String {
        format!("v1.{}", URL_SAFE_NO_PAD.encode(payload))
    }

    #[test]
    fn canonical_identity_golden() {
        let q = query();
        assert_eq!(
            String::from_utf8(identity(&q, CursorKind::Search).unwrap()).unwrap(),
            concat!(
                r#"[1,"search:event_time_desc,id_desc",["internal"],["all"],"#,
                r#"["none"],["none"],["none"],["none"],["none"],[],["10","100","2"]]"#,
            )
        );
        assert_eq!(
            fingerprint(&q, CursorKind::Search).unwrap(),
            "49b3f86267593e7ba22912a3c0d1d414f7a341472f5429ff65d1fe9e0757f727"
        );
        assert_eq!(
            fingerprint(&q, CursorKind::Tail).unwrap(),
            "71728cd436ed423d5bc00deafe8312b382982e6144954ec7a0a62ec1f698b9ef"
        );
    }

    #[test]
    fn envelope_round_trip_and_golden() {
        let q = query();
        let position = SearchPosition {
            high_water_id: 30,
            last_event_time: 50,
            last_event_id: 20,
        };
        let search = encode_search(&q, position).unwrap();
        assert_eq!(decode_search(&q, &search).unwrap(), position);
        let tail = encode_tail(&q, 20).unwrap();
        assert_eq!(decode_tail(&q, &tail).unwrap(), 20);
        assert_eq!(tail_token_len(&q, 20).unwrap(), tail.len());

        let payload = String::from_utf8(
            URL_SAFE_NO_PAD
                .decode(tail.strip_prefix("v1.").unwrap())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            payload,
            concat!(
                r#"{"v":1,"kind":"tail","query":""#,
                "71728cd436ed423d5bc00deafe8312b382982e6144954ec7a0a62ec1f698b9ef",
                r#"","after":"20"}"#,
            )
        );
    }

    #[test]
    fn equivalent_normalized_selections_share_identity() {
        let mut first = query();
        first.sources = vec!["b".into(), "a".into(), "a".into()];
        first.attributes = AttributeFilters::parse(
            concat!(
                r#"{"attributes":[{"key":"z","op":"eq","value":123.0},"#,
                r#"{"key":"a","op":"eq","value":true},"#,
                r#"{"key":"z","op":"eq","value":123}]}"#,
            ),
            1024,
        )
        .unwrap();

        let mut second = first.clone();
        second.sources = vec!["a".into(), "b".into()];
        second.attributes = AttributeFilters::parse(
            concat!(
                r#"{"attributes":[{"key":"a","op":"eq","value":true},"#,
                r#"{"key":"z","op":"eq","value":1.23e2}]}"#,
            ),
            1024,
        )
        .unwrap();

        assert_eq!(
            fingerprint(&first, CursorKind::Search).unwrap(),
            fingerprint(&second, CursorKind::Search).unwrap()
        );

        second.cursor = Some("ignored".into());
        assert_eq!(
            fingerprint(&first, CursorKind::Search).unwrap(),
            fingerprint(&second, CursorKind::Search).unwrap()
        );
    }

    #[test]
    fn every_selection_dimension_is_bound() {
        let q = query();
        let search = encode_search(
            &q,
            SearchPosition {
                high_water_id: 4,
                last_event_time: 50,
                last_event_id: 3,
            },
        )
        .unwrap();
        let tail = encode_tail(&q, 3).unwrap();

        let mut variants = Vec::new();
        let mut changed = q.clone();
        changed.cursor_scope = CursorScope::Legacy;
        variants.push(changed);
        let mut changed = q.clone();
        changed.cursor_scope = CursorScope::Named("default".into());
        variants.push(changed);
        let mut changed = q.clone();
        changed.sources.push("source".into());
        variants.push(changed);
        let mut changed = q.clone();
        changed.service = Some("service".into());
        variants.push(changed);
        let mut changed = q.clone();
        changed.logger = Some("logger".into());
        variants.push(changed);
        let mut changed = q.clone();
        changed.host = Some("host".into());
        variants.push(changed);
        let mut changed = q.clone();
        changed.message = Some(String::new());
        variants.push(changed);
        let mut changed = q.clone();
        changed.min_level = Some(0);
        variants.push(changed);
        let mut changed = q.clone();
        changed.attributes =
            AttributeFilters::parse(r#"{"attributes":[{"key":"a","op":"eq","value":1}]}"#, 1024)
                .unwrap();
        variants.push(changed);

        for changed in variants {
            assert!(decode_search(&changed, &search).is_err());
            assert!(decode_tail(&changed, &tail).is_err());
        }

        for changed in [
            Query {
                from: 11,
                ..q.clone()
            },
            Query {
                to: 101,
                ..q.clone()
            },
            Query {
                limit: 3,
                ..q.clone()
            },
        ] {
            assert!(decode_search(&changed, &search).is_err());
            assert_eq!(decode_tail(&changed, &tail).unwrap(), 3);
        }
    }

    #[test]
    fn attribute_key_value_and_type_changes_reject_continuations() {
        let mut q = query();
        q.attributes = AttributeFilters::parse(
            r#"{"attributes":[{"key":"run","op":"eq","value":123}]}"#,
            1024,
        )
        .unwrap();

        let search = encode_search(
            &q,
            SearchPosition {
                high_water_id: 4,
                last_event_time: 50,
                last_event_id: 3,
            },
        )
        .unwrap();
        let tail = encode_tail(&q, 3).unwrap();

        for filter in [
            r#"{"attributes":[{"key":"other","op":"eq","value":123}]}"#,
            r#"{"attributes":[{"key":"run","op":"eq","value":124}]}"#,
            r#"{"attributes":[{"key":"run","op":"eq","value":"123"}]}"#,
            r#"{"attributes":[]}"#,
        ] {
            let mut changed = q.clone();
            changed.attributes = AttributeFilters::parse(filter, 1024).unwrap();
            assert!(decode_search(&changed, &search).is_err());
            assert!(decode_tail(&changed, &tail).is_err());
        }

        let mut equivalent = q.clone();
        equivalent.attributes = AttributeFilters::parse(
            r#"{"attributes":[{"key":"run","op":"eq","value":123.0}]}"#,
            1024,
        )
        .unwrap();

        assert!(decode_search(&equivalent, &search).is_ok());
        assert_eq!(decode_tail(&equivalent, &tail).unwrap(), 3);

        let mut empty = query();
        empty.attributes = AttributeFilters::parse(r#"{"attributes":[]}"#, 1024).unwrap();
        assert_eq!(
            fingerprint(&empty, CursorKind::Search).unwrap(),
            fingerprint(&query(), CursorKind::Search).unwrap()
        );
    }

    #[test]
    fn scalar_types_and_exact_integers_are_distinct() {
        let mut fingerprints = Vec::new();
        for value in [
            r#""1""#,
            "true",
            "1",
            "1.5",
            "9007199254740992",
            "9007199254740993",
        ] {
            let mut q = query();
            q.attributes = AttributeFilters::parse(
                &format!(r#"{{"attributes":[{{"key":"a","op":"eq","value":{value}}}]}}"#),
                1024,
            )
            .unwrap();
            fingerprints.push(fingerprint(&q, CursorKind::Search).unwrap());
        }

        fingerprints.sort();
        fingerprints.dedup();
        assert_eq!(fingerprints.len(), 6);
    }

    #[test]
    fn rejects_malformed_envelopes_and_legacy_tokens() {
        let q = query();
        let digest = fingerprint(&q, CursorKind::Tail).unwrap();
        for after in ["01", "+1", "-0", " 1", "9223372036854775808"] {
            assert!(
                decode_tail(
                    &q,
                    &token(&format!(
                        r#"{{"v":1,"kind":"tail","query":"{digest}","after":"{after}"}}"#
                    ))
                )
                .is_err()
            );
        }

        for payload in [
            format!(r#"{{"v":1,"v":1,"kind":"tail","query":"{digest}","after":"1"}}"#),
            format!(r#"{{"v":1,"kind":"tail","query":"{digest}","after":"1","extra":0}}"#),
            format!(r#"{{"v":2,"kind":"tail","query":"{digest}","after":"1"}}"#),
            format!(r#"{{"v":1,"kind":"search","query":"{digest}","after":"1"}}"#),
            format!(
                r#"{{"v":1,"kind":"tail","query":"{}","after":"1"}}"#,
                digest.to_uppercase()
            ),
            format!(r#"{{"v":1,"kind":"tail","query":"{digest}","after":1}}"#),
        ] {
            assert!(decode_tail(&q, &token(&payload)).is_err());
        }

        for malformed in ["1", "4:50:3", "v2.e30", "v1.e30=", "v1.!", "v1.Zh", "v1."] {
            assert!(decode_tail(&q, malformed).is_err());
        }

        assert!(decode_tail(&q, &"a".repeat(MAX_CURSOR_BYTES + 1)).is_err());
        assert!(decode_search(&q, &encode_tail(&q, 1).unwrap()).is_err());
        assert!(
            decode_tail(
                &q,
                &encode_search(
                    &q,
                    SearchPosition {
                        high_water_id: 1,
                        last_event_time: 50,
                        last_event_id: 1
                    }
                )
                .unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn named_prefix_is_part_of_size_and_scope_validation() {
        let mut q = query();
        q.cursor_scope = CursorScope::Named("default".into());

        let encoded = encode_tail(&q, i64::MAX).unwrap();
        assert!(encoded.starts_with("default:v1."));
        assert_eq!(decode_tail(&q, &encoded).unwrap(), i64::MAX);

        q.cursor_scope = CursorScope::Named("other".into());
        assert!(decode_tail(&q, &encoded).is_err());

        q.cursor_scope = CursorScope::Named("x".repeat(MAX_CURSOR_BYTES));
        assert!(encode_tail(&q, 0).is_err());
    }
}
