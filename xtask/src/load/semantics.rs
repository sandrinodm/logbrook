//! Latency histograms, query shapes and response checks shared by the load workloads.
use crate::Result;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

/// One day in milliseconds.
pub const DAY_MS: i64 = 86_400_000;

/// Milliseconds in one second, as the integer timestamps of the API use them.
const MILLIS_PER_SEC: i64 = 1000;

/// Milliseconds in one second, for converting latencies measured as durations.
pub const MILLISECONDS_PER_SECOND: f64 = 1000.0;

/// The HTTP status of a successful request.
pub const HTTP_OK: u16 = 200;

/// Operation name of the event-ingest requests in the completion accounting.
pub const INGEST_OPERATION: &str = "ingest";

/// Operation name of the query requests in the completion accounting.
pub const QUERY_OPERATION: &str = "query";

/// Problem reported when the server accepts a different number of events than were sent.
pub const ACCEPTED_COUNT_MISMATCH: &str = "ingest accepted count does not match submitted batch";

/// Each latency bin is this much wider than the one before, so a quantile is within 2%.
const BIN_GROWTH: f64 = 1.02;

/// Bins are logarithmic in tenths of a millisecond.
const TENTHS_PER_MS: f64 = 10.0;

/// Quantiles in a latency report, as the report key and the fraction of samples at or below it.
const QUANTILES: [(&str, f64); 3] = [("p50", 0.5), ("p95", 0.95), ("p99", 0.99)];

/// The number of query shapes that `query_shape` rotates through.
const QUERY_SHAPE_COUNT: u64 = 9;

/// The `limit` that the event queries request, and the default when a response is checked.
const QUERY_LIMIT: usize = 100;

/// The span of the recent-events query.
const RECENT_SPAN_MS: i64 = 15 * 60 * 1000;

/// Bucket width that the histogram query requests, in milliseconds: one minute.
const HISTOGRAM_INTERVAL_MS: i64 = 60_000;

/// Longest padding string in one event's payload field. Further padding goes into new fields.
const MAX_PAYLOAD_FIELD_BYTES: usize = 4096;

/// Largest serialized event the generator will produce, in bytes.
const MAX_EVENT_BYTES: usize = 65536;

/// Event field that padding fills first and that trimming shortens to meet a body target.
const PAYLOAD_FIELD: &str = "payload";

/// Error when the body target would need an event larger than `MAX_EVENT_BYTES`.
const EVENT_LIMIT_ERROR: &str = "body exceeds event byte limit; increase --batch-size";

/// Error when emptying every payload still leaves the body above the target.
const BODY_TOO_SMALL_ERROR: &str = "--body-bytes is smaller than required event metadata";

/// A latency histogram: logarithmic bins with counts, plus the exact count, total and maximum.
#[derive(Default, Clone)]
pub struct Samples {
    pub bins: BTreeMap<u32, u64>,
    count: u64,
    total: f64,
    maximum: f64,
}

impl Samples {
    /// Records one latency in milliseconds. Negative values count as zero.
    pub fn add(&mut self, ms: f64) {
        let value = ms.max(0.0);
        // The float-to-integer `as` cast saturates, and a bin index for any latency fits in u32.
        let bin = ((value * TENTHS_PER_MS).ln_1p() / BIN_GROWTH.ln()).ceil() as u32;
        *self.bins.entry(bin).or_default() += 1;
        self.count += 1;
        self.total += value;
        self.maximum = self.maximum.max(value);
    }

    /// The summary: sample count, mean, maximum, the bound on quantile error, and an upper bound
    /// for each reported quantile (`null` when there are no samples).
    pub fn report(&self) -> Value {
        let mut report = json!({
            "samples": self.count,
            "mean": if self.count > 0 { Some(self.total / self.count as f64) } else { None },
            "max": self.maximum,
            "quantile_upper_error": "2% + 0.002ms"
        });
        for (name, fraction) in QUANTILES {
            let target = (self.count as f64 * fraction).ceil().max(1.0) as u64;
            let mut cumulative = 0;
            let bucket = self.bins.iter().find(|(_, count)| {
                cumulative += **count;
                cumulative >= target
            });
            report[name] = bucket
                .map(|(bucket, _)| json!(bin_upper_bound_ms(*bucket)))
                .unwrap_or(Value::Null);
        }

        report
    }
}

/// The upper bound in milliseconds of a latency bin.
fn bin_upper_bound_ms(bin: u32) -> f64 {
    (f64::from(bin) * BIN_GROWTH.ln()).exp_m1() / TENTHS_PER_MS
}

/// Encodes events as a JSON array, or as NDJSON when `shape` is `ndjson`.
fn encode(events: &[Value], shape: &str) -> Result<Vec<u8>> {
    if shape != "ndjson" {
        return Ok(serde_json::to_vec(events)?);
    }

    let mut out = Vec::new();
    for event in events {
        serde_json::to_writer(&mut out, event)?;
        out.push(b'\n');
    }
    Ok(out)
}

/// Removes `amount` bytes of payload from the events, spreading the removal over them.
fn trim(events: &mut [Value], mut amount: usize) -> Result<()> {
    let len = events.len();
    for (position, event) in events.iter_mut().enumerate() {
        let payload = event[PAYLOAD_FIELD].as_str().unwrap_or_default();
        let remove = payload.len().min(amount.div_ceil(len - position));
        event[PAYLOAD_FIELD] = json!(&payload[..payload.len() - remove]);
        amount -= remove;
    }

    if amount > 0 {
        return Err(BODY_TOO_SMALL_ERROR.into());
    }
    Ok(())
}

/// Grows the encoded body to `target` bytes by padding payloads, and by adding padding fields
/// once a payload is full.
fn pad_to(events: &mut [Value], shape: &str, target: usize, mut body: Vec<u8>) -> Result<Vec<u8>> {
    let mut field = PAYLOAD_FIELD.to_string();
    let mut field_index = 0;
    while body.len() < target {
        let mut difference = target - body.len();
        let len = events.len();
        for (position, event) in events.iter_mut().enumerate() {
            let old = event[&field].as_str().unwrap_or_default();
            let add = MAX_PAYLOAD_FIELD_BYTES
                .saturating_sub(old.len())
                .min(difference.div_ceil(len - position));
            event[&field] = json!(format!("{old}{}", "x".repeat(add)));
            difference -= add;
        }

        body = encode(events, shape)?;
        if body.len() == target {
            break;
        }

        field = format!("benchmark_padding_{field_index}");
        field_index += 1;
        for event in events.iter_mut() {
            event[&field] = json!("");
        }

        body = encode(events, shape)?;
        if body.len() > target {
            trim(events, body.len() - target)?;
            body = encode(events, shape)?;
        }
    }
    Ok(body)
}

/// Encodes events as a JSON array or NDJSON, padded or trimmed so the body is `target` bytes
/// (0 leaves it unpadded). Returns the body and its content type.
///
/// # Errors
///
/// Returns an error when the events cannot be encoded, when `target` needs more than
/// `MAX_EVENT_BYTES` per event, when the event metadata alone exceeds `target`, or when an event
/// is larger than `MAX_EVENT_BYTES`.
pub fn encode_events(
    events: &mut [Value],
    shape: &str,
    target: usize,
) -> Result<(Vec<u8>, &'static str)> {
    let mut body = encode(events, shape)?;
    if target > 0 {
        if target > events.len() * MAX_EVENT_BYTES {
            return Err(EVENT_LIMIT_ERROR.into());
        }

        if body.len() > target {
            trim(events, body.len() - target)?;
            body = encode(events, shape)?;
        }

        body = pad_to(events, shape, target, body)?;

        for event in events.iter() {
            if serde_json::to_vec(event)?.len() > MAX_EVENT_BYTES {
                return Err(EVENT_LIMIT_ERROR.into());
            }
        }
    }

    let content_type = if shape == "ndjson" {
        "application/x-ndjson"
    } else {
        "application/json"
    };
    Ok((body, content_type))
}

/// The milliseconds covered by a query over `history` days.
fn history_span_ms(history: f64) -> i64 {
    (history * DAY_MS as f64) as i64
}

/// One step of the query rotation before its path and parameters are assembled.
struct RotationStep {
    name: &'static str,
    endpoint: &'static str,
    span: i64,
    extra: Vec<(&'static str, String)>,
}

/// The rotation step for `step`, which is taken modulo `QUERY_SHAPE_COUNT`.
fn rotation_step(step: u64, marker: &str, history: f64) -> RotationStep {
    let limit = QUERY_LIMIT.to_string();
    let rare_match = format!("{marker} common event rare-match");
    let guaranteed_miss = format!("{marker} guaranteed-never-present");
    let history_span = history_span_ms(history);
    let (name, endpoint, span, extra) = match step % QUERY_SHAPE_COUNT {
        0 => (
            "recent",
            "/logs",
            RECENT_SPAN_MS,
            vec![
                ("service", "checkout".into()),
                ("min_level", "30".into()),
                ("limit", limit),
            ],
        ),
        1 => ("facets", "/logs/facets", DAY_MS, vec![]),
        2 => (
            "histogram",
            "/logs/histogram",
            DAY_MS,
            vec![("interval_ms", HISTOGRAM_INTERVAL_MS.to_string())],
        ),
        3 => (
            "rare_all",
            "/logs",
            history_span,
            vec![("message", rare_match), ("limit", limit)],
        ),
        4 => ("common_all", "/logs", history_span, vec![("limit", limit)]),
        5 => (
            "miss_all",
            "/logs",
            history_span,
            vec![("message", guaranteed_miss), ("limit", limit)],
        ),
        6 => (
            "rare_checkout",
            "/logs",
            history_span,
            vec![
                ("message", rare_match),
                ("service", "checkout".into()),
                ("limit", limit),
            ],
        ),
        7 => (
            "miss_checkout",
            "/logs",
            history_span,
            vec![
                ("message", guaranteed_miss),
                ("service", "checkout".into()),
                ("limit", limit),
            ],
        ),
        _ => ("count", "/logs/count", history_span, vec![]),
    };

    RotationStep {
        name,
        endpoint,
        span,
        extra,
    }
}

/// The query for one iteration of the rotation: its name, the path with its query string, and
/// the parameters.
pub fn query_shape(
    iteration: u64,
    marker: &str,
    history: f64,
    upper: i64,
) -> (String, String, BTreeMap<String, String>) {
    let step = rotation_step(iteration, marker, history);
    let mut params = BTreeMap::from([
        ("from".into(), (upper - step.span).to_string()),
        ("to".into(), upper.to_string()),
        ("message".into(), marker.into()),
    ]);
    for (key, value) in step.extra {
        params.insert(key.into(), value);
    }

    (
        step.name.into(),
        super::http::path(step.endpoint, &params),
        params,
    )
}

/// Checks one query response against its request. Returns a description when the response is
/// inconsistent with the request, and `None` when it is consistent.
pub fn validate_query(
    endpoint: &str,
    params: &BTreeMap<String, String>,
    data: &Value,
) -> Option<String> {
    check_response(endpoint, params, data)
        .is_none()
        .then(|| format!("invalid {endpoint} response semantics"))
}

/// The checks behind `validate_query`. Any failed check returns `None`.
fn check_response(endpoint: &str, params: &BTreeMap<String, String>, data: &Value) -> Option<()> {
    let lower = params.get("from")?.parse::<i64>().ok()?;
    let upper = params.get("to")?.parse::<i64>().ok()?;
    match endpoint {
        "/logs" => check_events(params, data, lower, upper),
        "/logs/count" => {
            data["count"].as_u64()?;
            Some(())
        }
        "/logs/facets" => check_facets(data),
        _ => check_histogram(params, data, lower, upper),
    }
}

/// Reads an optional numeric parameter. A missing one takes `default`; a present one that does not
/// parse fails the check.
fn optional_param<T: FromStr>(
    params: &BTreeMap<String, String>,
    key: &str,
    default: T,
) -> Option<T> {
    match params.get(key) {
        Some(value) => value.parse().ok(),
        None => Some(default),
    }
}

/// Checks an event page: distinct IDs in descending time order inside the window, filters and
/// limit honored, and a well-formed cursor.
fn check_events(
    params: &BTreeMap<String, String>,
    data: &Value,
    lower: i64,
    upper: i64,
) -> Option<()> {
    let rows = data["events"].as_array()?;
    if rows.len() > optional_param(params, "limit", QUERY_LIMIT)? {
        return None;
    }

    let mut ids = BTreeSet::new();
    let mut previous = None;
    for row in rows {
        let id = row["id"].as_str()?;
        if !ids.insert(id) {
            return None;
        }

        let key = (row["event_time"].as_i64()?, id.parse::<u64>().ok()?);
        if key.0 < lower || key.0 >= upper || previous.is_some_and(|p| p < key) {
            return None;
        }
        previous = Some(key);

        for field in ["service", "host", "logger", "source"] {
            if let Some(filter) = params.get(field)
                && row[field].as_str() != Some(filter)
            {
                return None;
            }
        }
        if row["level"].as_i64()? < optional_param::<i64>(params, "min_level", 0)? {
            return None;
        }
        if !row["message"].as_str()?.contains(
            params
                .get("message")
                .map(String::as_str)
                .unwrap_or_default(),
        ) {
            return None;
        }
    }

    if !data.get("next_cursor")?.is_null() && !data["next_cursor"].is_string() {
        return None;
    }
    Some(())
}

/// Checks that every facet list has only non-empty facets.
fn check_facets(data: &Value) -> Option<()> {
    for field in ["services", "hosts", "loggers", "sources", "levels"] {
        for facet in data[field].as_array()? {
            if facet["count"].as_u64()? == 0 {
                return None;
            }
        }
    }
    Some(())
}

/// Checks a histogram: a positive interval, and buckets that are increasing, on the interval grid
/// and inside the window.
fn check_histogram(
    params: &BTreeMap<String, String>,
    data: &Value,
    lower: i64,
    upper: i64,
) -> Option<()> {
    let interval = params.get("interval_ms")?.parse::<i64>().ok()?;
    if interval <= 0 {
        return None;
    }

    let mut previous = None;
    for bucket in data["buckets"].as_array()? {
        let time = bucket["time"].as_i64()?;
        bucket["count"].as_u64()?;
        if time < lower || time >= upper {
            return None;
        }
        if (time - lower) % interval != 0 || previous.is_some_and(|p| p >= time) {
            return None;
        }

        previous = Some(time);
    }

    Some(())
}

/// Event counts split around a cutoff: retained, expired, and uncertain.
struct Visibility {
    /// Events in seconds that start at or after the cutoff.
    retained: u64,
    /// Events in seconds that end at or before the cutoff.
    expired: u64,
    /// Events in seconds that straddle the cutoff.
    uncertain: u64,
}

/// Splits per-second event counts around `cutoff` (epoch milliseconds) into the counts retained,
/// expired, and uncertain. A second that straddles the cutoff is uncertain.
pub fn visibility_bounds(bins: &BTreeMap<i64, u64>, cutoff: i64) -> (u64, u64, u64) {
    let split = visibility(bins, cutoff);
    (split.retained, split.expired, split.uncertain)
}

/// The per-second split behind `visibility_bounds`.
fn visibility(bins: &BTreeMap<i64, u64>, cutoff: i64) -> Visibility {
    let mut split = Visibility {
        retained: 0,
        expired: 0,
        uncertain: 0,
    };
    for (&second, &count) in bins {
        if second * MILLIS_PER_SEC >= cutoff {
            split.retained += count;
        } else if (second + 1) * MILLIS_PER_SEC <= cutoff {
            split.expired += count;
        } else {
            split.uncertain += count;
        }
    }

    split
}

/// Event bounds for a final count, with the report fields that explain them.
struct Bounds {
    lower: u64,
    upper: u64,
    basis: Value,
}

/// The bounds when two committed cutoffs were read around the final query: the events retained
/// are bounded by the cutoff readings, and the ambiguous commits widen the upper bound.
fn committed_bounds(bins: &BTreeMap<i64, u64>, before: i64, after: i64, ambiguous: u64) -> Bounds {
    let earlier = visibility(bins, before);
    let later = visibility(bins, after);
    Bounds {
        lower: later.retained,
        upper: earlier.retained + earlier.uncertain + ambiguous,
        basis: json!({
            "expired_events_lower_bound": earlier.expired,
            "expired_events_upper_bound": later.expired + later.uncertain,
            "cutoff_before_ms": before,
            "cutoff_after_ms": after,
            "cutoff_advanced_during_count": before != after,
            "basis": "committed storage cutoff gauge before/after final query"
        }),
    }
}

/// Counts and cutoff readings collected around the final visibility query.
pub(super) struct VisibilityObservations {
    pub(super) acknowledged: u64,
    pub(super) ambiguous: u64,
    pub(super) before: Option<i64>,
    pub(super) after: Option<i64>,
    pub(super) retention: Option<i64>,
    pub(super) time: i64,
    pub(super) observed: Option<u64>,
}

/// Bounds the retained event count from the committed cutoff and the observed count.
pub(super) fn final_visibility(
    bins: &BTreeMap<i64, u64>,
    observations: VisibilityObservations,
) -> Value {
    let VisibilityObservations {
        acknowledged,
        ambiguous,
        before,
        after,
        retention,
        time,
        observed,
    } = observations;
    let verified = before.zip(after).is_some_and(|(b, a)| b <= a);
    let bounds = match before.zip(after).filter(|(b, a)| b <= a) {
        Some((b, a)) => committed_bounds(bins, b, a, ambiguous),
        None => uncommitted_bounds(bins, acknowledged, ambiguous, retention, time),
    };
    let (lower, upper, mut out) = (bounds.lower, bounds.upper, bounds.basis);
    let exact = lower == upper && ambiguous == 0 && (verified || retention.is_some());
    let within = observed.is_some_and(|v| v >= lower && v <= upper);
    out["retained_lower_bound"] = json!(lower);
    out["retained_upper_bound"] = json!(upper);
    out["cutoff_boundary_uncertain_events"] = json!(upper.saturating_sub(lower));
    out["ambiguous_commit_events"] = json!(ambiguous);
    out["exact"] = json!(exact);
    out["observed_within_bounds"] = json!(within);

    out["correctness_verified"] = json!(within && ambiguous == 0 && (verified || exact));
    out
}

/// The bounds when no committed cutoff pair is available: declared retention bounds the expiry
/// conservatively, and without it nothing expired can be verified.
fn uncommitted_bounds(
    bins: &BTreeMap<i64, u64>,
    acknowledged: u64,
    ambiguous: u64,
    retention: Option<i64>,
    time: i64,
) -> Bounds {
    match retention {
        Some(retention) => Bounds {
            lower: visibility_bounds(bins, time - retention).0,
            upper: acknowledged + ambiguous,
            basis: json!({"basis": concat!(
                "declared retention only; committed cutoff unavailable, ",
                "so lagging expiry is bounded conservatively"
            )}),
        },
        None => Bounds {
            lower: 0,
            upper: acknowledged + ambiguous,
            basis: json!({"basis": concat!(
                "no committed cutoff gauge or declared retention; ",
                "expiry cannot be verified"
            )}),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_is_bounded_and_conservative() {
        let mut samples = Samples::default();
        for _ in 0..100000 {
            samples.add(123.45);
        }
        assert_eq!(samples.bins.len(), 1);
        let p = samples.report()["p99"].as_f64().unwrap();
        assert!((123.45..126.0).contains(&p));
    }

    #[test]
    fn committed_visibility_advancement() {
        let bins = BTreeMap::from([(1, 10), (2, 20), (3, 30)]);
        let result = final_visibility(
            &bins,
            VisibilityObservations {
                acknowledged: 60,
                ambiguous: 0,
                before: Some(1500),
                after: Some(2500),
                retention: None,
                time: 4000,
                observed: Some(40),
            },
        );
        assert_eq!(result["retained_lower_bound"], 30);
        assert_eq!(result["retained_upper_bound"], 60);
        assert_eq!(result["correctness_verified"], true);
        assert_eq!(result["exact"], false);
        assert_eq!(
            final_visibility(
                &bins,
                VisibilityObservations {
                    acknowledged: 60,
                    ambiguous: 0,
                    before: Some(2000),
                    after: Some(2000),
                    retention: None,
                    time: 4000,
                    observed: Some(50)
                }
            )["exact"],
            true
        );

        let result = final_visibility(
            &bins,
            VisibilityObservations {
                acknowledged: 60,
                ambiguous: 0,
                before: Some(1500),
                after: Some(2500),
                retention: None,
                time: 4000,
                observed: Some(61),
            },
        );
        assert_eq!(result["correctness_verified"], false);

        let result = final_visibility(
            &bins,
            VisibilityObservations {
                acknowledged: 60,
                ambiguous: 0,
                before: None,
                after: None,
                retention: Some(1000),
                time: 3500,
                observed: Some(60),
            },
        );
        assert_eq!(result["correctness_verified"], false);

        let result = final_visibility(
            &bins,
            VisibilityObservations {
                acknowledged: 60,
                ambiguous: 5,
                before: Some(2000),
                after: Some(2000),
                retention: None,
                time: 4000,
                observed: Some(55),
            },
        );
        assert_eq!(result["correctness_verified"], false);
    }

    #[test]
    fn query_parameters_are_endpoint_specific() {
        for n in 0..9 {
            let (_, path, p) = query_shape(n, "run", 7.0, 1000000000);
            assert!(p["message"].contains("run"));
            if !path.starts_with("/logs?") {
                assert!(!p.contains_key("limit"));
                assert!(!p.contains_key("cursor"));
            }
            if !path.starts_with("/logs/histogram?") {
                assert!(!p.contains_key("interval_ms"));
            }
        }
    }

    #[test]
    fn query_validation_rejects_order_filters_boundary_and_grid() {
        let p = BTreeMap::from([
            ("from".into(), "0".into()),
            ("to".into(), "100".into()),
            ("message".into(), "run".into()),
            ("service".into(), "checkout".into()),
            ("limit".into(), "2".into()),
        ]);
        let event =
            json!({"id":"1","event_time":10,"message":"run event","service":"checkout","level":30});
        assert!(
            validate_query(
                "/logs",
                &p,
                &json!({"events":[event.clone()],"next_cursor":null})
            )
            .is_none()
        );

        for field in ["id", "service", "event_time"] {
            let mut other = event.clone();
            other[field] = match field {
                "id" => json!("2"),
                "service" => json!("worker"),
                _ => json!(100),
            };

            assert!(
                validate_query(
                    "/logs",
                    &p,
                    &json!({"events":[event.clone(),other],"next_cursor":null})
                )
                .is_some()
            );
        }

        let p = BTreeMap::from([
            ("from".into(), "10".into()),
            ("to".into(), "100".into()),
            ("interval_ms".into(), "10".into()),
        ]);
        assert!(
            validate_query(
                "/logs/histogram",
                &p,
                &json!({"buckets":[{"time":20,"count":1}]})
            )
            .is_none()
        );

        assert!(
            validate_query(
                "/logs/histogram",
                &p,
                &json!({"buckets":[{"time":21,"count":1}]})
            )
            .is_some()
        );

        assert_eq!(
            visibility_bounds(&BTreeMap::from([(1, 2), (2, 3), (3, 4)]), 2500),
            (4, 2, 3)
        );
    }
}
