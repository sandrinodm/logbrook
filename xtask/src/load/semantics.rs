use crate::Result;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub const DAY_MS: i64 = 86_400_000;

#[derive(Default, Clone)]
pub struct Samples {
    pub bins: BTreeMap<u32, u64>,
    count: u64,
    total: f64,
    maximum: f64,
}
impl Samples {
    pub fn add(&mut self, ms: f64) {
        let value = ms.max(0.0);
        *self
            .bins
            .entry(((value * 10.0).ln_1p() / 1.02_f64.ln()).ceil() as u32)
            .or_default() += 1;
        self.count += 1;
        self.total += value;
        self.maximum = self.maximum.max(value);
    }
    pub fn report(&self) -> Value {
        let mut report = json!({"samples":self.count,"mean":if self.count>0 {Some(self.total/self.count as f64)} else {None},"max":self.maximum,"quantile_upper_error":"2% + 0.002ms"});
        for (name, fraction) in [("p50", 0.5), ("p95", 0.95), ("p99", 0.99)] {
            let target = (self.count as f64 * fraction).ceil().max(1.0) as u64;
            let mut cumulative = 0;
            let bucket = self.bins.iter().find(|(_, count)| {
                cumulative += **count;
                cumulative >= target
            });
            report[name] = bucket
                .map(|(bucket, _)| json!((*bucket as f64 * 1.02_f64.ln()).exp_m1() / 10.0))
                .unwrap_or(Value::Null);
        }
        report
    }
}

pub fn encode_events(
    events: &mut [Value],
    shape: &str,
    target: usize,
) -> Result<(Vec<u8>, &'static str)> {
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
    fn trim(events: &mut [Value], mut amount: usize) -> Result<()> {
        let len = events.len();
        for (position, event) in events.iter_mut().enumerate() {
            let payload = event["payload"].as_str().unwrap_or_default();
            let remove = payload.len().min(amount.div_ceil(len - position));
            event["payload"] = json!(&payload[..payload.len() - remove]);
            amount -= remove;
        }
        if amount > 0 {
            return Err("--body-bytes is smaller than required event metadata".into());
        }
        Ok(())
    }
    let mut body = encode(events, shape)?;
    if target > 0 {
        if target > events.len() * 65536 {
            return Err("body exceeds event byte limit; increase --batch-size".into());
        }
        if body.len() > target {
            trim(events, body.len() - target)?;
            body = encode(events, shape)?;
        }
        let mut field = "payload".to_string();
        let mut field_index = 0;
        while body.len() < target {
            let mut difference = target - body.len();
            let len = events.len();
            for (position, event) in events.iter_mut().enumerate() {
                let old = event[&field].as_str().unwrap_or_default();
                let add =
                    (4096_usize.saturating_sub(old.len())).min(difference.div_ceil(len - position));
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
        for event in events.iter() {
            if serde_json::to_vec(event)?.len() > 65536 {
                return Err("body exceeds event byte limit; increase --batch-size".into());
            }
        }
    }
    Ok((
        body,
        if shape == "ndjson" {
            "application/x-ndjson"
        } else {
            "application/json"
        },
    ))
}

pub fn query_shape(
    iteration: u64,
    marker: &str,
    history: f64,
    upper: i64,
) -> (String, String, BTreeMap<String, String>) {
    let (name, endpoint, span, extra): (&str, &str, i64, Vec<(&str, String)>) = match iteration % 9
    {
        0 => (
            "recent",
            "/logs",
            900000,
            vec![
                ("service", "checkout".into()),
                ("min_level", "30".into()),
                ("limit", "100".into()),
            ],
        ),
        1 => ("facets", "/logs/facets", DAY_MS, vec![]),
        2 => (
            "histogram",
            "/logs/histogram",
            DAY_MS,
            vec![("interval_ms", "60000".into())],
        ),
        3 => (
            "rare_all",
            "/logs",
            (history * DAY_MS as f64) as i64,
            vec![
                ("message", format!("{marker} common event rare-match")),
                ("limit", "100".into()),
            ],
        ),
        4 => (
            "common_all",
            "/logs",
            (history * DAY_MS as f64) as i64,
            vec![("limit", "100".into())],
        ),
        5 => (
            "miss_all",
            "/logs",
            (history * DAY_MS as f64) as i64,
            vec![
                ("message", format!("{marker} guaranteed-never-present")),
                ("limit", "100".into()),
            ],
        ),
        6 => (
            "rare_checkout",
            "/logs",
            (history * DAY_MS as f64) as i64,
            vec![
                ("message", format!("{marker} common event rare-match")),
                ("service", "checkout".into()),
                ("limit", "100".into()),
            ],
        ),
        7 => (
            "miss_checkout",
            "/logs",
            (history * DAY_MS as f64) as i64,
            vec![
                ("message", format!("{marker} guaranteed-never-present")),
                ("service", "checkout".into()),
                ("limit", "100".into()),
            ],
        ),
        _ => (
            "count",
            "/logs/count",
            (history * DAY_MS as f64) as i64,
            vec![],
        ),
    };
    let mut params = BTreeMap::from([
        ("from".into(), (upper - span).to_string()),
        ("to".into(), upper.to_string()),
        ("message".into(), marker.into()),
    ]);
    for (key, value) in extra {
        params.insert(key.into(), value);
    }
    (name.into(), super::http::path(endpoint, &params), params)
}

pub fn validate_query(
    endpoint: &str,
    params: &BTreeMap<String, String>,
    data: &Value,
) -> Option<String> {
    let checked = (|| -> Option<()> {
        let lower = params.get("from")?.parse::<i64>().ok()?;
        let upper = params.get("to")?.parse::<i64>().ok()?;
        match endpoint {
            "/logs" => {
                let rows = data["events"].as_array()?;
                if rows.len()
                    > params
                        .get("limit")
                        .map(|v| v.parse().ok())
                        .unwrap_or(Some(100))?
                {
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
                    if row["level"].as_i64()?
                        < params
                            .get("min_level")
                            .map(|v| v.parse().ok())
                            .unwrap_or(Some(0))?
                    {
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
            }
            "/logs/count" => {
                data["count"].as_u64()?;
            }
            "/logs/facets" => {
                for field in ["services", "hosts", "loggers", "sources", "levels"] {
                    for facet in data[field].as_array()? {
                        if facet["count"].as_u64()? == 0 {
                            return None;
                        }
                    }
                }
            }
            _ => {
                let interval = params.get("interval_ms")?.parse::<i64>().ok()?;
                if interval <= 0 {
                    return None;
                }
                let mut previous = None;
                for bucket in data["buckets"].as_array()? {
                    let time = bucket["time"].as_i64()?;
                    bucket["count"].as_u64()?;
                    if time < lower
                        || time >= upper
                        || (time - lower) % interval != 0
                        || previous.is_some_and(|p| p >= time)
                    {
                        return None;
                    }
                    previous = Some(time);
                }
            }
        }
        Some(())
    })();
    checked
        .is_none()
        .then(|| format!("invalid {endpoint} response semantics"))
}

pub fn visibility_bounds(bins: &BTreeMap<i64, u64>, cutoff: i64) -> (u64, u64, u64) {
    let (mut retained, mut expired, mut uncertain) = (0, 0, 0);
    for (&second, &count) in bins {
        if second * 1000 >= cutoff {
            retained += count;
        } else if (second + 1) * 1000 <= cutoff {
            expired += count;
        } else {
            uncertain += count;
        }
    }
    (retained, expired, uncertain)
}

// Explicit cutoff and count observations match the report boundary contract.
#[allow(clippy::too_many_arguments)]
pub fn final_visibility(
    bins: &BTreeMap<i64, u64>,
    acknowledged: u64,
    ambiguous: u64,
    before: Option<i64>,
    after: Option<i64>,
    retention: Option<i64>,
    time: i64,
    observed: Option<u64>,
) -> Value {
    let verified = before.zip(after).is_some_and(|(b, a)| b <= a);
    let (lower, upper, mut out) = if let Some((b, a)) = before.zip(after).filter(|(b, a)| b <= a) {
        let (rb, eb, ub) = visibility_bounds(bins, b);
        let (ra, ea, ua) = visibility_bounds(bins, a);
        (
            ra,
            rb + ub + ambiguous,
            json!({"expired_events_lower_bound":eb,"expired_events_upper_bound":ea+ua,"cutoff_before_ms":b,"cutoff_after_ms":a,"cutoff_advanced_during_count":b!=a,"basis":"committed storage cutoff gauge before/after final query"}),
        )
    } else if let Some(retention) = retention {
        (
            visibility_bounds(bins, time - retention).0,
            acknowledged + ambiguous,
            json!({"basis":"declared retention only; committed cutoff unavailable, so lagging expiry is bounded conservatively"}),
        )
    } else {
        (
            0,
            acknowledged + ambiguous,
            json!({"basis":"no committed cutoff gauge or declared retention; expiry cannot be verified"}),
        )
    };
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
        let result = final_visibility(&bins, 60, 0, Some(1500), Some(2500), None, 4000, Some(40));
        assert_eq!(result["retained_lower_bound"], 30);
        assert_eq!(result["retained_upper_bound"], 60);
        assert_eq!(result["correctness_verified"], true);
        assert_eq!(result["exact"], false);
        assert_eq!(
            final_visibility(&bins, 60, 0, Some(2000), Some(2000), None, 4000, Some(50))["exact"],
            true
        );
        assert_eq!(
            final_visibility(&bins, 60, 0, Some(1500), Some(2500), None, 4000, Some(61))["correctness_verified"],
            false
        );
        assert_eq!(
            final_visibility(&bins, 60, 0, None, None, Some(1000), 3500, Some(60))["correctness_verified"],
            false
        );
        assert_eq!(
            final_visibility(&bins, 60, 5, Some(2000), Some(2000), None, 4000, Some(55))["correctness_verified"],
            false
        );
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
