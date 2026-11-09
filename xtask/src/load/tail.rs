//! Server-sent event tail probes that check ordering, isolation, filters and latency.

use super::{http::Http, semantics::Samples};
use crate::Result;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
};

/// Largest accepted SSE frame, in bytes, before the probe fails.
const MAX_FRAME_BYTES: usize = 2 * 1024 * 1024;

/// Time allowed to connect to the tail endpoint.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Time allowed for every tail task to stop after it is signalled.
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(2);

/// HTTP status of a successful tail request.
const STATUS_OK: u16 = 200;

/// `f64` values that convert exactly to an `i128`: from `i64::MIN` up to `u64::MAX + 1`.
const EXACT_INTEGER_RANGE: std::ops::Range<f64> =
    -9_223_372_036_854_775_808.0..18_446_744_073_709_551_616.0;

/// The state of one tail probe, updated as frames arrive.
struct TailState {
    summary: Value,
    latency: Samples,
    previous: u64,
    index: Option<String>,
    params: BTreeMap<String, String>,
    observed: Arc<AtomicU64>,
    ready: Option<oneshot::Sender<()>>,
}

impl TailState {
    fn frame(&mut self, entries: Vec<String>) -> Result<()> {
        let mut fields: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for entry in entries {
            if let Some((key, value)) = entry.split_once(':') {
                fields
                    .entry(key.into())
                    .or_default()
                    .push(value.trim_start().into());
            }
        }

        let kind = fields
            .get("event")
            .and_then(|v| v.first())
            .map(String::as_str)
            .unwrap_or("message");
        match kind {
            "gap" => {
                self.summary["gaps"] = json!(self.summary["gaps"].as_u64().unwrap_or(0) + 1);
                return Ok(());
            }
            "error" => return Err("tail error frame".into()),
            "cursor" => {
                let token = fields
                    .get("id")
                    .and_then(|values| values.first())
                    .filter(|value| !value.is_empty())
                    .ok_or("tail checkpoint missing resume token")?;
                self.summary["last_resume_token"] = json!(token);
                if let Some(ready) = self.ready.take() {
                    let _ = ready.send(());
                }
                return Ok(());
            }
            "log" => {}
            _ => return Ok(()),
        }

        self.observe_log(&fields)
    }

    /// Checks one `log` frame and records its payload in the summary.
    fn observe_log(&mut self, fields: &BTreeMap<String, Vec<String>>) -> Result<()> {
        let data = fields
            .get("data")
            .ok_or("tail log missing data")?
            .join("\n");
        let event: Value = serde_json::from_str(&data)?;
        let payload_id = event["id"]
            .as_str()
            .ok_or("tail missing payload ID")?
            .parse::<u64>()?;
        let header_id = fields.get("id").and_then(|v| v.first());
        let identifier = header_id.map(String::as_str).unwrap_or_default();
        if identifier.is_empty() {
            return Err("tail log missing resume token".into());
        }

        if payload_id <= self.previous {
            self.summary["monotonic"] = json!(false);
        }

        self.previous = payload_id;
        self.summary["last_id"] = json!(payload_id);
        self.summary["last_resume_token"] = json!(identifier);

        if let Some(index) = &self.index
            && (!identifier.starts_with(&format!("{index}:"))
                || !event["message"]
                    .as_str()
                    .unwrap_or_default()
                    .starts_with(&format!("{index} ")))
        {
            self.summary["isolated"] = json!(false);
        }

        for (key, filter) in &self.params {
            let matches = match key.as_str() {
                "message" => event["message"]
                    .as_str()
                    .is_some_and(|s| s.contains(filter)),
                "filter" => attribute_matches(&event["attributes"], filter)?,
                "min_level" => event["level"].as_i64().unwrap_or(0) >= filter.parse().unwrap_or(0),
                _ => event[key].as_str() == Some(filter),
            };
            if !matches {
                self.summary["filter_correct"] = json!(false);
            }
        }

        self.observed.fetch_add(1, Ordering::Relaxed);
        self.summary["events"] = json!(self.summary["events"].as_u64().unwrap_or(0) + 1);
        self.summary["observed"] = self.summary["events"].clone();

        if let Some(received) = event["received_at"].as_i64() {
            self.latency.add((crate::now_ms() - received).max(0) as f64);
        }

        Ok(())
    }

    async fn consume(
        &mut self,
        http: &Http,
        endpoint: &str,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let request = client
            .get(format!(
                "{}{}",
                http.base,
                super::http::path(endpoint, &self.params)
            ))
            .bearer_auth(&http.read)
            .header("Accept", "text/event-stream");

        let mut response = tokio::select! {
            _ = stop.changed() => return Ok(()),
            response = request.send() => response?,
        };
        self.summary["status"] = json!(response.status().as_u16());
        if response.status() != STATUS_OK {
            return Err("tail HTTP status was not 200".into());
        }

        let mut frame = Vec::new();
        let mut frame_bytes = 0;
        let mut buffered = Vec::new();
        loop {
            let chunk = tokio::select! {
                biased;
                _ = stop.changed() => break,
                chunk = response.chunk() => chunk?,
            };
            let Some(chunk) = chunk else {
                return Err("tail connection ended".into());
            };
            buffered.extend_from_slice(&chunk);

            while let Some(position) = buffered.iter().position(|b| *b == b'\n') {
                let line = String::from_utf8(buffered.drain(..=position).collect())?;
                frame_bytes += line.len();
                if frame_bytes > MAX_FRAME_BYTES {
                    return Err(frame_bound_error());
                }

                let line = line.trim_end_matches(['\r', '\n']);
                if line.is_empty() {
                    self.frame(std::mem::take(&mut frame))?;
                    frame_bytes = 0;
                } else {
                    frame.push(line.to_string());
                }
            }

            if buffered.len() + frame_bytes > MAX_FRAME_BYTES {
                return Err(frame_bound_error());
            }
        }
        Ok(())
    }
}

/// The error for a frame that grows past `MAX_FRAME_BYTES`.
fn frame_bound_error() -> Box<dyn std::error::Error + Send + Sync> {
    "tail frame exceeded client bound".into()
}

/// Whether every condition of the attribute filter `filter` holds for `attributes`.
fn attribute_matches(attributes: &Value, filter: &str) -> Result<bool> {
    let parsed: Value = serde_json::from_str(filter)?;
    let conditions = parsed["attributes"]
        .as_array()
        .ok_or("invalid attribute filter in tail probe")?;
    for condition in conditions {
        let key = condition["key"].as_str().ok_or("missing attribute key")?;
        let Some(actual) = attributes.as_object().and_then(|object| object.get(key)) else {
            return Ok(false);
        };
        let expected = &condition["value"];
        let matches = match (actual, expected) {
            (Value::Number(left), Value::Number(right)) => {
                let integer = |number: &serde_json::Number| {
                    number
                        .as_i64()
                        .map(i128::from)
                        .or_else(|| number.as_u64().map(i128::from))
                        .or_else(|| {
                            let value = number.as_f64()?;
                            (value.fract() == 0.0 && EXACT_INTEGER_RANGE.contains(&value))
                                .then_some(value as i128)
                        })
                };
                match (integer(left), integer(right)) {
                    (Some(a), Some(b)) => a == b,
                    (None, None) => left.as_f64() == right.as_f64(),
                    _ => false,
                }
            }
            (Value::String(left), Value::String(right)) => left == right,
            (Value::Bool(left), Value::Bool(right)) => left == right,
            _ => false,
        };
        if !matches {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Starts a fresh tail and reports readiness only after its initial checkpoint.
pub fn spawn_count_ready(
    http: Http,
    endpoint: String,
    params: BTreeMap<String, String>,
    subscriber: usize,
    index: Option<String>,
    mut stop: watch::Receiver<bool>,
    observed: Arc<AtomicU64>,
) -> (JoinHandle<Value>, oneshot::Receiver<()>) {
    let (ready, started) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut state = TailState {
            summary: json!({
                "subscriber": subscriber, "index": index, "events": 0, "observed": 0,
                "gaps": 0, "errors": 0, "status": null, "monotonic": true,
                "filter_correct": true, "isolated": true, "last_id": null,
            }),
            latency: Samples::default(),
            previous: 0,
            index,
            params,
            observed,
            ready: Some(ready),
        };
        if let Err(error) = state.consume(&http, &endpoint, &mut stop).await
            && !*stop.borrow()
        {
            state.summary["errors"] = json!(1);
            state.summary["error"] = json!(error.to_string());
        }

        state.summary["latency_ms"] = state.latency.report();
        state.summary
    });
    (task, started)
}

/// Waits for every tail task until `SHUTDOWN_DEADLINE`, aborting the ones that do not finish,
/// and returns one summary per task.
pub async fn finish(tasks: Vec<JoinHandle<Value>>) -> Vec<Value> {
    let mut results = Vec::new();
    let deadline = tokio::time::Instant::now() + SHUTDOWN_DEADLINE;
    for mut task in tasks {
        match tokio::time::timeout_at(deadline, &mut task).await {
            Ok(Ok(value)) => results.push(value),
            other => {
                task.abort();
                let _ = task.await;
                results.push(json!({"errors": 1, "error": format!("tail shutdown: {other:?}")}));
            }
        }
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> TailState {
        TailState {
            summary: json!({
                "gaps": 0,
                "events": 0,
                "monotonic": true,
                "isolated": true,
                "filter_correct": true,
            }),
            latency: Samples::default(),
            previous: 0,
            index: Some("payments".into()),
            params: BTreeMap::from([("service".into(), "checkout".into())]),
            observed: Arc::new(AtomicU64::new(0)),
            ready: None,
        }
    }

    fn frame(header: &str, payload: &str, message: &str, service: &str) -> Vec<String> {
        vec![
            "event: log".into(),
            format!("id: {header}"),
            format!(
                "data: {}",
                json!({
                    "id": payload,
                    "message": message,
                    "service": service,
                    "received_at": crate::now_ms() - 10,
                })
            ),
        ]
    }

    #[test]
    fn attribute_probe_preserves_types_and_exact_integer_values() {
        let matches = |actual: Value, expected: Value| {
            attribute_matches(
                &json!({"key":actual}),
                &json!({"attributes":[{"key":"key","op":"eq","value":expected}]}).to_string(),
            )
            .unwrap()
        };
        assert!(matches(json!(123), json!(123.0)));
        assert!(!matches(json!(123), json!("123")));
        assert!(!matches(json!(true), json!(1)));
        assert!(!matches(json!(1.5), json!(2)));
        assert!(!matches(
            json!(9007199254740993_u64),
            json!(9007199254740992.0)
        ));
        assert!(!matches(Value::Null, Value::Null));
        assert!(
            !attribute_matches(
                &json!(["x"]),
                r#"{"attributes":[{"key":"0","op":"eq","value":"x"}]}"#
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn checkpoint_readiness_preserves_opaque_resume_token() {
        let mut state = state();
        let (ready, started) = oneshot::channel();
        state.ready = Some(ready);

        state
            .frame(vec![
                "event: cursor".into(),
                "id: payments:v1.opaque-token".into(),
            ])
            .unwrap();
        started.await.unwrap();

        assert_eq!(
            state.summary["last_resume_token"],
            "payments:v1.opaque-token"
        );
        assert_eq!(state.summary["events"], 0);
    }

    #[test]
    fn scoped_tail_checks_payload_ids_isolation_filters_gaps_and_latency() {
        let mut state = state();
        state
            .frame(frame(
                "payments:v1.opaque-first",
                "1",
                "payments event",
                "checkout",
            ))
            .unwrap();

        assert_eq!(state.summary["monotonic"], true);
        assert_eq!(state.latency.report()["samples"], 1);

        state
            .frame(frame(
                "payments:v1.opaque-second",
                "1",
                "orders event",
                "worker",
            ))
            .unwrap();

        assert_eq!(state.summary["monotonic"], false);
        assert_eq!(state.summary["isolated"], false);
        assert_eq!(state.summary["filter_correct"], false);

        state.frame(vec!["event: gap".into()]).unwrap();
        assert_eq!(state.summary["gaps"], 1);
        assert!(state.frame(vec!["event: error".into()]).is_err());
    }
}
