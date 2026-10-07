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
use tokio::{sync::watch, task::JoinHandle};

struct TailState {
    summary: Value,
    latency: Samples,
    previous: u64,
    index: Option<String>,
    params: BTreeMap<String, String>,
    observed: Arc<AtomicU64>,
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
            "log" => {}
            _ => return Ok(()),
        }

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
        let numeric = if self.index.is_some() {
            identifier
                .rsplit(':')
                .next()
                .ok_or("tail missing scoped ID")?
                .parse::<u64>()?
        } else {
            payload_id
        };

        if numeric <= self.previous || numeric != payload_id {
            self.summary["monotonic"] = json!(false);
        }
        self.previous = numeric;
        self.summary["last_id"] = json!(numeric);

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
            .connect_timeout(Duration::from_secs(2))
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let mut request = client
            .get(format!(
                "{}{}",
                http.base,
                super::http::path(endpoint, &self.params)
            ))
            .bearer_auth(&http.read)
            .header("Accept", "text/event-stream");
        if let Some(index) = &self.index {
            request = request.header("Last-Event-ID", format!("{index}:0"));
        }

        let mut response = tokio::select! {
            _ = stop.changed() => return Ok(()),
            response = request.send() => response?,
        };
        self.summary["status"] = json!(response.status().as_u16());
        if response.status() != 200 {
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
                if frame_bytes > 2 * 1024 * 1024 {
                    return Err("tail frame exceeded client bound".into());
                }
                let line = line.trim_end_matches(['\r', '\n']);
                if line.is_empty() {
                    self.frame(std::mem::take(&mut frame))?;
                    frame_bytes = 0;
                } else {
                    frame.push(line.to_string());
                }
            }
            if buffered.len() + frame_bytes > 2 * 1024 * 1024 {
                return Err("tail frame exceeded client bound".into());
            }
        }
        Ok(())
    }
}

pub fn spawn(
    http: Http,
    endpoint: String,
    params: BTreeMap<String, String>,
    subscriber: usize,
    index: Option<String>,
    stop: watch::Receiver<bool>,
) -> JoinHandle<Value> {
    spawn_count(
        http,
        endpoint,
        params,
        subscriber,
        index,
        stop,
        Arc::new(AtomicU64::new(0)),
    )
}

pub fn spawn_count(
    http: Http,
    endpoint: String,
    params: BTreeMap<String, String>,
    subscriber: usize,
    index: Option<String>,
    mut stop: watch::Receiver<bool>,
    observed: Arc<AtomicU64>,
) -> JoinHandle<Value> {
    tokio::spawn(async move {
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
        };
        if let Err(error) = state.consume(&http, &endpoint, &mut stop).await
            && !*stop.borrow()
        {
            state.summary["errors"] = json!(1);
            state.summary["error"] = json!(error.to_string());
        }
        state.summary["latency_ms"] = state.latency.report();
        state.summary
    })
}

pub async fn finish(tasks: Vec<JoinHandle<Value>>) -> Vec<Value> {
    let mut results = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
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
            summary: json!({"gaps":0,"events":0,"monotonic":true,"isolated":true,"filter_correct":true}),
            latency: Samples::default(),
            previous: 0,
            index: Some("payments".into()),
            params: BTreeMap::from([("service".into(), "checkout".into())]),
            observed: Arc::new(AtomicU64::new(0)),
        }
    }
    fn frame(header: &str, payload: &str, message: &str, service: &str) -> Vec<String> {
        vec![
            "event: log".into(),
            format!("id: {header}"),
            format!(
                "data: {}",
                json!({"id":payload,"message":message,"service":service,"received_at":crate::now_ms()-10})
            ),
        ]
    }
    #[test]
    fn scoped_tail_checks_payload_ids_isolation_filters_gaps_and_latency() {
        let mut state = state();
        state
            .frame(frame("payments:1", "1", "payments event", "checkout"))
            .unwrap();
        assert_eq!(state.summary["monotonic"], true);
        assert_eq!(state.latency.report()["samples"], 1);
        state
            .frame(frame("payments:2", "1", "orders event", "worker"))
            .unwrap();
        assert_eq!(state.summary["monotonic"], false);
        assert_eq!(state.summary["isolated"], false);
        assert_eq!(state.summary["filter_correct"], false);
        state.frame(vec!["event: gap".into()]).unwrap();
        assert_eq!(state.summary["gaps"], 1);
        assert!(state.frame(vec!["event: error".into()]).is_err());
    }
}
