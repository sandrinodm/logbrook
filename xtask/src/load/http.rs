use crate::Result;
use futures_util::stream;
use reqwest::{Client, Method};
use serde_json::{Value, json};
use std::time::Duration;

const MAX_RESPONSE: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct Http {
    client: Client,
    pub base: String,
    pub read: String,
    pub write: String,
}

impl Http {
    pub fn new(base: &str, read: &str, write: &str) -> Result<Self> {
        let url = reqwest::Url::parse(base)?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !matches!(url.path(), "" | "/")
        {
            return Err(
                "--url must be an HTTP origin without credentials, path, query or fragment".into(),
            );
        }
        Ok(Self {
            // Disable connection reuse beyond server idle-header deadlines. reqwest can
            // recover stale pooled GET sockets; ingest bodies are never replayable.
            client: Client::builder()
                .timeout(Duration::from_secs(15))
                .pool_idle_timeout(Duration::from_millis(50))
                .retry(reqwest::retry::never())
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            base: base.trim_end_matches('/').into(),
            read: read.into(),
            write: write.into(),
        })
    }

    pub async fn request(
        &self,
        path: &str,
        body: Option<(Vec<u8>, &str, bool)>,
        raw: bool,
    ) -> (u16, Value) {
        let ingest = body.is_some();
        match self.try_request(path, body, raw).await {
            Ok(value) => value,
            Err(error) => (
                0,
                json!({"error":error.to_string(),"commit_ambiguous":ingest}),
            ),
        }
    }

    async fn try_request(
        &self,
        path: &str,
        body: Option<(Vec<u8>, &str, bool)>,
        raw: bool,
    ) -> Result<(u16, Value)> {
        let mut request = self
            .client
            .request(
                if body.is_some() {
                    Method::POST
                } else {
                    Method::GET
                },
                format!("{}{}", self.base, path),
            )
            .bearer_auth(if body.is_some() {
                &self.write
            } else {
                &self.read
            });
        if let Some((bytes, content_type, chunked)) = body {
            request = request.header("Content-Type", content_type);
            // A stream with no known size produces chunked transfer encoding and cannot
            // be cloned by the HTTP stack for an automatic write retry.
            let chunks: Vec<Result<Vec<u8>>> = if chunked {
                bytes.chunks(16384).map(|s| Ok(s.to_vec())).collect()
            } else {
                vec![Ok(bytes)]
            };
            if chunked {
                request = request.body(reqwest::Body::wrap_stream(stream::iter(chunks)));
            } else {
                request = request.body(chunks.into_iter().next().expect("one body")?);
            }
        }
        let mut response = request.send().await?;
        let status = response.status().as_u16();
        let declared = response.content_length();
        let mut data = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if data.len() + chunk.len() > MAX_RESPONSE {
                return Err("response exceeded client byte bound".into());
            }
            data.extend_from_slice(&chunk);
        }
        if declared.is_some_and(|len| len != data.len() as u64) {
            return Err("truncated response body".into());
        }
        let value = if raw {
            Value::String(String::from_utf8(data)?)
        } else {
            let value: Value = serde_json::from_slice(&data)?;
            if !value.is_object() {
                return Err("expected a JSON response object".into());
            }
            value
        };
        Ok((status, value))
    }
}

pub fn path(endpoint: &str, params: &std::collections::BTreeMap<String, String>) -> String {
    let mut url = reqwest::Url::parse("http://localhost").expect("literal URL");
    url.set_path(endpoint);
    url.query_pairs_mut().extend_pairs(params);
    format!("{}?{}", endpoint, url.query().unwrap_or_default())
}

pub fn metrics(value: &Value) -> std::collections::BTreeMap<String, f64> {
    let mut result = std::collections::BTreeMap::new();
    for line in value
        .as_str()
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.starts_with('#'))
    {
        if let Some((name, value)) = line.rsplit_once(' ')
            && let Ok(number) = value.parse()
        {
            result.insert(name.into(), number);
            if name.ends_with("{index=\"default\"}") {
                result.insert(name.split('{').next().unwrap_or(name).into(), number);
            }
        }
    }
    result
}
