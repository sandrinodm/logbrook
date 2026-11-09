//! HTTP client for load probes: bounded responses, no retries and no connection reuse for
//! ingest, which is not safe to replay.
use crate::Result;
use futures_util::stream;
use reqwest::{Client, Method, RequestBuilder, Response};
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};

/// Largest response body, in bytes, that a probe accepts.
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// Time limit for one request, from connection to the last body byte.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Time an idle pooled connection is kept. It is short, so the client does not reuse a socket
/// that the server has already closed.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_millis(50);

/// Piece size, in bytes, of a chunked ingest body.
const INGEST_CHUNK_BYTES: usize = 16 * 1024;

/// A client for one server, with the read and write tokens that its requests use.
#[derive(Clone)]
pub struct Http {
    client: Client,
    /// Origin of the server, without a trailing slash.
    pub base: String,
    /// Bearer token for read requests.
    pub read: String,
    /// Bearer token for write requests.
    pub write: String,
}

impl Http {
    /// Creates a client for `base`, which must be an HTTP origin without credentials, path,
    /// query or fragment.
    ///
    /// # Errors
    ///
    /// Returns an error when `base` is not such an origin or the client cannot be built.
    pub fn new(base: &str, read: &str, write: &str) -> Result<Self> {
        let url = reqwest::Url::parse(base)?;
        let is_http = matches!(url.scheme(), "http" | "https");
        let has_host = url.host_str().is_some();
        let has_credentials = !url.username().is_empty() || url.password().is_some();
        let has_query_or_fragment = url.query().is_some() || url.fragment().is_some();
        let has_path = !matches!(url.path(), "" | "/");
        if !is_http || !has_host || has_credentials || has_query_or_fragment || has_path {
            return Err(
                "--url must be an HTTP origin without credentials, path, query or fragment".into(),
            );
        }

        Ok(Self {
            // Disable connection reuse beyond server idle-header deadlines. reqwest can
            // recover stale pooled GET sockets; ingest bodies are never replayable.
            client: Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .pool_idle_timeout(POOL_IDLE_TIMEOUT)
                .retry(reqwest::retry::never())
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            base: base.trim_end_matches('/').into(),
            read: read.into(),
            write: write.into(),
        })
    }

    /// Sends one request and returns its status and body. A transport failure is returned as
    /// status `0` with the error text, and `commit_ambiguous` says whether it was an ingest.
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

    /// The request of [`Http::request`] with its errors kept.
    async fn try_request(
        &self,
        path: &str,
        body: Option<(Vec<u8>, &str, bool)>,
        raw: bool,
    ) -> Result<(u16, Value)> {
        let is_write = body.is_some();
        let (method, token) = if is_write {
            (Method::POST, &self.write)
        } else {
            (Method::GET, &self.read)
        };
        let mut request = self
            .client
            .request(method, format!("{}{}", self.base, path))
            .bearer_auth(token);
        if let Some((bytes, content_type, chunked)) = body {
            request = attach_body(request, bytes, content_type, chunked);
        }

        let mut response = request.send().await?;
        let status = response.status().as_u16();
        let data = read_bounded_body(&mut response).await?;
        Ok((status, decode_body(data, raw)?))
    }
}

/// Adds the `Content-Type` header and the body of an ingest request. A chunked body is
/// streamed in `INGEST_CHUNK_BYTES` pieces; any other body is sent with a known length.
fn attach_body(
    request: RequestBuilder,
    bytes: Vec<u8>,
    content_type: &str,
    chunked: bool,
) -> RequestBuilder {
    let request = request.header("Content-Type", content_type);
    // A stream with no known size produces chunked transfer encoding and cannot
    // be cloned by the HTTP stack for an automatic write retry.
    if chunked {
        let chunks: Vec<Result<Vec<u8>>> = bytes
            .chunks(INGEST_CHUNK_BYTES)
            .map(|piece| Ok(piece.to_vec()))
            .collect();
        request.body(reqwest::Body::wrap_stream(stream::iter(chunks)))
    } else {
        request.body(bytes)
    }
}

/// Reads the whole body of `response`.
///
/// # Errors
///
/// Returns an error when the body grows past `MAX_RESPONSE_BYTES`, ends before its declared
/// length, or cannot be read.
async fn read_bounded_body(response: &mut Response) -> Result<Vec<u8>> {
    let declared = response.content_length();
    let mut data = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if data.len() + chunk.len() > MAX_RESPONSE_BYTES {
            return Err("response exceeded client byte bound".into());
        }
        data.extend_from_slice(&chunk);
    }

    if declared.is_some_and(|len| len != data.len() as u64) {
        return Err("truncated response body".into());
    }

    Ok(data)
}

/// The body as text when `raw`, otherwise as a JSON object.
fn decode_body(data: Vec<u8>, raw: bool) -> Result<Value> {
    if raw {
        return Ok(Value::String(String::from_utf8(data)?));
    }

    let value: Value = serde_json::from_slice(&data)?;
    if !value.is_object() {
        return Err("expected a JSON response object".into());
    }

    Ok(value)
}

/// `endpoint` with `params` as its query string, for building request paths.
pub fn path(endpoint: &str, params: &BTreeMap<String, String>) -> String {
    let mut url = reqwest::Url::parse("http://localhost").expect("literal URL");
    url.set_path(endpoint);
    url.query_pairs_mut().extend_pairs(params);
    format!("{}?{}", endpoint, url.query().unwrap_or_default())
}

/// Parses a Prometheus text exposition into metric name to value. Samples of the default index
/// are also recorded under their unlabelled name.
pub fn metrics(value: &Value) -> BTreeMap<String, f64> {
    let mut result = BTreeMap::new();
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
