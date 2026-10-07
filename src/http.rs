//! Authentication, admission budgets, and routing shared by all indexes.

mod filters;
mod indexes;
mod metrics;
mod tail;

use filters::{AggregateFilters, HistogramFilters, SearchFilters, query};
use indexes::{
    create_index, decode_cursor, delete_index, encode_cursor, ensure_selected, get_index,
    list_indexes, select_index,
};
use metrics::metrics;
use tail::tail;

use crate::{
    config::Config,
    indexes::IndexRegistry,
    ingest,
    model::{Error, ErrorKind, Query},
    storage::Storage,
};
use axum::{
    Json, Router,
    extract::{Extension, Query as Params, Request, State},
    http::{HeaderMap, StatusCode, header},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event as SseEvent, KeepAlive},
    },
    routing::{get, post},
};
use http_body_util::BodyExt;
use serde::Deserialize;
use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::Semaphore;

#[derive(Clone)]
struct App {
    queries: Arc<AtomicU64>,
    query_errors: Arc<AtomicU64>,
    query_micros: Arc<AtomicU64>,

    total: Arc<AtomicU64>,
    failures: Arc<AtomicU64>,
    elapsed_micros: Arc<AtomicU64>,
    active: Arc<AtomicU64>,

    tails: Arc<Semaphore>,
    registry: Option<IndexRegistry>,
    index: Option<String>,
    storage: Storage,
    config: Config,

    requests: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    raw_bytes: Arc<Semaphore>,
    metrics_jobs: Arc<Semaphore>,

    rejected: Arc<AtomicU64>,
    // Seven cumulative latency buckets, summed microseconds, then request count.
    durations: Arc<Mutex<std::collections::BTreeMap<String, [u64; 9]>>>,
    overloads: Arc<Mutex<std::collections::BTreeMap<&'static str, u64>>>,
    tail_gaps: Arc<AtomicU64>,
}

pub fn router(storage: Storage, config: Config) -> Router {
    router_inner(storage, config, None)
}

pub fn router_with_registry(registry: IndexRegistry, config: Config) -> Router {
    let default = registry.get("default").expect("default index exists");
    router_inner(default.storage, config, Some(registry))
}

fn router_inner(storage: Storage, config: Config, registry: Option<IndexRegistry>) -> Router {
    let app = App {
        registry,
        index: None,
        queries: Arc::new(AtomicU64::new(0)),
        query_errors: Arc::new(AtomicU64::new(0)),
        query_micros: Arc::new(AtomicU64::new(0)),
        total: Arc::new(AtomicU64::new(0)),
        failures: Arc::new(AtomicU64::new(0)),
        elapsed_micros: Arc::new(AtomicU64::new(0)),
        active: Arc::new(AtomicU64::new(0)),
        tails: Arc::new(Semaphore::new(config.max_tail_subscribers)),
        storage,
        requests: Arc::new(Semaphore::new(config.max_inflight_requests)),
        bytes: Arc::new(Semaphore::new(config.max_decoded_bytes)),
        raw_bytes: Arc::new(Semaphore::new(config.max_inflight_body_bytes)),
        metrics_jobs: Arc::new(Semaphore::new(1)),
        config,
        rejected: Arc::new(AtomicU64::new(0)),
        durations: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
        overloads: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
        tail_gaps: Arc::new(AtomicU64::new(0)),
    };
    Router::new()
        .route("/indexes", get(list_indexes))
        .route(
            "/indexes/{index}",
            get(get_index).put(create_index).delete(delete_index),
        )
        .route("/indexes/{index}/logs/ingest", post(append))
        .route("/indexes/{index}/logs", get(search))
        .route("/indexes/{index}/logs/count", get(count))
        .route("/indexes/{index}/logs/facets", get(facets))
        .route("/indexes/{index}/logs/histogram", get(histogram))
        .route("/indexes/{index}/logs/tail", get(tail))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/logs/ingest", post(append))
        .route("/logs", get(search))
        .route("/logs/count", get(count))
        .route("/logs/facets", get(facets))
        .route("/logs/histogram", get(histogram))
        .route("/logs/tail", get(tail))
        .route("/openapi.json", get(openapi))
        .fallback(not_found)
        .layer(axum::middleware::from_fn_with_state(app.clone(), observe))
        .with_state(app)
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match self.kind {
            ErrorKind::Invalid | ErrorKind::CursorExpired => StatusCode::BAD_REQUEST,
            ErrorKind::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            ErrorKind::Overloaded => StatusCode::TOO_MANY_REQUESTS,
            ErrorKind::Unavailable | ErrorKind::Integrity => StatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Timeout => StatusCode::GATEWAY_TIMEOUT,
            ErrorKind::Unauthorized => StatusCode::UNAUTHORIZED,
            ErrorKind::Forbidden => StatusCode::FORBIDDEN,
            ErrorKind::NotFound => StatusCode::NOT_FOUND,
            ErrorKind::SizeLimit => StatusCode::INSUFFICIENT_STORAGE,
        };
        let mut response = (status, Json(json!({"error": self.message}))).into_response();

        if status == StatusCode::TOO_MANY_REQUESTS {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, "1".parse().unwrap());
        }

        response
    }
}

fn bearer(headers: &HeaderMap) -> Result<&str, Error> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Error::new(ErrorKind::Unauthorized, "Bearer token required"))
}

fn scopes(app: &App, headers: &HeaderMap) -> Result<Vec<String>, Error> {
    app.config
        .read_tokens
        .get(bearer(headers)?)
        .cloned()
        .ok_or_else(|| Error::new(ErrorKind::Unauthorized, "invalid read token"))
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({"status":"ok"}))
}

async fn ready(Extension(app): Extension<App>) -> Response {
    if app
        .registry
        .as_ref()
        .map_or_else(|| app.storage.is_ready(), IndexRegistry::is_ready)
    {
        Json(json!({"status":"ready"})).into_response()
    } else {
        Error::unavailable("storage unavailable").into_response()
    }
}

fn overload(app: &App, cause: &'static str, message: &str) -> Error {
    *app.overloads.lock().unwrap().entry(cause).or_default() += 1;
    Error::new(ErrorKind::Overloaded, message)
}

fn query_error(app: &App, error: Error) -> Error {
    if error.kind == ErrorKind::Overloaded {
        *app.overloads
            .lock()
            .unwrap()
            .entry("storage_query")
            .or_default() += 1;
    }

    error
}

struct ActiveGuard(Arc<AtomicU64>);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn observe(
    State(app): State<App>,
    mut request: Request,
    next: axum::middleware::Next,
) -> Response {
    app.active.fetch_add(1, Ordering::Relaxed);
    let _active = ActiveGuard(app.active.clone());
    app.total.fetch_add(1, Ordering::Relaxed);
    let start = std::time::Instant::now();

    let canonical = request
        .uri()
        .path()
        .strip_prefix("/indexes/")
        .and_then(|rest| rest.split_once('/'))
        .map(|(_, suffix)| format!("/{suffix}"));
    let route = canonical.as_deref().unwrap_or(request.uri().path());
    let matched = request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .is_some();
    let endpoint = match route {
        _ if !matched => "unknown".into(),
        "/health" | "/ready" | "/metrics" | "/logs/ingest" | "/logs" | "/logs/count"
        | "/logs/facets" | "/logs/histogram" | "/logs/tail" | "/openapi.json" => route.to_owned(),
        "/indexes" => "/indexes".into(),
        _ if request.uri().path().starts_with("/indexes/") => "/indexes/{index}".into(),
        _ => "unknown".into(),
    };

    let path = request.uri().path();
    let ingestion = path.ends_with("/logs/ingest");
    let tail_request = path.ends_with("/logs/tail");
    let query_request = matches!(
        route,
        "/logs" | "/logs/count" | "/logs/facets" | "/logs/histogram"
    );

    if query_request {
        app.queries.fetch_add(1, Ordering::Relaxed);
    }

    let capacity = if !ingestion && !tail_request {
        app.requests.clone().try_acquire_owned().map(Some)
    } else {
        Ok(None)
    };

    let mut response = match capacity {
        Ok(_capacity) => match if matched {
            select_index(&app, &request)
        } else {
            Ok(app.clone())
        } {
            Ok(selected) => {
                request.extensions_mut().insert(selected);
                next.run(request).await
            }

            Err(error) => error.into_response(),
        },
        Err(_) => overload(&app, "request_capacity", "HTTP capacity exhausted").into_response(),
    };

    for (name, value) in [
        ("x-content-type-options", "nosniff"),
        ("x-frame-options", "DENY"),
        (
            "content-security-policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; \
                 connect-src 'self'; img-src 'self' data:; object-src 'none'; \
                 frame-ancestors 'none'; base-uri 'self'",
        ),
        ("cache-control", "no-store"),
    ] {
        response.headers_mut().insert(
            axum::http::HeaderName::from_static(name),
            value.parse().unwrap(),
        );
    }

    let elapsed = start.elapsed().as_micros().min(u64::MAX as u128) as u64;
    {
        let mut durations = app.durations.lock().unwrap();
        let entry = durations.entry(endpoint).or_default();

        for (index, upper) in [1000, 10000, 100000, 500000, 1000000, 5000000, u64::MAX]
            .iter()
            .enumerate()
        {
            if elapsed <= *upper {
                entry[index] += 1;
            }
        }

        entry[7] = entry[7].saturating_add(elapsed);
        entry[8] += 1;
    }

    app.elapsed_micros.fetch_add(
        start.elapsed().as_micros().min(u64::MAX as u128) as u64,
        Ordering::Relaxed,
    );

    if query_request {
        app.query_micros.fetch_add(
            start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    if response.status().is_client_error() || response.status().is_server_error() {
        if query_request {
            app.query_errors.fetch_add(1, Ordering::Relaxed);
        }

        app.failures.fetch_add(1, Ordering::Relaxed);

        if ingestion {
            app.rejected.fetch_add(1, Ordering::Relaxed);
        }
    }

    response
}

async fn openapi() -> Response {
    (
        [(header::CONTENT_TYPE, "application/json")],
        include_str!("openapi.json"),
    )
        .into_response()
}

async fn append(
    Extension(mut app): Extension<App>,
    request: Request,
) -> Result<Json<serde_json::Value>, Error> {
    let source = app
        .config
        .ingest_tokens
        .get(bearer(request.headers())?)
        .cloned()
        .ok_or_else(|| Error::new(ErrorKind::Unauthorized, "invalid ingestion token"))?;

    let _request = app
        .requests
        .clone()
        .try_acquire_owned()
        .map_err(|_| overload(&app, "request_capacity", "HTTP capacity exhausted"))?;

    ensure_selected(&mut app).await?;

    if let Some(length) = request.headers().get(header::CONTENT_LENGTH) {
        let length = length
            .to_str()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .ok_or_else(|| Error::invalid("invalid Content-Length"))?;

        if length > app.config.max_body_bytes {
            return Err(Error::new(
                ErrorKind::TooLarge,
                "request body exceeds limit",
            ));
        }
    }

    if !app.storage.is_ready() {
        return Err(Error::unavailable("storage unavailable"));
    }

    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .split(';')
        .next()
        .unwrap_or("")
        .trim();
    let ndjson = match content_type {
        "application/json" => false,
        "application/x-ndjson" | "application/ndjson" => true,
        _ => {
            return Err(Error::invalid(
                "Content-Type must be application/json or application/x-ndjson",
            ));
        }
    };
    let mut stream = request.into_body();
    let mut body = Vec::new();
    let mut raw_permit: Option<tokio::sync::OwnedSemaphorePermit> = None;
    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_millis(app.config.body_total_timeout_ms);

    loop {
        let idle = tokio::time::Instant::now()
            + std::time::Duration::from_millis(app.config.body_idle_timeout_ms);
        let frame = tokio::time::timeout_at(deadline.min(idle), stream.frame())
            .await
            .map_err(|_| {
                Error::new(
                    ErrorKind::Timeout,
                    "request body receive deadline exceeded; nothing admitted",
                )
            })?;
        let Some(frame) = frame else { break };
        let frame = frame.map_err(|_| Error::invalid("request body failed to stream"))?;

        if let Ok(chunk) = frame.into_data() {
            if chunk.len() > app.config.max_body_bytes.saturating_sub(body.len()) {
                return Err(Error::new(
                    ErrorKind::TooLarge,
                    "request body exceeds limit",
                ));
            }

            // Never wait while holding partial reservations: contention rejects
            // atomically and releases this request's bytes, so producers cannot deadlock.
            let permit = app
                .raw_bytes
                .clone()
                .try_acquire_many_owned((chunk.len() * 2) as u32)
                .map_err(|_| overload(&app, "raw_bytes", "raw body budget exhausted"))?;

            if let Some(held) = raw_permit.as_mut() {
                held.merge(permit);
            } else {
                raw_permit = Some(permit);
            }

            body.reserve_exact(chunk.len());
            body.extend_from_slice(&chunk);
        }
    }

    let config = app.config.clone();
    let budget = app.bytes.clone();
    let (parsed, _request, _raw) = tokio::task::spawn_blocking(move || {
        (
            ingest::normalize_budgeted(&body, ndjson, &config, budget),
            _request,
            raw_permit,
        )
    })
    .await
    .map_err(|_| Error::unavailable("parser worker failed"))?;
    let (events, permit) = parsed.inspect_err(|error| {
        if error.kind == ErrorKind::Overloaded {
            *app.overloads
                .lock()
                .unwrap()
                .entry("decoded_bytes")
                .or_default() += 1;
        }
    })?;
    let _bytes = permit.expect("budgeted parser reserves decoded bytes");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Error::unavailable("system clock precedes epoch"))?
        .as_millis()
        .min(i64::MAX as u128) as i64;
    let oldest = now.saturating_sub(app.config.retention_ms);
    let newest = now.saturating_add(app.config.max_future_skew_ms);

    if events
        .iter()
        .any(|event| event.event_time < oldest || event.event_time > newest)
    {
        return Err(Error::invalid(
            "live event time outside retention/future-skew window",
        ));
    }

    let accepted = app
        .storage
        .append_with_permit(source, events, _bytes)
        .await
        .inspect_err(|error| {
            if error.kind == ErrorKind::Overloaded {
                *app.overloads
                    .lock()
                    .unwrap()
                    .entry("storage_queue")
                    .or_default() += 1;
            }
        })?;

    Ok(Json(json!({"accepted":accepted})))
}

async fn search(
    Extension(app): Extension<App>,
    headers: HeaderMap,
    filters: Result<Params<SearchFilters>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<crate::model::SearchResult>, Error> {
    let Params(filters) = filters.map_err(|error| Error::invalid(error.body_text()))?;
    let (query, _) = query(&app, &headers, filters.into())?;
    let mut page = app
        .storage
        .search(query)
        .await
        .map_err(|e| query_error(&app, e))?;
    page.next_cursor = page.next_cursor.map(|cursor| encode_cursor(&app, &cursor));

    Ok(Json(page))
}

async fn count(
    Extension(app): Extension<App>,
    headers: HeaderMap,
    filters: Result<Params<AggregateFilters>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<serde_json::Value>, Error> {
    let Params(filters) = filters.map_err(|error| Error::invalid(error.body_text()))?;
    let (query, _) = query(&app, &headers, filters.into())?;

    Ok(Json(
        json!({"count":app.storage.count(query).await.map_err(|e| query_error(&app,e))?}),
    ))
}

async fn facets(
    Extension(app): Extension<App>,
    headers: HeaderMap,
    filters: Result<Params<AggregateFilters>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<crate::model::Facets>, Error> {
    let Params(filters) = filters.map_err(|error| Error::invalid(error.body_text()))?;
    let (query, _) = query(&app, &headers, filters.into())?;

    Ok(Json(
        app.storage
            .facets(query)
            .await
            .map_err(|e| query_error(&app, e))?,
    ))
}

async fn histogram(
    Extension(app): Extension<App>,
    headers: HeaderMap,
    filters: Result<Params<HistogramFilters>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<serde_json::Value>, Error> {
    let Params(filters) = filters.map_err(|error| Error::invalid(error.body_text()))?;
    let (query, interval) = query(&app, &headers, filters.into())?;

    Ok(Json(
        json!({"buckets":app.storage.histogram(query,interval).await.map_err(|e| query_error(&app,e))?}),
    ))
}

async fn not_found(request: Request) -> Error {
    Error::new(
        ErrorKind::NotFound,
        format!("unknown endpoint: {}", request.uri().path()),
    )
}
