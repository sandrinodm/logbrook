//! Authentication, admission budgets, and routing shared by all indexes.

mod filters;
mod indexes;
mod metrics;
mod tail;

use filters::{AggregateFilters, HistogramFilters, LogQuery, SearchFilters, query};
use indexes::{
    create_index, cursor_scope, delete_index, ensure_selected, get_index, list_indexes,
    select_index,
};
use metrics::metrics;
use tail::tail;

use crate::{
    config::{Config, DEFAULT_INDEX_NAME},
    indexes::IndexRegistry,
    ingest,
    model::{Error, ErrorKind, Query},
    storage::Storage,
};
use axum::{
    Json, Router,
    body::Body,
    extract::{Extension, Query as Params, Request, State, rejection::QueryRejection},
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header},
    middleware::{Next, from_fn_with_state},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event as SseEvent, KeepAlive},
    },
    routing::{get, post},
};
use http_body_util::BodyExt;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Longest accepted request target (path and query) on a log endpoint, in bytes.
const MAX_LOG_REQUEST_TARGET_BYTES: usize = 32_768;

/// One cumulative latency bucket: an upper bound in microseconds and its Prometheus `le` label.
struct LatencyBucket {
    upper_micros: u64,
    le: &'static str,
}

/// The cumulative latency buckets, in ascending order. The last one admits every request.
const LATENCY_BUCKETS: [LatencyBucket; 7] = [
    LatencyBucket {
        upper_micros: 1_000,
        le: "0.001",
    },
    LatencyBucket {
        upper_micros: 10_000,
        le: "0.01",
    },
    LatencyBucket {
        upper_micros: 100_000,
        le: "0.1",
    },
    LatencyBucket {
        upper_micros: 500_000,
        le: "0.5",
    },
    LatencyBucket {
        upper_micros: 1_000_000,
        le: "1",
    },
    LatencyBucket {
        upper_micros: 5_000_000,
        le: "5",
    },
    LatencyBucket {
        upper_micros: u64::MAX,
        le: "+Inf",
    },
];

/// Slot of an endpoint's latency histogram that holds the summed latency, in microseconds.
const LATENCY_SUM_SLOT: usize = LATENCY_BUCKETS.len();

/// Slot of an endpoint's latency histogram that holds the request count.
const LATENCY_COUNT_SLOT: usize = LATENCY_BUCKETS.len() + 1;

/// One endpoint's histogram: a counter per bucket, then the latency sum and the request count.
type LatencySlots = [u64; LATENCY_BUCKETS.len() + 2];

/// A response header that every response carries.
struct SecurityHeader {
    name: &'static str,
    value: &'static str,
}

/// Headers that keep a browser from rendering or caching API output as a page.
const SECURITY_HEADERS: [SecurityHeader; 4] = [
    SecurityHeader {
        name: "x-content-type-options",
        value: "nosniff",
    },
    SecurityHeader {
        name: "x-frame-options",
        value: "DENY",
    },
    SecurityHeader {
        name: "content-security-policy",
        value: "default-src 'self'; script-src 'self'; style-src 'self'; \
             connect-src 'self'; img-src 'self' data:; object-src 'none'; \
             frame-ancestors 'none'; base-uri 'self'",
    },
    SecurityHeader {
        name: "cache-control",
        value: "no-store",
    },
];

/// Raw-budget bytes reserved per received body byte, so a chunk is admitted before it is buffered.
const RAW_BUDGET_PER_RECEIVED_BYTE: usize = 2;

/// Seconds a client should wait before retrying an overloaded request.
const RETRY_AFTER_SECONDS: &str = "1";

/// Refusal of a request because a shared admission budget is spent.
const CAPACITY_EXHAUSTED: &str = "HTTP capacity exhausted";

/// Refusal of a request body that is over the configured limit.
const BODY_TOO_LARGE: &str = "request body exceeds limit";

/// Refusal of a request while the storage behind it is stopped.
const STORAGE_UNAVAILABLE: &str = "storage unavailable";

/// Panic message for a poisoned overload-counter lock. The counters stay readable, so the
/// process can still report them.
const OVERLOAD_LOCK: &str = "HTTP overload lock";

/// Panic message for a poisoned latency-histogram lock.
const LATENCY_LOCK: &str = "HTTP latency lock";

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
    durations: Arc<Mutex<BTreeMap<String, LatencySlots>>>,
    overloads: Arc<Mutex<BTreeMap<&'static str, u64>>>,
    tail_gaps: Arc<AtomicU64>,
}

/// Routes for a single-index process, backed by one storage handle.
pub fn router(storage: Storage, config: Config) -> Router {
    router_inner(storage, config, None)
}

/// Routes for a registry of indexes. The unscoped `/logs` routes address the default index.
///
/// # Panics
///
/// Panics when the registry has no mandatory default index.
pub fn router_with_registry(registry: IndexRegistry, config: Config) -> Router {
    let default = registry
        .get(DEFAULT_INDEX_NAME)
        .expect("default index exists");
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
        durations: Arc::new(Mutex::new(BTreeMap::new())),
        overloads: Arc::new(Mutex::new(BTreeMap::new())),
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
        .layer(from_fn_with_state(app.clone(), observe))
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
            response.headers_mut().insert(
                header::RETRY_AFTER,
                HeaderValue::from_static(RETRY_AFTER_SECONDS),
            );
        }

        response
    }
}

/// The bearer token from the `Authorization` header, or an unauthorized error.
fn bearer(headers: &HeaderMap) -> Result<&str, Error> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Error::new(ErrorKind::Unauthorized, "Bearer token required"))
}

/// The source scopes granted to the read token in the request.
fn scopes(app: &App, headers: &HeaderMap) -> Result<Vec<String>, Error> {
    app.config
        .read_tokens
        .get(bearer(headers)?)
        .cloned()
        .ok_or_else(|| Error::new(ErrorKind::Unauthorized, "invalid read token"))
}

async fn health() -> Json<Value> {
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
        Error::unavailable(STORAGE_UNAVAILABLE).into_response()
    }
}

/// Counts an admission rejection under `cause` and returns the 429 error for it.
fn overload(app: &App, cause: &'static str, message: &str) -> Error {
    record_overload(app, cause);
    Error::new(ErrorKind::Overloaded, message).with_reason(cause)
}

fn record_overload(app: &App, cause: &'static str) {
    *app.overloads
        .lock()
        .expect(OVERLOAD_LOCK)
        .entry(cause)
        .or_default() += 1;
}

/// Counts an overload that surfaced from storage, and passes the error on unchanged.
fn query_error(app: &App, error: Error) -> Error {
    if error.kind == ErrorKind::Overloaded {
        record_overload(app, "storage_query");
    }

    error
}

/// Keeps the in-flight gauge correct when a handler returns or panics.
struct ActiveGuard(Arc<AtomicU64>);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// What a request does, decided once from its route and path. Admission and the counters depend
/// on it.
#[derive(Clone, Copy)]
struct RequestClass {
    /// The route matched a registered endpoint. Unmatched requests skip index selection.
    routed: bool,
    /// A search, count, facet or histogram: it feeds the query counters.
    query: bool,
    /// A batch write: it is not held to the request budget, and rejections feed the ingest counter.
    ingestion: bool,
    /// A live tail: it is held to its own subscriber budget, not the request budget.
    tail: bool,
}

/// Wraps every request: admission, index selection, security headers, and metrics.
async fn observe(State(app): State<App>, request: Request, next: Next) -> Response {
    app.active.fetch_add(1, Ordering::Relaxed);
    let _active = ActiveGuard(Arc::clone(&app.active));
    app.total.fetch_add(1, Ordering::Relaxed);
    let start = Instant::now();

    let canonical = canonical_route(request.uri().path());
    let route = canonical.as_deref().unwrap_or(request.uri().path());
    let matched = request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .is_some();
    let endpoint = if matched {
        endpoint_label(route, request.uri().path())
    } else {
        "unknown".into()
    };

    let path = request.uri().path();
    let class = RequestClass {
        routed: matched,
        query: is_query_route(route),
        ingestion: path.ends_with("/logs/ingest"),
        tail: path.ends_with("/logs/tail"),
    };
    if class.query {
        app.queries.fetch_add(1, Ordering::Relaxed);
    }

    let mut response = answer(&app, request, next, class).await;

    for header in SECURITY_HEADERS {
        response.headers_mut().insert(
            HeaderName::from_static(header.name),
            HeaderValue::from_static(header.value),
        );
    }

    let elapsed = elapsed_micros(start);
    record_latency(&app, endpoint, elapsed);
    app.elapsed_micros
        .fetch_add(elapsed_micros(start), Ordering::Relaxed);

    if class.query {
        app.query_micros
            .fetch_add(elapsed_micros(start), Ordering::Relaxed);
    }

    record_outcome(&app, response.status(), class);
    response
}

/// Admits a request under the request budget and runs it, or answers with the reason it was
/// refused. Requests that name too long a target are refused before any work starts.
async fn answer(app: &App, request: Request, next: Next, class: RequestClass) -> Response {
    // Ingestion and tails have their own budgets, so they do not take a request permit.
    let capacity = if !class.ingestion && !class.tail {
        Arc::clone(&app.requests).try_acquire_owned().map(Some)
    } else {
        Ok(None)
    };

    // Bound the encoded path plus query before any endpoint allocates decoded fields.
    let excessive_target = request.method() == Method::GET
        && (class.query || class.tail)
        && request
            .uri()
            .path_and_query()
            .is_some_and(|target| target.as_str().len() > MAX_LOG_REQUEST_TARGET_BYTES);
    if excessive_target {
        return Error::invalid(format!(
            "request target exceeds {MAX_LOG_REQUEST_TARGET_BYTES} bytes"
        ))
        .into_response();
    }

    match capacity {
        Ok(_capacity) => {
            let selection = if class.routed {
                select_index(app, &request)
            } else {
                Ok(app.clone())
            };
            dispatch(request, next, selection).await
        }
        Err(_) => overload(app, "request_capacity", CAPACITY_EXHAUSTED).into_response(),
    }
}

/// Runs the request in its selected index, or answers with the selection error.
async fn dispatch(mut request: Request, next: Next, selection: Result<App, Error>) -> Response {
    match selection {
        Ok(selected) => {
            request.extensions_mut().insert(selected);
            next.run(request).await
        }

        Err(error) => error.into_response(),
    }
}

/// The path with `/indexes/{name}` removed, so index-scoped and unscoped routes share one name.
fn canonical_route(path: &str) -> Option<String> {
    path.strip_prefix("/indexes/")
        .and_then(|rest| rest.split_once('/'))
        .map(|(_, suffix)| format!("/{suffix}"))
}

/// The bounded endpoint label used for latency metrics, so arbitrary paths cannot grow the map.
fn endpoint_label(route: &str, path: &str) -> String {
    match route {
        "/health" | "/ready" | "/metrics" | "/logs/ingest" | "/logs" | "/logs/count"
        | "/logs/facets" | "/logs/histogram" | "/logs/tail" | "/openapi.json" => route.to_owned(),
        "/indexes" => "/indexes".into(),
        _ if path.starts_with("/indexes/") => "/indexes/{index}".into(),
        _ => "unknown".into(),
    }
}

fn is_query_route(route: &str) -> bool {
    matches!(
        route,
        "/logs" | "/logs/count" | "/logs/facets" | "/logs/histogram"
    )
}

/// Microseconds since `start`, saturating at `u64::MAX`.
fn elapsed_micros(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX)
}

/// Adds one request to the cumulative buckets, the sum and the count of its endpoint.
fn record_latency(app: &App, endpoint: String, elapsed: u64) {
    let mut durations = app.durations.lock().expect(LATENCY_LOCK);
    let entry = durations.entry(endpoint).or_default();

    for (index, bucket) in LATENCY_BUCKETS.iter().enumerate() {
        if elapsed <= bucket.upper_micros {
            entry[index] += 1;
        }
    }

    entry[LATENCY_SUM_SLOT] = entry[LATENCY_SUM_SLOT].saturating_add(elapsed);
    entry[LATENCY_COUNT_SLOT] += 1;
}

/// Counts an error response against the endpoint's class.
fn record_outcome(app: &App, status: StatusCode, class: RequestClass) {
    if !(status.is_client_error() || status.is_server_error()) {
        return;
    }

    if class.query {
        app.query_errors.fetch_add(1, Ordering::Relaxed);
    }

    app.failures.fetch_add(1, Ordering::Relaxed);

    if class.ingestion {
        app.rejected.fetch_add(1, Ordering::Relaxed);
    }
}

async fn openapi() -> Response {
    (
        [(header::CONTENT_TYPE, "application/json")],
        include_str!("openapi.json"),
    )
        .into_response()
}

/// The source the request's ingestion token writes to.
fn ingest_source(app: &App, headers: &HeaderMap) -> Result<String, Error> {
    app.config
        .ingest_tokens
        .get(bearer(headers)?)
        .cloned()
        .ok_or_else(|| Error::new(ErrorKind::Unauthorized, "invalid ingestion token"))
}

fn check_content_length(app: &App, headers: &HeaderMap) -> Result<(), Error> {
    let Some(length) = headers.get(header::CONTENT_LENGTH) else {
        return Ok(());
    };
    let length = length
        .to_str()
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .ok_or_else(|| Error::invalid("invalid Content-Length"))?;
    if length > app.config.max_body_bytes {
        return Err(Error::new(ErrorKind::TooLarge, BODY_TOO_LARGE));
    }

    Ok(())
}

/// Whether the body is NDJSON (one event per line) rather than a single JSON document.
fn is_ndjson(headers: &HeaderMap) -> Result<bool, Error> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .split(';')
        .next()
        .unwrap_or("")
        .trim();
    match content_type {
        "application/json" => Ok(false),
        "application/x-ndjson" | "application/ndjson" => Ok(true),
        _ => Err(Error::invalid(
            "Content-Type must be application/json or application/x-ndjson",
        )),
    }
}

/// A request body held in memory, with the raw-budget permit that covers it.
struct ReceivedBody {
    bytes: Vec<u8>,
    raw_permit: Option<OwnedSemaphorePermit>,
}

/// Reads the body under the total and idle deadlines, reserving raw budget as it arrives.
async fn receive_body(app: &App, mut stream: Body) -> Result<ReceivedBody, Error> {
    let mut body = Vec::new();
    let mut raw_permit: Option<OwnedSemaphorePermit> = None;
    let deadline =
        tokio::time::Instant::now() + Duration::from_millis(app.config.body_total_timeout_ms);

    loop {
        let idle =
            tokio::time::Instant::now() + Duration::from_millis(app.config.body_idle_timeout_ms);
        let frame = tokio::time::timeout_at(deadline.min(idle), stream.frame())
            .await
            .map_err(|error| {
                Error::new(
                    ErrorKind::Timeout,
                    "request body receive deadline exceeded; nothing admitted",
                )
                .with_source(error)
            })?;
        let Some(frame) = frame else { break };
        let frame = frame
            .map_err(|error| Error::invalid("request body failed to stream").with_source(error))?;
        if let Ok(chunk) = frame.into_data() {
            if chunk.len() > app.config.max_body_bytes.saturating_sub(body.len()) {
                return Err(Error::new(ErrorKind::TooLarge, BODY_TOO_LARGE));
            }

            // Never wait while holding partial reservations: contention rejects
            // atomically and releases this request's bytes, so producers cannot deadlock.
            let reservation = chunk.len().saturating_mul(RAW_BUDGET_PER_RECEIVED_BYTE);
            let permit = Arc::clone(&app.raw_bytes)
                .try_acquire_many_owned(u32::try_from(reservation).unwrap_or(u32::MAX))
                .map_err(|error| {
                    overload(app, "raw_bytes", "raw body budget exhausted").with_source(error)
                })?;

            if let Some(held) = raw_permit.as_mut() {
                held.merge(permit);
            } else {
                raw_permit = Some(permit);
            }

            body.reserve_exact(chunk.len());
            body.extend_from_slice(&chunk);
        }
    }

    Ok(ReceivedBody {
        bytes: body,
        raw_permit,
    })
}

/// Ingests a batch of events for the source of the request's ingestion token.
async fn append(
    Extension(mut app): Extension<App>,
    request: Request,
) -> Result<Json<Value>, Error> {
    let source = ingest_source(&app, request.headers())?;

    // The HTTP admission permit travels with the parse and is released when this request ends.
    let admission = Arc::clone(&app.requests)
        .try_acquire_owned()
        .map_err(|error| {
            overload(&app, "request_capacity", CAPACITY_EXHAUSTED).with_source(error)
        })?;

    ensure_selected(&mut app).await?;
    check_content_length(&app, request.headers())?;

    if !app.storage.is_ready() {
        return Err(Error::unavailable(STORAGE_UNAVAILABLE));
    }

    let ndjson = is_ndjson(request.headers())?;
    let ReceivedBody {
        bytes: body,
        raw_permit,
    } = receive_body(&app, request.into_body()).await?;

    let config = app.config.clone();
    let budget = Arc::clone(&app.bytes);
    // The admission and raw permits return with the parse result, so they outlive it.
    let (parsed, _admission, _raw) = tokio::task::spawn_blocking(move || {
        (
            ingest::normalize_budgeted(&body, ndjson, &config, budget),
            admission,
            raw_permit,
        )
    })
    .await
    .map_err(|error| Error::unavailable("parser worker failed").with_source(error))?;
    let (events, permit) = parsed.inspect_err(|error| {
        if error.kind == ErrorKind::Overloaded {
            record_overload(&app, "decoded_bytes");
        }
    })?;
    let _bytes = permit.expect("budgeted parser reserves decoded bytes");
    let accepted = crate::ingestion::append_live_to_storage(
        &app.storage,
        &app.config,
        crate::ingestion::LiveBatch {
            source,
            events,
            permit: _bytes,
            limits: crate::ingestion::NormalizedLimits::http(&app.config),
        },
    )
    .await
    .inspect_err(|error| {
        if error.kind == ErrorKind::Overloaded {
            record_overload(&app, "storage_queue");
        }
    })?;
    Ok(Json(json!({"accepted":accepted})))
}

/// Unwraps query-string extraction, reporting the rejection text as a 400.
fn query_params<T>(filters: Result<Params<T>, QueryRejection>) -> Result<T, Error> {
    filters
        .map(|Params(filters)| filters)
        .map_err(|error| Error::invalid(error.body_text()).with_source(error))
}

async fn search(
    Extension(app): Extension<App>,
    headers: HeaderMap,
    filters: Result<Params<SearchFilters>, QueryRejection>,
) -> Result<Json<crate::model::SearchResult>, Error> {
    let LogQuery { query, .. } = query(&app, &headers, query_params(filters)?.into())?;
    let page = app
        .storage
        .search(query)
        .await
        .map_err(|e| query_error(&app, e))?;
    Ok(Json(page))
}

async fn count(
    Extension(app): Extension<App>,
    headers: HeaderMap,
    filters: Result<Params<AggregateFilters>, QueryRejection>,
) -> Result<Json<Value>, Error> {
    let LogQuery { query, .. } = query(&app, &headers, query_params(filters)?.into())?;
    let count = app
        .storage
        .count(query)
        .await
        .map_err(|e| query_error(&app, e))?;

    Ok(Json(json!({"count":count})))
}

async fn facets(
    Extension(app): Extension<App>,
    headers: HeaderMap,
    filters: Result<Params<AggregateFilters>, QueryRejection>,
) -> Result<Json<crate::model::Facets>, Error> {
    let LogQuery { query, .. } = query(&app, &headers, query_params(filters)?.into())?;
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
    filters: Result<Params<HistogramFilters>, QueryRejection>,
) -> Result<Json<Value>, Error> {
    let LogQuery { query, interval_ms } = query(&app, &headers, query_params(filters)?.into())?;
    let buckets = app
        .storage
        .histogram(query, interval_ms)
        .await
        .map_err(|e| query_error(&app, e))?;

    Ok(Json(json!({"buckets":buckets})))
}

async fn not_found(request: Request) -> Error {
    Error::new(
        ErrorKind::NotFound,
        format!("unknown endpoint: {}", request.uri().path()),
    )
}
