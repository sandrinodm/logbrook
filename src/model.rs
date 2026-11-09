//! Shared event, query and error types used by storage, ingestion, HTTP and the CLI.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The most events one page of a search or tail may return.
pub const MAX_PAGE_SIZE: usize = 1_000;

/// The most buckets one histogram may return.
pub const MAX_HISTOGRAM_BUCKETS: i64 = 2_000;

mod normalized_event;
pub use normalized_event::NormalizedEvent;

/// A stored event, with the id and source the server assigned at ingestion.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    /// Server-assigned ingestion identifier, serialized as decimal text.
    pub id: String,
    /// Validated producer source assigned by the ingestion credential.
    pub source: String,
    /// Producer event time in Unix milliseconds, within the supported UTC calendar.
    pub event_time: i64,
    /// Server receipt time in Unix milliseconds.
    pub received_at: i64,
    /// Nonnegative severity on the producer's numeric scale.
    pub level: i32,
    /// Optional service name associated with the event.
    pub service: Option<String>,
    /// Optional logger name associated with the event.
    pub logger: Option<String>,
    /// Optional host name associated with the event.
    pub host: Option<String>,
    /// Optional nonnegative producer process identifier.
    pub pid: Option<i64>,
    /// The displayed event message, bounded by ingestion policy.
    pub message: String,
    /// Producer metadata retained independently of canonical event fields.
    pub attributes: Value,
}

/// A search over stored events: a half-open time range and the filters, with paging.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Query {
    /// Inclusive start, in Unix milliseconds.
    pub from: i64,
    /// Exclusive end, in Unix milliseconds.
    pub to: i64,
    /// Allowed source names; an empty list includes every source.
    pub sources: Vec<String>,
    /// Exact service-name filter, when set.
    pub service: Option<String>,
    /// Exact logger-name filter, when set.
    pub logger: Option<String>,
    /// Exact host-name filter, when set.
    pub host: Option<String>,
    /// Case-sensitive message substring filter, when set.
    pub message: Option<String>,
    /// Inclusive minimum numeric severity, when set.
    pub min_level: Option<i32>,
    /// Canonical equality filters over literal top-level attributes.
    #[serde(default)]
    pub attributes: crate::attribute_filter::AttributeFilters,
    /// Index and route identity binding continuation tokens to their query.
    #[serde(default)]
    pub cursor_scope: crate::query_cursor::CursorScope,
    /// Maximum events returned in a search page.
    pub limit: usize,
    /// Continuation token for the same normalized query, when paging.
    pub cursor: Option<String>,
}

/// One page of search results and the cursor for the next page, if there is one.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchResult {
    /// Events in this page, in the endpoint's defined order.
    pub events: Vec<Event>,
    /// Continuation position after this page, when available.
    pub next_cursor: Option<String>,
}

/// One distinct value in a facet and how many matching events carry it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FacetValue {
    /// One distinct field value represented as text.
    pub value: String,
    /// Number of matching events represented by this facet or bucket.
    pub count: u64,
}

/// The distinct values of each filterable field among the matching events, with their counts.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Facets {
    /// Distinct service values among matching events.
    pub services: Vec<FacetValue>,
    /// Distinct logger values among matching events.
    pub loggers: Vec<FacetValue>,
    /// Distinct host values among matching events.
    pub hosts: Vec<FacetValue>,
    /// Distinct numeric severity values represented as text.
    pub levels: Vec<FacetValue>,
    /// Distinct source names among matching events.
    pub sources: Vec<FacetValue>,
}

/// One histogram bucket: the start of its interval and the matching events it holds.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistogramBucket {
    /// Inclusive start of this histogram bucket, in Unix milliseconds.
    pub time: i64,
    /// Number of matching events represented by this facet or bucket.
    pub count: u64,
}

/// The category of a failure. It decides the HTTP status a failure maps to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// The request or configuration violates a validated requirement.
    Invalid,
    /// Retention has removed the history needed by a continuation.
    CursorExpired,
    /// Stored data or metadata violates a consistency invariant.
    Integrity,
    /// The input or output exceeds a configured size limit.
    TooLarge,
    /// An admission budget has no room for the work.
    Overloaded,
    /// The required worker, storage, or transport cannot run the operation.
    Unavailable,
    /// An operation exceeded its deadline.
    Timeout,
    /// The request has no accepted credential.
    Unauthorized,
    /// The accepted credential does not grant this operation or scope.
    Forbidden,
    /// The requested endpoint or index does not exist.
    NotFound,
    /// The measured disk target prevents additional writes.
    SizeLimit,
}

/// A failure with a category and a message that is safe to show to an operator.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{message}")]
pub struct Error {
    /// Failure category used by transports and retry policy.
    pub kind: ErrorKind,
    /// Operator-safe description of this failure.
    pub message: String,
    /// Optional stable diagnostic code independent of the displayed message.
    pub reason: Option<String>,
    /// Optional underlying failure retained as an error-chain source.
    #[source]
    pub source: Option<Arc<dyn std::error::Error + Send + Sync>>,
}

impl Error {
    /// Creates an error of the given kind with the given message.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            reason: None,
            source: None,
        }
    }

    /// Creates an [`ErrorKind::Invalid`] error: the request or configuration is wrong.
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Invalid, message)
    }

    /// Creates an [`ErrorKind::Unavailable`] error: the work cannot run right now.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unavailable, message)
    }

    /// Attaches a short diagnostic reason code to this error.
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Attaches the underlying cause of this error.
    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Arc::new(source));
        self
    }
}
