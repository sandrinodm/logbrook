use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MAX_PAGE_SIZE: usize = 1_000;

pub const MAX_HISTOGRAM_BUCKETS: i64 = 2_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NormalizedEvent {
    pub event_time: i64,
    pub level: i32,
    pub service: Option<String>,
    pub logger: Option<String>,
    pub host: Option<String>,
    pub pid: Option<i64>,
    pub message: String,
    pub attributes: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    pub source: String,
    pub event_time: i64,
    pub received_at: i64,
    pub level: i32,
    pub service: Option<String>,
    pub logger: Option<String>,
    pub host: Option<String>,
    pub pid: Option<i64>,
    pub message: String,
    pub attributes: Value,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Query {
    pub from: i64,
    pub to: i64,
    pub sources: Vec<String>,
    pub service: Option<String>,
    pub logger: Option<String>,
    pub host: Option<String>,
    pub message: Option<String>,
    pub min_level: Option<i32>,
    pub limit: usize,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchResult {
    pub events: Vec<Event>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FacetValue {
    pub value: String,
    pub count: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Facets {
    pub services: Vec<FacetValue>,
    pub loggers: Vec<FacetValue>,
    pub hosts: Vec<FacetValue>,
    pub levels: Vec<FacetValue>,
    pub sources: Vec<FacetValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub time: i64,
    pub count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    Invalid,
    CursorExpired,
    Integrity,
    TooLarge,
    Overloaded,
    Unavailable,
    Timeout,
    Unauthorized,
    Forbidden,
    NotFound,
    SizeLimit,
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
}

impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Invalid, message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unavailable, message)
    }
}
