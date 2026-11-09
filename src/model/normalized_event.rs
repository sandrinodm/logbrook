//! Event representation shared by ingestion and pure protocol normalization.
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// An event as it is parsed from a producer, before the server assigns its identity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NormalizedEvent {
    /// Producer event time in Unix milliseconds.
    pub event_time: i64,
    /// Nonnegative producer severity on its numeric scale.
    pub level: i32,
    /// Optional service name before storage assigns event identity.
    pub service: Option<String>,
    /// Optional producer logger name.
    pub logger: Option<String>,
    /// Optional producer host name.
    pub host: Option<String>,
    /// Optional nonnegative producer process identifier.
    pub pid: Option<i64>,
    /// Displayed event text, subject to the transport's byte limit.
    pub message: String,
    /// Producer metadata kept separate from the canonical event fields.
    pub attributes: Value,
}
