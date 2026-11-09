//! Typed receiver configuration. Secrets are references and never implement Debug.
pub use super::normalize::{JsonProfile, NormalizationProfile};
use super::{
    FRAME_PEAK_MULTIPLIER, READ_SCRATCH_BYTES, UNIX_MODE_ALLOWED_BITS, UNIX_MODE_REQUIRED_BITS,
};
use crate::{
    config::{IndexSettings, validate_index_name, validate_source},
    model::Error,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    ops::RangeInclusive,
    path::PathBuf,
};

// Binary size units for the byte limits below.
const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

/// Accepted `max_connections`.
const CONNECTIONS_RANGE: RangeInclusive<usize> = 1..=65_536;

/// Accepted `max_frame_bytes`.
const FRAME_BYTES_RANGE: RangeInclusive<usize> = KIB..=64 * MIB;

/// Largest accepted `max_expanded_bytes`.
const MAX_EXPANDED_BYTES: usize = 256 * MIB;

/// Accepted `max_events_per_frame`.
const EVENTS_PER_FRAME_RANGE: RangeInclusive<usize> = 1..=10_000;

/// Accepted `max_event_bytes`.
const EVENT_BYTES_RANGE: RangeInclusive<usize> = 1..=MIB;

/// Largest accepted `max_normalized_bytes`.
const MAX_NORMALIZED_BYTES: usize = 64 * MIB;

/// Accepted `max_metadata_depth`.
const METADATA_DEPTH_RANGE: RangeInclusive<usize> = 1..=64;

/// Accepted `max_tag_bytes`, the longest Forward tag a frame may carry.
const TAG_BYTES_RANGE: RangeInclusive<usize> = 1..=4 * KIB;

/// Accepted `max_chunk_bytes`, the longest `chunk` option a frame may carry.
const CHUNK_BYTES_RANGE: RangeInclusive<usize> = 1..=4 * KIB;

/// Accepted `max_cpu_jobs`.
const CPU_JOBS_RANGE: RangeInclusive<usize> = 1..=256;

/// Largest accepted `max_gzip_members`.
const MAX_GZIP_MEMBERS: usize = 1024;

/// Accepted value of every timeout, in milliseconds.
const TIMEOUT_MS_RANGE: RangeInclusive<u64> = 1..=3_600_000;

/// Most entries in `listeners`.
const MAX_LISTENERS: usize = 64;

/// Most entries in the `principals` table.
const MAX_PRINCIPALS: usize = 1024;

/// Most entries in `routes`.
const MAX_ROUTES: usize = 1024;

/// Most addresses in one listener's `allow_peers`.
const MAX_PEER_ACL: usize = 1024;

/// Most index names in one principal's `indexes`.
const MAX_PRINCIPAL_INDEXES: usize = 64;

/// Most entries in one `shared_key` listener's `users` table.
const MAX_LISTENER_USERS: usize = 1024;

/// Most entries in one listener's `tls.client_fingerprints` table.
const MAX_FINGERPRINTS: usize = 1024;

/// Hex characters in a SHA-256 certificate fingerprint.
const FINGERPRINT_HEX_CHARS: usize = 64;

/// Longest user name in a listener's `users` table, in bytes. A client PING is held to the same
/// bound.
const MAX_USER_NAME_BYTES: usize = 128;

/// Longest `server_hostname` a `shared_key` listener may send in its handshake, in bytes.
const MAX_SERVER_HOSTNAME_BYTES: usize = 255;

/// Largest secret a `shared_key` or user `password` reference may yield, in bytes.
const MAX_SECRET_BYTES: usize = 4 * KIB;

/// Most entries in one route's `message_fields`.
const MAX_ROUTE_MESSAGE_FIELDS: usize = 32;

/// Longest field name in a route selector or message-field list.
const MAX_FIELD_NAME_BYTES: usize = KIB;

/// Longest `service` or `host` a route may set.
const MAX_ROUTE_METADATA_BYTES: usize = 4 * KIB;

/// Receiver settings from the `[forward]` table. Sizes are in bytes and durations in
/// milliseconds, as the field names say. Limits are checked by [`ForwardConfig::validate`].
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ForwardConfig {
    /// Whether to bind the listeners at all.
    pub enabled: bool,
    /// Sessions open at once, across all listeners.
    pub max_connections: usize,
    /// Largest encoded frame a client may send.
    pub max_frame_bytes: usize,
    /// Largest payload after decompression.
    pub max_expanded_bytes: usize,
    /// Events accepted from one frame.
    pub max_events_per_frame: usize,
    /// Largest encoded event, including its metadata.
    pub max_event_bytes: usize,
    /// Longest message an event may carry.
    pub max_message_bytes: usize,
    /// Largest normalized output for one envelope.
    pub max_normalized_bytes: usize,
    /// Deepest nesting of arrays, maps and JSON in one frame.
    pub max_metadata_depth: usize,
    /// Longest tag a client may send.
    pub max_tag_bytes: usize,
    /// Longest acknowledgement chunk token.
    pub max_chunk_bytes: usize,
    /// Bytes of socket input buffered across all sessions.
    pub raw_budget_bytes: usize,
    /// Bytes of decode working memory across all sessions.
    pub decoded_budget_bytes: usize,
    /// Frames decoded at once.
    pub max_cpu_jobs: usize,
    /// Largest ratio of expanded to compressed bytes a payload may reach.
    pub max_expansion_ratio: usize,
    /// Gzip members one payload may contain.
    pub max_gzip_members: usize,
    /// Budget for decoding one frame.
    pub decode_timeout_ms: u64,
    /// Budget for the shared-key or TLS handshake.
    pub handshake_timeout_ms: u64,
    /// Longest wait for the next byte of a partly received frame.
    pub frame_idle_timeout_ms: u64,
    /// Longest total time to receive one frame.
    pub frame_total_timeout_ms: u64,
    /// Idle time allowed between frames on one session.
    pub session_idle_timeout_ms: u64,
    /// Longest wait for capacity before a frame is refused.
    pub admission_timeout_ms: u64,
    /// Longest time to write a response or acknowledgement, and to close a connection.
    pub ack_write_timeout_ms: u64,
    /// Whether a chunk token may be binary instead of a string.
    pub allow_binary_chunk: bool,
    /// Stamp events that carry no time with their receipt time.
    pub zero_time_to_receipt: bool,
    /// Listening endpoints.
    pub listeners: Vec<ListenerConfig>,
    /// Named sources and the indexes each may write to.
    pub principals: BTreeMap<String, PrincipalConfig>,
    /// Routes from a listener's tag to an index.
    pub routes: Vec<RouteConfig>,
}

impl Default for ForwardConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_connections: 1024,
            max_frame_bytes: 8 * MIB,
            max_expanded_bytes: 32 * MIB,
            max_events_per_frame: 10_000,
            max_event_bytes: 256 * KIB,
            max_message_bytes: 256 * KIB,
            max_normalized_bytes: 8 * MIB,
            max_metadata_depth: 32,
            max_tag_bytes: 1024,
            max_chunk_bytes: 1024,
            raw_budget_bytes: 128 * MIB,
            decoded_budget_bytes: 128 * MIB,
            max_cpu_jobs: 4,
            max_expansion_ratio: 1000,
            max_gzip_members: 128,
            decode_timeout_ms: 5000,
            handshake_timeout_ms: 5000,
            frame_idle_timeout_ms: 5000,
            frame_total_timeout_ms: 30_000,
            session_idle_timeout_ms: 300_000,
            admission_timeout_ms: 5000,
            ack_write_timeout_ms: 5000,
            allow_binary_chunk: false,
            zero_time_to_receipt: true,
            listeners: Vec::new(),
            principals: BTreeMap::new(),
            routes: Vec::new(),
        }
    }
}

/// A named source that routes may write to, with the indexes it is allowed to use.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalConfig {
    /// The source label stored with the events this principal sends.
    pub source: String,
    /// Index names this principal may write to.
    pub indexes: Vec<String>,
}

/// How clients prove their identity to a listener.
#[derive(Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityMode {
    /// No authentication. Only for loopback or a trusted network, with a fixed principal.
    #[default]
    TrustedLocal,
    /// The client proves knowledge of a shared key, and optionally a user password.
    SharedKey,
    /// The client presents a TLS certificate whose fingerprint maps to a principal.
    Mtls,
}

/// A secret read from exactly one file or environment variable.
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SecretRef {
    /// Path of a file whose bytes are the secret.
    pub file: Option<PathBuf>,
    /// Name of an environment variable holding the secret.
    pub env: Option<String>,
}

impl SecretRef {
    /// Reads the secret, which must be 1 to `MAX_SECRET_BYTES` bytes.
    ///
    /// # Errors
    ///
    /// Returns an invalid error unless exactly one reference is set and it yields a secret of
    /// the allowed size.
    pub fn load(&self) -> Result<Vec<u8>, Error> {
        let value = match (&self.file, &self.env) {
            (Some(path), None) => std::fs::read(path).map_err(|error| {
                Error::invalid("cannot read Forward secret file").with_source(error)
            })?,
            (None, Some(name)) => std::env::var(name)
                .map_err(|error| {
                    Error::invalid("cannot read Forward secret environment variable")
                        .with_source(error)
                })?
                .into_bytes(),
            _ => {
                return Err(Error::invalid(
                    "Forward secret requires exactly one file or env reference",
                ));
            }
        };
        if value.is_empty() || value.len() > MAX_SECRET_BYTES {
            return Err(Error::invalid(format!(
                "Forward secret must contain 1–{MAX_SECRET_BYTES} bytes"
            )));
        }

        Ok(value)
    }
}

/// A user of a shared-key listener: the principal their password grants and the password.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserConfig {
    /// Principal granted to this user.
    pub principal: String,
    /// Where the password comes from.
    pub password: SecretRef,
}

/// TLS material for a listener.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// PEM certificate chain the listener presents.
    pub certificate: PathBuf,
    /// PEM private key for `certificate`.
    pub private_key: PathBuf,
    /// PEM CA bundle that client certificates must chain to. Required for `mtls`.
    pub client_ca: Option<PathBuf>,
    /// Lowercase hex SHA-256 fingerprints of client certificates, each mapped to a principal.
    #[serde(default)]
    pub client_fingerprints: BTreeMap<String, String>,
}

/// One listening endpoint: a TCP address or a Unix socket, and how its clients authenticate.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ListenerConfig {
    /// Name that routes refer to this listener by.
    pub name: String,
    /// TCP address to bind. Exactly one of `bind` and `unix_path` is set.
    pub bind: Option<SocketAddr>,
    /// Unix socket path to bind. Must be absolute.
    pub unix_path: Option<PathBuf>,
    /// Permission bits of the Unix socket.
    pub unix_mode: Option<u32>,
    /// How clients authenticate.
    pub security: SecurityMode,
    /// Principal granted to every connection, unless the mode derives one per client from users or
    /// a certificate mapping.
    pub principal: Option<String>,
    /// Allows a `trusted_local` listener to bind a non-loopback address.
    pub trusted_network: bool,
    /// Peer addresses allowed to connect. Empty allows every peer.
    pub allow_peers: Vec<IpAddr>,
    /// Whether to answer one-byte UDP heartbeats on the same address.
    pub udp_heartbeat: bool,
    /// Whether the session stays open after a batch. When false, it closes after its first batch.
    pub keepalive: bool,
    /// TLS settings. Required for `mtls`.
    pub tls: Option<TlsConfig>,
    /// Shared key for `shared_key` listeners.
    pub shared_key: Option<SecretRef>,
    /// Users of a `shared_key` listener, by user name.
    pub users: BTreeMap<String, UserConfig>,
    /// Hostname sent in the handshake, for `shared_key` listeners.
    pub server_hostname: Option<String>,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            bind: None,
            unix_path: None,
            unix_mode: None,
            security: SecurityMode::TrustedLocal,
            principal: None,
            trusted_network: false,
            allow_peers: Vec::new(),
            udp_heartbeat: false,
            keepalive: true,
            tls: None,
            shared_key: None,
            users: BTreeMap::new(),
            server_hostname: None,
        }
    }
}

/// Routes frames from one listener to one index, and sets how their events are normalized.
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RouteConfig {
    /// Name of the listener this route serves.
    pub listener: String,
    /// Exact tag to match. Mutually exclusive with `tag_prefix`.
    pub tag: Option<String>,
    /// Tag prefix to match when `tag` is unset.
    pub tag_prefix: String,
    /// Index that receives the events.
    pub index: String,
    /// Normalizer: `generic` for the generic path, `docker` for container logs.
    pub normalizer: NormalizationProfile,
    /// JSON profile: `text`, `pino`, or `json_auto`.
    pub json_profile: JsonProfile,
    /// Service label to store. Takes precedence over `service_from`.
    pub service: Option<String>,
    /// Derives the service from the tag, currently only `tag_suffix`.
    pub service_from: Option<String>,
    /// Host label to store.
    pub host: Option<String>,
    /// Level assigned to events that carry none. Non-negative.
    pub default_level: Option<i32>,
    /// Fields to read the message from. Defaults to `message` and `msg`.
    pub message_fields: Vec<String>,
    /// Field whose value names the compose service of a container log.
    pub compose_service_label: Option<String>,
    /// Field that holds the application's own timestamp.
    pub application_time_field: Option<String>,
    /// Format of `application_time_field`: `unix_ms` or `unix_seconds`.
    pub application_time_format: Option<String>,
}

impl ForwardConfig {
    /// Checks every limit, timeout and cross-reference, so a receiver never starts with a
    /// configuration it cannot enforce.
    ///
    /// # Errors
    ///
    /// Returns an invalid error for the first requirement that fails.
    pub fn validate(&self, indexes: &HashMap<String, IndexSettings>) -> Result<(), Error> {
        self.validate_limits()?;
        self.validate_timeouts()?;
        self.validate_counts()?;
        self.validate_principals(indexes)?;
        let listener_names = self.validate_listeners()?;
        self.validate_routes(indexes, &listener_names)
    }

    /// Peak coexistence: expanded payload, decoded tree and normalized representation.
    pub fn decode_reservation(&self) -> usize {
        self.max_expanded_bytes
            .saturating_mul(2)
            .saturating_add(self.max_normalized_bytes)
    }

    fn validate_limits(&self) -> Result<(), Error> {
        self.validate_frame_limits()?;
        self.validate_decode_limits()?;
        self.validate_budget_limits()
    }

    /// Sizes of connections, frames, events and normalized output, each inside its range and
    /// inside the limit that contains it.
    fn validate_frame_limits(&self) -> Result<(), Error> {
        if !CONNECTIONS_RANGE.contains(&self.max_connections) {
            return Err(Error::invalid(format!(
                "Forward max_connections must be {}–{}",
                CONNECTIONS_RANGE.start(),
                CONNECTIONS_RANGE.end()
            )));
        }

        if !FRAME_BYTES_RANGE.contains(&self.max_frame_bytes) {
            return Err(Error::invalid(format!(
                "Forward max_frame_bytes must be {}–{} bytes",
                FRAME_BYTES_RANGE.start(),
                FRAME_BYTES_RANGE.end()
            )));
        }

        if self.max_expanded_bytes < self.max_frame_bytes {
            return Err(Error::invalid(
                "Forward max_expanded_bytes must be at least max_frame_bytes",
            ));
        }

        if self.max_expanded_bytes > MAX_EXPANDED_BYTES {
            return Err(Error::invalid(format!(
                "Forward max_expanded_bytes must not exceed {MAX_EXPANDED_BYTES} bytes"
            )));
        }

        if !EVENTS_PER_FRAME_RANGE.contains(&self.max_events_per_frame) {
            return Err(Error::invalid(format!(
                "Forward max_events_per_frame must be {}–{}",
                EVENTS_PER_FRAME_RANGE.start(),
                EVENTS_PER_FRAME_RANGE.end()
            )));
        }

        if !EVENT_BYTES_RANGE.contains(&self.max_event_bytes) {
            return Err(Error::invalid(format!(
                "Forward max_event_bytes must be {}–{} bytes",
                EVENT_BYTES_RANGE.start(),
                EVENT_BYTES_RANGE.end()
            )));
        }

        if self.max_message_bytes == 0 {
            return Err(Error::invalid(
                "Forward max_message_bytes must be at least 1 byte",
            ));
        }

        if self.max_message_bytes > self.max_event_bytes {
            return Err(Error::invalid(
                "Forward max_message_bytes must not exceed max_event_bytes",
            ));
        }

        if self.max_normalized_bytes < self.max_event_bytes {
            return Err(Error::invalid(
                "Forward max_normalized_bytes must be at least max_event_bytes",
            ));
        }

        if self.max_normalized_bytes > MAX_NORMALIZED_BYTES {
            return Err(Error::invalid(format!(
                "Forward max_normalized_bytes must not exceed {MAX_NORMALIZED_BYTES} bytes"
            )));
        }

        Ok(())
    }

    /// Metadata depth, tag and chunk sizes, decompression ratio and CPU parallelism.
    fn validate_decode_limits(&self) -> Result<(), Error> {
        if !METADATA_DEPTH_RANGE.contains(&self.max_metadata_depth) {
            return Err(Error::invalid(format!(
                "Forward max_metadata_depth must be {}–{}",
                METADATA_DEPTH_RANGE.start(),
                METADATA_DEPTH_RANGE.end()
            )));
        }

        if !TAG_BYTES_RANGE.contains(&self.max_tag_bytes) {
            return Err(Error::invalid(format!(
                "Forward max_tag_bytes must be {}–{} bytes",
                TAG_BYTES_RANGE.start(),
                TAG_BYTES_RANGE.end()
            )));
        }

        if !CHUNK_BYTES_RANGE.contains(&self.max_chunk_bytes) {
            return Err(Error::invalid(format!(
                "Forward max_chunk_bytes must be {}–{} bytes",
                CHUNK_BYTES_RANGE.start(),
                CHUNK_BYTES_RANGE.end()
            )));
        }

        if !CPU_JOBS_RANGE.contains(&self.max_cpu_jobs) {
            return Err(Error::invalid(format!(
                "Forward max_cpu_jobs must be {}–{}",
                CPU_JOBS_RANGE.start(),
                CPU_JOBS_RANGE.end()
            )));
        }

        if self.max_expansion_ratio == 0 {
            return Err(Error::invalid(
                "Forward max_expansion_ratio must be at least 1",
            ));
        }

        if self.max_gzip_members == 0 {
            return Err(Error::invalid(
                "Forward max_gzip_members must be at least 1",
            ));
        }

        if self.max_gzip_members > MAX_GZIP_MEMBERS {
            return Err(Error::invalid(format!(
                "Forward max_gzip_members must not exceed {MAX_GZIP_MEMBERS}"
            )));
        }

        Ok(())
    }

    /// Memory budgets must hold the largest frame and decode they admit, and fit a semaphore
    /// permit count.
    fn validate_budget_limits(&self) -> Result<(), Error> {
        if self.raw_budget_bytes
            < self
                .max_frame_bytes
                .saturating_mul(FRAME_PEAK_MULTIPLIER)
                .saturating_add(READ_SCRATCH_BYTES)
        {
            return Err(Error::invalid(
                "Forward raw_budget_bytes is too small for max_frame_bytes",
            ));
        }

        if u32::try_from(self.raw_budget_bytes).is_err() {
            return Err(Error::invalid("Forward raw_budget_bytes exceeds u32::MAX"));
        }

        if self.decoded_budget_bytes < self.decode_reservation() {
            return Err(Error::invalid(
                "Forward decoded_budget_bytes is too small for decode reservation",
            ));
        }

        if u32::try_from(self.decoded_budget_bytes).is_err() {
            return Err(Error::invalid(
                "Forward decoded_budget_bytes exceeds u32::MAX",
            ));
        }

        Ok(())
    }

    fn validate_timeouts(&self) -> Result<(), Error> {
        let timeouts = [
            ("handshake_timeout_ms", self.handshake_timeout_ms),
            ("frame_idle_timeout_ms", self.frame_idle_timeout_ms),
            ("frame_total_timeout_ms", self.frame_total_timeout_ms),
            ("session_idle_timeout_ms", self.session_idle_timeout_ms),
            ("admission_timeout_ms", self.admission_timeout_ms),
            ("ack_write_timeout_ms", self.ack_write_timeout_ms),
            ("decode_timeout_ms", self.decode_timeout_ms),
        ];
        for (name, timeout) in timeouts {
            if !TIMEOUT_MS_RANGE.contains(&timeout) {
                return Err(Error::invalid(format!(
                    "Forward {name} must be {}–{}ms",
                    TIMEOUT_MS_RANGE.start(),
                    TIMEOUT_MS_RANGE.end()
                )));
            }
        }

        if self.frame_total_timeout_ms < self.frame_idle_timeout_ms {
            return Err(Error::invalid(
                "Forward total frame timeout must cover idle timeout",
            ));
        }

        Ok(())
    }

    fn validate_counts(&self) -> Result<(), Error> {
        if self.listeners.len() > MAX_LISTENERS {
            return Err(Error::invalid("too many configured Forward listeners"));
        }

        if self.principals.len() > MAX_PRINCIPALS {
            return Err(Error::invalid("too many configured Forward principals"));
        }

        if self.routes.len() > MAX_ROUTES {
            return Err(Error::invalid("too many configured Forward routes"));
        }

        if self.enabled {
            if self.listeners.is_empty() {
                return Err(Error::invalid(
                    "enabled Forward requires at least one listener",
                ));
            }

            if self.principals.is_empty() {
                return Err(Error::invalid(
                    "enabled Forward requires at least one principal",
                ));
            }

            if self.routes.is_empty() {
                return Err(Error::invalid(
                    "enabled Forward requires at least one route",
                ));
            }
        }

        Ok(())
    }

    fn validate_principals(&self, indexes: &HashMap<String, IndexSettings>) -> Result<(), Error> {
        for (name, principal) in &self.principals {
            validate_source(name)?;
            validate_source(&principal.source)?;
            if principal.indexes.is_empty() {
                return Err(Error::invalid(
                    "Forward principal requires at least one index",
                ));
            }

            if principal.indexes.len() > MAX_PRINCIPAL_INDEXES {
                return Err(Error::invalid("Forward principal exceeds index limit"));
            }

            for index in &principal.indexes {
                validate_index_name(index)?;
                if index != "default" && !indexes.contains_key(index) {
                    return Err(Error::invalid(
                        "Forward principal index must be predeclared",
                    ));
                }
            }
        }

        Ok(())
    }

    /// Checks each listener and returns the set of listener names that routes may refer to.
    fn validate_listeners(&self) -> Result<HashSet<&str>, Error> {
        let mut names = HashSet::new();
        for listener in &self.listeners {
            validate_source(&listener.name)?;
            if !names.insert(listener.name.as_str()) {
                return Err(Error::invalid("duplicate Forward listener name"));
            }

            self.validate_listener(listener)?;
        }

        Ok(names)
    }

    fn validate_listener(&self, listener: &ListenerConfig) -> Result<(), Error> {
        validate_listener_endpoint(listener)?;
        self.validate_listener_security(listener)?;

        super::security::Security::load(listener)?;
        if let Some(tls) = &listener.tls {
            self.validate_fingerprints(tls)?;
        }

        Ok(())
    }

    fn validate_listener_security(&self, listener: &ListenerConfig) -> Result<(), Error> {
        if let Some(principal) = &listener.principal
            && !self.principals.contains_key(principal)
        {
            return Err(Error::invalid("unknown Forward listener principal"));
        }

        validate_security_fields(listener)?;

        if listener.udp_heartbeat
            && listener.bind.is_some_and(|bind| !bind.ip().is_loopback())
            && listener.allow_peers.is_empty()
        {
            return Err(Error::invalid(
                "non-loopback Forward heartbeat requires explicit peer ACL",
            ));
        }

        match listener.security {
            SecurityMode::SharedKey => self.validate_shared_key(listener),
            SecurityMode::Mtls => validate_mtls(listener),
            SecurityMode::TrustedLocal => Ok(()),
        }
    }

    fn validate_shared_key(&self, listener: &ListenerConfig) -> Result<(), Error> {
        listener
            .shared_key
            .as_ref()
            .ok_or_else(|| Error::invalid("secure Forward requires shared_key"))?
            .load()?;
        let Some(hostname) = &listener.server_hostname else {
            return Err(Error::invalid("secure Forward requires server_hostname"));
        };
        validate_shared_key_hostname(hostname)?;
        self.validate_shared_key_users(listener)
    }

    fn validate_shared_key_users(&self, listener: &ListenerConfig) -> Result<(), Error> {
        if listener.users.len() > MAX_LISTENER_USERS {
            return Err(Error::invalid("Forward listener users exceed limit"));
        }

        if listener.users.is_empty() && listener.principal.is_none() {
            return Err(Error::invalid("secure Forward requires principal or users"));
        }

        for (name, user) in &listener.users {
            if name.is_empty() {
                return Err(Error::invalid("Forward user name must not be empty"));
            }

            if name.len() > MAX_USER_NAME_BYTES {
                return Err(Error::invalid("Forward user name exceeds limit"));
            }

            if !self.principals.contains_key(&user.principal) {
                return Err(Error::invalid("unknown Forward user principal"));
            }

            user.password.load()?;
        }

        Ok(())
    }

    fn validate_fingerprints(&self, tls: &TlsConfig) -> Result<(), Error> {
        for (fingerprint, principal) in &tls.client_fingerprints {
            if fingerprint.len() != FINGERPRINT_HEX_CHARS {
                return Err(Error::invalid(
                    "Forward certificate fingerprint must be 64 hex characters",
                ));
            }

            if !fingerprint
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            {
                return Err(Error::invalid(
                    "Forward certificate fingerprint must be lowercase hex",
                ));
            }

            if !self.principals.contains_key(principal) {
                return Err(Error::invalid(
                    "unknown Forward certificate fingerprint principal",
                ));
            }
        }

        Ok(())
    }

    fn validate_routes(
        &self,
        indexes: &HashMap<String, IndexSettings>,
        listener_names: &HashSet<&str>,
    ) -> Result<(), Error> {
        for (position, route) in self.routes.iter().enumerate() {
            self.validate_route_tag(route)?;
            validate_route_fields(route)?;
            self.validate_route_target(route, indexes, listener_names)?;
            validate_route_normalization(route)?;
            if self.routes[..position].iter().any(|previous| {
                previous.listener == route.listener && routes_overlap(previous, route)
            }) {
                return Err(Error::invalid("ambiguous overlapping Forward routes"));
            }
        }

        Ok(())
    }

    fn validate_route_tag(&self, route: &RouteConfig) -> Result<(), Error> {
        if let Some(tag) = &route.tag {
            if tag.is_empty() {
                return Err(Error::invalid("Forward route tag must not be empty"));
            }

            if tag.len() > self.max_tag_bytes {
                return Err(Error::invalid("Forward route tag exceeds max_tag_bytes"));
            }

            if !route.tag_prefix.is_empty() {
                return Err(Error::invalid(
                    "Forward route cannot set both tag and tag_prefix",
                ));
            }
        }

        if route.tag.is_some() && route.service_from.is_some() {
            return Err(Error::invalid(
                "exact Forward tag routes require explicit service instead of tag_suffix",
            ));
        }

        Ok(())
    }

    fn validate_route_target(
        &self,
        route: &RouteConfig,
        indexes: &HashMap<String, IndexSettings>,
        listener_names: &HashSet<&str>,
    ) -> Result<(), Error> {
        if !listener_names.contains(route.listener.as_str()) {
            return Err(Error::invalid("Forward route references unknown listener"));
        }

        if route.index != "default" && !indexes.contains_key(&route.index) {
            return Err(Error::invalid("Forward route references unknown index"));
        }

        if route.tag_prefix.len() > self.max_tag_bytes {
            return Err(Error::invalid(
                "Forward route tag_prefix exceeds max_tag_bytes",
            ));
        }

        validate_index_name(&route.index)
    }
}

/// Checks the endpoint and transport policy before any security material is loaded.
fn validate_listener_endpoint(listener: &ListenerConfig) -> Result<(), Error> {
    if listener.bind.is_none() && listener.unix_path.is_none() {
        return Err(Error::invalid(
            "Forward listener requires bind or unix_path",
        ));
    }

    if listener.bind.is_some() && listener.unix_path.is_some() {
        return Err(Error::invalid(
            "Forward listener cannot set both bind and unix_path",
        ));
    }

    if listener.allow_peers.len() > MAX_PEER_ACL {
        return Err(Error::invalid("Forward peer ACL exceeds limit"));
    }

    validate_unix_listener(listener)?;
    if listener.security == SecurityMode::TrustedLocal {
        validate_trusted_local(listener)?;
    }

    Ok(())
}

/// Refuses credentials and mappings that do not belong to the selected security mode.
fn validate_security_fields(listener: &ListenerConfig) -> Result<(), Error> {
    if listener.security != SecurityMode::SharedKey {
        if listener.shared_key.is_some() {
            return Err(Error::invalid(
                "Forward shared_key requires security=shared_key",
            ));
        }

        if !listener.users.is_empty() {
            return Err(Error::invalid("Forward users require security=shared_key"));
        }

        if listener.server_hostname.is_some() {
            return Err(Error::invalid(
                "Forward server_hostname requires security=shared_key",
            ));
        }
    }

    if listener.security != SecurityMode::Mtls
        && listener
            .tls
            .as_ref()
            .is_some_and(|tls| !tls.client_fingerprints.is_empty())
    {
        return Err(Error::invalid(
            "Forward certificate principal mappings require security=mtls",
        ));
    }

    Ok(())
}

/// The shared-key hostname is explicit and bounded before it enters a challenge.
fn validate_shared_key_hostname(hostname: &str) -> Result<(), Error> {
    if hostname.is_empty() {
        return Err(Error::invalid(
            "secure Forward server_hostname must not be empty",
        ));
    }

    if hostname.len() > MAX_SERVER_HOSTNAME_BYTES {
        return Err(Error::invalid(
            "secure Forward server_hostname exceeds limit",
        ));
    }

    Ok(())
}

/// The Unix socket policy: an absolute path, a restrictive mode, and no TLS or UDP.
fn validate_unix_listener(listener: &ListenerConfig) -> Result<(), Error> {
    if let Some(path) = &listener.unix_path {
        if !path.is_absolute() {
            return Err(Error::invalid(
                "Unix Forward listener requires absolute unix_path",
            ));
        }

        if listener.tls.is_some() {
            return Err(Error::invalid("Unix Forward listener cannot use TLS"));
        }

        if listener.udp_heartbeat {
            return Err(Error::invalid(
                "Unix Forward listener cannot use udp_heartbeat",
            ));
        }

        if listener.security == SecurityMode::Mtls {
            return Err(Error::invalid(
                "Unix Forward listener cannot use security=mtls",
            ));
        }

        let Some(mode) = listener.unix_mode else {
            return Err(Error::invalid("Unix Forward listener requires unix_mode"));
        };
        if mode & !UNIX_MODE_ALLOWED_BITS != 0
            || mode & UNIX_MODE_REQUIRED_BITS != UNIX_MODE_REQUIRED_BITS
        {
            return Err(Error::invalid(
                "Unix Forward listener requires restrictive unix_mode",
            ));
        }
    }

    Ok(())
}

/// A `trusted_local` listener must have a principal, and a non-loopback address only with
/// `trusted_network`.
fn validate_trusted_local(listener: &ListenerConfig) -> Result<(), Error> {
    if listener.bind.is_some_and(|bind| !bind.ip().is_loopback()) && !listener.trusted_network {
        return Err(Error::invalid(
            "non-loopback trusted Forward requires trusted_network=true",
        ));
    }

    if listener.principal.is_none() {
        return Err(Error::invalid(
            "trusted Forward listener requires principal",
        ));
    }

    Ok(())
}

fn validate_mtls(listener: &ListenerConfig) -> Result<(), Error> {
    let tls = listener
        .tls
        .as_ref()
        .ok_or_else(|| Error::invalid("mTLS Forward requires TLS configuration"))?;
    if tls.client_ca.is_none() {
        return Err(Error::invalid("mTLS Forward requires client_ca"));
    }

    if tls.client_fingerprints.is_empty() {
        return Err(Error::invalid("mTLS Forward requires client_fingerprints"));
    }

    if tls.client_fingerprints.len() > MAX_FINGERPRINTS {
        return Err(Error::invalid(
            "mTLS Forward client_fingerprints exceed limit",
        ));
    }

    Ok(())
}

/// Route fields that bound what a route reads: the application time and the selectors.
fn validate_route_fields(route: &RouteConfig) -> Result<(), Error> {
    if route.application_time_field.is_some() != route.application_time_format.is_some() {
        return Err(Error::invalid(
            "Forward application time requires both application_time_field and \
             application_time_format",
        ));
    }

    if route
        .application_time_format
        .as_ref()
        .is_some_and(|format| !["unix_ms", "unix_seconds"].contains(&format.as_str()))
    {
        return Err(Error::invalid(
            "Forward application_time_format must be unix_ms or unix_seconds",
        ));
    }

    if let Some(field) = &route.application_time_field {
        if field.is_empty() {
            return Err(Error::invalid(
                "Forward application_time_field must not be empty",
            ));
        }

        if field.len() > MAX_FIELD_NAME_BYTES {
            return Err(Error::invalid(
                "Forward application_time_field exceeds limit",
            ));
        }
    }

    if let Some(label) = &route.compose_service_label {
        if label.is_empty() {
            return Err(Error::invalid(
                "Forward compose_service_label must not be empty",
            ));
        }

        if label.len() > MAX_FIELD_NAME_BYTES {
            return Err(Error::invalid(
                "Forward compose_service_label exceeds limit",
            ));
        }
    }

    Ok(())
}

/// The service source, the default level, and the bounds of the message-field list and the
/// metadata.
fn validate_route_normalization(route: &RouteConfig) -> Result<(), Error> {
    if route
        .service_from
        .as_ref()
        .is_some_and(|v| v != "tag_suffix")
    {
        return Err(Error::invalid(
            "Forward route service_from must be tag_suffix",
        ));
    }

    if route.default_level.is_some_and(|v| v < 0) {
        return Err(Error::invalid(
            "Forward route default_level must be non-negative",
        ));
    }

    if route.message_fields.len() > MAX_ROUTE_MESSAGE_FIELDS {
        return Err(Error::invalid("Forward route message_fields exceed limit"));
    }

    if route.message_fields.iter().any(String::is_empty) {
        return Err(Error::invalid(
            "Forward route message_fields entry must not be empty",
        ));
    }

    if route
        .message_fields
        .iter()
        .any(|v| v.len() > MAX_FIELD_NAME_BYTES)
    {
        return Err(Error::invalid(
            "Forward route message_fields entry exceeds limit",
        ));
    }

    if route
        .service
        .as_ref()
        .is_some_and(|v| v.len() > MAX_ROUTE_METADATA_BYTES)
    {
        return Err(Error::invalid("Forward route service exceeds limit"));
    }

    if route
        .host
        .as_ref()
        .is_some_and(|v| v.len() > MAX_ROUTE_METADATA_BYTES)
    {
        return Err(Error::invalid("Forward route host exceeds limit"));
    }

    Ok(())
}

/// Whether two routes on the same listener could match the same tag.
fn routes_overlap(left: &RouteConfig, right: &RouteConfig) -> bool {
    match (&left.tag, &right.tag) {
        (Some(left), Some(right)) => left == right,
        (Some(tag), None) => tag.starts_with(&right.tag_prefix),
        (None, Some(tag)) => tag.starts_with(&left.tag_prefix),
        (None, None) => {
            left.tag_prefix.starts_with(&right.tag_prefix)
                || right.tag_prefix.starts_with(&left.tag_prefix)
        }
    }
}

#[cfg(test)]
mod hostname_tests {
    use super::*;

    #[test]
    fn shared_key_requires_explicit_bounded_server_hostname() {
        let key = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(key.path(), b"secret").unwrap();

        let mut listener = ListenerConfig {
            security: SecurityMode::SharedKey,
            principal: Some("p".into()),
            shared_key: Some(SecretRef {
                file: Some(key.path().to_owned()),
                env: None,
            }),
            ..ListenerConfig::default()
        };
        let config = ForwardConfig::default();
        for hostname in [None, Some(String::new()), Some("x".repeat(256))] {
            listener.server_hostname = hostname;
            assert!(config.validate_shared_key(&listener).is_err());
        }

        listener.server_hostname = Some("forward.example".into());
        config.validate_shared_key(&listener).unwrap();
    }

    #[test]
    fn route_normalizer_and_json_profile_deserialize_as_enums_and_validate_distinct_checks() {
        let route: RouteConfig = toml::from_str(
            r#"
            listener = "main"
            normalizer = "docker"
            json_profile = "pino"
            "#,
        )
        .unwrap();
        assert_eq!(route.normalizer, NormalizationProfile::Docker);
        assert_eq!(route.json_profile, JsonProfile::Pino);

        assert!(toml::from_str::<RouteConfig>(r#"normalizer = "unknown""#).is_err());
        assert!(toml::from_str::<RouteConfig>(r#"json_profile = "unknown""#).is_err());

        let indexes = HashMap::new();
        let bad_frame = ForwardConfig {
            max_frame_bytes: 512,
            ..ForwardConfig::default()
        };
        let bad_expanded = ForwardConfig {
            max_expanded_bytes: 512,
            ..ForwardConfig::default()
        };
        assert_ne!(
            bad_frame.validate(&indexes).unwrap_err().message,
            bad_expanded.validate(&indexes).unwrap_err().message
        );
    }
}
