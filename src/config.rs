//! Process configuration: defaults, the optional TOML file, environment overrides, and the
//! validation that every consumer relies on.

use std::{
    collections::{HashMap, HashSet},
    env::{self, VarError},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{attribute_filter::MAX_ATTRIBUTE_KEY_BYTES, model::Error};

/// The index every installation has, and the one that unscoped requests use.
pub const DEFAULT_INDEX_NAME: &str = "default";

/// The source name given to the ingestion token when `LOGBROOK_SOURCE` is unset.
const DEFAULT_SOURCE_NAME: &str = "default";

// Durations in milliseconds, the unit of every `_ms` setting.
const DAY_MS: i64 = 86_400_000;
const HOUR_MS: i64 = 3_600_000;

// Decimal size units: `memory_limit`, `temp_limit` and `max_size_gb` count in powers of 1,000.
const BYTES_PER_KB: u64 = 1_000;
const BYTES_PER_MB: u64 = 1_000_000;
const BYTES_PER_GB: u64 = 1_000_000_000;

/// A decimal size unit accepted in `memory_limit`, `temp_limit` and per-index budgets.
struct SizeUnit {
    suffix: &'static str,
    bytes: u64,
}

/// The suffixes accepted in `storage.memory_limit` and `storage.temp_limit`, with byte values.
const SIZE_UNITS: [SizeUnit; 3] = [
    SizeUnit {
        suffix: "KB",
        bytes: BYTES_PER_KB,
    },
    SizeUnit {
        suffix: "MB",
        bytes: BYTES_PER_MB,
    },
    SizeUnit {
        suffix: "GB",
        bytes: BYTES_PER_GB,
    },
];

/// Largest number of bytes one permit request can take: `try_acquire_many_owned` counts in `u32`.
// Supported targets are 32- or 64-bit, so this semaphore limit fits usize exactly.
const MAX_PERMIT_BYTES: usize = u32::MAX as usize;

/// Upper bound accepted for `max_indexes` and `LOGBROOK_MAX_INDEXES`.
const MAX_INDEXES_LIMIT: usize = 64;

/// Longest index name, in bytes; names become directory names and metric labels.
const MAX_INDEX_NAME_BYTES: usize = 63;

/// Longest source name, in bytes, in `ingest_tokens` values and `read_tokens` allowlists.
const MAX_SOURCE_BYTES: usize = 128;

/// Shortest ingest, read or admin token accepted, in bytes.
const MIN_TOKEN_BYTES: usize = 16;

/// Longest ingest, read or admin token accepted, in bytes.
const MAX_TOKEN_BYTES: usize = 512;

/// Most keys allowed in `storage.promoted_string_keys`, process-wide or per index.
const MAX_PROMOTED_STRING_KEYS: usize = 8;

/// Floor for `storage.queue_bytes`, both process-wide and per index after division, and for
/// `storage.max_query_bytes`.
const MIN_QUEUE_BYTES: usize = 1024;

/// Smallest disk target accepted for `max_size_bytes` (16 MiB), process-wide or per index.
const MIN_MAX_SIZE_BYTES: u64 = 16 * 1024 * 1024;

/// Largest number allowed before the unit in `storage.memory_limit` and `storage.temp_limit`.
const MAX_SIZE_NUMBER: u64 = 1_000_000;

/// Upper bound accepted for `max_body_bytes` (16 MiB).
const MAX_BODY_BYTES_LIMIT: usize = 16 * 1024 * 1024;

/// Upper bound accepted for `max_events`, per HTTP ingestion batch.
const MAX_EVENTS_LIMIT: usize = 10_000;

/// Upper bound accepted for `max_metadata_depth`, in nesting levels.
const MAX_METADATA_DEPTH_LIMIT: usize = 64;

/// Upper bound accepted for `max_inflight_requests`.
const MAX_INFLIGHT_REQUESTS_LIMIT: usize = 1024;

/// Upper bound accepted for `body_total_timeout_ms` (five minutes), which also caps
/// `body_idle_timeout_ms`.
const MAX_BODY_TIMEOUT_MS: u64 = 300_000;

/// Upper bound accepted for `max_future_skew_ms`: one day.
const MAX_FUTURE_SKEW_MS: i64 = DAY_MS;

/// Lower bound accepted for `max_tail_buffer_bytes`.
const MIN_TAIL_BUFFER_BYTES: usize = 1024;

/// Upper bound accepted for `max_tail_buffer_bytes` (64 MiB).
const MAX_TAIL_BUFFER_BYTES: usize = 64 * 1024 * 1024;

/// Room a tail buffer keeps beyond two maximum events, for the framing of the frames it carries.
const TAIL_HEADROOM_BYTES: usize = 1024;

/// Upper bound accepted for `max_connections`.
const MAX_CONNECTIONS_LIMIT: usize = 65_536;

/// Upper bound accepted for `header_timeout_ms` (five minutes).
const MAX_HEADER_TIMEOUT_MS: u64 = 300_000;

/// Upper bound accepted for `max_query_range_ms`: 366 days.
const MAX_QUERY_RANGE_LIMIT_MS: i64 = 366 * DAY_MS;

/// Upper bound accepted for `max_tail_subscribers`.
const MAX_TAIL_SUBSCRIBERS_LIMIT: usize = 1024;

/// Lower bound accepted for `tail_poll_ms`, so an idle tail wakes at most four times a second.
const MIN_TAIL_POLL_MS: u64 = 250;

/// Upper bound accepted for `storage.reader_threads`, per index.
const MAX_READER_THREADS: usize = 16;

/// Upper bound accepted for `storage.queue_capacity`, per index.
const MAX_QUEUE_CAPACITY: usize = 1024;

/// Upper bound accepted for `storage.duckdb_threads`, per `DuckDB` connection.
const MAX_DUCKDB_THREADS: usize = 64;

/// Upper bound accepted for `storage.query_timeout_ms` (five minutes).
const MAX_QUERY_TIMEOUT_MS: u64 = 300_000;

/// Upper bound accepted for `storage.max_query_bytes` (64 MiB).
const MAX_QUERY_BYTES_LIMIT: usize = 64 * 1024 * 1024;

/// Upper bound accepted for `storage.archive_batch_rows`.
const MAX_ARCHIVE_BATCH_ROWS: usize = 1_000_000;

/// Upper bound accepted for `storage.archive_partition_ms`: one day.
const MAX_ARCHIVE_PARTITION_MS: i64 = DAY_MS;

/// Lower bound accepted for `storage.compact_max_files`; a batch of one file has nothing to merge.
const MIN_COMPACT_MAX_FILES: usize = 2;

/// Upper bound accepted for `storage.compact_max_files`.
const MAX_COMPACT_MAX_FILES: usize = 256;

/// Lower bound accepted for `storage.compact_max_bytes`.
const MIN_COMPACT_MAX_BYTES: u64 = 1024;

/// Upper bound accepted for `storage.compact_max_bytes` (8 GiB).
const MAX_COMPACT_MAX_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Upper bound accepted for `storage.maintenance_timeout_ms` (five minutes).
const MAX_MAINTENANCE_TIMEOUT_MS: u64 = 300_000;

/// Address the server listens on when `bind` and `LOGBROOK_BIND` are unset: loopback, port 3100.
const DEFAULT_BIND: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3100);

/// Default `storage.data_dir`, relative to the working directory; `LOGBROOK_DATA_DIR` overrides it.
const DEFAULT_DATA_DIR: &str = "data";

/// Default `storage.reader_threads`: read workers per index.
const DEFAULT_READER_THREADS: usize = 2;

/// Default `storage.queue_capacity`: queued append requests per index.
const DEFAULT_QUEUE_CAPACITY: usize = 16;

/// Default `storage.queue_bytes` (32 MiB), a process-wide budget split among `max_indexes` slots.
const DEFAULT_QUEUE_BYTES: usize = 32 * 1024 * 1024;

/// Default `storage.memory_limit`, a process-wide budget split among `max_indexes` slots.
const DEFAULT_MEMORY_LIMIT: &str = "512MB";

/// Default `storage.temp_limit`, a process-wide budget split among `max_indexes` slots.
const DEFAULT_TEMP_LIMIT: &str = "2GB";

/// Default `storage.duckdb_threads`: execution threads per `DuckDB` connection.
const DEFAULT_DUCKDB_THREADS: usize = 2;

/// Default `storage.query_timeout_ms`: five seconds.
const DEFAULT_QUERY_TIMEOUT_MS: u64 = 5_000;

/// Default `storage.max_query_bytes` (8 MiB).
const DEFAULT_MAX_QUERY_BYTES: usize = 8 * 1024 * 1024;

/// Default `storage.archive_batch_rows`: rows written per archive batch.
const DEFAULT_ARCHIVE_BATCH_ROWS: usize = 100_000;

/// Default `storage.archive_partition_ms`: one hour.
const DEFAULT_ARCHIVE_PARTITION_MS: i64 = HOUR_MS;

/// Default `storage.compact_max_files`: archive files merged per compaction batch.
const DEFAULT_COMPACT_MAX_FILES: usize = 16;

/// Default `storage.compact_max_bytes` (128 MiB).
const DEFAULT_COMPACT_MAX_BYTES: u64 = 128 * 1024 * 1024;

/// Default `storage.maintenance_timeout_ms`: 30 seconds.
const DEFAULT_MAINTENANCE_TIMEOUT_MS: u64 = 30_000;

/// Default `max_indexes`, counting the mandatory `default` index.
const DEFAULT_MAX_INDEXES: usize = 4;

/// Default `max_body_bytes` (1 MiB).
const DEFAULT_MAX_BODY_BYTES: usize = 1024 * 1024;

/// Default `max_event_bytes` (64 KiB).
const DEFAULT_MAX_EVENT_BYTES: usize = 64 * 1024;

/// Default `max_events`: events per HTTP ingestion batch.
const DEFAULT_MAX_EVENTS: usize = 1_000;

/// Default `max_metadata_depth`: nesting levels of event metadata.
const DEFAULT_MAX_METADATA_DEPTH: usize = 16;

/// Default `max_field_bytes`: UTF-8 bytes per event field, metadata key or metadata string.
const DEFAULT_MAX_FIELD_BYTES: usize = 4096;

/// Default `max_inflight_requests`: concurrently admitted HTTP requests.
const DEFAULT_MAX_INFLIGHT_REQUESTS: usize = 16;

/// Default `max_decoded_bytes` (64 MiB).
const DEFAULT_MAX_DECODED_BYTES: usize = 64 * 1024 * 1024;

/// Default `max_inflight_body_bytes` (16 MiB).
const DEFAULT_MAX_INFLIGHT_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Default `body_idle_timeout_ms`: five seconds.
const DEFAULT_BODY_IDLE_TIMEOUT_MS: u64 = 5_000;

/// Default `body_total_timeout_ms`: 30 seconds.
const DEFAULT_BODY_TOTAL_TIMEOUT_MS: u64 = 30_000;

/// Default `max_future_skew_ms`: five minutes.
const DEFAULT_MAX_FUTURE_SKEW_MS: i64 = 300_000;

/// Default `max_tail_buffer_bytes` (1 MiB).
const DEFAULT_MAX_TAIL_BUFFER_BYTES: usize = 1024 * 1024;

/// Default `max_connections`: simultaneously served HTTP connections.
const DEFAULT_MAX_CONNECTIONS: usize = 128;

/// Default `header_timeout_ms`: ten seconds.
const DEFAULT_HEADER_TIMEOUT_MS: u64 = 10_000;

/// Default `max_query_range_ms`: 31 days.
const DEFAULT_MAX_QUERY_RANGE_MS: i64 = 31 * DAY_MS;

/// Default `max_tail_subscribers`: concurrent live-tail subscribers.
const DEFAULT_MAX_TAIL_SUBSCRIBERS: usize = 8;

/// Default `tail_poll_ms`: one second.
const DEFAULT_TAIL_POLL_MS: u64 = 1_000;

/// Default `archive_after_ms`: events become eligible for archival after one day.
const DEFAULT_ARCHIVE_AFTER_MS: i64 = DAY_MS;

/// Default `retention_ms`: seven days.
const DEFAULT_RETENTION_MS: i64 = 7 * DAY_MS;

/// Default `maintenance_interval_secs`: one minute between maintenance passes.
const DEFAULT_MAINTENANCE_INTERVAL_SECS: u64 = 60;

/// Default `shutdown_timeout_secs`: 30 seconds to drain listeners, sessions and storage.
const DEFAULT_SHUTDOWN_TIMEOUT_SECS: u64 = 30;

/// Returned by `Config::validate` when any top-level request, body, memory, timeout, tail or
/// query limit is out of range.
const INVALID_HTTP_LIMITS: &str = "invalid HTTP limits: check body/event/count/depth/concurrency/\
     decoded memory/query/tail budgets";

/// Returned by `Config::validate` when any `[storage]` thread, queue, query, size, archive or
/// compaction setting is out of range.
const INVALID_STORAGE_BUDGETS: &str =
    "invalid storage budgets; sizes must be positive integer KB, MB, or GB values";

/// Returned by `Config::validate` when an ingest, read or admin token is malformed or still a
/// `replace-with-` example value.
const INVALID_TOKEN: &str = "tokens must contain 16–512 visible ASCII characters without \
     whitespace; replace example credentials";

/// Returned by `Config::validate` and `Config::for_index` when `max_indexes` is outside 1 to 64.
const INVALID_INDEX_COUNT: &str = "max_indexes must be between 1 and 64";

/// Storage engine settings for one process, or for one index after `Config::for_index`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// Immutable, opt-in top-level string projections (at most eight literal keys).
    pub promoted_string_keys: Vec<String>,
    /// Root directory for managed index data; effective per-index configs point at one index.
    pub data_dir: PathBuf,

    /// Number of dedicated read workers for each index.
    pub reader_threads: usize,
    /// Maximum queued append requests per index.
    pub queue_capacity: usize,
    /// Maximum reserved append bytes per index after process-budget division.
    pub queue_bytes: usize,

    /// `DuckDB` memory budget as a positive decimal KB, MB, or GB size.
    pub memory_limit: String,
    /// `DuckDB` temporary-disk budget as a positive decimal KB, MB, or GB size.
    pub temp_limit: String,
    /// Maximum execution threads used by each `DuckDB` connection.
    pub duckdb_threads: usize,

    /// Deadline for a storage query, in milliseconds.
    pub query_timeout_ms: u64,
    /// Maximum serialized result bytes returned by a storage query.
    pub max_query_bytes: usize,

    /// Maximum rows written in one archive batch.
    pub archive_batch_rows: usize,
    /// Width of one archive time partition, in milliseconds.
    pub archive_partition_ms: i64,
    /// Maximum archive files considered by one compaction batch.
    pub compact_max_files: usize,
    /// Maximum source archive bytes considered by one compaction batch.
    pub compact_max_bytes: u64,
    /// Deadline for one storage maintenance operation, in milliseconds.
    pub maintenance_timeout_ms: u64,

    /// Per-index measured disk footprint target; None disables size retention.
    pub max_size_bytes: Option<u64>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from(DEFAULT_DATA_DIR),
            promoted_string_keys: Vec::new(),

            reader_threads: DEFAULT_READER_THREADS,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            queue_bytes: DEFAULT_QUEUE_BYTES,

            memory_limit: DEFAULT_MEMORY_LIMIT.into(),
            temp_limit: DEFAULT_TEMP_LIMIT.into(),
            duckdb_threads: DEFAULT_DUCKDB_THREADS,

            query_timeout_ms: DEFAULT_QUERY_TIMEOUT_MS,
            max_query_bytes: DEFAULT_MAX_QUERY_BYTES,

            archive_batch_rows: DEFAULT_ARCHIVE_BATCH_ROWS,
            archive_partition_ms: DEFAULT_ARCHIVE_PARTITION_MS,
            compact_max_files: DEFAULT_COMPACT_MAX_FILES,
            compact_max_bytes: DEFAULT_COMPACT_MAX_BYTES,
            maintenance_timeout_ms: DEFAULT_MAINTENANCE_TIMEOUT_MS,

            max_size_bytes: None,
        }
    }
}

/// Per-index overrides from the `[indexes.<name>]` tables. Unset fields inherit the process value.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct IndexSettings {
    /// Optional immutable top-level string projections; absent inherits the process keys.
    pub promoted_string_keys: Option<Vec<String>>,
    /// Optional whole-day retention alias, resolved into retention_ms.
    pub retention_days: Option<u64>,
    /// Optional retention window in milliseconds; absent inherits the process value.
    pub retention_ms: Option<i64>,
    /// Optional archive age in milliseconds; absent inherits the process value.
    pub archive_after_ms: Option<i64>,
    /// Optional decimal-gigabyte disk target alias, resolved into max_size_bytes.
    pub max_size_gb: Option<u64>,
    /// Optional measured disk target in bytes; absent inherits the process target.
    pub max_size_bytes: Option<u64>,
}

/// The whole process configuration. Every limit is checked by `Config::validate`.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// HTTP listener address, overridden by LOGBROOK_BIND when present.
    pub bind: SocketAddr,
    /// Process-wide storage settings divided into fixed per-index budgets.
    pub storage: StorageConfig,
    /// Native Forward receiver listeners, routes, authentication, and admission limits.
    pub forward: crate::forward::config::ForwardConfig,

    /// Optional whole-day retention alias, resolved into retention_ms.
    pub retention_days: Option<u64>,
    /// Optional decimal-gigabyte disk target alias, resolved into max_size_bytes.
    pub max_size_gb: Option<u64>,
    /// Maximum managed indexes, including the mandatory default index.
    pub max_indexes: usize,
    /// Named per-index policy overrides applied before opening storage.
    pub indexes: HashMap<String, IndexSettings>,

    /// Optional index allowlists by read token; an empty list permits every index.
    pub read_index_scopes: HashMap<String, Vec<String>>,
    /// Optional index allowlists by ingest token; an empty list permits every index.
    pub ingest_index_scopes: HashMap<String, Vec<String>>,
    /// Ingest credentials mapped to the validated source each one writes.
    pub ingest_tokens: HashMap<String, String>,
    /// Read credentials mapped to source allowlists; an empty list permits every source.
    pub read_tokens: HashMap<String, Vec<String>>,
    /// Administration credentials, distinct from ingest and read credentials.
    pub admin_tokens: Vec<String>,

    /// Maximum encoded HTTP ingestion body bytes.
    pub max_body_bytes: usize,
    /// Maximum encoded HTTP event bytes.
    pub max_event_bytes: usize,
    /// Maximum events in one HTTP ingestion batch.
    pub max_events: usize,
    /// Maximum nested event-metadata depth accepted over HTTP.
    pub max_metadata_depth: usize,
    /// Maximum UTF-8 bytes in HTTP event fields, metadata keys, and metadata strings.
    pub max_field_bytes: usize,

    /// Maximum HTTP requests admitted concurrently, excluding separately budgeted tails.
    pub max_inflight_requests: usize,
    /// Process-wide decoded HTTP body budget, one semaphore permit per byte.
    pub max_decoded_bytes: usize,
    /// Process-wide raw HTTP body allocation budget, in bytes.
    pub max_inflight_body_bytes: usize,

    /// Maximum idle time between received HTTP body frames, in milliseconds.
    pub body_idle_timeout_ms: u64,
    /// Maximum total HTTP body receive time, in milliseconds.
    pub body_total_timeout_ms: u64,
    /// How far live event timestamps may exceed receipt time, in milliseconds.
    pub max_future_skew_ms: i64,
    /// Maximum queued wire-output bytes per live-tail subscriber.
    pub max_tail_buffer_bytes: usize,
    /// Maximum simultaneously served HTTP connections.
    pub max_connections: usize,
    /// Deadline for receiving HTTP request headers, in milliseconds.
    pub header_timeout_ms: u64,

    /// Maximum half-open query time span, in milliseconds.
    pub max_query_range_ms: i64,
    /// Maximum events returned by a search or tail page.
    pub max_page_size: usize,
    /// Maximum concurrent live-tail subscribers.
    pub max_tail_subscribers: usize,
    /// Fallback wait for a new live-tail commit, in milliseconds.
    pub tail_poll_ms: u64,

    /// Age in milliseconds after which hot events are eligible for archival.
    pub archive_after_ms: i64,
    /// Event retention window in milliseconds; must exceed archive_after_ms.
    pub retention_ms: i64,
    /// Delay between successful maintenance passes, in seconds.
    pub maintenance_interval_secs: u64,
    /// Deadline for draining listeners, sessions, and storage, in seconds.
    pub shutdown_timeout_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: DEFAULT_BIND,
            storage: StorageConfig::default(),
            forward: crate::forward::config::ForwardConfig::default(),
            retention_days: None,
            max_size_gb: None,
            max_indexes: DEFAULT_MAX_INDEXES,
            indexes: HashMap::new(),
            read_index_scopes: HashMap::new(),
            ingest_index_scopes: HashMap::new(),
            ingest_tokens: HashMap::new(),
            read_tokens: HashMap::new(),
            admin_tokens: Vec::new(),
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_event_bytes: DEFAULT_MAX_EVENT_BYTES,
            max_events: DEFAULT_MAX_EVENTS,
            max_metadata_depth: DEFAULT_MAX_METADATA_DEPTH,
            max_field_bytes: DEFAULT_MAX_FIELD_BYTES,
            max_inflight_requests: DEFAULT_MAX_INFLIGHT_REQUESTS,
            max_decoded_bytes: DEFAULT_MAX_DECODED_BYTES,
            max_inflight_body_bytes: DEFAULT_MAX_INFLIGHT_BODY_BYTES,
            body_idle_timeout_ms: DEFAULT_BODY_IDLE_TIMEOUT_MS,
            body_total_timeout_ms: DEFAULT_BODY_TOTAL_TIMEOUT_MS,
            max_future_skew_ms: DEFAULT_MAX_FUTURE_SKEW_MS,
            max_tail_buffer_bytes: DEFAULT_MAX_TAIL_BUFFER_BYTES,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            header_timeout_ms: DEFAULT_HEADER_TIMEOUT_MS,
            max_query_range_ms: DEFAULT_MAX_QUERY_RANGE_MS,
            max_page_size: crate::model::MAX_PAGE_SIZE,
            max_tail_subscribers: DEFAULT_MAX_TAIL_SUBSCRIBERS,
            tail_poll_ms: DEFAULT_TAIL_POLL_MS,
            archive_after_ms: DEFAULT_ARCHIVE_AFTER_MS,
            retention_ms: DEFAULT_RETENTION_MS,
            maintenance_interval_secs: DEFAULT_MAINTENANCE_INTERVAL_SECS,
            shutdown_timeout_secs: DEFAULT_SHUTDOWN_TIMEOUT_SECS,
        }
    }
}

fn read_config<T: DeserializeOwned>(path: &Path) -> Result<T, Error> {
    let text = std::fs::read_to_string(path).map_err(|error| {
        Error::invalid(format!("cannot read config: {error}")).with_source(error)
    })?;
    toml::from_str(&text).map_err(|error| {
        // Parser messages and source excerpts can contain credential keys or values.
        // Retain only the location, never the parser's Display or message output.
        let location = error.span().map(|span| toml_location(&text, span.start));
        let message = match location {
            Some((line, column)) => format!("invalid TOML config at line {line}, column {column}"),
            None => "invalid TOML config".into(),
        };

        Error::invalid(format!(
            "{message}; check syntax, setting names, and value types"
        ))
        .with_source(error)
    })
}

/// The one-based line and column of a byte offset in the configuration text.
fn toml_location(text: &str, offset: usize) -> (usize, usize) {
    text.char_indices()
        .take_while(|(index, _)| *index < offset)
        .fold((1, 1), |(line, column), (_, character)| {
            if character == '\n' {
                (line + 1, 1)
            } else {
                (line, column + 1)
            }
        })
}

/// Parses `LOGBROOK_BIND`, which overrides the TOML bind address.
fn parse_bind(value: &str) -> Result<SocketAddr, Error> {
    value
        .parse()
        // The address parser only says the text is not an address, which the message says.
        .map_err(|error| Error::invalid("LOGBROOK_BIND must be an IP:port").with_source(error))
}

impl Config {
    /// Read only the endpoint needed by the unauthenticated readiness probe.
    /// Health checks do not need producer/read tokens or storage configuration.
    ///
    /// # Errors
    ///
    /// Fails when the file cannot be read or parsed, or when `LOGBROOK_BIND` is malformed.
    pub fn healthcheck_address(path: Option<&Path>) -> Result<SocketAddr, Error> {
        #[derive(Deserialize)]
        struct Endpoint {
            #[serde(default = "default_bind")]
            bind: SocketAddr,
        }

        fn default_bind() -> SocketAddr {
            DEFAULT_BIND
        }

        let mut address = match path {
            Some(path) => read_config::<Endpoint>(path)?.bind,

            None => default_bind(),
        };

        if let Ok(bind) = env::var("LOGBROOK_BIND") {
            address = parse_bind(&bind)?;
        }

        Ok(address)
    }

    /// Defaults < optional TOML file < explicitly supported environment variables.
    ///
    /// # Errors
    ///
    /// Fails when the file cannot be read or parsed, an environment value is malformed, or the
    /// resulting configuration does not pass `validate`.
    pub fn load(path: Option<&Path>) -> Result<Self, Error> {
        let mut config: Self = match path {
            Some(path) => read_config(path)?,
            None => Self::default(),
        };

        config.apply_environment()?;
        config.resolve_policy()?;
        config.validate()?;

        Ok(config)
    }

    /// Applies the environment overrides in a fixed order, so the first invalid value reported
    /// is the same on every run.
    fn apply_environment(&mut self) -> Result<(), Error> {
        if let Ok(bind) = env::var("LOGBROOK_BIND") {
            self.bind = parse_bind(&bind)?;
        }

        if let Some(path) = env::var_os("LOGBROOK_DATA_DIR") {
            self.storage.data_dir = path.into();
        }

        self.apply_environment_tokens()?;
        self.apply_environment_retention()?;
        self.apply_environment_size()?;
        self.apply_environment_indexes()?;
        self.apply_environment_schedule()
    }

    fn apply_environment_tokens(&mut self) -> Result<(), Error> {
        if let Ok(token) = env::var("LOGBROOK_INGEST_TOKEN") {
            self.ingest_tokens.clear();
            self.ingest_tokens.insert(
                token,
                env::var("LOGBROOK_SOURCE").unwrap_or_else(|_| DEFAULT_SOURCE_NAME.into()),
            );
        }

        if let Ok(token) = env::var("LOGBROOK_READ_TOKEN") {
            let sources = self.environment_read_sources(&token)?;
            self.read_tokens.insert(token, sources);
        }

        if let Ok(token) = env::var("LOGBROOK_ADMIN_TOKEN") {
            self.admin_tokens = vec![token];
        }

        Ok(())
    }

    /// The source scope for an environment read token. An explicit `LOGBROOK_READ_SOURCES` wins;
    /// without one, a token that is not also configured in TOML would silently gain every source.
    fn environment_read_sources(&self, token: &str) -> Result<Vec<String>, Error> {
        match env::var("LOGBROOK_READ_SOURCES") {
            Ok(value) if value.is_empty() => Ok(Vec::new()),
            Ok(value) => Ok(value.split(',').map(str::to_owned).collect()),
            Err(_) if self.read_tokens.is_empty() => Ok(Vec::new()),
            Err(_) => self.read_tokens.get(token).cloned().ok_or_else(|| {
                Error::invalid(
                    "LOGBROOK_READ_SOURCES must explicitly set the scope when adding \
                     an environment token to configured read tokens (empty grants all sources)",
                )
            }),
        }
    }

    fn apply_environment_retention(&mut self) -> Result<(), Error> {
        let days = positive_environment_integer::<u64>("LOGBROOK_RETENTION_DAYS")?;
        let millis = positive_environment_integer::<i64>("LOGBROOK_RETENTION_MS")?;
        if days.is_some() && millis.is_some() {
            return Err(Error::invalid(
                "set only one of LOGBROOK_RETENTION_DAYS and LOGBROOK_RETENTION_MS",
            ));
        }

        if let Some(value) = days {
            self.retention_days = Some(value);
        }

        if let Some(value) = millis {
            self.retention_days = None;
            self.retention_ms = value;
        }

        Ok(())
    }

    fn apply_environment_size(&mut self) -> Result<(), Error> {
        let gigabytes = positive_environment_integer::<u64>("LOGBROOK_MAX_SIZE_GB")?;
        let bytes = positive_environment_integer::<u64>("LOGBROOK_MAX_SIZE_BYTES")?;
        if gigabytes.is_some() && bytes.is_some() {
            return Err(Error::invalid(
                "set only one of LOGBROOK_MAX_SIZE_GB and LOGBROOK_MAX_SIZE_BYTES",
            ));
        }

        if let Some(value) = gigabytes {
            self.max_size_gb = Some(value);
        }

        if let Some(value) = bytes {
            self.max_size_gb = None;
            self.storage.max_size_bytes = Some(value);
        }

        Ok(())
    }

    fn apply_environment_indexes(&mut self) -> Result<(), Error> {
        if let Some(value) = positive_environment_integer::<usize>("LOGBROOK_MAX_INDEXES")? {
            self.max_indexes = value;
        }

        if let Ok(names) = env::var("LOGBROOK_INDEXES") {
            for name in names.split(',').filter(|name| !name.is_empty()) {
                validate_index_name(name)?;
                self.indexes.entry(name.to_owned()).or_default();
            }
        }

        Ok(())
    }

    fn apply_environment_schedule(&mut self) -> Result<(), Error> {
        if let Some(value) = positive_environment_integer::<i64>("LOGBROOK_ARCHIVE_AFTER_MS")? {
            self.archive_after_ms = value;
        }

        if let Some(value) =
            positive_environment_integer::<u64>("LOGBROOK_MAINTENANCE_INTERVAL_SECS")?
        {
            self.maintenance_interval_secs = value;
        }

        Ok(())
    }

    /// Turns the `retention_days` and `max_size_gb` aliases into their canonical fields.
    fn resolve_policy(&mut self) -> Result<(), Error> {
        if let Some(days) = self.retention_days {
            self.retention_ms = days_to_millis(days)?;
        }

        if let Some(gb) = self.max_size_gb {
            self.storage.max_size_bytes = Some(gb_to_bytes(gb)?);
        }

        Ok(())
    }

    /// Resolve one index's policy and its fixed share of the process storage budgets.
    ///
    /// # Errors
    ///
    /// Fails when the name is invalid, the index policy is contradictory or out of range, or a
    /// share of the storage budget would be too small to use.
    pub fn for_index(&self, name: &str) -> Result<Self, Error> {
        validate_index_name(name)?;

        if !(1..=MAX_INDEXES_LIMIT).contains(&self.max_indexes) {
            return Err(Error::invalid(INVALID_INDEX_COUNT));
        }

        let mut effective = self.clone();
        effective.resolve_policy()?;

        if let Some(settings) = self.indexes.get(name) {
            effective.apply_index_settings(settings)?;
        }

        // Aliases are already resolved; consumers use the effective canonical fields.
        effective.retention_days = None;
        effective.max_size_gb = None;
        validate_policy(&effective)?;
        validate_promoted_string_keys(&effective.storage.promoted_string_keys)?;

        effective.storage.data_dir = self.storage.data_dir.join("indexes").join(name);
        effective.storage.memory_limit =
            divided_size(&self.storage.memory_limit, self.max_indexes)?;
        effective.storage.temp_limit = divided_size(&self.storage.temp_limit, self.max_indexes)?;
        effective.storage.queue_bytes /= self.max_indexes;

        if effective.storage.queue_bytes < MIN_QUEUE_BYTES {
            return Err(Error::invalid(
                "storage.queue_bytes must provide at least 1024 bytes per index slot",
            ));
        }

        Ok(effective)
    }

    /// Applies one `[indexes.<name>]` table on top of the process policy.
    fn apply_index_settings(&mut self, settings: &IndexSettings) -> Result<(), Error> {
        if let Some(keys) = &settings.promoted_string_keys {
            self.storage.promoted_string_keys = keys.clone();
        }

        if settings.retention_days.is_some() && settings.retention_ms.is_some() {
            return Err(Error::invalid(
                "index policy must use retention_days or retention_ms, not both",
            ));
        }

        if settings.max_size_gb.is_some() && settings.max_size_bytes.is_some() {
            return Err(Error::invalid(
                "index policy must use max_size_gb or max_size_bytes, not both",
            ));
        }

        if let Some(days) = settings.retention_days {
            self.retention_ms = days_to_millis(days)?;
        }

        if let Some(ms) = settings.retention_ms {
            self.retention_ms = ms;
        }

        if let Some(ms) = settings.archive_after_ms {
            self.archive_after_ms = ms;
        }

        if let Some(gb) = settings.max_size_gb {
            self.storage.max_size_bytes = Some(gb_to_bytes(gb)?);
        }

        if let Some(bytes) = settings.max_size_bytes {
            self.storage.max_size_bytes = Some(bytes);
        }

        Ok(())
    }

    /// Checks the whole configuration. Consumers rely on this instead of re-checking fields.
    ///
    /// # Errors
    ///
    /// Returns the first violated rule, with a message that names the setting or limit.
    pub fn validate(&self) -> Result<(), Error> {
        validate_promoted_string_keys(&self.storage.promoted_string_keys)?;
        self.forward.validate(&self.indexes)?;
        if self.forward.enabled && self.storage.queue_capacity < 2 {
            return Err(Error::invalid(
                "Forward requires storage.queue_capacity >= 2 to reserve HTTP writer admission",
            ));
        }

        self.validate_tokens()?;
        self.validate_http_limits()?;
        self.validate_storage_budgets()?;
        self.validate_index_policies()?;

        if self.maintenance_interval_secs == 0 {
            return Err(Error::invalid("maintenance_interval_secs must be positive"));
        }

        if self.shutdown_timeout_secs == 0 {
            return Err(Error::invalid("shutdown_timeout_secs must be positive"));
        }

        validate_literal_path(&self.storage.data_dir)
    }

    /// Credentials: presence, format, separation between roles, and source names.
    fn validate_tokens(&self) -> Result<(), Error> {
        if self.ingest_tokens.is_empty() || self.read_tokens.is_empty() {
            return Err(Error::invalid(
                "configure at least one ingest token and one read token \
                 (TOML or LOGBROOK_INGEST_TOKEN/LOGBROOK_READ_TOKEN)",
            ));
        }

        let any_malformed_token = self
            .ingest_tokens
            .keys()
            .chain(self.read_tokens.keys())
            .chain(self.admin_tokens.iter())
            .any(|token| !token_is_valid(token));
        if any_malformed_token {
            return Err(Error::invalid(INVALID_TOKEN));
        }

        if self
            .ingest_tokens
            .keys()
            .any(|token| self.read_tokens.contains_key(token))
        {
            return Err(Error::invalid(
                "use separate credentials for ingestion and reading",
            ));
        }

        if self.admin_tokens.iter().any(|token| {
            self.ingest_tokens.contains_key(token) || self.read_tokens.contains_key(token)
        }) {
            return Err(Error::invalid(
                "use separate credentials for administration, ingestion and reading",
            ));
        }

        for source in self
            .ingest_tokens
            .values()
            .chain(self.read_tokens.values().flatten())
        {
            validate_source(source)?;
        }

        Ok(())
    }

    /// Request, body, stream and tail limits, grouped by what they bound. Every violation
    /// reports the same message, which names the budgets the operator should review.
    fn validate_http_limits(&self) -> Result<(), Error> {
        let groups_valid = [
            self.body_and_event_limits_valid(),
            self.memory_limits_valid(),
            self.timeout_limits_valid(),
            self.tail_limits_valid(),
            self.query_limits_valid(),
        ];
        if groups_valid.iter().all(|valid| *valid) {
            Ok(())
        } else {
            Err(Error::invalid(INVALID_HTTP_LIMITS))
        }
    }

    /// The request body and the events, fields and requests that fit in it.
    fn body_and_event_limits_valid(&self) -> bool {
        (1..=MAX_BODY_BYTES_LIMIT).contains(&self.max_body_bytes)
            && (1..=self.max_body_bytes).contains(&self.max_event_bytes)
            && (1..=MAX_EVENTS_LIMIT).contains(&self.max_events)
            && (1..=MAX_METADATA_DEPTH_LIMIT).contains(&self.max_metadata_depth)
            && (1..=self.max_event_bytes).contains(&self.max_field_bytes)
            && (1..=MAX_INFLIGHT_REQUESTS_LIMIT).contains(&self.max_inflight_requests)
    }

    /// The decoded and wire-body budgets. Both must hold at least one event or body, and both
    /// must fit the u32 permit counts.
    fn memory_limits_valid(&self) -> bool {
        self.max_decoded_bytes >= self.max_event_bytes
            && self.max_decoded_bytes <= MAX_PERMIT_BYTES
            && self.max_inflight_body_bytes >= self.max_body_bytes.saturating_mul(2)
            && self.max_inflight_body_bytes <= MAX_PERMIT_BYTES
    }

    /// Body, header and idle timeouts, the clock-skew window, and the tail poll interval.
    fn timeout_limits_valid(&self) -> bool {
        self.body_idle_timeout_ms > 0
            && self.body_total_timeout_ms >= self.body_idle_timeout_ms
            && self.body_total_timeout_ms <= MAX_BODY_TIMEOUT_MS
            && (0..=MAX_FUTURE_SKEW_MS).contains(&self.max_future_skew_ms)
            && (1..=MAX_HEADER_TIMEOUT_MS).contains(&self.header_timeout_ms)
            && self.tail_poll_ms >= MIN_TAIL_POLL_MS
    }

    /// The tail byte budget, which must hold two maximum events plus framing, and the subscriber
    /// limit.
    fn tail_limits_valid(&self) -> bool {
        (MIN_TAIL_BUFFER_BYTES..=MAX_TAIL_BUFFER_BYTES).contains(&self.max_tail_buffer_bytes)
            && self.max_tail_buffer_bytes
                >= self
                    .max_event_bytes
                    .saturating_mul(2)
                    .saturating_add(TAIL_HEADROOM_BYTES)
            && (1..=MAX_TAIL_SUBSCRIBERS_LIMIT).contains(&self.max_tail_subscribers)
    }

    /// Connection count, page size, and the query time range.
    fn query_limits_valid(&self) -> bool {
        (1..=MAX_CONNECTIONS_LIMIT).contains(&self.max_connections)
            && (1..=crate::model::MAX_PAGE_SIZE).contains(&self.max_page_size)
            && self.max_query_range_ms > 0
            && self.max_query_range_ms <= MAX_QUERY_RANGE_LIMIT_MS
    }

    /// Thread counts, queue sizes, query budgets and disk budgets for the storage engine.
    fn validate_storage_budgets(&self) -> Result<(), Error> {
        let groups_valid = [
            self.queue_limits_valid(),
            self.execution_limits_valid(),
            self.size_limits_valid(),
            self.retention_limits_valid(),
        ];
        if groups_valid.iter().all(|valid| *valid) {
            Ok(())
        } else {
            Err(Error::invalid(INVALID_STORAGE_BUDGETS))
        }
    }

    /// Reader threads, the queue, and the byte budget of the queue.
    fn queue_limits_valid(&self) -> bool {
        let storage = &self.storage;
        (1..=MAX_READER_THREADS).contains(&storage.reader_threads)
            && (1..=MAX_QUEUE_CAPACITY).contains(&storage.queue_capacity)
            && (MIN_QUEUE_BYTES..=MAX_PERMIT_BYTES).contains(&storage.queue_bytes)
    }

    /// `DuckDB` threads, query and maintenance timeouts, and the query byte budget.
    fn execution_limits_valid(&self) -> bool {
        let storage = &self.storage;
        (1..=MAX_DUCKDB_THREADS).contains(&storage.duckdb_threads)
            && storage.query_timeout_ms > 0
            && storage.query_timeout_ms <= MAX_QUERY_TIMEOUT_MS
            && (MIN_QUEUE_BYTES..=MAX_QUERY_BYTES_LIMIT).contains(&storage.max_query_bytes)
            && storage.maintenance_timeout_ms > 0
            && storage.maintenance_timeout_ms <= MAX_MAINTENANCE_TIMEOUT_MS
    }

    /// The memory and temporary-storage limits, which must be sizes such as `512MB`.
    fn size_limits_valid(&self) -> bool {
        valid_size(&self.storage.memory_limit) && valid_size(&self.storage.temp_limit)
    }

    /// Archive batch size, archive partition width, and compaction thresholds.
    fn retention_limits_valid(&self) -> bool {
        let storage = &self.storage;
        (1..=MAX_ARCHIVE_BATCH_ROWS).contains(&storage.archive_batch_rows)
            && (1..=MAX_ARCHIVE_PARTITION_MS).contains(&storage.archive_partition_ms)
            && (MIN_COMPACT_MAX_FILES..=MAX_COMPACT_MAX_FILES).contains(&storage.compact_max_files)
            && (MIN_COMPACT_MAX_BYTES..=MAX_COMPACT_MAX_BYTES).contains(&storage.compact_max_bytes)
    }

    /// Policies of the process and of every configured index, the index count, and the scopes.
    fn validate_index_policies(&self) -> Result<(), Error> {
        let mut resolved = self.clone();
        resolved.resolve_policy()?;
        validate_policy(&resolved)?;

        if !(1..=MAX_INDEXES_LIMIT).contains(&self.max_indexes) {
            return Err(Error::invalid(INVALID_INDEX_COUNT));
        }

        let configured_count =
            self.indexes.len() + usize::from(!self.indexes.contains_key(DEFAULT_INDEX_NAME));
        if configured_count > self.max_indexes {
            return Err(Error::invalid(
                "configured indexes, including default, exceed max_indexes",
            ));
        }

        for name in self
            .indexes
            .keys()
            .map(String::as_str)
            .chain(std::iter::once(DEFAULT_INDEX_NAME))
        {
            self.for_index(name)?;
        }

        self.validate_index_scopes()
    }

    /// Index scopes may only name configured tokens and valid index names.
    fn validate_index_scopes(&self) -> Result<(), Error> {
        for (scopes, tokens) in [
            (
                &self.read_index_scopes,
                self.read_tokens.keys().collect::<Vec<_>>(),
            ),
            (
                &self.ingest_index_scopes,
                self.ingest_tokens.keys().collect::<Vec<_>>(),
            ),
        ] {
            for (token, names) in scopes {
                if !tokens.contains(&token) {
                    return Err(Error::invalid(
                        "index scope refers to an unconfigured token",
                    ));
                }

                for name in names {
                    validate_index_name(name)?;
                }
            }
        }

        Ok(())
    }
}

/// A bearer token is 16 to 512 visible ASCII characters and is not the shipped example value.
fn token_is_valid(token: &str) -> bool {
    (MIN_TOKEN_BYTES..=MAX_TOKEN_BYTES).contains(&token.len())
        && token.bytes().all(|byte| byte.is_ascii_graphic())
        && !token.starts_with("replace-with-")
}

/// Checks a source name, which tags every event and scopes every read token.
///
/// # Errors
///
/// Fails unless the name is 1 to 128 ASCII letters, digits, `-`, `_` or `.`.
pub fn validate_source(source: &str) -> Result<(), Error> {
    let length_valid = (1..=MAX_SOURCE_BYTES).contains(&source.len());
    let characters_valid = source
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b));
    if length_valid && characters_valid {
        Ok(())
    } else {
        Err(Error::invalid(
            "source names must contain 1–128 ASCII letters, digits, '-', '_' or '.'",
        ))
    }
}

/// `DuckDB` expands these characters even inside a list of existing file paths.
///
/// # Errors
///
/// Fails when the path is not UTF-8 or contains a `DuckDB` glob character.
pub fn validate_literal_path(path: &Path) -> Result<(), Error> {
    let text = path
        .to_str()
        .ok_or_else(|| Error::invalid("paths must be UTF-8"))?;
    if text.chars().any(|ch| matches!(ch, '*' | '?' | '[' | ']')) {
        return Err(Error::invalid(
            "paths must not contain DuckDB glob characters: *, ?, [, ]",
        ));
    }

    Ok(())
}

fn environment_not_positive(name: &str) -> Error {
    Error::invalid(format!("{name} must be greater than zero"))
}

fn positive_environment_integer<T>(name: &str) -> Result<Option<T>, Error>
where
    T: FromStr + PartialOrd + From<u8>,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let value = match env::var(name) {
        Ok(value) => value,
        Err(VarError::NotPresent) => return Ok(None),
        Err(VarError::NotUnicode(_)) => {
            return Err(Error::invalid(format!("{name} must be a positive integer")));
        }
    };
    if value.starts_with('-') || value == "0" {
        return Err(environment_not_positive(name));
    }

    // ParseIntError says only which way the parse failed, and the message covers both.
    let parsed = value.parse::<T>().map_err(|error| {
        Error::invalid(format!(
            "{name} must be a positive integer within the supported numeric range"
        ))
        .with_source(error)
    })?;
    if parsed <= T::from(0) {
        return Err(environment_not_positive(name));
    }

    Ok(Some(parsed))
}

fn valid_size(value: &str) -> bool {
    SIZE_UNITS.iter().any(|unit| {
        value
            .strip_suffix(unit.suffix)
            .and_then(|number| number.parse::<u64>().ok())
            .is_some_and(|number| number > 0 && number <= MAX_SIZE_NUMBER)
    })
}

/// Index names become directory names and metric labels, so they are a short, lowercase subset.
///
/// # Errors
///
/// Fails unless the name is 1 to 63 lowercase ASCII letters, digits, `-` or `_`, starting with a
/// letter or digit.
pub fn validate_index_name(name: &str) -> Result<(), Error> {
    let length_valid = (1..=MAX_INDEX_NAME_BYTES).contains(&name.len());
    let starts_valid = name
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric);
    let characters_valid = name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-_".contains(&b));
    if length_valid && starts_valid && characters_valid {
        Ok(())
    } else {
        Err(Error::invalid(
            "index names must contain 1–63 lowercase ASCII letters, digits, '-' or '_', \
             starting with a letter or digit",
        ))
    }
}

fn days_to_millis(days: u64) -> Result<i64, Error> {
    i64::try_from(days)
        .ok()
        .and_then(|days| days.checked_mul(DAY_MS))
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            Error::invalid(
                "retention_days must be a positive integer within the supported millisecond range",
            )
        })
}

fn gb_to_bytes(gb: u64) -> Result<u64, Error> {
    gb.checked_mul(BYTES_PER_GB)
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            Error::invalid("max_size_gb must be a positive integer within the supported byte range")
        })
}

fn validate_policy(config: &Config) -> Result<(), Error> {
    if config.archive_after_ms <= 0 {
        return Err(Error::invalid("archive_after_ms must be positive"));
    }

    if config.retention_ms <= 0 {
        return Err(Error::invalid("retention_ms must be positive"));
    }

    if config.retention_ms <= config.archive_after_ms {
        return Err(Error::invalid(
            "retention_ms must be greater than archive_after_ms",
        ));
    }

    if config
        .storage
        .max_size_bytes
        .is_some_and(|bytes| bytes < MIN_MAX_SIZE_BYTES)
    {
        return Err(Error::invalid(
            "max_size_bytes must be at least 16 MiB (16777216 bytes)",
        ));
    }

    Ok(())
}

/// Divides a process-wide storage budget such as `512MB` into one index's share, as `KB`.
fn divided_size(value: &str, slots: usize) -> Result<String, Error> {
    let bytes = SIZE_UNITS
        .into_iter()
        .find_map(|unit| {
            value
                .strip_suffix(unit.suffix)
                .and_then(|number| number.parse::<u64>().ok())
                .and_then(|number| number.checked_mul(unit.bytes))
        })
        .ok_or_else(|| {
            Error::invalid("storage budgets must be positive integer KB, MB, or GB values")
        })?;
    // The index count is validated to 1..=64 before any share is computed, so this never fails.
    let slots = u64::try_from(slots)
        .map_err(|error| Error::invalid(INVALID_INDEX_COUNT).with_source(error))?;
    let kb = bytes / slots / BYTES_PER_KB;
    if kb == 0 {
        return Err(Error::invalid(
            "storage budgets must provide at least 1KB per index slot",
        ));
    }

    Ok(format!("{kb}KB"))
}

/// Validate literal keys before touching durable storage or admitting a worker.
pub(crate) fn validate_promoted_string_keys(keys: &[String]) -> Result<(), Error> {
    if keys.len() > MAX_PROMOTED_STRING_KEYS {
        return Err(Error::invalid(
            "promoted_string_keys supports at most eight keys",
        ));
    }

    let mut unique = HashSet::new();
    for key in keys {
        if key.is_empty() || key.len() > MAX_ATTRIBUTE_KEY_BYTES {
            return Err(Error::invalid(
                "promoted_string_keys keys must contain 1–256 UTF-8 bytes",
            ));
        }

        if !unique.insert(key) {
            return Err(Error::invalid(
                "promoted_string_keys must not contain duplicate keys",
            ));
        }
    }

    Ok(())
}
