use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::model::Error;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    pub data_dir: PathBuf,

    pub reader_threads: usize,
    pub queue_capacity: usize,
    pub queue_bytes: usize,

    pub memory_limit: String,
    pub temp_limit: String,
    pub duckdb_threads: usize,

    pub query_timeout_ms: u64,
    pub max_query_bytes: usize,

    pub archive_batch_rows: usize,
    pub archive_partition_ms: i64,
    pub compact_max_files: usize,
    pub compact_max_bytes: u64,
    pub maintenance_timeout_ms: u64,

    /// Per-index measured disk footprint target; None disables size retention.
    pub max_size_bytes: Option<u64>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),

            reader_threads: 2,
            queue_capacity: 16,
            queue_bytes: 32 * 1024 * 1024,

            memory_limit: "512MB".into(),
            temp_limit: "2GB".into(),
            duckdb_threads: 2,

            query_timeout_ms: 5_000,
            max_query_bytes: 8 * 1024 * 1024,

            archive_batch_rows: 100_000,
            archive_partition_ms: 3_600_000,
            compact_max_files: 16,
            compact_max_bytes: 128 * 1024 * 1024,
            maintenance_timeout_ms: 30_000,

            max_size_bytes: None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct IndexSettings {
    pub retention_days: Option<u64>,
    pub retention_ms: Option<i64>,
    pub archive_after_ms: Option<i64>,
    pub max_size_gb: Option<u64>,
    pub max_size_bytes: Option<u64>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub bind: SocketAddr,
    pub storage: StorageConfig,

    pub retention_days: Option<u64>,
    pub max_size_gb: Option<u64>,
    pub max_indexes: usize,
    pub indexes: HashMap<String, IndexSettings>,

    pub read_index_scopes: HashMap<String, Vec<String>>,
    pub ingest_index_scopes: HashMap<String, Vec<String>>,
    pub ingest_tokens: HashMap<String, String>,
    pub read_tokens: HashMap<String, Vec<String>>,
    pub admin_tokens: Vec<String>,

    pub max_body_bytes: usize,
    pub max_event_bytes: usize,
    pub max_events: usize,
    pub max_metadata_depth: usize,
    pub max_field_bytes: usize,

    pub max_inflight_requests: usize,
    pub max_decoded_bytes: usize,
    pub max_inflight_body_bytes: usize,

    pub body_idle_timeout_ms: u64,
    pub body_total_timeout_ms: u64,
    pub max_future_skew_ms: i64,
    pub max_tail_buffer_bytes: usize,
    pub max_connections: usize,
    pub header_timeout_ms: u64,

    pub max_query_range_ms: i64,
    pub max_page_size: usize,
    pub max_tail_subscribers: usize,
    pub tail_poll_ms: u64,

    pub archive_after_ms: i64,
    pub retention_ms: i64,
    pub maintenance_interval_secs: u64,
    pub shutdown_timeout_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:3100".parse().expect("constant bind address"),
            storage: StorageConfig::default(),
            retention_days: None,
            max_size_gb: None,
            max_indexes: 4,
            indexes: HashMap::new(),
            read_index_scopes: HashMap::new(),
            ingest_index_scopes: HashMap::new(),
            ingest_tokens: HashMap::new(),
            read_tokens: HashMap::new(),
            admin_tokens: Vec::new(),
            max_body_bytes: 1024 * 1024,
            max_event_bytes: 64 * 1024,
            max_events: 1_000,
            max_metadata_depth: 16,
            max_field_bytes: 4096,
            max_inflight_requests: 16,
            max_decoded_bytes: 64 * 1024 * 1024,
            max_inflight_body_bytes: 16 * 1024 * 1024,
            body_idle_timeout_ms: 5_000,
            body_total_timeout_ms: 30_000,
            max_future_skew_ms: 300_000,
            max_tail_buffer_bytes: 1024 * 1024,
            max_connections: 128,
            header_timeout_ms: 10_000,
            max_query_range_ms: 31 * 86_400_000,
            max_page_size: crate::model::MAX_PAGE_SIZE,
            max_tail_subscribers: 8,
            tail_poll_ms: 1_000,
            archive_after_ms: 86_400_000,
            retention_ms: 7 * 86_400_000,
            maintenance_interval_secs: 60,
            shutdown_timeout_secs: 30,
        }
    }
}

fn read_config<T: DeserializeOwned>(path: &Path) -> Result<T, Error> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| Error::invalid(format!("cannot read config: {error}")))?;

    toml::from_str(&text).map_err(|error| {
        // Parser messages and source excerpts can contain credential keys or values.
        // Retain only the location, never the parser's Display or message output.
        let location = error.span().map(|span| {
            text.char_indices()
                .take_while(|(offset, _)| *offset < span.start)
                .fold((1, 1), |(line, column), (_, character)| {
                    if character == '\n' {
                        (line + 1, 1)
                    } else {
                        (line, column + 1)
                    }
                })
        });

        let message = match location {
            Some((line, column)) => format!("invalid TOML config at line {line}, column {column}"),
            None => "invalid TOML config".into(),
        };

        Error::invalid(format!(
            "{message}; check syntax, setting names, and value types"
        ))
    })
}

impl Config {
    /// Read only the endpoint needed by the unauthenticated readiness probe.
    /// Health checks do not need producer/read tokens or storage configuration.
    pub fn healthcheck_address(path: Option<&Path>) -> Result<SocketAddr, Error> {
        #[derive(Deserialize)]
        struct Endpoint {
            #[serde(default = "default_bind")]
            bind: SocketAddr,
        }

        fn default_bind() -> SocketAddr {
            Config::default().bind
        }

        let mut address = match path {
            Some(path) => read_config::<Endpoint>(path)?.bind,

            None => default_bind(),
        };

        if let Ok(bind) = std::env::var("LOGBROOK_BIND") {
            address = bind
                .parse()
                .map_err(|_| Error::invalid("LOGBROOK_BIND must be an IP:port"))?;
        }

        Ok(address)
    }

    /// Defaults < optional TOML file < explicitly supported environment variables.
    pub fn load(path: Option<&Path>) -> Result<Self, Error> {
        let mut config: Self = match path {
            Some(path) => read_config(path)?,
            None => Self::default(),
        };

        if let Ok(bind) = std::env::var("LOGBROOK_BIND") {
            config.bind = bind
                .parse()
                .map_err(|_| Error::invalid("LOGBROOK_BIND must be an IP:port"))?;
        }

        if let Some(path) = std::env::var_os("LOGBROOK_DATA_DIR") {
            config.storage.data_dir = path.into();
        }

        if let Ok(token) = std::env::var("LOGBROOK_INGEST_TOKEN") {
            config.ingest_tokens.clear();
            config.ingest_tokens.insert(
                token,
                std::env::var("LOGBROOK_SOURCE").unwrap_or_else(|_| "default".into()),
            );
        }

        if let Ok(token) = std::env::var("LOGBROOK_READ_TOKEN") {
            let sources = match std::env::var("LOGBROOK_READ_SOURCES") {
                Ok(value) if value.is_empty() => Vec::new(),
                Ok(value) => value.split(',').map(str::to_owned).collect(),
                Err(_) if config.read_tokens.is_empty() => Vec::new(),
                Err(_) => config.read_tokens.get(&token).cloned().ok_or_else(|| {
                        Error::invalid(
                            "LOGBROOK_READ_SOURCES must explicitly set the scope when adding \
                             an environment token to configured read tokens (empty grants all sources)",
                        )
                })?,
            };
            config.read_tokens.insert(token, sources);
        }

        if let Ok(token) = std::env::var("LOGBROOK_ADMIN_TOKEN") {
            config.admin_tokens = vec![token];
        }

        let days = positive_environment_integer::<u64>("LOGBROOK_RETENTION_DAYS")?;
        let millis = positive_environment_integer::<i64>("LOGBROOK_RETENTION_MS")?;

        if days.is_some() && millis.is_some() {
            return Err(Error::invalid(
                "set only one of LOGBROOK_RETENTION_DAYS and LOGBROOK_RETENTION_MS",
            ));
        }

        if let Some(value) = days {
            config.retention_days = Some(value);
        }

        if let Some(value) = millis {
            config.retention_days = None;
            config.retention_ms = value;
        }

        let gigabytes = positive_environment_integer::<u64>("LOGBROOK_MAX_SIZE_GB")?;
        let bytes = positive_environment_integer::<u64>("LOGBROOK_MAX_SIZE_BYTES")?;

        if gigabytes.is_some() && bytes.is_some() {
            return Err(Error::invalid(
                "set only one of LOGBROOK_MAX_SIZE_GB and LOGBROOK_MAX_SIZE_BYTES",
            ));
        }

        if let Some(value) = gigabytes {
            config.max_size_gb = Some(value);
        }

        if let Some(value) = bytes {
            config.max_size_gb = None;
            config.storage.max_size_bytes = Some(value);
        }

        if let Some(value) = positive_environment_integer::<usize>("LOGBROOK_MAX_INDEXES")? {
            config.max_indexes = value;
        }

        if let Ok(names) = std::env::var("LOGBROOK_INDEXES") {
            for name in names.split(',').filter(|name| !name.is_empty()) {
                validate_index_name(name)?;
                config.indexes.entry(name.to_owned()).or_default();
            }
        }

        if let Some(value) = positive_environment_integer::<i64>("LOGBROOK_ARCHIVE_AFTER_MS")? {
            config.archive_after_ms = value;
        }

        if let Some(value) =
            positive_environment_integer::<u64>("LOGBROOK_MAINTENANCE_INTERVAL_SECS")?
        {
            config.maintenance_interval_secs = value;
        }

        config.resolve_policy()?;
        config.validate()?;

        Ok(config)
    }

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
    pub fn for_index(&self, name: &str) -> Result<Self, Error> {
        validate_index_name(name)?;

        if !(1..=64).contains(&self.max_indexes) {
            return Err(Error::invalid("max_indexes must be between 1 and 64"));
        }

        let mut effective = self.clone();
        effective.resolve_policy()?;

        if let Some(settings) = self.indexes.get(name) {
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
                effective.retention_ms = days_to_millis(days)?;
            }

            if let Some(ms) = settings.retention_ms {
                effective.retention_ms = ms;
            }

            if let Some(ms) = settings.archive_after_ms {
                effective.archive_after_ms = ms;
            }

            if let Some(gb) = settings.max_size_gb {
                effective.storage.max_size_bytes = Some(gb_to_bytes(gb)?);
            }

            if let Some(bytes) = settings.max_size_bytes {
                effective.storage.max_size_bytes = Some(bytes);
            }
        }

        // Aliases are already resolved; consumers use the effective canonical fields.
        effective.retention_days = None;
        effective.max_size_gb = None;
        validate_policy(&effective)?;
        effective.storage.data_dir = self.storage.data_dir.join("indexes").join(name);
        effective.storage.memory_limit =
            divided_size(&self.storage.memory_limit, self.max_indexes)?;
        effective.storage.temp_limit = divided_size(&self.storage.temp_limit, self.max_indexes)?;
        effective.storage.queue_bytes /= self.max_indexes;

        if effective.storage.queue_bytes < 1024 {
            return Err(Error::invalid(
                "storage.queue_bytes must provide at least 1024 bytes per index slot",
            ));
        }

        Ok(effective)
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.ingest_tokens.is_empty() || self.read_tokens.is_empty() {
            return Err(Error::invalid(
                "configure at least one ingest token and one read token \
                 (TOML or LOGBROOK_INGEST_TOKEN/LOGBROOK_READ_TOKEN)",
            ));
        }

        for token in self
            .ingest_tokens
            .keys()
            .chain(self.read_tokens.keys())
            .chain(self.admin_tokens.iter())
        {
            if token.len() < 16
                || token.len() > 512
                || !token.bytes().all(|byte| byte.is_ascii_graphic())
                || token.starts_with("replace-with-")
            {
                return Err(Error::invalid(
                    "tokens must contain 16–512 visible ASCII characters without whitespace; replace example credentials",
                ));
            }
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

        if self.max_body_bytes == 0
            || self.max_body_bytes > 16 * 1024 * 1024
            || self.max_event_bytes == 0
            || self.max_event_bytes > self.max_body_bytes
            || self.max_events == 0
            || self.max_events > 10_000
            || self.max_metadata_depth == 0
            || self.max_metadata_depth > 64
            || self.max_field_bytes == 0
            || self.max_field_bytes > self.max_event_bytes
            || self.max_inflight_requests == 0
            || self.max_inflight_requests > 1024
            || self.max_decoded_bytes < self.max_event_bytes
            || self.max_decoded_bytes > u32::MAX as usize
            || self.max_inflight_body_bytes < self.max_body_bytes.saturating_mul(2)
            || self.max_inflight_body_bytes > u32::MAX as usize
            || self.body_idle_timeout_ms == 0
            || self.body_total_timeout_ms < self.body_idle_timeout_ms
            || self.body_total_timeout_ms > 300_000
            || !(0..=86_400_000).contains(&self.max_future_skew_ms)
            || !(1024..=64 * 1024 * 1024).contains(&self.max_tail_buffer_bytes)
            || self.max_tail_buffer_bytes
                < self.max_event_bytes.saturating_mul(2).saturating_add(1024)
            || !(1..=65_536).contains(&self.max_connections)
            || !(1..=300_000).contains(&self.header_timeout_ms)
            || !(1..=crate::model::MAX_PAGE_SIZE).contains(&self.max_page_size)
            || self.max_query_range_ms <= 0
            || self.max_query_range_ms > 366 * 86_400_000
            || self.max_tail_subscribers == 0
            || self.max_tail_subscribers > 1024
            || self.tail_poll_ms < 250
        {
            return Err(Error::invalid(
                "invalid HTTP limits: check body/event/count/depth/concurrency/decoded memory/query/tail budgets",
            ));
        }

        if !(1..=16).contains(&self.storage.reader_threads)
            || !(1..=1024).contains(&self.storage.queue_capacity)
            || !(1024..=u32::MAX as usize).contains(&self.storage.queue_bytes)
            || !(1..=64).contains(&self.storage.duckdb_threads)
            || self.storage.query_timeout_ms == 0
            || self.storage.query_timeout_ms > 300_000
            || !(1024..=64 * 1024 * 1024).contains(&self.storage.max_query_bytes)
            || !valid_size(&self.storage.memory_limit)
            || !valid_size(&self.storage.temp_limit)
            || !(1..=1_000_000).contains(&self.storage.archive_batch_rows)
            || !(1..=86_400_000).contains(&self.storage.archive_partition_ms)
            || !(2..=256).contains(&self.storage.compact_max_files)
            || !(1024..=8 * 1024 * 1024 * 1024).contains(&self.storage.compact_max_bytes)
            || !(1..=300_000).contains(&self.storage.maintenance_timeout_ms)
        {
            return Err(Error::invalid(
                "invalid storage budgets; sizes must be positive integer KB, MB, or GB values",
            ));
        }

        let mut resolved = self.clone();
        resolved.resolve_policy()?;
        validate_policy(&resolved)?;

        if !(1..=64).contains(&self.max_indexes) {
            return Err(Error::invalid("max_indexes must be between 1 and 64"));
        }

        let configured_count =
            self.indexes.len() + usize::from(!self.indexes.contains_key("default"));

        if configured_count > self.max_indexes {
            return Err(Error::invalid(
                "configured indexes, including default, exceed max_indexes",
            ));
        }

        for name in self
            .indexes
            .keys()
            .map(String::as_str)
            .chain(std::iter::once("default"))
        {
            self.for_index(name)?;
        }

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

        if self.maintenance_interval_secs == 0 {
            return Err(Error::invalid("maintenance_interval_secs must be positive"));
        }

        if self.shutdown_timeout_secs == 0 {
            return Err(Error::invalid("shutdown_timeout_secs must be positive"));
        }

        validate_literal_path(&self.storage.data_dir)?;

        Ok(())
    }
}

pub fn validate_source(source: &str) -> Result<(), Error> {
    if source.is_empty()
        || source.len() > 128
        || !source
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    {
        return Err(Error::invalid(
            "source names must contain 1–128 ASCII letters, digits, '-', '_' or '.'",
        ));
    }

    Ok(())
}

/// DuckDB expands these characters even inside a list of existing file paths.
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

fn positive_environment_integer<T>(name: &str) -> Result<Option<T>, Error>
where
    T: std::str::FromStr + PartialOrd + From<u8>,
{
    let value = match std::env::var(name) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(Error::invalid(format!("{name} must be a positive integer")));
        }
    };

    if value.starts_with('-') || value == "0" {
        return Err(Error::invalid(format!("{name} must be greater than zero")));
    }

    let parsed = value.parse::<T>().map_err(|_| {
        Error::invalid(format!(
            "{name} must be a positive integer within the supported numeric range"
        ))
    })?;

    if parsed <= T::from(0) {
        return Err(Error::invalid(format!("{name} must be greater than zero")));
    }

    Ok(Some(parsed))
}

fn valid_size(value: &str) -> bool {
    ["KB", "MB", "GB"].into_iter().any(|unit| {
        value
            .strip_suffix(unit)
            .and_then(|number| number.parse::<u64>().ok())
            .is_some_and(|number| number > 0 && number <= 1_000_000)
    })
}

pub fn validate_index_name(name: &str) -> Result<(), Error> {
    if name.is_empty()
        || name.len() > 63
        || !name.as_bytes()[0].is_ascii_alphanumeric()
        || !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-_".contains(&b))
    {
        return Err(Error::invalid(
            "index names must contain 1–63 lowercase ASCII letters, digits, '-' or '_', \
             starting with a letter or digit",
        ));
    }

    Ok(())
}

fn days_to_millis(days: u64) -> Result<i64, Error> {
    days.checked_mul(86_400_000)
        .filter(|value| *value > 0)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| {
            Error::invalid(
                "retention_days must be a positive integer within the supported millisecond range",
            )
        })
}

fn gb_to_bytes(gb: u64) -> Result<u64, Error> {
    gb.checked_mul(1_000_000_000)
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
        .is_some_and(|bytes| bytes < 16 * 1024 * 1024)
    {
        return Err(Error::invalid(
            "max_size_bytes must be at least 16 MiB (16777216 bytes)",
        ));
    }

    Ok(())
}

fn divided_size(value: &str, slots: usize) -> Result<String, Error> {
    let bytes = [("KB", 1_000_u64), ("MB", 1_000_000), ("GB", 1_000_000_000)]
        .into_iter()
        .find_map(|(unit, multiplier)| {
            value
                .strip_suffix(unit)
                .and_then(|number| number.parse::<u64>().ok())
                .and_then(|number| number.checked_mul(multiplier))
        })
        .ok_or_else(|| {
            Error::invalid("storage budgets must be positive integer KB, MB, or GB values")
        })?;
    let kb = bytes / slots as u64 / 1_000;

    if kb == 0 {
        return Err(Error::invalid(
            "storage budgets must provide at least 1KB per index slot",
        ));
    }

    Ok(format!("{kb}KB"))
}
