//! Frozen reference dimensions and bounded workload limits.
use std::time::Duration;

/// Scenario name recorded in every attribute workload report.
pub const SCENARIO: &str = "attribute-reference-v2";

/// Server configuration of the legacy-compatible fixture, one `key=value` setting per line.
pub const SERVER_CONFIG: &str = concat!(
    "max_indexes=1\n",
    "archive_after_ms=172800000\n",
    "retention_ms=604800000\n",
    "maintenance_interval_secs=86400\n",
    "[storage]\n",
    "reader_threads=2\n",
    "duckdb_threads=2\n",
    "memory_limit='512MB'\n",
    "temp_limit='2GB'\n",
    "query_timeout_ms=5000\n",
    "archive_batch_rows=100000\n",
);

/// Seed of the generated fixture, recorded in the report.
pub(super) const FIXTURE_SEED: u64 = 123;

/// Rows in the reference fixture, and the default `--rows`.
pub(in crate::load) const REFERENCE_ROWS: u64 = 1_000_000;

/// Runs in the reference fixture, and the default `--runs`.
pub(in crate::load) const REFERENCE_RUNS: u64 = 1000;

/// Measured requests per cell in the reference run, and the default `--samples`.
pub(super) const REFERENCE_SAMPLES: u64 = 1000;

/// Warmup requests per cell in the reference run, and the default `--warmup`.
pub(super) const REFERENCE_WARMUP: u64 = 100;

/// Metadata bytes per event in the reference fixture, and the default `--metadata-bytes`.
pub(in crate::load) const REFERENCE_METADATA_BYTES: usize = 1024;

/// Percentage of the fixture window archived in the reference run.
pub(in crate::load) const REFERENCE_ARCHIVE_PERCENT: u64 = 90;

/// Measured live seconds in the reference run, and the default `--duration`.
pub(in crate::load) const REFERENCE_DURATION_SECONDS: f64 = 600.0;

/// Offered live ingest events per second in the reference run, and the default `--rate`.
pub(in crate::load) const REFERENCE_RATE: f64 = 1000.0;

/// Offered live queries per second in the reference run, and the default `--query-rate`.
pub(in crate::load) const REFERENCE_QUERY_RATE: f64 = 2.0;

/// Unmeasured live seconds before the reference measurement, and the default
/// `--live-warmup-seconds`.
pub(in crate::load) const REFERENCE_LIVE_WARMUP_SECONDS: f64 = 60.0;

/// Paired repetitions in the reference run, and the default `--paired-repetitions`.
pub(in crate::load) const REFERENCE_PAIRED_REPETITIONS: usize = 3;

/// Largest accepted `--rows`; also the scale at which a deadline response counts as an
/// envelope measurement.
pub(super) const MAX_ROWS: u64 = 10_000_000;

/// Largest accepted `--runs`.
pub(super) const MAX_RUNS: u64 = 1000;

/// Largest accepted `--samples`.
pub(super) const MAX_SAMPLES: u64 = 10_000;

/// Largest accepted `--warmup`.
pub(super) const MAX_WARMUP: u64 = 10_000;

/// Smallest accepted `--metadata-bytes`.
pub(super) const MIN_METADATA_BYTES: usize = 256;

/// Largest accepted `--metadata-bytes`.
pub(super) const MAX_METADATA_BYTES: usize = 4096;

/// Largest accepted `--archive-percent`.
pub(super) const MAX_ARCHIVE_PERCENT: u64 = 100;

/// Smallest accepted `--duration`, in seconds.
pub(super) const MIN_DURATION_SECONDS: f64 = 0.1;

/// Largest accepted `--duration`, in seconds.
pub(super) const MAX_DURATION_SECONDS: f64 = 3600.0;

/// Smallest accepted `--rate`, in events per second.
pub(super) const MIN_RATE: f64 = 1.0;

/// Largest accepted `--rate`, in events per second.
pub(super) const MAX_RATE: f64 = 10_000.0;

/// Smallest accepted `--query-rate`, in queries per second.
pub(super) const MIN_QUERY_RATE: f64 = 0.1;

/// Largest accepted `--query-rate`, in queries per second.
pub(super) const MAX_QUERY_RATE: f64 = 100.0;

/// Largest accepted `--live-warmup-seconds`.
pub(super) const MAX_LIVE_WARMUP_SECONDS: f64 = 600.0;

/// Largest accepted `--paired-repetitions`.
pub(super) const MAX_PAIRED_REPETITIONS: usize = 3;

/// Largest number of promoted string keys.
pub(super) const MAX_PROMOTED_STRING_KEYS: usize = 8;

/// Longest promoted string key, in bytes.
pub(super) const MAX_PROMOTED_KEY_BYTES: usize = 256;

/// Distinct actors per run in the fixture.
pub(in crate::load) const ACTORS_PER_RUN: u64 = 10;

/// Distinct videos per actor in the fixture.
pub(super) const VIDEOS_PER_ACTOR: u64 = 10;

/// Distinct hosts the fixture cycles through.
pub(super) const HOST_COUNT: u64 = 8;

/// Log level of every tenth video's events, which are the fixture's errors.
pub(super) const ERROR_LEVEL: u64 = 50;

/// Log level of all other fixture events.
pub(super) const INFO_LEVEL: u64 = 30;

/// Stride of the ingestion-order permutation over event ordinals.
pub(super) const INGEST_ORDER_STRIDE: u64 = 997;

/// Offset of the ingestion-order permutation over event ordinals.
pub(super) const INGEST_ORDER_OFFSET: u64 = 123;

/// Oracle entries are trimmed to their newest events once they exceed this many.
pub(super) const ORACLE_TRIM_THRESHOLD: usize = 200;

/// Events kept per oracle entry after trimming.
pub(super) const ORACLE_RETAINED: usize = 100;

/// Width of one histogram bucket, in milliseconds.
pub(super) const HISTOGRAM_INTERVAL_MS: i64 = 60_000;

/// Page size of the search requests.
pub(super) const SEARCH_PAGE_LIMIT: u64 = 100;

/// Milliseconds the fixture window starts before the run's clock, past the one-day window.
pub(super) const FIXTURE_LEAD_MS: i64 = 1000;

/// Facet dimensions every facet response reports.
pub(super) const FACET_DIMENSIONS: [&str; 5] =
    ["services", "loggers", "hosts", "levels", "sources"];

/// Attribute conditions of the maximum-shape filter, in the order they are sent.
pub(super) const MAXIMUM_FILTER_KEYS: [&str; 16] = [
    "runId",
    "actorJobId",
    "videoId",
    "sequence",
    "active",
    "tag0",
    "tag1",
    "tag2",
    "tag3",
    "tag4",
    "tag5",
    "tag6",
    "tag7",
    "tag8",
    "tag9",
    "tag10",
];

/// Most late cursor pages the probe will fetch before it reports a failure.
pub(super) const MAX_LATE_PAGES: u64 = 10_000;

/// Longest error text kept per query sample, in characters.
pub(super) const QUERY_ERROR_CHARS: usize = 500;

/// Failure examples kept per cell report.
pub(super) const FAILURE_EXAMPLE_LIMIT: usize = 20;

/// Longest a timeout-recovery probe waits for readers to go idle.
pub(super) const RECOVERY_DEADLINE: Duration = Duration::from_secs(3);

/// Pause between reader-idle polls during timeout recovery.
pub(super) const RECOVERY_POLL: Duration = Duration::from_millis(50);

/// Slowest deadline response, in milliseconds, that still counts as a bounded envelope limit.
pub(super) const MAX_BOUNDED_DEADLINE_MS: f64 = 7000.0;

/// HTTP status of a server-side query deadline.
pub(super) const GATEWAY_TIMEOUT_STATUS: u16 = 504;

/// Largest accepted `--hardware-provenance` file, in bytes.
pub(super) const MAX_HARDWARE_PROVENANCE_BYTES: u64 = 65_536;

/// Docker format string that prints a container's host configuration as one JSON document.
pub(in crate::load) const HOST_CONFIG_FORMAT: &str = "{{json .HostConfig}}";

/// Timeout of each host probe that records provenance, in seconds.
pub(in crate::load) const PROBE_TIMEOUT_SECS: u64 = 5;

/// Timeout of each offline import, archive and projection command, in seconds.
pub(in crate::load) const OFFLINE_COMMAND_SECS: u64 = 3600;

/// Target container CPUs.
pub(in crate::load) const CONTAINER_CPUS: usize = 4;

/// Target container memory limit, in MiB.
pub(in crate::load) const CONTAINER_MEMORY_MIB: usize = 1024;

/// Target container CPU quota, in nano-CPUs as Docker reports it.
pub(in crate::load) const CONTAINER_NANO_CPUS: u64 = 4_000_000_000;

/// Target container memory limit, in bytes as Docker reports it.
pub(in crate::load) const CONTAINER_MEMORY_BYTES: u64 = 1_073_741_824;

/// Minimum median committed-throughput ratio of the comparison to the baseline.
pub(super) const MIN_THROUGHPUT_RATIO: f64 = 0.8;

/// Reference p95 limit of search cells, in milliseconds.
pub(super) const SEARCH_P95_LIMIT_MS: f64 = 500.0;

/// Reference p95 limit of count, histogram and facet cells, in milliseconds.
pub(super) const OTHER_P95_LIMIT_MS: f64 = 2000.0;

/// Offered dedicated tail-profile ingest rate, in events per second.
pub(in crate::load) const DEDICATED_TAIL_RATE: f64 = 100.0;

/// p95 limit of the dedicated tail measurement, in milliseconds.
pub(super) const TAIL_P95_LIMIT_MS: f64 = 1500.0;

/// Durable projection layout version that the projection status must report.
pub(super) const PROJECTION_SCHEMA_VERSION: u64 = 4;

/// Message of the report entry that holds the completed runs.
pub(in crate::load) const REPORT_RUNS: &str = "report runs array";
