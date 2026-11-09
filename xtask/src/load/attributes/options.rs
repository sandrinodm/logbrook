//! Attribute workload command-line options and validation.
use super::constants::{
    MAX_ARCHIVE_PERCENT, MAX_DURATION_SECONDS, MAX_LIVE_WARMUP_SECONDS, MAX_METADATA_BYTES,
    MAX_PAIRED_REPETITIONS, MAX_PROMOTED_KEY_BYTES, MAX_PROMOTED_STRING_KEYS, MAX_QUERY_RATE,
    MAX_RATE, MAX_ROWS, MAX_RUNS, MAX_SAMPLES, MAX_WARMUP, MIN_DURATION_SECONDS,
    MIN_METADATA_BYTES, MIN_QUERY_RATE, MIN_RATE, REFERENCE_ARCHIVE_PERCENT,
    REFERENCE_DURATION_SECONDS, REFERENCE_LIVE_WARMUP_SECONDS, REFERENCE_METADATA_BYTES,
    REFERENCE_PAIRED_REPETITIONS, REFERENCE_QUERY_RATE, REFERENCE_RATE, REFERENCE_ROWS,
    REFERENCE_RUNS, REFERENCE_SAMPLES, REFERENCE_WARMUP, SERVER_CONFIG,
};
use crate::Result;
use std::{collections::BTreeSet, path::PathBuf};

// Command-line options of the attribute workload.
#[derive(clap::Args, Clone, Debug, serde::Serialize)]
pub struct Args {
    // Binary under test, unless --image selects a container image.
    #[arg(
        long,
        default_value = "target/release/logbrook",
        conflicts_with = "image"
    )]
    pub binary: PathBuf,
    // Container image under test.
    #[arg(long)]
    pub image: Option<String>,
    /// JSON evidence of externally verified host CPU, memory, and local SSD storage.
    #[arg(long)]
    pub hardware_provenance: Option<PathBuf>,
    // Rows in the fixture.
    #[arg(long, default_value_t = REFERENCE_ROWS)]
    pub rows: u64,
    // Runs in the fixture.
    #[arg(long, default_value_t = REFERENCE_RUNS)]
    pub runs: u64,
    // Measured requests per cell.
    #[arg(long, default_value_t = REFERENCE_SAMPLES)]
    pub samples: u64,
    // Warmup requests per cell.
    #[arg(long, default_value_t = REFERENCE_WARMUP)]
    pub warmup: u64,
    /// Collect optional whole-container CPU deltas around ten-request cell batches.
    #[arg(long)]
    pub cpu_diagnostics: bool,
    /// Promote a top-level string key in the durable fixture layout (repeatable, maximum eight).
    #[arg(long = "promoted-string-key")]
    pub promoted_string_keys: Vec<String>,
    // Metadata bytes per event.
    #[arg(long, default_value_t = REFERENCE_METADATA_BYTES)]
    pub metadata_bytes: usize,
    // Percentage of the fixture window to archive.
    #[arg(long, default_value_t = REFERENCE_ARCHIVE_PERCENT)]
    pub archive_percent: u64,
    // Which measurements to run: queries, tail, paired or all.
    #[arg(long, default_value="queries", value_parser=["queries","tail","paired","all"])]
    pub profile: String,
    // Measured live seconds.
    #[arg(long, default_value_t = REFERENCE_DURATION_SECONDS)]
    pub duration: f64,
    // Offered live ingest events per second.
    #[arg(long, default_value_t = REFERENCE_RATE)]
    pub rate: f64,
    // Offered live queries per second.
    #[arg(long, default_value_t = REFERENCE_QUERY_RATE)]
    pub query_rate: f64,
    // Unmeasured live seconds before measurement.
    #[arg(long, default_value_t = REFERENCE_LIVE_WARMUP_SECONDS)]
    pub live_warmup_seconds: f64,
    // Paired repetitions of the baseline and comparison builds.
    #[arg(long, default_value_t = REFERENCE_PAIRED_REPETITIONS)]
    pub paired_repetitions: usize,
    // Report path.
    #[arg(long, default_value = "artifacts/attributes.json")]
    pub output: PathBuf,
}

impl Args {
    /// Checks every numeric bound and the promoted-key rules before any fixture starts.
    ///
    /// # Errors
    ///
    /// Returns an error when a numeric argument is outside its supported range, or when the
    /// promoted string keys are duplicated, empty or too long.
    pub(in crate::load) fn validate(&self) -> Result<()> {
        if !self.workload_within_bounds() {
            return Err("attribute workload arguments exceed bounded supported limits".into());
        }
        if !self.promoted_keys_within_bounds() {
            return Err(concat!(
                "promoted string keys require at most eight distinct nonempty keys ",
                "of at most 256 bytes"
            )
            .into());
        }
        Ok(())
    }

    /// Whether every numeric argument is inside its supported range.
    fn workload_within_bounds(&self) -> bool {
        let counts = (1..=MAX_ROWS).contains(&self.rows)
            && (1..=MAX_RUNS).contains(&self.runs)
            && self.runs <= self.rows
            && (1..=MAX_SAMPLES).contains(&self.samples)
            && self.warmup <= MAX_WARMUP
            && self.archive_percent <= MAX_ARCHIVE_PERCENT;
        let metadata = (MIN_METADATA_BYTES..=MAX_METADATA_BYTES).contains(&self.metadata_bytes);
        let durations = in_float_range(self.duration, MIN_DURATION_SECONDS, MAX_DURATION_SECONDS)
            && in_float_range(self.rate, MIN_RATE, MAX_RATE)
            && in_float_range(self.query_rate, MIN_QUERY_RATE, MAX_QUERY_RATE)
            && in_float_range(self.live_warmup_seconds, 0.0, MAX_LIVE_WARMUP_SECONDS);
        let repetitions = (1..=MAX_PAIRED_REPETITIONS).contains(&self.paired_repetitions);
        counts && metadata && durations && repetitions
    }

    /// Whether the promoted string keys are few, distinct, nonempty and short enough.
    fn promoted_keys_within_bounds(&self) -> bool {
        let keys = &self.promoted_string_keys;
        let distinct = keys.iter().collect::<BTreeSet<_>>().len();
        keys.len() <= MAX_PROMOTED_STRING_KEYS
            && keys
                .iter()
                .all(|key| !key.is_empty() && key.len() <= MAX_PROMOTED_KEY_BYTES)
            && distinct == keys.len()
    }

    /// Server configuration for this fixture; promoted keys are appended as one setting.
    pub(in crate::load) fn server_config(&self) -> String {
        if self.promoted_string_keys.is_empty() {
            return SERVER_CONFIG.to_owned();
        }
        let keys =
            serde_json::to_string(&self.promoted_string_keys).expect("string vector serialization");
        format!("{SERVER_CONFIG}promoted_string_keys={keys}\n")
    }
}

/// Whether a finite `value` lies in the inclusive range `min..=max`.
fn in_float_range(value: f64, min: f64, max: f64) -> bool {
    value.is_finite() && (min..=max).contains(&value)
}
