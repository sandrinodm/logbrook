//! Durable archive staging, recovery, publication, and retention.
use super::{
    Result, db_error, projection, projection_integrity, query::QueryDeadline, quota::DiskBudget,
    quote, readers::check_cancelled,
};
use crate::model::{Error, ErrorKind};
use duckdb::{Connection, params};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

mod compaction;
mod export;
mod migrations;
mod publication;
mod recovery;
mod retention;

pub(super) use compaction::compact_export;
#[cfg(test)]
pub(super) use export::export;
pub(super) use export::export_bounded;
pub(super) use migrations::migrate;
#[cfg(test)]
pub(super) use publication::publish;
pub(super) use publication::publish_bounded;
pub(super) use recovery::recover;
#[cfg(test)]
pub(super) use retention::retain;
pub(super) use retention::{expired_id, retain_bounded};

#[cfg(test)]
mod tests;

/// Last archive schema without promoted columns and a durable digest.
const LEGACY_ARCHIVE_SCHEMA: i32 = 3;

/// Archive schema that records promoted columns and their durable digest.
const PROJECTION_ARCHIVE_SCHEMA: i32 = 4;

/// Extra disk reservation for archive output beyond its estimated payload.
const ARCHIVE_DISK_HEADROOM_BYTES: u64 = 1024 * 1024;

/// One bounded export of the oldest hot event-time partition.
pub(super) struct ExportRequest<'a> {
    pub(super) dir: &'a Path,
    pub(super) before: i64,
    pub(super) batch_rows: usize,
    pub(super) partition_ms: i64,
    pub(super) work: ArchiveWork<'a>,
}

/// One bounded compaction of archives within an event-time partition.
pub(super) struct CompactionRequest<'a> {
    pub(super) dir: &'a Path,
    pub(super) max_files: usize,
    pub(super) max_bytes: u64,
    pub(super) partition_ms: i64,
    pub(super) work: ArchiveWork<'a>,
}

#[derive(Clone)]
pub(super) struct ArtifactGuard {
    temporary: std::path::PathBuf,
    final_path: std::path::PathBuf,
    armed: Arc<AtomicBool>,
}

impl Drop for ArtifactGuard {
    fn drop(&mut self) {
        // Cleanup ownership follows every queued publication clone. Only the
        // final owner can unlink an export that never became durable manifest state.
        if Arc::strong_count(&self.armed) == 1 && self.armed.load(Ordering::Acquire) {
            let _ = std::fs::remove_file(&self.temporary);
            let _ = std::fs::remove_file(&self.final_path);
        }
    }
}

/// Fails once the query's cancellation flag is set or its deadline has passed.
fn check_deadline(deadline: QueryDeadline<'_>) -> Result<()> {
    check_cancelled(deadline.cancelled, deadline.at)
}

/// Counts the rows of a Parquet file. Engine failures become archive validation errors.
fn count_parquet_rows(
    conn: &Connection,
    path: &Path,
    deadline: Option<QueryDeadline<'_>>,
) -> Result<i64> {
    conn.query_row(
        &format!(
            "SELECT count(*) FROM read_parquet('{}')",
            quote(&path.to_string_lossy())
        ),
        [],
        |r| r.get(0),
    )
    .map_err(|error| projection_integrity::validation_error(error, deadline))
}

/// Counts the rows of a file the engine has just written, under the query deadline.
fn read_back_row_count(conn: &Connection, path: &Path, deadline: QueryDeadline<'_>) -> Result<i64> {
    check_deadline(deadline)?;
    count_parquet_rows(conn, path, Some(deadline))
}

/// The row count the manifest records for `path`.
fn manifest_row_count(conn: &Connection, path: &str) -> Result<i64> {
    conn.query_row(
        "SELECT row_count FROM archives WHERE path=?",
        params![path],
        |r| r.get(0),
    )
    .map_err(db_error)
}

/// The schema recorded by a written archive, with or without promoted columns.
fn archive_schema(layout: Option<&projection::ProjectionLayout>) -> i32 {
    if layout.is_some() {
        PROJECTION_ARCHIVE_SCHEMA
    } else {
        LEGACY_ARCHIVE_SCHEMA
    }
}

/// The promoted-projection columns appended to an archive's SELECT list. Empty without a layout.
fn projection_columns(layout: Option<&projection::ProjectionLayout>) -> String {
    layout
        .map(|layout| format!(",{}", layout.backfill_columns_sql("attributes")))
        .unwrap_or_default()
}

/// A per-file stamp, so two exports never share a staging name.
fn file_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

/// A row count that does not fit the unsigned range the writer reports.
pub(super) fn row_count_error(error: std::num::TryFromIntError) -> Error {
    Error::new(ErrorKind::Integrity, "archive row count out of range").with_source(error)
}

/// The digest of a freshly written file. Validates the promoted projection first when there is a
/// layout, and returns `None` otherwise.
fn projected_digest(
    conn: &Connection,
    path: &Path,
    layout: Option<&projection::ProjectionLayout>,
    deadline: QueryDeadline<'_>,
) -> Result<Option<String>> {
    let Some(layout) = layout else {
        return Ok(None);
    };
    projection_integrity::validate_output(conn, path, layout, Some(deadline))?;
    Ok(Some(projection_integrity::hash(path, Some(deadline))?))
}

pub(super) struct ArchiveWork<'a> {
    pub(super) cancelled: &'a AtomicBool,
    pub(super) deadline: Instant,
    pub(super) quota: Option<&'a Arc<DiskBudget>>,
}

/// A Parquet file under its temporary name, with the guard that removes both names unless the
/// manifest commit makes the file durable.
struct StagedArchive {
    name: String,
    final_path: PathBuf,
    temporary: PathBuf,
    artifact: ArtifactGuard,
}

impl StagedArchive {
    /// Stages `name` in `dir`, armed so that a failed export removes both of its names.
    fn new(dir: &Path, name: String) -> Self {
        let final_path = dir.join(&name);
        let temporary = final_path.with_extension("tmp");
        let artifact = ArtifactGuard {
            temporary: temporary.clone(),
            final_path: final_path.clone(),
            armed: Arc::new(AtomicBool::new(true)),
        };
        Self {
            name,
            final_path,
            temporary,
            artifact,
        }
    }

    /// Makes the temporary file durable, renames it into place and syncs the directory. The caller
    /// commits the manifest row afterwards.
    fn rename_into_place(&self, dir: &Path) -> Result<()> {
        std::fs::File::open(&self.temporary)
            .map_err(db_error)?
            .sync_all()
            .map_err(db_error)?;
        std::fs::rename(&self.temporary, &self.final_path).map_err(db_error)?;
        std::fs::File::open(dir)
            .map_err(db_error)?
            .sync_all()
            .map_err(db_error)?;
        Ok(())
    }
}

/// Copied rows and the measured size of the completed Parquet file.
struct CopyStats {
    rows: u64,
    bytes: u64,
}
