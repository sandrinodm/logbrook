//! Validates committed archives, backfills legacy metadata, and removes orphan files.
use super::{PROJECTION_ARCHIVE_SCHEMA, count_parquet_rows, manifest_row_count};
use crate::{
    model::{Error, ErrorKind},
    storage::{Result, db_error, projection, projection_integrity, quote, validate_archive_name},
};
use duckdb::{Connection, params};
use std::path::Path;

/// One manifest row as recovery reads it, before its file is checked.
struct ManifestEntry {
    path: String,
    schema: i32,
    expected_bytes: Option<u64>,
    bounds_missing: bool,
}

/// Checks every committed archive against its manifest row and removes the files the manifest
/// does not reference, before queries are admitted.
pub(in crate::storage) fn recover(conn: &Connection, dir: &Path) -> Result<()> {
    let layout = projection::ProjectionLayout::load(conn)?;
    let paths = manifest_paths(conn)?;
    for path in &paths {
        validate_archive_name(path)?;

        if !dir.join(path).is_file() {
            return Err(Error::new(
                ErrorKind::Integrity,
                format!("referenced archive missing: {path}"),
            ));
        }
    }

    // Older manifests have no ID or byte bounds. Backfill them from the immutable file
    // before admitting queries; legacy files remain readable and eligible for compaction.
    for path in &paths {
        let entry = load_manifest_entry(conn, path)?;
        verify_manifest_entry(conn, dir, layout.as_ref(), &entry)?;
        if entry.bounds_missing {
            backfill_bounds(conn, dir, path)?;
        }
    }

    remove_orphan_files(dir, &paths)
}

fn manifest_paths(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn
        .prepare("SELECT path FROM archives")
        .map_err(db_error)?;
    stmt.query_map([], |r| r.get::<_, String>(0))
        .map_err(db_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(db_error)
}

fn load_manifest_entry(conn: &Connection, path: &str) -> Result<ManifestEntry> {
    let bounds_missing: bool = conn
        .query_row(
            "SELECT min_id IS NULL OR max_id IS NULL OR file_bytes IS NULL \
             FROM archives WHERE path=?",
            params![path],
            |r| r.get(0),
        )
        .map_err(db_error)?;
    let (expected_bytes, schema): (Option<u64>, i32) = conn
        .query_row(
            "SELECT file_bytes,coalesce(schema_version,1) FROM archives WHERE path=?",
            params![path],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(db_error)?;
    if !(1..=PROJECTION_ARCHIVE_SCHEMA).contains(&schema) {
        return Err(Error::new(
            ErrorKind::Integrity,
            "unsupported committed archive schema version",
        ));
    }

    Ok(ManifestEntry {
        path: path.to_owned(),
        schema,
        expected_bytes,
        bounds_missing,
    })
}

/// Verifies the file behind one manifest row against its schema generation and recorded size.
fn verify_manifest_entry(
    conn: &Connection,
    dir: &Path,
    layout: Option<&projection::ProjectionLayout>,
    entry: &ManifestEntry,
) -> Result<()> {
    let full = dir.join(&entry.path);
    if entry.schema < PROJECTION_ARCHIVE_SCHEMA && layout.is_some() {
        verify_legacy_projection(conn, &entry.path, &full)?;
    }

    if entry.schema == PROJECTION_ARCHIVE_SCHEMA {
        verify_promoted_archive(conn, &entry.path, &full, layout)?;
    }

    check_expected_bytes(&full, entry.expected_bytes)
}

/// A legacy archive predates projections: its manifest must not record a digest, and its file
/// must not carry projection columns.
fn verify_legacy_projection(conn: &Connection, path: &str, full: &Path) -> Result<()> {
    let digest: Option<String> = conn
        .query_row(
            "SELECT projection_sha256 FROM archives WHERE path=?",
            params![path],
            |r| r.get(0),
        )
        .map_err(db_error)?;
    if digest.is_some() {
        return Err(projection_integrity::integrity(
            "legacy archive has unexpected projection digest",
        ));
    }

    projection_integrity::validate_legacy_schema(conn, full, None)
}

/// A promoted archive must match its durable digest, its projection schema and its manifest row
/// count.
fn verify_promoted_archive(
    conn: &Connection,
    path: &str,
    full: &Path,
    layout: Option<&projection::ProjectionLayout>,
) -> Result<()> {
    let layout = layout.ok_or_else(|| {
        projection_integrity::integrity("promoted archive has no durable index layout")
    })?;
    let digest: Option<String> = conn
        .query_row(
            "SELECT projection_sha256 FROM archives WHERE path=?",
            params![path],
            |r| r.get(0),
        )
        .map_err(db_error)?;
    projection_integrity::ArchiveIntegrity::new(full.to_path_buf(), digest)?.verify(None)?;
    projection_integrity::validate_output(conn, full, layout, None)?;

    let actual = count_parquet_rows(conn, full, None)?;
    if actual != manifest_row_count(conn, path)? {
        return Err(projection_integrity::integrity(
            "promoted archive row count mismatch",
        ));
    }

    Ok(())
}

fn check_expected_bytes(full: &Path, expected: Option<u64>) -> Result<()> {
    if expected.is_some_and(|bytes| {
        std::fs::metadata(full).map_or(true, |metadata| metadata.len() != bytes)
    }) {
        return Err(Error::new(
            ErrorKind::Integrity,
            "committed archive byte size mismatch",
        ));
    }

    Ok(())
}

/// Fills in the ID and byte bounds of a legacy manifest row from its immutable file.
fn backfill_bounds(conn: &Connection, dir: &Path, path: &str) -> Result<()> {
    let full = dir.join(path);
    let (min_id, max_id, count): (i64, i64, i64) = conn
        .query_row(
            &format!(
                "SELECT min(id),max(id),count(*) FROM read_parquet('{}')",
                quote(&full.to_string_lossy())
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(|e| projection_integrity::validation_error(e, None))?;
    if manifest_row_count(conn, path)? != count {
        return Err(Error::new(
            ErrorKind::Integrity,
            "legacy archive row count mismatch",
        ));
    }

    conn.execute(
        "UPDATE archives SET min_id=?,max_id=?,file_bytes=? WHERE path=?",
        params![
            min_id,
            max_id,
            std::fs::metadata(full).map_err(db_error)?.len(),
            path
        ],
    )
    .map_err(db_error)?;
    Ok(())
}

/// Removes temporary and unreferenced Parquet files that an interrupted export left behind.
fn remove_orphan_files(dir: &Path, referenced: &[String]) -> Result<()> {
    for entry in std::fs::read_dir(dir).map_err(db_error)? {
        let path = entry.map_err(db_error)?.path();
        if path.is_file()
            && !referenced.contains(&path.file_name().unwrap().to_string_lossy().to_string())
            && matches!(
                path.extension().and_then(|v| v.to_str()),
                Some("tmp" | "parquet")
            )
        {
            std::fs::remove_file(path).map_err(db_error)?;
        }
    }

    Ok(())
}
