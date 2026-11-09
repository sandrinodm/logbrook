//! Versioned hot-table and archive-manifest migrations.

use crate::{
    model::Error,
    storage::{Result, db_error},
};
use duckdb::Connection;

/// Newest hot-table schema accepted by this executable.
const MAX_SUPPORTED_SCHEMA_VERSION: i32 = 4;

/// Hot-table schema that adds durable archive bounds and byte metadata.
const ARCHIVE_METADATA_SCHEMA_VERSION: i32 = 3;

pub(in crate::storage) fn migrate(conn: &mut Connection) -> Result<()> {
    let tx = conn.transaction().map_err(db_error)?;
    tx.execute_batch("CREATE TABLE IF NOT EXISTS schema_version(version INTEGER PRIMARY KEY)")
        .map_err(db_error)?;
    let version: i32 = tx
        .query_row(
            "SELECT coalesce(max(version),0) FROM schema_version",
            [],
            |r| r.get(0),
        )
        .map_err(db_error)?;

    if version > MAX_SUPPORTED_SCHEMA_VERSION {
        return Err(Error::unavailable(
            "database schema is newer than this executable",
        ));
    }

    if version == 0 {
        tx.execute_batch(include_str!("../../../migrations/001_initial.sql"))
            .map_err(db_error)?;
    }

    if version < 2 {
        let expiry: i64 = tx
            .query_row(
                "SELECT count(*) FROM information_schema.columns \
                 WHERE table_name='retention_state' AND column_name='expired_through'",
                [],
                |r| r.get(0),
            )
            .map_err(db_error)?;
        if expiry == 0 {
            tx.execute_batch(
                "ALTER TABLE retention_state ADD COLUMN expired_through BIGINT DEFAULT 0",
            )
            .map_err(db_error)?;
        }

        tx.execute("INSERT INTO schema_version VALUES (2)", [])
            .map_err(db_error)?;
    }

    if version < ARCHIVE_METADATA_SCHEMA_VERSION {
        tx.execute_batch(include_str!("../../../migrations/002_archive_metadata.sql"))
            .map_err(db_error)?;
    }

    tx.commit().map_err(db_error)
}
