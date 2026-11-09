//! Transactional manifest publication and hot-row deletion.
use super::{PROJECTION_ARCHIVE_SCHEMA, row_count_error};
#[cfg(test)]
use crate::storage::maintenance::MaintenanceBudget;
use crate::{
    model::{Error, ErrorKind},
    storage::{
        Result, db_error,
        maintenance::{Published, WriterScope},
    },
};
use duckdb::{Connection, params};
use std::sync::atomic::Ordering;
#[cfg(test)]
use std::{
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::{Duration, Instant},
};

#[cfg(test)]
pub(in crate::storage) fn publish(conn: &mut Connection, file: Published) -> Result<usize> {
    let budget = MaintenanceBudget {
        cancelled: Arc::new(AtomicBool::new(false)),
        deadline: Instant::now() + Duration::from_secs(30),
    };
    let scope = WriterScope::start(budget, &Arc::new(Mutex::new(None))).unwrap();
    publish_bounded(conn, file, &scope)
}

/// Records the manifest row of a published file. Promoted archives also record their projection
/// digest.
fn insert_manifest_row(
    tx: &duckdb::Transaction<'_>,
    file: &Published,
    scope: &WriterScope,
) -> Result<()> {
    if file.archive_schema == PROJECTION_ARCHIVE_SCHEMA {
        tx.execute(
            "INSERT INTO archives(path,min_time,max_time,row_count,min_id,max_id,file_bytes,\
             schema_version,projection_sha256) VALUES (?,?,?,?,?,?,?,?,?)",
            params![
                file.path,
                file.min,
                file.max,
                file.count,
                file.min_id,
                file.high,
                file.bytes,
                file.archive_schema,
                file.projection_sha256
            ],
        )
        .map_err(|error| scope.error(error))?;
    } else {
        tx.execute(
            "INSERT INTO archives(path,min_time,max_time,row_count,min_id,max_id,file_bytes,\
             schema_version) VALUES (?,?,?,?,?,?,?,?)",
            params![
                file.path,
                file.min,
                file.max,
                file.count,
                file.min_id,
                file.high,
                file.bytes,
                file.archive_schema
            ],
        )
        .map_err(|error| scope.error(error))?;
    }
    Ok(())
}

pub(in crate::storage) fn publish_bounded(
    conn: &mut Connection,
    file: Published,
    scope: &WriterScope,
) -> Result<usize> {
    scope.check()?;

    let count = usize::try_from(file.count).map_err(row_count_error)?;
    let tx = conn.transaction().map_err(|error| scope.error(error))?;
    insert_manifest_row(&tx, &file, scope)?;

    if !file.replaced.is_empty() {
        for path in &file.replaced {
            scope.check()?;
            let changed = tx
                .execute("DELETE FROM archives WHERE path=?", params![path])
                .map_err(|error| scope.error(error))?;
            if changed != 1 {
                scope.disarm();

                return Err(Error::unavailable(
                    "compaction source changed before publication",
                ));
            }
        }
    } else {
        let deleted = tx
            .execute(
                "DELETE FROM events WHERE event_time >= ? AND event_time < ? AND id <= ?",
                params![file.lower, file.before, file.high],
            )
            .map_err(|error| scope.error(error))?;
        if deleted != count {
            scope.disarm();

            return Err(Error::new(
                ErrorKind::Integrity,
                "archive publication delete count mismatch",
            ));
        }
    }

    // Commit and its uncertainty probe must never be interrupted. A timeout
    // after this boundary may return before the durable outcome is available;
    // ArtifactGuard still protects any possibly committed archive.
    scope.finish()?;

    if let Err(error) = tx.commit() {
        let _ = conn.execute_batch("ROLLBACK");

        let present = conn.query_row(
            "SELECT count(*) FROM archives WHERE path=?",
            params![file.path],
            |r| r.get::<_, i64>(0),
        );
        // An uncertain commit must keep the file for recovery; never unlink a
        // potentially committed object after a transport/durability error.
        if !matches!(present, Ok(0)) {
            file.artifact.armed.store(false, Ordering::Release);
        }

        return Err(db_error(error));
    }

    file.artifact.armed.store(false, Ordering::Release);

    Ok(count)
}
