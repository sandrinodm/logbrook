//! Retention snapshots and transactional bounded deletion.

#[cfg(test)]
use crate::storage::maintenance::MaintenanceBudget;
use crate::{
    model::Query,
    storage::{
        Result, db_error,
        leases::ArchiveLeases,
        maintenance::WriterScope,
        projection_integrity,
        query::{self, QueryDeadline},
        readers::check_cancelled,
    },
};
use duckdb::{Connection, params};
use std::{path::Path, sync::atomic::AtomicBool, time::Instant};
#[cfg(test)]
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

pub(in crate::storage) fn expired_id(
    conn: &mut Connection,
    dir: &Path,
    before: i64,
    cancelled: &AtomicBool,
    deadline: Instant,
) -> Result<i64> {
    check_cancelled(cancelled, deadline)?;

    let tx = conn.transaction().map_err(db_error)?;
    let scope = Query {
        from: i64::MIN,
        to: before,
        ..Query::default()
    };

    let cancellation = QueryDeadline {
        cancelled,
        at: deadline,
    };
    let selected = query::selected_dataset(&tx, dir, &scope, None, Some(cancellation))?;
    let data = &selected.sql;
    let expired = tx
        .query_row(
            &format!("SELECT coalesce(max(id),0) FROM ({data}) retained WHERE event_time < ?"),
            params![before],
            |r| r.get(0),
        )
        .map_err(db_error)?;
    projection_integrity::verify_all(&selected.integrity, Some(cancellation))?;
    tx.commit().map_err(db_error)?;

    Ok(expired)
}

#[cfg(test)]
pub(in crate::storage) fn retain(
    conn: &mut Connection,
    _dir: &Path,
    before: i64,
    expired: i64,
    leases: &mut ArchiveLeases,
    max_rows: usize,
) -> Result<usize> {
    let budget = MaintenanceBudget {
        cancelled: Arc::new(AtomicBool::new(false)),
        deadline: Instant::now() + Duration::from_secs(30),
    };
    let scope = WriterScope::start(budget, &Arc::new(Mutex::new(None))).unwrap();
    retain_bounded(conn, before, expired, leases, max_rows, &scope)
}

pub(in crate::storage) fn retain_bounded(
    conn: &mut Connection,
    before: i64,
    expired: i64,
    leases: &mut ArchiveLeases,
    max_rows: usize,
    scope: &WriterScope,
) -> Result<usize> {
    scope.check()?;

    let tx = conn.transaction().map_err(|error| scope.error(error))?;
    // Include late appends committed after the maintenance snapshot.
    let hot_expired: i64 = tx
        .query_row(
            "SELECT coalesce(max(id),0) FROM events WHERE event_time < ?",
            params![before],
            |r| r.get(0),
        )
        .map_err(|error| scope.error(error))?;

    let expired = expired.max(hot_expired);
    tx.execute(
        "UPDATE retention_state SET expired_through=greatest(expired_through,?) WHERE singleton=1",
        params![expired],
    )
    .map_err(|error| scope.error(error))?;

    let count = tx
        .execute(
            &format!(
                "DELETE FROM events WHERE id IN (SELECT id FROM events \
                 WHERE event_time < ? ORDER BY id LIMIT {max_rows})"
            ),
            params![before],
        )
        .map_err(|error| scope.error(error))?;

    let paths = {
        let mut stmt = tx
            .prepare("SELECT path FROM archives WHERE max_time < ?")
            .map_err(|error| scope.error(error))?;
        stmt.query_map(params![before], |r| r.get::<_, String>(0))
            .map_err(|error| scope.error(error))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| scope.error(error))?
    };

    tx.execute(
        "UPDATE retention_state SET before_time=greatest(before_time,?) WHERE singleton=1",
        params![before],
    )
    .map_err(|error| scope.error(error))?;
    tx.execute("DELETE FROM archives WHERE max_time < ?", params![before])
        .map_err(|error| scope.error(error))?;

    scope.finish()?;
    tx.commit().map_err(db_error)?;
    leases.retire(paths);

    Ok(count)
}
