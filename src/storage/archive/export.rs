//! Bounded export of hot events to staged immutable Parquet files.
#[cfg(test)]
use super::ArchiveWork;
use super::{
    ARCHIVE_DISK_HEADROOM_BYTES, CopyStats, ExportRequest, StagedArchive, archive_schema,
    check_deadline, file_nanos, projected_digest, projection_columns, read_back_row_count,
};
use crate::{
    model::{Error, ErrorKind},
    storage::{
        Result, db_error,
        maintenance::Published,
        projection,
        query::QueryDeadline,
        quota::{self, DiskBudget},
        quote, widen,
    },
};
use duckdb::{Connection, params};
use std::{path::Path, sync::Arc};
#[cfg(test)]
use std::{
    sync::atomic::AtomicBool,
    time::{Duration, Instant},
};

#[cfg(test)]
pub(in crate::storage) fn export(
    conn: &mut Connection,
    dir: &Path,
    before: i64,
) -> Result<Option<Published>> {
    export_bounded(
        conn,
        ExportRequest {
            dir,
            before,
            batch_rows: 100000,
            partition_ms: 3600000,
            work: ArchiveWork {
                cancelled: &AtomicBool::new(false),
                deadline: Instant::now() + Duration::from_secs(30),
                quota: None,
            },
        },
    )
}

/// The event-time partition and ID range one export reads. The query and the publication use the
/// same bounds.
#[derive(Clone, Copy)]
struct ExportWindow {
    lower: i64,
    before: i64,
    high: i64,
}

impl ExportWindow {
    /// Counts the window's events and reads their time and ID bounds.
    fn stats(self, conn: &Connection) -> Result<WindowStats> {
        let (count, min, max, min_id): (i64, Option<i64>, Option<i64>, i64) = conn
            .query_row(
                "SELECT count(*),min(event_time),max(event_time),coalesce(min(id),0) \
                 FROM events WHERE event_time >= ? AND event_time < ? AND id <= ?",
                params![self.lower, self.before, self.high],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .map_err(db_error)?;
        Ok(WindowStats {
            count,
            min,
            max,
            min_id,
        })
    }
}

/// The statistics of an export window's events.
struct WindowStats {
    count: i64,
    min: Option<i64>,
    max: Option<i64>,
    min_id: i64,
}

/// Chooses the oldest partition before `before`, capped at `batch_rows` IDs. Returns `None` when
/// no event is older than `before`.
fn export_window(
    conn: &Connection,
    before: i64,
    batch_rows: usize,
    partition_ms: i64,
) -> Result<Option<ExportWindow>> {
    let ingest_high: i64 = conn
        .query_row(
            "SELECT next_id-1 FROM ingest_state WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .map_err(db_error)?;
    let first: Option<i64> = conn
        .query_row(
            "SELECT min(event_time) FROM events WHERE event_time < ?",
            params![before],
            |r| r.get(0),
        )
        .map_err(db_error)?;
    let Some(first) = first else {
        return Ok(None);
    };

    let lower = first.div_euclid(partition_ms).saturating_mul(partition_ms);
    let before = before.min(lower.saturating_add(partition_ms));
    // The same event-time partition and ID bounds drive export and publication;
    // later appends must remain in the hot table when the writer publishes this file.
    let high: i64 = conn
        .query_row(
            &format!(
                "SELECT coalesce(max(id),0) FROM (SELECT id FROM events \
                 WHERE event_time >= ? AND event_time < ? AND id <= ? \
                 ORDER BY id LIMIT {batch_rows})"
            ),
            params![lower, before, ingest_high],
            |r| r.get(0),
        )
        .map_err(db_error)?;
    Ok(Some(ExportWindow {
        lower,
        before,
        high,
    }))
}

/// Reserves the disk an export may use: twice its JSON input, the promoted columns, and fixed
/// headroom. Returns `None` when no disk quota is configured.
fn reserve_export(
    conn: &Connection,
    budget: Option<&Arc<DiskBudget>>,
    layout: Option<&projection::ProjectionLayout>,
    window: ExportWindow,
) -> Result<Option<quota::Reservation>> {
    let Some(budget) = budget else {
        return Ok(None);
    };

    let ExportWindow {
        lower,
        before,
        high,
    } = window;
    let input: u64 = conn
        .query_row(
            "SELECT coalesce(sum(octet_length(encode(source))+\
             octet_length(encode(message))+octet_length(encode(attributes))+\
             octet_length(encode(coalesce(service,'')))+\
             octet_length(encode(coalesce(logger,'')))+\
             octet_length(encode(coalesce(host,'')))+128),0)::UBIGINT \
             FROM events WHERE event_time >= ? AND event_time < ? AND id <= ?",
            params![lower, before, high],
            |row| row.get(0),
        )
        .map_err(db_error)?;
    budget.reserve(
        input
            .saturating_mul(2 + layout.map_or(0, |layout| widen(layout.keys().len())))
            .saturating_add(ARCHIVE_DISK_HEADROOM_BYTES),
    )
}

/// Writes the window's events to the temporary file, sorted by event time and ID.
fn copy_window(
    conn: &Connection,
    window: ExportWindow,
    layout: Option<&projection::ProjectionLayout>,
    temporary: &Path,
) -> Result<CopyStats> {
    let ExportWindow {
        lower,
        before,
        high,
    } = window;
    let extra = projection_columns(layout);
    let rows: u64 = conn
        .query_row(
            &format!(
                "COPY (SELECT \
                 id,source,event_time,received_at,level,service,logger,host,pid,message,\
                 attributes{extra} \
                 FROM events WHERE event_time >= {lower} AND event_time < {before} \
                 AND id <= {high} \
                 ORDER BY event_time,id) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
                quote(&temporary.to_string_lossy())
            ),
            [],
            |r| r.get(0),
        )
        .map_err(db_error)?;

    // RETURN_STATS includes VARIANT-valued extra_info in DuckDB 2, which the
    // stable Rust row API cannot decode. Default COPY returns the row count;
    // measure the completed staged file directly for publication and quota checks.
    let bytes = std::fs::metadata(temporary).map_err(db_error)?.len();
    Ok(CopyStats { rows, bytes })
}

/// Exports the oldest partition before `before`, capped at `batch_rows` IDs, as a new archive
/// file, and commits its manifest row. Returns `None` when there is nothing to export.
pub(in crate::storage) fn export_bounded(
    conn: &mut Connection,
    request: ExportRequest<'_>,
) -> Result<Option<Published>> {
    let ExportRequest {
        dir,
        before,
        batch_rows,
        partition_ms,
        work,
    } = request;

    let cancellation = QueryDeadline {
        cancelled: work.cancelled,
        at: work.deadline,
    };
    check_deadline(cancellation)?;

    let tx = conn.transaction().map_err(db_error)?;
    let conn = &tx;
    let layout = projection::ProjectionLayout::load(conn)?;
    let Some(window) = export_window(conn, before, batch_rows, partition_ms)? else {
        return Ok(None);
    };

    let stats = window.stats(conn)?;
    if stats.count == 0 {
        return Ok(None);
    }

    let reservation = reserve_export(conn, work.quota, layout.as_ref(), window)?;
    let staged = StagedArchive::new(
        dir,
        format!("archive-{}-{}.parquet", window.high, file_nanos()),
    );

    check_deadline(cancellation)?;
    let copied = copy_window(conn, window, layout.as_ref(), &staged.temporary)?;
    let actual = read_back_row_count(conn, &staged.temporary, cancellation)?;
    if actual != stats.count || copied.rows != stats.count as u64 {
        return Err(Error::new(
            ErrorKind::Integrity,
            "archive validation count mismatch",
        ));
    }

    check_deadline(cancellation)?;
    let projection_sha256 =
        projected_digest(conn, &staged.temporary, layout.as_ref(), cancellation)?;
    staged.rename_into_place(dir)?;
    tx.commit().map_err(db_error)?;

    Ok(Some(Published {
        path: staged.name,
        min: stats.min.unwrap(),
        max: stats.max.unwrap(),
        count: stats.count,
        before: window.before,
        high: window.high,
        lower: window.lower,
        min_id: stats.min_id,
        artifact: staged.artifact,
        reservation,
        replaced: vec![],
        bytes: copied.bytes,
        archive_schema: archive_schema(layout.as_ref()),
        projection_sha256,
    }))
}
