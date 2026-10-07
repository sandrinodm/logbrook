//! Durable archive exports, transactional manifests, and versioned migrations.
use super::*;

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

pub(super) fn migrate(conn: &mut Connection) -> Result<()> {
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

    if version > 3 {
        return Err(Error::unavailable(
            "database schema is newer than this executable",
        ));
    }

    if version == 0 {
        tx.execute_batch(include_str!("../../migrations/001_initial.sql"))
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

    if version < 3 {
        tx.execute_batch(include_str!("../../migrations/002_archive_metadata.sql"))
            .map_err(db_error)?;
    }

    tx.commit().map_err(db_error)
}

pub(super) fn recover(conn: &Connection, dir: &Path) -> Result<()> {
    let mut stmt = conn
        .prepare("SELECT path FROM archives")
        .map_err(db_error)?;
    let paths = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(db_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(db_error)?;

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
        let missing: bool = conn
            .query_row(
                "SELECT min_id IS NULL OR max_id IS NULL OR file_bytes IS NULL \
                 FROM archives WHERE path=?",
                params![path],
                |r| r.get(0),
            )
            .map_err(db_error)?;
        let full = dir.join(path);
        let (expected_bytes, schema): (Option<u64>, i32) = conn
            .query_row(
                "SELECT file_bytes,coalesce(schema_version,1) FROM archives WHERE path=?",
                params![path],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(db_error)?;

        if !(1..=3).contains(&schema) {
            return Err(Error::new(
                ErrorKind::Integrity,
                "unsupported committed archive schema version",
            ));
        }

        if expected_bytes.is_some_and(|bytes| {
            std::fs::metadata(&full).map_or(true, |metadata| metadata.len() != bytes)
        }) {
            return Err(Error::new(
                ErrorKind::Integrity,
                "committed archive byte size mismatch",
            ));
        }

        if missing {
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
                .map_err(|e| Error::new(ErrorKind::Integrity, e.to_string()))?;
            let expected: i64 = conn
                .query_row(
                    "SELECT row_count FROM archives WHERE path=?",
                    params![path],
                    |r| r.get(0),
                )
                .map_err(db_error)?;

            if expected != count {
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
        }
    }

    for entry in std::fs::read_dir(dir).map_err(db_error)? {
        let path = entry.map_err(db_error)?.path();

        if path.is_file()
            && !paths.contains(&path.file_name().unwrap().to_string_lossy().to_string())
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

#[cfg(test)]
pub(super) fn export(conn: &mut Connection, dir: &Path, before: i64) -> Result<Option<Published>> {
    export_bounded(
        conn,
        dir,
        before,
        100000,
        3600000,
        ArchiveWork {
            cancelled: &AtomicBool::new(false),
            deadline: Instant::now() + Duration::from_secs(30),
            quota: None,
        },
    )
}

pub(super) struct ArchiveWork<'a> {
    pub(super) cancelled: &'a AtomicBool,
    pub(super) deadline: Instant,
    pub(super) quota: Option<&'a Arc<DiskBudget>>,
}

pub(super) fn export_bounded(
    conn: &mut Connection,
    dir: &Path,
    before: i64,
    batch_rows: usize,
    partition_ms: i64,
    work: ArchiveWork<'_>,
) -> Result<Option<Published>> {
    let cancelled = work.cancelled;
    let deadline = work.deadline;
    check_cancelled(cancelled, deadline)?;
    let tx = conn.transaction().map_err(db_error)?;
    let conn = &tx;
    let high: i64 = conn
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
            params![lower, before, high],
            |r| r.get(0),
        )
        .map_err(db_error)?;
    let (count, min, max, min_id): (i64, Option<i64>, Option<i64>, i64) = conn
        .query_row(
            "SELECT count(*),min(event_time),max(event_time),coalesce(min(id),0) \
             FROM events WHERE event_time >= ? AND event_time < ? AND id <= ?",
            params![lower, before, high],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .map_err(db_error)?;

    if count == 0 {
        return Ok(None);
    }

    let reservation = if let Some(quota) = work.quota {
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
        quota.reserve(input.saturating_mul(2).saturating_add(1024 * 1024))?
    } else {
        None
    };

    let final_path = dir.join(format!(
        "archive-{high}-{}.parquet",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let temporary = final_path.with_extension("tmp");
    let artifact = ArtifactGuard {
        temporary: temporary.clone(),
        final_path: final_path.clone(),
        armed: Arc::new(AtomicBool::new(true)),
    };

    check_cancelled(cancelled, deadline)?;
    let (written, output_bytes): (u64, u64) = conn
        .query_row(
            &format!(
                "COPY (SELECT \
                 id,source,event_time,received_at,level,service,logger,host,pid,message,attributes \
                 FROM events WHERE event_time >= {lower} AND event_time < {before} AND id <= {high} \
                 ORDER BY event_time,id) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD, RETURN_STATS)",
                quote(&temporary.to_string_lossy())
            ),
            [],
            |r| Ok((r.get(1)?, r.get(2)?)),
        )
        .map_err(db_error)?;
    check_cancelled(cancelled, deadline)?;
    let actual: i64 = conn
        .query_row(
            &format!(
                "SELECT count(*) FROM read_parquet('{}')",
                quote(&temporary.to_string_lossy())
            ),
            [],
            |r| r.get(0),
        )
        .map_err(db_error)?;

    if actual != count || written != count as u64 {
        return Err(Error::new(
            ErrorKind::Integrity,
            "archive validation count mismatch",
        ));
    }

    check_cancelled(cancelled, deadline)?;
    std::fs::File::open(&temporary)
        .map_err(db_error)?
        .sync_all()
        .map_err(db_error)?;
    std::fs::rename(&temporary, &final_path).map_err(db_error)?;
    std::fs::File::open(dir)
        .map_err(db_error)?
        .sync_all()
        .map_err(db_error)?;
    tx.commit().map_err(db_error)?;

    Ok(Some(Published {
        path: final_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string(),
        min: min.unwrap(),
        max: max.unwrap(),
        count,
        before,
        high,
        lower,
        min_id,
        artifact,
        reservation,
        replaced: vec![],
        bytes: output_bytes,
    }))
}

pub(super) fn compact_export(
    conn: &mut Connection,
    dir: &Path,
    max_files: usize,
    max_bytes: u64,
    partition_ms: i64,
    work: ArchiveWork<'_>,
) -> Result<Option<Published>> {
    let cancelled = work.cancelled;
    let deadline = work.deadline;
    check_cancelled(cancelled, deadline)?;
    let tx = conn.transaction().map_err(db_error)?;
    let mut stmt = tx
        .prepare(
            "SELECT path,min_time,max_time,row_count,min_id,max_id,coalesce(file_bytes,0) \
             FROM archives WHERE min_id IS NOT NULL AND max_id IS NOT NULL \
             ORDER BY min_time,path",
        )
        .map_err(db_error)?;
    let rows = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, u64>(6)?,
            ))
        })
        .map_err(db_error)?;

    let mut selected = Vec::new();
    let mut bytes = 0u64;
    let mut partition = None;

    for row in rows {
        check_cancelled(cancelled, deadline)?;
        let row = row.map_err(db_error)?;
        let key = row.1.div_euclid(partition_ms);

        if row.2.div_euclid(partition_ms) != key {
            continue;
        }

        if partition.is_some_and(|p| p != key) {
            if selected.len() >= 2 {
                break;
            }

            selected.clear();
            bytes = 0;
        }

        if row.6 > max_bytes || bytes.saturating_add(row.6) > max_bytes {
            continue;
        }
        partition = Some(key);
        bytes += row.6;
        selected.push(row);

        if selected.len() >= max_files {
            break;
        }
    }

    drop(stmt);

    if selected.len() < 2 {
        return Ok(None);
    }

    let count: i64 = selected.iter().map(|r| r.3).sum();
    let min = selected.iter().map(|r| r.1).min().unwrap();
    let max = selected.iter().map(|r| r.2).max().unwrap();
    let min_id = selected.iter().map(|r| r.4).min().unwrap();
    let high = selected.iter().map(|r| r.5).max().unwrap();
    let mut paths = Vec::new();

    for row in &selected {
        validate_archive_name(&row.0)?;
        paths.push(format!("'{}'", quote(&dir.join(&row.0).to_string_lossy())));
    }

    let reservation = if let Some(quota) = work.quota {
        quota.reserve(bytes.saturating_mul(2).saturating_add(1024 * 1024))?
    } else {
        None
    };

    let path = format!(
        "compact-{high}-{}.parquet",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let final_path = dir.join(&path);
    let temporary = final_path.with_extension("tmp");
    let artifact = ArtifactGuard {
        temporary: temporary.clone(),
        final_path: final_path.clone(),
        armed: Arc::new(AtomicBool::new(true)),
    };

    check_cancelled(cancelled, deadline)?;
    let (written, file_bytes): (u64, u64) = tx
        .query_row(
            &format!(
                "COPY (SELECT \
                 id,source,event_time,received_at,level,service,logger,host,pid,message,attributes \
                 FROM (SELECT * FROM events WHERE false \
                 UNION ALL BY NAME SELECT * FROM read_parquet([{}],union_by_name=true)) \
                 ORDER BY event_time,id) \
                 TO '{}' (FORMAT PARQUET,COMPRESSION ZSTD,RETURN_STATS)",
                paths.join(","),
                quote(&temporary.to_string_lossy())
            ),
            [],
            |r| Ok((r.get(1)?, r.get(2)?)),
        )
        .map_err(db_error)?;
    check_cancelled(cancelled, deadline)?;
    let actual: i64 = tx
        .query_row(
            &format!(
                "SELECT count(*) FROM read_parquet('{}')",
                quote(&temporary.to_string_lossy())
            ),
            [],
            |r| r.get(0),
        )
        .map_err(db_error)?;

    if actual != count || written != count as u64 {
        return Err(Error::new(
            ErrorKind::Integrity,
            "compaction count mismatch",
        ));
    }
    std::fs::File::open(&temporary)
        .map_err(db_error)?
        .sync_all()
        .map_err(db_error)?;
    std::fs::rename(&temporary, &final_path).map_err(db_error)?;
    std::fs::File::open(dir)
        .map_err(db_error)?
        .sync_all()
        .map_err(db_error)?;
    tx.commit().map_err(db_error)?;

    Ok(Some(Published {
        path,
        min,
        max,
        count,
        before: 0,
        high,
        min_id,
        bytes: file_bytes,
        lower: 0,
        artifact,
        reservation,
        replaced: selected.into_iter().map(|r| r.0).collect(),
    }))
}

#[cfg(test)]
pub(super) fn publish(conn: &mut Connection, file: Published) -> Result<usize> {
    let budget = MaintenanceBudget {
        cancelled: Arc::new(AtomicBool::new(false)),
        deadline: Instant::now() + Duration::from_secs(30),
    };

    let scope = WriterScope::start(budget, &Arc::new(Mutex::new(None))).unwrap();
    publish_bounded(conn, file, &scope)
}

pub(super) fn publish_bounded(
    conn: &mut Connection,
    file: Published,
    scope: &WriterScope,
) -> Result<usize> {
    scope.check()?;
    let tx = conn.transaction().map_err(|error| scope.error(error))?;
    tx.execute(
        "INSERT INTO \
         archives(path,min_time,max_time,row_count,min_id,max_id,file_bytes,schema_version) \
         VALUES (?,?,?,?,?,?,?,3)",
        params![
            file.path,
            file.min,
            file.max,
            file.count,
            file.min_id,
            file.high,
            file.bytes
        ],
    )
    .map_err(|error| scope.error(error))?;

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

        if deleted != file.count as usize {
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

    Ok(file.count as usize)
}

pub(super) fn expired_id(
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

    let data = dataset(&tx, dir, &scope)?;
    let expired = tx
        .query_row(
            &format!("SELECT coalesce(max(id),0) FROM ({data}) retained WHERE event_time < ?"),
            params![before],
            |r| r.get(0),
        )
        .map_err(db_error)?;
    tx.commit().map_err(db_error)?;

    Ok(expired)
}

#[cfg(test)]
pub(super) fn retain(
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

pub(super) fn retain_bounded(
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
