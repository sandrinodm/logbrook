//! A measured file-footprint retention target, not a filesystem hard quota.
//! Native allocation and bounded in-flight operations may temporarily exceed it.
use super::*;
use duckdb::OptionalExt;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct DiskFootprint {
    pub(super) database: u64,
    pub(super) wal: u64,
    pub(super) archives: u64,
    pub(super) temporary: u64,
    pub(super) other: u64,
}

impl DiskFootprint {
    pub(super) fn total(self) -> u64 {
        self.database
            .saturating_add(self.wal)
            .saturating_add(self.archives)
            .saturating_add(self.temporary)
            .saturating_add(self.other)
    }
}

fn file_bytes(path: &Path) -> Result<u64> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(db_error(error)),
    }
}

/// Include unpublished output and unknown files. Never follow links outside the index.
pub(super) fn footprint(root: &Path) -> Result<DiskFootprint> {
    let mut result = DiskFootprint::default();
    let mut pending = vec![root.to_owned()];

    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).map_err(db_error)? {
            let entry = entry.map_err(db_error)?;
            let path = entry.path();
            let metadata = match std::fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(db_error(error)),
            };

            if metadata.is_dir() {
                pending.push(path);
                continue;
            }

            let relative = path.strip_prefix(root).map_err(db_error)?;
            let bytes = metadata.len();
            let first = relative
                .components()
                .next()
                .and_then(|part| part.as_os_str().to_str());

            match relative.to_str() {
                Some("events.duckdb") => result.database = result.database.saturating_add(bytes),
                Some("events.duckdb.wal") => result.wal = result.wal.saturating_add(bytes),
                _ if first == Some("temp")
                    || path.extension().is_some_and(|extension| extension == "tmp") =>
                {
                    result.temporary = result.temporary.saturating_add(bytes)
                }
                _ if first == Some("archives") => {
                    result.archives = result.archives.saturating_add(bytes)
                }
                _ => result.other = result.other.saturating_add(bytes),
            }
        }
    }

    Ok(result)
}

pub(super) struct DiskBudget {
    directory: PathBuf,
    pub(super) limit: Option<u64>,
    archives: AtomicU64,
    temporary: AtomicU64,
    other: AtomicU64,
    pub(super) measured: AtomicU64,
    pub(super) reserved: AtomicU64,
    pub(super) blocked: AtomicBool,
    pub(super) hot_attempted: AtomicBool,
}

impl DiskBudget {
    pub(super) fn new(directory: PathBuf, limit: Option<u64>) -> Result<Arc<Self>> {
        let budget = Arc::new(Self {
            directory,
            limit,
            archives: AtomicU64::new(0),
            temporary: AtomicU64::new(0),
            other: AtomicU64::new(0),
            measured: AtomicU64::new(0),
            reserved: AtomicU64::new(0),
            blocked: AtomicBool::new(false),
            hot_attempted: AtomicBool::new(false),
        });

        if limit.is_some() {
            budget.audit()?;
        }

        Ok(budget)
    }

    /// Serialized maintenance audits immutable output; the writer only stats two native files.
    pub(super) fn audit(&self) -> Result<u64> {
        if self.limit.is_none() {
            return Ok(0);
        }

        let snapshot = footprint(&self.directory)?;
        self.archives.store(snapshot.archives, Ordering::Release);
        self.temporary.store(snapshot.temporary, Ordering::Release);
        self.other.store(snapshot.other, Ordering::Release);
        let previous = self.measured.swap(snapshot.total(), Ordering::AcqRel);

        if snapshot.total() < previous {
            self.hot_attempted.store(false, Ordering::Release);
        }

        self.update_blocked(snapshot.total());

        Ok(snapshot.total())
    }

    pub(super) fn refresh_native(&self) -> Result<u64> {
        if self.limit.is_none() {
            return Ok(0);
        }

        let total = file_bytes(&self.directory.join("events.duckdb"))?
            .saturating_add(file_bytes(&self.directory.join("events.duckdb.wal"))?)
            .saturating_add(self.archives.load(Ordering::Acquire))
            .saturating_add(self.temporary.load(Ordering::Acquire))
            .saturating_add(self.other.load(Ordering::Acquire));
        let previous = self.measured.swap(total, Ordering::AcqRel);

        if total < previous {
            self.hot_attempted.store(false, Ordering::Release);
        }

        self.update_blocked(total);

        Ok(total)
    }

    fn update_blocked(&self, total: u64) {
        self.blocked.store(
            self.limit.is_some_and(|limit| {
                total.saturating_add(self.reserved.load(Ordering::Acquire)) >= limit
            }),
            Ordering::Release,
        );
    }

    pub(super) fn check_write(&self) -> Result<()> {
        self.refresh_native()?;

        if self.blocked.load(Ordering::Acquire) {
            return Err(size_error());
        }

        Ok(())
    }

    pub(super) fn published(&self, bytes: u64) {
        if self.limit.is_some() {
            self.archives.fetch_add(bytes, Ordering::AcqRel);
            self.hot_attempted.store(false, Ordering::Release);
        }
    }

    pub(super) fn reserve(self: &Arc<Self>, bytes: u64) -> Result<Option<Reservation>> {
        let Some(limit) = self.limit else {
            return Ok(None);
        };

        let current = self.refresh_native()?;
        let previous = self.reserved.fetch_add(bytes, Ordering::AcqRel);

        if current.saturating_add(previous).saturating_add(bytes) >= limit {
            self.reserved.fetch_sub(bytes, Ordering::AcqRel);

            return Err(size_error());
        }

        Ok(Some(Reservation {
            _ticket: Arc::new(ReservationTicket {
                budget: self.clone(),
                bytes,
            }),
        }))
    }
}

// Clones share one ticket: reserved bytes are released by its final owner.
#[derive(Clone)]
pub(super) struct Reservation {
    _ticket: Arc<ReservationTicket>,
}

struct ReservationTicket {
    budget: Arc<DiskBudget>,
    bytes: u64,
}

impl Drop for ReservationTicket {
    fn drop(&mut self) {
        self.budget.reserved.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

pub(super) fn size_error() -> Error {
    Error::new(
        ErrorKind::SizeLimit,
        "index size retention target has no safe disk headroom; retry after retention or increase the \
            target; an unreclaimable DuckDB file may require an offline rebuild",
    )
}

pub(super) struct SizeWork<'a> {
    pub(super) gate: &'a Arc<Mutex<ArchiveLeases>>,
    pub(super) rows: usize,
    pub(super) budget: &'a MaintenanceBudget,
}

fn checkpoint(writer: &mpsc::Sender<Write>, work: &SizeWork<'_>) -> Result<()> {
    let (reply, receiver) = oneshot::channel();
    maintenance_send(
        writer,
        Write::Checkpoint(work.budget.clone(), reply),
        work.budget,
    )?;
    maintenance_reply(receiver, work.budget)
}

fn expire(
    conn: &mut Connection,
    archives: &Path,
    writer: &mpsc::Sender<Write>,
    cutoff: i64,
    work: &SizeWork<'_>,
) -> Result<usize> {
    let expired = expired_id(
        conn,
        archives,
        cutoff,
        &work.budget.cancelled,
        work.budget.deadline,
    )?;
    let (reply, receiver) = oneshot::channel();
    maintenance_send(
        writer,
        Write::Retain(cutoff, expired, work.budget.clone(), reply),
        work.budget,
    )?;
    maintenance_reply(receiver, work.budget)
}

/// Expire sealed files in event-time order. A pinned generation stops further
/// eviction rather than deleting newer data while the original bytes remain.
pub(super) fn enforce_size(
    conn: &mut Connection,
    archives: &Path,
    budget: &Arc<DiskBudget>,
    writer: &mpsc::Sender<Write>,
    work: SizeWork<'_>,
) -> Result<usize> {
    let Some(limit) = budget.limit else {
        return Ok(0);
    };

    cleanup_archives(work.gate, archives);
    let mut total = budget.audit()?;

    if total < limit {
        return Ok(0);
    }

    // Reclaim WAL before expiring data. CHECKPOINT may refuse active readers;
    // that is retryable, while an engine integrity failure remains fatal.
    if let Err(error) = checkpoint(writer, &work)
        && matches!(error.kind, ErrorKind::Integrity | ErrorKind::Timeout)
    {
        return Err(error);
    }
    total = budget.audit()?;

    if total < limit {
        return Ok(0);
    }

    let mut expired_rows = 0usize;

    for _ in 0..32 {
        work.budget.check()?;
        let oldest: Option<i64> = conn
            .query_row(
                "SELECT max_time FROM archives ORDER BY min_time,max_time,path LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(db_error)?;
        let Some(last) = oldest else {
            break;
        };

        let cutoff = last.checked_add(1).ok_or_else(size_error)?;
        let archived: i64 = conn
            .query_row(
                "SELECT coalesce(sum(row_count),0) FROM archives WHERE max_time < ?",
                params![cutoff],
                |row| row.get(0),
            )
            .map_err(db_error)?;
        expired_rows = expired_rows
            .saturating_add(expire(conn, archives, writer, cutoff, &work)?)
            .saturating_add(archived as usize);

        cleanup_archives(work.gate, archives);
        let previous = total;
        total = budget.audit()?;

        if total < limit {
            return Ok(expired_rows);
        }

        if maintenance_gate(work.gate, work.budget)?.pending_count() > 0 && total >= previous {
            return Err(size_error());
        }

        // WAL from manifest mutation can outweigh a tiny expired Parquet file.
        // Plain CHECKPOINT may refuse active readers; never force away snapshots.
        if let Err(error) = checkpoint(writer, &work)
            && matches!(error.kind, ErrorKind::Integrity | ErrorKind::Timeout)
        {
            return Err(error);
        }
        total = budget.audit()?;

        if total < limit {
            return Ok(expired_rows);
        }
    }

    if maintenance_gate(work.gate, work.budget)?.pending_count() > 0 {
        return Err(size_error());
    }

    let sealed: i64 = conn
        .query_row("SELECT count(*) FROM archives", [], |row| row.get(0))
        .map_err(db_error)?;

    if sealed > 0 || budget.other.load(Ordering::Acquire) >= limit {
        return Err(size_error());
    }

    if maintenance_gate(work.gate, work.budget)?.has_readers() {
        return Err(size_error());
    }

    if !budget.hot_attempted.swap(true, Ordering::AcqRel) {
        work.budget.check()?;
        let oldest_batch = format!(
            "SELECT max(event_time) FROM \
             (SELECT event_time FROM events ORDER BY event_time,id LIMIT {})",
            work.rows
        );
        let last: Option<i64> = conn
            .query_row(&oldest_batch, [], |row| row.get(0))
            .map_err(db_error)?;

        if let Some(last) = last {
            let cutoff = last.checked_add(1).ok_or_else(size_error)?;
            expired_rows =
                expired_rows.saturating_add(expire(conn, archives, writer, cutoff, &work)?);

            if let Err(error) = checkpoint(writer, &work)
                && matches!(error.kind, ErrorKind::Integrity | ErrorKind::Timeout)
            {
                return Err(error);
            }

            cleanup_archives(work.gate, archives);
            total = budget.audit()?;

            if total < limit {
                return Ok(expired_rows);
            }
        }
    }

    Err(size_error())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footprint_includes_native_archive_temporary_and_unknown_files() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join("archives")).unwrap();
        std::fs::create_dir_all(directory.path().join("temp/nested")).unwrap();

        for (name, bytes) in [
            ("events.duckdb", 10),
            ("events.duckdb.wal", 20),
            ("archives/a.parquet", 30),
            ("archives/staging.tmp", 40),
            ("temp/nested/spill", 50),
            ("unmanaged", 60),
        ] {
            std::fs::write(directory.path().join(name), vec![0; bytes]).unwrap();
        }

        let result = footprint(directory.path()).unwrap();

        assert_eq!(result.database, 10);
        assert_eq!(result.wal, 20);
        assert_eq!(result.archives, 30);
        assert_eq!(result.temporary, 90);
        assert_eq!(result.other, 60);
        assert_eq!(result.total(), 210);
    }

    #[test]
    fn measured_progress_resets_hot_reclaim_attempt_and_reservations_release() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("events.duckdb");
        std::fs::write(&file, vec![0; 100]).unwrap();
        let budget = DiskBudget::new(directory.path().to_owned(), Some(1000)).unwrap();
        budget.hot_attempted.store(true, Ordering::Release);
        std::fs::write(&file, vec![0; 90]).unwrap();
        budget.audit().unwrap();

        assert!(!budget.hot_attempted.load(Ordering::Acquire));

        let reservation = budget.reserve(500).unwrap();

        assert_eq!(budget.reserved.load(Ordering::Acquire), 500);
        assert!(budget.reserve(500).is_err());

        drop(reservation);

        assert_eq!(budget.reserved.load(Ordering::Acquire), 0);
    }
}
