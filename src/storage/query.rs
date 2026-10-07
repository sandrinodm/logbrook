//! Snapshot registration, filtered scans, and bounded query result handling.
use super::*;

pub(super) fn dataset(conn: &Connection, dir: &Path, q: &Query) -> Result<String> {
    dataset_after(conn, dir, q, None, None)
}

pub(super) fn dataset_after(
    conn: &Connection,
    dir: &Path,
    q: &Query,
    after: Option<i64>,
    cancellation: Option<QueryDeadline<'_>>,
) -> Result<String> {
    let mut stmt = conn
        .prepare(
            "SELECT path,file_bytes FROM archives WHERE min_time < ? AND max_time >= ? \
             AND (? IS NULL OR max_id IS NULL OR max_id > ?) ORDER BY path",
        )
        .map_err(db_error)?;
    let paths = stmt
        .query_map(params![q.to, q.from, after, after], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<u64>>(1)?))
        })
        .map_err(db_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(db_error)?;

    let columns =
        "id,source,event_time,received_at,level,service,logger,host,pid,message,attributes";
    let mut sql = format!("SELECT {columns} FROM events");
    let mut files = Vec::new();

    for (path, expected_bytes) in paths {
        if let Some(check) = cancellation {
            check_cancelled(check.cancelled, check.at)?;
        }

        validate_archive_name(&path)?;
        let path = dir.join(path);

        if !path.is_file() {
            return Err(Error::new(
                ErrorKind::Integrity,
                format!("missing archive: {}", path.display()),
            ));
        }

        if expected_bytes
            .is_some_and(|bytes| std::fs::metadata(&path).map_or(true, |m| m.len() != bytes))
        {
            return Err(Error::new(
                ErrorKind::Integrity,
                "committed archive byte size mismatch",
            ));
        }

        files.push(format!("'{}'", quote(&path.to_string_lossy())));
    }

    if !files.is_empty() {
        // The empty hot-table branch supplies nullable columns absent from
        // legacy archives; name-based union keeps their field order irrelevant.
        sql.push_str(&format!(
            " UNION ALL SELECT {columns} FROM (SELECT {columns} FROM events WHERE false \
             UNION ALL BY NAME SELECT * FROM read_parquet([{}], union_by_name=true)) archive_schema",
            files.join(",")
        ));
    }

    Ok(sql)
}

pub(super) fn filters(q: &Query, high: i64) -> (String, Vec<Value>) {
    let mut sql = "event_time >= ? AND event_time < ? AND id <= ?".to_string();
    let mut values = vec![
        Value::BigInt(q.from),
        Value::BigInt(q.to),
        Value::BigInt(high),
    ];

    if !q.sources.is_empty() {
        sql.push_str(&format!(
            " AND source IN ({})",
            vec!["?"; q.sources.len()].join(",")
        ));
        values.extend(q.sources.iter().cloned().map(Value::Text));
    }

    for (field, value) in [
        ("service", &q.service),
        ("logger", &q.logger),
        ("host", &q.host),
    ] {
        if let Some(value) = value {
            sql.push_str(&format!(" AND {field} = ?"));
            values.push(Value::Text(value.clone()));
        }
    }

    if let Some(level) = q.min_level {
        sql.push_str(" AND level >= ?");
        values.push(Value::Int(level));
    }

    if let Some(message) = &q.message {
        sql.push_str(" AND contains(message, ?)");
        values.push(Value::Text(message.clone()));
    }

    (sql, values)
}

#[derive(Clone, Copy)]
pub(super) struct QueryDeadline<'a> {
    pub(super) cancelled: &'a AtomicBool,
    pub(super) at: Instant,
}

pub(super) fn run_query(
    conn: &mut Connection,
    dir: &Path,
    q: &Query,
    op: Operation,
    max_bytes: usize,
    gate: &Arc<Mutex<ArchiveLeases>>,
    cancellation: QueryDeadline<'_>,
) -> Result<Answer> {
    let cancelled = cancellation.cancelled;
    let deadline = cancellation.at;
    let mut budget = ResultBudget {
        remaining: max_bytes,
        deadline: Some(deadline),
        cancelled: Some(cancelled),
    };

    if q.from >= q.to || q.limit == 0 || q.limit > crate::model::MAX_PAGE_SIZE {
        return Err(Error::invalid("invalid time range or page limit"));
    }

    check_cancelled(cancelled, deadline)?;

    // Snapshot creation and reader registration share the publication gate, so
    // retired archive files stay available for the lifetime of this transaction.
    let mut registration = gate.lock().unwrap();
    let _lease;
    let tx = conn.transaction().map_err(db_error)?;
    let cutoff: i64 = tx
        .query_row(
            "SELECT before_time FROM retention_state WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .map_err(db_error)?;
    _lease = registration.register(gate);

    drop(registration);

    let anchor = q.from;
    let mut bounded = q.clone();
    bounded.from = bounded.from.max(cutoff);
    let q = &bounded;
    let newest: i64 = tx
        .query_row(
            "SELECT next_id-1 FROM ingest_state WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .map_err(db_error)?;

    let cursor = q
        .cursor
        .as_deref()
        .map(|s| {
            let parts = s
                .split(':')
                .map(str::parse::<i64>)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|_| Error::invalid("invalid cursor"))?;

            if parts.len() != 3 {
                return Err(Error::invalid("invalid cursor"));
            }

            Ok((parts[0], parts[1], parts[2]))
        })
        .transpose()?;

    if cursor.is_some_and(|(_, time, _)| time < cutoff) {
        return Err(Error::new(
            ErrorKind::CursorExpired,
            "cursor expired by retention",
        ));
    }

    if cursor.is_some_and(|(high, _, id)| high > newest || high < 0 || id <= 0 || id > high) {
        return Err(Error::invalid("invalid cursor high-water mark"));
    }

    if cursor.is_some_and(|(_, time, _)| time < q.from || time >= q.to) {
        return Err(Error::invalid("cursor outside query time range"));
    }

    if let Operation::Tail(Some(after), _) = &op {
        let expired: i64 = tx
            .query_row(
                "SELECT expired_through FROM retention_state WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .map_err(db_error)?;

        if *after < expired {
            return Err(Error::new(
                ErrorKind::CursorExpired,
                "tail cursor expired by retention",
            ));
        }

        if *after < 0 || *after > newest {
            return Err(Error::invalid("invalid tail cursor"));
        }
    }

    let high = cursor.map_or(newest, |c| c.0);
    check_cancelled(cancelled, deadline)?;
    let after = if let Operation::Tail(after, _) = &op {
        *after
    } else {
        None
    };

    let data = dataset_after(&tx, dir, q, after, Some(cancellation))?;
    check_cancelled(cancelled, deadline)?;
    let (mut filter, mut values) = filters(q, high);
    let answer = match op {
        Operation::Search => {
            if let Some((_, time, id)) = cursor {
                filter.push_str(" AND (event_time < ? OR (event_time = ? AND id < ?))");
                values.extend([Value::BigInt(time), Value::BigInt(time), Value::BigInt(id)]);
            }

            let sql = format!(
                "SELECT * FROM ({data}) retained WHERE {filter} ORDER BY event_time DESC,id DESC LIMIT {}",
                q.limit + 1
            );
            let mut stmt = tx.prepare(&sql).map_err(db_error)?;
            let mut rows = stmt
                .query_map(params_from_iter(values), map_event)
                .map_err(db_error)?;
            let events = budget.collect(rows.by_ref().take(q.limit))?;
            let more = rows.next().transpose().map_err(db_error)?.is_some();
            let next_cursor = if more {
                events
                    .last()
                    .map(|e| format!("{high}:{}:{}", e.event_time, e.id))
            } else {
                None
            };

            Answer::Search(SearchResult {
                events,
                next_cursor,
            })
        }
        Operation::Tail(after, extra_frame_bytes) => {
            if let Some(after) = after {
                filter.push_str(" AND id > ?");
                values.push(Value::BigInt(after));
                let mut stmt = tx
                    .prepare(&format!(
                        "SELECT * FROM ({data}) retained WHERE {filter} ORDER BY id ASC LIMIT {}",
                        q.limit
                    ))
                    .map_err(db_error)?;
                let rows = stmt
                    .query_map(params_from_iter(values), map_event)
                    .map_err(db_error)?;
                let mut events = Vec::new();
                let mut remaining = max_bytes;

                for row in rows {
                    check_cancelled(cancelled, deadline)?;
                    let event = row.map_err(db_error)?;
                    let mut counter = ResultBudget {
                        remaining: usize::MAX,
                        deadline: Some(deadline),
                        cancelled: Some(cancelled),
                    };
                    serde_json::to_writer(&mut counter, &event).map_err(db_error)?;
                    let cost = (usize::MAX - counter.remaining)
                        .saturating_add(event.id.len() + 32 + extra_frame_bytes);

                    if cost > remaining {
                        if events.is_empty() {
                            return Err(Error::new(
                                ErrorKind::TooLarge,
                                "tail event exceeds output byte budget",
                            ));
                        }
                        break;
                    }
                    remaining -= cost;
                    events.push(event);
                }

                let next_cursor = Some(
                    events
                        .last()
                        .map_or_else(|| high.to_string(), |e| e.id.clone()),
                );

                Answer::Search(SearchResult {
                    events,
                    next_cursor,
                })
            } else {
                Answer::Search(SearchResult {
                    events: vec![],
                    next_cursor: Some(high.to_string()),
                })
            }
        }
        Operation::Count => Answer::Count(
            tx.query_row(
                &format!("SELECT count(*) FROM ({data}) retained WHERE {filter}"),
                params_from_iter(values),
                |r| r.get::<_, i64>(0),
            )
            .map_err(db_error)? as u64,
        ),
        Operation::Facets => {
            // GROUPING_ID identifies the retained facet; rank each one separately
            // so a frequent source cannot crowd out service, logger, or host values.
            let sql = format!(
                "WITH grouped AS (SELECT service,logger,host,level,source,count(*) n, \
                 grouping_id(service,logger,host,level,source) g \
                 FROM ({data}) retained WHERE {filter} \
                 GROUP BY GROUPING SETS ((service),(logger),(host),(level),(source))), \
                 ranked AS (SELECT *, \
                 row_number() OVER(PARTITION BY g ORDER BY n DESC, service,logger,host,level,source) rank \
                 FROM grouped WHERE CASE g \
                 WHEN 15 THEN service IS NOT NULL \
                 WHEN 23 THEN logger IS NOT NULL \
                 WHEN 27 THEN host IS NOT NULL \
                 WHEN 29 THEN level IS NOT NULL \
                 WHEN 30 THEN source IS NOT NULL END) \
                 SELECT g, CASE g \
                 WHEN 15 THEN service WHEN 23 THEN logger WHEN 27 THEN host \
                 WHEN 29 THEN CAST(level AS VARCHAR) WHEN 30 THEN source END,n \
                 FROM ranked WHERE rank <= 100 ORDER BY g,rank"
            );
            check_cancelled(cancelled, deadline)?;
            let mut stmt = tx.prepare(&sql).map_err(db_error)?;
            let rows = stmt
                .query_map(params_from_iter(values), |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        FacetValue {
                            value: r.get(1)?,
                            count: r.get::<_, i64>(2)? as u64,
                        },
                    ))
                })
                .map_err(db_error)?;
            let mut result = Facets {
                services: vec![],
                loggers: vec![],
                hosts: vec![],
                levels: vec![],
                sources: vec![],
            };

            for row in rows {
                check_cancelled(cancelled, deadline)?;
                let (group, value) = row.map_err(db_error)?;
                serde_json::to_writer(&mut budget, &value).map_err(|_| {
                    Error::new(ErrorKind::TooLarge, "query response exceeds byte budget")
                })?;

                match group {
                    15 => result.services.push(value),
                    23 => result.loggers.push(value),
                    27 => result.hosts.push(value),
                    29 => result.levels.push(value),
                    30 => result.sources.push(value),
                    _ => unreachable!(),
                }
            }

            Answer::Facets(result)
        }
        #[cfg(test)]
        Operation::Expensive => Answer::Count(
            tx.query_row(
                "SELECT sum(i)::BIGINT FROM range(10000000000) t(i)",
                [],
                |r| r.get::<_, i64>(0),
            )
            .map_err(db_error)? as u64,
        ),
        Operation::Stop => unreachable!(),
        Operation::Histogram(interval) => {
            if (q.to as i128 - q.from as i128 + interval as i128 - 1) / interval as i128
                > crate::model::MAX_HISTOGRAM_BUCKETS as i128
            {
                return Err(Error::invalid("too many histogram buckets"));
            }

            // Widen before subtracting the anchor so extreme timestamps remain
            // exact and cannot overflow BIGINT during bucket arithmetic.
            let mut stmt = tx
                .prepare(&format!(
                    "SELECT CAST(((event_time::HUGEINT-({})) // {interval})*{interval}+{} \
                     AS BIGINT),count(*) FROM ({data}) retained WHERE {filter} \
                     GROUP BY 1 ORDER BY 1",
                    anchor, anchor
                ))
                .map_err(db_error)?;
            let buckets = budget.collect(
                stmt.query_map(params_from_iter(values), |r| {
                    Ok(HistogramBucket {
                        time: r.get(0)?,
                        count: r.get::<_, i64>(1)? as u64,
                    })
                })
                .map_err(db_error)?,
            )?;

            Answer::Histogram(buckets)
        }
    };

    check_cancelled(cancelled, deadline)?;
    tx.commit().map_err(db_error)?;

    Ok(answer)
}

pub(super) fn map_event(r: &duckdb::Row<'_>) -> duckdb::Result<Event> {
    let attrs: String = r.get(10)?;

    Ok(Event {
        id: r.get::<_, i64>(0)?.to_string(),
        source: r.get(1)?,
        event_time: r.get(2)?,
        received_at: r.get(3)?,
        level: r.get(4)?,
        service: r.get(5)?,
        logger: r.get(6)?,
        host: r.get(7)?,
        pid: r.get(8)?,
        message: r.get(9)?,
        attributes: serde_json::from_str(&attrs).map_err(|e| {
            duckdb::Error::FromSqlConversionFailure(10, duckdb::types::Type::Text, Box::new(e))
        })?,
    })
}

// Count JSON bytes without allocating a second serialized copy of each result.
pub(super) struct ResultBudget<'a> {
    pub(super) remaining: usize,
    pub(super) deadline: Option<Instant>,
    pub(super) cancelled: Option<&'a AtomicBool>,
}

impl std::io::Write for ResultBudget<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(std::io::Error::other("query response exceeds byte budget"));
        }

        self.remaining -= bytes.len();

        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ResultBudget<'_> {
    fn collect<T: serde::Serialize>(
        &mut self,
        rows: impl Iterator<Item = duckdb::Result<T>>,
    ) -> Result<Vec<T>> {
        let mut result = Vec::new();

        for row in rows {
            if let (Some(cancelled), Some(deadline)) = (self.cancelled, self.deadline) {
                check_cancelled(cancelled, deadline)?;
            }

            let row = row.map_err(db_error)?;
            serde_json::to_writer(&mut *self, &row).map_err(|_| {
                Error::new(ErrorKind::TooLarge, "query response exceeds byte budget")
            })?;
            result.push(row);
        }

        Ok(result)
    }
}
