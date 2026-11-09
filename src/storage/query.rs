//! Snapshot registration, filtered scans, and bounded query result handling.

use super::{
    ProjectionStatus, Result, db_error,
    leases::ArchiveLeases,
    projection, projection_integrity, quote,
    readers::{Answer, Operation, check_cancelled},
    validate_archive_name,
};
use crate::model::{
    Error, ErrorKind, Event, FacetValue, Facets, HistogramBucket, Query, SearchResult,
};
use duckdb::{Connection, params, params_from_iter, types::Value};
use std::{
    path::Path,
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::Instant,
};

use crate::attribute_filter::{AttributeCondition, AttributeValue};
use crate::query_cursor::{SearchPosition, TailCursor};
use duckdb::Transaction;
use projection::ORIGINAL_COLUMNS;

/// Message for a query whose response, after serialization, would exceed its byte budget.
pub(super) const RESPONSE_BUDGET_MESSAGE: &str = "query response exceeds byte budget";

/// Message for a tail checkpoint token that does not fit the byte budget.
const TAIL_CHECKPOINT_BUDGET_MESSAGE: &str = "tail checkpoint exceeds output byte budget";

/// Fixed JSON framing reserved for one tail checkpoint in a response.
const TAIL_CHECKPOINT_FRAME_BYTES: usize = 36;

/// Fixed JSON framing reserved for one tail event in a response.
const TAIL_EVENT_FRAME_BYTES: usize = 24;

/// The newest archive schema that has neither projection columns nor a digest.
const LAST_LEGACY_ARCHIVE_SCHEMA: i32 = 3;

/// The archive schema that stores projection columns and their digest.
const PROJECTION_ARCHIVE_SCHEMA: i32 = 4;

/// The facet query's `grouping_id(service,logger,host,level,source)` for the `(service)` set:
/// one bit per column left out of the set, the first column highest, so 0b01111.
const FACET_SERVICE_GROUP: i64 = 15;

/// The facet query's grouping id for the `(logger)` grouping set: 0b10111.
const FACET_LOGGER_GROUP: i64 = 23;

/// The facet query's grouping id for the `(host)` grouping set: 0b11011.
const FACET_HOST_GROUP: i64 = 27;

/// The facet query's grouping id for the `(level)` grouping set: 0b11101.
const FACET_LEVEL_GROUP: i64 = 29;

/// The facet query's grouping id for the `(source)` grouping set: 0b11110.
const FACET_SOURCE_GROUP: i64 = 30;

/// Attribute JSON's position in the original durable event-column order.
const ATTRIBUTES_COLUMN_INDEX: usize = 10;

/// The error for a stored event id that cannot be parsed back into a cursor position.
fn invalid_event_id() -> Error {
    Error::invalid("invalid stored event ID")
}

/// The error for a response that exceeds its byte budget.
pub(super) fn response_too_large() -> Error {
    Error::new(ErrorKind::TooLarge, RESPONSE_BUDGET_MESSAGE)
}

pub(super) struct SelectedDataset {
    pub(super) sql: String,
    pub(super) integrity: Vec<projection_integrity::ArchiveIntegrity>,
}

#[cfg(test)]
pub(super) fn dataset(conn: &Connection, dir: &Path, q: &Query) -> Result<String> {
    dataset_after(conn, dir, q, None, None)
}

#[cfg(test)]
pub(super) fn dataset_after(
    conn: &Connection,
    dir: &Path,
    q: &Query,
    after: Option<i64>,
    cancellation: Option<QueryDeadline<'_>>,
) -> Result<String> {
    let selected = selected_dataset(conn, dir, q, after, cancellation)?;
    Ok(format!("SELECT {ORIGINAL_COLUMNS} FROM ({})", selected.sql))
}

pub(super) fn selected_dataset(
    conn: &Connection,
    dir: &Path,
    q: &Query,
    after: Option<i64>,
    cancellation: Option<QueryDeadline<'_>>,
) -> Result<SelectedDataset> {
    let layout = projection::ProjectionLayout::load(conn)?;
    selected_dataset_with_layout(conn, dir, q, after, cancellation, layout.as_ref())
}

fn selected_dataset_with_layout(
    conn: &Connection,
    dir: &Path,
    q: &Query,
    after: Option<i64>,
    cancellation: Option<QueryDeadline<'_>>,
    layout: Option<&projection::ProjectionLayout>,
) -> Result<SelectedDataset> {
    let digest_column = if layout.is_some() {
        "projection_sha256"
    } else {
        "NULL::VARCHAR"
    };
    let archives = archive_manifest(conn, q, after, digest_column)?;

    let extra = layout
        .map(|l| format!(",{}", l.extra_column_names().join(",")))
        .unwrap_or_default();
    let columns = format!("{ORIGINAL_COLUMNS}{extra}");
    let mut sql = format!("SELECT {columns} FROM events");
    let mut files = Vec::new();
    let mut integrity = Vec::new();
    for archive in archives {
        if let Some(check) = cancellation {
            check_cancelled(check.cancelled, check.at)?;
        }

        let checked = check_archive(conn, dir, layout, archive, cancellation)?;
        files.push(checked.quoted);
        if let Some(file) = checked.integrity {
            integrity.push(file);
        }
    }

    if !files.is_empty() {
        sql.push_str(&archive_union(&columns, layout, &files));
    }

    Ok(SelectedDataset { sql, integrity })
}

/// One committed archive that the manifest selects for a query.
struct ArchiveEntry {
    path: String,
    file_bytes: Option<u64>,
    schema_version: i32,
    projection_digest: Option<String>,
}

/// Lists the archives whose time and ID ranges can hold rows for `q`, in path order.
fn archive_manifest(
    conn: &Connection,
    q: &Query,
    after: Option<i64>,
    digest_column: &str,
) -> Result<Vec<ArchiveEntry>> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT path,file_bytes,coalesce(schema_version,1),{digest_column} FROM archives \
         WHERE min_time < ? AND max_time >= ? \
         AND (? IS NULL OR max_id IS NULL OR max_id > ?) ORDER BY path"
        ))
        .map_err(db_error)?;
    stmt.query_map(params![q.to, q.from, after, after], |r| {
        Ok(ArchiveEntry {
            path: r.get(0)?,
            file_bytes: r.get(1)?,
            schema_version: r.get(2)?,
            projection_digest: r.get(3)?,
        })
    })
    .map_err(db_error)?
    .collect::<std::result::Result<Vec<_>, _>>()
    .map_err(db_error)
}

/// An archive that passed its checks: its quoted path for the union, and the integrity record that
/// the read verifies once the answer is built, when the archive has one.
struct CheckedArchive {
    quoted: String,
    integrity: Option<projection_integrity::ArchiveIntegrity>,
}

/// Checks one archive against the filesystem, its manifest entry and the projection layout.
fn check_archive(
    conn: &Connection,
    dir: &Path,
    layout: Option<&projection::ProjectionLayout>,
    archive: ArchiveEntry,
    cancellation: Option<QueryDeadline<'_>>,
) -> Result<CheckedArchive> {
    validate_archive_name(&archive.path)?;

    let path = dir.join(archive.path);
    if !path.is_file() {
        return Err(projection_integrity::integrity(format!(
            "missing archive: {}",
            path.display()
        )));
    }

    if archive
        .file_bytes
        .is_some_and(|bytes| std::fs::metadata(&path).map_or(true, |m| m.len() != bytes))
    {
        return Err(projection_integrity::integrity(
            "committed archive byte size mismatch",
        ));
    }

    let quoted = format!("'{}'", quote(&path.to_string_lossy()));
    let integrity = match (archive.schema_version, layout) {
        (1..=LAST_LEGACY_ARCHIVE_SCHEMA, _) if archive.projection_digest.is_none() => {
            if layout.is_some() {
                projection_integrity::validate_legacy_schema(conn, &path, cancellation)?;
            }

            None
        }
        (PROJECTION_ARCHIVE_SCHEMA, Some(layout)) => {
            let file = projection_integrity::ArchiveIntegrity::new(
                path.clone(),
                archive.projection_digest,
            )?;
            file.verify(cancellation)?;
            projection_integrity::validate_schema(conn, &path, layout, cancellation)?;
            Some(file)
        }
        _ => {
            return Err(projection_integrity::integrity(
                "unsupported archive schema/layout metadata",
            ));
        }
    };
    Ok(CheckedArchive { quoted, integrity })
}

/// The SQL that appends the archive files to the hot rows, in the manifest's path order.
fn archive_union(
    columns: &str,
    layout: Option<&projection::ProjectionLayout>,
    files: &[String],
) -> String {
    let normalized_extra = layout
        .map(|l| {
            format!(
                ",coalesce(pa_covered,false) AS pa_covered,{}",
                l.slot_names().join(",")
            )
        })
        .unwrap_or_default();
    // Preserve the manifest's existing path order. The empty hot branch
    // supplies nullable legacy columns, including the projection slots.
    format!(
        " UNION ALL SELECT {ORIGINAL_COLUMNS}{normalized_extra} FROM \
         (SELECT {columns} FROM events WHERE false \
         UNION ALL BY NAME SELECT * FROM read_parquet([{}], union_by_name=true)) archive_schema",
        files.join(",")
    )
}

pub(super) fn filters(q: &Query, high: i64) -> (String, Vec<Value>) {
    filters_with_projection(q, high, None)
}

/// A WHERE clause under construction, with the value bound to each `?` in order.
struct Clause {
    sql: String,
    values: Vec<Value>,
}

/// Builds the WHERE clause for `q`. With a projection layout, string conditions on promoted keys
/// compare their slot columns; every other attribute condition reads the JSON attributes.
fn filters_with_projection(
    q: &Query,
    high: i64,
    layout: Option<&projection::ProjectionLayout>,
) -> (String, Vec<Value>) {
    let mut clause = Clause {
        sql: "event_time >= ? AND event_time < ? AND id <= ?".to_owned(),
        values: vec![
            Value::BigInt(q.from),
            Value::BigInt(q.to),
            Value::BigInt(high),
        ],
    };
    push_column_filters(&mut clause, q);
    let remaining = push_projected_attributes(&mut clause, q.attributes.conditions(), layout);
    push_attribute_filters(&mut clause, &remaining);
    (clause.sql, clause.values)
}

/// Appends the filters on event columns: sources, service, logger, host, level and message.
fn push_column_filters(clause: &mut Clause, q: &Query) {
    if !q.sources.is_empty() {
        clause.sql.push_str(&format!(
            " AND source IN ({})",
            vec!["?"; q.sources.len()].join(",")
        ));
        clause
            .values
            .extend(q.sources.iter().cloned().map(Value::Text));
    }

    for (field, value) in [
        ("service", &q.service),
        ("logger", &q.logger),
        ("host", &q.host),
    ] {
        if let Some(value) = value {
            clause.sql.push_str(&format!(" AND {field} = ?"));
            clause.values.push(Value::Text(value.clone()));
        }
    }

    if let Some(level) = q.min_level {
        clause.sql.push_str(" AND level >= ?");
        clause.values.push(Value::Int(level));
    }

    if let Some(message) = &q.message {
        clause.sql.push_str(" AND contains(message, ?)");
        clause.values.push(Value::Text(message.clone()));
    }
}

/// Appends the string conditions on promoted keys as slot comparisons, and returns the remaining
/// conditions in their original order.
fn push_projected_attributes<'a>(
    clause: &mut Clause,
    conditions: &'a [AttributeCondition],
    layout: Option<&projection::ProjectionLayout>,
) -> Vec<&'a AttributeCondition> {
    conditions
        .iter()
        .filter(|condition| {
            if let (Some(layout), AttributeValue::String(value)) = (layout, condition.value())
                && let Some(slot) = layout.keys().iter().position(|key| key == condition.key())
            {
                clause.sql.push_str(&format!(" AND pa_{slot} = ?"));
                clause.values.push(Value::Text(value.clone()));
                return false;
            }

            true
        })
        .collect()
}

/// Appends the attribute conditions that did not use a slot.
fn push_attribute_filters(clause: &mut Clause, remaining: &[&AttributeCondition]) {
    let strings: Vec<&AttributeCondition> = remaining
        .iter()
        .copied()
        .filter(|condition| matches!(condition.value(), AttributeValue::String(_)))
        .collect();

    if !remaining.is_empty() && strings.is_empty() {
        // A pointer such as /0 must never select an element of a root array.
        clause.sql.push_str(" AND json_type(attributes) = 'OBJECT'");
    }

    if let [first, second] = strings.as_slice() {
        push_string_pair(clause, first, second, &strings);
    }

    let mut root_guarded = false;
    for condition in remaining {
        let (predicate, expected) = match condition.value() {
            AttributeValue::String(value) => {
                // A pair of string conditions was already compared together above.
                if strings.len() != 2 {
                    push_string_match(clause, condition, value, &mut root_guarded);
                }

                continue;
            }
            AttributeValue::Boolean(value) => (
                "json_type(attributes, ?) = 'BOOLEAN' \
                 AND TRY_CAST(json_extract_string(attributes, ?) AS BOOLEAN) = ?"
                    .to_owned(),
                Value::Boolean(*value),
            ),
            AttributeValue::Integer(value) => (integer_predicate(), Value::HugeInt(*value)),
            AttributeValue::Float(bits) => (
                "json_type(attributes, ?) = 'DOUBLE' \
                 AND TRY_CAST(json_extract_string(attributes, ?) AS DOUBLE) = ?"
                    .to_owned(),
                Value::Double(f64::from_bits(*bits)),
            ),
        };
        clause.sql.push_str(" AND (");
        clause.sql.push_str(&predicate);
        clause.sql.push(')');
        clause.values.extend(std::iter::repeat_n(
            Value::Text(condition.pointer()),
            predicate.matches('?').count() - 1,
        ));
        clause.values.push(expected);
    }
}

/// Compares exactly two string conditions in one CASE expression.
fn push_string_pair(
    clause: &mut Clause,
    first: &AttributeCondition,
    second: &AttributeCondition,
    strings: &[&AttributeCondition],
) {
    // Two paths share one JSON parse. Keep larger conjunctions scalar so a
    // selective condition can reject rows before extracting every path.
    clause.sql.push_str(
        " AND CASE WHEN json_extract_string(attributes, [?, ?]) = [?, ?] \
         THEN json_type(attributes) = 'OBJECT' \
         AND json_type(attributes, [?, ?]) = ['VARCHAR', 'VARCHAR'] ELSE false END",
    );
    clause
        .values
        .extend([Value::Text(first.pointer()), Value::Text(second.pointer())]);
    clause.values.extend(strings.iter().map(|condition| {
        let AttributeValue::String(value) = condition.value() else {
            unreachable!("string conditions carry string values")
        };
        Value::Text(value.clone())
    }));
    clause
        .values
        .extend([Value::Text(first.pointer()), Value::Text(second.pointer())]);
}

/// Appends one exact string match. The first match also carries the object guard that every string
/// match shares.
fn push_string_match(
    clause: &mut Clause,
    condition: &AttributeCondition,
    value: &str,
    root_guarded: &mut bool,
) {
    // Type/root checks are needed only for decoded-value matches.
    // One mandatory string CASE supplies the shared object guard.
    clause
        .sql
        .push_str(" AND CASE WHEN json_extract_string(attributes, ?) = ? THEN ");
    if !*root_guarded {
        clause.sql.push_str("json_type(attributes) = 'OBJECT' AND ");
        *root_guarded = true;
    }

    clause
        .sql
        .push_str("json_type(attributes, ?) = 'VARCHAR' ELSE false END");
    clause.values.extend([
        Value::Text(condition.pointer()),
        Value::Text(value.to_owned()),
        Value::Text(condition.pointer()),
    ]);
}

/// The predicate for an integer condition. A stored integer may be BIGINT, UBIGINT or a DOUBLE
/// with an integral value.
fn integer_predicate() -> String {
    // Never pass stored integers through DOUBLE. TRY_CAST is safe
    // even if the optimizer evaluates an expression before its guard.
    let double = "TRY_CAST(json_extract_string(attributes, ?) AS DOUBLE)";
    format!(
        "CASE json_type(attributes, ?) \
         WHEN 'BIGINT' THEN TRY_CAST(json_extract_string(attributes, ?) AS HUGEINT) \
         WHEN 'UBIGINT' THEN TRY_CAST(json_extract_string(attributes, ?) AS HUGEINT) \
         WHEN 'DOUBLE' THEN CASE WHEN isfinite({double}) \
         AND trunc({double}) = {double} \
         AND {double} >= -9223372036854775808.0::DOUBLE \
         AND {double} < 18446744073709551616.0::DOUBLE \
         THEN TRY_CAST({double} AS HUGEINT) END END = ?"
    )
}

pub(super) fn projected_dataset(
    data: &str,
    q: &Query,
    high: i64,
    layout: &projection::ProjectionLayout,
) -> (String, Vec<Value>) {
    let (covered, mut values) = filters_with_projection(q, high, Some(layout));
    let (uncovered, fallback_values) = filters(q, high);
    values.extend(fallback_values);
    (
        format!(
            "SELECT {ORIGINAL_COLUMNS} FROM ({data}) covered WHERE pa_covered AND {covered} \
              UNION ALL SELECT {ORIGINAL_COLUMNS} FROM ({data}) uncovered \
             WHERE NOT pa_covered AND {uncovered}"
        ),
        values,
    )
}

#[derive(Clone, Copy)]
pub(super) struct QueryDeadline<'a> {
    pub(super) cancelled: &'a AtomicBool,
    pub(super) at: Instant,
}

pub(super) fn archive_upper_bound(to: i64, last_event_time: i64) -> i64 {
    // Manifest selection has an exclusive upper bound, while a continuation
    // still needs rows tied on time with a smaller ID. Saturation avoids an
    // overflow at the timestamp boundary and never widens the original range.
    to.min(last_event_time.saturating_add(1))
}

/// Only search pages reuse metadata preparation; other operations keep their original
/// preparation and layout-validation path.
#[derive(Clone, Copy)]
enum MetadataPreparation {
    Search,
    Uncached,
}

impl MetadataPreparation {
    fn for_operation(operation: &Operation) -> Self {
        if matches!(operation, Operation::Search) {
            Self::Search
        } else {
            Self::Uncached
        }
    }

    /// Executes one fixed, single-row metadata lookup against the current transaction.
    fn read_i64(self, conn: &Connection, sql: &str) -> Result<i64> {
        match self {
            Self::Search => conn
                .prepare_cached(sql)
                .map_err(db_error)?
                .query_row([], |r| r.get(0)),
            Self::Uncached => conn.query_row(sql, [], |r| r.get(0)),
        }
        .map_err(db_error)
    }
}

/// A read transaction and the archive lease that keeps its files alive while it runs.
struct ReadSnapshot<'conn> {
    tx: Transaction<'conn>,
    /// Rows and cursors older than this time have been removed by retention.
    cutoff: i64,
    metadata: MetadataPreparation,
    // Declared after `tx`, so the transaction ends before the lease is released.
    _lease: super::leases::ReadLease,
}

impl<'conn> ReadSnapshot<'conn> {
    /// Opens a snapshot and registers its reader under the publication gate.
    fn open(
        conn: &'conn mut Connection,
        gate: &Arc<Mutex<ArchiveLeases>>,
        metadata: MetadataPreparation,
    ) -> Result<Self> {
        // Snapshot creation and reader registration share the publication gate, so
        // retired archive files stay available for the lifetime of this transaction.
        let mut registration = gate.lock().unwrap();
        let tx = conn.transaction().map_err(db_error)?;
        let cutoff = metadata.read_i64(
            &tx,
            "SELECT before_time FROM retention_state WHERE singleton=1",
        )?;
        let lease = registration.register(gate);
        drop(registration);

        Ok(Self {
            tx,
            cutoff,
            metadata,
            _lease: lease,
        })
    }

    /// Ends the snapshot and keeps the reads it made.
    fn commit(self) -> Result<()> {
        self.tx.commit().map_err(db_error)
    }
}

/// What one read runs: the query with its start clipped at retention, the cursor it resumes from,
/// the snapshot's high-water mark, and the limits the read must respect.
struct Plan<'a> {
    /// The query as the caller sent it. Cursors are encoded from it, and histograms anchor on its
    /// start.
    original: &'a Query,
    /// The query with its start clipped to the retention cutoff.
    query: Query,
    cursor: Option<SearchPosition>,
    /// Events above this ID were ingested after the snapshot and are excluded.
    high: i64,
    max_bytes: usize,
    cancellation: QueryDeadline<'a>,
}

impl<'a> Plan<'a> {
    /// Decodes the cursor, clips the start to retention, and checks the cursor and the tail
    /// position against the snapshot.
    fn resolve(
        snapshot: &ReadSnapshot<'_>,
        original: &'a Query,
        op: &Operation,
        max_bytes: usize,
        cancellation: QueryDeadline<'a>,
    ) -> Result<Self> {
        let cursor = original
            .cursor
            .as_deref()
            .map(|token| crate::query_cursor::decode_search(original, token))
            .transpose()?;
        let mut query = original.clone();
        query.from = query.from.max(snapshot.cutoff);
        let newest = snapshot.metadata.read_i64(
            &snapshot.tx,
            "SELECT next_id-1 FROM ingest_state WHERE singleton=1",
        )?;

        if let Some(position) = cursor {
            check_search_cursor(position, snapshot.cutoff, newest, &query)?;
        }

        if let Operation::Tail(Some(after)) = op {
            check_tail_cursor(&snapshot.tx, *after, newest)?;
        }

        let high = cursor.map_or(newest, |position| position.high_water_id);
        Ok(Self {
            original,
            query,
            cursor,
            high,
            max_bytes,
            cancellation,
        })
    }

    /// Answers an initial tail request with the snapshot's high-water mark and no rows.
    fn initial_tail(&self, snapshot: ReadSnapshot<'_>) -> Result<Answer> {
        // Initial tail requests only publish the snapshot's high-water mark. They
        // never replay rows, so opening or validating archive files is unnecessary.
        let token = TailCursor::new(self.original)?.encode(self.high)?;
        if token.len().saturating_add(TAIL_CHECKPOINT_FRAME_BYTES) > self.max_bytes {
            return Err(Error::new(
                ErrorKind::TooLarge,
                TAIL_CHECKPOINT_BUDGET_MESSAGE,
            ));
        }

        self.check_live()?;
        snapshot.commit()?;
        Ok(Answer::Search(SearchResult {
            events: vec![],
            next_cursor: Some(token),
        }))
    }

    /// Fails once the read has been cancelled or its deadline has passed.
    fn check_live(&self) -> Result<()> {
        check_cancelled(self.cancellation.cancelled, self.cancellation.at)
    }
}

/// Rejects a search cursor that retention has expired, that points outside the snapshot, or that
/// falls outside the query's time range.
fn check_search_cursor(
    position: SearchPosition,
    cutoff: i64,
    newest: i64,
    query: &Query,
) -> Result<()> {
    if position.last_event_time < cutoff {
        return Err(Error::new(
            ErrorKind::CursorExpired,
            "cursor expired by retention",
        ));
    }

    let high_water_invalid = position.high_water_id > newest || position.high_water_id < 0;
    let last_event_invalid =
        position.last_event_id <= 0 || position.last_event_id > position.high_water_id;
    if high_water_invalid || last_event_invalid {
        return Err(Error::invalid("invalid cursor high-water mark"));
    }

    if position.last_event_time < query.from || position.last_event_time >= query.to {
        return Err(Error::invalid("cursor outside query time range"));
    }

    Ok(())
}

/// Rejects a tail position that is negative, past the snapshot, or expired by retention.
fn check_tail_cursor(tx: &Transaction<'_>, after: i64, newest: i64) -> Result<()> {
    if after < 0 || after > newest {
        return Err(Error::invalid("invalid tail cursor"));
    }

    let expired: i64 = tx
        .query_row(
            "SELECT expired_through FROM retention_state WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .map_err(db_error)?;
    if after < expired {
        return Err(Error::new(
            ErrorKind::CursorExpired,
            "tail cursor expired by retention",
        ));
    }

    Ok(())
}

/// The rows a read selects: the source they come from, the filter applied to them and its
/// parameters, and the archive records the read verifies when it finishes.
struct Dataset {
    selected: SelectedDataset,
    layout: Option<projection::ProjectionLayout>,
    data: String,
    filter: String,
    values: Vec<Value>,
}

impl Dataset {
    /// Selects the archives the read needs and builds the SQL that reads hot and archived rows.
    fn read(
        snapshot: &ReadSnapshot<'_>,
        dir: &Path,
        plan: &Plan<'_>,
        op: &Operation,
    ) -> Result<Self> {
        let after = if let Operation::Tail(after) = op {
            *after
        } else {
            None
        };

        let mut archive_query = plan.query.clone();
        if let (Operation::Search, Some(position)) = (op, plan.cursor) {
            archive_query.to = archive_upper_bound(plan.query.to, position.last_event_time);
        }

        let (selected, layout) = match snapshot.metadata {
            MetadataPreparation::Search => {
                // Archive selection and query projection share one validated mapping
                // in this transaction. Later search requests still execute every check.
                let layout = projection::ProjectionLayout::load_cached(&snapshot.tx)?;
                let selected = selected_dataset_with_layout(
                    &snapshot.tx,
                    dir,
                    &archive_query,
                    after,
                    Some(plan.cancellation),
                    layout.as_ref(),
                )?;
                plan.check_live()?;
                (selected, layout)
            }
            MetadataPreparation::Uncached => {
                // Preserve the original non-search metadata reads and their ordering.
                let selected = selected_dataset(
                    &snapshot.tx,
                    dir,
                    &archive_query,
                    after,
                    Some(plan.cancellation),
                )?;
                plan.check_live()?;
                let layout = projection::ProjectionLayout::load(&snapshot.tx)?;
                (selected, layout)
            }
        };
        let (data, filter, values) = if let Some(layout) = &layout {
            let (sql, values) = projected_dataset(&selected.sql, &plan.query, plan.high, layout);
            (sql, "true".to_owned(), values)
        } else {
            let (filter, values) = filters(&plan.query, plan.high);
            (selected.sql.clone(), filter, values)
        };
        Ok(Self {
            selected,
            layout,
            data,
            filter,
            values,
        })
    }
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
    q.attributes.validate(usize::MAX)?;
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

    let metadata = MetadataPreparation::for_operation(&op);
    let snapshot = ReadSnapshot::open(conn, gate, metadata)?;
    let plan = Plan::resolve(&snapshot, q, &op, max_bytes, cancellation)?;
    plan.check_live()?;

    // Keep the initial tail check after query/cursor validation and the transactional metadata
    // reads, and before any archive file is opened.
    if matches!(op, Operation::Tail(None)) {
        return plan.initial_tail(snapshot);
    }

    let mut dataset = Dataset::read(&snapshot, dir, &plan, &op)?;
    let answer = match op {
        Operation::Search => answer_search(&snapshot.tx, &plan, &mut dataset, &mut budget)?,
        Operation::Tail(Some(after)) => answer_tail(&snapshot.tx, &plan, &mut dataset, after)?,
        Operation::Tail(None) => unreachable!("initial tail answered before archive selection"),
        Operation::ProjectionStatus => projection_status(&snapshot.tx, &plan, &dataset)?,
        Operation::Count => count_matches(&snapshot.tx, &dataset)?,
        Operation::Facets => answer_facets(&snapshot.tx, &plan, &dataset, &mut budget)?,
        #[cfg(test)]
        Operation::ObserveNative(probe) => {
            let _ = probe.started.send(());
            let answer = expensive_query(&snapshot.tx);
            let _ = probe
                .completed
                .send(answer.as_ref().err().map(ToString::to_string));
            // Keep the worker's running slot and busy-reader count held until the test
            // has checked that abandoning its waiter cannot hand this reader out early.
            let _ = probe.release.recv();
            answer?
        }
        Operation::Stop => unreachable!("stop is answered by the reader loop"),
        Operation::Histogram(interval) => {
            answer_histogram(&snapshot.tx, &plan, &dataset, interval, &mut budget)?
        }
    };

    plan.check_live()?;
    projection_integrity::verify_all(&dataset.selected.integrity, Some(cancellation))?;
    snapshot.commit()?;

    Ok(answer)
}

#[cfg(test)]
fn expensive_query(tx: &Transaction<'_>) -> Result<Answer> {
    tx.query_row(
        "SELECT sum(i)::BIGINT FROM range(10000000000) t(i)",
        [],
        |r| r.get::<_, i64>(0),
    )
    .map(|count| Answer::Count(count as u64))
    .map_err(db_error)
}

/// Answers a search page: up to `limit` matching events, newest first, with a cursor when more
/// remain.
fn answer_search(
    tx: &Transaction<'_>,
    plan: &Plan<'_>,
    dataset: &mut Dataset,
    budget: &mut ResultBudget<'_>,
) -> Result<Answer> {
    let Dataset {
        data,
        filter,
        values,
        ..
    } = dataset;
    if let Some(position) = plan.cursor {
        let time = position.last_event_time;
        let id = position.last_event_id;
        filter.push_str(" AND (event_time < ? OR (event_time = ? AND id < ?))");
        values.extend([Value::BigInt(time), Value::BigInt(time), Value::BigInt(id)]);
    }

    let sql = format!(
        "SELECT * FROM ({data}) retained WHERE {filter} ORDER BY event_time DESC,id DESC LIMIT {}",
        plan.query.limit + 1
    );
    let mut stmt = tx.prepare(&sql).map_err(db_error)?;
    let mut rows = stmt
        .query_map(params_from_iter(values.iter()), map_event)
        .map_err(db_error)?;
    let events = budget.collect(rows.by_ref().take(plan.query.limit))?;
    let more = rows.next().transpose().map_err(db_error)?.is_some();
    let next_cursor = if more {
        events
            .last()
            .map(|event| {
                crate::query_cursor::encode_search(
                    plan.original,
                    SearchPosition {
                        high_water_id: plan.high,
                        last_event_time: event.event_time,
                        last_event_id: event
                            .id
                            .parse()
                            .map_err(|error| invalid_event_id().with_source(error))?,
                    },
                )
            })
            .transpose()?
    } else {
        None
    };

    Ok(Answer::Search(SearchResult {
        events,
        next_cursor,
    }))
}

/// Answers a tail page: the events after `after` that fit the byte budget, and the checkpoint to
/// resume from.
fn answer_tail(
    tx: &Transaction<'_>,
    plan: &Plan<'_>,
    dataset: &mut Dataset,
    after: i64,
) -> Result<Answer> {
    let tail_cursor = TailCursor::new(plan.original)?;
    let Dataset {
        data,
        filter,
        values,
        ..
    } = dataset;
    filter.push_str(" AND id > ?");
    values.push(Value::BigInt(after));
    let mut stmt = tx
        .prepare(&format!(
            "SELECT * FROM ({data}) retained WHERE {filter} ORDER BY id ASC LIMIT {}",
            plan.query.limit
        ))
        .map_err(db_error)?;
    let rows = stmt
        .query_map(params_from_iter(values.iter()), map_event)
        .map_err(db_error)?;

    let mut events = Vec::new();
    let mut remaining = plan.max_bytes;
    for row in rows {
        plan.check_live()?;
        let event = row.map_err(db_error)?;
        let cost = tail_event_cost(plan, &tail_cursor, &event)?;
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

    let resume_after = events.last().map_or(Ok(plan.high), |event| {
        event
            .id
            .parse()
            .map_err(|error| invalid_event_id().with_source(error))
    })?;
    let token = tail_cursor.encode(resume_after)?;
    if events.is_empty() && token.len().saturating_add(TAIL_CHECKPOINT_FRAME_BYTES) > plan.max_bytes
    {
        return Err(Error::new(
            ErrorKind::TooLarge,
            TAIL_CHECKPOINT_BUDGET_MESSAGE,
        ));
    }

    Ok(Answer::Search(SearchResult {
        events,
        next_cursor: Some(token),
    }))
}

/// The bytes one tail event costs in the response: its JSON, its checkpoint token and its framing.
fn tail_event_cost(plan: &Plan<'_>, tail_cursor: &TailCursor, event: &Event) -> Result<usize> {
    let mut counter = ResultBudget {
        remaining: usize::MAX,
        deadline: Some(plan.cancellation.at),
        cancelled: Some(plan.cancellation.cancelled),
    };
    serde_json::to_writer(&mut counter, event).map_err(db_error)?;

    let id = event
        .id
        .parse()
        .map_err(|error| invalid_event_id().with_source(error))?;
    Ok((usize::MAX - counter.remaining)
        .saturating_add(tail_cursor.token_len(id)?)
        .saturating_add(TAIL_EVENT_FRAME_BYTES))
}

/// Summarizes the rows the read can see, and how many of them the projection covers.
fn projection_status(tx: &Transaction<'_>, plan: &Plan<'_>, dataset: &Dataset) -> Result<Answer> {
    let coverage = if dataset.layout.is_some() {
        "count(*) FILTER (WHERE pa_covered)"
    } else {
        "0::BIGINT"
    };
    let hot_sql = format!(
        "SELECT count(*),{coverage} FROM events \
         WHERE event_time >= ? AND event_time < ? AND id <= ?"
    );
    let (hot_rows, hot_covered_rows): (i64, i64) = tx
        .query_row(
            &hot_sql,
            params![plan.query.from, plan.query.to, plan.high],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(db_error)?;

    let total_sql = format!(
        "SELECT count(*),{coverage} FROM ({}) retained \
         WHERE event_time >= ? AND event_time < ? AND id <= ?",
        dataset.selected.sql
    );
    let (total, covered): (i64, i64) = tx
        .query_row(
            &total_sql,
            params![plan.query.from, plan.query.to, plan.high],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(db_error)?;

    let schema_version = tx
        .query_row("SELECT max(version) FROM schema_version", [], |r| r.get(0))
        .map_err(db_error)?;
    let legacy_archive_files: i64 = tx
        .query_row(
            "SELECT count(*) FROM archives WHERE coalesce(schema_version,1)<4",
            [],
            |r| r.get(0),
        )
        .map_err(db_error)?;
    Ok(Answer::ProjectionStatus(ProjectionStatus {
        schema_version,
        promoted_string_keys: dataset
            .layout
            .as_ref()
            .map(|l| l.keys().to_vec())
            .unwrap_or_default(),
        hot_rows: hot_rows as u64,
        hot_covered_rows: hot_covered_rows as u64,
        archived_rows: (total - hot_rows) as u64,
        archived_covered_rows: (covered - hot_covered_rows) as u64,
        legacy_archive_files: legacy_archive_files as u64,
    }))
}

/// Counts the rows that match the query.
fn count_matches(tx: &Transaction<'_>, dataset: &Dataset) -> Result<Answer> {
    let Dataset {
        data,
        filter,
        values,
        ..
    } = dataset;
    let count = tx
        .query_row(
            &format!("SELECT count(*) FROM ({data}) retained WHERE {filter}"),
            params_from_iter(values.iter()),
            |r| r.get::<_, i64>(0),
        )
        .map_err(db_error)?;
    Ok(Answer::Count(count as u64))
}

/// Answers a facet request: the most frequent values of each facet field, at most 100 per field.
fn answer_facets(
    tx: &Transaction<'_>,
    plan: &Plan<'_>,
    dataset: &Dataset,
    budget: &mut ResultBudget<'_>,
) -> Result<Answer> {
    let Dataset {
        data,
        filter,
        values,
        ..
    } = dataset;
    // GROUPING_ID identifies the retained facet; rank each one separately
    // so a frequent source cannot crowd out service, logger, or host values.
    let sql = format!(
        "WITH grouped AS (SELECT service,logger,host,level,source,count(*) n, \
         grouping_id(service,logger,host,level,source) g \
         FROM ({data}) retained WHERE {filter} \
         GROUP BY GROUPING SETS ((service),(logger),(host),(level),(source))), \
         ranked AS (SELECT *, \
         row_number() OVER(PARTITION BY g ORDER BY n DESC, service,logger,host,level,source) \
         rank \
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

    plan.check_live()?;
    let mut stmt = tx.prepare(&sql).map_err(db_error)?;
    let rows = stmt
        .query_map(params_from_iter(values.iter()), |r| {
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
        plan.check_live()?;
        let (group, value) = row.map_err(db_error)?;
        serde_json::to_writer(&mut *budget, &value)
            .map_err(|error| response_too_large().with_source(error))?;

        match group {
            FACET_SERVICE_GROUP => result.services.push(value),
            FACET_LOGGER_GROUP => result.loggers.push(value),
            FACET_HOST_GROUP => result.hosts.push(value),
            FACET_LEVEL_GROUP => result.levels.push(value),
            FACET_SOURCE_GROUP => result.sources.push(value),
            _ => unreachable!("facet groups are the five selected columns"),
        }
    }

    Ok(Answer::Facets(result))
}

/// Answers a histogram: the event count in each `interval`-wide bucket, anchored at the request's
/// start.
fn answer_histogram(
    tx: &Transaction<'_>,
    plan: &Plan<'_>,
    dataset: &Dataset,
    interval: i64,
    budget: &mut ResultBudget<'_>,
) -> Result<Answer> {
    let bucket_count =
        (i128::from(plan.query.to) - i128::from(plan.query.from) + i128::from(interval) - 1)
            / i128::from(interval);
    if bucket_count > i128::from(crate::model::MAX_HISTOGRAM_BUCKETS) {
        return Err(Error::invalid("too many histogram buckets"));
    }

    // Widen before subtracting the anchor so extreme timestamps remain
    // exact and cannot overflow BIGINT during bucket arithmetic.
    let anchor = plan.original.from;
    let Dataset {
        data,
        filter,
        values,
        ..
    } = dataset;
    let mut stmt = tx
        .prepare(&format!(
            "SELECT CAST(((event_time::HUGEINT-({})) // {interval})*{interval}+{} \
             AS BIGINT),count(*) FROM ({data}) retained WHERE {filter} \
             GROUP BY 1 ORDER BY 1",
            anchor, anchor
        ))
        .map_err(db_error)?;
    let buckets = budget.collect(
        stmt.query_map(params_from_iter(values.iter()), |r| {
            Ok(HistogramBucket {
                time: r.get(0)?,
                count: r.get::<_, i64>(1)? as u64,
            })
        })
        .map_err(db_error)?,
    )?;
    Ok(Answer::Histogram(buckets))
}

pub(super) fn map_event(r: &duckdb::Row<'_>) -> duckdb::Result<Event> {
    let attrs: String = r.get(ATTRIBUTES_COLUMN_INDEX)?;

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
            duckdb::Error::FromSqlConversionFailure(
                ATTRIBUTES_COLUMN_INDEX,
                duckdb::types::Type::Text,
                Box::new(e),
            )
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
            return Err(std::io::Error::other(RESPONSE_BUDGET_MESSAGE));
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
            serde_json::to_writer(&mut *self, &row)
                .map_err(|error| response_too_large().with_source(error))?;
            result.push(row);
        }

        Ok(result)
    }
}
