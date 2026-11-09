//! Selects, verifies, and merges bounded sets of archive inputs.
use super::{
    ARCHIVE_DISK_HEADROOM_BYTES, CompactionRequest, CopyStats, LEGACY_ARCHIVE_SCHEMA,
    PROJECTION_ARCHIVE_SCHEMA, StagedArchive, archive_schema, check_deadline, file_nanos,
    projected_digest, projection_columns, read_back_row_count, row_count_error,
};
use crate::{
    model::{Error, ErrorKind},
    storage::{
        Result, db_error,
        maintenance::Published,
        projection, projection_integrity,
        query::QueryDeadline,
        quota::{self, DiskBudget},
        quote, validate_archive_name, widen,
    },
};
use duckdb::Connection;
use std::{path::Path, sync::Arc};

/// Per-row disk estimate for one promoted string slot during compaction.
const PROJECTED_SLOT_OVERHEAD_BYTES: u64 = 32;

/// One eligible manifest row, as compaction selects and verifies its inputs.
#[derive(Clone)]
struct ArchiveRow {
    path: String,
    min_time: i64,
    max_time: i64,
    row_count: i64,
    min_id: i64,
    max_id: i64,
    file_bytes: u64,
    schema_version: i32,
    projection_sha256: Option<String>,
}

/// The size and partition limits one compaction works within.
struct CompactionLimits {
    max_files: usize,
    max_bytes: u64,
    partition_ms: i64,
}

/// The rows chosen for one compaction, and the bytes they occupy.
struct Selection {
    rows: Vec<ArchiveRow>,
    bytes: u64,
}

/// The inputs of one compaction, and the totals its output must reproduce.
struct CompactionPlan {
    inputs: Vec<ArchiveRow>,
    bytes: u64,
    count: i64,
    count_u64: u64,
    min: i64,
    max: i64,
    min_id: i64,
    high: i64,
}

impl CompactionPlan {
    fn new(selection: Selection) -> Result<Self> {
        let inputs = selection.rows;
        let count: i64 = inputs.iter().map(|row| row.row_count).sum();
        let count_u64 = u64::try_from(count).map_err(row_count_error)?;
        Ok(Self {
            bytes: selection.bytes,
            count,
            count_u64,
            min: inputs.iter().map(|row| row.min_time).min().unwrap(),
            max: inputs.iter().map(|row| row.max_time).max().unwrap(),
            min_id: inputs.iter().map(|row| row.min_id).min().unwrap(),
            high: inputs.iter().map(|row| row.max_id).max().unwrap(),
            inputs,
        })
    }
}

/// Reads the manifest rows compaction may consider: those with ID bounds recorded.
fn eligible_archive_rows(
    conn: &Connection,
    layout: Option<&projection::ProjectionLayout>,
) -> Result<Vec<ArchiveRow>> {
    let digest_column = if layout.is_some() {
        "projection_sha256"
    } else {
        "NULL::VARCHAR"
    };
    let mut stmt = conn
        .prepare(&format!(
            "SELECT path,min_time,max_time,row_count,min_id,max_id,coalesce(file_bytes,0),\
             coalesce(schema_version,1),{digest_column} \
             FROM archives WHERE min_id IS NOT NULL AND max_id IS NOT NULL \
             ORDER BY min_time,path"
        ))
        .map_err(db_error)?;
    stmt.query_map([], |r| {
        Ok(ArchiveRow {
            path: r.get(0)?,
            min_time: r.get(1)?,
            max_time: r.get(2)?,
            row_count: r.get(3)?,
            min_id: r.get(4)?,
            max_id: r.get(5)?,
            file_bytes: r.get(6)?,
            schema_version: r.get(7)?,
            projection_sha256: r.get(8)?,
        })
    })
    .map_err(db_error)?
    .collect::<std::result::Result<Vec<_>, _>>()
    .map_err(db_error)
}

/// Picks one legacy-schema row to rewrite on its own, so that it gains the promoted projection,
/// when no two rows fit together. Applies only when a projection layout is configured.
fn lone_legacy_input(
    rows: Vec<ArchiveRow>,
    limits: &CompactionLimits,
    layout: Option<&projection::ProjectionLayout>,
) -> Option<ArchiveRow> {
    if layout.is_none() || limits.max_files == 0 {
        return None;
    }
    rows.into_iter().find(|row| {
        row.schema_version < PROJECTION_ARCHIVE_SCHEMA
            && row.file_bytes <= limits.max_bytes
            && row.min_time.div_euclid(limits.partition_ms)
                == row.max_time.div_euclid(limits.partition_ms)
    })
}

/// Chooses the inputs of one compaction. Returns `None` unless some partition yields at least one
/// input, or two inputs that fit together within the byte limit.
fn select_inputs(
    mut rows: Vec<ArchiveRow>,
    limits: CompactionLimits,
    layout: Option<&projection::ProjectionLayout>,
    cancellation: QueryDeadline<'_>,
) -> Result<Option<Selection>> {
    let partition_ms = limits.partition_ms;
    // Prefer the oldest eligible partition, then its smallest inputs. Starting
    // with a large singleton can otherwise prevent two smaller files from ever
    // fitting, even though the same unchanged manifest is retried indefinitely.
    rows.sort_by(|a, b| {
        (
            a.min_time.div_euclid(partition_ms),
            a.file_bytes,
            a.min_time,
            &a.path,
        )
            .cmp(&(
                b.min_time.div_euclid(partition_ms),
                b.file_bytes,
                b.min_time,
                &b.path,
            ))
    });

    let mut selected = Vec::new();
    let mut bytes = 0u64;
    let mut partition = None;
    for row in &rows {
        check_deadline(cancellation)?;
        let key = row.min_time.div_euclid(partition_ms);
        if row.max_time.div_euclid(partition_ms) != key {
            continue;
        }

        if partition.is_some_and(|p| p != key) {
            if selected.len() >= 2 {
                break;
            }

            selected.clear();
            bytes = 0;
        }

        if row.file_bytes > limits.max_bytes
            || bytes.saturating_add(row.file_bytes) > limits.max_bytes
        {
            continue;
        }
        partition = Some(key);
        bytes += row.file_bytes;
        selected.push(row.clone());

        if selected.len() >= limits.max_files {
            break;
        }
    }

    if selected.len() < 2 {
        selected.clear();
        if let Some(row) = lone_legacy_input(rows, &limits, layout) {
            bytes = row.file_bytes;
            selected.push(row);
        }

        if selected.is_empty() {
            return Ok(None);
        }
    }

    Ok(Some(Selection {
        rows: selected,
        bytes,
    }))
}

/// Verified merge paths and the digests checked again after reading the inputs.
struct VerifiedInputs {
    paths: Vec<String>,
    digests: Vec<projection_integrity::ArchiveIntegrity>,
}

/// Checks each input against its manifest row. Returns the quoted input paths for the merge, and
/// the digests the merged output is checked against once it is written.
fn verify_inputs(
    conn: &Connection,
    dir: &Path,
    layout: Option<&projection::ProjectionLayout>,
    inputs: &[ArchiveRow],
    cancellation: QueryDeadline<'_>,
) -> Result<VerifiedInputs> {
    let mut paths = Vec::new();
    let mut digests = Vec::new();

    for row in inputs {
        validate_archive_name(&row.path)?;
        match (row.schema_version, layout) {
            (1..=LEGACY_ARCHIVE_SCHEMA, _) if row.projection_sha256.is_none() => {
                if layout.is_some() {
                    projection_integrity::validate_legacy_schema(
                        conn,
                        &dir.join(&row.path),
                        Some(cancellation),
                    )?;
                }
            }
            (PROJECTION_ARCHIVE_SCHEMA, Some(layout)) => {
                let full = dir.join(&row.path);
                let file = projection_integrity::ArchiveIntegrity::new(
                    full.clone(),
                    row.projection_sha256.clone(),
                )?;
                file.verify(Some(cancellation))?;
                projection_integrity::validate_schema(conn, &full, layout, Some(cancellation))?;
                digests.push(file);
            }
            _ => {
                return Err(projection_integrity::integrity(
                    "unsupported compaction archive schema/layout",
                ));
            }
        }

        paths.push(format!(
            "'{}'",
            quote(&dir.join(&row.path).to_string_lossy())
        ));
    }

    Ok(VerifiedInputs { paths, digests })
}

/// Reserves the disk a compaction may use: the input bytes twice over, the JSON the promoted
/// columns are decoded from, and fixed headroom. Returns `None` when no quota is configured.
fn reserve_compaction(
    conn: &Connection,
    budget: Option<&Arc<DiskBudget>>,
    layout: Option<&projection::ProjectionLayout>,
    plan: &CompactionPlan,
    paths: &[String],
) -> Result<Option<quota::Reservation>> {
    let Some(budget) = budget else {
        return Ok(None);
    };

    let projection_bytes = if let Some(layout) = layout {
        // Compressed input size does not bound the new string columns.
        // Original JSON bytes bound the decoded promoted strings even for
        // escaped JSON; include per-slot row overhead and output headroom.
        let json_bytes: u64 = conn
            .query_row(
                &format!(
                    "SELECT coalesce(sum(octet_length(encode(attributes))),0)::UBIGINT \
                     FROM read_parquet([{}],union_by_name=true)",
                    paths.join(",")
                ),
                [],
                |r| r.get(0),
            )
            .map_err(db_error)?;
        json_bytes.saturating_mul(2).saturating_add(
            plan.count_u64
                .saturating_mul(widen(layout.keys().len()))
                .saturating_mul(PROJECTED_SLOT_OVERHEAD_BYTES),
        )
    } else {
        0
    };
    budget.reserve(
        plan.bytes
            .saturating_mul(2)
            .saturating_add(projection_bytes)
            .saturating_add(ARCHIVE_DISK_HEADROOM_BYTES),
    )
}

/// Merges the quoted input files into the temporary file, sorted by event time and ID.
fn copy_inputs(
    conn: &Connection,
    layout: Option<&projection::ProjectionLayout>,
    paths: &[String],
    temporary: &Path,
) -> Result<CopyStats> {
    let extra = projection_columns(layout);
    let rows: u64 = conn
        .query_row(
            &format!(
                "COPY (SELECT \
                 id,source,event_time,received_at,level,service,logger,host,pid,message,\
                 attributes{extra} \
                 FROM (SELECT * FROM events WHERE false \
                 UNION ALL BY NAME SELECT * FROM read_parquet([{}],union_by_name=true)) \
                 ORDER BY event_time,id) \
                 TO '{}' (FORMAT PARQUET,COMPRESSION ZSTD)",
                paths.join(","),
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

/// Merges a few small archives of one partition into one archive, and commits the swap in the
/// manifest. Returns `None` when no set of inputs fits the limits.
pub(in crate::storage) fn compact_export(
    conn: &mut Connection,
    request: CompactionRequest<'_>,
) -> Result<Option<Published>> {
    let CompactionRequest {
        dir,
        max_files,
        max_bytes,
        partition_ms,
        work,
    } = request;
    let cancellation = QueryDeadline {
        cancelled: work.cancelled,
        at: work.deadline,
    };
    check_deadline(cancellation)?;

    let tx = conn.transaction().map_err(db_error)?;
    let layout = projection::ProjectionLayout::load(&tx)?;
    let rows = eligible_archive_rows(&tx, layout.as_ref())?;

    let limits = CompactionLimits {
        max_files,
        max_bytes,
        partition_ms,
    };
    let Some(selection) = select_inputs(rows, limits, layout.as_ref(), cancellation)? else {
        return Ok(None);
    };
    let plan = CompactionPlan::new(selection)?;
    let VerifiedInputs { paths, digests } =
        verify_inputs(&tx, dir, layout.as_ref(), &plan.inputs, cancellation)?;
    let reservation = reserve_compaction(&tx, work.quota, layout.as_ref(), &plan, &paths)?;

    let staged = StagedArchive::new(
        dir,
        format!("compact-{}-{}.parquet", plan.high, file_nanos()),
    );
    check_deadline(cancellation)?;
    let copied = copy_inputs(&tx, layout.as_ref(), &paths, &staged.temporary)?;
    let actual = read_back_row_count(&tx, &staged.temporary, cancellation)?;
    if actual != plan.count || copied.rows != plan.count_u64 {
        return Err(Error::new(
            ErrorKind::Integrity,
            "compaction count mismatch",
        ));
    }

    projection_integrity::verify_all(&digests, Some(cancellation))?;
    let projection_sha256 =
        projected_digest(&tx, &staged.temporary, layout.as_ref(), cancellation)?;

    staged.rename_into_place(dir)?;
    tx.commit().map_err(db_error)?;

    Ok(Some(Published {
        path: staged.name,
        min: plan.min,
        max: plan.max,
        count: plan.count,
        before: 0,
        high: plan.high,
        min_id: plan.min_id,
        bytes: copied.bytes,
        archive_schema: archive_schema(layout.as_ref()),
        projection_sha256,
        lower: 0,
        artifact: staged.artifact,
        reservation,
        replaced: plan.inputs.into_iter().map(|row| row.path).collect(),
    }))
}
