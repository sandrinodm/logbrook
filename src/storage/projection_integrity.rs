//! Immutable promoted archive verification. No metadata-only digest cache is trusted.
#[cfg(test)]
use super::{
    ArchiveLeases, ArchiveWork, MaintenanceBudget, NormalizedEvent, Operation, Query, Storage,
    StorageConfig, Write, WriterScope, archive, compact_export, maintenance_outcome, migrate,
    params_from_iter, query, recover,
};
use super::{Result, db_error, projection, query::QueryDeadline, quote, readers::check_cancelled};
use crate::model::{Error, ErrorKind};
#[cfg(test)]
use crate::storage::archive::CompactionRequest;
use duckdb::Connection;
#[cfg(test)]
use duckdb::params;
use sha2::{Digest, Sha256};
use std::{
    fmt::Write as _,
    io::Read,
    path::{Path, PathBuf},
};
#[cfg(test)]
use std::{
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::{Duration, Instant},
};
#[cfg(test)]
use tokio::sync::oneshot;

/// Bytes read per chunk while hashing immutable archive files.
const HASH_READ_BUFFER_BYTES: usize = 64 * 1024;

/// Lowercase hexadecimal characters in one SHA-256 digest.
const SHA256_HEX_CHARACTERS: usize = 64;

#[derive(Clone)]
pub(super) struct ArchiveIntegrity {
    path: PathBuf,
    expected: String,
}

pub(super) fn hash(path: &Path, deadline: Option<QueryDeadline<'_>>) -> Result<String> {
    let mut file = std::fs::File::open(path).map_err(|e| integrity(e.to_string()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; HASH_READ_BUFFER_BYTES];
    loop {
        if let Some(check) = deadline {
            check_cancelled(check.cancelled, check.at)?;
        }
        let count = file
            .read(&mut buffer)
            .map_err(|e| integrity(e.to_string()))?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }

    Ok(digest.finalize().iter().fold(
        String::with_capacity(SHA256_HEX_CHARACTERS),
        |mut hex, byte| {
            write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
            hex
        },
    ))
}

pub(super) fn integrity(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Integrity, message)
}

// Archive format/schema failures are fatal, while native resource limits and
// watchdog interruption must retain their ordinary operational error kinds.
pub(super) fn validation_error(error: duckdb::Error, deadline: Option<QueryDeadline<'_>>) -> Error {
    if let Some(check) = deadline
        && let Err(error) = check_cancelled(check.cancelled, check.at)
    {
        return error;
    }

    let message = error.to_string();
    let details = serde_json::from_str::<serde_json::Value>(&message).ok();
    let kind = details
        .as_ref()
        .and_then(|v| v.get("exception_type"))
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| {
            message
                .split_once(" Error:")
                .map_or("", |(prefix, _)| prefix)
        });
    let kind = kind.to_ascii_lowercase().replace([' ', '_'], "");
    match kind.as_str() {
        "interrupt" => {
            Error::new(ErrorKind::Timeout, "archive validation interrupted").with_source(error)
        }
        "outofmemory" => Error::unavailable(message).with_source(error),
        "io" | "invalidinput" | "conversion" | "binder" | "catalog" => {
            integrity(message).with_source(error)
        }
        _ => db_error(error),
    }
}

fn validation_check(deadline: Option<QueryDeadline<'_>>) -> Result<()> {
    if let Some(check) = deadline {
        check_cancelled(check.cancelled, check.at)?;
    }
    Ok(())
}

fn validate_rows(conn: &Connection, sql: &str, deadline: Option<QueryDeadline<'_>>) -> Result<i64> {
    validation_check(deadline)?;
    let result = conn
        .query_row(sql, [], |r| r.get(0))
        .map_err(|e| validation_error(e, deadline))?;
    validation_check(deadline)?;
    Ok(result)
}

impl ArchiveIntegrity {
    pub(super) fn new(path: PathBuf, expected: Option<String>) -> Result<Self> {
        let expected =
            expected.ok_or_else(|| integrity("promoted archive missing durable digest"))?;
        if expected.len() != SHA256_HEX_CHARACTERS
            || !expected
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(integrity("invalid promoted archive digest"));
        }

        Ok(Self { path, expected })
    }

    pub(super) fn verify(&self, deadline: Option<QueryDeadline<'_>>) -> Result<()> {
        if hash(&self.path, deadline)? != self.expected {
            return Err(integrity("promoted archive SHA-256 mismatch"));
        }
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    // Thread-local and keyed by the selected path: parallel tests cannot mutate
    // another query's archives. No hook exists in production builds.
    static POST_READ_MUTATION: std::cell::RefCell<Option<(PathBuf, Vec<u8>)>> =
        const { std::cell::RefCell::new(None) };
}

pub(super) fn verify_all(
    files: &[ArchiveIntegrity],
    deadline: Option<QueryDeadline<'_>>,
) -> Result<()> {
    for file in files {
        #[cfg(test)]
        POST_READ_MUTATION.with(|hook| {
            let mut hook = hook.borrow_mut();
            if hook.as_ref().is_some_and(|(path, _)| path == &file.path) {
                let (path, bytes) = hook.take().unwrap();
                std::fs::write(path, bytes).unwrap();
            }
        });
        file.verify(deadline)?;
    }
    Ok(())
}

pub(super) fn validate_schema(
    conn: &Connection,
    path: &Path,
    layout: &projection::ProjectionLayout,
    deadline: Option<QueryDeadline<'_>>,
) -> Result<()> {
    validation_check(deadline)?;

    let mut statement = conn
        .prepare(&format!(
            "DESCRIBE SELECT * FROM read_parquet('{}')",
            quote(&path.to_string_lossy())
        ))
        .map_err(|e| validation_error(e, deadline))?;
    let columns = statement
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
        .map_err(|e| validation_error(e, deadline))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| validation_error(e, deadline))?;

    let mut expected = vec![
        ("id".to_owned(), "BIGINT"),
        ("source".to_owned(), "VARCHAR"),
        ("event_time".to_owned(), "BIGINT"),
        ("received_at".to_owned(), "BIGINT"),
        ("level".to_owned(), "INTEGER"),
        ("service".to_owned(), "VARCHAR"),
        ("logger".to_owned(), "VARCHAR"),
        ("host".to_owned(), "VARCHAR"),
        ("pid".to_owned(), "BIGINT"),
        ("message".to_owned(), "VARCHAR"),
        ("attributes".to_owned(), "VARCHAR"),
        ("pa_covered".to_owned(), "BOOLEAN"),
    ];
    expected.extend(
        layout
            .slot_names()
            .into_iter()
            .map(|slot| (slot, "VARCHAR")),
    );

    for (name, kind) in &expected {
        if !columns.iter().any(|(n, k)| n == name && k == kind) {
            return Err(integrity(format!(
                "promoted archive missing or invalid column {name}"
            )));
        }
    }

    if columns
        .iter()
        .any(|(name, _)| name.starts_with("pa_") && !expected.iter().any(|(n, _)| n == name))
    {
        return Err(integrity(
            "promoted archive has unexpected projection columns",
        ));
    }
    Ok(())
}

pub(super) fn validate_output(
    conn: &Connection,
    path: &Path,
    layout: &projection::ProjectionLayout,
    deadline: Option<QueryDeadline<'_>>,
) -> Result<()> {
    validate_schema(conn, path, layout, deadline)?;
    let invalid = validate_rows(
        conn,
        &format!(
            "SELECT count(*) FROM read_parquet('{}') WHERE NOT ({}) OR pa_covered IS NULL",
            quote(&path.to_string_lossy()),
            layout.covered_validation_sql("attributes")
        ),
        deadline,
    )?;
    if invalid != 0 {
        return Err(integrity("archive projection validation failed"));
    }
    Ok(())
}

pub(super) fn validate_legacy_schema(
    conn: &Connection,
    path: &Path,
    deadline: Option<QueryDeadline<'_>>,
) -> Result<()> {
    let count = validate_rows(
        conn,
        &format!(
            "SELECT count(*) FROM parquet_schema('{}') WHERE starts_with(name,'pa_')",
            quote(&path.to_string_lossy())
        ),
        deadline,
    )?;
    if count != 0 {
        return Err(integrity(
            "legacy archive contains unexpected projection columns",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn interrupted_row_validation_preserves_readiness_and_connection_use() {
        let dir = tempfile::tempdir().unwrap();
        let db = Storage::open(StorageConfig {
            data_dir: dir.path().to_owned(),
            promoted_string_keys: vec!["runId".into()],
            ..StorageConfig::default()
        })
        .unwrap();
        let ready = db.inner.ready.clone();
        let metrics = db.inner.metrics.clone();
        let cancelled = db.inner.maintenance_cancel.clone();
        let (reply, answer) = oneshot::channel();
        db.inner
            .writer
            .send(Write::Test(Box::new(move |conn, active| {
                let budget = MaintenanceBudget {
                    cancelled,
                    deadline: Instant::now() + Duration::from_millis(50),
                };
                let scope = WriterScope::start(budget.clone(), active).unwrap();
                // This uses the same row-validation executor and error conversion
                // as archive covered-row validation. The writer watchdog interrupts
                // a query that cannot finish before the armed deadline.
                let result = validate_rows(
                    conn,
                    "SELECT sum(i)::BIGINT FROM range(10000000000) t(i)",
                    Some(QueryDeadline {
                        cancelled: &budget.cancelled,
                        at: budget.deadline,
                    }),
                )
                .map_err(|error| budget.error(error));
                drop(scope);
                maintenance_outcome(&result, &metrics, &ready);
                reply.send(result).unwrap();
            })))
            .await
            .unwrap();

        let error = tokio::time::timeout(Duration::from_secs(3), answer)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Timeout);
        assert!(db.is_ready());

        let event = NormalizedEvent {
            event_time: 10,
            level: 30,
            service: None,
            logger: None,
            host: None,
            pid: None,
            message: "usable".into(),
            attributes: serde_json::json!({"runId":"x"}),
        };
        assert_eq!(db.append("s".into(), vec![event]).await.unwrap(), 1);

        assert_eq!(
            db.count(Query {
                from: 0,
                to: 100,
                limit: 10,
                ..Query::default()
            })
            .await
            .unwrap(),
            1
        );

        assert!(db.is_ready());
        db.shutdown().await.unwrap();
    }

    #[test]
    fn native_resource_errors_are_operational_and_bad_parquet_is_integrity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("invalid.parquet");
        std::fs::write(&path, b"invalid parquet footer").unwrap();

        let mut conn = Connection::open_in_memory().unwrap();
        migrate(&mut conn).unwrap();
        let layout = projection::activate(&mut conn, &["runId".into()])
            .unwrap()
            .unwrap();

        for json_errors in [false, true] {
            conn.execute_batch(&format!("SET errors_as_json={json_errors}"))
                .unwrap();
            assert_eq!(
                validate_schema(&conn, &path, &layout, None)
                    .unwrap_err()
                    .kind,
                ErrorKind::Integrity
            );

            let native_error = conn.execute_batch("SET memory_limit='1B'").unwrap_err();
            let error = validation_error(native_error, None);
            assert_eq!(error.kind, ErrorKind::Unavailable, "{}", error.message);
            conn.execute_batch("SET memory_limit='1GB'").unwrap();
        }
    }

    #[test]
    fn actual_query_and_compaction_reject_mutation_between_hash_passes() {
        struct ResetHook;
        impl Drop for ResetHook {
            fn drop(&mut self) {
                POST_READ_MUTATION.with(|hook| {
                    hook.borrow_mut().take();
                });
            }
        }

        let _reset = ResetHook;
        let dir = tempfile::tempdir().unwrap();
        let mut conn = Connection::open_in_memory().unwrap();
        migrate(&mut conn).unwrap();
        projection::activate(&mut conn, &["runId".into()]).unwrap();

        for id in 1..=2 {
            conn.execute(
                "INSERT INTO events VALUES(?,'s',10,10,2,NULL,NULL,NULL,NULL,'m',\
                 '{\"runId\":\"x\"}',true,'x')",
                params![id],
            )
            .unwrap();
            conn.execute("UPDATE ingest_state SET next_id=?", params![id + 1])
                .unwrap();
            let file = archive::export(&mut conn, dir.path(), 100)
                .unwrap()
                .unwrap();
            archive::publish(&mut conn, file).unwrap();
        }

        let names = {
            let mut stmt = conn
                .prepare("SELECT path FROM archives ORDER BY path")
                .unwrap();
            stmt.query_map([], |r| r.get::<_, String>(0))
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
        };
        let path = dir.path().join(&names[0]);
        let original = std::fs::read(&path).unwrap();
        let mut changed = original.clone();
        changed[8] ^= 1;

        let cancelled = AtomicBool::new(false);
        let deadline = Instant::now() + Duration::from_secs(30);
        let q = Query {
            from: 0,
            to: 100,
            limit: 10,
            ..Query::default()
        };

        POST_READ_MUTATION.with(|hook| *hook.borrow_mut() = Some((path.clone(), changed.clone())));
        let error = query::run_query(
            &mut conn,
            dir.path(),
            &q,
            Operation::Count,
            1024 * 1024,
            &Arc::new(Mutex::new(ArchiveLeases::default())),
            QueryDeadline {
                cancelled: &cancelled,
                at: deadline,
            },
        )
        .err()
        .expect("post-read query mutation must fail");
        assert_eq!(error.kind, ErrorKind::Integrity);
        assert!(error.message.contains("SHA-256 mismatch"));
        POST_READ_MUTATION.with(|hook| {
            assert!(
                hook.borrow().is_none(),
                "query must execute the postcheck hook"
            )
        });

        std::fs::write(&path, &original).unwrap();
        POST_READ_MUTATION.with(|hook| *hook.borrow_mut() = Some((path.clone(), changed)));
        let error = compact_export(
            &mut conn,
            CompactionRequest {
                dir: dir.path(),
                max_files: 4,
                max_bytes: 1024 * 1024,
                partition_ms: 3600000,
                work: ArchiveWork {
                    cancelled: &cancelled,
                    deadline,
                    quota: None,
                },
            },
        )
        .err()
        .expect("post-read compaction mutation must fail");
        assert_eq!(error.kind, ErrorKind::Integrity);
        assert!(error.message.contains("SHA-256 mismatch"));
        POST_READ_MUTATION.with(|hook| {
            assert!(
                hook.borrow().is_none(),
                "compaction must execute the postcheck hook"
            )
        });

        assert_eq!(
            conn.query_row("SELECT count(*) FROM archives", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );

        let mut disk_names = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        disk_names.sort();
        assert_eq!(
            disk_names, names,
            "failed compaction must not retain or publish output"
        );

        std::fs::write(&path, original).unwrap();
        recover(&conn, dir.path()).unwrap();
    }

    #[test]
    fn streaming_digest_preserves_known_sha256_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("archive.parquet");
        std::fs::write(&path, b"abc").unwrap();
        assert_eq!(
            hash(&path, None).unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn streaming_digest_detects_same_size_mutation_and_observes_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("archive.parquet");
        std::fs::write(&path, b"original").unwrap();

        let expected = hash(&path, None).unwrap();
        let file = ArchiveIntegrity::new(path.clone(), Some(expected)).unwrap();
        file.verify(None).unwrap();

        std::fs::write(&path, b"modified").unwrap();
        assert_eq!(file.verify(None).unwrap_err().kind, ErrorKind::Integrity);

        let cancelled = AtomicBool::new(true);
        assert_eq!(
            hash(
                &path,
                Some(QueryDeadline {
                    cancelled: &cancelled,
                    at: Instant::now() + Duration::from_secs(5)
                })
            )
            .unwrap_err()
            .kind,
            ErrorKind::Timeout
        );
    }

    #[test]
    fn single_legacy_backfill_finishes_with_uncovered_corruption_and_typed_pushdown() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = Connection::open_in_memory().unwrap();
        migrate(&mut conn).unwrap();
        conn.execute(
            "INSERT INTO events VALUES(1,'s',10,10,2,NULL,NULL,NULL,NULL,'m',\
             '{\"runId\":\"x\"}'),(2,'s',11,10,2,NULL,NULL,NULL,NULL,'m','invalid')",
            [],
        )
        .unwrap();
        conn.execute("UPDATE ingest_state SET next_id=3", [])
            .unwrap();
        let file = archive::export(&mut conn, dir.path(), 100)
            .unwrap()
            .unwrap();
        archive::publish(&mut conn, file).unwrap();

        let layout = projection::activate(&mut conn, &["runId".into()])
            .unwrap()
            .unwrap();
        let cancelled = AtomicBool::new(false);
        let deadline = Instant::now() + Duration::from_secs(30);
        let file = compact_export(
            &mut conn,
            CompactionRequest {
                dir: dir.path(),
                max_files: 4,
                max_bytes: 1024 * 1024,
                partition_ms: 3600000,
                work: ArchiveWork {
                    cancelled: &cancelled,
                    deadline,
                    quota: None,
                },
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(file.archive_schema, 4);
        assert!(file.projection_sha256.is_some());
        archive::publish(&mut conn, file).unwrap();
        assert!(
            compact_export(
                &mut conn,
                CompactionRequest {
                    dir: dir.path(),
                    max_files: 4,
                    max_bytes: 1024 * 1024,
                    partition_ms: 3600000,
                    work: ArchiveWork {
                        cancelled: &cancelled,
                        deadline,
                        quota: None
                    }
                }
            )
            .unwrap()
            .is_none()
        );

        recover(&conn, dir.path()).unwrap();

        let mut q = Query {
            from: 0,
            to: 100,
            ..Query::default()
        };
        q.attributes = crate::attribute_filter::AttributeFilters::parse(
            r#"{"attributes":[{"key":"runId","op":"eq","value":"x"}]}"#,
            1024,
        )
        .unwrap();
        let selected = query::selected_dataset(&conn, dir.path(), &q, None, None).unwrap();
        let coverage: (i64, i64) = conn
            .query_row(
                &format!(
                    "SELECT count(*) FILTER(WHERE pa_covered),\
                     count(*) FILTER(WHERE NOT pa_covered) FROM ({})",
                    selected.sql
                ),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(coverage, (1, 1));

        let (data, values) = query::projected_dataset(&selected.sql, &q, 2, &layout);
        // The uncovered malformed original must still visit the JSON compiler.
        assert!(
            conn.query_row(
                &format!("SELECT count(*) FROM ({data})"),
                params_from_iter(values.clone()),
                |r| r.get::<_, i64>(0)
            )
            .is_err()
        );

        let mut explain = conn
            .prepare(&format!("EXPLAIN SELECT count(*) FROM ({data})"))
            .unwrap();
        let plans = explain
            .query_map(params_from_iter(values), |r| r.get::<_, String>(1))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        eprintln!("{plans}");
        assert!(plans.contains("READ_PARQUET"));

        // DuckDB 2 adds spaces around operators in its text renderer.
        let compact_plan: String = plans.split_whitespace().collect();
        assert!(compact_plan.contains("pa_0='x'"));
        assert!(plans.contains("Filters:"));
    }
}
