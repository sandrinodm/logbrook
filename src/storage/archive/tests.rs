//! Archive compaction coverage for bounded file selection and durable row preservation.
use super::{ArchiveWork, CompactionRequest, compact_export, export, migrate, publish, recover};
use crate::storage::quote;
use duckdb::{Connection, params};
use std::{
    path::Path,
    sync::atomic::AtomicBool,
    time::{Duration, Instant},
};

fn archived_rows(conn: &Connection, dir: &Path) -> Vec<(i64, i64, String)> {
    let paths = conn
        .prepare("SELECT path FROM archives ORDER BY path")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|path| format!("'{}'", quote(&dir.join(path.unwrap()).to_string_lossy())))
        .collect::<Vec<_>>()
        .join(",");
    conn.prepare(&format!(
        "SELECT id,event_time,message FROM read_parquet([{paths}]) ORDER BY id"
    ))
    .unwrap()
    .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
    .unwrap()
    .collect::<std::result::Result<Vec<_>, _>>()
    .unwrap()
}

#[test]
fn smaller_archives_compact_behind_large_singleton_within_limits() {
    let dir = tempfile::tempdir().unwrap();
    let mut conn = Connection::open_in_memory().unwrap();
    migrate(&mut conn).unwrap();

    // Deterministic, poorly compressible text makes the first archive large
    // enough to fit alone but prevent any following input from joining it.
    let mut state = 1u64;
    let large_message: String = (0..32768)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'!' + (state % 90) as u8)
        })
        .collect();

    let mut originals = Vec::new();
    for (index, time) in [10, 20, 21, 22, 1010].into_iter().enumerate() {
        let id = index as i64 + 1;
        conn.execute(
            "INSERT INTO events VALUES(?,'s',?,10,2,NULL,NULL,NULL,NULL,?,'{}')",
            params![id, time, if index == 0 { &large_message } else { "small" }],
        )
        .unwrap();
        conn.execute("UPDATE ingest_state SET next_id=?", params![id + 1])
            .unwrap();

        let file = export(&mut conn, dir.path(), time + 1).unwrap().unwrap();
        originals.push((file.path.clone(), file.bytes));
        publish(&mut conn, file).unwrap();
    }

    let before = archived_rows(&conn, dir.path());
    let max_bytes = originals[0].1;
    assert!(originals[1].1 + originals[2].1 < max_bytes);

    let cancelled = AtomicBool::new(false);
    let work = || ArchiveWork {
        cancelled: &cancelled,
        deadline: Instant::now() + Duration::from_secs(30),
        quota: None,
    };

    for max_files in [0, 1] {
        assert!(
            compact_export(
                &mut conn,
                CompactionRequest {
                    dir: dir.path(),
                    max_files,
                    max_bytes,
                    partition_ms: 1000,
                    work: work()
                }
            )
            .unwrap()
            .is_none()
        );
    }

    // Even the two smallest inputs cannot fit this budget.
    let smallest = originals.iter().map(|r| r.1).min().unwrap();
    assert!(
        compact_export(
            &mut conn,
            CompactionRequest {
                dir: dir.path(),
                max_files: 10,
                max_bytes: smallest * 2 - 1,
                partition_ms: 1000,
                work: work()
            }
        )
        .unwrap()
        .is_none()
    );

    let file = compact_export(
        &mut conn,
        CompactionRequest {
            dir: dir.path(),
            max_files: 2,
            max_bytes,
            partition_ms: 1000,
            work: work(),
        },
    )
    .unwrap()
    .expect("the smaller same-partition files must make progress");
    assert_eq!(file.replaced.len(), 2);
    assert!(!file.replaced.contains(&originals[0].0));
    assert!(!file.replaced.contains(&originals[4].0));

    let input_bytes: u64 = originals
        .iter()
        .filter(|(name, _)| file.replaced.contains(name))
        .map(|(_, bytes)| bytes)
        .sum();
    assert!(input_bytes <= max_bytes);
    assert_eq!(file.count, 2);
    publish(&mut conn, file).unwrap();
    assert_eq!(archived_rows(&conn, dir.path()), before);

    recover(&conn, dir.path()).unwrap();
    assert_eq!(archived_rows(&conn, dir.path()), before);

    // With one-millisecond partitions, every remaining eligible input is
    // isolated; a merged file spanning partitions must also be skipped.
    assert!(
        compact_export(
            &mut conn,
            CompactionRequest {
                dir: dir.path(),
                max_files: 10,
                max_bytes: u64::MAX,
                partition_ms: 1,
                work: work()
            }
        )
        .unwrap()
        .is_none()
    );
}
