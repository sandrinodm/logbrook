//! Storage durability, querying, retention and maintenance regression tests.

use logbrook::{
    config::StorageConfig,
    model::{ErrorKind, NormalizedEvent, Query},
    storage::Storage,
};
use serde_json::json;

fn config(path: &std::path::Path) -> StorageConfig {
    StorageConfig {
        data_dir: path.to_owned(),
        reader_threads: 2,
        queue_capacity: 8,
        queue_bytes: 1024 * 1024,
        memory_limit: "128MB".into(),
        temp_limit: "256MB".into(),
        duckdb_threads: 1,
        query_timeout_ms: 10000,
        max_query_bytes: 1024 * 1024,
        ..StorageConfig::default()
    }
}

fn event(time: i64, message: &str) -> NormalizedEvent {
    NormalizedEvent {
        event_time: time,
        level: 30,
        service: Some("api".into()),
        logger: None,
        host: None,
        pid: None,
        message: message.into(),
        attributes: json!({
            "nested": {
                "ok": true,
            },
        }),
    }
}

fn query() -> Query {
    Query {
        from: 0,
        to: 1000,
        sources: vec!["a".into()],
        service: None,
        logger: None,
        host: None,
        message: None,
        min_level: None,
        limit: 2,
        cursor: None,
        ..Query::default()
    }
}

#[tokio::test]
async fn durable_archive_late_arrival_and_stable_paging() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path())).unwrap();
    db.append(
        "a".into(),
        vec![event(10, "old"), event(20, "middle"), event(20, "new")],
    )
    .await
    .unwrap();

    let first = db.search(query()).await.unwrap();
    assert_eq!(first.events.len(), 2);
    assert_eq!(first.events[0].message, "new");

    db.append("a".into(), vec![event(15, "late")])
        .await
        .unwrap();

    let mut page = query();
    page.cursor = first.next_cursor;

    let next = db.search(page).await.unwrap();
    assert_eq!(next.events.len(), 1);
    assert_eq!(next.events[0].message, "old");
    assert_eq!(db.archive(21).await.unwrap(), 4);
    assert_eq!(db.count(query()).await.unwrap(), 4);

    db.append("a".into(), vec![event(5, "later still")])
        .await
        .unwrap();

    assert_eq!(db.count(query()).await.unwrap(), 5);

    let facets = db.facets(query()).await.unwrap();
    assert_eq!(facets.services[0].count, 5);
    assert_eq!(
        db.histogram(query(), 10)
            .await
            .unwrap()
            .iter()
            .map(|b| b.count)
            .sum::<u64>(),
        5
    );

    db.shutdown().await.unwrap();

    drop(db);

    let db = Storage::open(config(dir.path())).unwrap();
    assert_eq!(db.count(query()).await.unwrap(), 5);

    db.retain(15).await.unwrap();

    assert_eq!(db.count(query()).await.unwrap(), 3);

    db.append("a".into(), vec![event(30, "after restart")])
        .await
        .unwrap();

    let events = db.search(query()).await.unwrap();
    assert_eq!(events.events[0].id, "6");

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn search_continuation_skips_newer_archives_without_losing_timestamp_ties() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path())).unwrap();
    db.append("a".into(), vec![event(10, "old"), event(20, "first tie")])
        .await
        .unwrap();
    db.archive(21).await.unwrap();
    db.append(
        "a".into(),
        vec![event(20, "second tie"), event(20, "third tie")],
    )
    .await
    .unwrap();
    db.archive(21).await.unwrap();

    let archive_paths = || {
        std::fs::read_dir(dir.path().join("archives"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<std::collections::BTreeSet<_>>()
    };
    let previous = archive_paths();
    db.append("a".into(), vec![event(90, "newest archived")])
        .await
        .unwrap();
    db.archive(100).await.unwrap();
    let added: Vec<_> = archive_paths().difference(&previous).cloned().collect();
    assert_eq!(added.len(), 1);
    db.append("a".into(), vec![event(20, "hot tie")])
        .await
        .unwrap();

    let first = db.search(query()).await.unwrap();
    assert_eq!(
        first
            .events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        ["5", "6"]
    );
    let mut page_query = query();
    page_query.cursor = first.next_cursor;
    assert!(page_query.cursor.is_some());

    // Later ingestion must remain outside this search snapshot, including rows
    // tied with its continuation timestamp and rows below it.
    db.append(
        "a".into(),
        vec![event(20, "late tie"), event(10, "late old")],
    )
    .await
    .unwrap();

    // This file has no rows at or below the continuation timestamp. Making it
    // temporarily unavailable proves continuation planning excludes it before
    // integrity checks and Parquet registration, rather than only filtering SQL.
    let parked = dir.path().join("newer-archive.held");
    std::fs::rename(&added[0], &parked).unwrap();

    let continued: Result<Vec<String>, logbrook::model::Error> = async {
        let mut ids = Vec::new();
        loop {
            let page = db.search(page_query.clone()).await?;
            ids.extend(page.events.into_iter().map(|event| event.id));
            if page.next_cursor.is_none() {
                break;
            }

            page_query.cursor = page.next_cursor;
        }

        Ok(ids)
    }
    .await;

    // Restore even when the query failed so the injected fault cannot affect
    // shutdown or mask the continuation failure under test.
    std::fs::rename(&parked, &added[0]).unwrap();
    assert_eq!(continued.unwrap(), ["4", "3", "2", "1"]);
    assert_eq!(db.count(query()).await.unwrap(), 8);
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_reader_connections_and_source_filters() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path())).unwrap();
    db.append(
        "a".into(),
        vec![event(10, "quote '%_\\"), event(20, "second")],
    )
    .await
    .unwrap();

    db.append("b".into(), vec![event(10, "secret")])
        .await
        .unwrap();
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let db = db.clone();
        tasks.push(tokio::spawn(
            async move { db.count(query()).await.unwrap() },
        ));
    }

    for task in tasks {
        assert_eq!(task.await.unwrap(), 2);
    }

    let mut q = query();
    q.message = Some("'%_\\".into());

    assert_eq!(db.search(q).await.unwrap().events.len(), 1);

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn referenced_missing_archive_fails_startup() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path())).unwrap();
    db.append("a".into(), vec![event(10, "old")]).await.unwrap();

    db.archive(20).await.unwrap();

    db.shutdown().await.unwrap();

    drop(db);
    let path = std::fs::read_dir(dir.path().join("archives"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::remove_file(path).unwrap();

    assert!(Storage::open(config(dir.path())).is_err());
}

#[tokio::test]
async fn tail_uses_ingestion_order_for_late_events() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path())).unwrap();
    db.append("a".into(), vec![event(20, "initial")])
        .await
        .unwrap();
    let initial = db.tail(None, vec!["a".into()], 10).await.unwrap();
    assert!(initial.events.is_empty());

    db.append("a".into(), vec![event(5, "late")]).await.unwrap();

    db.append("b".into(), vec![event(8, "hidden")])
        .await
        .unwrap();

    let tail = db
        .tail(initial.next_cursor, vec!["a".into()], 10)
        .await
        .unwrap();
    assert_eq!(tail.events.len(), 1);
    assert_eq!(tail.events[0].message, "late");

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn archive_publication_preserves_concurrent_appends() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path())).unwrap();
    db.append("a".into(), (0..500).map(|_| event(10, "old")).collect())
        .await
        .unwrap();
    let maintenance = db.clone();
    let archive = tokio::spawn(async move { maintenance.archive(20).await.unwrap() });

    for _ in 0..10 {
        db.append("a".into(), vec![event(5, "late")]).await.unwrap();
    }

    archive.await.unwrap();

    assert_eq!(db.count(query()).await.unwrap(), 510);

    db.archive(20).await.unwrap();

    assert_eq!(db.count(query()).await.unwrap(), 510);

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn archive_manifest_survives_directory_relocation_and_orphans_are_ignored() {
    let parent = tempfile::tempdir().unwrap();
    let original = parent.path().join("original");
    let restored = parent.path().join("restored");
    let db = Storage::open(config(&original)).unwrap();
    db.append("a".into(), vec![event(10, "archived")])
        .await
        .unwrap();

    db.archive(20).await.unwrap();

    db.shutdown().await.unwrap();

    drop(db);

    std::fs::write(
        original.join("archives/orphan.parquet"),
        "not a valid parquet file",
    )
    .unwrap();
    std::fs::write(original.join("archives/export.tmp"), "incomplete").unwrap();
    std::fs::rename(&original, &restored).unwrap();

    let db = Storage::open(config(&restored)).unwrap();
    assert_eq!(db.count(query()).await.unwrap(), 1);
    assert!(!restored.join("archives/orphan.parquet").exists());
    assert!(!restored.join("archives/export.tmp").exists());

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn response_budget_rejects_large_rows_and_reader_remains_usable() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.reader_threads = 1;
    cfg.max_query_bytes = 512;

    let db = Storage::open(cfg).unwrap();
    db.append("a".into(), vec![event(10, &"large".repeat(1000))])
        .await
        .unwrap();

    assert!(db.search(query()).await.is_err());
    assert_eq!(db.count(query()).await.unwrap(), 1);

    for cursor in ["2:10:1", "-1:10:1", "1:1000:1", "1:10:0"] {
        let mut q = query();
        q.cursor = Some(cursor.into());

        assert!(db.search(q).await.is_err());
    }

    assert!(
        db.tail(Some("-1".into()), vec!["a".into()], 10)
            .await
            .is_err()
    );

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn histogram_preserves_integer_precision_at_extreme_timestamps() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path())).unwrap();
    let start = 9_007_199_254_740_990;
    db.append(
        "a".into(),
        vec![
            event(start, "a"),
            event(start + 3, "b"),
            event(start + 4, "c"),
        ],
    )
    .await
    .unwrap();

    let mut q = query();
    q.from = start;
    q.to = start + 10;
    let buckets = db.histogram(q, 4).await.unwrap();
    assert_eq!(
        buckets
            .iter()
            .map(|b| (b.time, b.count))
            .collect::<Vec<_>>(),
        vec![(start, 2), (start + 4, 1)]
    );

    db.shutdown().await.unwrap();
}

#[test]
fn future_schema_is_rejected_before_migration_mutates_database() {
    let dir = tempfile::tempdir().unwrap();
    let conn = duckdb::Connection::open(dir.path().join("events.duckdb")).unwrap();
    conn.execute_batch(
        "CREATE TABLE schema_version(version INTEGER PRIMARY KEY); \
            INSERT INTO schema_version VALUES (5); \
            CREATE TABLE future_only(marker VARCHAR); \
            INSERT INTO future_only VALUES ('preserved')",
    )
    .unwrap();

    fn schema(conn: &duckdb::Connection) -> Vec<(String, String, String)> {
        conn.prepare(
            "SELECT table_name,column_name,data_type FROM information_schema.columns \
                WHERE table_schema='main' ORDER BY table_name,ordinal_position",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
    }

    let before = schema(&conn);

    drop(conn);

    let error = match Storage::open(config(dir.path())) {
        Ok(_) => panic!("future schema was accepted"),
        Err(error) => error,
    };
    assert_eq!(error.kind, ErrorKind::Unavailable);
    assert_eq!(
        error.message,
        "database schema is newer than this executable"
    );

    let conn = duckdb::Connection::open(dir.path().join("events.duckdb")).unwrap();
    assert_eq!(schema(&conn), before);
    assert_eq!(
        conn.query_row("SELECT max(version) FROM schema_version", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        5
    );
    assert_eq!(
        conn.query_row("SELECT marker FROM future_only", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "preserved"
    );
}

#[tokio::test]
async fn retention_expires_tail_cursor_and_commit_counter_tracks_writer() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path())).unwrap();
    db.append("a".into(), vec![event(20, "initial")])
        .await
        .unwrap();
    let checkpoint = db
        .tail(None, vec!["a".into()], 10)
        .await
        .unwrap()
        .next_cursor;

    db.append("a".into(), vec![event(5, "late expired")])
        .await
        .unwrap();

    assert_eq!(db.committed_events(), 2);

    db.archive(10).await.unwrap();

    db.retain(10).await.unwrap();

    let error = db.tail(checkpoint, vec!["a".into()], 10).await.unwrap_err();
    assert!(error.message.contains("expired by retention"));

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn histogram_supports_negative_time_and_limits_actual_bucket_count() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path())).unwrap();
    db.append(
        "a".into(),
        vec![event(-9, "before epoch"), event(-5, "later")],
    )
    .await
    .unwrap();

    let mut q = query();
    q.from = -10;
    q.to = 0;
    let buckets = db.histogram(q.clone(), 4).await.unwrap();
    assert_eq!(
        buckets
            .iter()
            .map(|b| (b.time, b.count))
            .collect::<Vec<_>>(),
        vec![(-10, 1), (-6, 1)]
    );

    q.from = 0;
    q.to = 10001;
    assert!(db.histogram(q, 1).await.is_err());

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn previous_schema_upgrades_retention_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let conn = duckdb::Connection::open(dir.path().join("events.duckdb")).unwrap();
    conn.execute_batch(include_str!("../migrations/001_initial.sql"))
        .unwrap();
    conn.execute_batch("ALTER TABLE retention_state DROP COLUMN expired_through")
        .unwrap();

    drop(conn);

    let db = Storage::open(config(dir.path())).unwrap();
    let checkpoint = db
        .tail(None, vec!["a".into()], 10)
        .await
        .unwrap()
        .next_cursor;
    db.append("a".into(), vec![event(10, "upgraded")])
        .await
        .unwrap();

    db.retain(20).await.unwrap();

    assert_eq!(
        db.tail(checkpoint, vec!["a".into()], 10)
            .await
            .unwrap_err()
            .kind,
        logbrook::model::ErrorKind::CursorExpired
    );

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn query_bound_cursors_survive_restart_and_original_retention_bounds() {
    use logbrook::query_cursor::{CursorScope, decode_tail, encode_tail};
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let db = Storage::open(cfg.clone()).unwrap();
    db.append(
        "a".into(),
        vec![event(10, "old"), event(20, "middle"), event(30, "new")],
    )
    .await
    .unwrap();

    let mut q = query();
    q.limit = 1;
    q.cursor_scope = CursorScope::Named("default".into());
    let first = db.search(q.clone()).await.unwrap();
    let cursor = first.next_cursor.unwrap();

    db.retain(15).await.unwrap();
    db.shutdown().await.unwrap();
    drop(db);

    let db = Storage::open(cfg).unwrap();
    q.cursor = Some(cursor.clone());
    let second = db.search(q.clone()).await.unwrap();
    assert_eq!(
        second
            .events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["2"]
    );

    for mut changed in [
        Query {
            from: 15,
            ..q.clone()
        },
        Query {
            to: 999,
            ..q.clone()
        },
        Query {
            limit: 2,
            ..q.clone()
        },
        Query {
            sources: vec!["b".into()],
            ..q.clone()
        },
        Query {
            cursor_scope: CursorScope::Legacy,
            ..q.clone()
        },
        Query {
            message: Some(String::new()),
            ..q.clone()
        },
    ] {
        changed.cursor = Some(cursor.clone());
        assert_eq!(
            db.search(changed).await.unwrap_err().kind,
            ErrorKind::Invalid
        );
    }

    let tail = encode_tail(&q, 2).unwrap();
    assert_eq!(decode_tail(&q, &tail).unwrap(), 2);

    let mut changed = q.clone();
    changed.cursor = None;
    changed.service = Some("other".into());
    assert_eq!(
        db.tail_filtered(Some(tail), changed)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Invalid
    );
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn versioned_cursor_positions_still_enforce_snapshot_and_retention() {
    use logbrook::query_cursor::{SearchPosition, encode_search, encode_tail};
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path())).unwrap();
    db.append("a".into(), vec![event(10, "first"), event(20, "last")])
        .await
        .unwrap();
    let q = query();
    for position in [
        SearchPosition {
            high_water_id: 3,
            last_event_time: 10,
            last_event_id: 1,
        },
        SearchPosition {
            high_water_id: -1,
            last_event_time: 10,
            last_event_id: 1,
        },
        SearchPosition {
            high_water_id: 2,
            last_event_time: 1000,
            last_event_id: 1,
        },
        SearchPosition {
            high_water_id: 2,
            last_event_time: 10,
            last_event_id: 0,
        },
        SearchPosition {
            high_water_id: 1,
            last_event_time: 10,
            last_event_id: 2,
        },
    ] {
        let mut page = q.clone();
        page.cursor = Some(encode_search(&q, position).unwrap());
        assert_eq!(db.search(page).await.unwrap_err().kind, ErrorKind::Invalid);
    }

    for after in [-1, 3] {
        assert_eq!(
            db.tail_filtered(Some(encode_tail(&q, after).unwrap()), q.clone())
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Invalid
        );
    }

    let expired_search = encode_search(
        &q,
        SearchPosition {
            high_water_id: 2,
            last_event_time: 10,
            last_event_id: 1,
        },
    )
    .unwrap();
    let expired_tail = encode_tail(&q, 0).unwrap();

    db.retain(15).await.unwrap();
    let mut page = q.clone();
    page.cursor = Some(expired_search);
    assert_eq!(
        db.search(page).await.unwrap_err().kind,
        ErrorKind::CursorExpired
    );
    assert_eq!(
        db.tail_filtered(Some(expired_tail), q.clone())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::CursorExpired
    );

    for legacy in ["0", "2:10:1"] {
        let mut page = q.clone();
        page.cursor = Some(legacy.into());
        assert_eq!(db.search(page).await.unwrap_err().kind, ErrorKind::Invalid);
        assert_eq!(
            db.tail_filtered(Some(legacy.into()), q.clone())
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Invalid
        );
    }

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn versioned_tail_frame_budget_is_exact_for_logs_and_checkpoints() {
    use logbrook::query_cursor::{CursorScope, decode_tail, encode_tail};
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path())).unwrap();
    db.append("a".into(), vec![event(10, "💡é")]).await.unwrap();

    let mut q = query();
    q.cursor_scope = CursorScope::Named("long-index-name".into());
    let stored = db.search(q.clone()).await.unwrap().events.remove(0);
    let log_bytes =
        serde_json::to_vec(&stored).unwrap().len() + encode_tail(&q, 1).unwrap().len() + 24;
    let start = encode_tail(&q, 0).unwrap();
    assert_eq!(
        db.tail_filtered_bounded(Some(start.clone()), q.clone(), log_bytes - 1)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::TooLarge
    );

    let page = db
        .tail_filtered_bounded(Some(start), q.clone(), log_bytes)
        .await
        .unwrap();
    assert_eq!(page.events.len(), 1);
    assert_eq!(
        decode_tail(&q, page.next_cursor.as_deref().unwrap()).unwrap(),
        1
    );

    let checkpoint_bytes = encode_tail(&q, 1).unwrap().len() + 36;
    assert_eq!(
        db.tail_filtered_bounded(page.next_cursor.clone(), q.clone(), checkpoint_bytes - 1)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::TooLarge
    );

    assert!(
        db.tail_filtered_bounded(page.next_cursor, q.clone(), checkpoint_bytes)
            .await
            .unwrap()
            .events
            .is_empty()
    );

    assert_eq!(
        db.tail_filtered_bounded(None, q.clone(), checkpoint_bytes - 1)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::TooLarge
    );

    assert!(
        db.tail_filtered_bounded(None, q, checkpoint_bytes)
            .await
            .unwrap()
            .events
            .is_empty()
    );
    db.shutdown().await.unwrap();
}
