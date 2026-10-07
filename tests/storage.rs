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
        attributes: json!({"nested":{"ok":true}}),
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
         INSERT INTO schema_version VALUES (4); \
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
        4
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

    db.append("a".into(), vec![event(10, "upgraded")])
        .await
        .unwrap();

    db.retain(20).await.unwrap();

    assert!(
        db.tail(Some("0".into()), vec!["a".into()], 10)
            .await
            .is_err()
    );

    db.shutdown().await.unwrap();
}
