//! Independent public-API checks for durable string attribute projections.
use logbrook::{
    attribute_filter::AttributeFilters,
    config::StorageConfig,
    model::{ErrorKind, NormalizedEvent, Query},
    storage::Storage,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path};

fn config(path: &Path, promoted: bool) -> StorageConfig {
    StorageConfig {
        data_dir: path.to_owned(),
        promoted_string_keys: if promoted {
            vec!["run/~雪".into(), "user".into()]
        } else {
            vec![]
        },
        reader_threads: 1,
        duckdb_threads: 1,
        memory_limit: "128MB".into(),
        query_timeout_ms: 10000,
        ..StorageConfig::default()
    }
}

fn event(index: usize, attributes: Value) -> NormalizedEvent {
    NormalizedEvent {
        event_time: 10 + (index / 2) as i64 * 10,
        level: if index.is_multiple_of(2) { 30 } else { 50 },
        service: Some(format!("service-{}", index % 2)),
        logger: Some("worker".into()),
        host: Some("host".into()),
        pid: None,
        message: index.to_string(),
        attributes,
    }
}

fn query(conditions: &[(&str, Value)]) -> Query {
    Query {
        from: 0,
        to: 1000,
        limit: 2,
        attributes: AttributeFilters::parse(
            &json!({
                "attributes": conditions
                    .iter()
                    .map(|(key, value)| {
                        json!({
                            "key": key,
                            "op": "eq",
                            "value": value,
                        })
                    })
                    .collect::<Vec<_>>(),
            })
            .to_string(),
            usize::MAX,
        )
        .unwrap(),
        ..Query::default()
    }
}

fn rows() -> Vec<NormalizedEvent> {
    [
        json!({
            "run/~雪": "é\n\u{0}",
            "user": "a",
            "active": true,
            "n": 9007199254740993_u64,
        }),
        json!({
            "run/~雪": "é\n\u{0}",
            "user": "a",
            "active": true,
            "n": 9007199254740993_u64,
        }),
        json!({
            "run/~雪": "é\n\u{0}",
            "user": "b",
            "active": true,
            "n": 9007199254740993_u64,
        }),
        json!({
            "run/~雪": "é\n\u{0}",
            "user": "a",
            "active": false,
            "n": 9007199254740993_u64,
        }),
        json!({
            "run/~雪": "e\u{301}\n\u{0}",
            "user": "a",
            "active": true,
        }),
        json!({
            "run/~雪": true,
            "user": "a",
            "active": true,
        }),
        json!({
            "run/~雪": 123,
            "user": "a",
        }),
        json!({
            "run/~雪": null,
            "user": "a",
        }),
        json!({
            "run/~雪": ["é\n\u{0}"],
            "user": "a",
        }),
        json!({
            "run/~雪": {
                "value": "é\n\u{0}",
            },
            "user": "a",
        }),
        json!(["é\n\u{0}", "a"]),
        json!("é\n\u{0}"),
        Value::Null,
        json!({
            "user": "a",
        }),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, attrs)| event(index, attrs))
    .collect()
}

async fn assert_oracle(db: &Storage, data: &[NormalizedEvent], conditions: &[(&str, Value)]) {
    let q = query(conditions);
    let mut expected: Vec<_> = data
        .iter()
        .enumerate()
        .filter(|(_, event)| {
            event.attributes.as_object().is_some_and(|object| {
                conditions
                    .iter()
                    .all(|(key, value)| object.get(*key) == Some(value))
            })
        })
        .map(|(index, event)| (event.event_time, index + 1, event))
        .collect();
    expected.sort_by_key(|(time, id, _)| std::cmp::Reverse((*time, *id)));

    let mut actual = Vec::new();
    let mut page_query = q.clone();
    loop {
        let page = db.search(page_query.clone()).await.unwrap();
        actual.extend(page.events.into_iter().map(|event| event.id));
        if page.next_cursor.is_none() {
            break;
        }
        page_query.cursor = page.next_cursor;
    }

    assert_eq!(
        actual,
        expected
            .iter()
            .map(|(_, id, _)| id.to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(db.count(q.clone()).await.unwrap(), expected.len() as u64);

    let mut buckets = BTreeMap::new();
    for (time, _, _) in &expected {
        *buckets.entry(time / 10 * 10).or_insert(0_u64) += 1;
    }

    assert_eq!(
        db.histogram(q.clone(), 10)
            .await
            .unwrap()
            .into_iter()
            .map(|b| (b.time, b.count))
            .collect::<BTreeMap<_, _>>(),
        buckets
    );

    let facets = db.facets(q.clone()).await.unwrap();
    let mut services = BTreeMap::new();
    for (_, _, event) in &expected {
        *services
            .entry(event.service.clone().unwrap())
            .or_insert(0_u64) += 1;
    }

    assert_eq!(
        facets
            .services
            .into_iter()
            .map(|f| (f.value, f.count))
            .collect::<BTreeMap<_, _>>(),
        services
    );

    for dimension in [facets.sources, facets.loggers, facets.hosts, facets.levels] {
        assert_eq!(
            dimension.iter().map(|f| f.count).sum::<u64>(),
            expected.len() as u64
        );
    }

    let mut after = Some(logbrook::query_cursor::encode_tail(&q, 0).unwrap());
    let mut tail_ids = Vec::new();
    loop {
        let page = db.tail_filtered(after, q.clone()).await.unwrap();
        let empty = page.events.is_empty();
        tail_ids.extend(page.events.into_iter().map(|event| event.id));
        after = page.next_cursor;
        if empty {
            break;
        }
    }

    let mut expected_ids = expected.iter().map(|(_, id, _)| *id).collect::<Vec<_>>();
    expected_ids.sort();
    assert_eq!(
        tail_ids,
        expected_ids
            .into_iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn serde_oracle_covers_hot_legacy_typed_mixed_backfill_and_restart() {
    for legacy in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let data = rows();
        let db = Storage::open(config(dir.path(), !legacy)).unwrap();
        db.append("source".into(), data[..4].to_vec())
            .await
            .unwrap();

        if legacy {
            db.archive(1000).await.unwrap();
            db.shutdown().await.unwrap();
            drop(db);
        } else {
            db.shutdown().await.unwrap();
            drop(db);
        }

        let db = Storage::open(config(dir.path(), true)).unwrap();
        assert_oracle(&db, &data[..4], &[("user", json!("a"))]).await;
        db.append("source".into(), data[4..].to_vec())
            .await
            .unwrap();

        let conditions = [
            vec![("run/~雪", json!("é\n\u{0}"))],
            vec![("run/~雪", json!("é\n\u{0}")), ("user", json!("a"))],
            vec![
                ("run/~雪", json!("é\n\u{0}")),
                ("user", json!("a")),
                ("active", json!(true)),
                ("n", json!(9007199254740993_u64)),
            ],
            vec![("run/~雪", json!(true))],
            vec![("user", json!("a"))],
        ];
        for conditions in &conditions {
            assert_oracle(&db, &data, conditions).await;
        }

        db.archive(1000).await.unwrap();
        for conditions in &conditions {
            assert_oracle(&db, &data, conditions).await;
        }

        db.compact().await.unwrap();
        db.shutdown().await.unwrap();
        drop(db);

        // Omission inherits the recorded layout, and IDs continue after recovery.
        let db = Storage::open(config(dir.path(), false)).unwrap();
        for conditions in &conditions {
            assert_oracle(&db, &data, conditions).await;
        }

        db.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn projected_paging_highwater_late_ids_and_retention_preserve_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path(), true)).unwrap();
    let attrs = json!({
        "user": "a",
    });
    db.append(
        "s".into(),
        (0..5).map(|n| event(n, attrs.clone())).collect(),
    )
    .await
    .unwrap();
    db.archive(20).await.unwrap();

    let q = query(&[("user", json!("a"))]);
    let first = db.search(q.clone()).await.unwrap();
    assert_eq!(
        first
            .events
            .iter()
            .map(|e| e.id.as_str())
            .collect::<Vec<_>>(),
        ["5", "4"]
    );

    db.append("s".into(), vec![event(0, attrs)]).await.unwrap();
    db.archive(1000).await.unwrap();

    let mut next = q.clone();
    next.cursor = first.next_cursor;
    let second = db.search(next.clone()).await.unwrap();
    assert_eq!(
        second
            .events
            .iter()
            .map(|e| e.id.as_str())
            .collect::<Vec<_>>(),
        ["3", "2"]
    );

    next.cursor = second.next_cursor;
    assert_eq!(db.search(next).await.unwrap().events[0].id, "1");

    assert_eq!(db.count(q.clone()).await.unwrap(), 6);
    db.retain(20).await.unwrap();
    let mut retained = q;
    retained.from = 20;
    assert_eq!(db.count(retained).await.unwrap(), 3);
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn changed_layout_rejected_and_indexes_remain_independent() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let db = Storage::open(config(a.path(), true)).unwrap();
    db.shutdown().await.unwrap();
    drop(db);

    let mut changed = config(a.path(), true);
    changed.promoted_string_keys = vec!["different".into()];
    let error = Storage::open(changed)
        .err()
        .expect("layout change rejected");
    assert_eq!(error.kind, ErrorKind::Invalid);

    let db = Storage::open(config(b.path(), false)).unwrap();
    db.shutdown().await.unwrap();
    drop(db);

    let conn = duckdb::Connection::open(b.path().join("events.duckdb")).unwrap();
    assert_eq!(
        conn.query_row("SELECT max(version) FROM schema_version", [], |r| r
            .get::<_, i32>(0))
            .unwrap(),
        3
    );
}

#[tokio::test]
async fn ordinary_sql_cannot_create_stale_or_invalid_covered_hot_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path(), true)).unwrap();
    db.append(
        "s".into(),
        vec![event(
            0,
            json!({
                "user": "a",
                "run/~雪": "r",
            }),
        )],
    )
    .await
    .unwrap();
    db.shutdown().await.unwrap();
    drop(db);

    let conn = duckdb::Connection::open(dir.path().join("events.duckdb")).unwrap();
    for assignment in [
        "pa_0='stale'",
        "pa_0=NULL",
        "attributes='broken json'",
        "pa_covered=FALSE",
        "pa_1='wrong'",
    ] {
        assert!(
            conn.execute(&format!("UPDATE events SET {assignment} WHERE id=1"), [])
                .is_err(),
            "accepted {assignment}"
        );
    }

    assert_eq!(
        conn.query_row("SELECT attributes FROM events WHERE id=1", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        json!({
            "user": "a",
            "run/~雪": "r",
        })
        .to_string()
    );

    // Explicitly uncovered data is valid only when every slot is NULL; its
    // malformed JSON must still reach the existing error-producing fallback.
    conn.execute(
        "UPDATE events SET \
            pa_covered=FALSE,pa_0=NULL,pa_1=NULL,attributes='broken json' WHERE id=1",
        [],
    )
    .unwrap();
    drop(conn);

    let db = Storage::open(config(dir.path(), false)).unwrap();
    assert!(db.count(query(&[("user", json!("a"))])).await.is_err());
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn single_legacy_archive_backfill_is_bounded_finishes_and_preserves_malformed_errors() {
    for malformed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = Storage::open(config(dir.path(), false)).unwrap();
        db.append(
            "s".into(),
            vec![event(
                0,
                json!({
                    "user": "a",
                }),
            )],
        )
        .await
        .unwrap();
        db.shutdown().await.unwrap();
        drop(db);

        if malformed {
            let conn = duckdb::Connection::open(dir.path().join("events.duckdb")).unwrap();
            conn.execute("UPDATE events SET attributes='malformed'", [])
                .unwrap();
        }

        let db = Storage::open(config(dir.path(), false)).unwrap();
        db.archive(1000).await.unwrap();
        db.shutdown().await.unwrap();
        drop(db);

        let db = Storage::open(config(dir.path(), true)).unwrap();
        assert!(
            db.compact().await.unwrap() > 0,
            "a single legacy file needs a bounded rewrite"
        );
        assert_eq!(
            db.compact().await.unwrap(),
            0,
            "typed output must not be rewritten forever"
        );

        let result = db.count(query(&[("user", json!("a"))])).await;
        if malformed {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap(), 1);
        }

        let status = db.projection_status().await.unwrap();
        assert_eq!(status.archived_rows, 1);
        assert_eq!(status.archived_covered_rows, if malformed { 0 } else { 1 });
        assert_eq!(status.legacy_archive_files, 0);

        db.shutdown().await.unwrap();
        drop(db);

        let db = Storage::open(config(dir.path(), false)).unwrap();
        let result = db.count(query(&[("user", json!("a"))])).await;
        if malformed {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap(), 1);
        }

        db.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn same_size_typed_archive_tamper_is_integrity_error_for_queries_and_recovery() {
    use std::io::{Read, Seek, SeekFrom, Write};
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path(), true)).unwrap();
    db.append("s".into(), rows()).await.unwrap();
    db.archive(1000).await.unwrap();
    db.append("s".into(), rows()).await.unwrap();
    db.archive(1000).await.unwrap();

    let path = std::fs::read_dir(dir.path().join("archives"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|ext| ext == "parquet"))
        .unwrap();
    let before = std::fs::metadata(&path).unwrap().len();
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let offset = before / 2;
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
    drop(file);

    assert_eq!(std::fs::metadata(&path).unwrap().len(), before);

    let q = query(&[("user", json!("a"))]);
    assert_eq!(
        db.count(q.clone()).await.unwrap_err().kind,
        ErrorKind::Integrity
    );

    // An integrity failure closes admission across the storage instance.
    assert!(!db.is_ready());
    assert_eq!(
        db.search(q.clone()).await.unwrap_err().kind,
        ErrorKind::Unavailable
    );
    assert_eq!(
        db.histogram(q.clone(), 10).await.unwrap_err().kind,
        ErrorKind::Unavailable
    );
    assert_eq!(
        db.facets(q.clone()).await.unwrap_err().kind,
        ErrorKind::Unavailable
    );

    let cursor = Some(logbrook::query_cursor::encode_tail(&q, 0).unwrap());
    assert_eq!(
        db.tail_filtered(cursor, q).await.unwrap_err().kind,
        ErrorKind::Unavailable
    );
    assert_eq!(db.compact().await.unwrap_err().kind, ErrorKind::Unavailable);

    db.shutdown().await.unwrap();
    drop(db);

    assert_eq!(
        Storage::open(config(dir.path(), false))
            .err()
            .expect("tampered archive recovery rejected")
            .kind,
        ErrorKind::Integrity
    );
}

#[tokio::test]
async fn supported_legacy_versions_activate_without_rewriting_malformed_hot_json() {
    for version in [1, 2, 3] {
        let dir = tempfile::tempdir().unwrap();
        let conn = duckdb::Connection::open(dir.path().join("events.duckdb")).unwrap();
        conn.execute_batch(include_str!("../migrations/001_initial.sql"))
            .unwrap();

        if version >= 2 {
            conn.execute("INSERT INTO schema_version VALUES (2)", [])
                .unwrap();
        }

        if version >= 3 {
            conn.execute_batch(include_str!("../migrations/002_archive_metadata.sql"))
                .unwrap();
        }

        conn.execute(
            "INSERT INTO events VALUES (1,'s',10,10,30,NULL,NULL,NULL,NULL,'old','broken')",
            [],
        )
        .unwrap();
        conn.execute("UPDATE ingest_state SET next_id=2", [])
            .unwrap();
        drop(conn);

        let db = Storage::open(config(dir.path(), true)).unwrap();
        assert!(db.count(query(&[("user", json!("a"))])).await.is_err());

        db.append(
            "s".into(),
            vec![event(
                1,
                json!({
                    "user": "a",
                }),
            )],
        )
        .await
        .unwrap();
        db.shutdown().await.unwrap();
        drop(db);

        let conn = duckdb::Connection::open(dir.path().join("events.duckdb")).unwrap();
        assert_eq!(
            conn.query_row("SELECT max(version) FROM schema_version", [], |r| r
                .get::<_, i32>(0))
                .unwrap(),
            4
        );
        assert_eq!(
            conn.query_row("SELECT attributes FROM events WHERE id=1", [], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "broken"
        );
        assert!(
            !conn
                .query_row("SELECT pa_covered FROM events WHERE id=1", [], |r| r
                    .get::<_, bool>(0))
                .unwrap()
        );
        assert!(
            conn.query_row("SELECT pa_covered FROM events WHERE id=2", [], |r| r
                .get::<_, bool>(0))
                .unwrap()
        );
    }
}

#[tokio::test]
async fn oversized_legacy_archive_stays_queryable_without_exceeding_backfill_budget() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path(), false)).unwrap();
    db.append("s".into(), rows()).await.unwrap();
    db.archive(1000).await.unwrap();
    db.shutdown().await.unwrap();
    drop(db);

    let mut settings = config(dir.path(), true);
    settings.compact_max_bytes = 1024;
    let db = Storage::open(settings).unwrap();
    assert_eq!(db.compact().await.unwrap(), 0);

    let status = db.projection_status().await.unwrap();
    assert_eq!(status.legacy_archive_files, 1);
    assert_eq!(status.archived_covered_rows, 0);
    assert_eq!(db.count(query(&[("user", json!("a"))])).await.unwrap(), 10);
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_projection_manifest_metadata_is_rejected_on_recovery() {
    for mutation in [
        "UPDATE archives SET projection_sha256=NULL",
        "UPDATE archives SET projection_sha256='bad'",
        "UPDATE archives SET schema_version=99",
        "UPDATE projection_layout SET keys_json='[\"other\"]'",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = Storage::open(config(dir.path(), true)).unwrap();
        db.append("s".into(), rows()).await.unwrap();
        db.archive(1000).await.unwrap();
        db.shutdown().await.unwrap();
        drop(db);

        let conn = duckdb::Connection::open(dir.path().join("events.duckdb")).unwrap();
        conn.execute(mutation, []).unwrap();
        drop(conn);

        let error = Storage::open(config(dir.path(), false))
            .err()
            .expect("invalid durable metadata must be rejected");
        assert_eq!(error.kind, ErrorKind::Integrity, "{mutation}: {error}");
    }
}
