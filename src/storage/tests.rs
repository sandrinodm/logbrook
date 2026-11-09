//! Storage integration cases for admission, snapshots, maintenance, and worker lifetimes.
use super::archive::ExportRequest;
use super::*;

fn database(path: &Path) -> Connection {
    let mut conn = Connection::open(path).unwrap();
    migrate(&mut conn).unwrap();

    conn
}

fn event() -> NormalizedEvent {
    NormalizedEvent {
        event_time: 10,
        level: 30,
        service: None,
        logger: None,
        host: None,
        pid: None,
        message: "test".into(),
        attributes: serde_json::json!({}),
    }
}

#[test]
fn progress_tracking_is_enabled_separately_without_terminal_output() {
    let conn = Connection::open_in_memory().unwrap();
    let cloned = conn.try_clone().unwrap();
    for connection in [&conn, &cloned] {
        let handle = track_query_progress(connection).unwrap();
        let settings: (bool, bool) = connection
            .query_row(
                "SELECT current_setting('enable_progress_bar'), \
                 current_setting('enable_progress_bar_print')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(settings, (true, false));
        assert!(handle.is_connected());
        assert_eq!(handle.query_progress().percentage(), None);
    }
}

#[test]
fn progress_metrics_observe_active_native_work_and_clear_after_completion() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("SET threads=1").unwrap();
    let handle = track_query_progress(&conn).unwrap();
    let interrupt = conn.interrupt_handle();
    let maintenance = Connection::open_in_memory().unwrap();
    let maintenance_handle = track_query_progress(&maintenance).unwrap();
    let worker =
        thread::spawn(move || conn.execute_batch("SELECT sum(i) FROM range(1000000000000) t(i)"));

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut metrics = String::new();
    while Instant::now() < deadline && !worker.is_finished() {
        metrics.clear();
        push_query_progress(
            &mut metrics,
            std::slice::from_ref(&handle),
            &maintenance_handle,
        );
        if metrics
            .contains("logbrook_storage_query_progress_available{role=\"reader\",worker=\"0\"} 1\n")
        {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    // Always stop the native work before asserting, including when an estimate
    // was unexpectedly unavailable, so a failing test cannot leave it running.
    interrupt.interrupt();
    let result = worker.join().unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .to_ascii_lowercase()
            .contains("interrupt")
    );

    let sample = |name: &str| -> f64 {
        let prefix =
            format!("logbrook_storage_query_progress_{name}{{role=\"reader\",worker=\"0\"}} ");
        metrics
            .lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .unwrap()
            .parse()
            .unwrap()
    };
    assert_eq!(
        sample("available"),
        1.0,
        "native query did not report progress"
    );
    assert!((0.0..=1.0).contains(&sample("fraction")));
    assert!(sample("rows_total") > 0.0);
    assert!(sample("rows_processed") <= sample("rows_total"));

    metrics.clear();
    push_query_progress(&mut metrics, &[handle], &maintenance_handle);
    assert!(
        metrics.contains(
            "logbrook_storage_query_progress_available{role=\"reader\",worker=\"0\"} 0\n"
        )
    );
    assert!(
        metrics.contains(
            "logbrook_storage_query_progress_fraction{role=\"reader\",worker=\"0\"} NaN\n"
        )
    );
}

#[tokio::test]
async fn progress_metrics_mark_idle_and_closed_connections_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        reader_threads: 2,
        ..StorageConfig::default()
    })
    .unwrap();
    for closed in [false, true] {
        if closed {
            db.shutdown().await.unwrap();
        }

        let metrics = db.metrics_prometheus();
        for (role, worker) in [("reader", 0), ("reader", 1), ("maintenance", 0)] {
            let labels = format!("{{role=\"{role}\",worker=\"{worker}\"}}");
            assert!(metrics.contains(&format!(
                "logbrook_storage_query_progress_available{labels} 0\n"
            )));
            for gauge in ["fraction", "rows_processed", "rows_total"] {
                assert!(metrics.contains(&format!(
                    "logbrook_storage_query_progress_{gauge}{labels} NaN\n"
                )));
            }
        }

        assert!(
            db.inner
                .query_progress
                .iter()
                .all(|handle| handle.is_connected() != closed)
        );

        assert_ne!(db.inner.maintenance_progress.is_connected(), closed);
    }
}

#[test]
fn continuation_archive_upper_bound_preserves_ties_and_timestamp_limits() {
    for (to, last_event_time, expected) in [
        (100, 40, 41),
        (41, 40, 41),
        (40, 40, 40),
        (i64::MAX, i64::MIN, i64::MIN + 1),
        (i64::MAX, i64::MAX - 1, i64::MAX),
        (i64::MAX, i64::MAX, i64::MAX),
        (100, i64::MAX, 100),
    ] {
        assert_eq!(
            query::archive_upper_bound(to, last_event_time),
            expected,
            "to={to}, last_event_time={last_event_time}"
        );
    }
}

#[test]
fn continuation_archive_manifest_prunes_newer_files_and_keeps_timestamp_ties() {
    let dir = tempfile::tempdir().unwrap();
    let conn = database(&dir.path().join("events.duckdb"));
    for (name, min_time, max_time) in [
        ("older.parquet", 10, 30),
        ("tie.parquet", 40, 40),
        ("compact-100.parquet", 40, 40),
        ("archive-9.parquet", 20, 30),
        ("overlap.parquet", 30, 60),
        ("newer.parquet", 41, 60),
        ("expired.parquet", 0, 9),
    ] {
        conn.execute(
            "INSERT INTO archives(path,min_time,max_time,row_count,schema_version) \
             VALUES (?,?,?,1,1)",
            params![name, min_time, max_time],
        )
        .unwrap();
        // Unselected archives are deliberately absent. Selection must prune
        // them before checking file integrity or constructing read_parquet.
        if !matches!(name, "newer.parquet" | "expired.parquet") {
            std::fs::write(dir.path().join(name), b"manifest selection only").unwrap();
        }
    }

    let q = Query {
        from: 10,
        to: query::archive_upper_bound(100, 40),
        ..Query::default()
    };
    let data = dataset(&conn, dir.path(), &q).unwrap();
    for name in [
        "older.parquet",
        "tie.parquet",
        "compact-100.parquet",
        "archive-9.parquet",
        "overlap.parquet",
    ] {
        assert!(data.contains(name));
    }

    assert!(!data.contains("newer.parquet"));
    assert!(!data.contains("expired.parquet"));

    // Generic callers retain path ordering, including compaction and digit widths.
    let mut expected = [
        "archive-9.parquet",
        "compact-100.parquet",
        "older.parquet",
        "overlap.parquet",
        "tie.parquet",
    ];
    expected.sort();
    let positions: Vec<_> = expected
        .iter()
        .map(|name| data.find(name).unwrap())
        .collect();
    assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));

    // Unknown legacy max_id remains selected for tail catch-up. Known old files
    // can be pruned, while generic/aggregate dataset selection remains unchanged.
    conn.execute(
        "UPDATE archives SET max_id=2 WHERE path='archive-9.parquet'",
        [],
    )
    .unwrap();

    let tail = query::dataset_after(&conn, dir.path(), &q, Some(2), None).unwrap();
    assert!(!tail.contains("archive-9.parquet"));
    assert!(tail.contains("compact-100.parquet"));
    assert!(
        dataset(&conn, dir.path(), &q)
            .unwrap()
            .contains("archive-9.parquet")
    );
}

#[test]
fn selected_archives_are_all_checked_before_query_execution() {
    let dir = tempfile::tempdir().unwrap();
    let conn = database(&dir.path().join("db"));
    // Even a file that a top-N heap might prune must pass integrity validation.
    conn.execute(
        "INSERT INTO archives(path,min_time,max_time,row_count,schema_version,file_bytes) \
         VALUES ('compact-999.parquet',10,10,1,3,8)",
        [],
    )
    .unwrap();

    let q = Query {
        from: 0,
        to: 100,
        ..Query::default()
    };
    assert_eq!(
        dataset(&conn, dir.path(), &q).unwrap_err().kind,
        ErrorKind::Integrity
    );

    std::fs::write(dir.path().join("compact-999.parquet"), b"short").unwrap();
    assert_eq!(
        dataset(&conn, dir.path(), &q).unwrap_err().kind,
        ErrorKind::Integrity
    );
    std::fs::write(dir.path().join("compact-999.parquet"), b"12345678").unwrap();

    assert!(dataset(&conn, dir.path(), &q).is_ok());

    let cancelled = AtomicBool::new(true);
    let result = query::dataset_after(
        &conn,
        dir.path(),
        &q,
        None,
        Some(query::QueryDeadline {
            cancelled: &cancelled,
            at: Instant::now() + Duration::from_secs(10),
        }),
    );
    assert!(result.is_err());
}

#[tokio::test]
async fn search_pages_keep_archive_hot_ties_late_ids_and_high_water_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let archives = dir.path().join("archives");
    std::fs::create_dir(&archives).unwrap();
    let mut conn = database(&dir.path().join("events.duckdb"));

    let at = |time| {
        let mut e = event();
        e.event_time = time;
        e
    };
    // IDs rise independently of event time. Ties span hot rows and three files.
    for (index, times) in [[40, 50], [50, 10], [50, 70]].into_iter().enumerate() {
        append(&mut conn, "a", &times.map(at)).unwrap();
        let file = export(&mut conn, &archives, 80).unwrap().unwrap();
        let original = file.path.clone();
        publish(&mut conn, file).unwrap();
        if index == 2 {
            std::fs::rename(archives.join(&original), archives.join("compact-6.parquet")).unwrap();
            conn.execute(
                "UPDATE archives SET path='compact-6.parquet' WHERE path=?",
                params![original],
            )
            .unwrap();
        }
    }

    // Retain legacy nullable manifest bounds/byte sizes for query compatibility.
    conn.execute(
        "UPDATE archives SET min_id=NULL,max_id=NULL,file_bytes=NULL,schema_version=1",
        [],
    )
    .unwrap();

    append(&mut conn, "a", &[at(50), at(90)]).unwrap();
    drop(conn);

    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();

    let mut q = Query {
        from: 0,
        to: 100,
        limit: 2,
        ..Query::default()
    };
    let first = db.search(q.clone()).await.unwrap();
    let ids = |events: &[Event]| {
        events
            .iter()
            .map(|e| e.id.parse::<u64>().unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&first.events), [8, 6]);

    let position =
        crate::query_cursor::decode_search(&q, first.next_cursor.as_deref().unwrap()).unwrap();
    assert_eq!(position.high_water_id, 8);

    // Later ingestion must stay absent even after hot rows move into an archive.
    db.append("a".into(), vec![at(50), at(5)]).await.unwrap();
    db.archive(60).await.unwrap();

    q.cursor = first.next_cursor;
    let mut pages = vec![];
    loop {
        let page = db.search(q.clone()).await.unwrap();
        pages.push(ids(&page.events));
        q.cursor = page.next_cursor;
        if q.cursor.is_none() {
            break;
        }
    }

    assert_eq!(pages, [vec![7, 5], vec![3, 2], vec![1, 4]]);

    q.cursor = None;
    assert_eq!(db.count(q.clone()).await.unwrap(), 10);

    q.limit = 100;
    let tail = db
        .tail_filtered(Some(crate::query_cursor::encode_tail(&q, 0).unwrap()), q)
        .await
        .unwrap();
    assert_eq!(ids(&tail.events), (1..=10).collect::<Vec<_>>());

    db.shutdown().await.unwrap();
}

#[test]
fn attribute_string_compiler_binds_scalar_paths_and_values_for_each_shape() {
    let conn = Connection::open_in_memory().unwrap();
    let expected = "' OR true --";
    for count in 1..=3 {
        let conditions: Vec<_> = (0..count)
            .map(|index| {
                serde_json::json!({"key": format!("a/b~{index}"), "op": "eq", "value": expected})
            })
            .collect();

        let q = Query {
            from: 0,
            to: 100,
            attributes: crate::attribute_filter::AttributeFilters::parse(
                &serde_json::json!({"attributes": conditions}).to_string(),
                usize::MAX,
            )
            .unwrap(),
            ..Query::default()
        };
        let (filter, values) = query::filters(&q, 10);
        assert!(!filter.contains(expected));

        assert_eq!(
            filter.matches("json_type(attributes) = 'OBJECT'").count(),
            1
        );

        assert_eq!(
            filter.contains("json_extract_string(attributes, [?, ?])"),
            count == 2
        );

        assert!(
            values
                .iter()
                .all(|value| matches!(value, Value::BigInt(_) | Value::Text(_)))
        );

        assert_eq!(filter.matches('?').count(), values.len());

        let matching: serde_json::Map<_, _> = (0..count)
            .map(|index| (format!("a/b~{index}"), serde_json::json!(expected)))
            .collect();
        let sql = format!(
            "SELECT {filter} FROM (SELECT 10::BIGINT event_time,1::BIGINT id,?::VARCHAR attributes)"
        );
        let mut bound = values.clone();
        bound.push(Value::Text(serde_json::Value::Object(matching).to_string()));
        let matches: bool = conn
            .query_row(&sql, params_from_iter(bound), |row| row.get(0))
            .unwrap();
        assert!(matches);

        let mut bound = values;
        bound.push(Value::Text("{}".to_owned()));
        let matches: bool = conn
            .query_row(&sql, params_from_iter(bound), |row| row.get(0))
            .unwrap();
        assert!(!matches);
    }
}

fn maintenance_budget(milliseconds: u64) -> MaintenanceBudget {
    MaintenanceBudget {
        cancelled: Arc::new(AtomicBool::new(false)),
        deadline: Instant::now() + Duration::from_millis(milliseconds),
    }
}

struct WriterRelease(Option<std::sync::mpsc::Sender<()>>);

impl WriterRelease {
    fn send(mut self, value: ()) -> std::result::Result<(), std::sync::mpsc::SendError<()>> {
        self.0.take().expect("writer release sender").send(value)
    }
}

impl Drop for WriterRelease {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

async fn writer_barrier(storage: &Storage) -> WriterRelease {
    let (started, receiver) = oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    storage
        .inner
        .writer
        .send(Write::Test(Box::new(move |_, _| {
            started.send(()).unwrap();
            blocked.recv_timeout(Duration::from_secs(10)).unwrap();
        })))
        .await
        .unwrap();
    receiver.await.unwrap();

    WriterRelease(Some(release))
}

async fn wait_for_queued(storage: &Storage, count: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while storage.inner.writer.max_capacity() - storage.inner.writer.capacity() < count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("preparation must enqueue before the test advances");
}

fn assert_timeout<T>(result: Result<T>) {
    assert!(matches!(
        result,
        Err(Error {
            kind: ErrorKind::Timeout,
            ..
        })
    ));
}

#[test]
fn maintenance_handoff_queue_submission_and_reply_observe_deadlines() {
    let (writer, _queue) = mpsc::channel(1);
    let budget = maintenance_budget(30);
    let (reply, _answer) = oneshot::channel();
    writer
        .try_send(Write::Checkpoint(budget.clone(), reply))
        .unwrap();

    let (reply, _answer) = oneshot::channel();
    let started = Instant::now();
    assert_timeout(maintenance_send(
        &writer,
        Write::Checkpoint(budget.clone(), reply),
        &budget,
    ));
    assert!(started.elapsed() < Duration::from_secs(1));

    let budget = maintenance_budget(30);
    let (_reply, answer) = oneshot::channel::<Result<()>>();
    assert_timeout(maintenance_reply(answer, &budget));

    let budget = maintenance_budget(1000);
    budget.cancelled.store(true, Ordering::Release);
    let (reply, _answer) = oneshot::channel();
    assert_timeout(maintenance_send(
        &writer,
        Write::Checkpoint(budget.clone(), reply),
        &budget,
    ));
}

#[tokio::test]
async fn queued_expired_or_cancelled_writer_retention_never_mutates_rows() {
    let dir = tempfile::tempdir().unwrap();

    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();
    db.append("a".into(), vec![event()]).await.unwrap();

    for cancelled in [false, true] {
        let release = writer_barrier(&db).await;
        let budget = maintenance_budget(if cancelled { 1000 } else { 0 });
        let (reply, answer) = oneshot::channel();
        db.inner
            .writer
            .send(Write::Retain(20, 1, budget.clone(), reply))
            .await
            .unwrap();

        if cancelled {
            budget.cancelled.store(true, Ordering::Release);
        }

        release.send(()).unwrap();

        assert_timeout(
            tokio::time::timeout(Duration::from_secs(1), answer)
                .await
                .unwrap()
                .unwrap(),
        );
    }

    assert_eq!(db.append("a".into(), vec![event()]).await.unwrap(), 1);
    assert_eq!(
        db.count(Query {
            from: 0,
            to: 100,
            limit: 10,
            ..Query::default()
        })
        .await
        .unwrap(),
        2
    );
    assert!(db.is_ready());

    tokio::time::timeout(Duration::from_secs(2), db.shutdown())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn timed_out_publication_waiter_keeps_output_owned_until_writer_rejects_it() {
    let dir = tempfile::tempdir().unwrap();

    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();
    db.append("a".into(), vec![event()]).await.unwrap();

    let archives = dir.path().join("archives");
    let (reply, answer) = oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    db.inner
        .writer
        .send(Write::Test(Box::new(move |conn, _| {
            reply
                .send(export(conn, &archives, 20).unwrap().unwrap())
                .ok()
                .unwrap();
            blocked.recv().unwrap();
        })))
        .await
        .unwrap();

    let file = answer.await.unwrap();
    let path = dir.path().join("archives").join(&file.path);
    let cleanup = file.artifact.clone();
    let budget = maintenance_budget(30);
    let (reply, answer) = oneshot::channel();
    db.inner
        .writer
        .send(Write::Publish(file, budget.clone(), reply))
        .await
        .unwrap();

    assert_timeout(maintenance_reply(answer, &budget));

    drop(cleanup);

    assert!(path.exists());
    release.send(()).unwrap();

    db.append("a".into(), vec![event()]).await.unwrap();

    assert!(!path.exists());
    assert_eq!(
        db.count(Query {
            from: 0,
            to: 100,
            limit: 10,
            ..Query::default()
        })
        .await
        .unwrap(),
        2
    );

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn writer_maintenance_native_deadline_and_cancellation_leave_ingestion_usable() {
    for cancel in [false, true] {
        let dir = tempfile::tempdir().unwrap();

        let db = Storage::open(StorageConfig {
            data_dir: dir.path().to_owned(),
            ..StorageConfig::default()
        })
        .unwrap();
        let budget = MaintenanceBudget {
            cancelled: db.inner.maintenance_cancel.clone(),
            deadline: Instant::now() + Duration::from_millis(if cancel { 5000 } else { 60 }),
        };

        let mut native_budget = budget.clone();
        let (started, running) = oneshot::channel();
        let (reply, answer) = oneshot::channel();
        db.inner
            .writer
            .send(Write::Test(Box::new(move |conn, active| {
                native_budget.deadline =
                    Instant::now() + Duration::from_millis(if cancel { 5000 } else { 60 });
                let scope = WriterScope::start(native_budget, active).unwrap();
                let tx = conn.transaction().unwrap();
                tx.execute("UPDATE retention_state SET before_time=20", [])
                    .unwrap();
                started.send(()).unwrap();

                let result = tx
                    .query_row("SELECT sum(i) FROM range(10000000000) t(i)", [], |r| {
                        r.get::<_, i64>(0)
                    })
                    .map_err(|error| scope.error(error));

                drop(tx);

                drop(scope);
                reply.send(result).unwrap();
            })))
            .await
            .unwrap();
        running.await.unwrap();

        if cancel {
            db.cancel_maintenance();
        }

        assert_timeout(
            tokio::time::timeout(Duration::from_secs(2), answer)
                .await
                .unwrap()
                .unwrap(),
        );
        assert_eq!(db.append("a".into(), vec![event()]).await.unwrap(), 1);

        let (reply, answer) = oneshot::channel();
        db.inner
            .writer
            .send(Write::Test(Box::new(move |conn, _| {
                reply
                    .send(
                        conn.query_row("SELECT before_time FROM retention_state", [], |r| {
                            r.get::<_, i64>(0)
                        })
                        .unwrap(),
                    )
                    .unwrap();
            })))
            .await
            .unwrap();

        assert_eq!(answer.await.unwrap(), i64::MIN);
        assert!(db.is_ready());

        tokio::time::timeout(Duration::from_secs(2), db.shutdown())
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn maintenance_handoff_timeout_counts_failure_and_skips_late_retention() {
    let dir = tempfile::tempdir().unwrap();

    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        maintenance_timeout_ms: 50,
        ..StorageConfig::default()
    })
    .unwrap();
    db.append("a".into(), vec![event()]).await.unwrap();

    let release = writer_barrier(&db).await;

    assert_timeout(
        tokio::time::timeout(Duration::from_secs(1), db.retain(20))
            .await
            .unwrap(),
    );
    assert_eq!(
        db.inner
            .metrics
            .maintenance_failures
            .load(Ordering::Acquire),
        1
    );
    release.send(()).unwrap();

    db.append("a".into(), vec![event()]).await.unwrap();

    assert_eq!(
        db.count(Query {
            from: 0,
            to: 100,
            limit: 10,
            ..Query::default()
        })
        .await
        .unwrap(),
        2
    );

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn writer_retention_deadline_rolls_back_deleted_rows_and_cutoff() {
    let dir = tempfile::tempdir().unwrap();

    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();

    let (reply, answer) = oneshot::channel();
    let (seeded, running) = oneshot::channel();
    db.inner
        .writer
        .send(Write::Test(Box::new(move |conn, active| {
            conn.execute_batch(
                "INSERT INTO events SELECT i,'a',10,10,30,NULL,NULL,NULL,NULL,'test','{}' FROM \
            range(1,1000001) t(i); UPDATE ingest_state SET next_id=1000001",
            )
            .unwrap();
            seeded.send(()).unwrap();

            // Start the budget on the writer itself so queue scheduling cannot
            // account for the timeout. This exercises the formerly unbounded
            // retention delete, including rollback of its earlier state update.
            let scope = WriterScope::start(maintenance_budget(20), active).unwrap();
            let result = archive::retain_bounded(
                conn,
                20,
                1000000,
                &mut ArchiveLeases::default(),
                100000,
                &scope,
            );
            drop(scope);
            reply.send(result).unwrap();
        })))
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(30), running)
        .await
        .unwrap()
        .unwrap();

    assert_timeout(
        tokio::time::timeout(Duration::from_secs(5), answer)
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        db.count(Query {
            from: 0,
            to: 100,
            limit: 10,
            ..Query::default()
        })
        .await
        .unwrap(),
        1000000
    );

    let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
    assert_eq!(
        db.append_with_permit("a".into(), vec![event()], permit)
            .await
            .unwrap(),
        1
    );
    assert!(db.is_ready());

    tokio::time::timeout(Duration::from_secs(5), db.shutdown())
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn publication_recovery_before_and_after_manifest_commit() {
    let dir = tempfile::tempdir().unwrap();
    let archives = dir.path().join("archives");
    std::fs::create_dir(&archives).unwrap();
    let mut conn = database(&dir.path().join("events.duckdb"));
    append(&mut conn, "a", &[event()]).unwrap();
    let orphan = export(&mut conn, &archives, 20).unwrap().unwrap();
    assert!(archives.join(&orphan.path).is_file());
    recover(&conn, &archives).unwrap();
    assert!(!archives.join(&orphan.path).exists());
    assert_eq!(
        conn.query_row("SELECT count(*) FROM events", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );

    let published = export(&mut conn, &archives, 20).unwrap().unwrap();
    let filename = published.path.clone();
    publish(&mut conn, published).unwrap();
    recover(&conn, &archives).unwrap();

    assert!(archives.join(filename).is_file());
    assert_eq!(
        conn.query_row("SELECT count(*) FROM events", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        conn.query_row("SELECT sum(row_count) FROM archives", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn readers_defer_unlink_without_blocking_publication() {
    let dir = tempfile::tempdir().unwrap();
    let archives = dir.path().join("archives");
    std::fs::create_dir(&archives).unwrap();
    let mut conn = database(&dir.path().join("db"));
    append(&mut conn, "a", &[event()]).unwrap();

    let file = export(&mut conn, &archives, 20).unwrap().unwrap();
    let path = file.path.clone();
    publish(&mut conn, file).unwrap();
    let gate = Arc::new(Mutex::new(ArchiveLeases::default()));
    let lease = gate.lock().unwrap().register(&gate);
    retain(
        &mut conn,
        &archives,
        20,
        1,
        &mut gate.lock().unwrap(),
        100000,
    )
    .unwrap();

    assert!(archives.join(&path).exists());

    drop(lease);

    cleanup_archives(&gate, &archives);

    assert!(!archives.join(path).exists());
}

#[test]
fn archive_export_failure_and_abandoned_publication_preserve_hot_events() {
    let dir = tempfile::tempdir().unwrap();
    let archives = dir.path().join("archives");
    std::fs::create_dir(&archives).unwrap();
    let mut conn = database(&dir.path().join("events.duckdb"));
    append(&mut conn, "a", &[event()]).unwrap();
    let quota = DiskBudget::new(dir.path().to_owned(), Some(1024 * 1024 * 1024)).unwrap();
    let cancelled = AtomicBool::new(false);
    let work = || ArchiveWork {
        cancelled: &cancelled,
        deadline: Instant::now() + Duration::from_secs(30),
        quota: Some(&quota),
    };

    // COPY cannot create an artifact beneath a regular file. This fails
    // after quota reservation without a timing-sensitive fault injector.
    let blocked = dir.path().join("blocked");
    std::fs::write(&blocked, b"not a directory").unwrap();

    assert!(
        export_bounded(
            &mut conn,
            ExportRequest {
                dir: &blocked,
                before: 20,
                batch_rows: 100,
                partition_ms: 3600000,
                work: work()
            }
        )
        .is_err()
    );
    assert_eq!(quota.reserved.load(Ordering::Acquire), 0);

    // Export completes sync/rename, but publication is abandoned before
    // the writer can commit a manifest or remove acknowledged hot events.
    let file = export_bounded(
        &mut conn,
        ExportRequest {
            dir: &archives,
            before: 20,
            batch_rows: 100,
            partition_ms: 3600000,
            work: work(),
        },
    )
    .unwrap()
    .unwrap();
    let path = archives.join(&file.path);
    assert!(path.is_file());
    assert!(quota.reserved.load(Ordering::Acquire) > 0);

    drop(file);

    assert!(!path.exists());
    assert_eq!(std::fs::read_dir(&archives).unwrap().count(), 0);
    assert_eq!(quota.reserved.load(Ordering::Acquire), 0);
    assert_eq!(
        conn.query_row("SELECT count(*) FROM events", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        conn.query_row("SELECT count(*) FROM archives", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        0
    );

    let file = export_bounded(
        &mut conn,
        ExportRequest {
            dir: &archives,
            before: 20,
            batch_rows: 100,
            partition_ms: 3600000,
            work: work(),
        },
    )
    .unwrap()
    .unwrap();
    publish(&mut conn, file).unwrap();

    assert_eq!(quota.reserved.load(Ordering::Acquire), 0);
    assert_eq!(
        conn.query_row("SELECT sum(row_count) FROM archives", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn bounded_archive_compaction_and_histogram_anchor() {
    let dir = tempfile::tempdir().unwrap();

    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        archive_batch_rows: 2,
        ..StorageConfig::default()
    })
    .unwrap();

    let mut events = vec![event(); 5];
    for (index, event) in events.iter_mut().enumerate() {
        event.event_time = 10 + index as i64;
        event.level = [2, 10, 100, 10, 2][index];
    }

    db.append("a".into(), events).await.unwrap();

    assert_eq!(db.archive(20).await.unwrap(), 2);
    assert_eq!(db.archive(20).await.unwrap(), 2);
    assert_eq!(db.archive(20).await.unwrap(), 1);
    assert_eq!(db.archive(20).await.unwrap(), 0);

    let q = Query {
        from: 0,
        to: 100,
        limit: 10,
        ..Query::default()
    };
    assert_eq!(db.count(q.clone()).await.unwrap(), 5);
    assert_eq!(db.compact().await.unwrap(), 5);
    assert_eq!(db.count(q.clone()).await.unwrap(), 5);

    let facets = db.facets(q.clone()).await.unwrap();
    assert_eq!(
        facets
            .levels
            .iter()
            .map(|v| v.value.as_str())
            .collect::<Vec<_>>(),
        vec!["2", "10", "100"]
    );

    db.retain(11).await.unwrap();
    let buckets = db.histogram(q, 10).await.unwrap();
    assert_eq!(buckets[0].time, 10);

    let filtered = Query {
        from: 0,
        to: 100,
        min_level: Some(100),
        limit: 10,
        ..Query::default()
    };
    let tail = db
        .tail_filtered(
            Some(crate::query_cursor::encode_tail(&filtered, 0).unwrap()),
            filtered,
        )
        .await;
    assert!(tail.is_err());

    db.shutdown().await.unwrap();

    let reopened = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();
    assert_eq!(
        reopened
            .count(Query {
                from: 0,
                to: 100,
                limit: 10,
                ..Query::default()
            })
            .await
            .unwrap(),
        4
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn filtered_tail_advances_past_nonmatching_commits_and_prunes_archives() {
    let dir = tempfile::tempdir().unwrap();

    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();
    db.append("a".into(), vec![event(), event()]).await.unwrap();

    db.archive(20).await.unwrap();
    let commits = db.subscribe_commits();

    db.append("b".into(), vec![event()]).await.unwrap();

    assert!(commits.has_changed().unwrap());

    let q = Query {
        from: 0,
        to: 100,
        sources: vec!["missing".into()],
        limit: 10,
        ..Query::default()
    };
    let result = db
        .tail_filtered(
            Some(crate::query_cursor::encode_tail(&q, 0).unwrap()),
            q.clone(),
        )
        .await
        .unwrap();
    assert!(result.events.is_empty());
    assert_eq!(
        crate::query_cursor::decode_tail(&q, result.next_cursor.as_deref().unwrap()).unwrap(),
        3
    );

    let mut q = q;
    q.sources = vec!["b".into()];

    let result = db
        .tail_filtered(Some(crate::query_cursor::encode_tail(&q, 2).unwrap()), q)
        .await
        .unwrap();
    assert_eq!(result.events.len(), 1);
    assert_eq!(result.events[0].id, "3");

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn legacy_archive_missing_nullable_columns_uses_named_schema_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let archives = dir.path().join("archives");
    std::fs::create_dir(&archives).unwrap();
    let mut conn = database(&dir.path().join("events.duckdb"));
    append(&mut conn, "a", &[event()]).unwrap();
    conn.execute_batch(&format!(
        "COPY (SELECT id,source,event_time,received_at,level,message,attributes FROM events) \
         TO '{}' (FORMAT PARQUET); DELETE FROM events; INSERT INTO \
         archives(path,min_time,max_time,row_count,schema_version) VALUES \
         ('legacy.parquet',10,10,1,1)",
        quote(&archives.join("legacy.parquet").to_string_lossy())
    ))
    .unwrap();

    drop(conn);

    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();
    let events = db
        .search(Query {
            from: 0,
            to: 100,
            limit: 10,
            ..Query::default()
        })
        .await
        .unwrap()
        .events;
    assert_eq!(events.len(), 1);
    assert!(events[0].service.is_none());
    assert_eq!(events[0].message, "test");

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn compact_legacy_archives_supplies_missing_columns_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let archives = dir.path().join("archives");
    std::fs::create_dir(&archives).unwrap();
    let mut conn = database(&dir.path().join("events.duckdb"));

    for index in 0..2 {
        let name = format!("legacy-{index}.parquet");
        append(&mut conn, "a", &[event()]).unwrap();
        conn.execute_batch(&format!(
            "COPY (SELECT id,source,event_time,received_at,level,message,attributes FROM events) \
             TO '{}' (FORMAT PARQUET); DELETE FROM events; INSERT INTO \
             archives(path,min_time,max_time,row_count,schema_version) VALUES \
             ('{name}',10,10,1,1)",
            quote(&archives.join(&name).to_string_lossy())
        ))
        .unwrap();
    }

    drop(conn);

    let config = StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    };
    let db = Storage::open(config.clone()).unwrap();

    let query = Query {
        from: 0,
        to: 100,
        limit: 10,
        ..Query::default()
    };
    let before = db.search(query.clone()).await.unwrap();
    assert_eq!(before.events.len(), 2);

    assert_eq!(db.compact().await.unwrap(), 2);

    let after = db.search(query.clone()).await.unwrap();
    assert_eq!(
        serde_json::to_value(&after).unwrap(),
        serde_json::to_value(&before).unwrap()
    );
    assert!(db.is_ready());
    db.shutdown().await.unwrap();

    let conn = database(&dir.path().join("events.duckdb"));
    let count: i64 = conn
        .query_row("SELECT count(*) FROM archives", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
    drop(conn);

    let db = Storage::open(config).unwrap();
    let restored = db.search(query).await.unwrap();
    assert_eq!(
        serde_json::to_value(&restored).unwrap(),
        serde_json::to_value(&before).unwrap()
    );

    for event in restored.events {
        assert!(event.service.is_none());
        assert!(event.logger.is_none());
        assert!(event.host.is_none());
        assert!(event.pid.is_none());
    }

    assert_eq!(db.compact().await.unwrap(), 0);
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn legacy_archives_backfill_and_many_file_scan() {
    let dir = tempfile::tempdir().unwrap();
    let archives = dir.path().join("archives");
    std::fs::create_dir(&archives).unwrap();
    let mut conn = database(&dir.path().join("events.duckdb"));

    for _ in 0..32 {
        append(&mut conn, "a", &[event()]).unwrap();

        let file = export(&mut conn, &archives, 20).unwrap().unwrap();

        publish(&mut conn, file).unwrap();
    }

    conn.execute(
        "UPDATE archives SET min_id=NULL,max_id=NULL,file_bytes=NULL,schema_version=1",
        [],
    )
    .unwrap();

    drop(conn);

    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();

    let q = Query {
        from: 0,
        to: 100,
        limit: 100,
        ..Query::default()
    };
    assert_eq!(db.count(q.clone()).await.unwrap(), 32);
    assert!(db.compact().await.unwrap() > 0);
    assert_eq!(db.count(q).await.unwrap(), 32);

    db.shutdown().await.unwrap();
}

#[test]
fn interrupts_and_connection_destruction_share_lifetime_mutex() {
    for _ in 0..64 {
        let conn = Connection::open_in_memory().unwrap();
        let interrupt = conn.interrupt_handle();
        let state = Arc::new(Mutex::new(Some(1u64)));
        let alive = Arc::new(AtomicBool::new(true));
        let watch_state = state.clone();
        let watch_alive = alive.clone();
        let owner = ConnectionOwner {
            connection: Some(conn),
            state,
            alive,
        };

        let worker = thread::spawn(move || {
            while watch_alive.load(Ordering::Acquire) {
                let active = watch_state.lock().unwrap();
                if active.is_some() {
                    interrupt.interrupt();
                }
            }
        });
        thread::yield_now();

        drop(owner);
        worker.join().unwrap();
    }
}

#[tokio::test]
async fn temporary_export_failure_does_not_poison_readiness() {
    let dir = tempfile::tempdir().unwrap();

    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();
    db.append("a".into(), vec![event()]).await.unwrap();

    let archive = dir.path().join("archives");
    let moved = dir.path().join("archives-moved");
    std::fs::rename(&archive, &moved).unwrap();

    assert!(db.archive(20).await.is_err());
    assert!(db.is_ready());

    std::fs::rename(&moved, &archive).unwrap();
    assert_eq!(db.archive(20).await.unwrap(), 1);

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

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn idle_reader_takes_shared_queue_while_other_reader_is_busy() {
    use super::readers::NativeCompletionProbe;

    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        reader_threads: 2,
        query_timeout_ms: 30_000,
        ..StorageConfig::default()
    })
    .unwrap();
    let q = Query {
        from: 0,
        to: 100,
        limit: 10,
        ..Query::default()
    };

    let (started, started_rx) = oneshot::channel();
    let (completed, completed_rx) = oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    let (reply, mut reply_rx) = oneshot::channel();
    let cancelled = Arc::new(AtomicBool::new(false));
    let id = db.inner.next.fetch_add(1, Ordering::Relaxed);
    db.inner.readers[0]
        .try_send(Read {
            id,
            query: Some(q.clone()),
            operation: Operation::ObserveNative(NativeCompletionProbe {
                started,
                completed,
                release: release_rx,
            }),
            reply,
            cancelled: cancelled.clone(),
            deadline: Instant::now() + Duration::from_secs(30),
            max_bytes: db.inner.max_result_bytes,
        })
        .unwrap_or_else(|_| panic!("reader queue must accept the probe"));

    tokio::time::timeout(Duration::from_secs(30), started_rx)
        .await
        .unwrap()
        .unwrap();

    cancelled.store(true, Ordering::Release);
    for interrupt in &db.inner.interrupts {
        interrupt(id);
    }

    let native_error = tokio::time::timeout(Duration::from_secs(30), completed_rx)
        .await
        .unwrap()
        .unwrap()
        .expect("the native query must be interrupted");
    assert!(native_error.to_ascii_lowercase().contains("interrupt"));

    // Keep one reader explicitly occupied until every count has returned. This
    // proves another worker serves the shared queue, independently of CPU speed.
    for _ in 0..3 {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(30), db.count(q.clone()))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        assert_eq!(db.inner.metrics.busy_readers.load(Ordering::Relaxed), 1);
        assert!(matches!(
            reply_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }

    release.send(()).unwrap();
    assert_timeout(reply_rx.await.unwrap());
    assert_eq!(db.inner.metrics.busy_readers.load(Ordering::Relaxed), 0);
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn size_retention_respects_reader_epochs_then_resumes_admission() {
    let dir = tempfile::tempdir().unwrap();
    let base = StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    };
    let seed = Storage::open(base.clone()).unwrap();
    let mut random = 1u64;

    for time in [10, 20, 30] {
        let mut events = Vec::new();

        for _ in 0..100 {
            let mut item = event();
            item.event_time = time;
            let mut message = String::new();

            for _ in 0..1024 {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                message.push_str(&format!("{random:016x}"));
            }

            item.message = message;
            events.push(item);
        }

        seed.append("a".into(), events).await.unwrap();
        seed.archive(time + 1).await.unwrap();
    }

    seed.shutdown().await.unwrap();

    let total = quota::footprint(dir.path()).unwrap().total();
    let smallest = std::fs::read_dir(dir.path().join("archives"))
        .unwrap()
        .flatten()
        .map(|file| file.metadata().unwrap().len())
        .min()
        .unwrap();
    let target = total - smallest / 2;

    let db = Storage::open(StorageConfig {
        max_size_bytes: Some(target),
        ..base
    })
    .unwrap();
    let lease = db
        .inner
        .archive_leases
        .lock()
        .unwrap()
        .register(&db.inner.archive_leases);
    assert!(matches!(
        db.enforce_size_limit().await,
        Err(Error {
            kind: ErrorKind::SizeLimit,
            ..
        })
    ));
    assert!(db.is_ready());
    assert!(matches!(
        db.append("a".into(), vec![event()]).await,
        Err(Error {
            kind: ErrorKind::SizeLimit,
            ..
        })
    ));

    let files_before = std::fs::read_dir(dir.path().join("archives"))
        .unwrap()
        .count();
    assert_eq!(files_before, 3);

    drop(lease);
    db.enforce_size_limit().await.unwrap();

    assert_eq!(
        std::fs::read_dir(dir.path().join("archives"))
            .unwrap()
            .count(),
        2
    );
    assert!(quota::footprint(dir.path()).unwrap().total() < target);

    let mut fresh = event();
    fresh.event_time = 40;

    assert_eq!(db.append("a".into(), vec![fresh]).await.unwrap(), 1);
    assert_eq!(
        db.count(Query {
            from: 0,
            to: 100,
            limit: 1000,
            ..Query::default()
        })
        .await
        .unwrap(),
        201
    );

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn size_pressure_does_not_repeat_hot_deletion_without_physical_progress() {
    let dir = tempfile::tempdir().unwrap();
    let base = StorageConfig {
        data_dir: dir.path().to_owned(),
        archive_batch_rows: 1,
        ..StorageConfig::default()
    };
    let seed = Storage::open(base.clone()).unwrap();

    let mut events = vec![event(); 3];
    for (index, event) in events.iter_mut().enumerate() {
        event.event_time = 10 + index as i64 * 10;
    }

    seed.append("a".into(), events).await.unwrap();
    seed.shutdown().await.unwrap();
    let original = quota::footprint(dir.path()).unwrap().total();

    let db = Storage::open(StorageConfig {
        max_size_bytes: Some(original - 1),
        ..base
    })
    .unwrap();
    let first = db.enforce_size_limit().await;
    let after = quota::footprint(dir.path()).unwrap().total();
    if matches!(
        first,
        Err(Error {
            kind: ErrorKind::SizeLimit,
            ..
        })
    ) && after >= original
    {
        let q = Query {
            from: 0,
            to: 100,
            limit: 10,
            ..Query::default()
        };
        let remaining = db.count(q.clone()).await.unwrap();
        assert_eq!(remaining, 2);
        assert!(matches!(
            db.enforce_size_limit().await,
            Err(Error {
                kind: ErrorKind::SizeLimit,
                ..
            })
        ));

        assert_eq!(db.count(q).await.unwrap(), remaining);
    } else {
        assert!(
            after < original,
            "bounded reclaim must make measured progress or stop"
        );
    }

    assert!(db.is_ready());

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn unreclaimable_size_target_returns_507_kind_without_destroying_hot_rows() {
    let dir = tempfile::tempdir().unwrap();
    let base = StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    };
    let seed = Storage::open(base.clone()).unwrap();
    seed.append("a".into(), vec![event()]).await.unwrap();
    seed.shutdown().await.unwrap();
    let total = quota::footprint(dir.path()).unwrap().total();
    std::fs::write(
        dir.path().join("unmanaged"),
        vec![0; (total + 1024 * 1024) as usize],
    )
    .unwrap();

    let db = Storage::open(StorageConfig {
        max_size_bytes: Some(total + 1),
        ..base.clone()
    })
    .unwrap();

    for _ in 0..2 {
        assert!(matches!(
            db.enforce_size_limit().await,
            Err(Error {
                kind: ErrorKind::SizeLimit,
                ..
            })
        ));
    }

    assert!(db.is_ready());
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

    db.shutdown().await.unwrap();

    let total = quota::footprint(dir.path()).unwrap().total();
    let larger = Storage::open(StorageConfig {
        max_size_bytes: Some(total + 8 * 1024 * 1024),
        ..base
    })
    .unwrap();
    assert_eq!(larger.append("a".into(), vec![event()]).await.unwrap(), 1);
    assert_eq!(
        larger
            .count(Query {
                from: 0,
                to: 100,
                limit: 10,
                ..Query::default()
            })
            .await
            .unwrap(),
        2
    );
    larger.shutdown().await.unwrap();
}

#[tokio::test]
async fn retention_metric_tracks_only_committed_monotonic_cutoffs() {
    fn cutoff(storage: &Storage) -> i64 {
        storage
            .metrics_prometheus()
            .lines()
            .find_map(|line| line.strip_prefix("logbrook_storage_retention_before_ms "))
            .unwrap()
            .parse()
            .unwrap()
    }

    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    };
    let db = Storage::open(config.clone()).unwrap();
    assert_eq!(cutoff(&db), i64::MIN);

    db.retain(20).await.unwrap();

    assert_eq!(cutoff(&db), 20);

    db.retain(10).await.unwrap();

    assert_eq!(cutoff(&db), 20);

    let mut retained = event();
    retained.event_time = 30;

    db.append("metric".into(), vec![retained]).await.unwrap();

    db.archive(40).await.unwrap();
    let path = std::fs::read_dir(dir.path().join("archives"))
        .unwrap()
        .flatten()
        .find(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "parquet")
        })
        .unwrap()
        .path();
    let original = std::fs::read(&path).unwrap();
    std::fs::write(&path, b"broken").unwrap();

    assert!(db.retain(50).await.is_err());
    assert_eq!(cutoff(&db), 20);
    std::fs::write(path, original).unwrap();

    db.shutdown().await.unwrap();

    let reopened = Storage::open(config).unwrap();
    assert_eq!(cutoff(&reopened), 20);
    reopened.shutdown().await.unwrap();
}

#[test]
fn newer_readers_do_not_pin_retired_generation_files() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("old.parquet"), b"old").unwrap();
    let gate = Arc::new(Mutex::new(ArchiveLeases::default()));
    let older = gate.lock().unwrap().register(&gate);
    gate.lock().unwrap().retire(vec!["old.parquet".into()]);
    let newer = gate.lock().unwrap().register(&gate);

    cleanup_archives(&gate, dir.path());

    assert!(dir.path().join("old.parquet").exists());

    drop(older);

    cleanup_archives(&gate, dir.path());

    assert!(!dir.path().join("old.parquet").exists());
    assert!(gate.lock().unwrap().has_readers());

    drop(newer);
}

#[test]
fn initial_tail_checkpoint_skips_archive_files_but_replay_checks_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let mut conn = database(&dir.path().join("events.duckdb"));
    projection::activate(&mut conn, &["env".into()]).unwrap();
    append(&mut conn, "a", &[event()]).unwrap();
    let file = export(&mut conn, dir.path(), 20).unwrap().unwrap();
    let path = dir.path().join(&file.path);
    publish(&mut conn, file).unwrap();

    let original = std::fs::read(&path).unwrap();
    let gate = Arc::new(Mutex::new(ArchiveLeases::default()));
    let cancelled = AtomicBool::new(false);
    let q = Query {
        from: 0,
        to: 100,
        limit: 10,
        ..Query::default()
    };
    for missing in [false, true] {
        if missing {
            std::fs::remove_file(&path).unwrap();
        } else {
            // Preserve the file length so validation cannot rely on size alone.
            std::fs::write(&path, vec![0; original.len()]).unwrap();
        }

        let deadline = query::QueryDeadline {
            cancelled: &cancelled,
            at: Instant::now() + Duration::from_secs(10),
        };
        let answer = query::run_query(
            &mut conn,
            dir.path(),
            &q,
            Operation::Tail(None),
            usize::MAX,
            &gate,
            deadline,
        )
        .unwrap();
        let Answer::Search(page) = answer else {
            panic!("expected tail result")
        };
        assert!(page.events.is_empty());
        assert_eq!(
            crate::query_cursor::decode_tail(&q, page.next_cursor.as_deref().unwrap()).unwrap(),
            1
        );

        assert!(matches!(
            query::run_query(
                &mut conn,
                dir.path(),
                &q,
                Operation::Tail(Some(0)),
                usize::MAX,
                &gate,
                deadline
            ),
            Err(Error {
                kind: ErrorKind::Integrity,
                ..
            })
        ));
    }

    for already_cancelled in [false, true] {
        cancelled.store(already_cancelled, Ordering::Release);
        let deadline = query::QueryDeadline {
            cancelled: &cancelled,
            at: if already_cancelled {
                Instant::now() + Duration::from_secs(10)
            } else {
                Instant::now() - Duration::from_secs(1)
            },
        };
        assert!(matches!(
            query::run_query(
                &mut conn,
                dir.path(),
                &q,
                Operation::Tail(None),
                usize::MAX,
                &gate,
                deadline,
            ),
            Err(Error {
                kind: ErrorKind::Timeout,
                ..
            })
        ));
    }
}

#[tokio::test]
async fn initial_tail_validates_query_and_named_cursor_exact_byte_budget() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();
    db.append("a".into(), vec![event()]).await.unwrap();
    db.retain(5).await.unwrap();
    let q = Query {
        from: 0,
        to: 100,
        limit: 10,
        cursor_scope: crate::query_cursor::CursorScope::Named("long-stream-name".into()),
        attributes: crate::attribute_filter::AttributeFilters::parse(
            r#"{"attributes":[{"key":"env","op":"eq","value":"prod"}]}"#,
            usize::MAX,
        )
        .unwrap(),
        ..Query::default()
    };
    let token = crate::query_cursor::encode_tail(&q, 1).unwrap();
    let frame = format!("event: cursor\nid: {token}\ndata: caught up\n\n");
    let page = db
        .tail_filtered_bounded(None, q.clone(), frame.len())
        .await
        .unwrap();
    assert!(page.events.is_empty());
    assert_eq!(page.next_cursor.as_deref(), Some(token.as_str()));
    assert_eq!(crate::query_cursor::decode_tail(&q, &token).unwrap(), 1);

    assert!(matches!(
        db.tail_filtered_bounded(None, q.clone(), frame.len() - 1)
            .await,
        Err(Error {
            kind: ErrorKind::TooLarge,
            ..
        })
    ));

    // Replay and empty catch-up frames use the same complete named token.
    let replay_q = Query {
        attributes: Default::default(),
        ..q.clone()
    };
    let start = crate::query_cursor::encode_tail(&replay_q, 0).unwrap();
    let replay = db
        .tail_filtered(Some(start.clone()), replay_q.clone())
        .await
        .unwrap();
    assert_eq!(replay.events.len(), 1);
    let event_token = crate::query_cursor::encode_tail(&replay_q, 1).unwrap();
    let data = serde_json::to_string(&replay.events[0]).unwrap();
    let event_frame = format!("event: log\nid: {event_token}\ndata: {data}\n\n");
    assert_eq!(
        db.tail_filtered_bounded(Some(start.clone()), replay_q.clone(), event_frame.len())
            .await
            .unwrap()
            .events
            .len(),
        1
    );

    assert!(matches!(
        db.tail_filtered_bounded(Some(start), replay_q.clone(), event_frame.len() - 1)
            .await,
        Err(Error {
            kind: ErrorKind::TooLarge,
            ..
        })
    ));

    let empty_frame = format!("event: cursor\nid: {event_token}\ndata: caught up\n\n");
    assert!(
        db.tail_filtered_bounded(
            Some(event_token.clone()),
            replay_q.clone(),
            empty_frame.len()
        )
        .await
        .unwrap()
        .events
        .is_empty()
    );

    assert!(matches!(
        db.tail_filtered_bounded(Some(event_token), replay_q, empty_frame.len() - 1)
            .await,
        Err(Error {
            kind: ErrorKind::TooLarge,
            ..
        })
    ));

    for invalid in [
        Query {
            from: 100,
            ..q.clone()
        },
        Query {
            limit: 0,
            ..q.clone()
        },
        Query {
            limit: crate::model::MAX_PAGE_SIZE + 1,
            ..q.clone()
        },
        Query {
            cursor: Some("invalid".into()),
            ..q.clone()
        },
    ] {
        assert!(matches!(
            db.tail_filtered(None, invalid).await,
            Err(Error {
                kind: ErrorKind::Invalid,
                ..
            })
        ));
    }

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn bounded_tail_pages_follow_actual_bytes_without_losing_rows() {
    let dir = tempfile::tempdir().unwrap();

    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();
    db.append("a".into(), vec![event(); 10]).await.unwrap();
    let q = Query {
        from: 0,
        to: 100,
        limit: 1000,
        ..Query::default()
    };
    let mut after = Some(crate::query_cursor::encode_tail(&q, 0).unwrap());
    let mut ids = Vec::new();

    loop {
        let page = db
            .tail_filtered_bounded(after, q.clone(), 600)
            .await
            .unwrap();
        if page.events.is_empty() {
            break;
        }
        after = page.next_cursor;
        ids.extend(page.events.into_iter().map(|event| event.id));
    }

    assert_eq!(ids.len(), 10);
    assert_eq!(ids[9], "10");

    assert!(matches!(
        db.tail_filtered_bounded(Some(crate::query_cursor::encode_tail(&q, 0).unwrap()), q, 1)
            .await,
        Err(Error {
            kind: ErrorKind::TooLarge,
            ..
        })
    ));

    db.shutdown().await.unwrap();
}

#[test]
fn abandoned_exports_clean_files_and_committed_size_mismatch_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let mut conn = database(&dir.path().join("db"));
    append(&mut conn, "a", &[event()]).unwrap();

    let file = export(&mut conn, dir.path(), 20).unwrap().unwrap();
    let path = dir.path().join(&file.path);
    drop(file);

    assert!(!path.exists());

    let file = export(&mut conn, dir.path(), 20).unwrap().unwrap();
    let path = dir.path().join(&file.path);
    publish(&mut conn, file).unwrap();
    std::fs::write(path, b"truncated").unwrap();

    assert!(matches!(
        recover(&conn, dir.path()),
        Err(Error {
            kind: ErrorKind::Integrity,
            ..
        })
    ));
}

#[test]
fn structured_fatal_errors_and_queue_closure_are_typed() {
    assert!(matches!(
        db_error(std::io::Error::other(
            r#"{"exception_type":"Internal","exception_message":"fatal"}"#
        ))
        .kind,
        ErrorKind::Integrity
    ));
    assert!(matches!(
        db_error(std::io::Error::other(
            r#"{"exception_type":"IO","exception_message":"disk full"}"#
        ))
        .kind,
        ErrorKind::Unavailable
    ));
    assert!(matches!(
        queue_error(mpsc::error::TrySendError::Closed(())).kind,
        ErrorKind::Unavailable
    ));
    assert!(matches!(
        queue_error(mpsc::error::TrySendError::Full(())).kind,
        ErrorKind::Overloaded
    ));
}

#[tokio::test]
async fn live_writer_rechecks_persisted_retention_cutoff() {
    let dir = tempfile::tempdir().unwrap();

    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();
    db.retain(20).await.unwrap();

    let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
    assert!(matches!(
        db.append_with_permit("a".into(), vec![event()], permit)
            .await,
        Err(Error {
            kind: ErrorKind::Invalid,
            ..
        })
    ));
    assert!(db.is_ready());
    assert_eq!(db.append("import".into(), vec![event()]).await.unwrap(), 1);

    db.shutdown().await.unwrap();
}

#[test]
fn publication_mismatch_rolls_back_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let mut conn = database(&dir.path().join("db"));
    append(&mut conn, "a", &[event()]).unwrap();

    let mut file = export(&mut conn, dir.path(), 20).unwrap().unwrap();
    file.count += 1;

    assert!(publish(&mut conn, file).is_err());
    assert_eq!(
        conn.query_row("SELECT count(*) FROM archives", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );

    assert_eq!(
        conn.query_row("SELECT count(*) FROM events", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn migrations_are_versioned_and_literal_paths_rejected() {
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(include_str!("../../migrations/001_initial.sql"))
        .unwrap();
    migrate(&mut conn).unwrap();
    migrate(&mut conn).unwrap();

    assert_eq!(
        conn.query_row("SELECT max(version) FROM schema_version", [], |r| r
            .get::<_, i32>(0))
            .unwrap(),
        3
    );
    assert!(validate_archive_name("literal[1].parquet").is_err());
    assert!(validate_archive_name("../a.parquet").is_err());
}

#[test]
fn failed_flush_rolls_back_rows_and_sequence() {
    let mut conn = database(Path::new(":memory:"));
    append(&mut conn, "a", &[event()]).unwrap();
    conn.execute("UPDATE ingest_state SET next_id=1", [])
        .unwrap();

    assert!(append(&mut conn, "a", &[event(), event()]).is_err());
    assert_eq!(
        conn.query_row("SELECT count(*) FROM events", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );

    assert_eq!(
        conn.query_row("SELECT next_id FROM ingest_state", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );

    conn.execute("UPDATE ingest_state SET next_id=2", [])
        .unwrap();

    append(&mut conn, "a", &[event()]).unwrap();
}

#[tokio::test]
async fn timed_out_query_releases_reader_only_after_native_completion() {
    use super::readers::NativeCompletionProbe;

    let dir = tempfile::tempdir().unwrap();
    // Recovery queries get a normal scheduling budget. The short caller deadline is
    // applied only after the probe reaches native execution, independently of suite load.
    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        reader_threads: 1,
        query_timeout_ms: 30_000,
        ..StorageConfig::default()
    })
    .unwrap();
    let q = Query {
        from: 0,
        to: 100,
        limit: 10,
        ..Query::default()
    };
    // Warm search metadata on this same reader before exercising interruption.
    assert!(db.search(q.clone()).await.unwrap().events.is_empty());

    let (started, started_rx) = oneshot::channel();
    let (completed, completed_rx) = oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    let (reply, reply_rx) = oneshot::channel();
    let cancelled = Arc::new(AtomicBool::new(false));
    let id = db.inner.next.fetch_add(1, Ordering::Relaxed);
    db.inner.readers[0]
        .try_send(Read {
            id,
            query: Some(q.clone()),
            operation: Operation::ObserveNative(NativeCompletionProbe {
                started,
                completed,
                release: release_rx,
            }),
            reply,
            cancelled: cancelled.clone(),
            deadline: Instant::now() + Duration::from_secs(30),
            max_bytes: db.inner.max_result_bytes,
        })
        .unwrap_or_else(|_| panic!("reader queue must accept the probe"));

    tokio::time::timeout(Duration::from_secs(30), started_rx)
        .await
        .unwrap()
        .unwrap();
    // Exercise the same reply deadline and abandon path as a configured query timeout,
    // starting the short wait only once the worker has reached the native call.
    assert_timeout(
        db.wait_for_query(id, cancelled, reply_rx, Duration::from_millis(1))
            .await,
    );

    assert_eq!(db.inner.metrics.timeouts.load(Ordering::Relaxed), 1);

    let native_error = tokio::time::timeout(Duration::from_secs(30), completed_rx)
        .await
        .unwrap()
        .unwrap()
        .expect("the native query must be interrupted");
    assert!(native_error.to_ascii_lowercase().contains("interrupt"));
    assert_eq!(db.inner.metrics.busy_readers.load(Ordering::Relaxed), 1);

    // Native progress has ended while the worker still owns the connection.
    // An unavailable estimate never authorizes releasing the reader.
    assert!(
        db.metrics_prometheus().contains(
            "logbrook_storage_query_progress_available{role=\"reader\",worker=\"0\"} 0\n"
        )
    );

    let recovery_db = db.clone();
    let recovery_q = q.clone();
    let recovery = tokio::spawn(async move { recovery_db.count(recovery_q).await });
    let reader = &db.inner.readers[0];
    tokio::time::timeout(Duration::from_secs(30), async {
        while reader.capacity() == reader.max_capacity() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !recovery.is_finished(),
        "reader must stay occupied until completion"
    );
    release.send(()).unwrap();

    assert_eq!(recovery.await.unwrap().unwrap(), 0);
    assert_eq!(db.count(q.clone()).await.unwrap(), 0);
    assert!(db.search(q).await.unwrap().events.is_empty());
    assert_eq!(db.inner.metrics.busy_readers.load(Ordering::Relaxed), 0);

    db.shutdown().await.unwrap();
}

#[test]
fn native_interruption_allows_connection_reuse() {
    let conn = Connection::open_in_memory().unwrap();
    let interrupt = conn.interrupt_handle();
    let (started, wait) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        started.send(()).unwrap();

        assert!(
            conn.query_row("SELECT sum(i) FROM range(10000000000) t(i)", [], |r| r
                .get::<_, i64>(0))
                .is_err()
        );
        assert_eq!(
            conn.query_row("SELECT 42", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            42
        );
    });
    wait.recv().unwrap();
    thread::sleep(Duration::from_millis(50));
    interrupt.interrupt();
    worker.join().unwrap();
}

#[tokio::test]
async fn explicit_ingestion_policy_does_not_infer_retention_privilege_from_permits() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();
    db.retain(20).await.unwrap();

    let rejected = db
        .append_with_policy("live".into(), vec![event()], IngestionPolicy::Live, None)
        .await;
    assert_eq!(rejected.unwrap_err().kind, ErrorKind::Invalid);

    let budget = Arc::new(Semaphore::new(100));
    let permit = budget.clone().try_acquire_many_owned(100).unwrap();
    assert_eq!(
        db.append_with_policy(
            "import".into(),
            vec![event()],
            IngestionPolicy::Import,
            Some(permit)
        )
        .await
        .unwrap(),
        1
    );

    assert_eq!(budget.available_permits(), 100);
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn accepted_append_survives_caller_cancellation_and_draining() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    };
    let db = Storage::open(config.clone()).unwrap();
    let release = writer_barrier(&db).await;
    let budget = Arc::new(Semaphore::new(100));
    let permit = budget.clone().try_acquire_many_owned(100).unwrap();
    let mut append = Box::pin(db.append_with_policy(
        "accepted".into(),
        vec![event()],
        IngestionPolicy::Live,
        Some(permit),
    ));
    assert!(futures_util::poll!(&mut append).is_pending());
    wait_for_queued(&db, 1).await;
    // The decoded input is released only after its queued representation is owned.
    assert_eq!(budget.available_permits(), 100);

    drop(append);

    db.stop_admission();
    assert!(!db.is_ready());
    assert!(db.inner.ready.load(Ordering::Acquire));

    assert_eq!(
        db.append("late".into(), vec![event()])
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unavailable
    );
    release.send(()).unwrap();
    db.shutdown().await.unwrap();

    assert_eq!(db.committed_events(), 1);
    assert_eq!(db.inner.bytes.available_permits(), config.queue_bytes);

    let reopened = Storage::open(config).unwrap();
    let result = reopened
        .search(Query {
            from: 0,
            to: 100,
            limit: 10,
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(result.events.len(), 1);
    assert_eq!(result.events[0].source, "accepted");
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn grouped_requests_keep_sources_receipts_and_writer_barriers() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();
    let release = writer_barrier(&db).await;
    let mut first = Box::pin(db.append("first".into(), vec![event(), event()]));
    let mut second = Box::pin(db.append("second".into(), vec![event()]));
    assert!(futures_util::poll!(&mut first).is_pending());
    wait_for_queued(&db, 1).await;
    assert!(futures_util::poll!(&mut second).is_pending());
    wait_for_queued(&db, 2).await;

    let (seen, observed) = oneshot::channel();
    db.inner
        .writer
        .try_send(Write::Test(Box::new(move |conn, _| {
            let count: i64 = conn
                .query_row("SELECT count(*) FROM events", [], |r| r.get(0))
                .unwrap();
            seen.send(count).unwrap();
        })))
        .unwrap();

    let mut third = Box::pin(db.append("third".into(), vec![event()]));
    assert!(futures_util::poll!(&mut third).is_pending());
    wait_for_queued(&db, 4).await;

    release.send(()).unwrap();

    assert_eq!(first.await.unwrap(), 2);
    assert_eq!(second.await.unwrap(), 1);
    assert_eq!(observed.await.unwrap(), 3);
    assert_eq!(third.await.unwrap(), 1);
    assert_eq!(db.inner.metrics.commits.load(Ordering::Relaxed), 2);
    assert_eq!(db.inner.metrics.group_requests.load(Ordering::Relaxed), 3);

    let result = db
        .search(Query {
            from: 0,
            to: 100,
            limit: 10,
            ..Query::default()
        })
        .await
        .unwrap();
    let sources: Vec<_> = result
        .events
        .iter()
        .map(|event| event.source.as_str())
        .collect();
    assert_eq!(
        sources.iter().filter(|source| **source == "first").count(),
        2
    );

    assert!(sources.contains(&"second"));
    assert!(sources.contains(&"third"));
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn expired_live_request_does_not_fail_valid_group_members() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();
    db.retain(20).await.unwrap();

    let release = writer_barrier(&db).await;
    let mut imported = Box::pin(db.append_with_policy(
        "valid-live".into(),
        vec![NormalizedEvent {
            event_time: 30,
            ..event()
        }],
        IngestionPolicy::Live,
        None,
    ));

    let mut live =
        Box::pin(db.append_with_policy("live".into(), vec![event()], IngestionPolicy::Live, None));
    assert!(futures_util::poll!(&mut imported).is_pending());
    wait_for_queued(&db, 1).await;
    assert!(futures_util::poll!(&mut live).is_pending());
    wait_for_queued(&db, 2).await;

    release.send(()).unwrap();
    assert_eq!(imported.await.unwrap(), 1);
    assert_eq!(live.await.unwrap_err().kind, ErrorKind::Invalid);
    assert_eq!(db.committed_events(), 1);
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
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn forward_quota_preserves_http_credit_and_serialization_permits_release() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        data_dir: dir.path().to_owned(),
        queue_capacity: 4,
        queue_bytes: 4096,
        ..StorageConfig::default()
    };
    let db = Storage::open(config.clone()).unwrap();
    let release = writer_barrier(&db).await;
    let budget = Arc::new(Semaphore::new(1000));
    let mut forward = Box::pin(db.append_forward(
        "forward".into(),
        vec![event()],
        budget.clone().try_acquire_owned().unwrap(),
    ));
    let mut second = Box::pin(db.append_forward(
        "forward".into(),
        vec![event()],
        budget.clone().try_acquire_owned().unwrap(),
    ));
    assert!(futures_util::poll!(&mut forward).is_pending());
    wait_for_queued(&db, 1).await;
    assert!(futures_util::poll!(&mut second).is_pending());
    wait_for_queued(&db, 2).await;

    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(5),
            db.append_forward(
                "forward".into(),
                vec![event()],
                budget.clone().try_acquire_owned().unwrap()
            )
        )
        .await
        .expect("full Forward quota must reject without waiting for the writer")
        .unwrap_err()
        .kind,
        ErrorKind::Overloaded
    );

    let mut http = Box::pin(db.append_with_permit(
        "http".into(),
        vec![event()],
        budget.clone().try_acquire_owned().unwrap(),
    ));
    assert!(futures_util::poll!(&mut http).is_pending());
    wait_for_queued(&db, 3).await;

    release.send(()).unwrap();
    assert_eq!(forward.await.unwrap(), 1);
    assert_eq!(second.await.unwrap(), 1);
    assert_eq!(http.await.unwrap(), 1);

    // Once the original requests release their credits, Forward can advance
    // again without reconnect-specific state or a permanently exhausted quota.
    assert_eq!(
        db.append_forward(
            "recovered-forward".into(),
            vec![event()],
            budget.clone().try_acquire_owned().unwrap()
        )
        .await
        .unwrap(),
        1
    );
    assert_eq!(db.committed_events(), 4);

    let mut escaped = event();
    escaped.attributes = serde_json::json!({"escaped": "\u{0}".repeat(700)});
    assert_eq!(
        db.append_with_permit(
            "http".into(),
            vec![escaped],
            budget.clone().try_acquire_owned().unwrap()
        )
        .await
        .unwrap_err()
        .kind,
        ErrorKind::TooLarge
    );

    assert_eq!(budget.available_permits(), 1000);
    assert_eq!(db.inner.bytes.available_permits(), config.queue_bytes);
    assert_eq!(
        db.inner.forward_bytes.available_permits(),
        config.queue_bytes / 2
    );
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn actual_group_transaction_failure_rolls_back_all_included_requests() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();
    db.append("existing".into(), vec![event()]).await.unwrap();

    let (corrupted, observed) = oneshot::channel();
    db.inner
        .writer
        .send(Write::Test(Box::new(move |conn, _| {
            conn.execute("UPDATE ingest_state SET next_id=1", [])
                .unwrap();
            corrupted.send(()).unwrap();
        })))
        .await
        .unwrap();
    observed.await.unwrap();

    let release = writer_barrier(&db).await;
    let mut first = Box::pin(db.append("first".into(), vec![event(), event()]));
    let mut second = Box::pin(db.append("second".into(), vec![event()]));
    assert!(futures_util::poll!(&mut first).is_pending());
    wait_for_queued(&db, 1).await;
    assert!(futures_util::poll!(&mut second).is_pending());
    wait_for_queued(&db, 2).await;

    release.send(()).unwrap();
    assert!(first.await.is_err());
    assert!(second.await.is_err());

    let (verified, verification) = oneshot::channel();
    db.inner
        .writer
        .send(Write::Test(Box::new(move |conn, _| {
            let count = conn
                .query_row("SELECT count(*) FROM events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap();
            let next = conn
                .query_row("SELECT next_id FROM ingest_state", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap();
            // Restore the deliberately corrupted sequence before a public snapshot:
            // query high-water marks intentionally hide IDs >= next_id.
            conn.execute("UPDATE ingest_state SET next_id=2", [])
                .unwrap();
            verified.send((count, next)).unwrap();
        })))
        .await
        .unwrap();
    assert_eq!(verification.await.unwrap(), (1, 1));

    let page = db
        .search(Query {
            from: 0,
            to: 100,
            limit: 10,
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(page.events.len(), 1);
    assert_eq!(page.events[0].source, "existing");
    assert_eq!(db.committed_events(), 1);
    assert!(db.is_ready());
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn forward_preparation_wait_is_bounded_and_cannot_admit_after_drain() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();
    let held = db
        .inner
        .admission
        .forward_preparations
        .clone()
        .acquire_many_owned(3)
        .await
        .unwrap();

    let budget = Arc::new(Semaphore::new(1));
    let error = db
        .append_admitted(
            "forward".into(),
            vec![event()],
            Some(budget.clone().try_acquire_owned().unwrap()),
            AppendOptions {
                policy: IngestionPolicy::Live,
                lane: AppendLane::Forward,
                validation: None,
                preparation_wait: Some(Duration::from_millis(20)),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Timeout);

    assert_eq!(budget.available_permits(), 1);

    let mut pending = Box::pin(db.append_admitted(
        "forward".into(),
        vec![event()],
        Some(budget.clone().try_acquire_owned().unwrap()),
        AppendOptions {
            policy: IngestionPolicy::Live,
            lane: AppendLane::Forward,
            validation: None,
            preparation_wait: Some(Duration::from_secs(1)),
        },
    ));
    assert!(futures_util::poll!(&mut pending).is_pending());

    db.stop_admission();
    drop(held);

    assert_eq!(pending.await.unwrap_err().kind, ErrorKind::Unavailable);
    assert_eq!(db.committed_events(), 0);
    assert_eq!(budget.available_permits(), 1);
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn permanently_oversized_forward_envelope_differs_from_occupied_credit() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        queue_bytes: 2048,
        ..StorageConfig::default()
    })
    .unwrap();

    let budget = Arc::new(Semaphore::new(1));
    let mut large = event();
    large.message = "x".repeat(800);
    assert_eq!(
        db.append_forward(
            "forward".into(),
            vec![large],
            budget.clone().try_acquire_owned().unwrap()
        )
        .await
        .unwrap_err()
        .kind,
        ErrorKind::TooLarge
    );

    let held = db
        .inner
        .forward_bytes
        .clone()
        .acquire_many_owned(1024)
        .await
        .unwrap();
    assert_eq!(
        db.append_forward(
            "forward".into(),
            vec![event()],
            budget.clone().try_acquire_owned().unwrap()
        )
        .await
        .unwrap_err()
        .kind,
        ErrorKind::Overloaded
    );

    drop(held);

    assert_eq!(
        db.append_forward(
            "forward".into(),
            vec![event()],
            budget.clone().try_acquire_owned().unwrap()
        )
        .await
        .unwrap(),
        1
    );

    assert_eq!(budget.available_permits(), 1);
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_drain_enforces_retention_and_answers_queued_commands() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        ..StorageConfig::default()
    })
    .unwrap();
    db.retain(50).await.unwrap();

    let release = writer_barrier(&db).await;

    let (stop_reply, stop_rx) = oneshot::channel();
    db.inner.writer.send(Write::Stop(stop_reply)).await.unwrap();

    let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
    let mut expired = event();
    expired.event_time = 10;
    let mut append_task = Box::pin(db.append_with_permit("src".into(), vec![expired], permit));
    assert!(futures_util::poll!(&mut append_task).is_pending());
    wait_for_queued(&db, 2).await;

    let (checkpoint_reply, checkpoint_rx) = oneshot::channel();
    db.inner
        .writer
        .send(Write::Checkpoint(
            MaintenanceBudget {
                cancelled: Arc::new(AtomicBool::new(false)),
                deadline: Instant::now() + Duration::from_secs(1),
            },
            checkpoint_reply,
        ))
        .await
        .unwrap();

    release.send(()).unwrap();

    let append_err = append_task.await.unwrap_err();
    assert_eq!(append_err.kind, ErrorKind::Invalid);

    let checkpoint_err = checkpoint_rx.await.unwrap().unwrap_err();
    assert_eq!(checkpoint_err.kind, ErrorKind::Unavailable);

    stop_rx.await.unwrap().unwrap();
}

#[tokio::test]
async fn reused_reader_metadata_tracks_ingest_archive_retention_and_query_parameters() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        reader_threads: 1,
        ..StorageConfig::default()
    })
    .unwrap();
    let q = Query {
        from: 0,
        to: 100,
        limit: 10,
        ..Query::default()
    };
    assert!(db.search(q.clone()).await.unwrap().events.is_empty());

    db.append("first".into(), vec![event()]).await.unwrap();
    assert_eq!(
        db.search(q.clone()).await.unwrap().events[0].source,
        "first"
    );

    let mut later = event();
    later.event_time = 30;
    db.append("second".into(), vec![later]).await.unwrap();
    for source in ["first", "second", "first"] {
        let filtered = Query {
            sources: vec![source.into()],
            ..q.clone()
        };
        assert_eq!(db.search(filtered).await.unwrap().events[0].source, source);
    }

    assert_eq!(db.search(q.clone()).await.unwrap().events.len(), 2);

    db.archive(20).await.unwrap();
    assert_eq!(db.search(q.clone()).await.unwrap().events.len(), 2);

    db.retain(20).await.unwrap();

    let page = db.search(q.clone()).await.unwrap();
    assert_eq!(page.events.len(), 1);
    assert_eq!(page.events[0].source, "second");

    // A failed request must leave the same reader usable with fresh metadata.
    let invalid = Query { to: 0, ..q.clone() };
    assert!(db.search(invalid).await.is_err());
    assert_eq!(db.search(q).await.unwrap().events[0].source, "second");
    db.shutdown().await.unwrap();
}
