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

fn maintenance_budget(milliseconds: u64) -> MaintenanceBudget {
    MaintenanceBudget {
        cancelled: Arc::new(AtomicBool::new(false)),
        deadline: Instant::now() + Duration::from_millis(milliseconds),
    }
}

async fn writer_barrier(storage: &Storage) -> std::sync::mpsc::Sender<()> {
    let (started, receiver) = oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    storage
        .inner
        .writer
        .send(Write::Test(Box::new(move |_, _| {
            started.send(()).unwrap();
            blocked.recv().unwrap();
        })))
        .await
        .unwrap();
    receiver.await.unwrap();

    release
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

    assert!(export_bounded(&mut conn, &blocked, 20, 100, 3600000, work()).is_err());
    assert_eq!(quota.reserved.load(Ordering::Acquire), 0);

    // Export completes sync/rename, but publication is abandoned before
    // the writer can commit a manifest or remove acknowledged hot events.
    let file = export_bounded(&mut conn, &archives, 20, 100, 3600000, work())
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

    let file = export_bounded(&mut conn, &archives, 20, 100, 3600000, work())
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

    let tail = db.tail_filtered(Some("0".into()), filtered).await;

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

    let result = db.tail_filtered(Some("0".into()), q.clone()).await.unwrap();

    assert!(result.events.is_empty());
    assert_eq!(result.next_cursor.as_deref(), Some("3"));

    let mut q = q;
    q.sources = vec!["b".into()];

    let result = db.tail_filtered(Some("2".into()), q).await.unwrap();

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
    let dir = tempfile::tempdir().unwrap();

    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        reader_threads: 2,
        query_timeout_ms: 500,
        ..StorageConfig::default()
    })
    .unwrap();
    let q = Query {
        from: 0,
        to: 100,
        limit: 10,
        ..Query::default()
    };

    let busy = db.clone();
    let busy_q = q.clone();
    let running = tokio::spawn(async move { busy.query(busy_q, Operation::Expensive).await });
    tokio::time::sleep(Duration::from_millis(20)).await;

    for _ in 0..3 {
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(200), db.count(q.clone()))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    }

    assert!(matches!(
        running.await.unwrap(),
        Err(Error {
            kind: ErrorKind::Timeout,
            ..
        })
    ));

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

    let mut after = Some("0".into());
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
        db.tail_filtered_bounded(Some("0".into()), q, 1).await,
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
        db_error(r#"{"exception_type":"Internal","exception_message":"fatal"}"#).kind,
        ErrorKind::Integrity
    ));
    assert!(matches!(
        db_error(r#"{"exception_type":"IO","exception_message":"disk full"}"#).kind,
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
    let dir = tempfile::tempdir().unwrap();

    let db = Storage::open(StorageConfig {
        data_dir: dir.path().to_owned(),
        reader_threads: 1,
        query_timeout_ms: 100,
        ..StorageConfig::default()
    })
    .unwrap();
    let q = Query {
        from: 0,
        to: 100,
        limit: 10,
        ..Query::default()
    };

    assert!(matches!(
        db.query(q.clone(), Operation::Expensive).await,
        Err(Error {
            kind: ErrorKind::Timeout,
            ..
        })
    ));
    assert_eq!(db.count(q.clone()).await.unwrap(), 0);
    assert_eq!(db.count(q).await.unwrap(), 0);

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
