use duckdb::Connection;
use logbrook::{config::Config, legacy::import_legacy, model::Query, storage::Storage};
use serde_json::json;

fn quoted(path: &std::path::Path) -> String {
    format!("'{}'", path.to_str().unwrap().replace('\'', "''"))
}

#[tokio::test]
async fn imports_read_only_database_and_recursive_parquet_without_deduplicating() {
    let directory = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let database = directory.path().join("legacy's.db");
    let archive_dir = directory.path().join("archives/year=2020/month=1/day=1");
    std::fs::create_dir_all(&archive_dir).unwrap();

    let archive = archive_dir.join("logs's.parquet");
    {
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE logs(level INTEGER,timestamp TIMESTAMP,service TEXT,name TEXT,\
                 pid INTEGER,hostname TEXT,msg TEXT,metadata JSON); \
                 INSERT INTO logs VALUES (30,TIMESTAMP '2020-01-01 00:00:00.123',\
                 'api','server',42,'worker','canonical',\
                 '{\"time\":0,\"level\":99,\"msg\":\"spoof\",\"source\":\"spoof\",\
                 \"extra\":{\"ok\":true}}');",
            )
            .unwrap();
        connection
            .execute_batch(&format!(
                "COPY (SELECT *,2020 AS year,1 AS month,1 AS day FROM logs) \
                 TO {} (FORMAT PARQUET); CHECKPOINT;",
                quoted(&archive)
            ))
            .unwrap();
    }

    let original_database = std::fs::read(&database).unwrap();
    let original_archive = std::fs::read(&archive).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = directory.path().join("target");
    config.max_events = 1;

    let storage = Storage::open(config.storage.clone()).unwrap();

    assert_eq!(
        import_legacy(
            &storage,
            &config,
            database.clone(),
            Some(directory.path().join("archives")),
            "legacy".into()
        )
        .await
        .unwrap(),
        2
    );

    let result = storage
        .search(Query {
            from: 0,
            to: 2_000_000_000_000,
            sources: vec!["legacy".into()],
            service: None,
            logger: None,
            host: None,
            message: None,
            min_level: None,
            limit: 10,
            cursor: None,
        })
        .await
        .unwrap();

    assert_eq!(result.events.len(), 2);

    for event in &result.events {
        assert_eq!(event.event_time, 1_577_836_800_123);
        assert_eq!(event.level, 30);
        assert_eq!(event.message, "canonical");
        assert_eq!(event.service.as_deref(), Some("api"));
        assert_eq!(event.logger.as_deref(), Some("server"));
        assert_eq!(event.host.as_deref(), Some("worker"));
        assert_eq!(event.pid, Some(42));
        assert_eq!(event.source, "legacy");
        assert_eq!(
            event.attributes,
            json!({ "source": "spoof", "extra": { "ok": true } })
        );
    }

    assert_ne!(result.events[0].id, result.events[1].id);
    assert_eq!(std::fs::read(&database).unwrap(), original_database);
    assert_eq!(std::fs::read(&archive).unwrap(), original_archive);

    storage.shutdown().await.unwrap();

    assert!(
        std::fs::read_dir(&config.storage.data_dir)
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("legacy-import-"))
    );
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_symlinked_legacy_inputs() {
    let directory = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let database = directory.path().join("legacy.db");
    {
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE logs(level INTEGER,timestamp TIMESTAMP,service TEXT,name TEXT,\
                 pid INTEGER,hostname TEXT,msg TEXT,metadata JSON)",
            )
            .unwrap();
    }

    let link = directory.path().join("linked.db");
    std::os::unix::fs::symlink(&database, &link).unwrap();

    let mut config = Config::default();
    config.storage.data_dir = directory.path().join("target");

    let storage = Storage::open(config.storage.clone()).unwrap();

    let error = import_legacy(&storage, &config, link, None, "legacy".into())
        .await
        .unwrap_err();

    assert!(error.message.contains("symlinks"));

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejects_glob_paths_before_reading_other_files() {
    let directory = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let database = directory.path().join("legacy.db");
    {
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE logs(level INTEGER,timestamp TIMESTAMP,service TEXT,name TEXT,\
                 pid INTEGER,hostname TEXT,msg TEXT,metadata JSON)",
            )
            .unwrap();
    }

    let archives = directory.path().join("archives[1]");
    std::fs::create_dir(&archives).unwrap();

    let mut config = Config::default();
    config.storage.data_dir = directory.path().join("target");

    let storage = Storage::open(config.storage.clone()).unwrap();

    let error = import_legacy(&storage, &config, database, Some(archives), "legacy".into())
        .await
        .unwrap_err();

    assert!(error.message.contains("glob"));
    assert!(
        std::fs::read_dir(&config.storage.data_dir)
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("legacy-import-"))
    );

    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_later_batch_reports_previously_committed_imports() {
    let directory = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let database = directory.path().join("legacy.db");
    {
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE logs(level INTEGER,timestamp TIMESTAMP,service TEXT,name TEXT,\
                 pid INTEGER,hostname TEXT,msg TEXT,metadata JSON); \
                 INSERT INTO logs VALUES \
                 (30,TIMESTAMP '2020-01-01',NULL,NULL,NULL,NULL,'one',NULL),\
                 (30,TIMESTAMP '2020-01-02',NULL,NULL,NULL,NULL,'two',NULL),\
                 (30,TIMESTAMP '2020-01-03',NULL,NULL,NULL,NULL,'bad','[]'); CHECKPOINT;",
            )
            .unwrap();
    }

    let mut config = Config::default();
    config.storage.data_dir = directory.path().join("target");
    config.max_events = 1;

    let storage = Storage::open(config.storage.clone()).unwrap();

    let error = import_legacy(&storage, &config, database, None, "legacy".into())
        .await
        .unwrap_err();

    assert!(error.message.contains("metadata must be an object"));
    assert!(error.message.contains("1 events already committed"));
    assert_eq!(storage.committed_events(), 1);

    storage.shutdown().await.unwrap();
}
