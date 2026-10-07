use logbrook::{
    config::{Config, IndexSettings},
    indexes::IndexRegistry,
    model::{NormalizedEvent, Query},
};

fn config(root: &std::path::Path) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = root.to_owned();
    config
        .ingest_tokens
        .insert("synthetic-index-write-token".into(), "app".into());
    config
        .read_tokens
        .insert("synthetic-index-read-token".into(), vec![]);
    config
}

fn event(message: &str) -> NormalizedEvent {
    NormalizedEvent {
        event_time: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        level: 30,
        message: message.into(),
        service: None,
        logger: None,
        host: None,
        pid: None,
        attributes: serde_json::json!({}),
    }
}

#[tokio::test]
async fn isolates_indexes_and_discovers_them_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    config.indexes.insert(
        "payments".into(),
        IndexSettings {
            retention_days: Some(30),
            max_size_gb: Some(20),
            ..Default::default()
        },
    );

    let registry = IndexRegistry::open(config.clone()).await.unwrap();

    assert_eq!(
        registry.get("payments").unwrap().config.retention_ms,
        30 * 86_400_000
    );

    let payments = registry.get("payments").unwrap();
    let audit = registry.ensure("audit").await.unwrap();
    payments
        .storage
        .append("app".into(), vec![event("payment")])
        .await
        .unwrap();
    audit
        .storage
        .append("app".into(), vec![event("audit"), event("audit-2")])
        .await
        .unwrap();

    assert!(root.path().join("indexes/payments/events.duckdb").is_file());
    assert!(root.path().join("indexes/audit/events.duckdb").is_file());
    assert!(!root.path().join("events.duckdb").exists());

    registry.shutdown().await.unwrap();

    let restarted = IndexRegistry::open(config).await.unwrap();
    let all = Query {
        from: 0,
        to: i64::MAX,
        limit: 10,
        ..Default::default()
    };

    assert_eq!(
        restarted
            .get("default")
            .unwrap()
            .storage
            .count(all.clone())
            .await
            .unwrap(),
        0
    );

    assert_eq!(
        restarted
            .get("payments")
            .unwrap()
            .storage
            .count(all.clone())
            .await
            .unwrap(),
        1
    );

    assert_eq!(
        restarted
            .get("audit")
            .unwrap()
            .storage
            .count(all)
            .await
            .unwrap(),
        2
    );

    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_creation_is_idempotent_bounded_and_shutdown_closes_admission() {
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    config.max_indexes = 2;

    let registry = IndexRegistry::open(config).await.unwrap();

    let (first, second) = tokio::join!(registry.ensure("payments"), registry.ensure("payments"));
    first.unwrap();
    second.unwrap();

    assert_eq!(registry.list().len(), 2);
    assert!(registry.ensure("audit").await.is_err());
    assert!(!root.path().join("indexes/audit").exists());
    registry.stop_admission();

    assert!(!registry.is_ready());
    assert!(registry.ensure("payments").await.is_err());

    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn old_flat_layout_is_not_silently_ignored() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("events.duckdb"), b"untouched fixture").unwrap();

    let result = IndexRegistry::open(config(root.path())).await;

    assert!(
        result
            .err()
            .unwrap()
            .message
            .contains("legacy flat data layout")
    );
    assert_eq!(
        std::fs::read(root.path().join("events.duckdb")).unwrap(),
        b"untouched fixture"
    );
    assert!(!root.path().join("indexes").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn managed_index_symlinks_are_rejected() {
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("indexes")).unwrap();
    symlink(outside.path(), root.path().join("indexes/default")).unwrap();

    assert!(IndexRegistry::open(config(root.path())).await.is_err());
    std::fs::remove_file(root.path().join("indexes/default")).unwrap();
    std::fs::create_dir(root.path().join("indexes/default")).unwrap();
    symlink(outside.path(), root.path().join("indexes/default/archives")).unwrap();

    assert!(IndexRegistry::open(config(root.path())).await.is_err());
    assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn offline_flat_layout_move_preserves_hot_and_archived_data() {
    use logbrook::storage::Storage;

    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());

    // Simulate the previous layout and its manifest-relative archive paths.
    let old = Storage::open(config.storage.clone()).unwrap();
    let first = event("archived-before-layout-move");
    old.append("app".into(), vec![first.clone()]).await.unwrap();
    old.archive(first.event_time + 1).await.unwrap();
    old.append("app".into(), vec![event("hot-before-layout-move")])
        .await
        .unwrap();

    old.shutdown().await.unwrap();

    let destination = root.path().join("indexes/default");
    std::fs::create_dir_all(&destination).unwrap();
    for name in ["events.duckdb", "events.duckdb.wal", "archives", "temp"] {
        let original = root.path().join(name);
        if original.exists() {
            std::fs::rename(original, destination.join(name)).unwrap();
        }
    }

    let registry = IndexRegistry::open(config).await.unwrap();
    let result = registry
        .get("default")
        .unwrap()
        .storage
        .search(Query {
            from: 0,
            to: i64::MAX,
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(result.events.len(), 2);

    let mut messages: Vec<_> = result
        .events
        .into_iter()
        .map(|event| event.message)
        .collect();
    messages.sort();

    assert_eq!(
        messages,
        ["archived-before-layout-move", "hot-before-layout-move"]
    );

    registry.shutdown().await.unwrap();
}

fn all_events() -> Query {
    Query {
        from: 0,
        to: i64::MAX,
        limit: 10,
        ..Default::default()
    }
}

#[tokio::test]
async fn deletion_removes_all_data_reclaims_capacity_and_never_reuses_old_handles() {
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    config.max_indexes = 2;

    let registry = IndexRegistry::open(config.clone()).await.unwrap();
    let old = registry.ensure("audit").await.unwrap();
    let archived = event("deleted-archive");
    old.storage
        .append("app".into(), vec![archived.clone()])
        .await
        .unwrap();
    old.storage.archive(archived.event_time + 1).await.unwrap();
    old.storage
        .append("app".into(), vec![event("deleted-hot")])
        .await
        .unwrap();
    std::fs::write(
        old.config.storage.data_dir.join("unmanaged-file"),
        b"delete this too",
    )
    .unwrap();

    assert!(registry.ensure("third").await.is_err());

    registry.delete("audit").await.unwrap();

    assert!(registry.get("audit").is_none());
    assert!(!root.path().join("indexes/audit").exists());
    assert!(!old.storage.is_ready());
    assert!(
        old.storage
            .append("app".into(), vec![event("stale-write")])
            .await
            .is_err()
    );
    assert!(old.storage.count(all_events()).await.is_err());
    assert!(registry.is_ready());

    let replacement = registry.ensure("audit").await.unwrap();

    assert_eq!(replacement.storage.count(all_events()).await.unwrap(), 0);

    // A cloned obsolete handle cannot close the replacement's connections.
    let (first, second) = tokio::join!(old.storage.shutdown(), old.storage.shutdown());
    first.unwrap();
    second.unwrap();
    replacement
        .storage
        .append("app".into(), vec![event("replacement")])
        .await
        .unwrap();

    assert_eq!(replacement.storage.count(all_events()).await.unwrap(), 1);

    registry.shutdown().await.unwrap();

    let restarted = IndexRegistry::open(config.clone()).await.unwrap();

    assert_eq!(
        restarted
            .get("audit")
            .unwrap()
            .storage
            .count(all_events())
            .await
            .unwrap(),
        1
    );
    restarted.delete("audit").await.unwrap();

    restarted.shutdown().await.unwrap();

    let restarted = IndexRegistry::open(config).await.unwrap();

    assert!(restarted.get("audit").is_none());
    restarted.ensure("third").await.unwrap();

    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn default_and_preconfigured_indexes_require_configuration_changes() {
    use logbrook::model::ErrorKind;

    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    config
        .indexes
        .insert("managed".into(), IndexSettings::default());

    let registry = IndexRegistry::open(config).await.unwrap();
    let error = registry.delete("default").await.unwrap_err();

    assert_eq!(error.kind, ErrorKind::Invalid);
    assert!(error.message.contains("default"));

    let error = registry.delete("managed").await.unwrap_err();

    assert_eq!(error.kind, ErrorKind::Invalid);
    assert!(error.message.contains("configuration and restart"));
    assert_eq!(
        registry.delete("unknown").await.unwrap_err().kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        registry.delete("../outside").await.unwrap_err().kind,
        ErrorKind::Invalid
    );
    assert!(registry.is_ready());
    assert_eq!(registry.list().len(), 2);

    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn deletion_completion_survives_the_admitted_callers_cancellation() {
    let root = tempfile::tempdir().unwrap();

    let registry = IndexRegistry::open(config(root.path())).await.unwrap();
    let old = registry.ensure("cancelled").await.unwrap();
    old.storage
        .append("app".into(), vec![event("old")])
        .await
        .unwrap();

    // Extra real data gives cleanup work to drain after admission closes.
    for n in 0..200 {
        let nested = old.config.storage.data_dir.join(format!("extra-{n}"));
        std::fs::create_dir(&nested).unwrap();
        std::fs::write(nested.join("file"), b"complete cleanup").unwrap();
    }

    let caller = tokio::spawn({
        let registry = registry.clone();
        async move { registry.delete("cancelled").await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while old.storage.is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    caller.abort();

    let _ = caller.await;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if registry.get("cancelled").is_none() {
                // ensure shares the lifecycle gate and waits for physical cleanup.
                let replacement = registry.ensure("cancelled").await.unwrap();

                assert_eq!(replacement.storage.count(all_events()).await.unwrap(), 0);
                break;
            }

            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn deletion_and_recreation_are_serialized_and_shutdown_drains_both() {
    use logbrook::model::ErrorKind;

    let root = tempfile::tempdir().unwrap();

    let registry = IndexRegistry::open(config(root.path())).await.unwrap();
    let old = registry.ensure("racing").await.unwrap();
    old.storage
        .append("app".into(), vec![event("old")])
        .await
        .unwrap();

    let deletion = tokio::spawn({
        let registry = registry.clone();
        async move { registry.delete("racing").await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while old.storage.is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let creation = tokio::spawn({
        let registry = registry.clone();
        async move { registry.ensure("racing").await }
    });

    let (deletion, creation, shutdown) = tokio::join!(deletion, creation, registry.shutdown());

    // Shutdown may close admission before or after the admitted deletion.
    assert!(deletion.unwrap().is_ok());

    if let Err(error) = creation.unwrap() {
        assert_eq!(error.kind, ErrorKind::Unavailable);
    }

    shutdown.unwrap();

    assert!(!old.storage.is_ready());
    assert!(!registry.is_ready());
    assert!(registry.list().is_empty());
    assert!(registry.ensure("racing").await.is_err());
}

#[tokio::test]
async fn restart_cleans_committed_deletion_tombstones_without_discovery() {
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    config.max_indexes = 1;
    let tombstone = root.path().join(".index-trash/deleted-crashed/data");
    std::fs::create_dir_all(tombstone.join("archives")).unwrap();
    std::fs::write(tombstone.join("events.duckdb"), b"deleted database").unwrap();
    std::fs::write(tombstone.join("archives/old.parquet"), b"deleted archive").unwrap();

    let registry = IndexRegistry::open(config).await.unwrap();

    assert_eq!(registry.list().len(), 1);
    assert!(!tombstone.parent().unwrap().exists());

    registry.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn deletion_rejects_symlinks_and_closed_failure_can_be_retried_safely() {
    use logbrook::model::ErrorKind;
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("untouched"), b"outside data").unwrap();

    let registry = IndexRegistry::open(config(root.path())).await.unwrap();
    let index = registry.ensure("safe").await.unwrap();
    let link = index.config.storage.data_dir.join("outside-link");
    symlink(outside.path(), &link).unwrap();

    assert_eq!(
        registry.delete("safe").await.unwrap_err().kind,
        ErrorKind::Invalid
    );
    assert!(index.storage.is_ready());
    assert!(registry.is_ready());
    std::fs::remove_file(link).unwrap();
    symlink(outside.path(), root.path().join(".index-trash")).unwrap();

    assert_eq!(
        registry.delete("safe").await.unwrap_err().kind,
        ErrorKind::Invalid
    );
    assert!(!index.storage.is_ready());
    assert!(!registry.is_ready());
    assert!(registry.get("safe").is_some());
    assert_eq!(
        registry.ensure("safe").await.err().unwrap().kind,
        ErrorKind::Unavailable
    );
    assert!(root.path().join("indexes/safe/events.duckdb").is_file());
    assert_eq!(
        std::fs::read(outside.path().join("untouched")).unwrap(),
        b"outside data"
    );
    assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 1);
    std::fs::remove_file(root.path().join(".index-trash")).unwrap();
    registry.delete("safe").await.unwrap();

    assert!(registry.is_ready());

    registry.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn trash_symlinks_and_unknown_entries_cannot_be_removed_on_restart() {
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("untouched"), b"outside data").unwrap();

    let tombstone = root.path().join(".index-trash/deleted-unsafe");
    std::fs::create_dir_all(&tombstone).unwrap();
    symlink(outside.path(), tombstone.join("data")).unwrap();

    assert!(IndexRegistry::open(config(root.path())).await.is_err());
    assert_eq!(
        std::fs::read(outside.path().join("untouched")).unwrap(),
        b"outside data"
    );
    std::fs::remove_file(tombstone.join("data")).unwrap();
    std::fs::remove_dir(&tombstone).unwrap();

    let unknown = root.path().join(".index-trash/my-personal-data");
    std::fs::create_dir(&unknown).unwrap();
    std::fs::write(unknown.join("keep"), b"not an index tombstone").unwrap();

    assert!(IndexRegistry::open(config(root.path())).await.is_err());
    assert!(unknown.join("keep").is_file());
}
