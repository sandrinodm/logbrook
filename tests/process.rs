mod support;

use serde_json::{Value, json};
use std::{collections::BTreeMap, fs, path::Path};
use support::{ADMIN, Fixture, READ, metric, now};

async fn recovered(fixture: &Fixture, timestamp: i64) -> BTreeMap<String, u64> {
    assert_eq!(fixture.count("default", "process-fixture-").await, 1003);
    let mut seen = BTreeMap::new();
    let mut cursor: Option<String> = None;

    // The page bound also makes a broken nonterminating cursor fail promptly.
    for _ in 0..20 {
        let mut url = reqwest::Url::parse(&format!("{}/logs", fixture.base)).unwrap();
        url.query_pairs_mut()
            .append_pair("from", &(timestamp - 3_600_000).to_string())
            .append_pair("to", &(timestamp + 10000).to_string())
            .append_pair("limit", "137");
        if let Some(cursor) = cursor {
            url.query_pairs_mut().append_pair("cursor", &cursor);
        }
        let (status, page) = fixture
            .request(
                &format!("{}?{}", url.path(), url.query().unwrap()),
                None,
                READ,
            )
            .await;
        assert_eq!(status, 200, "{page}");
        for event in page["events"].as_array().unwrap() {
            assert!(
                seen.insert(
                    event["id"].as_str().unwrap().into(),
                    event["attributes"]["fixture"].as_u64().unwrap()
                )
                .is_none()
            );
        }
        cursor = page["next_cursor"].as_str().map(String::from);
        if cursor.is_none() {
            let mut values: Vec<_> = seen.values().copied().collect();
            values.sort_unstable();
            assert_eq!(values, (0..1003).collect::<Vec<_>>());
            return seen;
        }
    }
    panic!("cursor did not terminate");
}

fn copy_tree(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let destination = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &destination);
        } else {
            fs::copy(entry.path(), destination).unwrap();
        }
    }
}

#[tokio::test]
async fn acknowledged_batches_survive_crash_archive_and_relocated_restore() {
    let mut fixture = Fixture::new();
    let timestamp = now();
    let events: Vec<_> = (0..1003).map(|index| json!({"time": timestamp-index, "level": 30, "msg": format!("process-fixture-{index}"), "fixture": index, "service": "process-check"})).collect();
    fixture.cli(&["check-config"], true, false);
    let input = fixture.root.path().join("seed.json");
    fs::write(&input, serde_json::to_vec(&events[..3]).unwrap()).unwrap();
    let output = fixture.cli(
        &["import", input.to_str().unwrap(), "--source", "default"],
        true,
        false,
    );
    assert_eq!(
        serde_json::from_str::<Value>(&output).unwrap(),
        json!({"imported":3})
    );
    fixture.start().await;
    assert_eq!(
        fixture
            .request("/logs?from=0&to=1", None, "invalid-synthetic-token")
            .await
            .0,
        401
    );

    // Four producers finish all acknowledgements before the ungraceful SIGKILL.
    let batches: Vec<_> = events[3..].chunks(50).map(|batch| json!(batch)).collect();
    for group in batches.chunks(4) {
        let results = futures_util::future::join_all(
            group
                .iter()
                .map(|batch| fixture.ingest("default", batch.clone())),
        )
        .await;
        assert!(results.iter().all(|status| *status == 200));
    }
    assert_eq!(
        fixture
            .ingest("default", json!([events[0], {"time":"invalid"}]))
            .await,
        400
    );
    fixture.crash();
    fixture.start().await;
    let before = recovered(&fixture, timestamp).await;
    fixture.stop();
    let output = fixture.cli(
        &["archive", "--before", &(timestamp + 10000).to_string()],
        true,
        false,
    );
    assert_eq!(
        serde_json::from_str::<Value>(&output).unwrap(),
        json!({"archived":1003})
    );
    let restored = fixture.root.path().join("restored");
    copy_tree(&fixture.data, &restored);
    fixture.env.insert(
        "LOGBROOK_DATA_DIR".into(),
        restored.to_str().unwrap().into(),
    );
    fixture.start().await;
    assert_eq!(recovered(&fixture, timestamp).await, before);
    assert_eq!(
        fixture
            .ingest(
                "default",
                json!([{"time": timestamp, "level":30,"msg":"after restore"}])
            )
            .await,
        200
    );
    let (_, page) = fixture
        .request(
            &format!(
                "/logs?from={}&to={}&message=after%20restore",
                timestamp - 3600000,
                timestamp + 10000
            ),
            None,
            READ,
        )
        .await;
    let new_id: u64 = page["events"][0]["id"].as_str().unwrap().parse().unwrap();
    assert!(
        new_id
            > before
                .keys()
                .map(|id| id.parse::<u64>().unwrap())
                .max()
                .unwrap()
    );
    fixture.stop();
}

#[tokio::test]
async fn retention_override_reclaims_archives_and_persists_monotonic_cutoff() {
    let mut fixture = Fixture::new();
    let config = fixture.root.path().join("config.toml");
    fs::write(
        &config,
        "retention_ms = 900000\narchive_after_ms = 240000\nmaintenance_interval_secs = 1\n",
    )
    .unwrap();
    fixture.config = Some(config);
    let timestamp = now();
    let old = json!({"time":timestamp-600000,"level":30,"msg":"old-retention-fixture"});
    let recent = json!({"time":timestamp,"level":30,"msg":"recent-retention-fixture"});
    fixture.start().await;
    assert_eq!(fixture.ingest("default", json!([old, recent])).await, 200);
    fixture
        .wait_metric("logbrook_storage_archive_files", "default", |value| {
            value >= 1.0
        })
        .await;
    assert_eq!(fixture.count("default", "retention-fixture").await, 2);
    fixture.stop();

    fixture
        .env
        .insert("LOGBROOK_RETENTION_MS".into(), "300000".into());
    fixture.start().await;
    fixture
        .wait_metric("logbrook_storage_retention_before_ms", "default", |value| {
            value > (timestamp - 600000) as f64
        })
        .await;
    assert_eq!(fixture.count("default", "retention-fixture").await, 1);
    fixture
        .wait_metric("logbrook_storage_archive_files", "default", |value| {
            value == 0.0
        })
        .await;
    let cutoff = metric(
        &fixture.metrics().await,
        "logbrook_storage_retention_before_ms",
        "default",
    );
    assert_eq!(fixture.ingest("default", json!([old])).await, 400);
    assert_eq!(fixture.count("default", "retention-fixture").await, 1);
    fixture.stop();

    fixture
        .env
        .insert("LOGBROOK_RETENTION_MS".into(), "900000".into());
    fixture.start().await;
    assert_eq!(fixture.count("default", "retention-fixture").await, 1);
    assert!(
        metric(
            &fixture.metrics().await,
            "logbrook_storage_retention_before_ms",
            "default"
        ) >= cutoff
    );
    assert_eq!(fixture.ingest("default", json!([old])).await, 400);
    assert_eq!(fixture.count("default", "retention-fixture").await, 1);
    fixture.stop();
}

#[tokio::test]
async fn management_cli_preserves_roles_pagination_and_index_lifecycle() {
    let mut fixture = Fixture::new();
    fixture.start().await;
    for name in ["payments", "payments", "audit"] {
        assert_eq!(fixture.cli_json(&["indexes", "create", name])["name"], name);
    }
    let timestamp = now();
    let message = "checkout / ? & café";
    assert_eq!(fixture.ingest("payments", json!([{"time":timestamp,"level":40,"msg":message,"service":"checkout"},{"time":timestamp+1,"level":30,"msg":"other","service":"worker"}])).await, 200);
    assert_eq!(
        fixture
            .ingest(
                "audit",
                json!([{"time":timestamp,"level":30,"msg":"sibling"}])
            )
            .await,
        200
    );
    let listing = fixture.cli_json(&["indexes", "list"]);
    let info = listing["indexes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["name"] == "payments")
        .unwrap();
    assert!(info["size_bytes"].as_u64().unwrap() > 0);
    assert_eq!(info["ready"], true);
    assert!(
        fixture.cli_json(&["indexes", "show", "payments"])["size_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        fixture
            .cli(&["indexes", "list"], true, true)
            .contains("payments")
    );
    let from = (timestamp - 1000).to_string();
    let to = (timestamp + 1000).to_string();
    let bounds = ["--from", &from, "--to", &to];
    let query = |prefix: &[&str], suffix: &[&str]| {
        let mut args = prefix.to_vec();
        args.extend(bounds);
        args.extend_from_slice(suffix);
        fixture.cli_json(&args)
    };
    assert_eq!(query(&["count", "payments"], &[])["count"], 2);
    let page = query(&["query", "payments"], &["--message", message]);
    assert_eq!(page["events"].as_array().unwrap().len(), 1);
    assert_eq!(page["events"][0]["message"], message);
    let first = query(&["query", "payments"], &["--limit", "1"]);
    let second = query(
        &["query", "payments"],
        &[
            "--limit",
            "1",
            "--cursor",
            first["next_cursor"].as_str().unwrap(),
        ],
    );
    assert_eq!(second["events"].as_array().unwrap().len(), 1);
    assert_ne!(first["events"][0]["id"], second["events"][0]["id"]);
    for args in [
        vec!["indexes", "delete", "payments"],
        vec!["--token", READ, "indexes", "delete", "payments", "--yes"],
        vec![
            "--token", ADMIN, "query", "payments", "--from", &from, "--to", &to,
        ],
        vec!["indexes", "delete", "default", "--yes"],
    ] {
        fixture.cli(&args, false, true);
    }
    assert_eq!(query(&["count", "payments"], &[])["count"], 2);
    assert_eq!(
        fixture.cli_json(&["indexes", "delete", "payments", "--yes"]),
        json!({"deleted":"payments"})
    );
    fixture.cli(&["indexes", "show", "payments"], false, true);
    assert_eq!(query(&["count", "audit"], &[])["count"], 1);
    assert_eq!(
        fixture.cli_json(&["indexes", "create", "payments"])["name"],
        "payments"
    );
    assert_eq!(query(&["count", "payments"], &[])["count"], 0);
    fixture.cli_json(&["indexes", "delete", "payments", "--yes"]);
    fixture.stop();
    fixture.start().await;
    assert!(
        fixture.cli_json(&["indexes", "list"])["indexes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["name"] != "payments")
    );
    assert_eq!(fixture.count("audit", "sibling").await, 1);
    fixture.stop();
}

const DAY: i64 = 86_400_000;
const SMALL_CAP: u64 = 16 * 1024 * 1024;
const RAISED_CAP: u64 = 64 * 1024 * 1024;

fn write_limits(fixture: &Fixture, cap: u64) {
    fs::write(
        fixture.config.as_ref().unwrap(),
        format!(
            r#"retention_days = 2
max_size_gb = 1
max_indexes = 3
maintenance_interval_secs = 1

[storage]
reader_threads = 1
duckdb_threads = 1
memory_limit = "384MB"
temp_limit = "512MB"

[indexes.payments]
retention_days = 7
max_size_bytes = {cap}

[indexes.orders]
retention_days = 30
max_size_gb = 20
"#
        ),
    )
    .unwrap();
}

async fn payment_visibility(fixture: &Fixture, accepted: &BTreeMap<i64, u64>) -> i64 {
    // Maintenance may commit a cutoff while the count runs; bracket its valid range.
    let earlier = metric(
        &fixture.metrics().await,
        "logbrook_storage_retention_before_ms",
        "payments",
    ) as i64;
    let observed = fixture.count("payments", "index-limits-fixture").await;
    let later = metric(
        &fixture.metrics().await,
        "logbrook_storage_retention_before_ms",
        "payments",
    ) as i64;
    assert!(later >= earlier);
    let lower: u64 = accepted.range(later..).map(|(_, count)| count).sum();
    let upper: u64 = accepted.range(earlier..).map(|(_, count)| count).sum();
    assert!(
        (lower..=upper).contains(&observed),
        "observed {observed}, expected {lower}..={upper}, cutoffs {earlier}..={later}"
    );
    later
}

#[tokio::test]
async fn named_index_limits_isolate_pressure_and_resume_after_policy_restart() {
    let mut fixture = Fixture::new();
    fixture.config = Some(fixture.root.path().join("config.toml"));
    fixture.env.extend([
        ("LOGBROOK_RETENTION_DAYS".into(), "7".into()),
        ("LOGBROOK_MAX_SIZE_GB".into(), "20".into()),
        ("LOGBROOK_INDEXES".into(), "payments,orders".into()),
    ]);
    write_limits(&fixture, SMALL_CAP);
    fixture.start().await;
    let initial = fixture.metrics().await;
    for index in ["default", "orders"] {
        assert_eq!(
            metric(&initial, "logbrook_storage_size_target_bytes", index),
            20_000_000_000.0
        );
    }
    assert_eq!(
        metric(&initial, "logbrook_storage_size_target_bytes", "payments"),
        SMALL_CAP as f64
    );
    let (status, names) = fixture.request("/indexes", None, READ).await;
    assert_eq!(status, 200);
    let mut names: Vec<_> = names["indexes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["name"].as_str().unwrap())
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["default", "orders", "payments"]);
    let timestamp = now();
    let five_day =
        json!({"time":timestamp-5*DAY,"level":30,"msg":"index-limits-fixture default-five-day"});
    let fifteen_day =
        json!({"time":timestamp-15*DAY,"level":30,"msg":"index-limits-fixture orders-fifteen-day"});
    assert_eq!(fixture.ingest("default", json!([five_day])).await, 200);
    assert_eq!(fixture.ingest("orders", json!([fifteen_day])).await, 200);
    for index in ["payments", "default"] {
        assert_eq!(fixture.ingest(index, json!([fifteen_day])).await, 400);
        assert_eq!(fixture.count(index, "orders-fifteen-day").await, 0);
    }
    assert_eq!(fixture.count("orders", "default-five-day").await, 0);
    assert_eq!(fixture.count("orders", "orders-fifteen-day").await, 1);
    assert_eq!(fixture.count("default", "default-five-day").await, 1);
    let mut accepted = BTreeMap::new();
    let timestamp = now();
    assert_eq!(
        fixture
            .ingest(
                "payments",
                json!([{"time":timestamp,"level":30,"msg":"index-limits-fixture payment-initial"}])
            )
            .await,
        200
    );
    accepted.insert(timestamp, 1);

    let mut rejected = None;
    let mut random_state = 0x4d595df4d0f33173_u64;
    for batch in 0..128 {
        let timestamp = now();
        let events: Vec<_> = (0..64).map(|offset| {
            // Seeded, varied ASCII keeps this pressure fixture reproducible and hard to compress.
            let payload: String = (0..2560).map(|_| {
                random_state ^= random_state << 13;
                random_state ^= random_state >> 7;
                random_state ^= random_state << 17;
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"[(random_state & 63) as usize] as char
            }).collect();
            json!({"time":timestamp,"level":30,"msg":format!("index-limits-fixture payment-batch-{batch}-event-{offset}"),"payload":payload})
        }).collect();
        assert!(serde_json::to_vec(&events).unwrap().len() <= 1024 * 1024);
        let status = fixture.ingest("payments", json!(events)).await;
        if status == 507 {
            rejected = Some(events[0]["msg"].as_str().unwrap().to_owned());
            break;
        }
        assert_eq!(status, 200);
        *accepted.entry(timestamp).or_default() += 64;
    }
    if rejected.is_none() {
        eprintln!(
            "size pressure used 32 MiB managed test ballast after 128 payload batches; this checks footprint admission, not sustained capacity"
        );
        use std::io::Write;
        // Test-owned ballast explicitly checks measured footprint admission if hot allocation stays small.
        let mut ballast =
            fs::File::create(fixture.data.join("indexes/payments/size-check-ballast.bin")).unwrap();
        let block = vec![0x71; 1024 * 1024];
        for _ in 0..32 {
            ballast.write_all(&block).unwrap();
        }
        ballast.sync_all().unwrap();
        fixture
            .wait_metric("logbrook_storage_size_bytes", "payments", |value| {
                value >= SMALL_CAP as f64
            })
            .await;
        let message = "index-limits-fixture payment-rejected";
        assert_eq!(
            fixture
                .ingest("payments", json!([{"time":now(),"level":30,"msg":message}]))
                .await,
            507
        );
        rejected = Some(message.into());
    }
    assert_eq!(
        fixture.count("payments", rejected.as_ref().unwrap()).await,
        0
    );
    assert_eq!(fixture.request("/ready", None, READ).await.0, 200);
    let before_restart = payment_visibility(&fixture, &accepted).await;
    assert_eq!(fixture.ingest("orders",json!([{"time":now(),"level":30,"msg":"index-limits-fixture orders-during-pressure"}])).await,200);
    assert_eq!(fixture.count("orders", "orders-during-pressure").await, 1);
    assert_eq!(fixture.count("payments", "orders-during-pressure").await, 0);
    for index in ["default", "orders"] {
        assert_eq!(fixture.count(index, "payment-").await, 0);
    }
    let mut directories: Vec<_> = fs::read_dir(fixture.data.join("indexes"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    directories.sort();
    assert_eq!(
        directories,
        ["default", "orders", "payments"].map(std::ffi::OsString::from)
    );
    let databases: Vec<_> = directories
        .iter()
        .map(|name| {
            fixture
                .data
                .join("indexes")
                .join(name)
                .join("events.duckdb")
        })
        .collect();
    assert!(databases.iter().all(|path| path.is_file()));
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let mut inodes: Vec<_> = databases
            .iter()
            .map(|path| fs::metadata(path).unwrap().ino())
            .collect();
        inodes.sort_unstable();
        inodes.dedup();
        assert_eq!(inodes.len(), 3);
    }
    assert!(!fixture.data.join("events.duckdb").exists());
    fixture.stop();

    write_limits(&fixture, RAISED_CAP);
    fixture.start().await;
    let raised = fixture.metrics().await;
    assert_eq!(
        metric(&raised, "logbrook_storage_size_target_bytes", "payments"),
        RAISED_CAP as f64
    );
    assert_eq!(
        metric(&raised, "logbrook_storage_size_target_bytes", "orders"),
        20_000_000_000.0
    );
    assert!(payment_visibility(&fixture, &accepted).await >= before_restart);
    assert_eq!(fixture.count("orders", "orders-fifteen-day").await, 1);
    assert_eq!(fixture.count("orders", "orders-during-pressure").await, 1);
    assert_eq!(fixture.count("default", "default-five-day").await, 1);
    for index in ["payments", "default"] {
        assert_eq!(fixture.ingest(index, json!([fifteen_day])).await, 400);
    }
    assert_eq!(fixture.ingest("orders",json!([{"time":now()-15*DAY,"level":30,"msg":"index-limits-fixture orders-restart-history"}])).await,200);
    let timestamp = now();
    assert_eq!(
        fixture
            .ingest(
                "payments",
                json!([{"time":timestamp,"level":30,"msg":"index-limits-fixture payment-resumed"}])
            )
            .await,
        200
    );
    *accepted.entry(timestamp).or_default() += 1;
    assert_eq!(fixture.count("payments", "payment-resumed").await, 1);
    payment_visibility(&fixture, &accepted).await;
    assert_eq!(fixture.request("/ready", None, READ).await.0, 200);
    fixture.stop();

    fixture.start().await;
    assert_eq!(
        metric(
            &fixture.metrics().await,
            "logbrook_storage_size_target_bytes",
            "payments"
        ),
        RAISED_CAP as f64
    );
    assert_eq!(fixture.count("payments", "payment-resumed").await, 1);
    for message in ["orders-fifteen-day", "orders-restart-history"] {
        assert_eq!(fixture.count("orders", message).await, 1);
    }
    assert_eq!(fixture.ingest("payments", json!([fifteen_day])).await, 400);
    payment_visibility(&fixture, &accepted).await;
    fixture.stop();
}
