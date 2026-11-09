//! Storage attribute filtering regressions checked against explicit fixtures.
use logbrook::{
    attribute_filter::{AttributeFilters, AttributeValue},
    config::StorageConfig,
    model::{NormalizedEvent, Query},
    storage::Storage,
};
use serde_json::{Value, json};

fn filter(key: &str, value: Value) -> AttributeFilters {
    AttributeFilters::parse(
        &json!({
            "attributes": [{"key":key,"op":"eq","value":value}],
        })
        .to_string(),
        usize::MAX,
    )
    .unwrap()
}

fn numeric_filter(value: &str) -> AttributeFilters {
    AttributeFilters::parse(
        &format!(r#"{{"attributes":[{{"key":"n","op":"eq","value":{value}}}]}}"#),
        usize::MAX,
    )
    .unwrap()
}

fn config(path: &std::path::Path) -> StorageConfig {
    StorageConfig {
        data_dir: path.to_owned(),
        reader_threads: 1,
        duckdb_threads: 1,
        memory_limit: "128MB".into(),
        query_timeout_ms: 10000,
        ..StorageConfig::default()
    }
}

fn query(attributes: AttributeFilters) -> Query {
    Query {
        from: 0,
        to: 1000,
        limit: 2,
        attributes,
        ..Query::default()
    }
}

fn event(time: i64, attributes: Value) -> NormalizedEvent {
    NormalizedEvent {
        event_time: time,
        level: 30,
        service: Some("api".into()),
        logger: Some("worker".into()),
        host: Some("host".into()),
        pid: None,
        message: "message".into(),
        attributes,
    }
}

async fn ids(db: &Storage, mut q: Query) -> Vec<String> {
    let mut result = Vec::new();
    loop {
        let page = db.search(q.clone()).await.unwrap();
        result.extend(page.events.into_iter().map(|event| event.id));
        if page.next_cursor.is_none() {
            break;
        }
        q.cursor = page.next_cursor;
    }
    result
}

#[test]
fn strict_parser_normalizes_without_losing_numeric_precision() {
    for value in ["123", "123.0", "1.23e2"] {
        assert_eq!(
            numeric_filter(value).conditions()[0].value(),
            &AttributeValue::Integer(123)
        );
    }
    for value in ["0", "-0", "-0.0", "1e-999"] {
        assert_eq!(
            numeric_filter(value).conditions()[0].value(),
            &AttributeValue::Integer(0)
        );
    }

    assert_eq!(
        numeric_filter("18446744073709551615").conditions()[0].value(),
        &AttributeValue::Integer(i128::from(u64::MAX))
    );
    assert_eq!(
        numeric_filter("18446744073709551616.0").conditions()[0].value(),
        &AttributeValue::Float(18_446_744_073_709_551_616.0_f64.to_bits())
    );
    assert_ne!(
        numeric_filter("9007199254740992"),
        numeric_filter("9007199254740993")
    );

    let dedup = AttributeFilters::parse(
        r#"{"attributes":[{"key":"z","op":"eq","value":1.0},{"key":"a/~","op":"eq","value":""},{"key":"z","op":"eq","value":1}]}"#,
        usize::MAX,
    ).unwrap();
    assert_eq!(dedup.conditions().len(), 2);
    assert_eq!(dedup.conditions()[0].pointer(), "/a~1~0");
    assert_eq!(
        dedup,
        serde_json::from_str(&serde_json::to_string(&dedup).unwrap()).unwrap()
    );

    assert!(
        AttributeFilters::parse(r#"{"attributes":[]}"#, 1)
            .unwrap()
            .is_empty()
    );

    for invalid in [
        "",
        "null",
        "{}",
        "[]",
        r#"{"attributes":{}}"#,
        r#"{"attributes":[],"extra":1}"#,
        r#"{"attributes":[],"attributes":[]}"#,
        r#"{"attributes":[{"key":"n","key":"n","op":"eq","value":1}]}"#,
        r#"{"attributes":[{"key":"n","op":"eq","value":1,"value":1}]}"#,
        r#"{"attributes":[{"key":"n","op":"eq","op":"eq","value":1}]}"#,
        r#"{"attributes":[{"key":"n","op":"gt","value":1}]}"#,
        r#"{"attributes":[{"key":"n","value":1}]}"#,
        r#"{"attributes":[{"key":"n","op":"eq"}]}"#,
        r#"{"attributes":[{"key":"","op":"eq","value":1}]}"#,
        r#"{"attributes":[{"key":"n","op":"eq","value":null}]}"#,
        r#"{"attributes":[{"key":"n","op":"eq","value":[]}]}"#,
        r#"{"attributes":[{"key":"n","op":"eq","value":{}}]}"#,
        r#"{"attributes":[{"key":"n","op":"eq","value":18446744073709551616}]}"#,
        r#"{"attributes":[{"key":"n","op":"eq","value":-9223372036854775809}]}"#,
        r#"{"attributes":[{"key":"n","op":"eq","value":1e999}]}"#,
        r#"{"attributes":[{"key":"n","op":"eq","value":1},{"key":"n","op":"eq","value":true}]}"#,
    ] {
        assert!(
            AttributeFilters::parse(invalid, usize::MAX).is_err(),
            "{invalid}"
        );
    }
}

#[test]
fn parser_limits_apply_before_deduplication_and_conversion() {
    for (key, value, valid) in [
        ("k".repeat(256), "v".repeat(1024), true),
        ("k".repeat(257), String::new(), false),
        ("é".repeat(128), String::new(), true),
        ("é".repeat(129), String::new(), false),
        ("k".into(), "v".repeat(1025), false),
    ] {
        let input = json!({
            "attributes": [{"key":key,"op":"eq","value":value}],
        })
        .to_string();
        assert_eq!(AttributeFilters::parse(&input, usize::MAX).is_ok(), valid);
    }

    let condition = json!({
        "key": "k",
        "op": "eq",
        "value": 1,
    });
    for count in [16, 17] {
        let input = json!({
            "attributes": vec![condition.clone(); count],
        })
        .to_string();
        assert_eq!(
            AttributeFilters::parse(&input, usize::MAX).is_ok(),
            count == 16
        );
    }

    let input = r#"{"attributes":[]}"#;
    assert!(
        AttributeFilters::parse(
            &format!("{input}{}", " ".repeat(4096 - input.len())),
            usize::MAX
        )
        .is_ok()
    );
    assert!(
        AttributeFilters::parse(
            &format!("{input}{}", " ".repeat(4097 - input.len())),
            usize::MAX
        )
        .is_err()
    );

    let token = format!("0.{}1", "0".repeat(125));
    assert_eq!(token.len(), 128);
    assert!(std::panic::catch_unwind(|| numeric_filter(&token)).is_ok());

    let input = format!(r#"{{"attributes":[{{"key":"n","op":"eq","value":{token}0}}]}}"#);
    assert!(AttributeFilters::parse(&input, usize::MAX).is_err());

    let filter = filter("é", json!("é"));
    assert!(filter.validate(1).is_err());
    assert!(filter.validate(2).is_ok());
}

#[tokio::test]
async fn bundled_numeric_predicates_are_exact_in_hot_and_archived_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path())).unwrap();

    let values = [
        "0",
        "-0.0",
        "1",
        "1.0",
        "1.5",
        "-1.5",
        "5e-324",
        "1e-999",
        "-9223372036854775808",
        "9223372036854775807",
        "18446744073709551615",
        "9007199254740992",
        "9007199254740993",
        "9007199254740993.0",
        "9223372036854775807.0",
        "9223372036854775808",
        "18446744073709551615.0",
        "18446744073709551616.0",
        "-9223372036854777856.0",
        "2",
        "\"1\"",
        "true",
        "null",
        "{}",
        "[]",
    ];
    db.append(
        "a".into(),
        values
            .iter()
            .enumerate()
            .map(|(i, value)| {
                event(
                    i as i64 + 1,
                    serde_json::from_str(&format!(r#"{{"n":{value}}}"#)).unwrap(),
                )
            })
            .collect(),
    )
    .await
    .unwrap();

    // Explicit expected IDs are independent of the SQL normalizer/compiler.
    let cases: &[(&str, &[&str])] = &[
        ("0", &["8", "2", "1"]),
        ("-0.0", &["8", "2", "1"]),
        ("1", &["4", "3"]),
        ("1e0", &["4", "3"]),
        ("1.5", &["5"]),
        ("-1.5", &["6"]),
        ("5e-324", &["7"]),
        ("-9223372036854775808", &["9"]),
        ("9223372036854775807", &["10"]),
        ("18446744073709551615", &["11"]),
        ("9007199254740992", &["12"]),
        ("9007199254740993", &["13"]),
        ("9007199254740994", &["14"]),
        ("9007199254740993.0", &["14"]),
        ("9223372036854775808", &["16", "15"]),
        ("18446744073709551616.0", &["18", "17"]),
        ("-9223372036854777856.0", &["19"]),
        ("2", &["20"]),
    ];
    for archived in [false, true] {
        if archived {
            db.archive(1000).await.unwrap();
        }

        for &(value, expected) in cases {
            let q = query(numeric_filter(value));
            assert_eq!(
                ids(&db, q.clone()).await,
                expected,
                "filter={value}, archived={archived}"
            );
            assert_eq!(db.count(q).await.unwrap(), expected.len() as u64);
        }
    }

    db.shutdown().await.unwrap();
}

async fn assert_selection(db: &Storage, q: &Query) {
    assert_eq!(ids(db, q.clone()).await, ["8", "7", "3", "1"]);
    assert_eq!(db.count(q.clone()).await.unwrap(), 4);

    let histogram = db.histogram(q.clone(), 10).await.unwrap();
    assert_eq!(
        histogram
            .iter()
            .map(|b| (b.time, b.count))
            .collect::<Vec<_>>(),
        [(10, 1), (20, 2), (30, 1)]
    );

    let facets = db.facets(q.clone()).await.unwrap();
    for dimension in [
        facets.services,
        facets.loggers,
        facets.hosts,
        facets.levels,
        facets.sources,
    ] {
        assert_eq!(dimension.iter().map(|v| v.count).sum::<u64>(), 4);
    }
}

#[tokio::test]
async fn predicates_precede_paging_aggregations_and_tail_across_compaction_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path())).unwrap();

    let attributes = AttributeFilters::parse(r#"{"attributes":[{"key":"run","op":"eq","value":"r"},{"key":"active","op":"eq","value":true}]}"#,
        usize::MAX).unwrap();
    let q = query(attributes);
    let initial = db.tail_filtered(None, q.clone()).await.unwrap().next_cursor;

    let matching = json!({
        "run": "r",
        "active": true,
    });
    db.append(
        "a".into(),
        vec![
            event(10, matching.clone()),
            event(
                50,
                json!({
                    "run": "other",
                    "active": true,
                }),
            ),
            event(20, matching.clone()),
        ],
    )
    .await
    .unwrap();
    db.archive(1000).await.unwrap();

    db.append(
        "a".into(),
        vec![
            event(
                60,
                json!({
                    "run": "r",
                    "active": "true",
                }),
            ),
            event(
                70,
                json!({
                    "run": "r",
                    "active": 1,
                }),
            ),
            event(
                80,
                json!({
                    "run": "r",
                    "active": null,
                }),
            ),
            event(20, matching.clone()),
        ],
    )
    .await
    .unwrap();
    db.archive(1000).await.unwrap();

    db.append(
        "a".into(),
        vec![
            event(30, matching),
            event(90, json!({})),
            event(
                95,
                json!({
                    "run": "r",
                    "active": false,
                }),
            ),
        ],
    )
    .await
    .unwrap();

    assert_selection(&db, &q).await;

    let mut after = initial;
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

    assert_eq!(tail_ids, ["1", "3", "7", "8"]);

    assert!(db.compact().await.unwrap() > 0);
    assert_selection(&db, &q).await;

    db.shutdown().await.unwrap();
    drop(db);

    let db = Storage::open(config(dir.path())).unwrap();
    assert_selection(&db, &q).await;
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn literal_keys_types_unicode_and_root_object_guard() {
    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path())).unwrap();
    let keys = [
        "actor.job",
        "a/b",
        "a~b",
        "*",
        "$",
        "' OR true --",
        "quote\"\\",
        "control\n\0",
        "é",
        "source",
        "0",
    ];
    for key in keys {
        db.append("a".into(), vec![event(1, json!({key:"value"}))])
            .await
            .unwrap();
    }

    db.append(
        "a".into(),
        vec![
            event(2, json!(["value"])),
            event(3, json!("value")),
            event(
                4,
                json!({
                    "actor": {
                        "job": "value",
                    },
                }),
            ),
            event(
                5,
                json!({
                    "é": "VALUE",
                }),
            ),
            event(
                6,
                json!({
                    "e\u{301}": "value",
                }),
            ),
        ],
    )
    .await
    .unwrap();

    for (index, key) in keys.into_iter().enumerate() {
        assert_eq!(
            ids(&db, query(filter(key, json!("value")))).await,
            [(index + 1).to_string()],
            "literal key {key:?}"
        );
    }

    assert_eq!(
        db.count(query(filter("é", json!("VALUE")))).await.unwrap(),
        1
    );
    assert_eq!(
        db.count(query(filter("e\u{301}", json!("value"))))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db.count(query(filter("missing", json!(true))))
            .await
            .unwrap(),
        0
    );

    db.shutdown().await.unwrap();
}

fn selection_case(
    conditions: &[(&str, Value)],
    base: Value,
    replacements: &[(&str, Value)],
    other_roots: &[Value],
) -> (AttributeFilters, Vec<Value>) {
    let attributes = AttributeFilters::parse(
        &json!({
            "attributes": conditions
                .iter()
                .map(|(key, value)| json!({"key": key, "op": "eq", "value": value}))
                .collect::<Vec<_>>(),
        })
        .to_string(),
        usize::MAX,
    )
    .unwrap();

    // The first three rows match. Every other row changes or removes one
    // mandatory value, or changes the root, independently of SQL compilation.
    let mut rows = vec![base.clone(); 3];
    for (key, replacement) in replacements {
        let mut changed = base.clone();
        changed[*key] = replacement.clone();
        rows.push(changed);
    }

    for (key, _) in conditions {
        let mut missing = base.clone();
        missing.as_object_mut().unwrap().remove(*key);
        rows.push(missing);

        let mut null = base.clone();
        null[*key] = Value::Null;
        rows.push(null);
    }

    rows.extend(other_roots.iter().cloned());
    (attributes, rows)
}

async fn assert_all_operations(db: &Storage, q: &Query, expected_ingestion_ids: &[String]) {
    let mut expected_search = expected_ingestion_ids.to_vec();
    expected_search.reverse();
    assert_eq!(ids(db, q.clone()).await, expected_search);
    assert_eq!(db.count(q.clone()).await.unwrap(), 3);

    assert_eq!(
        db.histogram(q.clone(), 10)
            .await
            .unwrap()
            .into_iter()
            .map(|bucket| (bucket.time, bucket.count))
            .collect::<Vec<_>>(),
        [(10, 1), (20, 2)]
    );

    let facets = db.facets(q.clone()).await.unwrap();
    for (dimension, expected) in [
        (facets.services, "api"),
        (facets.loggers, "worker"),
        (facets.hosts, "host"),
        (facets.levels, "30"),
        (facets.sources, q.sources[0].as_str()),
    ] {
        assert_eq!(dimension.len(), 1);
        assert_eq!(dimension[0].value, expected);
        assert_eq!(dimension[0].count, 3);
    }

    let mut after = Some(logbrook::query_cursor::encode_tail(q, 0).unwrap());
    let mut actual_tail = Vec::new();
    loop {
        let page = db.tail_filtered(after, q.clone()).await.unwrap();
        let empty = page.events.is_empty();
        actual_tail.extend(page.events.into_iter().map(|event| event.id));
        after = page.next_cursor;
        if empty {
            break;
        }
    }

    assert_eq!(actual_tail, expected_ingestion_ids);
}

#[tokio::test]
async fn scalar_and_batched_strings_preserve_typed_conjunctions_on_every_storage_tier() {
    let literal = "a/~\0雪";
    let text = "é\n\0' OR true --";
    let cases = [
        selection_case(
            &[("0", json!("true")), ("1", json!(text))],
            json!({
                "0": "true",
                "1": text,
            }),
            &[
                ("0", json!(true)),
                ("0", json!(1)),
                ("0", json!(["true"])),
                ("1", json!(false)),
                (
                    "1",
                    json!({
                        "text": text,
                    }),
                ),
                ("1", json!("e\u{301}\n\0' OR true --")),
            ],
            &[json!(["true", text]), json!(text), Value::Null],
        ),
        selection_case(
            &[(literal, json!(text)), ("actor.job", json!(""))],
            json!({
                literal: text,
                "actor.job": "",
            }),
            &[
                (literal, json!("É\n\0' OR true --")),
                (
                    literal,
                    json!({
                        "value": text,
                    }),
                ),
                ("actor.job", json!(false)),
                ("actor.job", json!([])),
            ],
            &[json!({
                "a": {
                    "~\0雪": text,
                },
                "actor": {
                    "job": "",
                },
            })],
        ),
        selection_case(
            &[
                ("0", json!("true")),
                ("1", json!(text)),
                ("enabled", json!(true)),
                ("n", json!(9_007_199_254_740_993_u64)),
            ],
            json!({
                "0": "true",
                "1": text,
                "enabled": true,
                "n": 9_007_199_254_740_993_u64,
            }),
            &[
                ("0", json!(true)),
                ("1", json!([text])),
                ("enabled", json!(1)),
                ("enabled", json!("true")),
                ("enabled", json!(false)),
                ("n", json!(9_007_199_254_740_992_u64)),
                ("n", json!(9_007_199_254_740_992.0)),
                ("n", json!("9007199254740993")),
            ],
            &[json!(["true", text])],
        ),
        selection_case(
            &[
                ("run", json!("r")),
                ("actor", json!("a")),
                ("u", json!(u64::MAX)),
                ("i", json!(i64::MIN)),
            ],
            json!({
                "run": "r",
                "actor": "a",
                "u": u64::MAX,
                "i": i64::MIN,
            }),
            &[
                ("u", json!(u64::MAX - 1)),
                ("u", json!(18_446_744_073_709_551_616.0)),
                ("u", json!("18446744073709551615")),
                ("i", json!(i64::MIN + 1)),
                ("i", json!(-9_223_372_036_854_777_856.0)),
            ],
            &[],
        ),
        selection_case(
            &[("0", json!("r")), ("1", json!("a")), (literal, json!(text))],
            json!({
                "0": "r",
                "1": "a", literal: text,
            }),
            &[
                ("0", json!(true)),
                ("1", json!(1)),
                (literal, json!([text])),
                (literal, json!("e\u{301}\n\0' OR true --")),
            ],
            &[json!(["r", "a", text])],
        ),
        selection_case(
            &[("0", json!(true)), ("1", json!(u64::MAX))],
            json!({
                "0": true,
                "1": u64::MAX,
            }),
            &[
                ("0", json!("true")),
                ("0", json!(1)),
                ("1", json!(u64::MAX - 1)),
                ("1", json!("18446744073709551615")),
            ],
            &[json!([true, u64::MAX]), json!(true)],
        ),
        selection_case(
            &[
                ("0", json!("true")),
                ("flag", json!(true)),
                ("n", json!(1.5)),
            ],
            json!({
                "0": "true",
                "flag": true,
                "n": 1.5,
            }),
            &[
                ("0", json!(true)),
                ("flag", json!(1)),
                ("n", json!(2)),
                ("n", json!("1.5")),
            ],
            &[json!(["true"])],
        ),
    ];

    let dir = tempfile::tempdir().unwrap();
    let db = Storage::open(config(dir.path())).unwrap();
    let mut selections = Vec::new();
    let mut next_id = 1;
    for (index, (attributes, rows)) in cases.into_iter().enumerate() {
        let source = format!("case-{index}");
        let expected = (next_id..next_id + 3)
            .map(|id| id.to_string())
            .collect::<Vec<_>>();
        next_id += rows.len();

        db.append(
            source.clone(),
            rows.into_iter()
                .enumerate()
                .map(|(index, attributes)| {
                    let time = match index {
                        0 => 10,
                        1 | 2 => 20,
                        _ => 100 + index as i64,
                    };
                    event(time, attributes)
                })
                .collect(),
        )
        .await
        .unwrap();

        let mut q = query(attributes);
        q.sources = vec![source];
        selections.push((q, expected));
    }

    // A matching row in each case moves to the archive before its two tied
    // matches, exercising union ordering and every operation on mixed storage.
    for archive_before in [None, Some(20), Some(1000)] {
        if let Some(before) = archive_before {
            db.archive(before).await.unwrap();
        }

        for (q, expected) in &selections {
            assert_all_operations(&db, q, expected).await;
        }
    }

    db.shutdown().await.unwrap();
}
