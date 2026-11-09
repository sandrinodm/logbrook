//! Generated fixtures compare DuckDB to a deliberately small, independent oracle.
use std::collections::BTreeMap;

use logbrook::{
    attribute_filter::AttributeFilters,
    config::StorageConfig,
    model::{FacetValue, NormalizedEvent, Query},
    storage::Storage,
};
use proptest::prelude::*;
use serde_json::{Value, json};

#[derive(Clone, Debug, Eq, PartialEq)]
enum Scalar {
    Text(&'static str),
    Boolean(bool),
    Integer(i128),
    Half,
}

// The oracle knows each fixture's mathematical value directly. It never calls
// production normalization, AttributeValue equality, or the predicate compiler.
fn scalar(code: u8) -> Option<Scalar> {
    match code % 16 {
        0 => Some(Scalar::Text("Run")),
        1 => Some(Scalar::Text("run")),
        2 => Some(Scalar::Text("")),
        3 => Some(Scalar::Text("é\n雪")),
        4 => Some(Scalar::Boolean(true)),
        5 => Some(Scalar::Boolean(false)),
        6 => Some(Scalar::Integer(-7)),
        7 | 8 => Some(Scalar::Integer(1)),
        9 => Some(Scalar::Half),
        10 => Some(Scalar::Integer(i128::from(u64::MAX))),
        11 => Some(Scalar::Integer(9_007_199_254_740_993)),
        12 => Some(Scalar::Integer(0)),
        _ => None,
    }
}

fn stored(code: u8) -> Option<Value> {
    Some(match code % 16 {
        0 => json!("Run"),
        1 => json!("run"),
        2 => json!(""),
        3 => json!("é\n雪"),
        4 => json!(true),
        5 => json!(false),
        6 => json!(-7),
        7 => json!(1),
        8 => json!(1.0),
        9 => json!(1.5),
        10 => json!(u64::MAX),
        11 => json!(9_007_199_254_740_993_u64),
        12 => json!(-0.0),
        13 => Value::Null,
        14 => json!({
            "nested": 1,
        }),
        _ => return None,
    })
}

struct Row {
    event: NormalizedEvent,
    oracle: Option<BTreeMap<String, Scalar>>,
}

fn row(index: usize, seed: [u8; 5], key: &str, guaranteed: Option<(u8, bool)>) -> Row {
    let [value, flag, root, time, fields] = seed;
    let (value, flag) = guaranteed.unwrap_or((value, flag % 2 == 0));
    let attributes = if guaranteed.is_some() || root % 5 < 3 {
        let mut map = serde_json::Map::new();
        if let Some(value) = stored(value) {
            map.insert(key.into(), value);
        }
        map.insert("flag".into(), json!(flag));
        Value::Object(map)
    } else if root % 5 == 3 {
        // A literal numeric key must never address this array's element zero.
        json!([stored(value).unwrap_or(Value::Null)])
    } else {
        json!("scalar-root")
    };
    let oracle = attributes.as_object().map(|_| {
        let mut map = BTreeMap::from([("flag".into(), Scalar::Boolean(flag))]);
        if let Some(value) = scalar(value) {
            map.insert(key.into(), value);
        }
        map
    });
    Row {
        event: NormalizedEvent {
            event_time: if guaranteed.is_some() {
                20 + index as i64 % 3
            } else {
                i64::from(time % 45)
            },
            level: if fields % 3 == 0 { 20 } else { 50 },
            service: (fields % 4 != 0).then(|| format!("service-{}", fields % 2)),
            logger: Some(format!("logger-{}", fields % 3)),
            host: (fields % 5 != 0).then(|| format!("host-{}", fields % 2)),
            pid: None,
            message: format!("fixture-{index}"),
            attributes,
        },
        oracle,
    }
}

fn facet(values: impl Iterator<Item = Option<String>>) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for value in values.flatten() {
        *counts.entry(value).or_default() += 1;
    }
    counts
}

fn actual_facet(values: Vec<FacetValue>) -> BTreeMap<String, u64> {
    values
        .into_iter()
        .map(|value| (value.value, value.count))
        .collect()
}

async fn verify(
    db: &Storage,
    rows: &[Row],
    query: &Query,
    expected_conditions: &[(String, Scalar)],
) {
    let mut expected: Vec<_> = rows
        .iter()
        .enumerate()
        .filter(|(_, row)| {
            row.event.event_time >= query.from
                && row.event.event_time < query.to
                && query.min_level.is_none_or(|level| row.event.level >= level)
                && expected_conditions.iter().all(|(key, value)| {
                    row.oracle.as_ref().and_then(|map| map.get(key)) == Some(value)
                })
        })
        .collect();
    expected.sort_by_key(|(index, row)| std::cmp::Reverse((row.event.event_time, index + 1)));

    let expected_ids: Vec<_> = expected
        .iter()
        .map(|(index, _)| (index + 1).to_string())
        .collect();

    let mut actual_ids = Vec::new();
    let mut page_query = query.clone();

    // Reordered conditions, redundant conditions and numeric spellings must
    // still resume. Construct that second selection independently from the wire.
    let mut equivalent = serde_json::to_value(&query.attributes).unwrap();
    let conditions = equivalent["attributes"].as_array_mut().unwrap();
    conditions.reverse();
    if let Some(first) = conditions.first().cloned() {
        conditions.push(first);
    }

    let equivalent = AttributeFilters::parse(&equivalent.to_string(), 1024).unwrap();
    let mut terminated = false;
    for _ in 0..=rows.len() {
        let page = db.search(page_query.clone()).await.unwrap();
        actual_ids.extend(page.events.into_iter().map(|event| event.id));
        if page.next_cursor.is_none() {
            terminated = true;
            break;
        }

        page_query.cursor = page.next_cursor;
        page_query.attributes = equivalent.clone();
    }

    assert!(terminated, "pagination did not terminate");
    assert_eq!(actual_ids, expected_ids);

    assert_eq!(
        db.count(query.clone()).await.unwrap(),
        expected.len() as u64
    );

    let mut expected_histogram = BTreeMap::new();
    for (_, row) in &expected {
        let bucket = (row.event.event_time - query.from) / 7 * 7 + query.from;
        *expected_histogram.entry(bucket).or_insert(0_u64) += 1;
    }

    let actual_histogram: BTreeMap<_, _> = db
        .histogram(query.clone(), 7)
        .await
        .unwrap()
        .into_iter()
        .map(|bucket| (bucket.time, bucket.count))
        .collect();
    assert_eq!(actual_histogram, expected_histogram);

    let facets = db.facets(query.clone()).await.unwrap();
    assert_eq!(
        actual_facet(facets.services),
        facet(expected.iter().map(|(_, row)| row.event.service.clone()))
    );
    assert_eq!(
        actual_facet(facets.loggers),
        facet(expected.iter().map(|(_, row)| row.event.logger.clone()))
    );
    assert_eq!(
        actual_facet(facets.hosts),
        facet(expected.iter().map(|(_, row)| row.event.host.clone()))
    );
    assert_eq!(
        actual_facet(facets.levels),
        facet(
            expected
                .iter()
                .map(|(_, row)| Some(row.event.level.to_string()))
        )
    );
    assert_eq!(
        actual_facet(facets.sources),
        if expected.is_empty() {
            BTreeMap::new()
        } else {
            BTreeMap::from([("fixture".into(), expected.len() as u64)])
        }
    );
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 20,
        max_shrink_iters: 0,
        ..ProptestConfig::default() })]
    #[test]
    fn generated_attribute_selection_matches_independent_oracle(
        seeds in prop::collection::vec(any::<[u8; 5]>(), 8..25),
        key_index in 0_usize..8,
        selected in 0_u8..13,
        multiple in any::<bool>(),
        filtered in any::<bool>(),
        lower in 0_i64..16,
        upper in 24_i64..46,
        page_size in 1_usize..6,
        minimum in any::<bool>(),
    ) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let key = ["0", "run", "a.b", "a/~b", "*", "quote\"\\", "\u{0}\n", "雪"][key_index];
            let mut rows: Vec<_> = seeds
                .into_iter()
                .enumerate()
                .map(|(index, seed)| row(index, seed, key, None))
                .collect();

            for _ in 0..4 {
                let index = rows.len();
                rows.push(row(
                    index,
                    [selected, 0, 0, 20, 1],
                    key,
                    Some((selected, true)),
                ));
            }

            let mut wire = Vec::new();
            let mut conditions = Vec::new();
            if filtered {
                wire.push(json!({
                    "key": key,
                    "op": "eq",
                    "value": stored(selected).unwrap(),
                }));
                conditions.push((key.into(), scalar(selected).unwrap()));
                if multiple {
                    wire.push(json!({
                        "key": "flag",
                        "op": "eq",
                        "value": true,
                    }));
                    conditions.push(("flag".into(), Scalar::Boolean(true)));
                }
            }

            let query = Query {
                from: lower,
                to: upper,
                limit: page_size,
                min_level: minimum.then_some(30),
                attributes: AttributeFilters::parse(&json!({
                    "attributes": wire,
                }).to_string(), 1024)
                    .unwrap(),
                ..Query::default()
            };

            let directory = tempfile::tempdir().unwrap();
            let db = Storage::open(StorageConfig {
                data_dir: directory.path().to_owned(),
                reader_threads: 1,
                duckdb_threads: 1,
                memory_limit: "128MB".into(),
                query_timeout_ms: 10000,
                ..StorageConfig::default()
            })
            .unwrap();
            db.append(
                "fixture".into(),
                rows.iter().map(|row| row.event.clone()).collect(),
            )
            .await
            .unwrap();

            verify(&db, &rows, &query, &conditions).await;
            db.archive(23).await.unwrap();
            verify(&db, &rows, &query, &conditions).await;

            db.archive(100).await.unwrap();
            verify(&db, &rows, &query, &conditions).await;

            db.shutdown().await.unwrap();
        });
}
}
