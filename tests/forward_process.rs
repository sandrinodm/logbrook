//! Process-kill and response-loss behavior for committed Forward acknowledgments.

#[allow(dead_code)]
mod support;

use logbrook::{
    config::Config,
    forward::{
        config::{ListenerConfig, PrincipalConfig, RouteConfig},
        decode::{decode_value, encode_value},
        protocol::{CodecLimits, Value},
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::Path,
    time::Duration,
};
use support::Fixture;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn setup() -> (Fixture, SocketAddr) {
    let mut fixture = Fixture::new();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    let mut config = Config::default();
    config.forward.enabled = true;
    config.forward.listeners.push(ListenerConfig {
        name: "process".into(),
        bind: Some(address),
        principal: Some("producer".into()),
        ..Default::default()
    });
    config.forward.principals.insert(
        "producer".into(),
        PrincipalConfig {
            source: "forward-process".into(),
            indexes: vec!["default".into()],
        },
    );
    config.forward.routes.push(RouteConfig {
        listener: "process".into(),
        tag_prefix: "process.".into(),
        index: "default".into(),
        ..Default::default()
    });

    let path = fixture.root.path().join("forward.toml");
    std::fs::write(&path, toml::to_string(&config).unwrap()).unwrap();
    fixture.config = Some(path);
    (fixture, address)
}

fn batch(marker: &str, count: usize, chunk: &str) -> Vec<u8> {
    encode_value(&Value::Array(vec![
        Value::string("process.events"),
        Value::Array(
            (0..count)
                .map(|index| {
                    Value::Array(vec![
                        Value::Integer(0),
                        Value::Map(vec![
                            (
                                Value::string("message"),
                                Value::string(format!("{marker}-{index}")),
                            ),
                            (Value::string("sequence"), Value::Integer(index as i128)),
                        ]),
                    ])
                })
                .collect(),
        ),
        Value::Map(vec![(Value::string("chunk"), Value::string(chunk))]),
    ]))
}

async fn acknowledged(stream: &mut tokio::net::TcpStream, frame: &[u8], chunk: &str) {
    stream.write_all(frame).await.unwrap();
    let mut bytes = Vec::new();
    loop {
        let mut byte = [0];
        tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut byte))
            .await
            .unwrap()
            .unwrap();
        bytes.push(byte[0]);
        if let Ok(value) = decode_value(&bytes, &CodecLimits::default()) {
            assert_eq!(
                value,
                Value::Map(vec![(Value::string("ack"), Value::string(chunk))])
            );
            return;
        }
    }
}

async fn wait_count(fixture: &Fixture, marker: &str, count: u64) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if fixture.count("default", marker).await == count {
                return;
            }

            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let destination = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &destination);
        } else {
            std::fs::copy(entry.path(), destination).unwrap();
        }
    }
}

async fn durable_rows(fixture: &Fixture) -> BTreeMap<String, serde_json::Value> {
    let path = format!(
        "/logs?from={}&to={}&message=durable-process-&limit=1000",
        support::now() - 3600000,
        support::now() + 10000
    );
    let (status, page) = fixture.request(&path, None, support::READ).await;
    assert_eq!(status, 200, "{page}");

    let rows: BTreeMap<_, _> = page["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| (event["id"].as_str().unwrap().to_owned(), event.clone()))
        .collect();
    assert_eq!(rows.len(), 250);

    let expected: BTreeSet<_> = (0..10)
        .flat_map(|group| (0..25).map(move |event| format!("durable-process-{group}-{event}")))
        .collect();
    let actual: BTreeSet<_> = rows
        .values()
        .map(|event| event["message"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(actual, expected);

    assert!(
        rows.values()
            .all(|event| event["source"] == "forward-process")
    );
    rows
}

#[tokio::test]
async fn acknowledged_forward_envelopes_survive_sigkill_archive_and_relocated_restore() {
    let (mut fixture, address) = setup();
    fixture.start().await;
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    for group in 0..10 {
        let chunk = format!("durable-{group}");
        let frame = batch(&format!("durable-process-{group}"), 25, &chunk);
        acknowledged(&mut stream, &frame, &chunk).await;
    }

    fixture.crash();
    fixture.start().await;
    let snapshot = durable_rows(&fixture).await;

    fixture.stop();

    let before = (support::now() + 1000).to_string();
    let answer = fixture.cli(&["archive", "--before", &before], true, false);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&answer).unwrap()["archived"],
        250
    );

    fixture.start().await;
    assert_eq!(durable_rows(&fixture).await, snapshot);
    fixture.stop();

    let restored = fixture.root.path().join("forward-restored");
    copy_tree(&fixture.data, &restored);
    fixture.env.insert(
        "LOGBROOK_DATA_DIR".into(),
        restored.to_str().unwrap().into(),
    );

    fixture.start().await;
    assert_eq!(durable_rows(&fixture).await, snapshot);

    let mut restored_stream = tokio::net::TcpStream::connect(address).await.unwrap();
    acknowledged(
        &mut restored_stream,
        &batch("after-forward-restore", 1, "after-restore"),
        "after-restore",
    )
    .await;

    let path = format!(
        "/logs?from={}&to={}&message=after-forward-restore&limit=1",
        support::now() - 3600000,
        support::now() + 10000
    );
    let (status, page) = fixture.request(&path, None, support::READ).await;
    assert_eq!(status, 200);

    let new_id = page["events"][0]["id"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert!(
        new_id
            > snapshot
                .keys()
                .map(|id| id.parse::<u64>().unwrap())
                .max()
                .unwrap()
    );

    fixture.stop();
}

#[tokio::test]
async fn lost_ack_and_disconnected_sender_retain_commit_and_retry_can_duplicate() {
    let (mut fixture, address) = setup();
    fixture.start().await;
    let frame = batch("lost-forward-ack", 20, "same-chunk");
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(&frame).await.unwrap();

    // Observe the committed rows through HTTP while deliberately leaving the ACK unread.
    wait_count(&fixture, "lost-forward-ack", 20).await;
    drop(stream);

    fixture.crash();
    fixture.start().await;
    assert_eq!(fixture.count("default", "lost-forward-ack").await, 20);

    let mut retry = tokio::net::TcpStream::connect(address).await.unwrap();
    acknowledged(&mut retry, &frame, "same-chunk").await;
    assert_eq!(fixture.count("default", "lost-forward-ack").await, 40);

    let mut disconnected = tokio::net::TcpStream::connect(address).await.unwrap();
    disconnected
        .write_all(&batch("disconnected-forward", 10, "disconnect"))
        .await
        .unwrap();
    drop(disconnected);
    wait_count(&fixture, "disconnected-forward", 10).await;

    fixture.stop();
}

#[tokio::test]
async fn kill_before_observing_ack_never_leaves_a_partial_envelope() {
    let (mut fixture, address) = setup();
    fixture.start().await;
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream
        .write_all(&batch("uncertain-forward-commit", 500, "uncertain"))
        .await
        .unwrap();

    fixture.crash();
    fixture.start().await;

    let committed = fixture.count("default", "uncertain-forward-commit").await;
    assert!(
        [0, 500].contains(&committed),
        "an envelope must be atomic after crash, observed {committed}"
    );

    fixture.stop();
}
