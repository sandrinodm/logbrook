//! Socket tests use independently specified MessagePack frames and verify storage after ACK.
use logbrook::{
    config::Config,
    forward::{
        Receiver,
        config::{ForwardConfig, ListenerConfig, PrincipalConfig, RouteConfig},
        decode::{decode_value, encode_value},
        protocol::{CodecLimits, Value},
    },
    indexes::IndexRegistry,
    model::Query,
};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::watch,
};

fn config(root: &std::path::Path) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = root.to_owned();
    config
        .ingest_tokens
        .insert("synthetic-forward-ingest-token".into(), "http".into());
    config
        .read_tokens
        .insert("synthetic-forward-reader-token".into(), vec![]);
    config.forward.enabled = true;
    config.forward.listeners.push(ListenerConfig {
        name: "local".into(),
        bind: Some("127.0.0.1:0".parse().unwrap()),
        principal: Some("docker".into()),
        ..Default::default()
    });
    config.forward.principals.insert(
        "docker".into(),
        PrincipalConfig {
            source: "trusted".into(),
            indexes: vec!["default".into()],
        },
    );
    config.forward.routes.push(RouteConfig {
        listener: "local".into(),
        tag_prefix: "app.".into(),
        index: "default".into(),
        ..Default::default()
    });
    config
}

fn envelope(tag: &str, message: &str, chunk: &str) -> Vec<u8> {
    encode_value(&Value::Array(vec![
        Value::string(tag),
        Value::Integer(0),
        Value::Map(vec![
            (Value::string("message"), Value::string(message)),
            (Value::string("source"), Value::string("forged")),
        ]),
        Value::Map(vec![(Value::string("chunk"), Value::string(chunk))]),
    ]))
}

async fn ack<T: tokio::io::AsyncRead + Unpin>(stream: &mut T) -> Value {
    let mut data = Vec::new();
    loop {
        let mut byte = [0];
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut byte))
            .await
            .unwrap()
            .unwrap();
        data.push(byte[0]);
        if let Ok(value) = decode_value(&data, &CodecLimits::default()) {
            return value;
        }
    }
}

fn all() -> Query {
    Query {
        from: 0,
        to: i64::MAX,
        limit: 100,
        ..Default::default()
    }
}

#[tokio::test]
async fn split_coalesced_envelopes_ack_only_durable_events_and_shutdown_releases_partial_frames() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let registry = IndexRegistry::open(config.clone()).await.unwrap();
    let receiver = Receiver::bind(config.forward, registry.clone())
        .await
        .unwrap();
    let address = receiver.local_addresses().unwrap()[0];
    let (stop, stopped) = watch::channel(false);
    let server = tokio::spawn(receiver.serve(stopped));
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let first = envelope("app.api", "one", "chunk-one");
    for byte in first {
        stream.write_all(&[byte]).await.unwrap();
    }

    assert_eq!(
        ack(&mut stream).await,
        Value::Map(vec![(Value::string("ack"), Value::string("chunk-one"))])
    );

    let storage = &registry.get("default").unwrap().storage;
    assert_eq!(storage.count(all()).await.unwrap(), 1);
    let mut coalesced = envelope("app.api", "two", "chunk-two");
    coalesced.extend(envelope("app.api", "three", "chunk-three"));
    stream.write_all(&coalesced).await.unwrap();
    ack(&mut stream).await;
    ack(&mut stream).await;

    let events = storage.search(all()).await.unwrap().events;
    assert_eq!(events.len(), 3);
    assert!(events.iter().all(|event| event.source == "trusted"));

    stream.write_all(&[0x93, 0xa4, b'a']).await.unwrap();

    registry.stop_admission();
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn denied_route_and_invalid_last_record_leave_no_partial_events_or_ack() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let registry = IndexRegistry::open(config.clone()).await.unwrap();
    let receiver = Receiver::bind(config.forward, registry.clone())
        .await
        .unwrap();
    let address = receiver.local_addresses().unwrap()[0];
    let (stop, stopped) = watch::channel(false);
    let server = tokio::spawn(receiver.serve(stopped));

    for frame in [
        envelope("denied.api", "one", "bad-route"),
        encode_value(&Value::Array(vec![
            Value::string("app.api"),
            Value::Array(vec![
                Value::Array(vec![
                    Value::Integer(0),
                    Value::Map(vec![(
                        Value::string("message"),
                        Value::string("valid-prefix"),
                    )]),
                ]),
                Value::Array(vec![Value::Integer(0), Value::Boolean(false)]),
            ]),
            Value::Map(vec![(Value::string("chunk"), Value::string("bad-last"))]),
        ])),
    ] {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(&frame).await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(response.is_empty());
    }

    assert_eq!(
        registry
            .get("default")
            .unwrap()
            .storage
            .count(all())
            .await
            .unwrap(),
        0
    );

    registry.stop_admission();
    stop.send(true).unwrap();
    server.await.unwrap().unwrap();
    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn heartbeat_is_exact_small_and_stops_on_drain() {
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    config.forward.listeners[0].udp_heartbeat = true;
    let registry = IndexRegistry::open(config.clone()).await.unwrap();
    let receiver = Receiver::bind(config.forward, registry.clone())
        .await
        .unwrap();
    let address = receiver.local_addresses().unwrap()[0];
    let (stop, stopped) = watch::channel(false);
    let server = tokio::spawn(receiver.serve(stopped));

    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    socket.send_to(&[0], address).await.unwrap();
    let mut response = [0; 8];
    let (count, _) = tokio::time::timeout(Duration::from_secs(1), socket.recv_from(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(response[0], 0);

    socket.send_to(b"oversized", address).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), socket.recv_from(&mut response))
            .await
            .is_err()
    );

    registry.stop_admission();
    stop.send(true).unwrap();
    server.await.unwrap().unwrap();
    registry.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn unix_permissions_cleanup_and_existing_path_protection() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    let socket_path = root.path().join("receiver.sock");
    config.forward.listeners[0].bind = None;
    config.forward.listeners[0].unix_path = Some(socket_path.clone());
    config.forward.listeners[0].unix_mode = Some(0o600);
    let registry = IndexRegistry::open(config.clone()).await.unwrap();

    std::fs::write(&socket_path, b"must survive").unwrap();
    assert!(
        Receiver::bind(config.forward.clone(), registry.clone())
            .await
            .is_err()
    );

    assert_eq!(std::fs::read(&socket_path).unwrap(), b"must survive");
    std::fs::remove_file(&socket_path).unwrap();

    let receiver = Receiver::bind(config.forward, registry.clone())
        .await
        .unwrap();
    assert_eq!(
        std::fs::metadata(&socket_path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    let (stop, stopped) = watch::channel(false);
    let server = tokio::spawn(receiver.serve(stopped));
    let mut stream = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
    stream
        .write_all(&envelope("app.unix", "unix", "unix-chunk"))
        .await
        .unwrap();
    ack(&mut stream).await;

    registry.stop_admission();
    stop.send(true).unwrap();
    server.await.unwrap().unwrap();
    assert!(!socket_path.exists());
    registry.shutdown().await.unwrap();
}

#[test]
fn configuration_rejects_ambiguous_routes_implicit_remote_trust_and_unknown_fields() {
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    config.validate().unwrap();
    config.forward.routes.push(config.forward.routes[0].clone());
    assert!(config.validate().is_err());
    config.forward.routes.pop();
    config.forward.listeners[0].bind = Some("0.0.0.0:24224".parse().unwrap());
    assert!(config.validate().is_err());
    assert!(toml::from_str::<ForwardConfig>("enabld=true").is_err());
    assert!(ListenerConfig::default().keepalive);
}

fn map_field<'a>(value: &'a Value, name: &str) -> &'a Value {
    let Value::Map(fields) = value else {
        panic!("expected map")
    };
    &fields
        .iter()
        .find(|(key, _)| key.as_str() == Some(name))
        .unwrap()
        .1
}

fn bytes(value: &Value) -> &[u8] {
    match value {
        Value::Binary(bytes) | Value::String(bytes) => bytes,
        _ => panic!("expected bytes"),
    }
}

fn sha512(parts: &[&[u8]]) -> String {
    use sha2::{Digest, Sha512};
    let mut hash = Sha512::new();
    for part in parts {
        hash.update(part);
    }
    hash.finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[tokio::test]
async fn secure_forward_requires_handshake_maps_user_principal_and_rejects_bad_digest() {
    use logbrook::forward::config::{SecretRef, SecurityMode, UserConfig};
    let root = tempfile::tempdir().unwrap();
    let key = root.path().join("shared.key");
    let password = root.path().join("password");
    std::fs::write(&key, b"independent-shared-secret").unwrap();
    std::fs::write(&password, b"independent-password").unwrap();

    let mut config = config(root.path());
    let listener = &mut config.forward.listeners[0];
    listener.security = SecurityMode::SharedKey;
    listener.principal = None;
    listener.server_hostname = Some("receiver.example".into());
    listener.shared_key = Some(SecretRef {
        file: Some(key),
        env: None,
    });
    listener.users.insert(
        "alice".into(),
        UserConfig {
            principal: "docker".into(),
            password: SecretRef {
                file: Some(password),
                env: None,
            },
        },
    );

    let registry = IndexRegistry::open(config.clone()).await.unwrap();
    let receiver = Receiver::bind(config.forward, registry.clone())
        .await
        .unwrap();
    let address = receiver.local_addresses().unwrap()[0];
    let (stop, stopped) = watch::channel(false);
    let server = tokio::spawn(receiver.serve(stopped));

    for valid in [false, true] {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let helo = ack(&mut stream).await;
        let Value::Array(helo) = helo else {
            panic!("HELO array")
        };
        assert_eq!(helo[0].as_str(), Some("HELO"));

        let nonce = bytes(map_field(&helo[1], "nonce"));
        let auth = bytes(map_field(&helo[1], "auth"));
        let salt = b"sender-salt";
        let ping = Value::Array(vec![
            Value::string("PING"),
            Value::string("sender.example"),
            Value::Binary(salt.to_vec()),
            Value::string(if valid {
                sha512(&[salt, b"sender.example", nonce, b"independent-shared-secret"])
            } else {
                "wrong".into()
            }),
            Value::string("alice"),
            Value::string(sha512(&[auth, b"alice", b"independent-password"])),
        ]);
        stream.write_all(&encode_value(&ping)).await.unwrap();

        let Value::Array(pong) = ack(&mut stream).await else {
            panic!("PONG array")
        };
        assert_eq!(pong[0].as_str(), Some("PONG"));
        assert_eq!(pong[1], Value::Boolean(valid));
        if valid {
            assert_eq!(
                pong[4].as_str(),
                Some(
                    sha512(&[
                        salt,
                        b"receiver.example",
                        nonce,
                        b"independent-shared-secret"
                    ])
                    .as_str()
                )
            );

            stream
                .write_all(&envelope("app.secure", "secure", "secure-chunk"))
                .await
                .unwrap();
            ack(&mut stream).await;
        } else {
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
        }
    }

    assert_eq!(
        registry
            .get("default")
            .unwrap()
            .storage
            .count(all())
            .await
            .unwrap(),
        1
    );

    registry.stop_admission();
    stop.send(true).unwrap();
    server.await.unwrap().unwrap();
    registry.shutdown().await.unwrap();
}

fn fixture(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/forward/tls")
        .join(name)
}

fn certs(name: &str) -> Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>> {
    rustls_pemfile::certs(&mut std::io::BufReader::new(
        std::fs::File::open(fixture(name)).unwrap(),
    ))
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

fn tls_client(identity: bool) -> tokio_rustls::TlsConnector {
    use std::sync::Arc;
    use tokio_rustls::rustls::{self, RootCertStore};
    let mut roots = RootCertStore::empty();
    for cert in certs("ca.pem") {
        roots.add(cert).unwrap();
    }

    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots);
    let client = if identity {
        let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
            std::fs::File::open(fixture("client.key")).unwrap(),
        ))
        .unwrap()
        .unwrap();
        builder
            .with_client_auth_cert(certs("client.pem"), key)
            .unwrap()
    } else {
        builder.with_no_client_auth()
    };
    tokio_rustls::TlsConnector::from(Arc::new(client))
}

async fn tls_clean_close(keepalive: bool) {
    use logbrook::forward::config::TlsConfig;
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    config.forward.listeners[0].keepalive = keepalive;
    config.forward.listeners[0].tls = Some(TlsConfig {
        certificate: fixture("server.pem"),
        private_key: fixture("server.key"),
        client_ca: None,
        client_fingerprints: Default::default(),
    });

    let registry = IndexRegistry::open(config.clone()).await.unwrap();
    let receiver = Receiver::bind(config.forward, registry.clone())
        .await
        .unwrap();
    let address = receiver.local_addresses().unwrap()[0];
    let (stop, stopped) = watch::channel(false);
    let server = tokio::spawn(receiver.serve(stopped));
    let stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut tls = tls_client(false)
        .connect("localhost".try_into().unwrap(), stream)
        .await
        .unwrap();
    tls.write_all(&envelope(
        "app.tls-close",
        "committed-before-close",
        "close-chunk",
    ))
    .await
    .unwrap();
    tls.flush().await.unwrap();

    assert_eq!(
        ack(&mut tls).await,
        Value::Map(vec![(Value::string("ack"), Value::string("close-chunk"))])
    );
    assert_eq!(
        registry
            .get("default")
            .unwrap()
            .storage
            .count(all())
            .await
            .unwrap(),
        1
    );

    if keepalive {
        registry.stop_admission();
        stop.send(true).unwrap();
    }

    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), tls.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0,
        "normal TLS closure must deliver close_notify, rather than UnexpectedEof"
    );

    registry.stop_admission();
    stop.send_replace(true);
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn tls_close_after_envelope_sends_close_notify() {
    tls_clean_close(false).await;
}

#[tokio::test]
async fn tls_graceful_drain_sends_close_notify_after_committed_ack() {
    tls_clean_close(true).await;
}

#[tokio::test]
async fn mtls_requires_verified_certificate_and_explicit_fingerprint_mapping() {
    use logbrook::forward::config::{SecurityMode, TlsConfig};
    use sha2::{Digest, Sha256};
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    let fingerprint: String = Sha256::digest(certs("client.pem")[0].as_ref())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let listener = &mut config.forward.listeners[0];
    listener.security = SecurityMode::Mtls;
    listener.principal = None;
    listener.tls = Some(TlsConfig {
        certificate: fixture("server.pem"),
        private_key: fixture("server.key"),
        client_ca: Some(fixture("ca.pem")),
        client_fingerprints: std::collections::BTreeMap::from([(fingerprint, "docker".into())]),
    });
    let mut unmapped = listener.clone();
    unmapped.name = "unmapped".into();
    unmapped.tls.as_mut().unwrap().client_fingerprints =
        std::collections::BTreeMap::from([("0".repeat(64), "docker".into())]);
    config.forward.listeners.push(unmapped);

    let errors_before =
        logbrook::forward::metrics::AUTH_ERRORS.load(std::sync::atomic::Ordering::Relaxed);

    let registry = IndexRegistry::open(config.clone()).await.unwrap();
    let receiver = Receiver::bind(config.forward.clone(), registry.clone())
        .await
        .unwrap();
    let addresses = receiver.local_addresses().unwrap();
    let address = addresses[0];
    let (stop, stopped) = watch::channel(false);
    let server = tokio::spawn(receiver.serve(stopped));
    let socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut tls = tls_client(true)
        .connect("localhost".try_into().unwrap(), socket)
        .await
        .unwrap();
    tls.write_all(&envelope("app.mtls", "verified", "mtls-chunk"))
        .await
        .unwrap();
    ack(&mut tls).await;

    assert_eq!(
        registry
            .get("default")
            .unwrap()
            .storage
            .count(all())
            .await
            .unwrap(),
        1
    );

    let socket = tokio::net::TcpStream::connect(address).await.unwrap();
    if let Ok(mut tls) = tls_client(false)
        .connect("localhost".try_into().unwrap(), socket)
        .await
    {
        let _ = tls
            .write_all(&envelope("app.mtls", "denied", "denied-chunk"))
            .await;
        let mut response = [0];
        let result = tokio::time::timeout(Duration::from_secs(1), tls.read(&mut response))
            .await
            .unwrap();
        assert!(matches!(result, Ok(0) | Err(_)));
    }

    let socket = tokio::net::TcpStream::connect(addresses[1]).await.unwrap();
    let mut unmapped = tls_client(true)
        .connect("localhost".try_into().unwrap(), socket)
        .await
        .unwrap();
    let _ = unmapped
        .write_all(&envelope("app.mtls", "unmapped", "unmapped"))
        .await;
    let mut response = [0];
    let result = tokio::time::timeout(Duration::from_secs(1), unmapped.read(&mut response))
        .await
        .unwrap();
    assert!(matches!(result, Ok(0) | Err(_)));

    assert!(
        logbrook::forward::metrics::AUTH_ERRORS.load(std::sync::atomic::Ordering::Relaxed)
            >= errors_before + 2,
        "TLS missing identity and unmapped verified identity must both count"
    );

    registry.stop_admission();
    stop.send(true).unwrap();
    server.await.unwrap().unwrap();
    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn partial_frame_total_deadline_cannot_be_extended_by_slow_drip() {
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    config.forward.frame_idle_timeout_ms = 60;
    config.forward.frame_total_timeout_ms = 120;

    let registry = IndexRegistry::open(config.clone()).await.unwrap();
    let receiver = Receiver::bind(config.forward, registry.clone())
        .await
        .unwrap();
    let address = receiver.local_addresses().unwrap()[0];
    let (stop, stopped) = watch::channel(false);
    let server = tokio::spawn(receiver.serve(stopped));
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(&[0xdb, 0, 0, 4, 0]).await.unwrap();

    let started = tokio::time::Instant::now();
    loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if stream.write_all(b"a").await.is_err() {
            break;
        }
        if started.elapsed() > Duration::from_millis(240) {
            break;
        }
    }

    let mut response = [0];
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(100), stream.read(&mut response))
            .await
            .unwrap(),
        Ok(0) | Err(_)
    ));

    registry.stop_admission();
    stop.send(true).unwrap();
    server.await.unwrap().unwrap();
    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn exact_tag_route_is_authorized_without_accepting_unmatched_suffix() {
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    config.forward.routes[0].tag_prefix.clear();
    config.forward.routes[0].tag = Some("exact".into());

    let registry = IndexRegistry::open(config.clone()).await.unwrap();
    let receiver = Receiver::bind(config.forward, registry.clone())
        .await
        .unwrap();
    let address = receiver.local_addresses().unwrap()[0];
    let (stop, stopped) = watch::channel(false);
    let server = tokio::spawn(receiver.serve(stopped));
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream
        .write_all(&envelope("exact", "one", "exact-chunk"))
        .await
        .unwrap();
    ack(&mut stream).await;

    stream
        .write_all(&envelope("exact.suffix", "denied", "suffix"))
        .await
        .unwrap();
    let mut response = [0];
    assert_eq!(stream.read(&mut response).await.unwrap(), 0);

    assert_eq!(
        registry
            .get("default")
            .unwrap()
            .storage
            .count(all())
            .await
            .unwrap(),
        1
    );

    registry.stop_admission();
    stop.send(true).unwrap();
    server.await.unwrap().unwrap();
    registry.shutdown().await.unwrap();
}
