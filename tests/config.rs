use logbrook::config::Config;
use std::process::Command;

fn valid() -> Config {
    let mut config = Config::default();
    config
        .ingest_tokens
        .insert("synthetic-config-write-token".into(), "app".into());
    config
        .read_tokens
        .insert("synthetic-config-read-token".into(), vec!["app".into()]);
    config
}

#[test]
fn every_credential_role_rejects_non_ascii_and_control_characters() {
    let invalid = [
        "é".repeat(8),
        format!("synthetic-token-{}", '\0'),
        format!("synthetic-token-{}", '\u{7f}'),
        format!("synthetic-token-{}", '\u{1b}'),
        "synthetic-token with-space".into(),
        "synthetic-token\twith-tab".into(),
        "synthetic-token\nwith-newline".into(),
    ];

    for token in invalid {
        for role in ["ingest", "read", "admin"] {
            let mut config = valid();
            match role {
                "ingest" => {
                    config.ingest_tokens.insert(token.clone(), "app".into());
                }
                "read" => {
                    config.read_tokens.insert(token.clone(), vec![]);
                }
                "admin" => config.admin_tokens.push(token.clone()),
                _ => unreachable!(),
            }

            let error = config.validate().unwrap_err();
            assert!(error.message.contains("visible ASCII"), "{role}: {error}");
            assert!(!error.message.contains(&token));
        }
    }

    // Both inclusive length boundaries and visible punctuation remain usable.
    let mut config = valid();
    config.admin_tokens = vec!["!".repeat(16), "~".repeat(512)];
    config.validate().unwrap();
}

#[test]
fn malformed_configuration_never_prints_credential_keys_or_values() {
    let secret = "synthetic-secret-config-token";
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.toml");
    let cases = [
        (format!("[ingest_tokens]\n\"{secret}\" = 123\n"), false),
        (format!("[read_tokens]\n\"{secret}\" = false\n"), false),
        (format!("{secret} = true\n"), false),
        (format!("admin_tokens = \"{secret}\"\n"), false),
        (format!("bind = \"{secret}\"\n"), true),
        (format!("admin_tokens = [\"{secret}\"\n"), true),
    ];

    for (text, healthcheck_fails) in cases {
        std::fs::write(&path, &text).unwrap();

        for command in ["check-config", "serve", "healthcheck"] {
            // Healthcheck intentionally ignores otherwise well-formed token settings.
            if command == "healthcheck" && !healthcheck_fails {
                continue;
            }

            let mut process = Command::new(env!("CARGO_BIN_EXE_logbrook"));
            for (key, _) in std::env::vars().filter(|(key, _)| key.starts_with("LOGBROOK_")) {
                process.env_remove(key);
            }
            let output = process
                .args(["--config"])
                .arg(&path)
                .arg(command)
                .output()
                .unwrap();
            let stderr = String::from_utf8(output.stderr).unwrap();

            assert!(
                !output.status.success(),
                "{command} accepted invalid config"
            );
            assert!(output.stdout.is_empty());
            assert!(stderr.contains("invalid TOML config at line"), "{stderr}");
            assert!(stderr.contains("column"), "{stderr}");
            assert!(!stderr.contains(secret), "{command} leaked a credential");
        }
    }
}

#[test]
fn admin_tokens_are_optional_validated_and_separate_from_other_roles() {
    let mut config = valid();

    assert!(config.admin_tokens.is_empty());
    config.validate().unwrap();
    config.admin_tokens = vec!["synthetic-config-admin-token".into()];
    config.validate().unwrap();
    for token in [
        "",
        "short",
        "synthetic admin token",
        "replace-with-random-admin-token",
        "synthetic-config-read-token",
        "synthetic-config-write-token",
    ] {
        config.admin_tokens = vec![token.into()];

        assert!(config.validate().is_err(), "{token:?}");
    }

    config.admin_tokens = vec!["a".repeat(513)];

    assert!(config.validate().is_err());

    let serialized = toml::to_string(&valid()).unwrap();
    let without_admin = serialized
        .lines()
        .filter(|line| !line.starts_with("admin_tokens ="))
        .collect::<Vec<_>>()
        .join("\n");
    let parsed: Config = toml::from_str(&without_admin).unwrap();

    assert!(parsed.admin_tokens.is_empty());
}

#[test]
fn admin_environment_replaces_configured_tokens_and_rejects_empty() {
    let mut config = valid();
    config.admin_tokens = vec!["replace-with-random-admin-token".into()];

    assert!(!check_configuration(&config, &[]).status.success());
    assert!(
        check_configuration(
            &config,
            &[("LOGBROOK_ADMIN_TOKEN", "synthetic-environment-admin-token")]
        )
        .status
        .success()
    );

    for token in [
        "",
        "synthetic-config-read-token",
        "synthetic-config-write-token",
    ] {
        assert!(
            !check_configuration(&config, &[("LOGBROOK_ADMIN_TOKEN", token)])
                .status
                .success()
        );
    }
}

#[test]
fn rejects_example_credentials_and_unsafe_resource_limits() {
    let mut config = valid();
    config.validate().unwrap();
    config
        .read_tokens
        .insert("replace-with-random-reader-token".into(), vec![]);

    assert!(config.validate().unwrap_err().message.contains("example"));

    let mut config = valid();
    config.max_tail_buffer_bytes = config.max_event_bytes;

    assert!(config.validate().is_err());

    let mut config = valid();
    config.storage.data_dir = "data[1]".into();

    assert!(config.validate().unwrap_err().message.contains("glob"));
}

#[test]
fn environment_reader_requires_explicit_scope_when_configuration_has_readers() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("config.toml");
    std::fs::write(&file, toml::to_string(&valid()).unwrap()).unwrap();

    let command = || {
        let mut command = Command::new(env!("CARGO_BIN_EXE_logbrook"));
        for (key, _) in std::env::vars().filter(|(key, _)| key.starts_with("LOGBROOK_")) {
            command.env_remove(key);
        }

        command
            .args(["--config", file.to_str().unwrap(), "check-config"])
            .env("LOGBROOK_READ_TOKEN", "synthetic-environment-read-token");
        command
    };

    let rejected = command().output().unwrap();

    assert!(!rejected.status.success());
    assert!(rejected.stdout.is_empty());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("LOGBROOK_READ_SOURCES"));

    let accepted = command()
        .env("LOGBROOK_READ_SOURCES", "app")
        .output()
        .unwrap();

    assert!(
        accepted.status.success(),
        "{}",
        String::from_utf8_lossy(&accepted.stdout)
    );
}

fn check_configuration(config: &Config, overrides: &[(&str, &str)]) -> std::process::Output {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("config.toml");
    let mut config = config.clone();
    config.storage.data_dir = directory.path().join("data");
    std::fs::write(&file, toml::to_string(&config).unwrap()).unwrap();

    let mut command = Command::new(env!("CARGO_BIN_EXE_logbrook"));
    for (key, _) in std::env::vars().filter(|(key, _)| key.starts_with("LOGBROOK_")) {
        command.env_remove(key);
    }

    command.args(["--config", file.to_str().unwrap(), "check-config"]);
    for (name, value) in overrides {
        command.env(name, value);
    }

    let result = command.output().unwrap();

    assert!(
        !config.storage.data_dir.exists(),
        "configuration validation must not open storage"
    );
    result
}

#[test]
fn retention_environment_overrides_toml_with_thirty_day_window() {
    let day = 86_400_000;
    let mut config = valid();
    config.archive_after_ms = 14 * day;

    assert!(!check_configuration(&config, &[]).status.success());
    assert!(
        check_configuration(&config, &[("LOGBROOK_RETENTION_MS", "2592000000")])
            .status
            .success()
    );
    config.retention_ms = 30 * day;
    config.archive_after_ms = 29 * day;

    assert!(check_configuration(&config, &[]).status.success());
    config.archive_after_ms = config.retention_ms;

    assert!(
        check_configuration(&config, &[("LOGBROOK_ARCHIVE_AFTER_MS", "86400000")])
            .status
            .success()
    );
    config.archive_after_ms = day;
    config.maintenance_interval_secs = 0;

    assert!(
        check_configuration(&config, &[("LOGBROOK_MAINTENANCE_INTERVAL_SECS", "60")])
            .status
            .success()
    );
}

#[test]
fn retention_environment_rejects_invalid_numbers_and_cross_field_windows() {
    let config = valid();
    for name in [
        "LOGBROOK_RETENTION_MS",
        "LOGBROOK_ARCHIVE_AFTER_MS",
        "LOGBROOK_MAINTENANCE_INTERVAL_SECS",
    ] {
        for value in ["", "abc", "1.5", "0", "-1", "18446744073709551616"] {
            let result = check_configuration(&config, &[(name, value)]);
            let output = format!(
                "{}{}",
                String::from_utf8_lossy(&result.stdout),
                String::from_utf8_lossy(&result.stderr)
            );

            assert!(!result.status.success(), "{name}={value} should fail");
            assert!(output.contains(name), "{output}");
            assert!(
                output.contains("positive integer") || output.contains("greater than zero"),
                "{output}"
            );
        }
    }

    for (name, value) in [
        ("LOGBROOK_RETENTION_MS", "86400000"),
        ("LOGBROOK_RETENTION_MS", "1000"),
        ("LOGBROOK_ARCHIVE_AFTER_MS", "604800000"),
    ] {
        let result = check_configuration(&config, &[(name, value)]);

        assert!(!result.status.success());
        assert!(result.stdout.is_empty());
        assert!(
            String::from_utf8_lossy(&result.stderr)
                .contains("retention_ms must be greater than archive_after_ms")
        );
    }
}

#[test]
fn retention_toml_zero_and_negative_values_have_specific_errors() {
    let mut config = valid();
    config.retention_ms = 0;

    assert_eq!(
        config.validate().unwrap_err().message,
        "retention_ms must be positive"
    );
    config.retention_ms = -1;

    assert_eq!(
        config.validate().unwrap_err().message,
        "retention_ms must be positive"
    );
    config.retention_ms = 604800000;
    config.archive_after_ms = 0;

    assert_eq!(
        config.validate().unwrap_err().message,
        "archive_after_ms must be positive"
    );
    config.archive_after_ms = 86400000;
    config.maintenance_interval_secs = 0;

    assert_eq!(
        config.validate().unwrap_err().message,
        "maintenance_interval_secs must be positive"
    );
}

#[test]
fn days_size_and_index_overrides_resolve_with_bounded_resources() {
    use logbrook::config::IndexSettings;

    let mut config = valid();
    config.retention_days = Some(30);
    config.max_size_gb = Some(20);
    config.indexes.insert(
        "payments".into(),
        IndexSettings {
            retention_days: Some(90),
            max_size_gb: Some(5),
            ..Default::default()
        },
    );
    config.validate().unwrap();
    let default = config.for_index("default").unwrap();
    let payments = config.for_index("payments").unwrap();

    assert_eq!(default.retention_ms, 30 * 86_400_000);
    assert_eq!(default.storage.max_size_bytes, Some(20_000_000_000));
    assert_eq!(payments.retention_ms, 90 * 86_400_000);
    assert_eq!(payments.storage.max_size_bytes, Some(5_000_000_000));
    assert_eq!(
        payments.storage.data_dir,
        config.storage.data_dir.join("indexes/payments")
    );
    assert_eq!(payments.storage.memory_limit, "128000KB");
    assert_eq!(payments.storage.temp_limit, "500000KB");
    assert_eq!(payments.storage.queue_bytes, 8 * 1024 * 1024);
}

#[test]
fn invalid_index_policies_and_scope_names_are_rejected() {
    use logbrook::config::{IndexSettings, validate_index_name};

    for name in [
        "",
        "../escape",
        "/absolute",
        "Default",
        "a.b",
        "-name",
        "foo%2fbar",
        "é",
    ] {
        assert!(validate_index_name(name).is_err(), "{name}");
    }

    for name in ["default", "payments", "a-b_2", "9"] {
        validate_index_name(name).unwrap();
    }

    assert!(validate_index_name(&"a".repeat(64)).is_err());

    let mut config = valid();
    config.retention_days = Some(u64::MAX);

    assert!(config.validate().is_err());
    config.retention_days = Some(30);
    config.max_size_gb = Some(u64::MAX);

    assert!(config.validate().is_err());
    config.max_size_gb = None;
    config.storage.max_size_bytes = Some(1024);

    assert!(config.validate().is_err());
    config.storage.max_size_bytes = None;
    config.max_indexes = 1;
    config
        .indexes
        .insert("payments".into(), IndexSettings::default());

    assert!(
        config
            .validate()
            .unwrap_err()
            .message
            .contains("max_indexes")
    );
    config.max_indexes = 4;
    config.indexes.get_mut("payments").unwrap().retention_days = Some(3);
    config.indexes.get_mut("payments").unwrap().retention_ms = Some(123);

    assert!(config.validate().is_err());
    config.indexes.clear();
    config.read_index_scopes.insert(
        "synthetic-config-read-token".into(),
        vec!["../escape".into()],
    );

    assert!(config.validate().is_err());
}

#[test]
fn days_and_size_environment_aliases_validate_and_conflicts_fail() {
    let config = valid();
    let ok = check_configuration(
        &config,
        &[
            ("LOGBROOK_RETENTION_DAYS", "30"),
            ("LOGBROOK_MAX_SIZE_GB", "20"),
            ("LOGBROOK_INDEXES", "payments,audit"),
        ],
    );

    assert!(
        ok.status.success(),
        "{}",
        String::from_utf8_lossy(&ok.stdout)
    );

    for overrides in [
        vec![
            ("LOGBROOK_RETENTION_DAYS", "30"),
            ("LOGBROOK_RETENTION_MS", "2592000000"),
        ],
        vec![
            ("LOGBROOK_MAX_SIZE_GB", "20"),
            ("LOGBROOK_MAX_SIZE_BYTES", "20000000000"),
        ],
        vec![("LOGBROOK_RETENTION_DAYS", "0")],
        vec![("LOGBROOK_MAX_SIZE_GB", "18446744073709551615")],
        vec![("LOGBROOK_INDEXES", "a,b,c,d")],
        vec![("LOGBROOK_INDEXES", "../escape")],
    ] {
        assert!(!check_configuration(&config, &overrides).status.success());
    }

    let mut days = config;
    days.retention_days = Some(30);

    assert!(
        check_configuration(&days, &[("LOGBROOK_RETENTION_MS", "604800000")])
            .status
            .success()
    );
}
