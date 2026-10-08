//! Keep the documented credential setup executable and fail closed on errors.
#![cfg(unix)]

use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};

fn credential_snippet() -> &'static str {
    include_str!("../../skills/logbrook/references/installation.md")
        .split("```sh\n")
        .skip(1)
        .map(|block| block.split("```").next().unwrap())
        .find(|script| script.contains("cat > .env"))
        .unwrap()
}

fn shell(directory: &Path, setup: &str) -> Command {
    let mut command = Command::new("/bin/bash");
    command
        .current_dir(directory)
        .env_remove("LOGBROOK_IMAGE")
        .args(["-c", &format!("{setup}\n{}", credential_snippet())]);
    command
}

#[test]
fn credentials_are_private_distinct_and_never_overwritten() {
    let directory = tempfile::tempdir().unwrap();
    let setup = "export LOGBROOK_IMAGE=logbrook:local";
    assert!(
        shell(directory.path(), setup)
            .output()
            .unwrap()
            .status
            .success()
    );
    let path = directory.path().join(".env");
    let original = fs::read_to_string(&path).unwrap();
    let tokens: Vec<_> = original
        .lines()
        .filter(|line| {
            line.starts_with("LOGBROOK_INGEST_TOKEN=") || line.starts_with("LOGBROOK_READ_TOKEN=")
        })
        .map(|line| line.split_once('=').unwrap().1)
        .collect();
    assert_eq!(tokens.len(), 2);
    assert_ne!(tokens[0], tokens[1]);
    assert!(
        tokens
            .iter()
            .all(|token| token.len() == 64 && token.bytes().all(|byte| byte.is_ascii_hexdigit()))
    );
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );

    assert!(
        !shell(directory.path(), setup)
            .output()
            .unwrap()
            .status
            .success()
    );
    assert_eq!(fs::read_to_string(path).unwrap(), original);
}

#[test]
fn missing_image_or_failed_token_generation_leaves_no_configuration_file() {
    for setup in [
        "",
        "export LOGBROOK_IMAGE=logbrook:local; openssl() { return 1; }",
        "export LOGBROOK_IMAGE=logbrook:local; openssl() { return 0; }",
        "export LOGBROOK_IMAGE=logbrook:local; PATH=/nonexistent",
    ] {
        let directory = tempfile::tempdir().unwrap();
        assert!(
            !shell(directory.path(), setup)
                .output()
                .unwrap()
                .status
                .success(),
            "accepted setup: {setup}"
        );
        assert!(!directory.path().join(".env").exists());
    }
}
