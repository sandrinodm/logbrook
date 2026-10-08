//! Exercise the actual workflow scripts against disposable Git repositories.

use super::{fingerprint, prepare, tests::fixture};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
};

fn run(command: &mut Command) -> Output {
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{command:?}: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn git(root: &Path, args: &[&str]) -> String {
    let output = run(Command::new("git")
        .current_dir(root)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .args(args));
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn repository() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_owned()
}

struct Release {
    work: tempfile::TempDir,
    remote: tempfile::TempDir,
    scratch: tempfile::TempDir,
    base: String,
    version: String,
}

impl Release {
    fn new(request: &str) -> Self {
        let work = fixture();
        let remote = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        git(work.path(), &["init", "--initial-branch=main"]);
        git(work.path(), &["config", "user.name", "Release test"]);
        git(
            work.path(),
            &["config", "user.email", "release@example.invalid"],
        );
        git(work.path(), &["add", "."]);
        git(work.path(), &["commit", "-m", "Initial version"]);
        let base = git(work.path(), &["rev-parse", "HEAD"]);
        git(remote.path(), &["init", "--bare", "--initial-branch=main"]);
        git(
            work.path(),
            &["remote", "add", "origin", remote.path().to_str().unwrap()],
        );
        git(work.path(), &["push", "origin", "main"]);

        let report = prepare(work.path(), request, false).unwrap();
        let version = report["version"].as_str().unwrap().to_owned();
        Self {
            work,
            remote,
            scratch,
            base,
            version,
        }
    }

    fn shell(&self, script: &Path) -> Command {
        let mut command = Command::new("bash");
        command
            .current_dir(self.work.path())
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env_remove("RELEASE_PUSH_TOKEN")
            .env("PREVIOUS_VERSION", "0.1.0")
            .env("RELEASE_VERSION", &self.version)
            .env("RELEASE_TAG", format!("v{}", self.version))
            .env("RELEASE_BASE", &self.base)
            .env("GITHUB_OUTPUT", self.scratch.path().join("output"))
            .arg(script);
        command
    }

    fn validate(&self) -> Output {
        let patch = self.scratch.path().join("release.patch");
        let output = run(Command::new("git").current_dir(self.work.path()).args([
            "diff",
            "--binary",
            "--no-ext-diff",
        ]));
        fs::write(&patch, output.stdout).unwrap();
        git(self.work.path(), &["restore", "--worktree", "."]);
        self.shell(&repository().join(".github/scripts/validate-release-patch.sh"))
            .arg(patch)
            .output()
            .unwrap()
    }

    fn push(&self) -> Output {
        self.shell(&repository().join(".github/scripts/push-release.sh"))
            .output()
            .unwrap()
    }
}

fn assert_success(output: Output) {
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

// Extract a named run step so regression tests exercise the shipped YAML rather
// than a second copy of its shell logic. Inputs in these steps arrive via env.
fn workflow_step(file: &str, name: &str) -> String {
    let text = fs::read_to_string(repository().join(file)).unwrap();
    let step = text.split(&format!("- name: {name}\n")).nth(1).unwrap();
    let body = step.split("        run: |\n").nth(1).unwrap();
    body.lines()
        .take_while(|line| line.is_empty() || line.starts_with("          "))
        .map(|line| line.strip_prefix("          ").unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn publishes_initial_bumped_and_unusual_prerelease_versions_without_hooks() {
    let guard = workflow_step(".github/workflows/release.yml", "Validate release tag");
    for request in ["current", "patch", "0.2.0-rc.1", "0.2.0--x"] {
        let release = Release::new(request);
        assert_success(release.validate());
        run(Command::new("bash")
            .args(["-e", "-c", &guard])
            .env("RELEASE_TAG", format!("v{}", release.version))
            .env("UPDATE_LATEST", "false"));

        let hook = release.work.path().join(".git/hooks/pre-push");
        fs::write(&hook, "#!/bin/sh\ntouch hook-executed\nexit 1\n").unwrap();
        fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).unwrap();
        assert_success(release.push());
        assert!(!release.work.path().join("hook-executed").exists());

        let tag = format!("refs/tags/v{}^{{commit}}", release.version);
        let remote_commit = git(release.remote.path(), &["rev-parse", &tag]);
        assert_eq!(
            remote_commit,
            git(release.remote.path(), &["rev-parse", "main"])
        );
        assert_eq!(remote_commit == release.base, request == "current");
        assert!(
            fs::read_to_string(release.scratch.path().join("output"))
                .unwrap()
                .contains(&remote_commit)
        );
        if request != "current" {
            assert_eq!(
                git(release.remote.path(), &["log", "-1", "--format=%s"]),
                format!("chore(release): v{}", release.version)
            );
        }
    }
}

#[test]
fn rejects_dependency_changes_extra_edits_and_wrong_notice_fingerprints() {
    for fault in ["dependency", "docker", "notice", "extra"] {
        let release = Release::new("patch");
        let root = release.work.path();
        match fault {
            "dependency" => {
                // Keep one changed version line, but move it to a dependency.
                let lock = fs::read_to_string(root.join("Cargo.lock"))
                    .unwrap()
                    .replacen("version = \"0.1.1\"", "version = \"0.1.0\"", 1)
                    .replace(
                        "name = \"dependency\"\nversion = \"0.1.0\"",
                        "name = \"dependency\"\nversion = \"0.1.1\"",
                    );
                fs::write(root.join("Cargo.lock"), &lock).unwrap();
                fs::write(
                    root.join("THIRD_PARTY_NOTICES.txt"),
                    format!("{}\n\nOriginal license text\n", fingerprint(&lock)),
                )
                .unwrap();
            }
            "docker" => {
                fs::write(root.join("Dockerfile"), "FROM example\nRUN echo unwanted\n").unwrap()
            }
            "notice" => fs::write(
                root.join("THIRD_PARTY_NOTICES.txt"),
                "Cargo.lock SHA-256: wrong\n\nOriginal license text\n",
            )
            .unwrap(),
            _ => {
                let file = root.join("Cargo.toml");
                let text = fs::read_to_string(&file).unwrap();
                fs::write(file, format!("{text}unwanted = \"1\"\n")).unwrap();
            }
        }
        let result = release.validate();
        assert!(
            !result.status.success(),
            "accepted {fault}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(
            git(release.remote.path(), &["rev-parse", "main"]),
            release.base
        );
    }
}

#[test]
fn refuses_stale_main_duplicate_tags_and_atomic_push_rejections() {
    for fault in ["stale", "duplicate", "atomic"] {
        let release = Release::new("patch");
        assert_success(release.validate());
        match fault {
            "stale" => {
                let clone = release.scratch.path().join("concurrent");
                git(
                    release.scratch.path(),
                    &[
                        "clone",
                        release.remote.path().to_str().unwrap(),
                        clone.to_str().unwrap(),
                    ],
                );
                git(
                    &clone,
                    &[
                        "-c",
                        "user.name=Other",
                        "-c",
                        "user.email=other@example.invalid",
                        "commit",
                        "--allow-empty",
                        "-m",
                        "Concurrent update",
                    ],
                );
                git(&clone, &["push", "origin", "main"]);
            }
            "duplicate" => {
                git(release.remote.path(), &["tag", "v0.1.1", &release.base]);
            }
            _ => {
                let hook = release.remote.path().join("hooks/pre-receive");
                fs::write(&hook, "#!/bin/sh\nwhile read -r old new ref; do\n  case $ref in refs/tags/*) exit 1 ;; esac\ndone\n").unwrap();
                fs::set_permissions(hook, fs::Permissions::from_mode(0o755)).unwrap();
            }
        }
        let before = git(release.remote.path(), &["show-ref"]);
        assert!(!release.push().status.success(), "accepted {fault}");
        assert_eq!(git(release.remote.path(), &["show-ref"]), before);
    }
}

#[test]
fn github_release_retry_reuses_existing_release_and_marks_prereleases() {
    let scratch = tempfile::tempdir().unwrap();
    let mock = scratch.path().join("gh");
    fs::write(
        &mock,
        r##"#!/bin/sh
set -eu
if [ "$2" = view ]; then
  if [ -f "$RUNNER_TEMP/created" ]; then
    printf '%s\n' '{"isDraft":false,"url":"https://example.invalid/release"}'
    exit 0
  fi
  exit 1
fi
printf '%s\n' "$@" > "$RUNNER_TEMP/arguments"
test ! -f "$RUNNER_TEMP/created"
touch "$RUNNER_TEMP/created"
printf '%s\n' 'https://example.invalid/release'
"##,
    )
    .unwrap();
    fs::set_permissions(mock, fs::Permissions::from_mode(0o755)).unwrap();
    let script = workflow_step(
        ".github/workflows/create-release.yml",
        "Create release with generated notes",
    );
    for _ in 0..2 {
        run(Command::new("bash")
            .args(["-e", "-o", "pipefail", "-c", &script])
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    scratch.path().display(),
                    std::env::var("PATH").unwrap()
                ),
            )
            .env("RUNNER_TEMP", scratch.path())
            .env("GITHUB_STEP_SUMMARY", scratch.path().join("summary"))
            .env("GITHUB_REPOSITORY", "Example/Logbrook")
            .env("TAG", "v0.2.0-rc.1")
            .env("VERSION", "0.2.0-rc.1")
            .env("RELEASE_COMMIT", "0123456789")
            .env("UPDATE_LATEST", "false"));
    }
    let arguments = fs::read_to_string(scratch.path().join("arguments")).unwrap();
    assert!(arguments.contains("--prerelease\n"));
    assert!(arguments.contains("--latest=false\n"));
    assert!(arguments.contains("--verify-tag\n"));
    let notes = fs::read_to_string(scratch.path().join("release-notes.md")).unwrap();
    assert!(notes.contains("ghcr.io/example/logbrook:0.2.0-rc.1"));
}
