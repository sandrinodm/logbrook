//! Prepare version-only changes while preserving manifest formatting and license texts.

use crate::Result;
use semver::Version;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{fs, path::Path};

#[cfg(all(test, unix))]
#[path = "release_workflow_tests.rs"]
mod workflow_tests;

#[derive(clap::Args)]
pub struct Args {
    /// current, patch, minor, major, or an explicit version without a leading v.
    pub version: String,
    /// Validate and report the target without modifying files.
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Deserialize)]
struct Manifest {
    package: Package,
}

#[derive(Deserialize)]
struct Lockfile {
    package: Vec<Package>,
}

#[derive(Deserialize)]
struct Package {
    name: String,
    version: toml::Spanned<String>,
    source: Option<String>,
}

pub fn run(args: Args) -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or("workspace root missing")?;
    crate::emit_json(&prepare(root, &args.version, args.dry_run)?)
}

fn target_version(current: &Version, requested: &str) -> Result<Version> {
    let mut target = current.clone();

    if matches!(requested, "patch" | "minor" | "major") {
        if !current.pre.is_empty() {
            return Err("use an explicit version when promoting a prerelease".into());
        }

        match requested {
            "major" => {
                target.major = target
                    .major
                    .checked_add(1)
                    .ok_or("major version overflow")?;
                target.minor = 0;
                target.patch = 0;
            }
            "minor" => {
                target.minor = target
                    .minor
                    .checked_add(1)
                    .ok_or("minor version overflow")?;
                target.patch = 0;
            }
            _ => {
                target.patch = target
                    .patch
                    .checked_add(1)
                    .ok_or("patch version overflow")?
            }
        }
    } else if requested != "current" {
        target = Version::parse(requested)?;
    }

    if !target.build.is_empty() {
        return Err("release versions cannot include build metadata".into());
    }

    if target < *current {
        return Err("release version cannot be lower than the current application version".into());
    }

    // Both the image version and the v-prefixed release tag must fit the publisher's limit.
    if target.to_string().len() + 1 > 128 {
        return Err("release tag exceeds the 128-character publication limit".into());
    }

    Ok(target)
}

fn fingerprint(text: &str) -> String {
    format!("Cargo.lock SHA-256: {:x}", Sha256::digest(text.as_bytes()))
}

fn replace_once(text: &str, old: &str, new: &str) -> Result<String> {
    if text.matches(old).count() != 1 {
        return Err(format!("expected exactly one version marker: {old}").into());
    }

    Ok(text.replacen(old, new, 1))
}

fn prepare(root: &Path, requested: &str, dry_run: bool) -> Result<serde_json::Value> {
    let manifest_path = root.join("Cargo.toml");
    let lock_path = root.join("Cargo.lock");
    let notices_path = root.join("THIRD_PARTY_NOTICES.txt");
    let docker_path = root.join("Dockerfile");
    let api_path = root.join("src/openapi.json");
    let mut manifest_text = fs::read_to_string(&manifest_path)?;
    let mut lock_text = fs::read_to_string(&lock_path)?;
    let notices_text = fs::read_to_string(&notices_path)?;
    let docker_text = fs::read_to_string(&docker_path)?;
    let api_text = fs::read_to_string(&api_path)?;
    let manifest: Manifest = toml::from_str(&manifest_text)?;
    let lock: Lockfile = toml::from_str(&lock_text)?;

    if manifest.package.name != "logbrook" {
        return Err("release preparation requires the Logbrook application manifest".into());
    }

    let candidates: Vec<_> = lock
        .package
        .iter()
        .filter(|package| package.name == "logbrook" && package.source.is_none())
        .collect();
    let [locked] = candidates.as_slice() else {
        return Err("Cargo.lock must contain exactly one local Logbrook package".into());
    };

    if locked.version.get_ref() != manifest.package.version.get_ref() {
        return Err("Cargo.toml and Cargo.lock application versions differ".into());
    }

    let old_fingerprint = fingerprint(&lock_text);
    if notices_text.matches(&old_fingerprint).count() != 1 {
        return Err(
            "dependency notices are stale; regenerate them before preparing a release".into(),
        );
    }

    let current = Version::parse(manifest.package.version.get_ref())?;
    let target = target_version(&current, requested)?;
    let changed = current != target;
    let replacement = format!("\"{target}\"");

    // These distributed metadata fields describe the application release too.
    // Validate all markers before writing any of the version files.
    let api: serde_json::Value = serde_json::from_str(&api_text)?;
    if api["info"]["version"] != current.to_string() {
        return Err("OpenAPI version differs from the application version".into());
    }
    let api_text = replace_once(
        &api_text,
        &format!("\"version\": \"{current}\""),
        &format!("\"version\": \"{target}\""),
    )?;
    let docker_text = replace_once(
        &docker_text,
        &format!("\nARG VERSION={current}\n"),
        &format!("\nARG VERSION={target}\n"),
    )?;
    manifest_text.replace_range(manifest.package.version.span(), &replacement);
    lock_text.replace_range(locked.version.span(), &replacement);

    // Only the local package version changed, so dependency license content is
    // identical. Preserve every notice byte except the validated lock fingerprint.
    // CI still regenerates and checks the complete bundle before publication.
    let notices_text = notices_text.replacen(&old_fingerprint, &fingerprint(&lock_text), 1);

    if changed && !dry_run {
        fs::write(manifest_path, manifest_text)?;
        fs::write(lock_path, lock_text)?;
        fs::write(notices_path, notices_text)?;
        fs::write(docker_path, docker_text)?;
        fs::write(api_path, api_text)?;
    }

    Ok(serde_json::json!({
        "previous": current.to_string(),
        "version": target.to_string(),
        "tag": format!("v{target}"),
        "changed": changed,
        "dry_run": dry_run,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn fixture() -> tempfile::TempDir {
        let directory = tempfile::tempdir().unwrap();
        let lock = "version = 4\n\n[[package]]\nname = \"logbrook\"\nversion = \"0.1.0\"\n\n[[package]]\nname = \"dependency\"\nversion = \"0.1.0\"\nsource = \"registry+example\"\n";
        fs::write(directory.path().join("Cargo.toml"), "# Keep this comment.\n[package]\nname = \"logbrook\"\nversion = \"0.1.0\" # application\n\n[dependencies]\ndependency = \"0.1.0\"\n").unwrap();
        fs::write(directory.path().join("Cargo.lock"), lock).unwrap();
        fs::write(
            directory.path().join("Dockerfile"),
            "FROM example\nARG VERSION=0.1.0\n",
        )
        .unwrap();
        fs::create_dir(directory.path().join("src")).unwrap();
        fs::write(
            directory.path().join("src/openapi.json"),
            "{\n  \"info\": {\n    \"version\": \"0.1.0\"\n  }\n}\n",
        )
        .unwrap();
        fs::write(
            directory.path().join("THIRD_PARTY_NOTICES.txt"),
            format!("{}\n\nOriginal license text\n", fingerprint(lock)),
        )
        .unwrap();
        directory
    }

    #[test]
    fn resolves_initial_incremented_and_explicit_versions() {
        let current = Version::parse("0.1.0").unwrap();
        for (requested, expected) in [
            ("current", "0.1.0"),
            ("patch", "0.1.1"),
            ("minor", "0.2.0"),
            ("major", "1.0.0"),
            ("0.2.0-rc.1", "0.2.0-rc.1"),
            ("0.2.0--x", "0.2.0--x"),
        ] {
            assert_eq!(
                target_version(&current, requested).unwrap().to_string(),
                expected
            );
        }

        for invalid in ["v0.2.0", "0.0.9", "01.2.3", "0.2.0+build", "--help", ""] {
            assert!(target_version(&current, invalid).is_err(), "{invalid}");
        }

        let prerelease = Version::parse("0.2.0-rc.1").unwrap();
        assert!(target_version(&prerelease, "patch").is_err());
        assert_eq!(
            target_version(&prerelease, "0.2.0").unwrap().to_string(),
            "0.2.0"
        );
        assert!(target_version(&current, &format!("0.2.0-{}", "x".repeat(123))).is_err());
        assert!(target_version(&Version::new(0, 1, u64::MAX), "patch").is_err());
    }

    #[test]
    fn updates_only_the_application_versions_and_notice_fingerprint() {
        let directory = fixture();
        let root = directory.path();
        let manifest = fs::read_to_string(root.join("Cargo.toml")).unwrap();
        let lock = fs::read_to_string(root.join("Cargo.lock")).unwrap();

        let preview = prepare(root, "minor", true).unwrap();
        assert_eq!(preview["tag"], "v0.2.0");
        assert_eq!(
            fs::read_to_string(root.join("Cargo.toml")).unwrap(),
            manifest
        );
        assert_eq!(fs::read_to_string(root.join("Cargo.lock")).unwrap(), lock);

        prepare(root, "minor", false).unwrap();
        assert_eq!(
            fs::read_to_string(root.join("Cargo.toml")).unwrap(),
            manifest.replacen("version = \"0.1.0\"", "version = \"0.2.0\"", 1)
        );
        let updated_lock = fs::read_to_string(root.join("Cargo.lock")).unwrap();
        assert_eq!(
            updated_lock,
            lock.replacen("version = \"0.1.0\"", "version = \"0.2.0\"", 1)
        );
        assert_eq!(
            fs::read_to_string(root.join("THIRD_PARTY_NOTICES.txt")).unwrap(),
            format!("{}\n\nOriginal license text\n", fingerprint(&updated_lock))
        );
        assert_eq!(prepare(root, "current", false).unwrap()["changed"], false);
        assert!(
            fs::read_to_string(root.join("Dockerfile"))
                .unwrap()
                .contains("ARG VERSION=0.2.0\n")
        );
        let api: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(root.join("src/openapi.json")).unwrap())
                .unwrap();
        assert_eq!(api["info"]["version"], "0.2.0");
    }

    #[test]
    fn refuses_stale_notices_before_changing_version_files() {
        let directory = fixture();
        let root = directory.path();
        let original = fs::read(root.join("Cargo.toml")).unwrap();
        fs::write(root.join("THIRD_PARTY_NOTICES.txt"), "stale").unwrap();

        assert!(prepare(root, "patch", false).is_err());
        assert_eq!(fs::read(root.join("Cargo.toml")).unwrap(), original);
    }

    #[test]
    fn rejects_inconsistent_lock_versions_and_downgrades_without_writes() {
        let directory = fixture();
        let root = directory.path();
        let original = fs::read_to_string(root.join("Cargo.toml")).unwrap();
        assert!(prepare(root, "0.0.9", false).is_err());
        assert_eq!(
            fs::read_to_string(root.join("Cargo.toml")).unwrap(),
            original
        );

        let changed = original.replace("version = \"0.1.0\"", "version = \"0.1.1\"");
        fs::write(root.join("Cargo.toml"), &changed).unwrap();
        assert!(prepare(root, "patch", false).is_err());
        assert_eq!(
            fs::read_to_string(root.join("Cargo.toml")).unwrap(),
            changed
        );
    }

    #[test]
    fn rejects_stale_distributed_versions_before_writing() {
        for file in ["Dockerfile", "src/openapi.json"] {
            let directory = fixture();
            let root = directory.path();
            let original = fs::read(root.join("Cargo.toml")).unwrap();
            let text = fs::read_to_string(root.join(file)).unwrap();
            fs::write(root.join(file), text.replace("0.1.0", "0.0.9")).unwrap();

            assert!(prepare(root, "patch", false).is_err());
            assert_eq!(fs::read(root.join("Cargo.toml")).unwrap(), original);
        }
    }

    #[test]
    fn repository_release_metadata_matches_manifest() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        assert_eq!(prepare(root, "current", true).unwrap()["changed"], false);
    }
}
