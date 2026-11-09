//! Load-test drivers and their shared helpers: reports, hashes and the probe modules.

mod engine;
pub mod forward;
mod http;
pub mod index;
pub mod mixed;
mod semantics;
mod tail;

use crate::Result;
use serde_json::Value;
use std::path::Path;

/// Bytes read per step when hashing a file.
const HASH_BUFFER_BYTES: usize = 64 * 1024;

/// Writes `report` to `path`, creating its parent directories, and prints it as JSON.
///
/// # Errors
///
/// Returns an error when the file or the output cannot be written.
pub fn write_report(path: &Path, report: &Value) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }

    std::fs::write(path, format!("{}\n", serde_json::to_string_pretty(report)?))?;
    crate::emit_json(report)
}

#[cfg(test)]
mod tests;

/// Hex SHA-256 of the file at `path`, read in `HASH_BUFFER_BYTES` pieces.
///
/// # Errors
///
/// Returns an error when the file cannot be opened or read.
pub fn file_sha256(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::Read;

    let mut file = std::fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0; HASH_BUFFER_BYTES];
    loop {
        let length = file.read(&mut buffer)?;
        if length == 0 {
            break;
        }
        hash.update(&buffer[..length]);
    }

    Ok(crate::hex_digest(&hash.finalize()))
}

/// Fingerprint of the load driver sources, so that a report records the code that produced it.
pub fn source_sha256() -> String {
    use sha2::{Digest, Sha256};

    let mut hash = Sha256::new();
    for (name, source) in [
        ("mod.rs", include_bytes!("mod.rs").as_slice()),
        (
            "attributes/constants.rs",
            include_bytes!("attributes/constants.rs").as_slice(),
        ),
        (
            "attributes/data.rs",
            include_bytes!("attributes/data.rs").as_slice(),
        ),
        (
            "attributes/execution.rs",
            include_bytes!("attributes/execution.rs").as_slice(),
        ),
        (
            "attributes/fixture.rs",
            include_bytes!("attributes/fixture.rs").as_slice(),
        ),
        (
            "attributes/mod.rs",
            include_bytes!("attributes/mod.rs").as_slice(),
        ),
        (
            "attributes/options.rs",
            include_bytes!("attributes/options.rs").as_slice(),
        ),
        (
            "attributes/provenance.rs",
            include_bytes!("attributes/provenance.rs").as_slice(),
        ),
        (
            "attributes/queries.rs",
            include_bytes!("attributes/queries.rs").as_slice(),
        ),
        (
            "attributes/report.rs",
            include_bytes!("attributes/report.rs").as_slice(),
        ),
        (
            "attributes/tests.rs",
            include_bytes!("attributes/tests.rs").as_slice(),
        ),
        (
            "attributes_compare.rs",
            include_bytes!("attributes_compare.rs").as_slice(),
        ),
        (
            "attributes_live.rs",
            include_bytes!("attributes_live.rs").as_slice(),
        ),
        (
            "attributes_schedule.rs",
            include_bytes!("attributes_schedule.rs").as_slice(),
        ),
        ("mixed.rs", include_bytes!("mixed.rs").as_slice()),
        ("index.rs", include_bytes!("index.rs").as_slice()),
        ("forward.rs", include_bytes!("forward.rs").as_slice()),
        ("engine.rs", include_bytes!("engine.rs").as_slice()),
        ("http.rs", include_bytes!("http.rs").as_slice()),
        ("semantics.rs", include_bytes!("semantics.rs").as_slice()),
        ("tail.rs", include_bytes!("tail.rs").as_slice()),
    ] {
        hash.update(name);
        hash.update(source);
    }

    crate::hex_digest(&hash.finalize())
}

pub mod attributes;
pub mod attributes_compare;
mod attributes_live;
mod attributes_schedule;
