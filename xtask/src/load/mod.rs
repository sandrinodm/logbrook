mod engine;
mod http;
pub mod index;
pub mod mixed;
mod semantics;
mod tail;

use crate::Result;
use serde_json::Value;
use std::path::Path;

pub fn write_report(path: &Path, report: &Value) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, format!("{}\n", serde_json::to_string_pretty(report)?))?;
    crate::emit_json(report)
}

#[cfg(test)]
mod tests;

pub fn file_sha256(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::Read;

    let mut file = std::fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let length = file.read(&mut buffer)?;
        if length == 0 {
            break;
        }
        hash.update(&buffer[..length]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

pub fn source_sha256() -> String {
    use sha2::{Digest, Sha256};

    let mut hash = Sha256::new();
    for (name, source) in [
        ("mod.rs", include_bytes!("mod.rs").as_slice()),
        ("mixed.rs", include_bytes!("mixed.rs").as_slice()),
        ("index.rs", include_bytes!("index.rs").as_slice()),
        ("engine.rs", include_bytes!("engine.rs").as_slice()),
        ("http.rs", include_bytes!("http.rs").as_slice()),
        ("semantics.rs", include_bytes!("semantics.rs").as_slice()),
        ("tail.rs", include_bytes!("tail.rs").as_slice()),
    ] {
        hash.update(name);
        hash.update(source);
    }
    format!("{:x}", hash.finalize())
}
