//! Development checks and load generators for Logbrook: container packaging, third-party
//! notices, release preparation and the load and qualification commands.
#![recursion_limit = "256"]
use std::io::{self, Write};
use std::time::{SystemTime, UNIX_EPOCH};

/// Result of a development command. Errors are boxed because each command reports its own.
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Milliseconds since the Unix epoch.
///
/// # Panics
///
/// Panics when the system clock is before the Unix epoch.
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_millis() as i64
}

/// Renders digest bytes with stable lowercase hexadecimal encoding.
pub(crate) fn hex_digest(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

/// Writes `value` to standard output as pretty JSON followed by a newline.
///
/// # Errors
///
/// Returns an error when serialization or writing fails.
pub fn emit_json(value: &impl serde::Serialize) -> Result<()> {
    let mut output = io::stdout().lock();
    serde_json::to_writer_pretty(&mut output, value)?;
    writeln!(output)?;
    Ok(())
}

pub mod container;
pub mod extended;
pub mod load;
pub mod notices;
pub mod release;
pub mod runtime;

#[cfg(test)]
mod digest_tests {
    use sha2::{Digest, Sha256};

    #[test]
    fn sha256_fingerprints_keep_lowercase_hex_and_leading_zeroes() {
        assert_eq!(
            super::hex_digest(&Sha256::digest(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(super::hex_digest(&[0, 1, 15, 16, 255]), "00010f10ff");
    }
}
