#![recursion_limit = "256"]
use std::io::{self, Write};
use std::time::{SystemTime, UNIX_EPOCH};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_millis() as i64
}

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
