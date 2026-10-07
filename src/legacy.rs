//! Offline migration from a DuckDB logs table and compatible Parquet archives.
//! Batches commit independently; retries can duplicate previously imported rows.

use std::path::{Path, PathBuf};

use duckdb::{AccessMode, Connection};
use serde_json::{Map, Value};
use tokio::sync::mpsc;

use crate::{
    config::Config,
    ingest,
    model::{Error, NormalizedEvent},
    storage::Storage,
};

type Result<T> = std::result::Result<T, Error>;

/// Read only offline copies. Neither the original database nor archive files are modified.
/// Existing duplicates across the database and archives are preserved.
pub async fn import_legacy(
    storage: &Storage,
    config: &Config,
    database: PathBuf,
    archives: Option<PathBuf>,
    source: String,
) -> Result<usize> {
    validate_source(&source)?;
    let config = config.clone();
    let (sender, mut receiver) = mpsc::channel(1);
    let producer = tokio::task::spawn_blocking(move || {
        read_batches(&config, &database, archives.as_deref(), sender)
    });
    let mut total = 0;
    let mut failure = None;

    while let Some(events) = receiver.recv().await {
        match storage.append(source.clone(), events).await {
            Ok(count) => total += count,
            Err(error) => {
                failure = Some(error);
                break;
            }
        }
    }

    drop(receiver);
    let produced = producer
        .await
        .map_err(|_| Error::unavailable("legacy reader worker failed"))?;

    if let Some(error) = failure.or_else(|| produced.err()) {
        return Err(Error::new(
            error.kind,
            format!("{}; {total} events already committed", error.message),
        ));
    }

    Ok(total)
}

pub fn validate_source(source: &str) -> Result<()> {
    crate::config::validate_source(source)
}

fn read_batches(
    config: &Config,
    database: &Path,
    archives: Option<&Path>,
    sender: mpsc::Sender<Vec<NormalizedEvent>>,
) -> Result<()> {
    ensure_plain_path(database)?;

    if !database.is_file() {
        return Err(Error::invalid("legacy database must be a regular file"));
    }

    let temporary = tempfile::Builder::new()
        .prefix("legacy-import-")
        .tempdir_in(&config.storage.data_dir)
        .map_err(|error| Error::unavailable(format!("legacy temporary directory: {error}")))?;
    let flags = duckdb::Config::default()
        .access_mode(AccessMode::ReadOnly)
        .map_err(legacy_error)?;
    let connection = Connection::open_with_flags(database, flags).map_err(legacy_error)?;
    let temporary = temporary
        .path()
        .to_str()
        .ok_or_else(|| Error::invalid("import temp path must be UTF-8"))?;
    connection
        .execute_batch(&format!("SET temp_directory={};", quote(temporary)))
        .map_err(legacy_error)?;

    // Native reader work shares the configured process budget instead of relying on engine defaults.
    connection
        .execute_batch(&format!(
            "SET threads={}; \
             SET memory_limit={}; \
             SET max_temp_directory_size={}; \
             SET autoinstall_known_extensions=false; \
             SET autoload_known_extensions=false;",
            config.storage.duckdb_threads,
            quote(&config.storage.memory_limit),
            quote(&config.storage.temp_limit),
        ))
        .map_err(legacy_error)?;

    let mut datasets = vec!["logs".to_owned()];

    if let Some(directory) = archives {
        let mut files = Vec::new();
        collect_parquet(directory, &mut files)?;
        files.sort();

        for file in files {
            let path = file
                .to_str()
                .ok_or_else(|| Error::invalid("archive path must be UTF-8"))?;
            datasets.push(format!(
                "read_parquet({}, hive_partitioning=false)",
                quote(path)
            ));
        }
    }

    let mut batch = Vec::new();
    let mut lines = 0;

    for dataset in datasets {
        // Legacy TIMESTAMP values represent UTC instants; epoch_ms converts them
        // to Unix milliseconds without applying the local time zone.
        let projection = "epoch_ms(timestamp)::BIGINT,level,service,name,hostname,\
                          CAST(pid AS BIGINT),msg,CAST(metadata AS VARCHAR)";

        // Check lengths before allocating Rust strings from a potentially oversized legacy row.
        let lengths = "octet_length(encode(coalesce(service,''))),\
                       octet_length(encode(coalesce(name,''))),\
                       octet_length(encode(coalesce(hostname,''))),\
                       octet_length(encode(coalesce(msg,''))),\
                       octet_length(encode(coalesce(CAST(metadata AS VARCHAR),'')))";
        let mut statement = connection
            .prepare(&format!("SELECT {projection},{lengths} FROM {dataset}"))
            .map_err(legacy_error)?;
        let mut rows = statement.query([]).map_err(legacy_error)?;

        while let Some(row) = rows.next().map_err(legacy_error)? {
            for column in 8..13 {
                let length: i64 = row.get(column).map_err(legacy_error)?;
                let maximum = if column == 12 {
                    config.max_event_bytes
                } else {
                    config.max_field_bytes
                };

                if length < 0 || length as u64 > maximum as u64 {
                    return Err(Error::invalid(
                        "legacy row exceeds configured field/event limits",
                    ));
                }
            }

            let metadata: Option<String> = row.get(7).map_err(legacy_error)?;
            let mut object = match metadata.as_deref() {
                None | Some("null") => Map::new(),
                Some(text) => match serde_json::from_str(text)
                    .map_err(|e| Error::invalid(format!("invalid legacy metadata: {e}")))?
                {
                    Value::Object(object) => object,
                    _ => return Err(Error::invalid("legacy metadata must be an object or null")),
                },
            };
            object.insert(
                "time".into(),
                Value::from(row.get::<_, i64>(0).map_err(legacy_error)?),
            );
            object.insert(
                "level".into(),
                Value::from(row.get::<_, i64>(1).map_err(legacy_error)?),
            );

            for (column, name) in [(2, "service"), (3, "name"), (4, "hostname"), (6, "msg")] {
                let value: Option<String> = row.get(column).map_err(legacy_error)?;
                object.insert(name.into(), value.map_or(Value::Null, Value::String));
            }

            object.insert(
                "pid".into(),
                row.get::<_, Option<i64>>(5)
                    .map_err(legacy_error)?
                    .map_or(Value::Null, Value::from),
            );
            let mut line = serde_json::to_vec(&Value::Object(object))
                .map_err(|e| Error::invalid(e.to_string()))?;

            if line.len() > config.max_event_bytes || line.len() + 1 > config.max_body_bytes {
                return Err(Error::invalid(
                    "legacy row exceeds configured event/body limits",
                ));
            }

            line.push(b'\n');

            if !batch.is_empty()
                && (batch.len() + line.len() > config.max_body_bytes || lines == config.max_events)
            {
                send_batch(config, &sender, &mut batch)?;
                lines = 0;
            }

            batch.extend_from_slice(&line);
            lines += 1;
        }
    }

    if !batch.is_empty() {
        send_batch(config, &sender, &mut batch)?;
    }

    Ok(())
}

fn send_batch(
    config: &Config,
    sender: &mpsc::Sender<Vec<NormalizedEvent>>,
    batch: &mut Vec<u8>,
) -> Result<()> {
    let events = ingest::normalize(batch, true, config)?;
    batch.clear();
    sender
        .blocking_send(events)
        .map_err(|_| Error::unavailable("legacy import consumer stopped"))
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn legacy_error(error: duckdb::Error) -> Error {
    Error::invalid(format!("legacy database: {error}"))
}

fn ensure_plain_path(path: &Path) -> Result<()> {
    crate::config::validate_literal_path(path)?;
    crate::config::validate_literal_path(
        &std::fs::canonicalize(path)
            .map_err(|error| Error::invalid(format!("legacy path: {error}")))?,
    )?;

    // Check every ancestor too: no indirect traversal through a symlinked directory.
    for ancestor in path.ancestors().filter(|path| !path.as_os_str().is_empty()) {
        let metadata = std::fs::symlink_metadata(ancestor)
            .map_err(|e| Error::invalid(format!("legacy path: {e}")))?;

        if metadata.file_type().is_symlink() {
            return Err(Error::invalid("legacy paths must not contain symlinks"));
        }
    }

    Ok(())
}

fn collect_parquet(directory: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    ensure_plain_path(directory)?;
    let mut directories = vec![directory.to_owned()];

    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(directory).map_err(|e| Error::invalid(e.to_string()))? {
            let entry = entry.map_err(|e| Error::invalid(e.to_string()))?;
            let kind = entry
                .file_type()
                .map_err(|e| Error::invalid(e.to_string()))?;

            if kind.is_symlink() {
                return Err(Error::invalid("legacy archives must not contain symlinks"));
            }

            if kind.is_dir() {
                directories.push(entry.path());
            } else if kind.is_file()
                && entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "parquet")
            {
                ensure_plain_path(&entry.path())?;
                files.push(entry.path());
            }
        }
    }

    Ok(())
}
