//! The `logbrook` command: serves the API, runs offline import and archive commands on a data
//! directory, and forwards remote commands to a running server.

use std::{
    collections::HashSet,
    fs::File,
    io::{BufRead, BufReader, Read},
    net::SocketAddr,
    path::PathBuf,
    time::Duration,
};

use clap::{Parser, Subcommand};
use logbrook::{
    config::{Config, DEFAULT_INDEX_NAME},
    indexes::IndexRegistry,
    ingest,
    model::Error,
    storage::Storage,
};
use tokio::{sync::watch, task::JoinError};

mod cli_output;
mod client_cli;
mod server;

/// How often the supervisor checks that the index registry is still ready.
const READINESS_POLL: Duration = Duration::from_millis(250);

/// The whole readiness probe, from connect to the status line.
const HEALTHCHECK_TIMEOUT: Duration = Duration::from_secs(3);

/// Length of the status line prefix the probe compares.
const READY_PREFIX_LEN: usize = 12;

/// The status line of a ready server, as its first bytes on the wire.
const READY_STATUS_PREFIX: &[u8; READY_PREFIX_LEN] = b"HTTP/1.1 200";

/// Bytes a line may exceed the event limit by: its `\r\n` terminator.
const LINE_TERMINATOR_BYTES: u64 = 2;

/// The newline that joins NDJSON lines in a batch.
const NEWLINE_BYTES: usize = 1;

#[derive(Parser)]
#[command(version, about = "Self-hosted log search")]
struct Cli {
    /// TOML configuration; supported LOGBROOK_* environment variables override it.
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    /// Index used by offline import and archive commands.
    #[arg(long, global = true, default_value = DEFAULT_INDEX_NAME)]
    index: String,
    #[command(flatten)]
    remote: client_cli::Options,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Manage indexes on a running Logbrook server.
    Indexes {
        #[command(subcommand)]
        command: client_cli::Indexes,
    },
    /// Search events on a running server, including the next-page cursor.
    Query(client_cli::Search),
    /// Count matching events on a running server.
    Count(client_cli::Selection),
    /// Start the HTTP API.
    Serve,
    /// Validate configuration without opening or changing the data directory.
    CheckConfig,
    /// Probe readiness for container health checks without needing credentials.
    Healthcheck,
    /// Import Pino JSON/NDJSON into an offline Logbrook data directory.
    /// Each bounded batch commits independently. Retrying may duplicate earlier batches.
    Import {
        file: PathBuf,
        #[arg(long)]
        source: String,
        #[arg(long)]
        ndjson: bool,
    },
    /// Inspect the durable projection layout and row coverage in an offline data directory.
    ProjectionStatus,
    /// Archive events before a Unix millisecond timestamp in an offline data directory.
    Archive {
        #[arg(long)]
        before: i64,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "logbrook=info".into()),
        )
        .json()
        .init();

    if let Err(error) = run(Cli::parse()).await {
        tracing::error!(error = %error, "Logbrook stopped");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), BoxError> {
    let remote = match &cli.command {
        Command::Indexes { command } => Some(client_cli::Operation::Indexes(command)),
        Command::Query(query) => Some(client_cli::Operation::Query(query)),
        Command::Count(selection) => Some(client_cli::Operation::Count(selection)),
        _ => None,
    };
    if let Some(operation) = remote {
        if cli.config.is_some() {
            return Err(Error::invalid(
                "--config is for local commands; remote commands use --url \
                 and client token environment variables",
            )
            .into());
        }

        return client_cli::run(&cli.remote, operation).await;
    }

    if matches!(&cli.command, Command::Healthcheck) {
        return healthcheck(Config::healthcheck_address(cli.config.as_deref())?).await;
    }

    let mut config = Config::load(cli.config.as_deref())?;

    if matches!(
        &cli.command,
        Command::Archive { .. } | Command::Import { .. } | Command::ProjectionStatus
    ) {
        config = prepare_offline_index(config, &cli.index)?;
    }

    match cli.command {
        Command::Indexes { .. } | Command::Query(_) | Command::Count(_) => {
            unreachable!("remote commands handled before local configuration")
        }

        Command::CheckConfig => cli_output::line(format_args!("Configuration is valid"))?,
        Command::Healthcheck => unreachable!("readiness probe handled before full configuration"),
        Command::Serve => serve(config).await?,
        Command::ProjectionStatus => projection_status(config).await?,
        Command::Archive { before } => archive_command(config, before).await?,
        Command::Import {
            file,
            source,
            ndjson,
        } => {
            let format = if ndjson {
                ImportFormat::Ndjson
            } else {
                ImportFormat::Json
            };
            import_command(config, file, source, format).await?;
        }
    }

    Ok(())
}

/// Prepares the data layout and the chosen index for an offline command that writes storage.
fn prepare_offline_index(mut config: Config, index: &str) -> Result<Config, Error> {
    let (root, names) = logbrook::indexes::prepare_layout(&config.storage.data_dir)?;
    logbrook::config::validate_index_name(index)?;

    let mut names: HashSet<String> = names.into_iter().collect();
    names.extend(config.indexes.keys().cloned());
    names.insert(DEFAULT_INDEX_NAME.to_owned());
    names.insert(index.to_owned());

    if names.len() > config.max_indexes {
        return Err(Error::invalid("index count would exceed max_indexes"));
    }

    config.storage.data_dir = root;
    let config = config.for_index(index)?;
    logbrook::indexes::prepare_index_directory(&config.storage.data_dir)?;

    Ok(config)
}

async fn projection_status(config: Config) -> Result<(), BoxError> {
    let storage = Storage::open(config.storage)?;
    let result = storage.projection_status().await;
    storage.shutdown().await?;
    cli_output::line(format_args!("{}", serde_json::to_string(&result?)?))?;

    Ok(())
}

async fn archive_command(config: Config, before: i64) -> Result<(), BoxError> {
    if before < 0 {
        return Err(Error::invalid("before must be nonnegative Unix milliseconds").into());
    }

    let storage = Storage::open(config.storage)?;
    let result = archive_all(&storage, before).await;
    storage.shutdown().await?;
    cli_output::line(format_args!("{}", serde_json::json!({"archived": result?})))?;

    Ok(())
}

/// Archives in batches until a batch finds nothing older than `before`. Returns the total.
async fn archive_all(storage: &Storage, before: i64) -> Result<usize, Error> {
    let mut total = 0;
    loop {
        let archived = storage.archive(before).await?;
        total += archived;

        if archived == 0 {
            break;
        }
    }

    Ok(total)
}

async fn import_command(
    config: Config,
    file: PathBuf,
    source: String,
    format: ImportFormat,
) -> Result<(), BoxError> {
    logbrook::config::validate_source(&source)?;

    let storage = Storage::open(config.storage.clone())?;
    let result = import(&storage, &config, file, source, format).await;
    storage.shutdown().await?;
    cli_output::line(format_args!("{}", serde_json::json!({"imported": result?})))?;

    Ok(())
}

/// Imports a file as one JSON document, or as NDJSON in bounded batches. Returns the number of
/// events committed.
async fn import(
    storage: &Storage,
    config: &Config,
    path: PathBuf,
    source: String,
    format: ImportFormat,
) -> Result<usize, BoxError> {
    let file = File::open(path)?;
    if matches!(format, ImportFormat::Ndjson) {
        import_ndjson(storage, config, file, source).await
    } else {
        import_json(storage, config, file, source).await
    }
}

async fn import_json(
    storage: &Storage,
    config: &Config,
    file: File,
    source: String,
) -> Result<usize, BoxError> {
    // One byte past the limit is read, so an oversized file is detected rather than truncated.
    let limit = u64::try_from(config.max_body_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut bytes = Vec::new();
    file.take(limit).read_to_end(&mut bytes)?;

    if bytes.len() > config.max_body_bytes {
        return Err(Error::invalid("JSON import exceeds body limit; use batched NDJSON").into());
    }

    Ok(storage
        .append(source, ingest::normalize(&bytes, false, config)?)
        .await?)
}

/// Commits NDJSON in batches that respect the body and event limits. Each batch commits on its
/// own, so a failure leaves the batches before it durable.
async fn import_ndjson(
    storage: &Storage,
    config: &Config,
    file: File,
    source: String,
) -> Result<usize, BoxError> {
    let mut reader = BufReader::new(file);
    let mut batch = Vec::new();
    let mut total = 0;
    let mut lines = 0;
    while let Some(line) = read_ndjson_line(&mut reader, config, total)? {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }

        if !batch.is_empty()
            && (batch.len() + line.len() + NEWLINE_BYTES > config.max_body_bytes
                || lines == config.max_events)
        {
            let events = ingest::normalize(&batch, true, config)?;
            total += storage.append(source.clone(), events).await?;
            batch.clear();
            lines = 0;
        }

        if !batch.is_empty() {
            batch.push(b'\n');
        }

        batch.extend_from_slice(&line);
        lines += 1;
    }

    if !batch.is_empty() {
        total += storage
            .append(source, ingest::normalize(&batch, true, config)?)
            .await?;
    }

    Ok(total)
}

/// Reads one NDJSON line without its terminator, or `None` at the end of the file.
///
/// `committed` is the number of events already stored, reported when a line is too long.
fn read_ndjson_line(
    reader: &mut BufReader<File>,
    config: &Config,
    committed: usize,
) -> Result<Option<Vec<u8>>, BoxError> {
    let limit = u64::try_from(config.max_event_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(LINE_TERMINATOR_BYTES);
    let mut line = Vec::new();
    let read = reader.take(limit).read_until(b'\n', &mut line)?;
    if read == 0 {
        return Ok(None);
    }

    if line.ends_with(b"\n") {
        line.pop();
    }

    if line.ends_with(b"\r") {
        line.pop();
    }

    if line.len() > config.max_event_bytes || line.len() > config.max_body_bytes {
        return Err(Error::invalid(format!(
            "NDJSON line exceeds event/body limit; {committed} events already committed"
        ))
        .into());
    }

    Ok(Some(line))
}

/// A boxed error for the command-level paths, which report any failure the same way.
type BoxError = Box<dyn std::error::Error>;

/// How an import file is read: as one JSON document, or as NDJSON in bounded batches.
#[derive(Clone, Copy)]
enum ImportFormat {
    Json,
    Ndjson,
}

/// Runs the HTTP API and the Forward receiver until a signal, a worker failure, or a storage
/// failure. Then it drains everything within the shutdown deadline.
async fn serve(config: Config) -> Result<(), BoxError> {
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    let registry = IndexRegistry::open(config.clone()).await?;
    let receiver =
        match logbrook::forward::Receiver::bind(config.forward.clone(), registry.clone()).await {
            Ok(receiver) => receiver,
            Err(error) => {
                registry.shutdown().await?;
                return Err(error.into());
            }
        };

    let (stop, stopped) = watch::channel(false);
    let router = logbrook::http::router_with_registry(registry.clone(), config.clone());
    tracing::info!(address = %listener.local_addr()?, "Logbrook listening");

    let mut server = tokio::spawn(server::serve(
        listener,
        router,
        config.max_connections,
        Duration::from_millis(config.header_timeout_ms),
        stopped.clone(),
    ));

    let mut server_finished = false;
    let mut forward = tokio::spawn(receiver.serve(stopped.clone()));
    let mut forward_finished = false;
    let mut failed = false;
    tokio::select! {
        result = &mut forward => {
            forward_finished = true;
            report_forward_exit(result);
            failed = true;
        }
        result = &mut server => {
            server_finished = true;
            failed = report_server_exit(result);
        }

        _ = shutdown_signal() => {
            tracing::info!("Shutdown requested");
        }

        _ = registry_unavailable(&registry) => {
            failed = true;
            tracing::error!("Storage worker unavailable; shutting down for restart");
        }
    }

    registry.stop_admission();
    let _ = stop.send(true);

    let shutdown = async {
        if !server_finished {
            server.await??;
        }

        if !forward_finished {
            forward.await??;
        }

        registry.shutdown().await?;

        Ok::<_, BoxError>(())
    };
    match tokio::time::timeout(Duration::from_secs(config.shutdown_timeout_secs), shutdown).await {
        Ok(result) => result?,
        Err(_) => {
            return Err(Error::unavailable(
                "shutdown deadline exceeded; unacknowledged batches may be retried",
            )
            .into());
        }
    }

    if failed {
        return Err(Error::unavailable("server or storage worker failure requires restart").into());
    }

    Ok(())
}

/// Logs how the Forward listener ended. Every exit of this task is a failure.
fn report_forward_exit(result: Result<Result<(), Error>, JoinError>) {
    match result {
        Ok(Err(error)) => tracing::error!(%error, "Forward listener failed; shutting down"),
        Err(error) => tracing::error!(%error, "Forward supervisor failed; shutting down"),
        Ok(Ok(())) => tracing::error!("Forward listener stopped unexpectedly; shutting down"),
    }
}

/// Logs how the HTTP listener ended. Returns whether that counts as a failure.
fn report_server_exit(result: Result<std::io::Result<()>, JoinError>) -> bool {
    match result {
        Ok(Ok(())) => false,
        Ok(Err(error)) => {
            tracing::error!(%error, "HTTP listener failed; shutting down");
            true
        }
        Err(error) => {
            tracing::error!(%error, "HTTP worker failed; shutting down");
            true
        }
    }
}

/// Resolves once the index registry reports that it is not ready.
async fn registry_unavailable(registry: &IndexRegistry) {
    while registry.is_ready() {
        tokio::time::sleep(READINESS_POLL).await;
    }
}

/// Checks the local `/ready` endpoint, so container health checks need no credentials.
async fn healthcheck(mut address: SocketAddr) -> Result<(), BoxError> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    if address.ip().is_unspecified() {
        address.set_ip(if address.is_ipv6() {
            "::1".parse()?
        } else {
            "127.0.0.1".parse()?
        });
    }

    tokio::time::timeout(HEALTHCHECK_TIMEOUT, async {
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        stream
            .write_all(b"GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await?;

        let mut prefix = [0_u8; READY_PREFIX_LEN];
        stream.read_exact(&mut prefix).await?;

        if &prefix != READY_STATUS_PREFIX {
            return Err(std::io::Error::other("Logbrook is not ready"));
        }

        Ok::<_, std::io::Error>(())
    })
    .await??;

    Ok(())
}

/// Resolves on Ctrl-C, or on SIGTERM where the platform has it.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
