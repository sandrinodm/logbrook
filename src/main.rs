use std::{
    io::{BufRead, Read},
    path::PathBuf,
    time::Duration,
};

use clap::{Parser, Subcommand};
use logbrook::{config::Config, indexes::IndexRegistry, ingest, model::Error, storage::Storage};
use tokio::sync::watch;

mod cli_output;
mod client_cli;
mod server;

#[derive(Parser)]
#[command(version, about = "Lightweight self-hosted log search")]
struct Cli {
    /// TOML configuration; supported LOGBROOK_* environment variables override it.
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    /// Index used by offline import and archive commands.
    #[arg(long, global = true, default_value = "default")]
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
    /// Import an offline DuckDB logs table and optional Parquet archives.
    /// Optional archives are read recursively; retries can duplicate committed batches.
    ImportLegacy {
        database: PathBuf,
        #[arg(long)]
        archives: Option<PathBuf>,
        #[arg(long)]
        source: String,
    },
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

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
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
        Command::Archive { .. } | Command::Import { .. } | Command::ImportLegacy { .. }
    ) {
        let (root, names) = logbrook::indexes::prepare_layout(&config.storage.data_dir)?;
        logbrook::config::validate_index_name(&cli.index)?;
        let mut names: std::collections::HashSet<_> = names.into_iter().collect();
        names.extend(config.indexes.keys().cloned());
        names.insert("default".to_owned());
        names.insert(cli.index.clone());

        if names.len() > config.max_indexes {
            return Err(Error::invalid("index count would exceed max_indexes").into());
        }

        config.storage.data_dir = root;
        config = config.for_index(&cli.index)?;
        logbrook::indexes::prepare_index_directory(&config.storage.data_dir)?;
    }

    match cli.command {
        Command::Indexes { .. } | Command::Query(_) | Command::Count(_) => {
            unreachable!("remote commands handled before local configuration")
        }

        Command::CheckConfig => cli_output::line(format_args!("Configuration is valid"))?,
        Command::Healthcheck => unreachable!("readiness probe handled before full configuration"),
        Command::Serve => serve(config).await?,
        Command::Archive { before } => {
            if before < 0 {
                return Err(Error::invalid("before must be nonnegative Unix milliseconds").into());
            }

            let storage = Storage::open(config.storage)?;
            let result = async {
                let mut total = 0;

                loop {
                    let archived = storage.archive(before).await?;
                    total += archived;

                    if archived == 0 {
                        break;
                    }
                }

                Ok::<_, Error>(total)
            }
            .await;
            storage.shutdown().await?;
            cli_output::line(format_args!("{}", serde_json::json!({"archived": result?})))?;
        }

        Command::ImportLegacy {
            database,
            archives,
            source,
        } => {
            logbrook::legacy::validate_source(&source)?;
            let storage = Storage::open(config.storage.clone())?;
            let result =
                logbrook::legacy::import_legacy(&storage, &config, database, archives, source)
                    .await;
            storage.shutdown().await?;
            cli_output::line(format_args!("{}", serde_json::json!({"imported": result?})))?;
        }

        Command::Import {
            file,
            source,
            ndjson,
        } => {
            logbrook::legacy::validate_source(&source)?;
            let storage = Storage::open(config.storage.clone())?;
            let result = import(&storage, &config, file, source, ndjson).await;
            storage.shutdown().await?;
            cli_output::line(format_args!("{}", serde_json::json!({"imported": result?})))?;
        }
    }

    Ok(())
}

async fn import(
    storage: &Storage,
    config: &Config,
    path: PathBuf,
    source: String,
    ndjson: bool,
) -> Result<usize, Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;

    if !ndjson {
        let mut bytes = Vec::new();
        file.take(config.max_body_bytes as u64 + 1)
            .read_to_end(&mut bytes)?;

        if bytes.len() > config.max_body_bytes {
            return Err(
                Error::invalid("JSON import exceeds body limit; use batched NDJSON").into(),
            );
        }

        return Ok(storage
            .append(source, ingest::normalize(&bytes, false, config)?)
            .await?);
    }

    let mut reader = std::io::BufReader::new(file);
    let mut batch = Vec::new();
    let mut total = 0;
    let mut lines = 0;

    loop {
        let mut line = Vec::new();
        let read = (&mut reader)
            .take(config.max_event_bytes as u64 + 2)
            .read_until(b'\n', &mut line)?;

        if read == 0 {
            break;
        }

        if line.ends_with(b"\n") {
            line.pop();
        }

        if line.ends_with(b"\r") {
            line.pop();
        }

        if line.len() > config.max_event_bytes || line.len() > config.max_body_bytes {
            return Err(Error::invalid(format!(
                "NDJSON line exceeds event/body limit; {total} events already committed"
            ))
            .into());
        }

        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }

        if !batch.is_empty()
            && (batch.len() + line.len() + 1 > config.max_body_bytes || lines == config.max_events)
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

async fn serve(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    let registry = IndexRegistry::open(config.clone()).await?;
    let (stop, stopped) = watch::channel(false);
    let router = logbrook::http::router_with_registry(registry.clone(), config.clone());
    let server_stop = stopped.clone();
    tracing::info!(address = %listener.local_addr()?, "Logbrook listening");
    let mut server = tokio::spawn(server::serve(
        listener,
        router,
        config.max_connections,
        Duration::from_millis(config.header_timeout_ms),
        server_stop,
    ));
    let mut server_finished = false;
    let mut failed = false;
    tokio::select! {
        result = &mut server => {
            server_finished = true;

            match result {
                Ok(Ok(())) => {},
                Ok(Err(error)) => {
                    failed = true;
                    tracing::error!(%error, "HTTP listener failed; shutting down");
                },
                Err(error) => {
                    failed = true;
                    tracing::error!(%error, "HTTP worker failed; shutting down");
                },
            }
        }

        _ = shutdown_signal() => {
            tracing::info!("Shutdown requested");
        }

        _ = async {
            while registry.is_ready() {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        } => {
            failed = true;
            tracing::error!("Storage worker unavailable; shutting down for restart");
        }
    }

    registry.stop_admission();
    let _ = stop.send(true);
    let shutdown = async {
        registry.shutdown().await?;

        if !server_finished {
            server.await??;
        }

        Ok::<_, Box<dyn std::error::Error>>(())
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

async fn healthcheck(mut address: std::net::SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    if address.ip().is_unspecified() {
        address.set_ip(if address.is_ipv6() {
            "::1".parse()?
        } else {
            "127.0.0.1".parse()?
        });
    }

    tokio::time::timeout(Duration::from_secs(3), async {
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        stream
            .write_all(b"GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await?;
        let mut prefix = [0_u8; 12];
        stream.read_exact(&mut prefix).await?;

        if &prefix != b"HTTP/1.1 200" {
            return Err(std::io::Error::other("Logbrook is not ready"));
        }

        Ok::<_, std::io::Error>(())
    })
    .await??;

    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
