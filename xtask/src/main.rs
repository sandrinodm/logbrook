use clap::{Parser, Subcommand};
use logbrook_dev::{Result, container, extended, load, notices, release};
use std::io::Write;

#[derive(Parser)]
#[command(about = "Disposable Logbrook development checks and load generators")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    ContainerSmoke(container::SmokeArgs),
    ImageInspect(container::InspectArgs),
    ContainerProbe(container::ProbeArgs),
    CheckCli(container::CliArgs),
    ExtendedLoad(extended::Options),
    MixedLoad(load::mixed::Args),
    IndexLoad(load::index::Args),
    Notices(notices::Args),
    /// Prepare the application version and release metadata without committing or publishing.
    ReleaseVersion(release::Args),
    Version,
}

#[tokio::main]
async fn main() -> Result<()> {
    let command = Cli::parse().command;
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let work = run_command(command);
    tokio::pin!(work);

    // Dropping the command future runs fixture cleanup before the runtime exits.
    tokio::select! {
        result = &mut work => result,
        interrupt = tokio::signal::ctrl_c() => {
            interrupt?;
            Err("development command interrupted".into())
        },
        _ = async {
            #[cfg(unix)]
            { terminate.recv().await; }
            #[cfg(not(unix))]
            { std::future::pending::<()>().await; }
        } => Err("development command terminated".into()),
    }
}

async fn run_command(command: Commands) -> Result<()> {
    match command {
        Commands::ContainerSmoke(args) => container::smoke(args).await,
        Commands::ImageInspect(args) => container::inspect(args).await,
        Commands::ContainerProbe(args) => {
            logbrook_dev::emit_json(&container::probe(&args.container, &args.counters).await?)
        }
        Commands::CheckCli(args) => container::check_cli(args).await,
        Commands::MixedLoad(args) => load::mixed::run(args).await,
        Commands::IndexLoad(args) => load::index::run(args).await,
        Commands::ExtendedLoad(args) => extended::run(args).await,
        Commands::Notices(args) => notices::run(args).await,
        Commands::ReleaseVersion(args) => release::run(args),
        Commands::Version => {
            let manifest: toml::Value = toml::from_str(include_str!("../../Cargo.toml"))?;
            let version = manifest["package"]["version"]
                .as_str()
                .ok_or("application version missing")?;
            writeln!(std::io::stdout().lock(), "{version}")?;
            Ok(())
        }
    }
}
