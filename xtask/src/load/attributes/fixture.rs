//! Offline fixture import, archive preparation, and projection coverage checks.
use super::{
    Args,
    constants::{
        CONTAINER_CPUS, CONTAINER_MEMORY_MIB, OFFLINE_COMMAND_SECS, PROJECTION_SCHEMA_VERSION,
    },
    data::{Seed, write_seed},
};
use crate::load::semantics::DAY_MS;
use crate::{
    Result,
    runtime::{Fixture, ResourceLimits},
};
use serde_json::{Value, json};
use std::{
    io::{self, Write},
    path::PathBuf,
    time::Instant,
};

/// Report entry for a run that did not measure projections.
pub(in crate::load) fn unrequested_projection_status() -> Value {
    json!({
        "status": "not_requested",
        "configured_keys": [],
        "durable_layout_verified": false,
        "coverage_verified": false,
        "interpretation": concat!(
            "Legacy-compatible fixture configuration; ",
            "no projection status command was requested and no covered-row claim is made."
        )
    })
}

/// Checks a projection status against the configured keys, the schema version, the row counts
/// and full coverage.
///
/// # Errors
///
/// Returns an error when the status is missing a count, or when any part of the layout or
/// coverage differs from what the configuration requires.
pub(in crate::load) fn verify_projection_status(
    args: &Args,
    status: &Value,
    expected_rows: u64,
) -> Result<()> {
    let mut configured = args.promoted_string_keys.clone();
    configured.sort();
    let actual: Vec<String> = serde_json::from_value(status["promoted_string_keys"].clone())?;
    let hot = status["hot_rows"]
        .as_u64()
        .ok_or("projection status missing hot row count")?;
    let archived = status["archived_rows"]
        .as_u64()
        .ok_or("projection status missing archive row count")?;
    let keys_match = actual == configured;
    let rows_match = hot + archived == expected_rows;
    let coverage_match = status["hot_covered_rows"].as_u64() == Some(hot)
        && status["archived_covered_rows"].as_u64() == Some(archived);
    let layout_match = status["schema_version"] == PROJECTION_SCHEMA_VERSION
        && status["legacy_archive_files"].as_u64() == Some(0);
    if !(keys_match && rows_match && coverage_match && layout_match) {
        return Err(
            format!("fixture durable projection layout or coverage mismatch: {status}").into(),
        );
    }

    Ok(())
}

/// A started fixture container and the seed that describes the data inside it.
pub(in crate::load) struct Prepared {
    pub(in crate::load) fixture: Fixture,
    pub(in crate::load) seed: Seed,
}

/// Prepares a fixture and starts it, leaving it ready for measurement.
pub(super) async fn prepare(args: &Args, from: i64) -> Result<Prepared> {
    let mut prepared = prepare_stopped(args, from).await?;
    prepared.fixture.start_again().await?;
    Ok(prepared)
}

/// Starts the fixture container with the target resources and stops it again.
async fn start_stopped_fixture(args: &Args) -> Result<Fixture> {
    let mut fixture = Fixture::start_config_with_resources(
        args.binary.clone(),
        args.image.clone(),
        &args.server_config(),
        Some(ResourceLimits {
            cpus: CONTAINER_CPUS,
            memory_mib: CONTAINER_MEMORY_MIB,
        }),
    )
    .await?;
    fixture.stop().await?;
    Ok(fixture)
}

/// Generates, imports and archives the fixture while its container is stopped.
///
/// # Errors
///
/// Returns an error when the container cannot start or stop, the fixture cannot be written,
/// or an offline import, archive or projection check fails.
pub(in crate::load) async fn prepare_stopped(args: &Args, from: i64) -> Result<Prepared> {
    let preparing = Instant::now();
    let fixture = start_stopped_fixture(args).await?;
    let _ = writeln!(
        io::stderr(),
        "attribute-load fixture stopped seconds={:.3}",
        preparing.elapsed().as_secs_f64()
    );
    let mut seed = write_seed(args, fixture.root.path(), from)?;
    let _ = writeln!(
        io::stderr(),
        "attribute-load fixture generated rows={} metadata_mean_bytes={:.3} seconds={:.3}",
        args.rows,
        seed.report["metadata_bytes"]["mean"]
            .as_f64()
            .unwrap_or(f64::NAN),
        seed.report["generation_seconds"]
            .as_f64()
            .unwrap_or(f64::NAN)
    );
    import_fixture(&fixture, args, &mut seed).await?;
    archive_fixture(&fixture, args, &mut seed).await?;

    let _ = writeln!(
        io::stderr(),
        "attribute-load fixture ready rows={} total_seconds={:.3}",
        args.rows,
        preparing.elapsed().as_secs_f64()
    );
    Ok(Prepared { fixture, seed })
}

/// Imports the generated NDJSON with the offline CLI and checks the imported row count.
async fn import_fixture(fixture: &Fixture, args: &Args, seed: &mut Seed) -> Result<()> {
    let import_path = if fixture.container.is_some() {
        PathBuf::from("/fixture/attributes.ndjson")
    } else {
        fixture.root.path().join("attributes.ndjson")
    };
    let started = Instant::now();
    let import_path = import_path.to_str().ok_or("non UTF-8 fixture path")?;
    let imported: Value = serde_json::from_str(
        &fixture
            .offline_command(
                &["import", import_path, "--source", "attributes", "--ndjson"],
                OFFLINE_COMMAND_SECS,
            )
            .await?,
    )?;
    if imported["imported"].as_u64() != Some(args.rows) {
        return Err("offline fixture import count mismatch".into());
    }

    seed.report["import_seconds"] = json!(started.elapsed().as_secs_f64());
    let _ = writeln!(
        io::stderr(),
        "attribute-load fixture imported rows={} seconds={:.3}",
        args.rows,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Archives the fixture window up to the configured percentage and records the projection.
async fn archive_fixture(fixture: &Fixture, args: &Args, seed: &mut Seed) -> Result<()> {
    let cutoff = archive_cutoff(seed.from, args.archive_percent);
    let started = Instant::now();
    let archived: Value = serde_json::from_str(
        &fixture
            .offline_command(
                &["archive", "--before", &cutoff.to_string()],
                OFFLINE_COMMAND_SECS,
            )
            .await?,
    )?;
    seed.report["archive_seconds"] = json!(started.elapsed().as_secs_f64());

    seed.report["archived_rows"] = archived["archived"].clone();
    seed.report["archive_cutoff"] = json!(cutoff);
    seed.report["projection_status"] = if args.promoted_string_keys.is_empty() {
        unrequested_projection_status()
    } else {
        projection_status(fixture, args).await?
    };
    let _ = writeln!(
        io::stderr(),
        "attribute-load fixture archived rows={} seconds={:.3}",
        archived["archived"],
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// One hundred percent: the denominator of the archive cutoff fraction.
const PERCENT: u64 = 100;

/// Epoch-millisecond cutoff that archives `percent` of the one-day window starting at `from`.
pub(in crate::load) fn archive_cutoff(from: i64, percent: u64) -> i64 {
    let span = i128::from(DAY_MS) * i128::from(percent) / i128::from(PERCENT);
    from + i64::try_from(span).expect("archive span is within one day")
}

/// Reads the durable projection status offline and verifies its layout and coverage.
async fn projection_status(fixture: &Fixture, args: &Args) -> Result<Value> {
    let status: Value = serde_json::from_str(
        &fixture
            .offline_command(&["projection-status"], OFFLINE_COMMAND_SECS)
            .await?,
    )?;
    verify_projection_status(args, &status, args.rows)?;
    Ok(status)
}
