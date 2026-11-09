//! Deterministic fixture generation and independent response oracles.
use super::{
    Args,
    constants::{
        ACTORS_PER_RUN, ERROR_LEVEL, FACET_DIMENSIONS, FIXTURE_SEED, HISTOGRAM_INTERVAL_MS,
        HOST_COUNT, INFO_LEVEL, INGEST_ORDER_OFFSET, INGEST_ORDER_STRIDE, ORACLE_RETAINED,
        ORACLE_TRIM_THRESHOLD, SEARCH_PAGE_LIMIT, VIDEOS_PER_ACTOR,
    },
};
use crate::{Result, load::semantics::DAY_MS};
use serde_json::{Map, Value, json};
use std::{
    collections::BTreeMap,
    io::{BufWriter, Write},
    path::Path,
    time::{Duration, Instant},
};

/// One event's place in the descending order of an oracle's first events.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Position {
    pub(super) time: i64,
    pub(super) id: u64,
}

/// The selection an oracle entry answers for: one run, optionally narrowed to one actor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Scope {
    run: u64,
    actor: Option<u64>,
}

impl Scope {
    pub(super) fn new(run: u64, actor: Option<u64>) -> Self {
        Self { run, actor }
    }
}

/// Independently computed answers for one scope of the fixture.
#[derive(Clone, Default)]
pub(super) struct Expected {
    pub(super) count: u64,
    // Descending event-time / ID order, trimmed after seed construction.
    first: Vec<Position>,
    histogram: BTreeMap<i64, u64>,
    facets: BTreeMap<String, BTreeMap<String, u64>>,
}

/// Fixture metadata and the independent oracle that every response is checked against.
#[derive(Clone)]
pub struct Seed {
    /// Start of the preloaded window, in epoch milliseconds.
    pub from: i64,
    /// End of the preloaded window, one day after `from`, in epoch milliseconds.
    pub to: i64,
    /// Number of events in the fixture.
    pub rows: u64,
    /// Number of runs in the fixture.
    pub runs: u64,
    pub(super) expected: BTreeMap<Scope, Expected>,
    /// Generation and import provenance written into the report.
    pub report: Value,
}

/// Returns the attribute filter that selects one run, narrowed to one actor when given.
pub fn selection(run: u64, actor: Option<u64>) -> String {
    let mut conditions = vec![json!({"key":"runId","op":"eq","value":format!("run-{run:04}")})];
    if let Some(actor) = actor {
        conditions.push(json!({"key":"actorJobId","op":"eq","value":format!("actor-{actor:02}")}));
    }
    json!({"attributes":conditions}).to_string()
}

/// Query-string parameters for a search over the fixture window and one selection.
pub fn query_params(seed: &Seed, run: u64, actor: Option<u64>) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("from".into(), seed.from.to_string()),
        ("to".into(), seed.to.to_string()),
        ("source".into(), "attributes".into()),
        ("filter".into(), selection(run, actor)),
    ])
}

/// Where one generated event sits in the run, actor and video grid.
struct Coordinate {
    run: u64,
    actor: u64,
    video: u64,
}

/// Maps the event ordinal `n` onto the run, actor and video grid.
fn coordinate(args: &Args, n: u64) -> Coordinate {
    let ordinal = n / args.runs;
    Coordinate {
        run: n % args.runs,
        actor: ordinal % ACTORS_PER_RUN,
        video: (ordinal / ACTORS_PER_RUN) % VIDEOS_PER_ACTOR,
    }
}

/// Returns the object fields of a JSON object literal built with `json!`.
fn literal_object(value: Value) -> Map<String, Value> {
    let Value::Object(object) = value else {
        unreachable!("json! object literal")
    };
    object
}

/// Builds fixture event `n`, with its time inside the one-day window that starts at `from`.
///
/// Pino input fields are top-level so ingestion retains the identifiers directly.
pub fn make_event(args: &Args, n: u64, from: i64) -> Value {
    let Coordinate { run, actor, video } = coordinate(args, n);
    let mut attributes = json!({
        "runId": format!("run-{run:04}"),
        "actorJobId": format!("actor-{actor:02}"),
        "videoId": format!("video-{video:02}"),
        "sequence": n,
        "active": true,
        "tag0": "v0",
        "tag1": "v1",
        "tag2": "v2",
        "tag3": "v3",
        "tag4": "v4",
        "tag5": "v5",
        "tag6": "v6",
        "tag7": "v7",
        "tag8": "v8",
        "tag9": "v9",
        "tag10": "v10",
        "padding": ""
    });
    // Exact byte target where fixed fields fit; metadata256 may be slightly
    // larger. Report observed sizes rather than claim the requested padding.
    let base = attributes.to_string().len();
    attributes["padding"] = json!("x".repeat(args.metadata_bytes.saturating_sub(base)));

    let mut object = literal_object(attributes);
    // Tied event times and a deterministic ingestion-order permutation.
    let rank = (n * INGEST_ORDER_STRIDE + INGEST_ORDER_OFFSET) % args.rows;
    let offset = i128::from(DAY_MS) * i128::from(rank / 2) / i128::from(args.rows.div_ceil(2));
    let time = from + i64::try_from(offset).expect("event time stays inside the one-day window");
    let level = if video == 0 { ERROR_LEVEL } else { INFO_LEVEL };
    let service = if actor.is_multiple_of(2) {
        "api"
    } else {
        "worker"
    };
    object.extend(literal_object(json!({
        "time": time,
        "level": level,
        "service": service,
        "name": "asset-downloader",
        "hostname": format!("host-{}", run % HOST_COUNT),
        "msg": format!("attribute fixture {n}")
    })));
    Value::Object(object)
}

/// Keeps only the newest events of an oracle entry once it has grown past the threshold.
fn trim_first(expected: &mut Expected) {
    expected.first.sort_unstable_by(|a, b| b.cmp(a));
    expected.first.truncate(ORACLE_RETAINED);
}

/// Adds one generated event to an oracle entry.
fn record(expected: &mut Expected, event: &Value, id: u64, from: i64) {
    expected.count += 1;
    let time = event["time"].as_i64().expect("fixture time");
    expected.first.push(Position { time, id });

    // Keep the oracle bounded for ten-million-row scale fixtures.
    if expected.first.len() > ORACLE_TRIM_THRESHOLD {
        trim_first(expected);
    }

    let bucket = from + (time - from) / HISTOGRAM_INTERVAL_MS * HISTOGRAM_INTERVAL_MS;
    *expected.histogram.entry(bucket).or_default() += 1;

    for (dimension, value) in facet_values(event) {
        *expected
            .facets
            .entry(dimension.into())
            .or_default()
            .entry(value)
            .or_default() += 1;
    }
}

/// The facet dimension and value that one event contributes to each facet response.
fn facet_values(event: &Value) -> [(&'static str, String); 5] {
    [
        (
            "services",
            event["service"]
                .as_str()
                .expect("fixture service")
                .to_owned(),
        ),
        ("loggers", "asset-downloader".into()),
        (
            "hosts",
            event["hostname"].as_str().expect("fixture host").to_owned(),
        ),
        ("levels", event["level"].to_string()),
        ("sources", "attributes".into()),
    ]
}

/// Totals gathered while the NDJSON fixture is written.
struct WriteStats {
    bytes: u64,
    metadata_total: u64,
    metadata_minimum: usize,
    metadata_maximum: usize,
}

impl WriteStats {
    fn new() -> Self {
        Self {
            bytes: 0,
            metadata_total: 0,
            metadata_minimum: usize::MAX,
            metadata_maximum: 0,
        }
    }

    /// Records the metadata size of one event.
    fn add_metadata(&mut self, bytes: usize) {
        self.metadata_total += len_u64(bytes);
        self.metadata_minimum = self.metadata_minimum.min(bytes);
        self.metadata_maximum = self.metadata_maximum.max(bytes);
    }
}

/// Widens a length to `u64`; every `usize` length fits.
pub(in crate::load) fn len_u64(length: usize) -> u64 {
    u64::try_from(length).expect("usize length fits in u64")
}

/// Mean of `total` over `count`, as the report prints it.
fn mean(total: u64, count: u64) -> f64 {
    // Lossy only above 2^53 events, far beyond any fixture size.
    total as f64 / count as f64
}

/// Generates the fixture NDJSON in `root`, recording the oracle and the write totals.
pub(super) fn write_seed(args: &Args, root: &Path, from: i64) -> Result<Seed> {
    let started = Instant::now();
    let mut seed = Seed {
        from,
        to: from + DAY_MS,
        rows: args.rows,
        runs: args.runs,
        expected: BTreeMap::new(),
        report: Value::Null,
    };
    let mut file = BufWriter::new(std::fs::File::create(root.join("attributes.ndjson"))?);
    let stats = write_events(args, from, &mut seed, &mut file)?;
    file.flush()?;

    for expected in seed.expected.values_mut() {
        trim_first(expected);
    }

    seed.report = seed_report(args, &seed, &stats, started.elapsed());
    Ok(seed)
}

/// Writes every event as one NDJSON line and records it in each oracle scope it belongs to.
fn write_events(
    args: &Args,
    from: i64,
    seed: &mut Seed,
    file: &mut impl Write,
) -> Result<WriteStats> {
    let mut stats = WriteStats::new();
    for n in 0..args.rows {
        let event = make_event(args, n, from);
        let Coordinate { run, actor, .. } = coordinate(args, n);
        for scope in [
            Scope::new(run, None),
            Scope::new(run, Some(actor)),
            Scope::new(u64::MAX, None),
        ] {
            record(seed.expected.entry(scope).or_default(), &event, n + 1, from);
        }

        let mut attributes = event.as_object().expect("fixture object").clone();
        for key in ["time", "level", "service", "name", "hostname", "msg"] {
            attributes.remove(key);
        }
        stats.add_metadata(serde_json::to_vec(&attributes)?.len());

        let line = serde_json::to_vec(&event)?;
        stats.bytes += len_u64(line.len()) + 1;
        file.write_all(&line)?;
        file.write_all(b"\n")?;
    }

    Ok(stats)
}

/// The generation section of the report: sizes, timings and the error distribution.
fn seed_report(args: &Args, seed: &Seed, stats: &WriteStats, generation: Duration) -> Value {
    json!({
        "seed": FIXTURE_SEED,
        "rows": args.rows,
        "runs": args.runs,
        "actors_per_run": ACTORS_PER_RUN,
        "videos_per_actor": VIDEOS_PER_ACTOR,
        "window_ms": DAY_MS,
        "from": seed.from,
        "to": seed.to,
        "ndjson_bytes": stats.bytes,
        "metadata_bytes": {
            "min": stats.metadata_minimum,
            "max": stats.metadata_maximum,
            "mean": mean(stats.metadata_total, args.rows)
        },
        "generation_seconds": generation.as_secs_f64(),
        "error_distribution": "ordinal / 10 modulo 10 = 0; exactly 10% per run for 1000 events/run"
    })
}

/// Kinds of read operation the workload measures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::load) enum Operation {
    Search,
    Count,
    Histogram,
    Facets,
}

impl Operation {
    /// Parses an operation name as the schedule and the report spell it.
    pub(in crate::load) fn parse(name: &str) -> Option<Self> {
        match name {
            "search" => Some(Self::Search),
            "count" => Some(Self::Count),
            "histogram" => Some(Self::Histogram),
            "facets" => Some(Self::Facets),
            _ => None,
        }
    }

    /// The operation name used in schedules and reports.
    pub(in crate::load) fn name(self) -> &'static str {
        match self {
            Self::Search => "search",
            Self::Count => "count",
            Self::Histogram => "histogram",
            Self::Facets => "facets",
        }
    }

    /// The HTTP endpoint that serves this operation.
    pub(in crate::load) fn endpoint(self) -> &'static str {
        match self {
            Self::Search => "/logs",
            Self::Count => "/logs/count",
            Self::Histogram => "/logs/histogram",
            Self::Facets => "/logs/facets",
        }
    }
}

/// Checks one response against the independent oracle for `operation` over `(run, actor)`.
///
/// # Errors
///
/// Returns an error when `operation` is unknown, the payload is malformed, or it differs from
/// the oracle.
pub fn verify_response(
    seed: &Seed,
    operation: &str,
    run: u64,
    actor: Option<u64>,
    payload: &Value,
) -> Result<()> {
    let empty = Expected::default();
    let expected = seed.expected.get(&Scope::new(run, actor)).unwrap_or(&empty);
    match Operation::parse(operation).ok_or("unknown attribute benchmark operation")? {
        Operation::Search => verify_search(expected, payload),
        Operation::Count => verify_count(expected, payload),
        Operation::Histogram => verify_histogram(expected, payload),
        Operation::Facets => verify_facets(expected, payload),
    }
}

/// Checks a search response: the IDs in order and the continuation for large results.
fn verify_search(expected: &Expected, payload: &Value) -> Result<()> {
    let events = payload["events"]
        .as_array()
        .ok_or("search omitted events")?;
    let actual: Vec<_> = events
        .iter()
        .map(|event| {
            event["id"]
                .as_str()
                .and_then(|id| id.parse::<u64>().ok())
                .ok_or("invalid event ID")
        })
        .collect::<std::result::Result<_, _>>()?;
    let expected_ids: Vec<u64> = expected.first.iter().map(|position| position.id).collect();
    if actual != expected_ids {
        return Err("search IDs differ from independent fixture oracle".into());
    }

    if expected.count > SEARCH_PAGE_LIMIT && !payload["next_cursor"].is_string() {
        return Err("search omitted continuation".into());
    }
    Ok(())
}

/// Checks a count response against the oracle's total.
fn verify_count(expected: &Expected, payload: &Value) -> Result<()> {
    if payload["count"].as_u64() != Some(expected.count) {
        return Err("count differs from independent fixture oracle".into());
    }
    Ok(())
}

/// Checks a histogram response bucket by bucket against the oracle.
fn verify_histogram(expected: &Expected, payload: &Value) -> Result<()> {
    let actual: BTreeMap<_, _> = payload["buckets"]
        .as_array()
        .ok_or("histogram omitted buckets")?
        .iter()
        .map(|bucket| {
            Ok((
                bucket["time"].as_i64().ok_or("histogram omitted time")?,
                bucket["count"].as_u64().ok_or("histogram omitted count")?,
            ))
        })
        .collect::<Result<_>>()?;
    if actual != expected.histogram {
        return Err("histogram differs from independent fixture oracle".into());
    }
    Ok(())
}

/// Checks every facet dimension of a facets response against the oracle.
fn verify_facets(expected: &Expected, payload: &Value) -> Result<()> {
    for dimension in FACET_DIMENSIONS {
        let empty = BTreeMap::new();
        let expected = expected.facets.get(dimension).unwrap_or(&empty);
        let actual: BTreeMap<_, _> = payload[dimension]
            .as_array()
            .ok_or("facets omitted dimension")?
            .iter()
            .map(|entry| {
                Ok((
                    entry["value"]
                        .as_str()
                        .ok_or("facet omitted value")?
                        .to_owned(),
                    entry["count"].as_u64().ok_or("facet omitted count")?,
                ))
            })
            .collect::<Result<_>>()?;
        if &actual != expected {
            return Err(format!("{dimension} differs from independent fixture oracle").into());
        }
    }
    Ok(())
}

/// Checks a response to an unfiltered request against the oracle's whole-fixture answer.
///
/// # Errors
///
/// Returns the same errors as [`verify_response`].
pub fn verify_unfiltered_response(seed: &Seed, operation: &str, payload: &Value) -> Result<()> {
    verify_response(seed, operation, u64::MAX, None, payload)
}
