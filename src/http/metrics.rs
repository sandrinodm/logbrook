//! The Prometheus text endpoint: HTTP and ingestion counters, admission budgets, latency
//! histograms, and each index's storage diagnostics.

use super::{
    App, Arc, DEFAULT_INDEX_NAME, Error, ErrorKind, Extension, HeaderMap, IndexRegistry,
    IntoResponse, LATENCY_BUCKETS, LATENCY_COUNT_SLOT, LATENCY_LOCK, LATENCY_SUM_SLOT,
    OVERLOAD_LOCK, Ordering, Response, bearer, header, overload, scopes,
};

use crate::storage::Storage;

/// Latency is accumulated in microseconds and exposed in seconds.
const MICROS_PER_SECOND: f64 = 1_000_000.0;

/// One scalar series: its name, its HELP text, its Prometheus type, and its current value.
struct Sample {
    name: &'static str,
    help: &'static str,
    kind: &'static str,
    value: f64,
}

/// One index's storage, by the name its samples are labeled with.
struct Store {
    name: String,
    storage: Storage,
}

/// How storage samples are labeled: one storage for the whole process, or one per index.
#[derive(Clone, Copy)]
enum Labeling {
    Process,
    PerIndex,
}

/// Prometheus samples are 64-bit floats. Every count and size exposed here stays far below 2^53,
/// where the conversion is exact.
fn sample(value: u64) -> f64 {
    value as f64
}

/// `sample` for a count held as `usize`. A value that does not fit in 64 bits saturates.
fn sample_usize(value: usize) -> f64 {
    sample(u64::try_from(value).unwrap_or(u64::MAX))
}

/// Metrics reveal every index's footprint, so only an unrestricted read token may scrape them.
fn authorize_metrics(app: &App, headers: &HeaderMap) -> Result<(), Error> {
    let token = bearer(headers)?;
    if !scopes(app, headers)?.is_empty()
        || app
            .config
            .read_index_scopes
            .get(token)
            .is_some_and(|names| !names.is_empty())
    {
        return Err(Error::new(
            ErrorKind::Forbidden,
            "metrics require an unrestricted read token",
        ));
    }

    Ok(())
}

/// A monotonically increasing series.
fn counter(name: &'static str, help: &'static str, value: f64) -> Sample {
    Sample {
        name,
        help,
        kind: "counter",
        value,
    }
}

/// A series that can go up and down.
fn gauge(name: &'static str, help: &'static str, value: f64) -> Sample {
    Sample {
        name,
        help,
        kind: "gauge",
        value,
    }
}

/// The process-wide scalar series, in exposition order.
fn scalar_samples(app: &App, stores: &[Store]) -> Vec<Sample> {
    let committed_events = app.registry.as_ref().map_or_else(
        || app.storage.committed_events(),
        IndexRegistry::committed_events,
    );
    let all_ready = stores.iter().all(|store| store.storage.is_ready());
    let tail_subscribers = app.config.max_tail_subscribers - app.tails.available_permits();
    vec![
        counter(
            "logbrook_ingested_events_total",
            "Durably committed events",
            sample(committed_events),
        ),
        counter(
            "logbrook_ingest_rejected_total",
            "Rejected ingest requests",
            sample(app.rejected.load(Ordering::Relaxed)),
        ),
        gauge(
            "logbrook_ready",
            "Storage readiness",
            f64::from(u8::from(all_ready)),
        ),
        counter(
            "logbrook_http_requests_total",
            "HTTP requests",
            sample(app.total.load(Ordering::Relaxed)),
        ),
        counter(
            "logbrook_http_errors_total",
            "HTTP error responses",
            sample(app.failures.load(Ordering::Relaxed)),
        ),
        gauge(
            "logbrook_http_active_requests",
            "Active HTTP handlers",
            sample(app.active.load(Ordering::Relaxed)),
        ),
        gauge(
            "logbrook_tail_active_subscribers",
            "Active tail subscribers",
            sample_usize(tail_subscribers),
        ),
        counter(
            "logbrook_tail_gaps_total",
            "Tail gaps caused by expiry or byte overrun",
            sample(app.tail_gaps.load(Ordering::Relaxed)),
        ),
        counter(
            "logbrook_query_requests_total",
            "Query requests",
            sample(app.queries.load(Ordering::Relaxed)),
        ),
        counter(
            "logbrook_query_errors_total",
            "Query error responses",
            sample(app.query_errors.load(Ordering::Relaxed)),
        ),
        gauge(
            "logbrook_ingest_available_requests",
            "Available HTTP admission permits",
            sample_usize(app.requests.available_permits()),
        ),
        gauge(
            "logbrook_decoded_available_bytes",
            "Available decoded memory budget",
            sample_usize(app.bytes.available_permits()),
        ),
        gauge(
            "logbrook_raw_available_bytes",
            "Available wire body memory budget",
            sample_usize(app.raw_bytes.available_permits()),
        ),
    ]
}

/// Appends the HELP and TYPE lines and the value line of each scalar series.
fn push_scalar_samples(output: &mut String, samples: Vec<Sample>) {
    for Sample {
        name,
        help,
        kind,
        value,
    } in samples
    {
        output.push_str(&format!(
            "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}\n"
        ));
    }
}

/// Appends the cumulative latency histogram for every endpoint.
fn push_latency_histogram(output: &mut String, app: &App) {
    output.push_str(
        "# HELP logbrook_http_duration_seconds Handler latency by endpoint\n\
         # TYPE logbrook_http_duration_seconds histogram\n",
    );

    for (endpoint, values) in app.durations.lock().expect(LATENCY_LOCK).iter() {
        for (index, bucket) in LATENCY_BUCKETS.iter().enumerate() {
            let bound = bucket.le;
            output.push_str(&format!(
                "logbrook_http_duration_seconds_bucket{{endpoint=\"{endpoint}\",\
                 le=\"{bound}\"}} {}\n",
                values[index],
            ));
        }

        // The accumulator stores duration in microseconds; Prometheus uses seconds.
        output.push_str(&format!(
            "logbrook_http_duration_seconds_sum{{endpoint=\"{endpoint}\"}} {}\n\
             logbrook_http_duration_seconds_count{{endpoint=\"{endpoint}\"}} {}\n",
            sample(values[LATENCY_SUM_SLOT]) / MICROS_PER_SECOND,
            values[LATENCY_COUNT_SLOT],
        ));
    }
}

/// Appends the admission-rejection counter, one series per cause.
fn push_overload_counter(output: &mut String, app: &App) {
    output.push_str(
        "# HELP logbrook_http_overload_total Admission rejections by cause\n\
         # TYPE logbrook_http_overload_total counter\n",
    );

    for (cause, count) in app.overloads.lock().expect(OVERLOAD_LOCK).iter() {
        output.push_str(&format!(
            "logbrook_http_overload_total{{cause=\"{cause}\"}} {count}\n"
        ));
    }
}

/// Adds an `index` label to one storage sample line. Lines without a value are dropped.
fn label_with_index(line: &str, index: &str) -> Option<String> {
    let (name, value) = line.split_once(' ')?;
    Some(match name.split_once('{') {
        Some((metric, labels)) => format!("{metric}{{index=\"{index}\",{labels} {value}\n"),
        None => format!("{name}{{index=\"{index}\"}} {value}\n"),
    })
}

/// Each store's storage metrics. Per index, every sample is labeled with its index, and the HELP
/// and TYPE lines come from the first index, before any samples, so each family is declared once.
fn storage_metric_text(stores: Vec<Store>, labeling: Labeling) -> String {
    let mut output = String::new();

    for (position, Store { name, storage }) in stores.into_iter().enumerate() {
        let diagnostics = storage.metrics_prometheus();

        if matches!(labeling, Labeling::Process) {
            output.push_str(&diagnostics);
            continue;
        }

        for line in diagnostics.lines() {
            if line.starts_with('#') {
                if position == 0 {
                    output.push_str(line);
                    output.push('\n');
                }

                continue;
            }

            if let Some(labeled) = label_with_index(line, &name) {
                output.push_str(&labeled);
            }
        }
    }

    output
}

/// Serves the Prometheus exposition for the process and its indexes.
pub(super) async fn metrics(
    Extension(app): Extension<App>,
    headers: HeaderMap,
) -> Result<Response, Error> {
    authorize_metrics(&app, &headers)?;

    let labeling = if app.registry.is_some() {
        Labeling::PerIndex
    } else {
        Labeling::Process
    };
    let stores: Vec<Store> = app.registry.as_ref().map_or_else(
        || {
            vec![Store {
                name: DEFAULT_INDEX_NAME.to_owned(),
                storage: app.storage.clone(),
            }]
        },
        |registry| {
            registry
                .list()
                .into_iter()
                .map(|handle| Store {
                    name: handle.name,
                    storage: handle.storage,
                })
                .collect()
        },
    );

    let mut output = String::new();
    push_scalar_samples(&mut output, scalar_samples(&app, &stores));
    push_latency_histogram(&mut output, &app);
    push_overload_counter(&mut output, &app);

    // Blocking work outlives a cancelled HTTP handler. Keep a dedicated permit
    // inside that work so disconnects cannot create an unbounded diagnostics queue.
    let permit = Arc::clone(&app.metrics_jobs)
        .try_acquire_owned()
        .map_err(|error| {
            overload(
                &app,
                "metrics_capacity",
                "metrics diagnostics already running",
            )
            .with_source(error)
        })?;

    let diagnostics = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        storage_metric_text(stores, labeling)
    })
    .await
    .map_err(|error| Error::unavailable("metrics worker failed").with_source(error))?;
    output.push_str(&diagnostics);
    output.push_str(&crate::forward::metrics::prometheus());

    Ok((
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        output,
    )
        .into_response())
}
