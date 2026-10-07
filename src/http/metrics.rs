use super::*;

pub(super) async fn metrics(
    Extension(app): Extension<App>,
    headers: HeaderMap,
) -> Result<Response, Error> {
    let token = bearer(&headers)?;

    if !scopes(&app, &headers)?.is_empty()
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

    let indexed = app.registry.is_some();
    let stores: Vec<_> = app.registry.as_ref().map_or_else(
        || vec![("default".to_owned(), app.storage.clone())],
        |registry| {
            registry
                .list()
                .into_iter()
                .map(|handle| (handle.name, handle.storage))
                .collect()
        },
    );
    let mut output = String::new();

    for (name, help, kind, value) in [
        (
            "logbrook_ingested_events_total",
            "Durably committed events",
            "counter",
            app.registry.as_ref().map_or_else(
                || app.storage.committed_events(),
                IndexRegistry::committed_events,
            ) as f64,
        ),
        (
            "logbrook_ingest_rejected_total",
            "Rejected ingest requests",
            "counter",
            app.rejected.load(Ordering::Relaxed) as f64,
        ),
        (
            "logbrook_ready",
            "Storage readiness",
            "gauge",
            u8::from(stores.iter().all(|(_, storage)| storage.is_ready())) as f64,
        ),
        (
            "logbrook_http_requests_total",
            "HTTP requests",
            "counter",
            app.total.load(Ordering::Relaxed) as f64,
        ),
        (
            "logbrook_http_errors_total",
            "HTTP error responses",
            "counter",
            app.failures.load(Ordering::Relaxed) as f64,
        ),
        (
            "logbrook_http_active_requests",
            "Active HTTP handlers",
            "gauge",
            app.active.load(Ordering::Relaxed) as f64,
        ),
        (
            "logbrook_tail_active_subscribers",
            "Active tail subscribers",
            "gauge",
            (app.config.max_tail_subscribers - app.tails.available_permits()) as f64,
        ),
        (
            "logbrook_tail_gaps_total",
            "Tail gaps caused by expiry or byte overrun",
            "counter",
            app.tail_gaps.load(Ordering::Relaxed) as f64,
        ),
        (
            "logbrook_query_requests_total",
            "Query requests",
            "counter",
            app.queries.load(Ordering::Relaxed) as f64,
        ),
        (
            "logbrook_query_errors_total",
            "Query error responses",
            "counter",
            app.query_errors.load(Ordering::Relaxed) as f64,
        ),
        (
            "logbrook_ingest_available_requests",
            "Available HTTP admission permits",
            "gauge",
            app.requests.available_permits() as f64,
        ),
        (
            "logbrook_decoded_available_bytes",
            "Available decoded memory budget",
            "gauge",
            app.bytes.available_permits() as f64,
        ),
        (
            "logbrook_raw_available_bytes",
            "Available wire body memory budget",
            "gauge",
            app.raw_bytes.available_permits() as f64,
        ),
    ] {
        output.push_str(&format!(
            "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}\n"
        ));
    }

    output.push_str(
        "# HELP logbrook_http_duration_seconds Handler latency by endpoint\n\
         # TYPE logbrook_http_duration_seconds histogram\n",
    );

    for (endpoint, values) in app.durations.lock().unwrap().iter() {
        for (index, bound) in ["0.001", "0.01", "0.1", "0.5", "1", "5", "+Inf"]
            .iter()
            .enumerate()
        {
            output.push_str(&format!(
                "logbrook_http_duration_seconds_bucket{{endpoint=\"{endpoint}\",le=\"{bound}\"}} {}\n",
                values[index],
            ));
        }

        // The accumulator stores duration in microseconds; Prometheus uses seconds.
        output.push_str(&format!(
            "logbrook_http_duration_seconds_sum{{endpoint=\"{endpoint}\"}} {}\n\
             logbrook_http_duration_seconds_count{{endpoint=\"{endpoint}\"}} {}\n",
            values[7] as f64 / 1_000_000.0,
            values[8],
        ));
    }

    output.push_str(
        "# HELP logbrook_http_overload_total Admission rejections by cause\n\
         # TYPE logbrook_http_overload_total counter\n",
    );

    for (cause, count) in app.overloads.lock().unwrap().iter() {
        output.push_str(&format!(
            "logbrook_http_overload_total{{cause=\"{cause}\"}} {count}\n"
        ));
    }

    // Blocking work outlives a cancelled HTTP handler. Keep a dedicated permit
    // inside that work so disconnects cannot create an unbounded diagnostics queue.
    let permit = app.metrics_jobs.clone().try_acquire_owned().map_err(|_| {
        overload(
            &app,
            "metrics_capacity",
            "metrics diagnostics already running",
        )
    })?;

    let diagnostics = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut output = String::new();

        for (index, storage) in stores {
            let diagnostics = storage.metrics_prometheus();

            if !indexed {
                output.push_str(&diagnostics);
                continue;
            }

            for line in diagnostics.lines() {
                if line.starts_with('#') {
                    if index == "default" {
                        output.push_str(line);
                        output.push('\n');
                    }

                    continue;
                }

                if let Some((name, value)) = line.split_once(' ') {
                    if let Some((metric, labels)) = name.split_once('{') {
                        output.push_str(&format!("{metric}{{index=\"{index}\",{labels} {value}\n"));
                    } else {
                        output.push_str(&format!("{name}{{index=\"{index}\"}} {value}\n"));
                    }
                }
            }
        }

        output
    })
    .await
    .map_err(|_| Error::unavailable("metrics worker failed"))?;
    output.push_str(&diagnostics);

    Ok((
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        output,
    )
        .into_response())
}
