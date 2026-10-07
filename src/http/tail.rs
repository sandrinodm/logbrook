use super::*;

fn tail_cursor_expired(error: &Error) -> bool {
    error.kind == ErrorKind::CursorExpired
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct TailFilters {
    source: Option<String>,
    service: Option<String>,
    logger: Option<String>,
    host: Option<String>,
    message: Option<String>,
    min_level: Option<i32>,
}

pub(super) async fn tail(
    Extension(app): Extension<App>,
    headers: HeaderMap,
    filters: Result<Params<TailFilters>, axum::extract::rejection::QueryRejection>,
) -> Result<Response, Error> {
    let Params(filters) = filters.map_err(|error| Error::invalid(error.body_text()))?;
    let allowed = scopes(&app, &headers)?;
    let sources: Vec<String> = filters
        .source
        .map(|v| v.split(',').map(str::to_owned).collect())
        .unwrap_or_else(|| allowed.clone());

    if sources.len() > 128
        || sources.iter().any(|source| {
            crate::config::validate_source(source).is_err()
                || (!allowed.is_empty() && !allowed.contains(source))
        })
    {
        return Err(Error::new(
            ErrorKind::Forbidden,
            "source outside read scope",
        ));
    }

    for field in [
        &filters.service,
        &filters.logger,
        &filters.host,
        &filters.message,
    ]
    .into_iter()
    .flatten()
    {
        if field.len() > app.config.max_field_bytes {
            return Err(Error::invalid("query field exceeds limit"));
        }
    }

    if filters.min_level.is_some_and(|v| v < 0) {
        return Err(Error::invalid("min_level must be nonnegative"));
    }

    let permit = app
        .tails
        .clone()
        .try_acquire_owned()
        .map_err(|_| overload(&app, "tail_subscribers", "tail subscriber limit exhausted"))?;
    let after = headers
        .get("last-event-id")
        .map(|v| v.to_str().map(str::to_owned))
        .transpose()
        .map_err(|_| Error::invalid("invalid Last-Event-ID"))?;
    let after = after
        .map(|cursor| decode_cursor(&app, &cursor))
        .transpose()?;

    if after
        .as_ref()
        .is_some_and(|v| v.len() > 20 || v.parse::<u64>().is_err())
    {
        return Err(Error::invalid("Last-Event-ID must be numeric"));
    }

    let query = Query {
        from: 0,
        to: 253_402_300_799_999,
        sources,
        service: filters.service,
        logger: filters.logger,
        host: filters.host,
        message: filters.message,
        min_level: filters.min_level,
        limit: app.config.max_page_size,
        cursor: None,
    };
    let commits = app.storage.subscribe_commits();
    let first = app
        .storage
        .tail_filtered_bounded_with_overhead(
            after,
            query.clone(),
            app.config.max_tail_buffer_bytes,
            app.index.as_ref().map_or(0, |name| name.len() + 1),
        )
        .await
        .map_err(|e| query_error(&app, e));

    if let Err(error) = &first
        && !tail_cursor_expired(error)
    {
        return Err(Error::new(error.kind, error.message.clone()));
    }

    // Pull one bounded page at a time. A full replay page is drained immediately;
    // downstream demand is the backpressure signal and no backlog size implies loss.
    let stream = futures_util::stream::unfold(
        (
            app,
            query,
            first,
            permit,
            std::collections::VecDeque::new(),
            false,
            false,
            false,
            commits,
        ),
        |(
            app,
            query,
            mut result,
            permit,
            mut pending,
            mut ended,
            mut wait,
            mut fetch,
            mut commits,
        )| async move {
            if ended && pending.is_empty() {
                return None;
            }

            if pending.is_empty() {
                if wait {
                    tokio::select! {
                        _ = commits.changed() => {},
                        _ = tokio::time::sleep(std::time::Duration::from_millis(app.config.tail_poll_ms)) => {},
                    }

                    if !app.storage.is_ready() {
                        return None;
                    }
                }

                if fetch {
                    let cursor = result.as_ref().ok().and_then(|p| p.next_cursor.clone());
                    result = app
                        .storage
                        .tail_filtered_bounded_with_overhead(
                            cursor,
                            query.clone(),
                            app.config.max_tail_buffer_bytes,
                            app.index.as_ref().map_or(0, |name| name.len() + 1),
                        )
                        .await;
                }

                match result {
                    Err(error) => {
                        ended = true;

                        if tail_cursor_expired(&error) || error.kind == ErrorKind::TooLarge {
                            app.tail_gaps.fetch_add(1, Ordering::Relaxed);
                        }

                        pending.push_back(
                            SseEvent::default()
                                .event(
                                    if tail_cursor_expired(&error)
                                        || error.kind == ErrorKind::TooLarge
                                    {
                                        "gap"
                                    } else {
                                        "error"
                                    },
                                )
                                .data(error.message),
                        );
                        result = Ok(crate::model::SearchResult {
                            events: vec![],
                            next_cursor: None,
                        });
                    }

                    Ok(ref page) => {
                        let mut bytes = 0usize;

                        for event in &page.events {
                            let data = serde_json::to_string(event).expect("event serialization");
                            bytes = bytes.saturating_add(
                                data.len()
                                    + event.id.len()
                                    + 32
                                    + app.index.as_ref().map_or(0, |name| name.len() + 1),
                            );

                            if bytes > app.config.max_tail_buffer_bytes {
                                ended = true;
                                app.tail_gaps.fetch_add(1, Ordering::Relaxed);
                                pending.clear();
                                pending.push_back(SseEvent::default().event("gap").data(
                                    "subscriber output byte budget exceeded; \
                         search retained history before reconnecting",
                                ));
                                break;
                            }

                            pending.push_back(
                                SseEvent::default()
                                    .event("log")
                                    .id(encode_cursor(&app, &event.id))
                                    .data(data),
                            );
                        }

                        wait = page.events.is_empty();
                        let cursor = page.next_cursor.clone();

                        if pending.is_empty() {
                            pending.push_back(
                                SseEvent::default()
                                    .event("cursor")
                                    .id(encode_cursor(&app, cursor.as_deref().unwrap_or("0")))
                                    .data("caught up"),
                            );
                        }

                        // Fetch only after the current page's frames have been consumed.
                    }
                }
            }

            let next = pending.pop_front().expect("SSE frame");

            if pending.is_empty() && !ended {
                fetch = true;
            }

            Some((
                Ok::<_, std::convert::Infallible>(next),
                (
                    app, query, result, permit, pending, ended, wait, fetch, commits,
                ),
            ))
        },
    );

    Ok(Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response())
}
