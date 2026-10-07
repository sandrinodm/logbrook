use super::*;

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct Filters {
    from: Option<i64>,
    to: Option<i64>,
    source: Option<String>,
    service: Option<String>,
    logger: Option<String>,
    host: Option<String>,
    message: Option<String>,
    min_level: Option<i32>,
    limit: Option<usize>,
    cursor: Option<String>,
    interval_ms: Option<i64>,
}

macro_rules! endpoint_filters {
    ($name:ident $(, $extra:ident : $kind:ty)*) => {
        #[derive(Deserialize, Default)]
        #[serde(deny_unknown_fields)]
        pub(super) struct $name {
            from: Option<i64>, to: Option<i64>, source: Option<String>,
            service: Option<String>, logger: Option<String>, host: Option<String>,
            message: Option<String>, min_level: Option<i32>,
            $( $extra: Option<$kind>, )*
        }

        impl From<$name> for Filters {
            fn from(value: $name) -> Self {
                Self { from: value.from, to: value.to, source: value.source,
                    service: value.service, logger: value.logger, host: value.host,
                    message: value.message, min_level: value.min_level,
                    $( $extra: value.$extra, )* ..Self::default() }
            }
        }
    }
}

endpoint_filters!(SearchFilters, limit: usize, cursor: String);
endpoint_filters!(AggregateFilters);
endpoint_filters!(HistogramFilters, interval_ms: i64);
pub(super) fn query(
    app: &App,
    headers: &HeaderMap,
    filters: Filters,
) -> Result<(Query, i64), Error> {
    let allowed = scopes(app, headers)?;
    let from = filters
        .from
        .ok_or_else(|| Error::invalid("from is required (Unix milliseconds)"))?;
    let to = filters
        .to
        .ok_or_else(|| Error::invalid("to is required (Unix milliseconds)"))?;

    if from < 0
        || to <= from
        || to > 253_402_300_799_999
        || to
            .checked_sub(from)
            .is_none_or(|range| range > app.config.max_query_range_ms)
    {
        return Err(Error::invalid("invalid or excessive half-open time range"));
    }

    let sources: Vec<String> = filters
        .source
        .map(|value| value.split(',').map(str::to_owned).collect())
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

    let limit = filters.limit.unwrap_or(100.min(app.config.max_page_size));

    if limit == 0 || limit > app.config.max_page_size {
        return Err(Error::invalid("invalid page limit"));
    }

    for value in [
        &filters.service,
        &filters.logger,
        &filters.host,
        &filters.message,
        &filters.cursor,
    ]
    .into_iter()
    .flatten()
    {
        if value.len() > app.config.max_field_bytes {
            return Err(Error::invalid("query field exceeds limit"));
        }
    }

    if filters.min_level.is_some_and(|level| level < 0) {
        return Err(Error::invalid("min_level must be nonnegative"));
    }

    let interval = filters.interval_ms.unwrap_or(60_000.max(
        (to - from + crate::model::MAX_HISTOGRAM_BUCKETS - 1) / crate::model::MAX_HISTOGRAM_BUCKETS,
    ));

    if interval <= 0 || (to - from - 1) / interval + 1 > crate::model::MAX_HISTOGRAM_BUCKETS {
        return Err(Error::invalid(
            "invalid histogram interval or too many buckets",
        ));
    }

    Ok((
        Query {
            from,
            to,
            sources,
            service: filters.service,
            logger: filters.logger,
            host: filters.host,
            message: filters.message,
            min_level: filters.min_level,
            limit,
            cursor: filters
                .cursor
                .map(|cursor| decode_cursor(app, &cursor))
                .transpose()?,
        },
        interval,
    ))
}
