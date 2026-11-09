//! Query-string parsing for the log endpoints, and the checks that turn parameters into a `Query`.

use super::{App, Deserialize, Error, ErrorKind, HeaderMap, Query, cursor_scope, scopes};

use crate::{
    config::validate_source,
    model::MAX_HISTOGRAM_BUCKETS,
    query_cursor::{decode_search, validate_token_size},
};

/// The latest timestamp a query may name: the end of year 9999, in Unix milliseconds.
const MAX_QUERY_TIMESTAMP_MS: i64 = 253_402_300_799_999;

/// The page size used when a request names no limit, before the configured cap applies.
const DEFAULT_PAGE_SIZE: usize = 100;

/// The histogram bucket width used when a request names no interval, before the bucket cap.
const DEFAULT_HISTOGRAM_INTERVAL_MS: i64 = 60_000;

/// The most sources one query may name.
const MAX_SOURCES_PER_QUERY: usize = 128;

/// A validated search query, and the histogram interval it was requested with.
pub(super) struct LogQuery {
    pub(super) query: Query,
    pub(super) interval_ms: i64,
}

/// The query parameters every log endpoint understands, before endpoint-specific ones are added.
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
    filter: Option<String>,
    limit: Option<usize>,
    cursor: Option<String>,
    interval_ms: Option<i64>,
}

/// Declares a per-endpoint query-string struct with the shared filters plus the listed extra
/// fields, and converts it into `Filters`. Unknown query parameters are rejected by serde.
macro_rules! endpoint_filters {
    ($name:ident $(, $extra:ident : $kind:ty)*) => {
        #[derive(Deserialize, Default)]
        #[serde(deny_unknown_fields)]
        pub(super) struct $name {
            from: Option<i64>,
            to: Option<i64>,
            source: Option<String>,
            service: Option<String>,
            logger: Option<String>,
            host: Option<String>,
            message: Option<String>,
            min_level: Option<i32>,
            filter: Option<String>,
            $( $extra: Option<$kind>, )*
        }

        impl From<$name> for Filters {
            fn from(value: $name) -> Self {
                Self {
                    from: value.from,
                    to: value.to,
                    source: value.source,
                    service: value.service,
                    logger: value.logger,
                    host: value.host,
                    message: value.message,
                    min_level: value.min_level,
                    filter: value.filter,
                    $( $extra: value.$extra, )*
                    ..Self::default()
                }
            }
        }
    };
}

endpoint_filters!(SearchFilters, limit: usize, cursor: String);
endpoint_filters!(AggregateFilters);
endpoint_filters!(HistogramFilters, interval_ms: i64);

/// The query parameters a live tail accepts. Paging and histogram parameters do not apply.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct TailFilters {
    source: Option<String>,
    service: Option<String>,
    logger: Option<String>,
    host: Option<String>,
    message: Option<String>,
    min_level: Option<i32>,
    filter: Option<String>,
}

impl From<TailFilters> for Filters {
    fn from(value: TailFilters) -> Self {
        Self {
            source: value.source,
            service: value.service,
            logger: value.logger,
            host: value.host,
            message: value.message,
            min_level: value.min_level,
            filter: value.filter,
            ..Self::default()
        }
    }
}

/// Builds the search `Query` for a search, count, facet or histogram request. The histogram
/// interval is ignored by the other endpoints.
///
/// # Errors
///
/// Fails when the time range, page limit or interval is out of bounds, a cursor does not match
/// the query, or any shared filter is invalid.
pub(super) fn query(app: &App, headers: &HeaderMap, filters: Filters) -> Result<LogQuery, Error> {
    let from = filters
        .from
        .ok_or_else(|| Error::invalid("from is required (Unix milliseconds)"))?;
    let to = filters
        .to
        .ok_or_else(|| Error::invalid("to is required (Unix milliseconds)"))?;
    if !time_range_valid(app, from, to) {
        return Err(Error::invalid("invalid or excessive half-open time range"));
    }

    let limit = filters
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE.min(app.config.max_page_size));
    if limit == 0 || limit > app.config.max_page_size {
        return Err(Error::invalid("invalid page limit"));
    }

    // The span is positive because `time_range_valid` requires `to > from`.
    let span = to - from;
    let smallest_fitting_interval = (span + MAX_HISTOGRAM_BUCKETS - 1) / MAX_HISTOGRAM_BUCKETS;
    let interval = filters
        .interval_ms
        .unwrap_or(DEFAULT_HISTOGRAM_INTERVAL_MS.max(smallest_fitting_interval));
    if interval <= 0 || (span - 1) / interval + 1 > MAX_HISTOGRAM_BUCKETS {
        return Err(Error::invalid(
            "invalid histogram interval or too many buckets",
        ));
    }

    let mut query = common_query(app, headers, filters)?;
    query.from = from;
    query.to = to;
    query.limit = limit;
    if let Some(cursor) = &query.cursor {
        decode_search(&query, cursor)?;
    }

    Ok(LogQuery {
        query,
        interval_ms: interval,
    })
}

/// A half-open `[from, to)` range must be ordered, lie within the supported calendar, and be no
/// wider than the configured query range.
fn time_range_valid(app: &App, from: i64, to: i64) -> bool {
    let ordered = from >= 0 && to > from && to <= MAX_QUERY_TIMESTAMP_MS;
    let within_limit = to
        .checked_sub(from)
        .is_some_and(|range| range <= app.config.max_query_range_ms);
    ordered && within_limit
}

/// The filters shared by every query, with the token's source scope applied.
///
/// # Errors
///
/// Fails when a source is outside the token's scope, a text field or cursor is too long, the
/// minimum level is negative, or the attribute filter is invalid.
pub(super) fn common_query(
    app: &App,
    headers: &HeaderMap,
    filters: Filters,
) -> Result<Query, Error> {
    let allowed = scopes(app, headers)?;
    let mut sources: Vec<String> = filters
        .source
        .map(|value| value.split(',').map(str::to_owned).collect())
        .unwrap_or_else(|| allowed.clone());
    if !sources_in_scope(&sources, &allowed) {
        return Err(Error::new(
            ErrorKind::Forbidden,
            "source outside read scope",
        ));
    }

    sources.sort();
    sources.dedup();

    for value in [
        &filters.service,
        &filters.logger,
        &filters.host,
        &filters.message,
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

    if let Some(cursor) = &filters.cursor {
        validate_token_size(cursor)?;
    }

    let attributes = filters
        .filter
        .map(|filter| {
            crate::attribute_filter::AttributeFilters::parse(&filter, app.config.max_field_bytes)
        })
        .transpose()?
        .unwrap_or_default();

    Ok(Query {
        from: 0,
        to: MAX_QUERY_TIMESTAMP_MS,
        sources,
        service: filters.service,
        logger: filters.logger,
        host: filters.host,
        message: filters.message,
        min_level: filters.min_level,
        attributes,
        cursor_scope: cursor_scope(app),
        limit: app.config.max_page_size,
        cursor: filters.cursor,
    })
}

/// The requested sources are a bounded list of valid names. An empty token scope grants all.
fn sources_in_scope(sources: &[String], allowed: &[String]) -> bool {
    sources.len() <= MAX_SOURCES_PER_QUERY
        && sources.iter().all(|source| {
            validate_source(source).is_ok() && (allowed.is_empty() || allowed.contains(source))
        })
}
