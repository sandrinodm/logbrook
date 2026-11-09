//! Remote commands do not load server configuration or touch local storage.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::{Args, Subcommand};
use logbrook::model::Error;
use serde_json::Value;

/// Largest response body the client reads, in bytes. The server's own limits are far below it.
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// Longest server error message shown to the operator, in characters.
const MAX_ERROR_DETAIL_CHARS: usize = 512;

/// Connection setup gets at most this long, even when the request deadline is longer.
const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Server contacted when neither `--url` nor `LOGBROOK_URL` is set: the server's default bind.
const DEFAULT_SERVER_URL: &str = "http://127.0.0.1:3100";

/// Default `--timeout`, in seconds, for a whole remote request including the response body.
const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Longest `--timeout` accepted, in seconds.
const MAX_TIMEOUT_SECS: u64 = 300;

/// Default `--limit` for `query`: events per page.
const DEFAULT_LIMIT: u16 = 100;

/// Largest `--limit` accepted, the same as the server's `MAX_PAGE_SIZE`.
const MAX_LIMIT: i64 = 1_000;

/// Look-back window for `query` and `count` when no `--since` or `--from`/`--to` is given.
const DEFAULT_SINCE: &str = "1h";

// Millisecond multipliers for the `--since` suffixes `s`, `m`, `h`, `d` and `w`.
const MS_PER_SECOND: i64 = 1_000;
const MS_PER_MINUTE: i64 = 60 * MS_PER_SECOND;
const MS_PER_HOUR: i64 = 60 * MS_PER_MINUTE;
const MS_PER_DAY: i64 = 24 * MS_PER_HOUR;
const MS_PER_WEEK: i64 = 7 * MS_PER_DAY;

/// The same day length as an unsigned count, for values the server reports as `u64`.
const MILLIS_PER_DAY: u64 = 86_400_000;

/// Binary units for table sizes, from bytes up to exbibytes.
const SIZE_UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];

/// Each binary unit is this many times the one before it.
const BYTES_PER_UNIT: f64 = 1024.0;

/// The header row of an index table.
const INDEX_HEADER: &str = "NAME\tSIZE\tMAX SIZE\tRETENTION\tREADY";

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Global options that apply to every remote command.
#[derive(Args)]
pub struct Options {
    /// Server base URL (LOGBROOK_URL; default http://127.0.0.1:3100).
    #[arg(long, global = true)]
    url: Option<String>,
    /// Bearer token (prefer LOGBROOK_TOKEN to avoid shell history).
    #[arg(long, global = true)]
    token: Option<String>,
    /// Emit the server JSON response on stdout.
    #[arg(long, global = true)]
    json: bool,
    /// Remote request deadline in seconds, including the response body.
    #[arg(
        long,
        global = true,
        default_value_t = DEFAULT_TIMEOUT_SECS,
        value_parser = clap::value_parser!(u64).range(1..=MAX_TIMEOUT_SECS)
    )]
    timeout: u64,
}

/// The `indexes` subcommands.
#[derive(Subcommand)]
pub enum Indexes {
    /// List visible indexes and their measured size.
    List,
    /// Show an index's measured size and retention policy.
    Show { name: String },
    /// Create an index; an existing index is left in place.
    Create { name: String },
    /// Permanently delete an index and its data; requires an admin credential.
    Delete {
        name: String,
        /// Confirm permanent deletion before making any HTTP request.
        #[arg(long)]
        yes: bool,
    },
}

/// An index and the time window and filters that select events from it.
#[derive(Args)]
pub struct Selection {
    /// Index name.
    name: String,
    #[command(flatten)]
    filters: Filters,
}

/// The `query` command: a selection plus paging.
#[derive(Args)]
pub struct Search {
    #[command(flatten)]
    selection: Selection,
    /// Maximum events in this page (server policies may impose a lower limit).
    #[arg(
        long,
        default_value_t = DEFAULT_LIMIT,
        value_parser = clap::value_parser!(u16).range(1..=MAX_LIMIT)
    )]
    limit: u16,
    /// Next-page cursor; reuse the same explicit --from/--to and filters.
    #[arg(long, requires = "from")]
    cursor: Option<String>,
}

/// A half-open time window in Unix milliseconds: `from` is inclusive and `to` is exclusive.
struct TimeRange {
    from: i64,
    to: i64,
}

/// Time window and event filters shared by `query` and `count`.
#[derive(Args)]
struct Filters {
    /// Look back from now (integer with ms/s/m/h/d/w suffix; default 1h).
    #[arg(long, conflicts_with_all = ["from", "to"])]
    since: Option<String>,
    /// Inclusive Unix millisecond bound; requires --to.
    #[arg(long, requires = "to", value_parser = clap::value_parser!(i64).range(0..))]
    from: Option<i64>,
    /// Exclusive Unix millisecond bound; requires --from.
    #[arg(long, requires = "from", value_parser = clap::value_parser!(i64).range(1..))]
    to: Option<i64>,
    #[arg(long)]
    source: Option<String>,
    #[arg(long)]
    service: Option<String>,
    #[arg(long)]
    logger: Option<String>,
    #[arg(long)]
    host: Option<String>,
    /// Case-sensitive message substring.
    #[arg(long)]
    message: Option<String>,
    #[arg(long, value_parser = clap::value_parser!(i32).range(0..))]
    min_level: Option<i32>,
    /// Exact top-level attribute equality as JSON: {"attributes":[{"key":"runId","op":"eq","value":"run-123"}]}.
    #[arg(long)]
    filter: Option<String>,
}

/// What the caller asked for, borrowed from the parsed command line.
#[derive(Clone, Copy)]
pub enum Operation<'a> {
    Indexes(&'a Indexes),
    Query(&'a Search),
    Count(&'a Selection),
}

/// One query-string parameter of a request.
struct Parameter {
    key: &'static str,
    value: String,
}

impl Parameter {
    fn new(key: &'static str, value: String) -> Self {
        Self { key, value }
    }
}

/// The HTTP request one command sends: its method, its path below the base URL, and its query.
struct RequestPlan<'a> {
    method: reqwest::Method,
    segments: Vec<&'a str>,
    parameters: Vec<Parameter>,
}

impl Filters {
    /// The query-string parameters for this selection, in the order they are sent.
    fn parameters(&self) -> Result<Vec<Parameter>> {
        let TimeRange { from, to } = self.time_range()?;
        if from >= to {
            return Err(Error::invalid("--from must be earlier than --to").into());
        }

        let mut parameters = vec![
            Parameter::new("from", from.to_string()),
            Parameter::new("to", to.to_string()),
        ];

        for (key, value) in [
            ("source", &self.source),
            ("service", &self.service),
            ("logger", &self.logger),
            ("host", &self.host),
            ("message", &self.message),
        ] {
            if let Some(value) = value {
                parameters.push(Parameter::new(key, value.clone()));
            }
        }

        if let Some(value) = self.min_level {
            parameters.push(Parameter::new("min_level", value.to_string()));
        }

        if let Some(filter) = &self.filter {
            logbrook::attribute_filter::AttributeFilters::parse(filter, usize::MAX)?;
            parameters.push(Parameter::new("filter", filter.clone()));
        }

        Ok(parameters)
    }

    /// The `[from, to)` window: explicit bounds, or the `--since` span ending now.
    fn time_range(&self) -> Result<TimeRange> {
        match (self.from, self.to) {
            (Some(from), Some(to)) => Ok(TimeRange { from, to }),
            (None, None) => {
                let to = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
                let duration = duration_ms(self.since.as_deref().unwrap_or(DEFAULT_SINCE))?;
                Ok(TimeRange {
                    from: to.saturating_sub(duration).max(0),
                    to,
                })
            }

            _ => Err(Error::invalid("--from and --to must be supplied together").into()),
        }
    }
}

/// Parses a relative window such as `15m` or `2d` into milliseconds.
fn duration_ms(value: &str) -> Result<i64> {
    let (number, multiplier) = [
        ("ms", 1_i64),
        ("s", MS_PER_SECOND),
        ("m", MS_PER_MINUTE),
        ("h", MS_PER_HOUR),
        ("d", MS_PER_DAY),
        ("w", MS_PER_WEEK),
    ]
    .into_iter()
    .find_map(|(suffix, multiplier)| {
        value
            .strip_suffix(suffix)
            .map(|number| (number, multiplier))
    })
    .ok_or_else(|| {
        Error::invalid("--since requires a positive integer with ms/s/m/h/d/w suffix")
    })?;
    number
        .parse::<i64>()
        .ok()
        .filter(|number| *number > 0)
        .and_then(|number| number.checked_mul(multiplier))
        .ok_or_else(|| {
            Error::invalid(
                "--since requires a positive duration within the supported millisecond range",
            )
            .into()
        })
}

/// An environment variable, treating an empty value as unset.
fn environment(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|value| !value.is_empty())
}

/// The credential environment variables a command may use, most specific role first.
fn credential_environment(operation: Operation<'_>) -> &'static [&'static str] {
    match operation {
        Operation::Indexes(Indexes::Delete { .. }) => &["LOGBROOK_ADMIN_TOKEN"],
        Operation::Indexes(Indexes::Create { .. }) => {
            &["LOGBROOK_ADMIN_TOKEN", "LOGBROOK_INGEST_TOKEN"]
        }

        Operation::Indexes(_) => &["LOGBROOK_ADMIN_TOKEN", "LOGBROOK_READ_TOKEN"],
        Operation::Query(_) | Operation::Count(_) => &["LOGBROOK_READ_TOKEN"],
    }
}

/// The token from `--token`, then `LOGBROOK_TOKEN`, then the operation's role variables.
fn token(options: &Options, operation: Operation<'_>) -> Option<String> {
    options
        .token
        .clone()
        .or_else(|| environment("LOGBROOK_TOKEN"))
        .or_else(|| {
            credential_environment(operation)
                .iter()
                .find_map(|key| environment(key))
        })
}

/// The message for a command that has no credential, naming the sources it would have read.
fn missing_credential_message(operation: Operation<'_>) -> &'static str {
    match operation {
        Operation::Indexes(Indexes::Delete { .. }) => {
            "provide --token, LOGBROOK_TOKEN, or LOGBROOK_ADMIN_TOKEN for deletion"
        }

        Operation::Indexes(Indexes::Create { .. }) => {
            "provide --token, LOGBROOK_TOKEN, LOGBROOK_ADMIN_TOKEN, or LOGBROOK_INGEST_TOKEN"
        }

        Operation::Indexes(_) => {
            "provide --token, LOGBROOK_TOKEN, LOGBROOK_ADMIN_TOKEN, or LOGBROOK_READ_TOKEN"
        }

        Operation::Query(_) | Operation::Count(_) => {
            "provide --token, LOGBROOK_TOKEN, or LOGBROOK_READ_TOKEN"
        }
    }
}

/// Runs one remote command and prints its result.
///
/// # Errors
///
/// Fails for an invalid index name, filter or URL, a missing or empty credential, a transport
/// failure, a non-success response, or a response that is not the expected JSON.
pub async fn run(options: &Options, operation: Operation<'_>) -> Result<()> {
    // The destructive-action guard comes before credential resolution and all I/O.
    if matches!(
        operation,
        Operation::Indexes(Indexes::Delete { yes: false, .. })
    ) {
        return Err(Error::invalid("index deletion is permanent; pass --yes to confirm").into());
    }

    let plan = plan_request(operation)?;
    let url = request_url(options, &plan)?;
    let token = resolve_token(options, operation)?;
    let RemoteResponse { status, body } = send(options, &token, plan.method.clone(), url).await?;
    if !status.is_success() {
        return Err(server_failure(status, &body, &token).into());
    }

    let value: Value = serde_json::from_slice(&body).map_err(|error| {
        Error::invalid("server returned an invalid JSON response").with_source(error)
    })?;
    let output = if options.json {
        value.to_string()
    } else {
        human(operation, &value, &plan.parameters)?
    };
    crate::cli_output::line(format_args!("{output}"))?;

    Ok(())
}

/// The method, path and query for an operation. Index names and filters are checked here.
fn plan_request(operation: Operation<'_>) -> Result<RequestPlan<'_>> {
    Ok(match operation {
        Operation::Indexes(Indexes::List) => RequestPlan {
            method: reqwest::Method::GET,
            segments: vec!["indexes"],
            parameters: vec![],
        },

        Operation::Indexes(Indexes::Show { name }) => {
            logbrook::config::validate_index_name(name)?;
            RequestPlan {
                method: reqwest::Method::GET,
                segments: vec!["indexes", name.as_str()],
                parameters: vec![],
            }
        }

        Operation::Indexes(Indexes::Create { name }) => {
            logbrook::config::validate_index_name(name)?;
            RequestPlan {
                method: reqwest::Method::PUT,
                segments: vec!["indexes", name.as_str()],
                parameters: vec![],
            }
        }

        Operation::Indexes(Indexes::Delete { name, .. }) => {
            logbrook::config::validate_index_name(name)?;
            RequestPlan {
                method: reqwest::Method::DELETE,
                segments: vec!["indexes", name.as_str()],
                parameters: vec![],
            }
        }

        Operation::Query(search) => {
            logbrook::config::validate_index_name(&search.selection.name)?;
            let mut parameters = search.selection.filters.parameters()?;
            parameters.push(Parameter::new("limit", search.limit.to_string()));

            if let Some(cursor) = &search.cursor {
                parameters.push(Parameter::new("cursor", cursor.clone()));
            }

            RequestPlan {
                method: reqwest::Method::GET,
                segments: vec!["indexes", search.selection.name.as_str(), "logs"],
                parameters,
            }
        }

        Operation::Count(selection) => {
            logbrook::config::validate_index_name(&selection.name)?;
            RequestPlan {
                method: reqwest::Method::GET,
                segments: vec!["indexes", selection.name.as_str(), "logs", "count"],
                parameters: selection.filters.parameters()?,
            }
        }
    })
}

/// The server base URL: `--url`, then `LOGBROOK_URL`, then the local default.
fn base_url(options: &Options) -> String {
    options
        .url
        .clone()
        .or_else(|| environment("LOGBROOK_URL"))
        .unwrap_or_else(|| DEFAULT_SERVER_URL.into())
}

/// Parses the base URL and rejects anything but a plain HTTP or HTTPS origin.
fn parse_base_url(text: &str) -> Result<reqwest::Url> {
    let (_, authority_and_path) = text
        .split_once("://")
        .ok_or_else(|| Error::invalid("--url must be an absolute HTTP or HTTPS server URL"))?;
    // An empty user part such as `http://@host` parses without a username, so the raw text
    // is checked for `@` as well as the parsed fields.
    let user_information = authority_and_path
        .split(['/', '?', '#'])
        .next()
        .is_some_and(|authority| authority.contains('@'));

    let url = reqwest::Url::parse(text).map_err(|error| {
        Error::invalid("--url must be a valid HTTP or HTTPS server URL").with_source(error)
    })?;
    let credentials_present =
        user_information || !url.username().is_empty() || url.password().is_some();
    let has_query_or_fragment = url.query().is_some() || url.fragment().is_some();
    let is_http_origin = matches!(url.scheme(), "http" | "https") && url.host_str().is_some();
    if !is_http_origin || credentials_present || has_query_or_fragment {
        return Err(Error::invalid(
            "--url must use HTTP or HTTPS and contain no user information, query, or fragment",
        )
        .into());
    }

    Ok(url)
}

/// The full request URL: the base URL with the plan's path segments and query parameters.
fn request_url(options: &Options, plan: &RequestPlan<'_>) -> Result<reqwest::Url> {
    let mut url = parse_base_url(&base_url(options))?;
    {
        // This API returns (), which carries no diagnostic information.
        let mut path = url
            .path_segments_mut()
            .map_err(|_| Error::invalid("--url cannot be used as a server base URL"))?;
        path.pop_if_empty();
        path.extend(&plan.segments);
    }

    if !plan.parameters.is_empty() {
        url.query_pairs_mut().extend_pairs(
            plan.parameters
                .iter()
                .map(|parameter| (parameter.key, parameter.value.as_str())),
        );
    }

    Ok(url)
}

/// The bearer token for the operation. An empty token counts as missing.
fn resolve_token(options: &Options, operation: Operation<'_>) -> Result<String> {
    let token = token(options, operation)
        .ok_or_else(|| Error::invalid(missing_credential_message(operation)))?;
    if token.is_empty() {
        return Err(Error::invalid("bearer token must not be empty").into());
    }

    Ok(token)
}

/// A response whose body has been read in full.
struct RemoteResponse {
    status: reqwest::StatusCode,
    body: Vec<u8>,
}

/// Sends the request and reads the whole body, refusing responses over the size limit.
async fn send(
    options: &Options,
    token: &str,
    method: reqwest::Method,
    url: reqwest::Url,
) -> Result<RemoteResponse> {
    let mut authorization = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|error| {
            Error::invalid("bearer token contains invalid HTTP header characters")
                .with_source(error)
        })?;
    authorization.set_sensitive(true);

    let timeout = Duration::from_secs(options.timeout);
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .timeout(timeout)
        .connect_timeout(timeout.min(MAX_CONNECT_TIMEOUT))
        .build()
        .map_err(|error| {
            Error::unavailable("cannot initialize the HTTPS client").with_source(error)
        })?;
    let request = client
        .request(method, url)
        .header(reqwest::header::AUTHORIZATION, authorization)
        .header(reqwest::header::ACCEPT, "application/json");
    let mut response = request.send().await.map_err(transport_error)?;
    let status = response.status();
    if response.content_length().is_some_and(|length| {
        usize::try_from(length).map_or(true, |length| length > MAX_RESPONSE_BYTES)
    }) {
        return Err(response_too_large().into());
    }

    let body = read_body(&mut response).await?;
    Ok(RemoteResponse { status, body })
}

/// Reads the body chunk by chunk, stopping as soon as it exceeds the limit.
async fn read_body(response: &mut reqwest::Response) -> Result<Vec<u8>> {
    let mut body = Vec::new();

    while let Some(chunk) = response.chunk().await.map_err(transport_error)? {
        if chunk.len() > MAX_RESPONSE_BYTES.saturating_sub(body.len()) {
            return Err(response_too_large().into());
        }

        body.extend_from_slice(&chunk);
    }

    Ok(body)
}

fn response_too_large() -> Error {
    Error::invalid("server response exceeds the 16 MiB client limit")
}

/// The error for a non-success response. The server's message is shown with the token redacted,
/// control characters escaped, and its length capped.
fn server_failure(status: reqwest::StatusCode, body: &[u8], token: &str) -> Error {
    let detail = serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .map(|message| message.replace(token, "[redacted]"))
        .map(|message| {
            message
                .chars()
                .take(MAX_ERROR_DETAIL_CHARS)
                .collect::<String>()
        })
        .map(|message| format!(": {}", cell(&message)))
        .unwrap_or_default();
    Error::unavailable(format!("server returned HTTP {status}{detail}"))
}

/// The error for a failed request, with a fixed message. The request URL and any credential
/// stay out of it.
fn transport_error(error: reqwest::Error) -> Error {
    // reqwest's Display includes URLs. Never include it in credential diagnostics.
    let message = if error.is_timeout() {
        "remote request deadline exceeded"
    } else if error.is_connect() {
        "cannot connect to the Logbrook server; check its address and TLS certificate"
    } else {
        "remote HTTP request or response transfer failed"
    };
    Error::unavailable(message).with_source(error.without_url())
}

/// Makes a value safe for one table cell: control characters are escaped.
fn cell(value: &str) -> String {
    value
        .chars()
        .flat_map(|character| {
            if character.is_control() {
                character.escape_default().collect::<Vec<_>>()
            } else {
                vec![character]
            }
        })
        .collect()
}

/// Formats a byte count in binary units for a table. An absent value shows as `-`.
fn size(value: Option<u64>) -> String {
    let Some(value) = value else {
        return "-".into();
    };
    // The table rounds to one decimal place; approximate binary units intentionally.
    let mut scaled = value as f64;
    let mut unit = 0;
    while scaled >= BYTES_PER_UNIT && unit + 1 < SIZE_UNITS.len() {
        scaled /= BYTES_PER_UNIT;
        unit += 1;
    }

    if unit == 0 {
        format!("{value} B")
    } else {
        format!("{scaled:.1} {}", SIZE_UNITS[unit])
    }
}

/// One row of an index table, from an index record in the server response.
fn index_row(record: &Value) -> Result<String> {
    let name = record
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::invalid("server index response is missing a name"))?;

    let duration = record
        .get("retention_ms")
        .and_then(Value::as_u64)
        .map(|ms| {
            if ms % MILLIS_PER_DAY == 0 {
                format!("{}d", ms / MILLIS_PER_DAY)
            } else {
                format!("{ms}ms")
            }
        })
        .unwrap_or_else(|| "-".into());
    let ready = record
        .get("ready")
        .and_then(Value::as_bool)
        .map(|ready| if ready { "yes" } else { "no" })
        .unwrap_or("-");
    Ok(format!(
        "{}\t{}\t{}\t{duration}\t{ready}",
        cell(name),
        size(record.get("size_bytes").and_then(Value::as_u64)),
        size(record.get("max_size_bytes").and_then(Value::as_u64))
    ))
}

/// The table or text a person reads for one operation's response.
fn human(operation: Operation<'_>, value: &Value, parameters: &[Parameter]) -> Result<String> {
    match operation {
        Operation::Indexes(Indexes::List) => index_list(value),
        Operation::Indexes(Indexes::Show { .. }) => index_detail(value),
        Operation::Indexes(Indexes::Create { .. }) => value
            .get("name")
            .and_then(Value::as_str)
            .map(|name| format!("Index {} is ready", cell(name)))
            .ok_or_else(|| {
                Error::invalid("server response is missing the created index name").into()
            }),
        Operation::Indexes(Indexes::Delete { .. }) => value
            .get("deleted")
            .and_then(Value::as_str)
            .map(|name| format!("Deleted index {}", cell(name)))
            .ok_or_else(|| {
                Error::invalid("server response is missing the deleted index name").into()
            }),
        Operation::Count(_) => value
            .get("count")
            .and_then(Value::as_u64)
            .map(|count| count.to_string())
            .ok_or_else(|| Error::invalid("server response is missing a count").into()),
        Operation::Query(_) => event_table(value, parameters),
    }
}

/// The `indexes list` table.
fn index_list(value: &Value) -> Result<String> {
    let records = value
        .get("indexes")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::invalid("server response is missing indexes"))?;
    let mut rows = vec![INDEX_HEADER.into()];
    for record in records {
        rows.push(index_row(record)?);
    }

    Ok(rows.join("\n"))
}

/// The `indexes show` table: the index row, then each storage component's size.
fn index_detail(value: &Value) -> Result<String> {
    let mut rows = vec![INDEX_HEADER.into(), index_row(value)?];

    for (label, key) in [
        ("Database", "database_bytes"),
        ("WAL", "wal_bytes"),
        ("Archives", "archive_bytes"),
        ("Temporary", "temp_bytes"),
        ("Other", "other_bytes"),
    ] {
        rows.push(format!(
            "{label}\t{}",
            size(value.get(key).and_then(Value::as_u64))
        ));
    }

    Ok(rows.join("\n"))
}

/// The `query` table: the range searched, one row per event, and the next cursor when there is
/// one.
fn event_table(value: &Value, parameters: &[Parameter]) -> Result<String> {
    let events = value
        .get("events")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::invalid("server response is missing events"))?;

    let parameter = |key: &str| {
        parameters
            .iter()
            .find(|parameter| parameter.key == key)
            .map_or("", |parameter| parameter.value.as_str())
    };
    let mut rows = vec![
        format!(
            "RANGE\t--from {} --to {}",
            parameter("from"),
            parameter("to")
        ),
        "TIME (ms)\tLEVEL\tSOURCE\tSERVICE\tMESSAGE".into(),
    ];
    for event in events {
        rows.push(
            ["event_time", "level", "source", "service", "message"]
                .map(|key| {
                    let field = &event[key];
                    field.as_str().map(cell).unwrap_or_else(|| {
                        if field.is_null() {
                            "-".into()
                        } else {
                            field.to_string()
                        }
                    })
                })
                .join("\t"),
        );
    }

    if let Some(cursor) = value.get("next_cursor").and_then(Value::as_str) {
        rows.push(format!("NEXT_CURSOR\t{}", serde_json::to_string(cursor)?));
    }

    Ok(rows.join("\n"))
}
