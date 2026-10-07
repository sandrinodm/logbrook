//! Remote commands do not load server configuration or touch local storage.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::{Args, Subcommand};
use logbrook::model::Error;
use serde_json::Value;

const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

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
    #[arg(long, global = true, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=300))]
    timeout: u64,
}

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

#[derive(Args)]
pub struct Selection {
    /// Index name.
    name: String,
    #[command(flatten)]
    filters: Filters,
}

#[derive(Args)]
pub struct Search {
    #[command(flatten)]
    selection: Selection,
    /// Maximum events in this page (server policies may impose a lower limit).
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u16).range(1..=1000))]
    limit: u16,
    /// Next-page cursor; reuse the same explicit --from/--to and filters.
    #[arg(long, requires = "from")]
    cursor: Option<String>,
}

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
}

pub enum Operation<'a> {
    Indexes(&'a Indexes),
    Query(&'a Search),
    Count(&'a Selection),
}

impl Filters {
    fn pairs(&self) -> Result<Vec<(String, String)>> {
        let (from, to) = match (self.from, self.to) {
            (Some(from), Some(to)) => (from, to),
            (None, None) => {
                let to = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
                let duration = duration_ms(self.since.as_deref().unwrap_or("1h"))?;
                (to.saturating_sub(duration).max(0), to)
            }

            _ => return Err(Error::invalid("--from and --to must be supplied together").into()),
        };

        if from >= to {
            return Err(Error::invalid("--from must be earlier than --to").into());
        }

        let mut pairs = vec![
            ("from".into(), from.to_string()),
            ("to".into(), to.to_string()),
        ];

        for (key, value) in [
            ("source", &self.source),
            ("service", &self.service),
            ("logger", &self.logger),
            ("host", &self.host),
            ("message", &self.message),
        ] {
            if let Some(value) = value {
                pairs.push((key.into(), value.clone()));
            }
        }

        if let Some(value) = self.min_level {
            pairs.push(("min_level".into(), value.to_string()));
        }

        Ok(pairs)
    }
}

fn duration_ms(value: &str) -> Result<i64> {
    let (number, multiplier) = [
        ("ms", 1_i64),
        ("s", 1_000),
        ("m", 60_000),
        ("h", 3_600_000),
        ("d", 86_400_000),
        ("w", 604_800_000),
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

fn environment(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|value| !value.is_empty())
}

fn token(options: &Options, operation: &Operation<'_>) -> Option<String> {
    options
        .token
        .clone()
        .or_else(|| environment("LOGBROOK_TOKEN"))
        .or_else(|| {
            let roles: &[&str] = match operation {
                Operation::Indexes(Indexes::Delete { .. }) => &["LOGBROOK_ADMIN_TOKEN"],
                Operation::Indexes(Indexes::Create { .. }) => {
                    &["LOGBROOK_ADMIN_TOKEN", "LOGBROOK_INGEST_TOKEN"]
                }

                Operation::Indexes(_) => &["LOGBROOK_ADMIN_TOKEN", "LOGBROOK_READ_TOKEN"],
                Operation::Query(_) | Operation::Count(_) => &["LOGBROOK_READ_TOKEN"],
            };
            roles.iter().find_map(|key| environment(key))
        })
}

pub async fn run(options: &Options, operation: Operation<'_>) -> Result<()> {
    // The destructive-action guard comes before credential resolution and all I/O.
    if matches!(
        &operation,
        Operation::Indexes(Indexes::Delete { yes: false, .. })
    ) {
        return Err(Error::invalid("index deletion is permanent; pass --yes to confirm").into());
    }

    let (method, segments, pairs) = match &operation {
        Operation::Indexes(Indexes::List) => (reqwest::Method::GET, vec!["indexes"], vec![]),
        Operation::Indexes(Indexes::Show { name }) => {
            logbrook::config::validate_index_name(name)?;
            (reqwest::Method::GET, vec!["indexes", name.as_str()], vec![])
        }

        Operation::Indexes(Indexes::Create { name }) => {
            logbrook::config::validate_index_name(name)?;
            (reqwest::Method::PUT, vec!["indexes", name.as_str()], vec![])
        }

        Operation::Indexes(Indexes::Delete { name, .. }) => {
            logbrook::config::validate_index_name(name)?;
            (
                reqwest::Method::DELETE,
                vec!["indexes", name.as_str()],
                vec![],
            )
        }

        Operation::Query(search) => {
            logbrook::config::validate_index_name(&search.selection.name)?;
            let mut pairs = search.selection.filters.pairs()?;
            pairs.push(("limit".into(), search.limit.to_string()));

            if let Some(cursor) = &search.cursor {
                pairs.push(("cursor".into(), cursor.clone()));
            }

            (
                reqwest::Method::GET,
                vec!["indexes", search.selection.name.as_str(), "logs"],
                pairs,
            )
        }

        Operation::Count(selection) => {
            logbrook::config::validate_index_name(&selection.name)?;
            (
                reqwest::Method::GET,
                vec!["indexes", selection.name.as_str(), "logs", "count"],
                selection.filters.pairs()?,
            )
        }
    };
    let url = options
        .url
        .clone()
        .or_else(|| environment("LOGBROOK_URL"))
        .unwrap_or_else(|| "http://127.0.0.1:3100".into());
    let (_, authority_and_path) = url
        .split_once("://")
        .ok_or_else(|| Error::invalid("--url must be an absolute HTTP or HTTPS server URL"))?;
    let user_information = {
        let rest = authority_and_path;
        rest.split(['/', '?', '#'])
            .next()
            .is_some_and(|authority| authority.contains('@'))
    };
    let mut url = reqwest::Url::parse(&url)
        .map_err(|_| Error::invalid("--url must be a valid HTTP or HTTPS server URL"))?;

    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || user_information
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::invalid(
            "--url must use HTTP or HTTPS and contain no user information, query, or fragment",
        )
        .into());
    }

    {
        let mut path = url
            .path_segments_mut()
            .map_err(|_| Error::invalid("--url cannot be used as a server base URL"))?;
        path.pop_if_empty();
        path.extend(segments);
    }

    if !pairs.is_empty() {
        url.query_pairs_mut()
            .extend_pairs(pairs.iter().map(|(key, value)| (key, value)));
    }

    let token = token(options, &operation).ok_or_else(|| {
        Error::invalid(match &operation {
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
        })
    })?;

    if token.is_empty() {
        return Err(Error::invalid("bearer token must not be empty").into());
    }

    let mut authorization = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|_| Error::invalid("bearer token contains invalid HTTP header characters"))?;
    authorization.set_sensitive(true);
    let timeout = Duration::from_secs(options.timeout);
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .timeout(timeout)
        .connect_timeout(timeout.min(Duration::from_secs(5)))
        .build()
        .map_err(|_| Error::unavailable("cannot initialize the HTTPS client"))?;
    let request = client
        .request(method, url)
        .header(reqwest::header::AUTHORIZATION, authorization)
        .header(reqwest::header::ACCEPT, "application/json");
    let mut response = request.send().await.map_err(transport_error)?;
    let status = response.status();

    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err(Error::invalid("server response exceeds the 16 MiB client limit").into());
    }

    let mut body = Vec::new();

    while let Some(chunk) = response.chunk().await.map_err(transport_error)? {
        if chunk.len() > MAX_RESPONSE_BYTES.saturating_sub(body.len()) {
            return Err(Error::invalid("server response exceeds the 16 MiB client limit").into());
        }

        body.extend_from_slice(&chunk);
    }

    if !status.is_success() {
        let detail = serde_json::from_slice::<Value>(&body)
            .ok()
            .and_then(|value| {
                value
                    .get("error")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .map(|message| message.replace(&token, "[redacted]"))
            .map(|message| message.chars().take(512).collect::<String>())
            .map(|message| format!(": {}", cell(&message)))
            .unwrap_or_default();
        return Err(Error::unavailable(format!("server returned HTTP {status}{detail}")).into());
    }

    let value: Value = serde_json::from_slice(&body)
        .map_err(|_| Error::invalid("server returned an invalid JSON response"))?;
    let output = if options.json {
        value.to_string()
    } else {
        human(&operation, &value, &pairs)?
    };
    crate::cli_output::line(format_args!("{output}"))?;

    Ok(())
}

fn transport_error(error: reqwest::Error) -> Error {
    // reqwest's Display includes URLs. Never include it in credential diagnostics.
    if error.is_timeout() {
        Error::unavailable("remote request deadline exceeded")
    } else if error.is_connect() {
        Error::unavailable(
            "cannot connect to the Logbrook server; check its address and TLS certificate",
        )
    } else {
        Error::unavailable("remote HTTP request or response transfer failed")
    }
}

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

fn size(value: Option<u64>) -> String {
    let Some(value) = value else {
        return "-".into();
    };
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    let mut scaled = value as f64;
    let mut unit = 0;

    while scaled >= 1024.0 && unit + 1 < UNITS.len() {
        scaled /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{value} B")
    } else {
        format!("{scaled:.1} {}", UNITS[unit])
    }
}

fn index_row(record: &Value) -> Result<String> {
    let name = record
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::invalid("server index response is missing a name"))?;
    let duration = record
        .get("retention_ms")
        .and_then(Value::as_u64)
        .map(|ms| {
            if ms % 86_400_000 == 0 {
                format!("{}d", ms / 86_400_000)
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

fn human(operation: &Operation<'_>, value: &Value, pairs: &[(String, String)]) -> Result<String> {
    match operation {
        Operation::Indexes(Indexes::List) => {
            let records = value
                .get("indexes")
                .and_then(Value::as_array)
                .ok_or_else(|| Error::invalid("server response is missing indexes"))?;
            let mut rows = vec!["NAME\tSIZE\tMAX SIZE\tRETENTION\tREADY".into()];

            for record in records {
                rows.push(index_row(record)?);
            }

            Ok(rows.join("\n"))
        }

        Operation::Indexes(Indexes::Show { .. }) => {
            let mut rows = vec![
                "NAME\tSIZE\tMAX SIZE\tRETENTION\tREADY".into(),
                index_row(value)?,
            ];

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
        Operation::Query(_) => {
            let events = value
                .get("events")
                .and_then(Value::as_array)
                .ok_or_else(|| Error::invalid("server response is missing events"))?;
            let mut rows = vec![
                format!(
                    "RANGE\t--from {} --to {}",
                    pairs
                        .iter()
                        .find(|(key, _)| key == "from")
                        .map(|(_, value)| value.as_str())
                        .unwrap_or(""),
                    pairs
                        .iter()
                        .find(|(key, _)| key == "to")
                        .map(|(_, value)| value.as_str())
                        .unwrap_or("")
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
    }
}
