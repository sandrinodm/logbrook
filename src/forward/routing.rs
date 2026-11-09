//! Route authorization uses only configured identities and destinations.
use super::{
    config::{ForwardConfig, RouteConfig},
    normalize::NormalizationOptions,
};
use crate::{
    indexes::{IndexHandle, IndexRegistry},
    model::Error,
};

/// Level given to events whose route sets no `default_level`: Pino's `info`.
const DEFAULT_EVENT_LEVEL: i32 = 30;

/// Fields searched for the message when a route names none.
const DEFAULT_MESSAGE_FIELDS: [&str; 2] = ["message", "msg"];

/// Error when the connection's principal is not in `principals` (reason `principal_denied`).
const IDENTITY_DENIED: &str = "Forward identity denied";

/// Error when the matched route's `index` is not among the principal's `indexes` (reason
/// `destination_denied`).
const DESTINATION_DENIED: &str = "Forward destination denied";

/// Error when no route on the listener matches the envelope's tag by exact `tag` or `tag_prefix`
/// (reason `route_denied`).
const ROUTE_DENIED: &str = "Forward route denied";

/// The `service_from` value that takes the service from the tag after its prefix.
const TAG_SUFFIX: &str = "tag_suffix";

/// The index and normalization settings chosen for one envelope.
pub struct ResolvedRoute {
    /// The index that receives the events.
    pub destination: IndexHandle,
    /// The source label stored with the events.
    pub source: String,
    /// How the envelope's events are normalized.
    pub options: NormalizationOptions,
}

/// Chooses the destination and normalization for an envelope from `principal` on `listener`.
///
/// # Errors
///
/// Returns an invalid error when the principal or route is not configured, or the principal may
/// not write to the route's index. Returns an unavailable error when the index is not open.
pub fn resolve(
    config: &ForwardConfig,
    registry: &IndexRegistry,
    listener: &str,
    principal: &str,
    tag: &str,
    received_at: i64,
) -> Result<ResolvedRoute, Error> {
    let identity = config
        .principals
        .get(principal)
        .ok_or_else(|| Error::invalid(IDENTITY_DENIED).with_reason("principal_denied"))?;
    let route = find_route(config, listener, tag)?;
    if !identity.indexes.contains(&route.index) {
        return Err(Error::invalid(DESTINATION_DENIED).with_reason("destination_denied"));
    }

    let destination = registry
        .get(&route.index)
        .ok_or_else(|| Error::unavailable("Forward destination unavailable"))?;
    let service = service_name(route, tag);
    let options = normalization_options(config, route, &destination, service, received_at);
    Ok(ResolvedRoute {
        destination,
        source: identity.source.clone(),
        options,
    })
}

/// The first route on `listener` whose exact tag or prefix matches `tag`.
fn find_route<'a>(
    config: &'a ForwardConfig,
    listener: &str,
    tag: &str,
) -> Result<&'a RouteConfig, Error> {
    config
        .routes
        .iter()
        .find(|route| {
            route.listener == listener
                && route
                    .tag
                    .as_ref()
                    .map_or_else(|| tag.starts_with(&route.tag_prefix), |exact| tag == exact)
        })
        .ok_or_else(|| Error::invalid(ROUTE_DENIED).with_reason("route_denied"))
}

/// The service label: an explicit `service` wins, otherwise the tag after the prefix when
/// `service_from` asks for it.
fn service_name(route: &RouteConfig, tag: &str) -> Option<String> {
    route.service.clone().or_else(|| {
        route
            .service_from
            .as_deref()
            .filter(|value| *value == TAG_SUFFIX)
            .map(|_| tag[route.tag_prefix.len()..].to_owned())
    })
}

/// The normalization options for an envelope received at `received_at`. The accepted event-time
/// window runs from the destination's retention before receipt to its allowed future skew after.
fn normalization_options(
    config: &ForwardConfig,
    route: &RouteConfig,
    destination: &IndexHandle,
    service: Option<String>,
    received_at: i64,
) -> NormalizationOptions {
    NormalizationOptions {
        compose_service_label: route.compose_service_label.clone(),
        application_time_field: route.application_time_field.clone(),
        application_time_format: route.application_time_format.clone(),
        oldest_time: received_at.saturating_sub(destination.config.retention_ms),
        newest_time: received_at.saturating_add(destination.config.max_future_skew_ms),
        profile: route.normalizer,
        json_profile: route.json_profile,
        received_at,
        zero_time_to_receipt: config.zero_time_to_receipt,
        service,
        host: route.host.clone(),
        default_level: route.default_level.unwrap_or(DEFAULT_EVENT_LEVEL),
        max_message_bytes: config.max_message_bytes,
        max_normalized_bytes: config.max_normalized_bytes,
        max_json_depth: config.max_metadata_depth,
        message_fields: if route.message_fields.is_empty() {
            DEFAULT_MESSAGE_FIELDS
                .into_iter()
                .map(String::from)
                .collect()
        } else {
            route.message_fields.clone()
        },
    }
}
