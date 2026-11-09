//! Index routes and the index selection that every index-scoped request passes through.

use super::{
    App, Arc, DEFAULT_INDEX_NAME, Error, ErrorKind, Extension, HeaderMap, IndexRegistry, Json,
    Method, OwnedSemaphorePermit, Request, Value, bearer, json, overload, scopes,
};

use crate::indexes::{DEFAULT_INDEX_UNDELETABLE, IndexHandle};

/// The index-diagnostics worker panicked or was cancelled.
const DIAGNOSTICS_WORKER_FAILED: &str = "index diagnostics worker failed";

/// No registered index has the requested name.
const UNKNOWN_INDEX: &str = "unknown index";

/// Which credential family a request is checked against.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Access {
    Read,
    Ingest,
}

/// Whether the token's index scope for this access kind allows `name`. An absent or empty scope
/// allows every index.
pub(super) fn index_allowed(app: &App, token: &str, name: &str, access: Access) -> bool {
    let scopes = match access {
        Access::Ingest => &app.config.ingest_index_scopes,
        Access::Read => &app.config.read_index_scopes,
    };
    scopes
        .get(token)
        .is_none_or(|names| names.is_empty() || names.iter().any(|value| value == name))
}

/// Chooses the index a request addresses and checks the caller may use it. Returns the app
/// state for that index, or the rejection.
pub(super) fn select_index(app: &App, request: &Request) -> Result<App, Error> {
    let path = request.uri().path();
    let named = path
        .strip_prefix("/indexes/")
        .and_then(|rest| rest.split('/').next());
    if named.is_none() && !path.starts_with("/logs") {
        return Ok(app.clone());
    }

    let name = named.unwrap_or(DEFAULT_INDEX_NAME);
    let management = path
        .strip_prefix("/indexes/")
        .is_some_and(|rest| !rest.contains('/'));
    let access =
        if (management && request.method() == Method::PUT) || path.ends_with("/logs/ingest") {
            Access::Ingest
        } else {
            Access::Read
        };

    let token = bearer(request.headers())?;
    let admin = management
        && matches!(
            *request.method(),
            Method::GET | Method::PUT | Method::DELETE
        )
        && app.config.admin_tokens.iter().any(|value| value == token);
    if management && request.method() == Method::DELETE && !admin {
        return Err(deletion_denied(app, token));
    }

    let valid = admin
        || match access {
            Access::Ingest => app.config.ingest_tokens.contains_key(token),
            Access::Read => app.config.read_tokens.contains_key(token),
        };
    if !valid {
        return Err(Error::new(ErrorKind::Unauthorized, "invalid token"));
    }

    if !admin && !index_allowed(app, token, name, access) {
        return Err(Error::new(
            ErrorKind::Forbidden,
            "index outside token scope",
        ));
    }

    crate::config::validate_index_name(name)?;
    let mut selected = app.clone();
    selected.index = named.map(str::to_owned);

    if let Some(registry) = &app.registry {
        if let Some(handle) = registry.get(name) {
            selected.storage = handle.storage;
            selected.config = handle.config;
        } else if access == Access::Read {
            return Err(Error::new(ErrorKind::NotFound, UNKNOWN_INDEX));
        }
    } else if name != DEFAULT_INDEX_NAME {
        return Err(Error::new(ErrorKind::NotFound, UNKNOWN_INDEX));
    }

    Ok(selected)
}

/// The rejection for an index deletion without an admin token. A known read or ingest token is
/// forbidden; anything else is unauthorized.
fn deletion_denied(app: &App, token: &str) -> Error {
    let kind = if app.config.read_tokens.contains_key(token)
        || app.config.ingest_tokens.contains_key(token)
    {
        ErrorKind::Forbidden
    } else {
        ErrorKind::Unauthorized
    };
    Error::new(kind, "index deletion requires an admin token")
}

/// Creates the index a request addresses when the registry is in use, so that ingestion can
/// open it on first write.
pub(super) async fn ensure_selected(app: &mut App) -> Result<(), Error> {
    if let (Some(registry), Some(name)) = (&app.registry, &app.index) {
        let handle = registry.ensure(name).await?;
        app.storage = handle.storage;
        app.config = handle.config;
    }

    Ok(())
}

/// The query-cursor scope for the index a request addresses.
pub(super) fn cursor_scope(app: &App) -> crate::query_cursor::CursorScope {
    app.index
        .as_ref()
        .map_or(crate::query_cursor::CursorScope::Legacy, |name| {
            crate::query_cursor::CursorScope::Named(name.clone())
        })
}

/// Lists the indexes the caller may see. Callers with a source-scoped token get names only.
pub(super) async fn list_indexes(
    Extension(app): Extension<App>,
    headers: HeaderMap,
) -> Result<Json<Value>, Error> {
    let token = bearer(&headers)?;
    let admin = app.config.admin_tokens.iter().any(|value| value == token);
    let source_scopes = if admin {
        Vec::new()
    } else {
        scopes(&app, &headers)?
    };

    let handles = app.registry.as_ref().map_or_else(
        || {
            vec![IndexHandle {
                name: DEFAULT_INDEX_NAME.to_owned(),
                storage: app.storage.clone(),
                config: app.config.clone(),
            }]
        },
        IndexRegistry::list,
    );
    let handles: Vec<_> = handles
        .into_iter()
        .filter(|handle| admin || index_allowed(&app, token, &handle.name, Access::Read))
        .collect();
    let indexes = if source_scopes.is_empty() {
        index_diagnostics(&app, handles).await?
    } else {
        handles
            .into_iter()
            .map(|handle| json!({"name":handle.name}))
            .collect()
    };

    Ok(Json(json!({"indexes":indexes})))
}

/// Measures each index's disk use on a blocking worker, under the diagnostics permit.
async fn index_diagnostics(app: &App, handles: Vec<IndexHandle>) -> Result<Vec<Value>, Error> {
    let permit = diagnostic_permit(app)?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        handles
            .into_iter()
            .map(index_info)
            .collect::<Result<Vec<_>, Error>>()
    })
    .await
    .map_err(|error| Error::unavailable(DIAGNOSTICS_WORKER_FAILED).with_source(error))?
}

/// The diagnostics permit. Disk measurement is slow, so only one runs at a time.
fn diagnostic_permit(app: &App) -> Result<OwnedSemaphorePermit, Error> {
    Arc::clone(&app.metrics_jobs)
        .try_acquire_owned()
        .map_err(|error| {
            overload(app, "metrics_capacity", "index diagnostics already running")
                .with_source(error)
        })
}

fn index_info(handle: IndexHandle) -> Result<Value, Error> {
    let disk = handle.storage.disk_usage()?;

    Ok(json!({
        "name": handle.name,
        "size_bytes": disk.size_bytes,
        "database_bytes": disk.database_bytes,
        "wal_bytes": disk.wal_bytes,
        "archive_bytes": disk.archive_bytes,
        "temp_bytes": disk.temp_bytes,
        "other_bytes": disk.other_bytes,
        "max_size_bytes": handle.config.storage.max_size_bytes,
        "retention_ms": handle.config.retention_ms,
        "ready": handle.storage.is_ready(),
    }))
}

/// One index's diagnostics. A source-scoped caller gets only the name.
pub(super) async fn get_index(
    Extension(app): Extension<App>,
    headers: HeaderMap,
) -> Result<Json<Value>, Error> {
    let token = bearer(&headers)?;
    let name = app
        .index
        .clone()
        .unwrap_or_else(|| DEFAULT_INDEX_NAME.to_owned());
    let admin = app.config.admin_tokens.iter().any(|value| value == token);
    if !admin && !scopes(&app, &headers)?.is_empty() {
        return Ok(Json(json!({"name": name})));
    }

    let permit = diagnostic_permit(&app)?;
    let handle = IndexHandle {
        name,
        storage: app.storage,
        config: app.config,
    };
    let info = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        index_info(handle)
    })
    .await
    .map_err(|error| Error::unavailable(DIAGNOSTICS_WORKER_FAILED).with_source(error))??;
    Ok(Json(info))
}

/// Deletes the index a request addresses. The default index is never deleted.
pub(super) async fn delete_index(Extension(app): Extension<App>) -> Result<Json<Value>, Error> {
    let name = app.index.as_deref().unwrap_or(DEFAULT_INDEX_NAME);
    let registry = app
        .registry
        .as_ref()
        .ok_or_else(|| Error::invalid(DEFAULT_INDEX_UNDELETABLE))?;
    registry.delete(name).await?;

    Ok(Json(json!({"deleted": name})))
}

/// Creates the index a request addresses, or confirms it already exists.
pub(super) async fn create_index(Extension(mut app): Extension<App>) -> Result<Json<Value>, Error> {
    ensure_selected(&mut app).await?;

    Ok(Json(
        json!({"name": app.index.as_deref().unwrap_or(DEFAULT_INDEX_NAME)}),
    ))
}
