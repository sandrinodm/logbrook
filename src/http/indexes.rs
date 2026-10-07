use super::*;

pub(super) fn index_allowed(app: &App, token: &str, name: &str, ingest: bool) -> bool {
    let scopes = if ingest {
        &app.config.ingest_index_scopes
    } else {
        &app.config.read_index_scopes
    };
    scopes
        .get(token)
        .is_none_or(|names| names.is_empty() || names.iter().any(|value| value == name))
}

pub(super) fn select_index(app: &App, request: &Request) -> Result<App, Error> {
    let path = request.uri().path();
    let named = path
        .strip_prefix("/indexes/")
        .and_then(|rest| rest.split('/').next());

    if named.is_none() && !path.starts_with("/logs") {
        return Ok(app.clone());
    }

    let name = named.unwrap_or("default");
    let management = path
        .strip_prefix("/indexes/")
        .is_some_and(|rest| !rest.contains('/'));
    let ingest = (management && request.method() == axum::http::Method::PUT)
        || path.ends_with("/logs/ingest");
    let token = bearer(request.headers())?;
    let admin = management
        && matches!(
            *request.method(),
            axum::http::Method::GET | axum::http::Method::PUT | axum::http::Method::DELETE
        )
        && app.config.admin_tokens.iter().any(|value| value == token);

    if management && request.method() == axum::http::Method::DELETE && !admin {
        let kind = if app.config.read_tokens.contains_key(token)
            || app.config.ingest_tokens.contains_key(token)
        {
            ErrorKind::Forbidden
        } else {
            ErrorKind::Unauthorized
        };
        return Err(Error::new(kind, "index deletion requires an admin token"));
    }

    let valid = admin
        || if ingest {
            app.config.ingest_tokens.contains_key(token)
        } else {
            app.config.read_tokens.contains_key(token)
        };

    if !valid {
        return Err(Error::new(ErrorKind::Unauthorized, "invalid token"));
    }

    if !admin && !index_allowed(app, token, name, ingest) {
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
        } else if !ingest {
            return Err(Error::new(ErrorKind::NotFound, "unknown index"));
        }
    } else if name != "default" {
        return Err(Error::new(ErrorKind::NotFound, "unknown index"));
    }

    Ok(selected)
}

pub(super) async fn ensure_selected(app: &mut App) -> Result<(), Error> {
    if let (Some(registry), Some(name)) = (&app.registry, &app.index) {
        let handle = registry.ensure(name).await?;
        app.storage = handle.storage;
        app.config = handle.config;
    }

    Ok(())
}

pub(super) fn encode_cursor(app: &App, cursor: &str) -> String {
    app.index
        .as_ref()
        .map_or_else(|| cursor.to_owned(), |name| format!("{name}:{cursor}"))
}

pub(super) fn decode_cursor(app: &App, cursor: &str) -> Result<String, Error> {
    match &app.index {
        Some(name) => cursor
            .strip_prefix(&format!("{name}:"))
            .map(str::to_owned)
            .ok_or_else(|| Error::invalid("cursor belongs to another index or legacy route")),
        None => Ok(cursor.to_owned()),
    }
}

pub(super) async fn list_indexes(
    Extension(app): Extension<App>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, Error> {
    let token = bearer(&headers)?;
    let admin = app.config.admin_tokens.iter().any(|value| value == token);
    let source_scopes = if admin {
        Vec::new()
    } else {
        scopes(&app, &headers)?
    };
    let handles = app.registry.as_ref().map_or_else(
        || {
            vec![crate::indexes::IndexHandle {
                name: "default".to_owned(),
                storage: app.storage.clone(),
                config: app.config.clone(),
            }]
        },
        IndexRegistry::list,
    );
    let handles: Vec<_> = handles
        .into_iter()
        .filter(|handle| admin || index_allowed(&app, token, &handle.name, false))
        .collect();
    let indexes = if source_scopes.is_empty() {
        let permit = diagnostic_permit(&app)?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            handles
                .into_iter()
                .map(index_info)
                .collect::<Result<Vec<_>, Error>>()
        })
        .await
        .map_err(|_| Error::unavailable("index diagnostics worker failed"))??
    } else {
        handles
            .into_iter()
            .map(|handle| json!({"name":handle.name}))
            .collect()
    };

    Ok(Json(json!({"indexes":indexes})))
}

fn diagnostic_permit(app: &App) -> Result<tokio::sync::OwnedSemaphorePermit, Error> {
    app.metrics_jobs
        .clone()
        .try_acquire_owned()
        .map_err(|_| overload(app, "metrics_capacity", "index diagnostics already running"))
}

fn index_info(handle: crate::indexes::IndexHandle) -> Result<serde_json::Value, Error> {
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

pub(super) async fn get_index(
    Extension(app): Extension<App>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, Error> {
    let token = bearer(&headers)?;
    let name = app.index.clone().unwrap_or_else(|| "default".to_owned());
    let admin = app.config.admin_tokens.iter().any(|value| value == token);

    if !admin && !scopes(&app, &headers)?.is_empty() {
        return Ok(Json(json!({"name": name})));
    }

    let permit = diagnostic_permit(&app)?;
    let handle = crate::indexes::IndexHandle {
        name,
        storage: app.storage,
        config: app.config,
    };
    let info = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        index_info(handle)
    })
    .await
    .map_err(|_| Error::unavailable("index diagnostics worker failed"))??;

    Ok(Json(info))
}

pub(super) async fn delete_index(
    Extension(app): Extension<App>,
) -> Result<Json<serde_json::Value>, Error> {
    let name = app.index.as_deref().unwrap_or("default");
    let registry = app
        .registry
        .as_ref()
        .ok_or_else(|| Error::invalid("the mandatory default index cannot be deleted"))?;
    registry.delete(name).await?;

    Ok(Json(json!({"deleted": name})))
}

pub(super) async fn create_index(
    Extension(mut app): Extension<App>,
) -> Result<Json<serde_json::Value>, Error> {
    ensure_selected(&mut app).await?;

    Ok(Json(
        json!({"name": app.index.as_deref().unwrap_or("default")}),
    ))
}
