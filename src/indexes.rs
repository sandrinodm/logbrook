//! Owns index discovery, bounded creation, deletion, maintenance, and orderly shutdown.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{
    sync::{Mutex as AsyncMutex, watch},
    task::spawn_blocking,
};

use crate::{
    config::{Config, DEFAULT_INDEX_NAME, validate_index_name, validate_literal_path},
    model::{Error, ErrorKind},
    storage::{IngressAdmission, Storage},
};

/// Refusal to delete the default index, which every installation needs.
pub(crate) const DEFAULT_INDEX_UNDELETABLE: &str = "the mandatory default index cannot be deleted";

/// The deletion worker panicked or was cancelled.
const DELETION_WORKER_FAILED: &str = "index deletion worker failed";

/// Directory under the data root that holds deleted indexes until cleanup completes.
const TRASH_DIRECTORY: &str = ".index-trash";

/// Prefix of each tombstone inside the trash directory. Cleanup only removes entries with it.
const TRASH_PREFIX: &str = "deleted-";

/// Subdirectory of a tombstone that holds the index's former data.
const TOMBSTONE_DATA: &str = "data";

/// Entries of the flat layout, which must move into `indexes/default` before this version starts.
const LEGACY_FLAT_ENTRIES: [&str; 3] = ["events.duckdb", "events.duckdb.wal", "archives"];

/// The shortest wait between retries of a failed maintenance pass.
const MIN_RETRY_DELAY: Duration = Duration::from_secs(1);

/// The longest wait between retries of a failed maintenance pass.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(900);

/// The retry delay doubles per consecutive failure, up to this many doublings.
const MAX_RETRY_DOUBLINGS: u32 = 8;

/// A registered index: its name, its open storage, and the configuration it runs with.
#[derive(Clone)]
pub struct IndexHandle {
    /// Validated index name used in routes, paths, and metric labels.
    pub name: String,
    /// Open storage for this index, shared by cloned handles.
    pub storage: Storage,
    /// Effective per-index policy and storage budgets.
    pub config: Config,
}

struct Entry {
    handle: IndexHandle,
    maintenance: Option<tokio::task::JoinHandle<()>>,
    stop: watch::Sender<bool>,
    deleting: bool,
}

struct Inner {
    config: Config,
    entries: Mutex<BTreeMap<String, Entry>>,
    /// Serializes creation and deletion, and lets shutdown wait for an admitted one to finish.
    creation: Arc<AsyncMutex<()>>,
    closed: AtomicBool,
    admission: Arc<IngressAdmission>,
    ingested: Arc<AtomicU64>,
}

/// The set of open indexes for one process. Cheap to clone; all clones share the same state.
#[derive(Clone)]
pub struct IndexRegistry(Arc<Inner>);

impl IndexRegistry {
    /// Validates the configuration, discovers the indexes on disk, and opens each one.
    ///
    /// # Errors
    ///
    /// Fails when the configuration is invalid, the data layout is unsafe, the index count
    /// exceeds `max_indexes`, or any index fails to open. Opened indexes are shut down again.
    pub async fn open(mut config: Config) -> Result<Self, Error> {
        config.validate()?;
        let root = config.storage.data_dir.clone();
        let (canonical, names) = spawn_blocking(move || prepare_layout(&root))
            .await
            .map_err(worker_failed("index discovery worker failed"))??;
        config.storage.data_dir = canonical;
        let mut names: BTreeSet<String> = names.into_iter().collect();
        names.extend(config.indexes.keys().cloned());
        names.insert(DEFAULT_INDEX_NAME.to_owned());

        if names.len() > config.max_indexes {
            return Err(Error::invalid(
                "existing and configured indexes exceed max_indexes",
            ));
        }

        let registry = Self(Arc::new(Inner {
            config,
            entries: Mutex::new(BTreeMap::new()),
            creation: Arc::new(AsyncMutex::new(())),
            closed: AtomicBool::new(false),
            admission: Arc::new(IngressAdmission::new()),
            ingested: Arc::new(AtomicU64::new(0)),
        }));

        for name in names {
            if let Err(error) = registry.ensure(&name).await {
                drop(registry.shutdown().await);
                return Err(error);
            }
        }

        Ok(registry)
    }

    fn entries(&self) -> MutexGuard<'_, BTreeMap<String, Entry>> {
        self.0.entries.lock().expect("index registry lock")
    }

    /// The registered handle for `name`, including one that is draining for deletion.
    pub fn get(&self, name: &str) -> Option<IndexHandle> {
        self.entries().get(name).map(|entry| entry.handle.clone())
    }

    /// Every registered handle, in name order.
    pub fn list(&self) -> Vec<IndexHandle> {
        self.entries()
            .values()
            .map(|entry| entry.handle.clone())
            .collect()
    }

    /// Events committed since opening this registry, including deleted indexes.
    pub fn committed_events(&self) -> u64 {
        self.0.ingested.load(Ordering::Relaxed)
    }

    /// Returns the index, creating it when `name` is valid and there is room under `max_indexes`.
    ///
    /// The owned task completes registration even if its HTTP caller disconnects.
    /// Creation and shutdown share a gate, so native workers cannot be orphaned.
    ///
    /// # Errors
    ///
    /// Fails for an invalid name, a stopped registry, a full registry, or a storage open failure.
    pub async fn ensure(&self, name: &str) -> Result<IndexHandle, Error> {
        validate_index_name(name)?;

        if self.0.closed.load(Ordering::Acquire) {
            return Err(shutting_down());
        }

        if let Some(handle) = self.get(name) {
            return available_handle(handle);
        }

        // Waiting callers remain cancellable; only the one admitted creator is
        // detached, so disconnected requests cannot build an unbounded task queue.
        let creation = Arc::clone(&self.0.creation).lock_owned().await;
        let registry = self.clone();
        let name = name.to_owned();

        tokio::spawn(async move {
            let _creation = creation;
            registry.create(name).await
        })
        .await
        .map_err(worker_failed("index creation worker failed"))?
    }

    /// Creates and publishes one index. Runs under the creation gate, detached from the caller.
    async fn create(&self, name: String) -> Result<IndexHandle, Error> {
        if self.0.closed.load(Ordering::Acquire) {
            return Err(shutting_down());
        }

        if let Some(handle) = self.get(&name) {
            return available_handle(handle);
        }

        if self.list().len() >= self.0.config.max_indexes {
            return Err(Error::new(
                ErrorKind::Overloaded,
                "maximum index count reached; increase max_indexes and restart",
            ));
        }

        let config = self.0.config.for_index(&name)?;
        let storage_config = config.storage.clone();
        let ingested = Arc::clone(&self.0.ingested);
        let admission = Arc::clone(&self.0.admission);
        let storage = spawn_blocking(move || {
            prepare_index_directory(&storage_config.data_dir)?;
            Storage::open_with_ingested_counter(storage_config, ingested, admission)
        })
        .await
        .map_err(worker_failed("index open worker failed"))??;

        let handle = IndexHandle {
            name: name.clone(),
            storage,
            config,
        };

        // stop_admission is synchronous and can race the native open. Do not
        // publish a new writable index after the shutdown signal.
        if self.0.closed.load(Ordering::Acquire) {
            handle.storage.shutdown().await?;
            return Err(shutting_down());
        }

        let (stop, stopped) = watch::channel(false);
        let maintenance = tokio::spawn(maintain(handle.clone(), stopped));
        self.entries().insert(
            name,
            Entry {
                handle: handle.clone(),
                maintenance: Some(maintenance),
                stop,
                deleting: false,
            },
        );

        // Cover a stop arriving between the check above and publication.
        if self.0.closed.load(Ordering::Acquire) {
            handle.storage.stop_admission();
        }

        Ok(handle)
    }

    /// Whether the registry can serve. An index being deleted is excluded: its drain is
    /// expected, and a failed drain is reported by the deletion itself.
    pub fn is_ready(&self) -> bool {
        !self.0.closed.load(Ordering::Acquire)
            && self
                .entries()
                .values()
                .all(|entry| entry.deleting || entry.handle.storage.is_ready())
    }

    /// Stops accepting new work in every index. This cannot be undone.
    pub fn stop_admission(&self) {
        self.0.admission.stop();
        self.0.closed.store(true, Ordering::Release);

        for entry in self.entries().values() {
            entry.stop.send_replace(true);
            entry.handle.storage.stop_admission();
        }
    }

    /// Deletes an unconfigured, non-default index and its data.
    ///
    /// Once admitted, deletion owns its lifecycle even if the caller disconnects.
    ///
    /// # Errors
    ///
    /// Fails for the default index, configured indexes, unknown names, a stopped registry, or
    /// any failure to drain the index or remove its data. A failed drain leaves the index
    /// visible as stopped so readiness reports it.
    pub async fn delete(&self, name: &str) -> Result<(), Error> {
        validate_index_name(name)?;

        if name == DEFAULT_INDEX_NAME {
            return Err(Error::invalid(DEFAULT_INDEX_UNDELETABLE));
        }

        if self.0.config.indexes.contains_key(name) {
            return Err(Error::invalid(
                "configured indexes cannot be deleted; remove the index from configuration \
                 and restart first",
            ));
        }

        if self.0.closed.load(Ordering::Acquire) {
            return Err(shutting_down());
        }

        let creation = Arc::clone(&self.0.creation).lock_owned().await;
        let registry = self.clone();
        let name = name.to_owned();
        tokio::spawn(async move {
            let _creation = creation;
            registry.delete_admitted(&name).await
        })
        .await
        .map_err(worker_failed(DELETION_WORKER_FAILED))?
    }

    /// The deletion steps for an admitted index. Runs under the creation gate, detached from
    /// the caller.
    async fn delete_admitted(&self, name: &str) -> Result<(), Error> {
        if self.0.closed.load(Ordering::Acquire) {
            return Err(shutting_down());
        }

        let handle = self
            .get(name)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "index not found"))?;
        // Validate before closing a healthy index, then again after its workers
        // have drained. Never follow symlinks while removing managed data.
        let path = handle.config.storage.data_dir.clone();
        spawn_blocking(move || validate_index_directory(&path))
            .await
            .map_err(worker_failed(DELETION_WORKER_FAILED))??;

        let maintenance = self.begin_deletion(name);
        let _deletion = DeletionGuard {
            registry: self.clone(),
            name: name.to_owned(),
        };
        let maintenance_failed = match maintenance {
            Some(maintenance) => maintenance.await.is_err(),
            None => false,
        };

        // Shutdown always joins the native workers, including failure paths.
        // On failure the stopped entry stays visible and makes readiness fail.
        let shutdown = handle.storage.shutdown().await;

        if maintenance_failed {
            return Err(maintenance_worker_failed());
        }

        shutdown?;

        let root = self.0.config.storage.data_dir.clone();
        let path = handle.config.storage.data_dir.clone();
        let Tombstone {
            path: staged,
            durability_error,
        } = spawn_blocking(move || stage_deletion(&root, &path))
            .await
            .map_err(worker_failed(DELETION_WORKER_FAILED))??;
        self.entries().remove(name);

        if let Some(error) = durability_error {
            return Err(error);
        }

        // A failed cleanup cannot resurrect the index: the durable tombstone
        // is outside discovery and cleanup is retried at startup/deletion.
        spawn_blocking(move || remove_trash_directory(&staged))
            .await
            .map_err(worker_failed("index cleanup worker failed"))?
    }

    /// Marks an index as deleting and stops its work. Returns its maintenance worker so the
    /// caller can join it outside the lock.
    fn begin_deletion(&self, name: &str) -> Option<tokio::task::JoinHandle<()>> {
        let mut entries = self.entries();
        let entry = entries.get_mut(name).expect("admitted index deletion");
        entry.deleting = true;
        entry.handle.storage.stop_admission();
        entry.stop.send_replace(true);
        entry.maintenance.take()
    }

    /// Stops admission, then drains every index's maintenance worker and storage.
    ///
    /// # Errors
    ///
    /// Returns the last worker or storage failure after every index has been drained.
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.stop_admission();
        let registry = self.clone();

        // Closing admission is irreversible: this owned task also owns waiting
        // for an already admitted creator/deleter before draining every worker.
        tokio::spawn(async move {
            let _creation = registry.0.creation.lock().await;
            let entries = std::mem::take(&mut *registry.entries());
            let mut failure = None;
            for (_, entry) in entries {
                if let Some(maintenance) = entry.maintenance
                    && maintenance.await.is_err()
                {
                    failure = Some(maintenance_worker_failed());
                }

                if let Err(error) = entry.handle.storage.shutdown().await {
                    failure = Some(error);
                }
            }

            failure.map_or(Ok(()), Err)
        })
        .await
        .map_err(worker_failed("index shutdown worker failed"))?
    }
}

fn shutting_down() -> Error {
    Error::unavailable("index registry is shutting down")
}

fn maintenance_worker_failed() -> Error {
    Error::unavailable("index maintenance worker failed")
}

/// Adds the operation's fixed operator message to a failed task, retaining its join error.
fn worker_failed(message: &'static str) -> impl FnOnce(tokio::task::JoinError) -> Error {
    move |error| Error::unavailable(message).with_source(error)
}

// A failed deletion leaves a stopped entry visible. Expected drains are excluded
// from readiness; failure and panic restore accounting before releasing the gate.
struct DeletionGuard {
    registry: IndexRegistry,
    name: String,
}

impl Drop for DeletionGuard {
    fn drop(&mut self) {
        // A poisoned lock still holds consistent entries, and this runs during unwinding.
        let mut entries = self
            .registry
            .0
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = entries.get_mut(&self.name) {
            entry.deleting = false;
        }
    }
}

/// Runs retention, size, archive and compaction on the configured interval until stopped.
/// Failures back off; a storage that has stopped ends the supervisor.
async fn maintain(index: IndexHandle, mut stopped: watch::Receiver<bool>) {
    let normal_delay = Duration::from_secs(index.config.maintenance_interval_secs);
    let mut delay = Duration::ZERO;
    let mut failures = 0_u32;

    loop {
        tokio::select! {
            biased;
            () = maintenance_stopped(&mut stopped) => break,
            _ = tokio::time::sleep(delay) => {}
        }

        let now = unix_millis_now();
        let result = tokio::select! {
            biased;
            () = maintenance_stopped(&mut stopped) => {
                index.storage.cancel_maintenance();
                break;
            }

            result = run_maintenance_pass(&index, now) => result,
        };
        match result {
            Ok(()) => {
                failures = 0;
                delay = normal_delay;
            }

            Err(error) if error.kind == ErrorKind::SizeLimit => {
                failures = 0;
                delay = normal_delay;
                tracing::warn!(
                    index = %index.name,
                    %error,
                    "Index at disk target; maintenance will retry at the normal interval"
                );
            }

            Err(error) if !index.storage.is_ready() => {
                tracing::error!(
                    index = %index.name,
                    %error,
                    "Index storage failed; supervisor will stop"
                );
                break;
            }

            Err(error) => {
                failures = failures.saturating_add(1);
                delay = retry_delay(normal_delay, failures);
                tracing::warn!(
                    index = %index.name,
                    %error,
                    retry_seconds = delay.as_secs(),
                    "Index maintenance failed; bounded retry scheduled"
                );
            }
        }
    }
}

/// Resolves when maintenance is stopped, including when the registry drops its sender.
async fn maintenance_stopped(stop: &mut watch::Receiver<bool>) {
    drop(stop.wait_for(|stopped| *stopped).await);
}

/// One maintenance pass at `now` (Unix milliseconds). Every step runs even when an earlier
/// step freed nothing.
async fn run_maintenance_pass(index: &IndexHandle, now: i64) -> Result<(), Error> {
    let expired = index
        .storage
        .retain(now.saturating_sub(index.config.retention_ms))
        .await?;
    let size_expired = index.storage.enforce_size_limit().await?;

    let archived = index
        .storage
        .archive(now.saturating_sub(index.config.archive_after_ms))
        .await?;
    let compacted = index.storage.compact().await?;

    let size_expired = size_expired + index.storage.enforce_size_limit().await?;
    if expired + size_expired + archived + compacted > 0 {
        tracing::info!(
            index = %index.name,
            expired,
            size_expired,
            archived,
            compacted,
            "Index maintenance completed"
        );
    }

    Ok(())
}

/// The delay before the next retry after `failures` consecutive failures: the normal interval
/// (at least one second), doubled per failure, and capped at fifteen minutes.
fn retry_delay(normal_delay: Duration, failures: u32) -> Duration {
    normal_delay
        .max(MIN_RETRY_DELAY)
        .saturating_mul(1_u32 << failures.min(MAX_RETRY_DOUBLINGS))
        .min(MAX_RETRY_DELAY)
}

/// Current Unix time in milliseconds. A clock set before 1970 reads as zero.
fn unix_millis_now() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

/// Discovers the index directories under `root`, creating the layout if it is missing.
///
/// Refuses the old flat layout so an upgrade cannot silently hide existing logs.
/// The data root may itself be a canonicalized mount/symlink; managed children may not.
///
/// # Errors
///
/// Fails on a legacy flat layout, a symlink or non-directory under `indexes/`, an index name
/// that is not valid, or any filesystem error.
pub fn prepare_layout(root: &Path) -> Result<(PathBuf, Vec<String>), Error> {
    validate_literal_path(root)?;
    fs::create_dir_all(root).map_err(io_error)?;
    let root = root.canonicalize().map_err(io_error)?;
    validate_literal_path(&root)?;
    reject_legacy_layout(&root)?;

    cleanup_trash(&root)?;

    let parent = root.join("indexes");
    reject_symlink(&parent)?;
    fs::create_dir_all(&parent).map_err(io_error)?;
    let names = discover_index_names(&parent)?;
    Ok((root, names))
}

fn reject_legacy_layout(root: &Path) -> Result<(), Error> {
    for legacy in LEGACY_FLAT_ENTRIES {
        if root.join(legacy).symlink_metadata().is_ok() {
            return Err(Error::invalid(
                "legacy flat data layout detected: stop Logbrook, back up the complete data \
                 directory, then move events.duckdb, its WAL, archives and temp together into \
                 <data_dir>/indexes/default/ before restarting",
            ));
        }
    }

    Ok(())
}

fn discover_index_names(parent: &Path) -> Result<Vec<String>, Error> {
    let mut names = Vec::new();

    for entry in fs::read_dir(parent).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        let kind = entry.file_type().map_err(io_error)?;

        if kind.is_symlink() || !kind.is_dir() {
            return Err(Error::invalid(
                "indexes directory must contain only real named index directories",
            ));
        }

        // OsString carries the invalid path bytes, so retaining it would expose filesystem
        // content without adding a structured cause. The fixed error identifies the violation.
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| Error::invalid("index directory names must be UTF-8"))?;
        validate_index_name(&name)?;
        names.push(name);
    }

    Ok(names)
}

/// Creates an index directory if it is missing and checks that it holds only real files and
/// directories.
///
/// # Errors
///
/// Fails on a symlink on the path, a non-directory, a symlink or special file inside, or any
/// filesystem error.
pub fn prepare_index_directory(path: &Path) -> Result<(), Error> {
    if let Some(parent) = path.parent() {
        reject_symlink(parent)?;
    }

    reject_symlink(path)?;
    fs::create_dir_all(path).map_err(io_error)?;
    validate_index_directory(path)
}

fn validate_index_directory(path: &Path) -> Result<(), Error> {
    if let Some(parent) = path.parent() {
        reject_symlink(parent)?;
    }

    reject_symlink(path)?;

    if !path.is_dir() {
        return Err(Error::invalid(
            "managed index path must be a real directory",
        ));
    }

    let mut pending = vec![path.to_owned()];
    while let Some(directory) = pending.pop() {
        reject_symlink(&directory)?;

        for entry in fs::read_dir(directory).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let kind = entry.file_type().map_err(io_error)?;

            if kind.is_symlink() || (!kind.is_dir() && !kind.is_file()) {
                return Err(Error::invalid(
                    "managed index data must contain only regular files and directories, \
                     without symlinks",
                ));
            }

            if kind.is_dir() {
                pending.push(entry.path());
            }
        }
    }

    Ok(())
}

fn available_handle(handle: IndexHandle) -> Result<IndexHandle, Error> {
    if handle.storage.is_ready() {
        Ok(handle)
    } else {
        Err(Error::unavailable(
            "index storage is stopped or unavailable",
        ))
    }
}

/// Finishes the cleanup of earlier deletions and rejects anything in the trash that is not one.
fn cleanup_trash(root: &Path) -> Result<(), Error> {
    let trash = root.join(TRASH_DIRECTORY);
    reject_symlink(&trash)?;

    if !trash.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(&trash).map_err(io_error)? {
        let path = entry.map_err(io_error)?.path();
        let name = path.file_name().and_then(|name| name.to_str());

        if !name.is_some_and(|name| name.starts_with(TRASH_PREFIX)) {
            return Err(Error::invalid("index trash contains an unrecognized entry"));
        }

        validate_index_directory(&path)?;

        for child in fs::read_dir(&path).map_err(io_error)? {
            let child = child.map_err(io_error)?;

            if child.file_name() != TOMBSTONE_DATA || !child.file_type().map_err(io_error)?.is_dir()
            {
                return Err(Error::invalid("index trash contains unmanaged data"));
            }
        }

        if let Err(error) = remove_trash_directory(&path) {
            tracing::warn!(
                %error,
                path = %path.display(),
                "Deleted index cleanup deferred until the next startup or deletion"
            );
        }
    }

    Ok(())
}

/// A deleted index's tombstone, which holds its data until cleanup removes it.
struct Tombstone {
    /// The tombstone directory, under the trash directory.
    path: PathBuf,
    /// Set when the move could not be made durable. The index is gone from discovery either way.
    durability_error: Option<Error>,
}

/// Moves an index directory into a new tombstone and makes the move durable.
///
/// Returns the tombstone and any durability error. The rename has already happened by then, so
/// the caller unpublishes the index either way.
fn stage_deletion(root: &Path, path: &Path) -> Result<Tombstone, Error> {
    reject_symlink(root)?;

    if path.parent() != Some(root.join("indexes").as_path()) {
        return Err(Error::invalid(
            "index deletion path is outside the managed indexes directory",
        ));
    }

    validate_index_directory(path)?;

    cleanup_trash(root)?;
    let trash = root.join(TRASH_DIRECTORY);
    reject_symlink(&trash)?;
    fs::create_dir_all(&trash).map_err(io_error)?;
    sync_directory(root)?;

    let staged = tempfile::Builder::new()
        .prefix(TRASH_PREFIX)
        .tempdir_in(&trash)
        .map_err(io_error)?
        .keep();
    if let Err(error) = fs::rename(path, staged.join(TOMBSTONE_DATA)) {
        drop(fs::remove_dir(&staged));
        return Err(io_error(error));
    }

    // A rename has already removed the active directory. Even a subsequent
    // fsync failure must unpublish the stopped handle and preserve the tombstone.
    let durability_error = sync_directory(path.parent().expect("managed index parent"))
        .and_then(|()| sync_directory(&staged))
        .and_then(|()| sync_directory(&trash))
        .err()
        .map(|error| {
            Error::unavailable(format!(
                "index was deleted, but persisting its tombstone failed; \
                 cleanup will be retried on startup or deletion: {error}"
            ))
            .with_source(error)
        });
    Ok(Tombstone {
        path: staged,
        durability_error,
    })
}

/// Flushes a directory's entries to disk, so a rename or create inside it survives a crash.
fn sync_directory(path: &Path) -> Result<(), Error> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(io_error)
}

fn remove_trash_directory(path: &Path) -> Result<(), Error> {
    validate_index_directory(path)?;
    fs::remove_dir_all(path).map_err(|error| {
        Error::unavailable(format!(
            "index was deleted, but data cleanup failed and will be retried on startup \
             or deletion: {error}"
        ))
        .with_source(error)
    })
}

fn reject_symlink(path: &Path) -> Result<(), Error> {
    match path.symlink_metadata() {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(Error::invalid("managed index paths must not be symlinks"))
        }

        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}

fn io_error(error: std::io::Error) -> Error {
    Error::unavailable(format!("index filesystem operation failed: {error}")).with_source(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn slow_deletion_keeps_registry_ready_and_admitted_cancel_still_drains() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.data_dir = root.path().to_owned();
        config
            .ingest_tokens
            .insert("synthetic-lifecycle-write-token".into(), "test".into());
        config
            .read_tokens
            .insert("synthetic-lifecycle-read-token".into(), vec![]);

        let registry = IndexRegistry::open(config.clone()).await.unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = crate::http::router_with_registry(registry.clone(), config);

        let (server_stop, stopped) = tokio::sync::oneshot::channel();
        let server_registry = registry.clone();
        let server = tokio::spawn(async move {
            tokio::select! {
                result = axum::serve(listener, router).with_graceful_shutdown(async {
                    let _ = stopped.await;
                }) => result,
                _ = async {
                    while server_registry.is_ready() {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                } => Err(std::io::Error::other("storage supervisor stopped the server")),
            }
        });

        let old = registry.ensure("slow").await.unwrap();

        // Replace the supervisor after draining the real one. Its held completion
        // deterministically extends deletion beyond the server's 250ms health poll.
        let maintenance = {
            let mut entries = registry.0.entries.lock().unwrap();
            let entry = entries.get_mut("slow").unwrap();
            entry.stop.send_replace(true);
            entry.maintenance.take().unwrap()
        };
        maintenance.await.unwrap();
        let (release, blocked) = tokio::sync::oneshot::channel();
        registry
            .0
            .entries
            .lock()
            .unwrap()
            .get_mut("slow")
            .unwrap()
            .maintenance = Some(tokio::spawn(async move {
            let _ = blocked.await;
        }));

        let caller = tokio::spawn({
            let registry = registry.clone();
            async move { registry.delete("slow").await }
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            while old.storage.is_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());

        tokio::time::sleep(Duration::from_millis(350)).await;
        assert!(
            registry.is_ready(),
            "expected deletion drain must not stop the server"
        );
        assert!(registry.get("slow").is_some());
        assert!(
            !server.is_finished(),
            "slow deletion must not trip the storage supervisor"
        );

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut request = tokio::net::TcpStream::connect(address).await.unwrap();
        request
            .write_all(b"GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        request.read_to_end(&mut response).await.unwrap();
        assert!(response.starts_with(b"HTTP/1.1 200"));

        release.send(()).unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            while registry.get("slow").is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        registry.ensure("slow").await.unwrap();
        assert!(registry.is_ready());

        server_stop.send(()).unwrap();
        server.await.unwrap().unwrap();
        registry.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_shutdown_waiting_for_a_mutation_still_drains_the_registry() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.data_dir = root.path().to_owned();
        config
            .ingest_tokens
            .insert("synthetic-shutdown-write-token".into(), "test".into());
        config
            .read_tokens
            .insert("synthetic-shutdown-read-token".into(), vec![]);
        let registry = IndexRegistry::open(config).await.unwrap();
        let old = registry.get("default").unwrap();
        // An admitted mutation holds this same gate throughout its native drain.
        let mutation = registry.0.creation.clone().lock_owned().await;
        let caller = tokio::spawn({
            let registry = registry.clone();
            async move { registry.shutdown().await }
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            while !registry.0.closed.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        drop(mutation);

        tokio::time::timeout(Duration::from_secs(5), async {
            while !registry.list().is_empty() {
                tokio::task::yield_now().await;
            }

            // Shutdown releases the gate only after joining every native worker.
            let _completed = registry.0.creation.lock().await;
            old.storage.shutdown().await.unwrap();
        })
        .await
        .unwrap();

        assert!(!old.storage.is_ready());
        assert!(!registry.is_ready());
    }
}
