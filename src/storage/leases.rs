//! Reader generations defer archive unlink until every older snapshot ends.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Default)]
pub(super) struct ArchiveLeases {
    epoch: u64,
    active: BTreeMap<u64, usize>,
    pending: Vec<(u64, String)>,
}

impl ArchiveLeases {
    /// The caller holds the registration lock while establishing its database
    /// snapshot. This method must not acquire that lock again.
    pub(super) fn register(&mut self, gate: &Arc<Mutex<Self>>) -> ReadLease {
        *self.active.entry(self.epoch).or_default() += 1;
        ReadLease {
            gate: gate.clone(),
            epoch: self.epoch,
        }
    }

    pub(super) fn retire(&mut self, paths: Vec<String>) {
        let retired = self.epoch;
        self.epoch += 1;
        self.pending
            .extend(paths.into_iter().map(|path| (retired, path)));
    }

    pub(super) fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub(super) fn has_readers(&self) -> bool {
        !self.active.is_empty()
    }

    /// Pure generation decision: a newer reader cannot see a retired file.
    fn generation_is_unused(&self, retired: u64) -> bool {
        self.active.range(..=retired).next().is_none()
    }

    #[cfg(test)]
    fn eligible(&mut self) -> Vec<(u64, String)> {
        let pending = std::mem::take(&mut self.pending);
        let (ready, waiting) = pending
            .into_iter()
            .partition(|(epoch, _)| self.generation_is_unused(*epoch));
        self.pending = waiting;

        ready
    }
}

pub(super) fn cleanup_archives(gate: &Arc<Mutex<ArchiveLeases>>, dir: &Path) {
    // Cleanup is opportunistic: a writer finalizing a commit must not prevent
    // maintenance from observing its deadline or receiving Stop. Keep pending
    // entries until successful unlink bookkeeping can also acquire the gate.
    let ready: Vec<_> = {
        let Ok(state) = gate.try_lock() else { return };
        state
            .pending
            .iter()
            .filter(|(epoch, _)| state.generation_is_unused(*epoch))
            .cloned()
            .collect()
    };

    let mut completed = BTreeSet::new();

    for item in ready {
        match std::fs::remove_file(dir.join(&item.1)) {
            Ok(()) => completed.insert(item),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => completed.insert(item),
            Err(_) => false,
        };
    }

    if let Ok(mut state) = gate.try_lock() {
        state.pending.retain(|item| !completed.contains(item));
    }
}

pub(super) struct ReadLease {
    gate: Arc<Mutex<ArchiveLeases>>,
    epoch: u64,
}

impl Drop for ReadLease {
    fn drop(&mut self) {
        let mut state = self.gate.lock().unwrap();
        let count = state.active.get_mut(&self.epoch).unwrap();
        *count -= 1;

        if *count == 0 {
            state.active.remove(&self.epoch);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generations_wait_for_every_older_reader_but_not_newer_readers() {
        let gate = Arc::new(Mutex::new(ArchiveLeases::default()));
        let first = gate.lock().unwrap().register(&gate);
        let second = gate.lock().unwrap().register(&gate);
        gate.lock().unwrap().retire(vec!["first.parquet".into()]);
        let middle = gate.lock().unwrap().register(&gate);
        gate.lock().unwrap().retire(vec!["second.parquet".into()]);
        let newest = gate.lock().unwrap().register(&gate);

        drop(first);

        assert!(gate.lock().unwrap().eligible().is_empty());

        drop(second);

        assert_eq!(
            gate.lock().unwrap().eligible(),
            vec![(0, "first.parquet".into())]
        );

        drop(middle);

        assert_eq!(
            gate.lock().unwrap().eligible(),
            vec![(1, "second.parquet".into())]
        );
        assert!(gate.lock().unwrap().has_readers());

        drop(newest);

        assert!(!gate.lock().unwrap().has_readers());
    }

    #[test]
    fn failed_unlink_is_retried_and_missing_files_are_completed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("retry.parquet");
        std::fs::create_dir(&path).unwrap();
        let gate = Arc::new(Mutex::new(ArchiveLeases::default()));
        gate.lock()
            .unwrap()
            .retire(vec!["retry.parquet".into(), "absent.parquet".into()]);

        cleanup_archives(&gate, dir.path());

        assert_eq!(gate.lock().unwrap().pending_count(), 1);
        std::fs::remove_dir(&path).unwrap();
        std::fs::write(&path, b"retry").unwrap();

        cleanup_archives(&gate, dir.path());

        assert_eq!(gate.lock().unwrap().pending_count(), 0);
        assert!(!path.exists());
    }

    #[test]
    fn unwinding_query_releases_its_reader_registration() {
        let gate = Arc::new(Mutex::new(ArchiveLeases::default()));
        let result = std::panic::catch_unwind(|| {
            let _lease = gate.lock().unwrap().register(&gate);
            panic!("query failed");
        });

        assert!(result.is_err());
        assert!(!gate.lock().unwrap().has_readers());
    }
}
