mod archive;
mod journal_anchor;
pub(crate) mod reclaim;
mod recovery_base;
pub(crate) mod snapshot;

use std::path::Path;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};

use protobuf::Message;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{GetEntriesContext, RaftState, Storage, StorageError};
use rocksdb::{DB, DBRecoveryMode, Direction, IteratorMode, Options, WriteBatch, WriteOptions};

use crate::{ReplicaError, checkpoint::CheckpointRef};

const CHECKPOINT: &[u8] = b"checkpoint/v1";

// This optional v1 record is two little-endian u64s and a SHA-256 digest.
fn read_checkpoint(db: &DB) -> Result<Option<CheckpointRef>, ReplicaError> {
    let Some(bytes) = db.get(CHECKPOINT)? else {
        return Ok(None);
    };
    if bytes.len() != 48 {
        return Err(invalid("invalid local checkpoint reference length"));
    }
    let (index, term, digest) = bincode::deserialize(&bytes)?;
    Ok(Some(CheckpointRef {
        index,
        term,
        digest,
    }))
}

fn term_fits(index: u64, term: u64, current: u64, checkpoint: Option<CheckpointRef>) -> bool {
    term != 0
        && term <= current
        && checkpoint.is_none_or(|cut| match index.cmp(&cut.index) {
            std::cmp::Ordering::Less => term <= cut.term,
            std::cmp::Ordering::Equal => term == cut.term,
            std::cmp::Ordering::Greater => term >= cut.term,
        })
}

// A leading zero puts the big-endian log indexes in a contiguous key range.
fn entry_key(index: u64) -> [u8; 9] {
    let mut key = [0; 9];
    key[1..].copy_from_slice(&index.to_be_bytes());
    key
}

// Rows are written by Entry::write_to_bytes. Verify that the decoded message
// has its stored length and the index named by its key, even for skipped history.
fn decode_entry(index: u64, bytes: &[u8]) -> Result<Entry, ReplicaError> {
    let entry = Entry::parse_from_bytes(bytes)?;
    if entry.index != index || entry.term == 0 || entry.compute_size() as usize != bytes.len() {
        return Err(invalid("inconsistent Raft entry"));
    }
    Ok(entry)
}

fn invalid(reason: impl Into<String>) -> ReplicaError {
    ReplicaError::Invalid(reason.into())
}

fn storage_error(error: ReplicaError) -> raft::Error {
    StorageError::Other(Box::new(error)).into()
}

fn required(db: &DB, key: &[u8]) -> Result<Vec<u8>, ReplicaError> {
    db.get(key)?.ok_or_else(|| {
        invalid(format!(
            "missing Raft metadata key {}",
            String::from_utf8_lossy(key)
        ))
    })
}

fn write_sync(db: &DB, batch: WriteBatch) -> Result<(), ReplicaError> {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    db.write_opt(batch, &options)?;
    Ok(())
}

fn db_options(create: bool) -> Options {
    let mut options = Options::default();
    options.create_if_missing(create);
    options.set_error_if_exists(create);
    options.set_paranoid_checks(true);
    // A Raft voter must not silently recover to an earlier term or vote after corruption.
    options.set_wal_recovery_mode(DBRecoveryMode::AbsoluteConsistency);
    options.set_track_and_verify_wals_in_manifest(true);
    options
}

#[derive(Clone)]
pub(crate) struct RaftStore {
    db: Arc<DB>,
    cold: Arc<reclaim::ColdState>,
    conf: ConfState,
    // Readers and cloned writers see the same bounds as the RocksDB records.
    meta: Arc<RwLock<Metadata>>,
    // Serialize optional local anchor updates independently of the Raft owner.
    anchor_writer: Arc<Mutex<()>>,
    history_pin: Arc<Mutex<std::sync::Weak<CheckpointRef>>>,
}

struct Metadata {
    hard: HardState,
    last: u64,
    last_term: u64,
    checkpoint: Option<CheckpointRef>,
    compacted: u64,
    archived: u64,
}

fn valid_membership(conf: &ConfState) -> bool {
    !conf.voters.is_empty()
        && conf.voters[0] != 0
        && conf.voters.windows(2).all(|p| p[0] < p[1])
        && conf.learners.len() <= 1
        && conf
            .learners
            .iter()
            .all(|id| *id != 0 && !conf.voters.contains(id))
        && conf.voters_outgoing.is_empty()
        && conf.learners_next.is_empty()
        && !conf.auto_leave
}

impl RaftStore {
    pub(crate) fn bootstrap(
        path: &Path,
        identity: &[u8],
        peers: &[u64],
    ) -> Result<Self, ReplicaError> {
        let mut voters = peers.to_vec();
        voters.sort_unstable();
        Self::bootstrap_members(
            path,
            identity,
            ConfState {
                voters,
                ..Default::default()
            },
        )
    }

    pub(crate) fn bootstrap_members(
        path: &Path,
        identity: &[u8],
        conf: ConfState,
    ) -> Result<Self, ReplicaError> {
        if identity.is_empty() || !valid_membership(&conf) {
            return Err(invalid(
                "invalid bootstrap identity or member configuration",
            ));
        }
        if path.exists() && std::fs::read_dir(path)?.next().is_some() {
            return Err(invalid("refusing to bootstrap a nonempty Raft directory"));
        }
        let db = DB::open(&db_options(true), path)?;
        let hard = HardState::default();
        let mut batch = WriteBatch::default();
        batch.put(b"identity/v1", identity);
        batch.put(b"configuration", conf.write_to_bytes()?);
        batch.put(b"hard", hard.write_to_bytes()?);
        batch.put(b"last", 0u64.to_be_bytes());
        write_sync(&db, batch)?;
        // A newly created database directory is itself part of the durable voter identity.
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::File::open(parent)?.sync_all()?;
        Ok(Self {
            db: Arc::new(db),
            cold: Default::default(),
            anchor_writer: Arc::default(),
            history_pin: Arc::default(),
            conf,
            meta: Arc::new(RwLock::new(Metadata {
                hard,
                last: 0,
                last_term: 0,
                checkpoint: None,
                compacted: 0,
                archived: 0,
            })),
        })
    }

    pub(crate) fn reopen(path: &Path, identity: &[u8]) -> Result<Self, ReplicaError> {
        Self::open_existing(path, identity, true)
    }

    /// Administrative rebuild from the retained log. Leave even a malformed
    /// pointer on disk until the caller confirms the complete rebuild succeeded.
    pub(crate) fn reopen_from_genesis(path: &Path, identity: &[u8]) -> Result<Self, ReplicaError> {
        Self::open_existing(path, identity, false)
    }

    fn open_existing(
        path: &Path,
        identity: &[u8],
        use_checkpoint: bool,
    ) -> Result<Self, ReplicaError> {
        if !path.join("CURRENT").is_file() {
            return Err(invalid("missing Raft database on voter restart"));
        }
        let db = DB::open(&db_options(false), path)?;
        if identity.is_empty() || required(&db, b"identity/v1")? != identity {
            return Err(invalid(
                "voter identity or configuration does not match this store",
            ));
        }
        let conf = ConfState::parse_from_bytes(&required(&db, b"configuration")?)?;
        let hard = HardState::parse_from_bytes(&required(&db, b"hard")?)?;
        let last = u64::from_be_bytes(
            required(&db, b"last")?
                .try_into()
                .map_err(|_| invalid("invalid Raft last index"))?,
        );
        if !valid_membership(&conf)
            || (hard.vote != 0 && (hard.term == 0 || !conf.voters.contains(&hard.vote)))
            || hard.commit > last
            || last == u64::MAX
        {
            return Err(invalid(
                "inconsistent voter configuration, hard state or last index",
            ));
        }
        let checkpoint = if use_checkpoint {
            read_checkpoint(&db)?
        } else {
            None
        };
        let archived = archive::last_range(&db)?;
        let durable_cut = if use_checkpoint {
            snapshot::read_compacted(&db)?
        } else {
            0
        };
        if archived > hard.commit
            || durable_cut > hard.commit
            || (use_checkpoint && durable_cut > checkpoint.map_or(0, |c| c.index))
        {
            return Err(invalid(
                "Raft history frontier outside durable checkpoint or commit",
            ));
        }
        if checkpoint.is_some_and(|c| {
            c.index == 0 || c.index > hard.commit || c.term == 0 || c.term > hard.term
        }) {
            return Err(invalid("local checkpoint outside committed Raft history"));
        }
        let cold = reclaim::ColdState::load(&db, identity)?;
        let floor = cold.through()?;
        if floor > hard.commit || (use_checkpoint && floor > durable_cut) {
            return Err(invalid(
                "physical history floor outside committed recovery boundary",
            ));
        }
        let store = Self {
            cold: Arc::new(cold),
            db: Arc::new(db),
            conf,
            anchor_writer: Arc::default(),
            history_pin: Arc::default(),
            meta: Arc::new(RwLock::new(Metadata {
                hard,
                last,
                last_term: 0,
                checkpoint,
                archived,
                compacted: durable_cut,
            })),
        };
        // Validate the entire serving suffix, including uncommitted entries.
        // Administrative repair reads from genesis through every adopted range.
        let mut index = checkpoint.map_or(1, |c| c.index);
        let mut term = 0;
        while index <= last {
            let entries = store.retained_entries(index, last + 1, Some(4 << 20))?;
            if entries.first().is_none_or(|e| e.term < term) {
                return Err(invalid("inconsistent retained Raft history"));
            }
            let end = entries.last().expect("checked nonempty page");
            term = end.term;
            index = end.index + 1;
        }
        if let Some((key, _)) = store
            .db
            .iterator(IteratorMode::From(&entry_key(last + 1), Direction::Forward))
            .next()
            .transpose()?
            && key.first() == Some(&0)
        {
            return Err(invalid("Raft log extends beyond durable last index"));
        }
        if let Some((key, _)) = store.db.iterator(IteratorMode::Start).next().transpose()?
            && key.first() == Some(&0)
            && key.as_ref() < entry_key(1).as_slice()
        {
            return Err(invalid("invalid Raft data before log index one"));
        }
        store
            .meta
            .write()
            .map_err(|_| invalid("Raft metadata lock poisoned"))?
            .last_term = term;
        if durable_cut != 0 && checkpoint.is_some_and(|cut| cut.index != durable_cut) {
            store.retained_term(durable_cut)?;
        }
        if floor != 0 && store.cold.term()? != Some(store.retained_term(floor)?) {
            return Err(invalid(
                "local reclamation boundary differs from independent base",
            ));
        }
        Ok(store)
    }

    fn metadata(&self) -> Result<RwLockReadGuard<'_, Metadata>, ReplicaError> {
        self.meta
            .read()
            .map_err(|_| invalid("Raft metadata lock poisoned"))
    }

    fn entry_term(&self, index: u64, archived: u64) -> Result<u64, ReplicaError> {
        if index == 0 {
            return Ok(0);
        }
        Ok(decode_entry(index, &self.raw_bytes(index, archived)?)?.term)
    }

    pub(crate) fn committed_index(&self) -> Result<u64, ReplicaError> {
        Ok(self.metadata()?.hard.commit)
    }

    pub(crate) fn checkpoint(&self) -> Result<Option<CheckpointRef>, ReplicaError> {
        Ok(self.metadata()?.checkpoint)
    }

    pub(crate) fn publish_checkpoint(&self, reference: CheckpointRef) -> Result<(), ReplicaError> {
        let mut meta = self
            .meta
            .write()
            .map_err(|_| invalid("Raft metadata lock poisoned"))?;
        if reference.index == 0
            || reference.index > meta.hard.commit
            || !term_fits(
                reference.index,
                reference.term,
                meta.hard.term,
                meta.checkpoint,
            )
            || self.entry_term(reference.index, meta.archived)? != reference.term
        {
            return Err(invalid(
                "local checkpoint does not match a committed Raft boundary",
            ));
        }
        // Read the durable pointer as well when an administrative open ignored it.
        // It remains protected from replacement until replay succeeds and clears it.
        let floor = self.reclaimed_through()?;
        let repairing_cold = floor > 0 && meta.checkpoint.is_none();
        let prior = if repairing_cold {
            None
        } else {
            read_checkpoint(&self.db)?
        };
        if prior != meta.checkpoint {
            return Err(invalid(
                "local checkpoint reference changed or awaits explicit rebuild",
            ));
        }
        if prior == Some(reference) {
            return Ok(());
        }
        if prior.is_some_and(|cut| cut.index >= reference.index) {
            return Err(invalid(
                "local checkpoint must advance the committed boundary",
            ));
        }
        let mut batch = WriteBatch::default();
        batch.put(
            CHECKPOINT,
            bincode::serialize(&(reference.index, reference.term, reference.digest))?,
        );
        if repairing_cold {
            batch.put(snapshot::COMPACTED, floor.to_be_bytes());
        }
        write_sync(&self.db, batch)?;
        if repairing_cold {
            meta.compacted = floor;
        }
        meta.checkpoint = Some(reference);
        Ok(())
    }

    pub(crate) fn clear_checkpoint(&self) -> Result<(), ReplicaError> {
        let mut meta = self
            .meta
            .write()
            .map_err(|_| invalid("Raft metadata lock poisoned"))?;
        if self.reclaimed_through()? != 0 {
            return Err(invalid("reclaimed history requires a sealed checkpoint"));
        }
        let mut batch = WriteBatch::default();
        batch.delete(CHECKPOINT);
        batch.delete(snapshot::COMPACTED);
        write_sync(&self.db, batch)?;
        meta.checkpoint = None;
        meta.compacted = 0;
        Ok(())
    }

    pub(crate) fn persist_ready(
        &self,
        entries: &[Entry],
        hs: Option<&HardState>,
    ) -> Result<bool, ReplicaError> {
        self.persist_ready_bytes(entries, hs, None)
    }

    /// Recovery supplies decoded headers together with their authenticated wire
    /// frames. Re-encoding protobufs could reorder fields and break raw history proofs.
    pub(crate) fn persist_recovery(
        &self,
        group: &ledger_recovery::JournalBatch,
        hs: &HardState,
    ) -> Result<bool, ReplicaError> {
        self.persist_ready_bytes(group.entries(), Some(hs), Some(group))
    }

    fn persist_ready_bytes(
        &self,
        entries: &[Entry],
        hs: Option<&HardState>,
        raw: Option<&ledger_recovery::JournalBatch>,
    ) -> Result<bool, ReplicaError> {
        if entries.is_empty() && hs.is_none() {
            return Ok(false);
        }
        let mut meta = self
            .meta
            .write()
            .map_err(|_| invalid("Raft metadata lock poisoned"))?;
        let hard = hs.unwrap_or(&meta.hard);
        let last = entries.last().map_or(meta.last, |entry| entry.index);
        if hard.term < meta.hard.term
            || hard.commit < meta.hard.commit
            || hard.commit > last
            || (hard.term == meta.hard.term && meta.hard.vote != 0 && hard.vote != meta.hard.vote)
            || (hard.vote != 0 && (hard.term == 0 || !self.conf.voters.contains(&hard.vote)))
        {
            return Err(invalid("invalid Raft hard state transition"));
        }
        let mut batch = WriteBatch::default();
        if let Some(first) = entries.first() {
            if first.index <= meta.hard.commit || first.index > meta.last + 1 || last == u64::MAX {
                return Err(invalid(
                    "Raft append would overwrite committed data, skip an index or exhaust log indexes",
                ));
            }
            let mut previous = first.index - 1;
            let mut term = if previous == meta.last {
                meta.last_term
            } else {
                self.entry_term(previous, meta.archived)?
            };
            let mut originals = raw.map(|g| g.encoded());
            for entry in entries {
                if previous.checked_add(1) != Some(entry.index)
                    || entry.term == 0
                    || entry.term < term
                    || entry.term > hard.term
                {
                    return Err(invalid(
                        "Raft append has noncontiguous indexes or invalid terms",
                    ));
                }
                match &mut originals {
                    Some(rows) => batch.put(
                        entry_key(entry.index),
                        rows.next()
                            .expect("authenticated entry and frame share one slot"),
                    ),
                    None => batch.put(entry_key(entry.index), entry.write_to_bytes()?),
                }
                previous = entry.index;
                term = entry.term;
            }
            if last < meta.last {
                batch.delete_range(entry_key(last + 1), entry_key(meta.last + 1));
            }
            batch.put(b"last", last.to_be_bytes());
        }
        if let Some(hs) = hs {
            batch.put(b"hard", hs.write_to_bytes()?);
        }
        write_sync(&self.db, batch)?;
        if let Some(hs) = hs {
            meta.hard = hs.clone();
        }
        meta.last = last;
        if let Some(entry) = entries.last() {
            meta.last_term = entry.term;
        }
        Ok(true)
    }

    pub(crate) fn persist_commit(&self, index: u64) -> Result<bool, ReplicaError> {
        let mut meta = self
            .meta
            .write()
            .map_err(|_| invalid("Raft metadata lock poisoned"))?;
        if index < meta.hard.commit || index > meta.last {
            return Err(invalid("invalid durable commit index"));
        }
        if index == meta.hard.commit {
            return Ok(false);
        }
        let mut hard = meta.hard.clone();
        hard.commit = index;
        let mut batch = WriteBatch::default();
        batch.put(b"hard", hard.write_to_bytes()?);
        write_sync(&self.db, batch)?;
        meta.hard = hard;
        Ok(true)
    }
}

impl Storage for RaftStore {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState::new(
            self.metadata().map_err(storage_error)?.hard.clone(),
            self.conf.clone(),
        ))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        self.log_entries(low, high, max_size.into(), true)
    }
    fn term(&self, index: u64) -> raft::Result<u64> {
        self.log_term(index, true)
    }
    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.metadata().map_err(storage_error)?.compacted + 1)
    }
    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.metadata().map_err(storage_error)?.last)
    }
    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        let reference = self
            .metadata()
            .map_err(storage_error)?
            .checkpoint
            .filter(|c| c.index >= request_index)
            .ok_or(StorageError::SnapshotTemporarilyUnavailable)?;
        let mut snapshot = Snapshot::default();
        let metadata = snapshot.mut_metadata();
        metadata.index = reference.index;
        metadata.term = reference.term;
        metadata.set_conf_state(self.conf.clone());
        snapshot.data = reference.snapshot_token().into();
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn entry(index: u64, term: u64) -> Entry {
        Entry {
            index,
            term,
            data: index.to_be_bytes().to_vec().into(),
            ..Default::default()
        }
    }

    fn state(term: u64, vote: u64, commit: u64) -> HardState {
        HardState {
            term,
            vote,
            commit,
            ..Default::default()
        }
    }

    #[test]
    fn fixed_membership_identity_and_empty_log_survive_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("voter");
        assert!(RaftStore::reopen(&path, b"cluster:1").is_err());
        assert!(!path.exists());
        std::fs::create_dir(&path).unwrap();
        assert!(RaftStore::reopen(&path, b"cluster:1").is_err());
        assert!(std::fs::read_dir(&path).unwrap().next().is_none());
        let store = RaftStore::bootstrap(&path, b"cluster:1", &[1, 2, 3]).unwrap();
        assert_eq!(store.first_index().unwrap(), 1);
        assert_eq!(store.last_index().unwrap(), 0);
        assert_eq!(store.term(0).unwrap(), 0);
        drop(store);
        assert!(RaftStore::bootstrap(&path, b"cluster:1", &[1, 2, 3]).is_err());
        assert!(RaftStore::reopen(&path, b"cluster:2").is_err());
        let store = RaftStore::reopen(&path, b"cluster:1").unwrap();
        assert_eq!(store.initial_state().unwrap().conf_state.voters, [1, 2, 3]);
        assert_eq!(store.committed_index().unwrap(), 0);
        assert!(matches!(
            store.snapshot(1, 2),
            Err(raft::Error::Store(
                StorageError::SnapshotTemporarilyUnavailable
            ))
        ));
    }

    #[test]
    fn persistence_reports_only_completed_wal_batches() {
        let dir = tempdir().unwrap();
        let store = RaftStore::bootstrap(&dir.path().join("voter"), b"one", &[1, 2, 3]).unwrap();
        assert!(!store.persist_ready(&[], None).unwrap());
        assert!(store.persist_ready(&[], Some(&state(1, 1, 0))).unwrap());
        assert!(
            store
                .persist_ready(&[entry(1, 1), entry(2, 1)], None)
                .unwrap()
        );
        assert!(!store.persist_commit(0).unwrap());
        assert!(store.persist_commit(2).unwrap());
        assert!(!store.persist_commit(2).unwrap());
        assert!(store.persist_commit(3).is_err());
    }

    #[test]
    fn overwritten_suffix_and_light_ready_commit_survive_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("voter");
        let store = RaftStore::bootstrap(&path, b"one", &[1, 2, 3]).unwrap();
        let old: Vec<_> = (1..=12).map(|i| entry(i, 1)).collect();
        store.persist_ready(&old, Some(&state(1, 1, 8))).unwrap();
        let suffix = [entry(9, 2), entry(10, 2)];
        store.persist_ready(&suffix, Some(&state(2, 2, 9))).unwrap();
        store.persist_commit(10).unwrap();
        drop(store);
        let store = RaftStore::reopen(&path, b"one").unwrap();
        assert_eq!(store.last_index().unwrap(), 10);
        assert_eq!(store.initial_state().unwrap().hard_state, state(2, 2, 10));
        assert_eq!(store.term(8).unwrap(), 1);
        assert_eq!(store.term(9).unwrap(), 2);
        assert!(matches!(
            store.term(11),
            Err(raft::Error::Store(StorageError::Unavailable))
        ));
        assert_eq!(
            store
                .entries(9, 11, None, GetEntriesContext::empty(false))
                .unwrap(),
            suffix
        );
        assert_eq!(
            store
                .entries(9, 11, Some(0), GetEntriesContext::empty(false))
                .unwrap(),
            [suffix[0].clone()]
        );
    }

    #[test]
    fn storage_serves_only_a_published_checkpoint_covering_the_requested_index() {
        let dir = tempdir().unwrap();
        let store = populated(&dir.path().join("voter"));
        let seal = cut(4, 1);
        store.publish_checkpoint(seal).unwrap();
        let image = store.snapshot(4, 2).unwrap();
        assert_eq!(image.get_metadata().index, 4);
        assert_eq!(image.get_metadata().term, 1);
        assert_eq!(image.get_metadata().get_conf_state(), &store.conf);
        assert!(!image.data.is_empty());
        assert!(matches!(
            store.snapshot(5, 2),
            Err(raft::Error::Store(
                StorageError::SnapshotTemporarilyUnavailable
            ))
        ));
    }

    #[test]
    fn refuses_gaps_committed_rewrites_and_double_votes_without_partial_writes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("voter");
        let store = RaftStore::bootstrap(&path, b"one", &[1, 2, 3]).unwrap();
        store
            .persist_ready(&[entry(1, 1), entry(2, 1)], Some(&state(1, 1, 1)))
            .unwrap();
        for (entries, hs) in [
            (vec![entry(4, 2)], state(2, 2, 2)),
            (vec![entry(3, 2), entry(5, 2)], state(2, 2, 2)),
            (vec![entry(1, 2)], state(2, 2, 1)),
            (vec![], state(1, 2, 1)),
            (vec![], state(1, 0, 1)),
            (vec![], state(1, 1, 0)),
            (vec![], state(2, 2, 3)),
        ] {
            assert!(store.persist_ready(&entries, Some(&hs)).is_err());
        }
        assert!(store.persist_commit(3).is_err());
        assert!(store.persist_commit(0).is_err());
        drop(store);
        let store = RaftStore::reopen(&path, b"one").unwrap();
        assert_eq!(store.last_index().unwrap(), 2);
        assert_eq!(store.initial_state().unwrap().hard_state, state(1, 1, 1));
    }

    #[test]
    fn refuses_recovery_from_a_log_with_a_missing_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("voter");
        let store = RaftStore::bootstrap(&path, b"one", &[1, 2, 3]).unwrap();
        store
            .persist_ready(
                &[entry(1, 1), entry(2, 1), entry(3, 1)],
                Some(&state(1, 1, 3)),
            )
            .unwrap();
        store.db.delete(entry_key(2)).unwrap();
        drop(store);
        assert!(RaftStore::reopen(&path, b"one").is_err());
    }

    fn cut(index: u64, term: u64) -> CheckpointRef {
        CheckpointRef {
            index,
            term,
            digest: [index as u8; 32],
        }
    }

    fn populated(path: &Path) -> RaftStore {
        let store = RaftStore::bootstrap(path, b"one", &[1, 2, 3]).unwrap();
        let entries = (1..=8)
            .map(|i| entry(i, if i < 5 { 1 } else { 2 }))
            .collect::<Vec<_>>();
        store
            .persist_ready(&entries, Some(&state(2, 1, 6)))
            .unwrap();
        store
    }

    #[test]
    fn only_monotonic_committed_checkpoints_can_be_published() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("voter");
        let store = populated(&path);
        let old = cut(3, 1);
        assert_eq!(store.checkpoint().unwrap(), None);
        store.publish_checkpoint(old).unwrap();
        store.publish_checkpoint(old).unwrap();
        for bad in [
            cut(0, 0),
            cut(4, 0),
            cut(2, 1),
            cut(5, 1),
            cut(7, 2),
            cut(99, 2),
            CheckpointRef {
                digest: [42; 32],
                ..old
            },
        ] {
            assert!(store.publish_checkpoint(bad).is_err());
            assert_eq!(store.checkpoint().unwrap(), Some(old));
        }
        let next = cut(6, 2);
        store.publish_checkpoint(next).unwrap();
        drop(store);
        let recovered = RaftStore::reopen(&path, b"one").unwrap();
        assert_eq!(recovered.checkpoint().unwrap(), Some(next));
        assert_eq!(recovered.last_index().unwrap(), 8);
        assert_eq!(recovered.committed_index().unwrap(), 6);
        assert_eq!(
            recovered
                .entries(7, 9, None, GetEntriesContext::empty(false))
                .unwrap(),
            [entry(7, 2), entry(8, 2)]
        );
    }

    #[test]
    fn checkpoint_restart_checks_its_boundary_and_the_entire_suffix_even_past_commit() {
        for damaged in [4, 5, 8] {
            let dir = tempdir().unwrap();
            let path = dir.path().join("voter");
            let store = populated(&path);
            store.publish_checkpoint(cut(4, 1)).unwrap();
            if damaged == 4 {
                store
                    .db
                    .put(entry_key(4), entry(4, 2).write_to_bytes().unwrap())
                    .unwrap();
            } else {
                store.db.delete(entry_key(damaged)).unwrap();
            }
            drop(store);
            assert!(RaftStore::reopen(&path, b"one").is_err());
        }
    }

    #[test]
    fn checkpoint_skips_old_bytes_on_restart_but_old_log_reads_and_full_rebuild_check_them() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("voter");
        let store = populated(&path);
        let pointer = cut(5, 2);
        store.publish_checkpoint(pointer).unwrap();
        store.db.put(entry_key(1), [0xff]).unwrap();
        store.db.delete(entry_key(2)).unwrap();
        let mut duplicated = entry(3, 1).write_to_bytes().unwrap();
        duplicated.extend_from_within(..);
        store.db.put(entry_key(3), duplicated).unwrap();
        drop(store);
        let store = RaftStore::reopen(&path, b"one").unwrap();
        assert_eq!(store.first_index().unwrap(), 1);
        for index in [1, 2, 3] {
            assert!(
                store
                    .entries(index, index + 1, None, GetEntriesContext::empty(false))
                    .is_err()
            );
            assert!(store.term(index).is_err());
        }
        assert_eq!(
            store
                .entries(4, 5, None, GetEntriesContext::empty(false))
                .unwrap(),
            [entry(4, 1)]
        );
        drop(store);
        assert!(RaftStore::reopen_from_genesis(&path, b"one").is_err());
        assert_eq!(
            RaftStore::reopen(&path, b"one")
                .unwrap()
                .checkpoint()
                .unwrap(),
            Some(pointer)
        );
    }

    #[test]
    fn malformed_pointer_can_be_ignored_for_explicit_full_verification_then_cleared() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("voter");
        let store = populated(&path);
        store.publish_checkpoint(cut(4, 1)).unwrap();
        store.db.put(CHECKPOINT, b"torn").unwrap();
        drop(store);
        assert!(RaftStore::reopen(&path, b"one").is_err());
        let repair = RaftStore::reopen_from_genesis(&path, b"one").unwrap();
        assert_eq!(repair.checkpoint().unwrap(), None);
        assert_eq!(repair.db.get(CHECKPOINT).unwrap().unwrap(), b"torn");
        assert!(repair.publish_checkpoint(cut(4, 1)).is_err());
        repair.clear_checkpoint().unwrap();
        drop(repair);
        let repaired = RaftStore::reopen(&path, b"one").unwrap();
        assert_eq!(repaired.checkpoint().unwrap(), None);
        assert_eq!(repaired.last_index().unwrap(), 8);
    }

    #[test]
    fn a_decodable_pointer_still_requires_a_positive_committed_index_and_matching_term() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("voter");
        let mut repair = populated(&path);
        for bad in [cut(0, 1), cut(7, 2), cut(4, 2), cut(6, 0)] {
            repair
                .db
                .put(
                    CHECKPOINT,
                    bincode::serialize(&(bad.index, bad.term, bad.digest)).unwrap(),
                )
                .unwrap();
            drop(repair);
            assert!(RaftStore::reopen(&path, b"one").is_err());
            repair = RaftStore::reopen_from_genesis(&path, b"one").unwrap();
        }
    }
}
