//! Durable snapshot handoff. Incoming history is keyed by the authenticated
//! manifest digest and invisible until Ready adopts the range in its WAL batch.
use super::{
    archive::{archive_key, range_key},
    *,
};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

mod replay;
pub use replay::{MAX_REPLAY_BYTES, MAX_REPLAY_ENTRIES, snapshot_replay_candidate};

pub(super) const COMPACTED: &[u8] = b"replication-compacted/v1";
const STAGED: &[u8] = b"snapshot-history-stage/v1";
const PAGE_BYTES: u64 = 2 << 20;
const PAGE_ENTRIES: u64 = 64;
// Entries cannot be split. The existing peer wire protocol accepts a single
// append message up to 4 MiB, including any legacy Raft context bytes.
const MAX_ENTRY_BYTES: u64 = 4 << 20;

pub(super) fn read_compacted(db: &DB) -> Result<u64, ReplicaError> {
    db.get(COMPACTED)?
        .map(|v| {
            v.try_into()
                .map(u64::from_be_bytes)
                .map_err(|_| invalid("invalid replication frontier"))
        })
        .transpose()
        .map(|n| n.unwrap_or(0))
}
#[derive(Clone, Serialize, Deserialize)]
struct Progress {
    remote: CheckpointRef,
    base: u64,
    through: u64,
    term: u64,
}
fn progress(db: &DB) -> Result<Option<Progress>, ReplicaError> {
    let Some(bytes) = db.get(STAGED)? else {
        return Ok(None);
    };
    if bytes.len() != 72 {
        return Err(invalid("invalid snapshot history stage size"));
    }
    let p: Progress = bincode::deserialize(&bytes)?;
    if p.base > p.through
        || p.through > p.remote.index
        || p.remote.index == u64::MAX
        || p.remote.index == 0
        || p.remote.term == 0
        || (p.through == 0) != (p.term == 0)
        || p.term > p.remote.term
    {
        return Err(invalid("invalid snapshot history stage frontier"));
    }
    Ok(Some(p))
}
fn save_progress(batch: &mut WriteBatch, p: &Progress) -> Result<(), ReplicaError> {
    batch.put(STAGED, bincode::serialize(p)?);
    Ok(())
}

fn delete_staged_rows(batch: &mut WriteBatch, p: &Progress) {
    if p.base < p.through {
        batch.delete_range(
            archive_key(&p.remote, p.base + 1),
            archive_key(&p.remote, p.through + 1),
        );
    }
}

/// Bounded raw history transfer for a single voter. The caller retains the
/// matching sealed checkpoint while serving its authenticated peer.
#[derive(Clone)]
pub struct SnapshotHistory {
    store: RaftStore,
}
/// Pins one resumable incoming history against replacement by another offer.
/// A stage contains no Raft authority prior to the receiver's Ready transaction.
pub struct HistoryStage {
    replay_verified_through: std::sync::atomic::AtomicU64,
    store: RaftStore,
    remote: Arc<CheckpointRef>,
}

impl RaftStore {
    pub(crate) fn committed_page<T>(
        &self,
        after: u64,
        through: u64,
        output: impl FnMut(Entry, Box<[u8]>) -> T,
    ) -> Result<Vec<T>, ReplicaError> {
        if after > through || through == u64::MAX || through > self.committed_index()? {
            return Err(invalid("export exceeds durable committed history"));
        }
        let end = through.min(after.saturating_add(PAGE_ENTRIES));
        Ok(self.log_rows(
            after + 1..end + 1,
            Some(PAGE_BYTES),
            false,
            MAX_ENTRY_BYTES,
            output,
        )?)
    }
}

impl SnapshotHistory {
    /// The exact, immutable membership of this Raft generation.
    pub fn membership(&self) -> &ConfState {
        &self.store.conf
    }

    pub fn permits_message(&self, from: u64, to: u64, kind: raft::eraftpb::MessageType) -> bool {
        let conf = self.membership();
        let member = |id| conf.voters.contains(&id) || conf.learners.contains(&id);
        if from == to || !member(from) || !member(to) {
            return false;
        }
        crate::replica::membership::role_permits(conf.learners.first().copied(), from, to, kind)
    }

    /// Reclaim an expired offer's unadopted history. A live transfer or Ready
    /// still holding this stage defers cleanup. Ready atomically clears STAGED
    /// when it adopts the rows, so an old receipt cannot remove published data.
    pub(crate) fn discard_stage(&self, reference: CheckpointRef) -> Result<bool, ReplicaError> {
        let store = &self.store;
        let pin = store
            .history_pin
            .lock()
            .map_err(|_| invalid("history stage lock poisoned"))?;
        let Some(p) = progress(&store.db)?.filter(|p| p.remote == reference) else {
            return Ok(true);
        };
        if pin.upgrade().is_some() {
            return Ok(false);
        }
        let mut batch = WriteBatch::default();
        delete_staged_rows(&mut batch, &p);
        batch.delete(STAGED);
        write_sync(&store.db, batch)?;
        Ok(true)
    }

    pub fn entries_after(
        &self,
        reference: CheckpointRef,
        after: u64,
    ) -> Result<Vec<Entry>, ReplicaError> {
        self.read_page(reference, after, |entry, _| entry)
    }

    /// Forward the original WAL byte representation, including protobuf field
    /// order and unknown fields, after validating each stored entry.
    pub fn encoded_entries_after(
        &self,
        reference: CheckpointRef,
        after: u64,
    ) -> Result<Vec<Vec<u8>>, ReplicaError> {
        self.read_page(reference, after, |_, bytes| bytes.into_vec())
    }

    fn read_page<T>(
        &self,
        reference: CheckpointRef,
        after: u64,
        output: impl FnMut(Entry, Box<[u8]>) -> T,
    ) -> Result<Vec<T>, ReplicaError> {
        let store = &self.store;
        if reference.index == 0
            || reference.index == u64::MAX
            || after > reference.index
            || reference.index > store.committed_index()?
            || store.retained_term(reference.index)? != reference.term
        {
            return Err(invalid(
                "history transfer outside committed checkpoint boundary",
            ));
        }
        store.committed_page(after, reference.index, output)
    }

    pub fn begin(&self, reference: CheckpointRef) -> Result<HistoryStage, ReplicaError> {
        let store = &self.store;
        let mut pin = store
            .history_pin
            .lock()
            .map_err(|_| invalid("history stage lock poisoned"))?;
        if reference.index == 0 || reference.index == u64::MAX || reference.term == 0 {
            return Err(invalid("invalid history transfer boundary"));
        }
        let remote = match pin.upgrade() {
            Some(active) if *active == reference => active,
            Some(_) => return Err(invalid("another snapshot history is pinned")),
            None => Arc::new(reference),
        };
        let old = progress(&store.db)?;
        if old.as_ref().is_none_or(|p| p.remote != reference) {
            let meta = store.metadata()?;
            let base = reference.index.min(meta.hard.commit);
            let archived = meta.archived;
            drop(meta);
            let term = store.entry_term(base, archived)?;
            if term > reference.term || (base == reference.index && term != reference.term) {
                return Err(invalid(
                    "snapshot conflicts with an existing committed term",
                ));
            }
            let mut batch = WriteBatch::default();
            if let Some(old) = old {
                delete_staged_rows(&mut batch, &old);
            }
            save_progress(
                &mut batch,
                &Progress {
                    remote: reference,
                    base,
                    through: base,
                    term,
                },
            )?;
            write_sync(&store.db, batch)?;
        }
        *pin = Arc::downgrade(&remote);
        Ok(HistoryStage {
            replay_verified_through: Default::default(),
            store: store.clone(),
            remote,
        })
    }
}
impl HistoryStage {
    pub(crate) fn belongs_to(&self, store: &RaftStore, reference: CheckpointRef) -> bool {
        Arc::ptr_eq(&self.store.db, &store.db) && *self.remote == reference
    }
    fn current_progress(
        &self,
        pin: &std::sync::Weak<CheckpointRef>,
    ) -> Result<Progress, ReplicaError> {
        if !pin.ptr_eq(&Arc::downgrade(&self.remote)) {
            return Err(invalid("snapshot history lease was replaced"));
        }
        progress(&self.store.db)?
            .filter(|p| p.remote == *self.remote)
            .ok_or_else(|| invalid("snapshot history stage no longer current"))
    }
    fn progress(&self) -> Result<Progress, ReplicaError> {
        let pin = self
            .store
            .history_pin
            .lock()
            .map_err(|_| invalid("history stage lock poisoned"))?;
        self.current_progress(&pin)
    }
    pub fn resume_after(&self) -> Result<u64, ReplicaError> {
        Ok(self.progress()?.through)
    }
    pub fn append(&self, entries: &[Entry]) -> Result<(), ReplicaError> {
        self.append_page(
            entries,
            |e| u64::from(e.compute_size()),
            |e, index| {
                if e.index != index {
                    return Err(invalid("unexpected snapshot history entry index"));
                }
                Ok((e.term, Cow::Owned(e.write_to_bytes()?)))
            },
        )
    }

    /// Check wire encodings, then persist exactly those bytes in the same
    /// synchronous batch as the resumable position.
    pub fn append_encoded(&self, entries: &[Vec<u8>]) -> Result<(), ReplicaError> {
        self.append_page(
            entries,
            |b| b.len() as u64,
            |b, index| Ok((decode_entry(index, b)?.term, Cow::Borrowed(b.as_slice()))),
        )
    }

    fn append_page<'a, T>(
        &self,
        entries: &'a [T],
        size: impl Fn(&T) -> u64,
        checked_row: impl Fn(&'a T, u64) -> Result<(u64, Cow<'a, [u8]>), ReplicaError>,
    ) -> Result<(), ReplicaError> {
        let pin = self
            .store
            .history_pin
            .lock()
            .map_err(|_| invalid("history stage lock poisoned"))?;
        let mut p = self.current_progress(&pin)?;
        if entries.is_empty() || entries.len() as u64 > PAGE_ENTRIES {
            return Err(invalid("invalid snapshot history page count"));
        }
        let mut bytes = 0u64;
        let mut batch = WriteBatch::default();
        for entry in entries {
            let size = size(entry);
            bytes = bytes.saturating_add(size);
            let index = p.through + 1;
            if (entries.len() > 1 && bytes > PAGE_BYTES)
                || size > MAX_ENTRY_BYTES
                || index > p.remote.index
            {
                return Err(invalid(
                    "snapshot history entry outside page or index limit",
                ));
            }
            let (term, encoded) = checked_row(entry, index)?;
            if term == 0
                || term < p.term
                || term > p.remote.term
                || (index == p.remote.index && term != p.remote.term)
            {
                return Err(invalid("invalid snapshot history entry term"));
            }
            batch.put(archive_key(&p.remote, index), encoded.as_ref());
            p.through = index;
            p.term = term;
        }
        save_progress(&mut batch, &p)?;
        write_sync(&self.store.db, batch)
    }
    pub fn finish(&self) -> Result<(), ReplicaError> {
        let p = self.progress()?;
        if p.through != p.remote.index || p.term != p.remote.term {
            return Err(invalid("snapshot history has not reached its seal"));
        }
        Ok(())
    }
}
impl RaftStore {
    pub(crate) fn snapshot_history(&self) -> SnapshotHistory {
        SnapshotHistory {
            store: self.clone(),
        }
    }
    pub(crate) fn compact_replication(&self, through: u64) -> Result<(), ReplicaError> {
        let mut meta = self
            .meta
            .write()
            .map_err(|_| invalid("Raft metadata lock poisoned"))?;
        if through <= meta.compacted {
            return Ok(());
        }
        if meta.checkpoint.is_none_or(|c| c.index < through) {
            return Err(invalid("replication cut exceeds published checkpoint"));
        }
        let mut batch = WriteBatch::default();
        batch.put(COMPACTED, through.to_be_bytes());
        write_sync(&self.db, batch)?;
        meta.compacted = through;
        Ok(())
    }
    pub(crate) fn persist_snapshot_ready(
        &self,
        snapshot: &Snapshot,
        local: CheckpointRef,
        history: &HistoryStage,
        entries: &[Entry],
        hs: Option<&HardState>,
    ) -> Result<(), ReplicaError> {
        let remote = CheckpointRef::from_snapshot(snapshot)?;
        if snapshot.get_metadata().get_conf_state() != &self.conf
            || local.index != remote.index
            || local.term != remote.term
            || !history.belongs_to(self, remote)
        {
            return Err(invalid(
                "snapshot membership, local seal or history proof mismatch",
            ));
        }
        // Lock order matches begin(). Append workers never take the owner lock.
        let mut pin = self
            .history_pin
            .lock()
            .map_err(|_| invalid("history stage lock poisoned"))?;
        let p = history.current_progress(&pin)?;
        let mut meta = self
            .meta
            .write()
            .map_err(|_| invalid("Raft metadata lock poisoned"))?;
        let old_commit = meta.hard.commit;
        if p.through != remote.index
            || p.term != remote.term
            || remote.index < old_commit
            || p.base > old_commit
            || meta.checkpoint.is_some_and(|c| c.index > remote.index)
        {
            return Err(invalid("incomplete or obsolete Raft snapshot"));
        }
        history.check_newly_committed(&meta)?;
        // Normal appends may have committed while the files were transferring.
        // A matching committed log entry identifies its Raft prefix. Compare
        // its exact local bytes as well before adopting the remaining suffix.
        if old_commit > p.base {
            let shared = self
                .db
                .get(archive_key(&remote, old_commit))?
                .ok_or_else(|| invalid("snapshot overlap is missing"))?;
            if shared != self.raw_bytes(old_commit, meta.archived)? {
                return Err(invalid("snapshot overlaps a different committed prefix"));
            }
        }
        let mut hard = hs.cloned().unwrap_or_else(|| meta.hard.clone());
        if hs.is_none() {
            hard.commit = remote.index;
        }
        let last = entries.last().map_or(remote.index, |e| e.index);
        if last == u64::MAX
            || hard.term < remote.term
            || hard.term < meta.hard.term
            || hard.commit < remote.index
            || hard.commit > last
            || (hard.term == meta.hard.term && meta.hard.vote != 0 && hard.vote != meta.hard.vote)
            || (hard.vote != 0 && !self.conf.voters.contains(&hard.vote))
        {
            return Err(invalid("invalid snapshot Ready hard state"));
        }
        let mut batch = WriteBatch::default();
        if old_commit < meta.last {
            batch.delete_range(entry_key(old_commit + 1), entry_key(meta.last + 1));
        }
        if old_commit < remote.index {
            batch.put(range_key(old_commit + 1), remote.snapshot_token());
        }
        if p.base < old_commit {
            batch.delete_range(
                archive_key(&remote, p.base + 1),
                archive_key(&remote, old_commit + 1),
            );
        }
        let mut index = remote.index;
        let mut term = remote.term;
        for e in entries {
            if index.checked_add(1) != Some(e.index)
                || e.index == u64::MAX
                || e.term < term
                || e.term > hard.term
            {
                return Err(invalid("invalid snapshot Ready suffix"));
            }
            batch.put(entry_key(e.index), e.write_to_bytes()?);
            index = e.index;
            term = e.term;
        }
        batch.put(
            CHECKPOINT,
            bincode::serialize(&(local.index, local.term, local.digest))?,
        );
        batch.put(COMPACTED, remote.index.to_be_bytes());
        batch.put(b"hard", hard.write_to_bytes()?);
        batch.put(b"last", last.to_be_bytes());
        batch.delete(STAGED);
        write_sync(&self.db, batch)?;
        *pin = std::sync::Weak::new();
        if old_commit < remote.index {
            meta.archived = remote.index;
        }
        meta.checkpoint = Some(local);
        meta.compacted = remote.index;
        meta.last = last;
        meta.last_term = term;
        meta.hard = hard;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
