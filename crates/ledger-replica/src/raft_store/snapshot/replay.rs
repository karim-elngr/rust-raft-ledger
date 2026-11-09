//! Relative proof for an authenticated offer. The receiver's physical digest
//! varies with its RocksDB layout. Its committed base row is part of the proof.
use super::*;
use rocksdb::{DBPinnableSlice, Snapshot as View};
use sha2::{Digest, Sha256};
use std::sync::atomic::Ordering;

pub const MAX_REPLAY_ENTRIES: u64 = 4_096;
pub const MAX_REPLAY_BYTES: usize = 32 << 20;

/// Shape and distance only. The proof reader checks both advertised terms
/// against actual committed rows, including when the two indices coincide.
pub fn snapshot_replay_candidate(target: CheckpointRef, base: CheckpointRef) -> bool {
    base.index != 0
        && base.term != 0
        && target.term != 0
        && target.index < u64::MAX
        && target
            .index
            .checked_sub(base.index)
            .is_some_and(|gap| gap <= MAX_REPLAY_ENTRIES)
}

struct Rows<'a> {
    view: View<'a>,
    committed: u64,
    archived: u64,
}

impl<'a> Rows<'a> {
    fn open(store: &'a RaftStore, base: u64) -> Result<Option<Self>, ReplicaError> {
        let meta = store.metadata()?;
        let (cold, view) = store.cold.with_view(&store.db)?;
        if base > meta.hard.commit || cold.is_some_and(|cut| base < cut.through()) {
            // An on-demand cold open can verify an entire independent image.
            // Keep it outside this bounded optimization.
            return Ok(None);
        }
        Ok(Some(Self {
            view,
            committed: meta.hard.commit,
            archived: meta.archived,
        }))
    }

    fn row(
        &self,
        index: u64,
        stage: Option<&Progress>,
    ) -> Result<DBPinnableSlice<'_>, ReplicaError> {
        let key = if let Some(p) = stage.filter(|p| index > p.base) {
            archive_key(&p.remote, index).to_vec()
        } else {
            archive::location(&self.view, index, index + 1, self.archived)?.0
        };
        self.view
            .get_pinned(key)?
            .ok_or_else(|| invalid(format!("missing replay proof entry {index}")))
    }

    fn digest(
        &self,
        target: CheckpointRef,
        base: CheckpointRef,
        stage: Option<&Progress>,
    ) -> Result<Option<[u8; 32]>, ReplicaError> {
        let mut hash = Sha256::new();
        hash.update(b"ledger/raft-snapshot/replay-proof/v1\0");
        hash.update(target.snapshot_token());
        hash.update(base.index.to_be_bytes());
        hash.update(base.term.to_be_bytes());
        let mut remaining = MAX_REPLAY_BYTES;
        let mut term = base.term;
        for i in base.index..=target.index {
            let bytes = self.row(i, stage)?;
            if bytes.len() as u64 > MAX_ENTRY_BYTES || bytes.len() > remaining {
                return Ok(None);
            }
            remaining -= bytes.len();
            let entry = decode_entry(i, &bytes)?;
            if (i == base.index && entry.term != base.term)
                || (i == target.index && entry.term != target.term)
                || entry.term < term
                || entry.term > target.term
            {
                return Err(invalid(
                    "replay proof conflicts with base, target or ordered Raft terms",
                ));
            }
            term = entry.term;
            if stage.is_some_and(|p| p.base < i && i <= self.committed)
                && *self.row(i, None)? != *bytes
            {
                return Err(invalid(
                    "staged replay row conflicts with a committed local entry",
                ));
            }
            hash.update((bytes.len() as u64).to_be_bytes());
            hash.update(&*bytes);
        }
        Ok(Some(hash.finalize().into()))
    }
}

impl SnapshotHistory {
    /// The peer caller pins the source image named by target. None means the
    /// interval cannot be verified within the local hot-history budget.
    pub fn replay_proof(
        &self,
        target: CheckpointRef,
        base: CheckpointRef,
    ) -> Result<Option<[u8; 32]>, ReplicaError> {
        if !snapshot_replay_candidate(target, base) {
            return Ok(None);
        }
        let Some(rows) = Rows::open(&self.store, base.index)? else {
            return Ok(None);
        };
        if target.index > rows.committed {
            return Ok(None);
        }
        rows.digest(target, base, None)
    }
}

impl HistoryStage {
    /// Read one consistent RocksDB view. Validate the proof before executing
    /// the first command; each pass needs only one bounded row at a time.
    pub fn replay_verified(
        &self,
        base: CheckpointRef,
        expected: [u8; 32],
        mut apply: impl FnMut(&Entry) -> Result<(), ReplicaError>,
    ) -> Result<bool, ReplicaError> {
        let target = *self.remote;
        if !snapshot_replay_candidate(target, base) {
            return Ok(false);
        }
        let p = self.progress()?;
        if p.through != target.index || p.term != target.term {
            return Err(invalid(
                "snapshot history has not reached its replay target",
            ));
        }
        if base.index > p.base {
            return Ok(false);
        }
        let Some(rows) = Rows::open(&self.store, base.index)? else {
            return Ok(false);
        };
        match rows.digest(target, base, Some(&p)) {
            Ok(Some(digest)) if digest == expected => {}
            Ok(None) => {
                self.invalidate_replay_stage()?;
                return Err(invalid(
                    "snapshot history exceeds the authenticated replay proof budget",
                ));
            }
            Ok(Some(_)) => {
                self.invalidate_replay_stage()?;
                return Err(invalid(
                    "snapshot replay digest differs from authenticated source",
                ));
            }
            Err(e @ (ReplicaError::Invalid(_) | ReplicaError::Protobuf(_))) => {
                self.invalidate_replay_stage()?;
                return Err(e);
            }
            Err(e) => return Err(e),
        }
        for i in base.index + 1..=target.index {
            let bytes = rows.row(i, Some(&p))?;
            apply(&decode_entry(i, &bytes)?)?;
        }
        self.finish()?;
        self.replay_verified_through
            .store(rows.committed.min(target.index), Ordering::Release);
        Ok(true)
    }

    // Ready already holds both the history lease lock and Raft metadata write
    // lock. A replayed image includes staged rows that may since have become
    // locally committed. Check every new overlap, including interior entries.
    pub(super) fn check_newly_committed(&self, meta: &Metadata) -> Result<(), ReplicaError> {
        let verified = self.replay_verified_through.load(Ordering::Acquire);
        let commit = meta.hard.commit;
        if verified == 0 || commit <= verified {
            return Ok(());
        }
        if commit - verified > MAX_REPLAY_ENTRIES {
            return Err(invalid("committed overlap exceeds replay proof span"));
        }
        let rows = Rows {
            view: self.store.db.snapshot(),
            committed: commit,
            archived: meta.archived,
        };
        let mut remaining = MAX_REPLAY_BYTES;
        for i in verified + 1..=commit {
            let staged = rows
                .view
                .get_pinned(archive_key(&self.remote, i))?
                .ok_or_else(|| invalid("missing staged entry in committed replay overlap"))?;
            if staged.len() > remaining || *staged != *rows.row(i, None)? {
                return Err(invalid(
                    "newly committed local row differs from verified replay history",
                ));
            }
            remaining -= staged.len();
        }
        Ok(())
    }

    fn invalidate_replay_stage(&self) -> Result<(), ReplicaError> {
        let mut pin = self
            .store
            .history_pin
            .lock()
            .map_err(|_| invalid("history stage lock poisoned"))?;
        if !pin.ptr_eq(&Arc::downgrade(&self.remote)) {
            return Ok(());
        }
        // Ready adoption and stage replacement take the same lock. Adopted
        // ranges survive: Ready removed STAGED in its durable transaction.
        let Some(p) = progress(&self.store.db)?.filter(|p| p.remote == *self.remote) else {
            return Ok(());
        };
        let mut batch = WriteBatch::default();
        delete_staged_rows(&mut batch, &p);
        batch.delete(STAGED);
        write_sync(&self.store.db, batch)?;
        // Stale handles must not mutate a replacement even for an identical offer.
        *pin = std::sync::Weak::new();
        Ok(())
    }
}
