//! A receiver may spend bounded replay work in place of transferring SSTs.
//! The source Raft token continues to identify the authenticated offer; Ready
//! publishes the independent local seal built from the receiver's own prefix.
use super::*;
use crate::HistoryStage;

pub(super) fn prefer(base: CheckpointRef, target: CheckpointRef, matched: u64, total: u64) -> bool {
    base.index < target.index
        && crate::snapshot_replay_candidate(target, base)
        && matched.saturating_mul(2) < total
}
fn bad_base(error: &ReplicaError) -> bool {
    match error {
        ReplicaError::Invalid(_) | ReplicaError::Encoding(_) => true,
        ReplicaError::Io(e) => matches!(
            e.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::UnexpectedEof | io::ErrorKind::InvalidData
        ),
        ReplicaError::Rocks(e) => matches!(
            e.kind(),
            rocksdb::ErrorKind::Corruption
                | rocksdb::ErrorKind::InvalidArgument
                | rocksdb::ErrorKind::NotFound
        ),
        _ => false,
    }
}
impl SnapshotReceiver {
    /// Candidate known to have been this voter's active durable checkpoint.
    /// Query before claiming physical file ranges.
    pub fn replay_base(&self) -> Option<CheckpointRef> {
        self.replay_base
    }

    /// Full-scrub the locally committed base, apply authenticated original
    /// entries to a private copy, and pin a new local seal. No authority moves
    /// until Raft selects this offer through the ordinary Ready transaction.
    pub fn replay(
        &mut self,
        history: &HistoryStage,
        proof: [u8; 32],
    ) -> Result<Option<ImportedCheckpoint>, ReplicaError> {
        let Some(base) = self.replay_base.take() else {
            return Ok(None);
        };
        let (store, _) = self
            .files
            .voter
            .as_ref()
            .ok_or_else(|| invalid("replay requires its owning voter"))?;
        if !history.belongs_to(store, self.reference) {
            self.replay_base = Some(base);
            return Err(invalid(
                "snapshot replay history belongs to a different voter or offer",
            ));
        }
        history.finish()?;
        let prior = self
            .reuse_pin
            .as_ref()
            .ok_or_else(|| invalid("snapshot replay lost its checkpoint pin"))?;
        let files = &self.files.files;
        let scratch = files.stage()?;
        let id = LedgerId(self.files.scope.ledger);
        let path = scratch.path().join("replay-db");
        let copy = prior
            .verify(true)
            .and_then(|()| prior.materialize(&path))
            .and_then(|()| Projection::open_replay_copy(&path, id, base.index));
        let mut projection = match copy {
            Ok(copy) => copy,
            Err(error) if bad_base(&error) => {
                // A damaged local image is replaceable. A mismatched history
                // proof must still abort before a physical image could adopt it.
                history.replay_verified(base, proof, |_| Ok(()))?;
                self.candidates.clear();
                self.reuse_pin.take();
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if !history.replay_verified(base, proof, |entry| projection.apply(entry))? {
            return Ok(None);
        }
        if projection.applied_index() != self.reference.index {
            return Err(invalid("snapshot replay did not reach the offered cut"));
        }
        let stage = files.stage()?;
        projection.capture(&stage.path().join("db"))?;
        drop(projection);
        let boundary = self.files.boundary(self.reference);
        let (local, pin) = files.seal_pinned(stage, &boundary, Some(&base))?;
        // Keep the empty inbox as a lease/expiry record for the staged original
        // log if the owner never installs it. A physical-file receipt would
        // incorrectly claim that this local layout matches the sender's bytes.
        Ok(Some(ImportedCheckpoint {
            local,
            source: self.reference,
            pin,
            root: files.root.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefer_physical_on_at_least_half_file_bytes_or_without_a_bounded_earlier_cut() {
        let base = CheckpointRef {
            index: 10,
            term: 2,
            digest: [0; 32],
        };
        let target = CheckpointRef { index: 11, ..base };
        assert!(prefer(base, target, 49, 100));
        assert!(!prefer(base, target, 50, 100));
        assert!(!prefer(base, base, 0, 100));
        assert!(!prefer(
            base,
            CheckpointRef {
                index: base.index + 4097,
                ..target
            },
            0,
            100
        ));
    }
}
