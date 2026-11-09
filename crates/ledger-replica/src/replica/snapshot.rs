use super::*;
use raft::SnapshotStatus;
use raft::eraftpb::{Entry, HardState, Snapshot};

pub(super) struct IncomingSnapshot {
    image: ImportedCheckpoint,
    history: HistoryStage,
    _guard: CaptureGuard,
}

impl Replica {
    pub fn snapshot_files(&self) -> Result<SnapshotFiles, ReplicaError> {
        Ok(SnapshotFiles::new(&self.root, &self.config)?
            .with_voter(self.raw.store().clone(), self.checkpoint_busy.clone()))
    }

    pub fn snapshot_history(&self) -> SnapshotHistory {
        self.raw.store().snapshot_history()
    }

    /// First index served by ordinary AppendEntries. The original committed
    /// entries behind this boundary remain accessible for repair and verification.
    pub fn first_replication_index(&self) -> Result<u64, ReplicaError> {
        Ok(self.raw.store().first_index()?)
    }

    pub fn compact_replication(&self, through: u64) -> Result<(), ReplicaError> {
        self.healthy()?;
        self.raw.store().compact_replication(through)
    }

    pub fn installed_snapshot(&self) -> Option<u64> {
        self.installed_snapshot
    }

    /// Highest index the current leader knows this voter has replicated.
    pub fn matched_index(&self, peer: u64) -> Option<u64> {
        self.raw
            .raft
            .prs()
            .get(peer)
            .filter(|_| self.is_leader())
            .map(|p| p.matched)
    }

    /// Peer I/O may finish after a new leader term or a newer offer. Only
    /// report completion for the transfer Raft is actually waiting for.
    pub fn report_snapshot(&mut self, peer: u64, term: u64, index: u64, success: bool) {
        if self.is_leader()
            && self.term() == term
            && index != 0
            && self
                .raw
                .raft
                .prs()
                .get(peer)
                .is_some_and(|p| p.pending_snapshot == index)
        {
            let status = if success {
                SnapshotStatus::Finish
            } else {
                SnapshotStatus::Failure
            };
            self.raw.report_snapshot(peer, status);
        }
    }

    /// Admit the original authenticated Raft message only after both the
    /// sealed image and the missing committed history have reached local disk.
    /// Raft may ignore it or match its log and fast-forward without installing.
    pub fn step_snapshot(
        &mut self,
        message: Message,
        image: ImportedCheckpoint,
        history: HistoryStage,
    ) -> Result<bool, ReplicaError> {
        self.healthy()?;
        if message.get_msg_type() != MessageType::MsgSnapshot
            || !message.has_snapshot()
            || message.term == 0
            || message.to != self.node_id()
            || message.from == self.node_id()
            || !self.voters().contains(&message.from)
        {
            return Err(ReplicaError::Invalid(
                "invalid snapshot envelope or route".into(),
            ));
        }
        let offered = message.get_snapshot();
        let remote = CheckpointRef::from_snapshot(offered)?;
        if offered.get_metadata().get_conf_state() != &self.raw.store().initial_state()?.conf_state
            || remote.term > message.term
            || remote != image.source_reference()
            || !image.belongs_to(&self.root, &self.config)?
            || !history.belongs_to(self.raw.store(), remote)
        {
            return Err(ReplicaError::Invalid(
                "snapshot membership, image or history does not match its offer".into(),
            ));
        }
        history.finish()?;
        let guard = CaptureGuard::acquire(&self.checkpoint_busy)?;
        self.raw.step(message)?;
        if let Some(accepted) = self.raw.raft.raft_log.unstable_snapshot() {
            if CheckpointRef::from_snapshot(accepted)? != remote {
                return Err(ReplicaError::Invalid(
                    "Raft selected a different snapshot".into(),
                ));
            }
            self.incoming_snapshot = Some(IncomingSnapshot {
                image,
                history,
                _guard: guard,
            });
            return Ok(true);
        }
        Ok(false)
    }

    pub(super) fn install_ready_snapshot(
        &mut self,
        snapshot: &Snapshot,
        entries: &[Entry],
        hard: Option<&HardState>,
    ) -> Result<(), ReplicaError> {
        let incoming = self
            .incoming_snapshot
            .take()
            .ok_or_else(|| ReplicaError::Invalid("Raft snapshot has no staged data".into()))?;
        let remote = CheckpointRef::from_snapshot(snapshot)?;
        if incoming.image.source_reference() != remote {
            return Err(ReplicaError::Invalid(
                "staged image differs from Raft Ready".into(),
            ));
        }
        let old_path = self.projection.working_root().to_owned();
        let next = if old_path.ends_with("peer-state") {
            "state"
        } else {
            "peer-state"
        };
        // Both working databases can be open during validation. The durable
        // reference always names an independent immutable generation. Reopen
        // recovers from that reference after a crash on either side of the swap.
        let next_projection = Projection::from_checkpoint(
            &self.root.join(next),
            self.config.ledger,
            remote.index,
            self.cache_budget,
            |dest| incoming.image.materialize(dest),
        )?;
        let local = incoming.image.reference();
        self.raw.store().persist_snapshot_ready(
            snapshot,
            local,
            &incoming.history,
            entries,
            hard,
        )?;
        self.projection = next_projection;
        self.inspection.take();
        self.installed_snapshot = Some(remote.index);
        if let Err(error) = Projection::discard_working(&old_path, self.config.ledger)
            .and_then(|()| CheckpointFiles::new(&self.root)?.collect(Some(&local)))
        {
            self.cleanup_warning = Some(error.to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
