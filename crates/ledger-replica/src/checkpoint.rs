use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tempfile::TempDir;

use crate::checkpoint_files::CheckpointFiles;
use crate::projection::Projection;
use crate::raft_store::RaftStore;
use crate::{ReplicaError, StaticReplicaConfig};

/// A sealed, local projection and the committed Raft entry it covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointRef {
    pub index: u64,
    pub term: u64,
    pub digest: [u8; 32],
}

// The voter ID is deliberately absent. A physical image describes the entire
// group's state at one position. The local Raft database binds its voter ID.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CheckpointBoundary {
    pub(crate) cluster: u128,
    pub(crate) ledger: u128,
    pub(crate) asset: String,
    pub(crate) scale: u8,
    pub(crate) voters: Vec<u64>,
    pub(crate) learner: Option<u64>,
    pub(crate) index: u64,
    pub(crate) term: u64,
}

impl CheckpointBoundary {
    pub(crate) fn new(cfg: impl Into<StaticReplicaConfig>, index: u64, term: u64) -> Self {
        let cfg = cfg.into();
        Self {
            cluster: cfg.cluster,
            ledger: cfg.ledger.0,
            asset: cfg.asset.clone(),
            scale: cfg.scale,
            voters: cfg.voters.clone(),
            learner: cfg.learner_id(),
            index,
            term,
        }
    }
}

pub(crate) struct CaptureGuard(Arc<AtomicBool>);

impl CaptureGuard {
    pub(crate) fn acquire(busy: &Arc<AtomicBool>) -> Result<Self, ReplicaError> {
        if busy.swap(true, Ordering::AcqRel) {
            return Err(ReplicaError::Invalid(
                "a local checkpoint is already in progress".into(),
            ));
        }
        Ok(Self(busy.clone()))
    }
}

impl Drop for CaptureGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// A reserved capture of the live projection. A storage worker may capture a
/// later committed entry than the one applied when this reservation was made.
pub struct CheckpointCapture {
    pub(crate) db: Arc<rocksdb::DB>,
    pub(crate) config: StaticReplicaConfig,
    pub(crate) root: PathBuf,
    pub(crate) minimum: u64,
    pub(crate) previous: Option<CheckpointRef>,
    pub(crate) store: RaftStore,
    pub(crate) guard: CaptureGuard,
}

impl CheckpointCapture {
    pub(crate) fn capture(self) -> Result<CheckpointTask, ReplicaError> {
        let files = CheckpointFiles::new(&self.root)?;
        let stage = files.stage()?;
        let (index, _) =
            Projection::capture_db(&self.db, self.config.ledger, &stage.path().join("db"))?;
        if index < self.minimum || index > self.store.committed_index()? {
            return Err(ReplicaError::Invalid(
                "captured projection outside durable commit range".into(),
            ));
        }
        let boundary =
            CheckpointBoundary::new(self.config, index, self.store.retained_term(index)?);
        Ok(CheckpointTask {
            stage,
            boundary,
            previous: self.previous,
            files,
            store: self.store,
            _guard: self.guard,
        })
    }

    pub fn finish(self) -> Result<CheckpointRef, ReplicaError> {
        self.capture()?.finish()
    }
}

/// The live database is no longer needed once a capture returns. Finish this
/// task on a storage worker so hashing and publication do not block Raft.
pub struct CheckpointTask {
    pub(crate) stage: TempDir,
    pub(crate) boundary: CheckpointBoundary,
    pub(crate) previous: Option<CheckpointRef>,
    pub(crate) files: CheckpointFiles,
    pub(crate) store: RaftStore,
    pub(crate) _guard: CaptureGuard,
}

impl CheckpointTask {
    pub fn finish(self) -> Result<CheckpointRef, ReplicaError> {
        let (reference, _pin) =
            self.files
                .seal_pinned(self.stage, &self.boundary, self.previous.as_ref())?;
        // Files and directory entries must reach disk before the Raft WAL may
        // advertise them. A crash beforehand leaves the earlier reference live.
        self.store.publish_checkpoint(reference)?;
        self.files.collect(Some(&reference))?;
        Ok(reference)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Replica, ReplicaConfig, codec::encode_batch};
    use ledger_core::{AccountId, Bounds, Command, LedgerId, Outcome, RequestId, Side};
    use raft::eraftpb::{Entry, Message, MessageType};
    use std::{fs, io::ErrorKind};
    use tempfile::tempdir;

    // Deliver a committed command through the follower's normal Ready/apply path.
    fn append_account(voter: &mut Replica, number: u128) -> AccountId {
        let account = AccountId::new(voter.ledger_id(), number);
        let command = Command::CreateAccount {
            account,
            code: 1,
            normal: Side::Credit,
            bounds: Bounds::default(),
        };
        let previous = voter.last_index();
        let next = previous + 1;
        let mut append = Message {
            from: 1,
            to: voter.node_id(),
            term: 1,
            index: previous,
            log_term: if previous == 0 { 0 } else { 1 },
            commit: next,
            ..Default::default()
        };
        append.set_msg_type(MessageType::MsgAppend);
        append.entries.push(Entry {
            index: next,
            term: 1,
            data: encode_batch(voter.ledger_id(), &[(RequestId(number), command)])
                .unwrap()
                .into(),
            ..Default::default()
        });
        voter.step(append).unwrap();
        voter.drain_ready().unwrap();
        assert_eq!(voter.applied_index(), next);
        account
    }

    #[test]
    fn restart_uses_published_cut_and_collects_a_sealed_unpublished_successor() {
        let disk = tempdir().unwrap();
        let root = disk.path().join("voter");
        let cfg = ReplicaConfig {
            cluster: 31,
            ledger: LedgerId(8),
            asset: "USD".into(),
            scale: 2,
            voters: vec![1, 2, 3],
            node: 2,
        };
        let mut voter = Replica::bootstrap(&root, cfg.clone()).unwrap();
        let first_account = append_account(&mut voter, 51);
        let first = voter.begin_checkpoint().unwrap().unwrap().finish().unwrap();
        let second_account = append_account(&mut voter, 52);
        // Stop at the exact crash boundary inside finish(): generation and parent
        // directory synced, Raft's active reference still pointing at the first cut.
        let (files, orphan, boundary) = {
            let task = voter.begin_checkpoint().unwrap().unwrap();
            assert_eq!(task.previous, Some(first));
            let orphan = task
                .files
                .seal(task.stage, &task.boundary, task.previous.as_ref())
                .unwrap();
            (task.files, orphan, task.boundary)
        };
        assert!(orphan.index > first.index);
        files.scrub(&orphan, &boundary).unwrap();
        assert_eq!(voter.current_checkpoint().unwrap(), Some(first));
        drop(voter);
        // A lost, unflushed working image cannot supply the second command.
        fs::rename(
            root.join("state"),
            disk.path().join("discarded-working-state"),
        )
        .unwrap();

        let recovered = Replica::reopen(&root, cfg).unwrap();
        assert_eq!(recovered.recovered_from(), first.index);
        assert_eq!(recovered.scrub_checkpoint().unwrap(), first);
        assert_eq!(recovered.applied_index(), orphan.index);
        for account in [first_account, second_account] {
            assert_eq!(recovered.account(account).unwrap().unwrap().id(), account);
            assert_eq!(
                recovered
                    .decision(RequestId(account.number))
                    .unwrap()
                    .unwrap()
                    .outcome,
                Ok(Outcome::AccountCreated(account))
            );
        }
        assert!(recovered.checkpoint_cleanup_warning().is_none());
        assert!(matches!(files.load(&orphan, &boundary),
            Err(ReplicaError::Io(error)) if error.kind()==ErrorKind::NotFound));
    }
}
