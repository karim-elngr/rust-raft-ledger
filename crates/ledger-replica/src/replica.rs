use std::cell::OnceCell;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::AtomicBool};

use ledger_core::{
    Account, AccountId, Command, Counters, Decision, EntryId, Error, JournalEntry, Ledger,
    LedgerId, Outcome, RequestId, Reservation, ReservationId,
};
use raft::eraftpb::{Message, MessageType};
use raft::{Config, RawNode, StateRole, Storage};

use crate::checkpoint::{
    CaptureGuard, CheckpointBoundary, CheckpointCapture, CheckpointRef, CheckpointTask,
};
use crate::checkpoint_files::CheckpointFiles;
use crate::codec::{MAX_BATCH_BYTES, encode_batch};
use crate::projection::Projection;
use crate::raft_store::RaftStore;
use crate::{AccountCacheStats, CommittedSource, ProposalBatch, ReplicaError};
use crate::{HistoryStage, ImportedCheckpoint, SnapshotFiles, SnapshotHistory};

mod reclaim;
mod snapshot;
use snapshot::IncomingSnapshot;

/// Maximum encoded ordinary Raft message accepted by the peer transport.
pub const MAX_RAFT_MESSAGE_BYTES: usize = 4 << 20;

/// Per-voter cache capacities for the rebuildable state database. These can
/// change at restart and are independent of replicated ledger identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheBudget {
    pub account_bytes: usize,
    pub block_bytes: usize,
}

impl Default for CacheBudget {
    fn default() -> Self {
        Self {
            account_bytes: 4 << 20,
            block_bytes: 12 << 20,
        }
    }
}

/// Durable configuration for the three-voter group. Use `with_learner` on
/// every member to add a permanent nonvoting member at bootstrap or restore.
#[derive(Clone, Debug)]
pub struct ReplicaConfig {
    pub cluster: u128,
    pub node: u64,
    pub voters: Vec<u64>,
    pub ledger: LedgerId,
    pub asset: String,
    pub scale: u8,
}

pub(crate) mod membership;
pub use membership::StaticReplicaConfig;

// Persist each newly created directory entry before a voter starts acknowledging
// log writes beneath it. The Raft store separately syncs its own directory entry.
pub(crate) fn create_replica_dir(path: &Path) -> io::Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    create_replica_dir(parent)?;
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => {}
        Err(error) => return Err(error),
    }
    fs::File::open(parent)?.sync_all()
}

/// Synchronous Raft WAL batches completed by this running voter. Bootstrap,
/// checkpoint housekeeping, journal anchors and background I/O are excluded.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RaftSyncStats {
    /// Synchronous Ready writes, including hard-state-only and snapshot Ready.
    pub ready_batches: u64,
    /// Synchronous LightReady writes that advanced the durable commit frontier.
    pub commit_batches: u64,
    /// Subset of Ready writes containing log entries.
    pub append_batches: u64,
    pub appended_entries: u64,
}

pub struct Replica {
    raft_syncs: RaftSyncStats,
    raw: RawNode<RaftStore>,
    projection: Projection,
    config: StaticReplicaConfig,
    root: PathBuf,
    recovered_from: u64,
    checkpoint_busy: Arc<AtomicBool>,
    cache_budget: CacheBudget,
    incoming_snapshot: Option<IncomingSnapshot>,
    installed_snapshot: Option<u64>,
    cleanup_warning: Option<String>,
    inspection: OnceCell<Ledger>,
    read_states: Vec<raft::ReadState>,
    stopped: bool,
}

impl Replica {
    /// Initialize a new identity. A previously initialized Raft directory is an error.
    pub fn bootstrap(
        path: &Path,
        cfg: impl Into<StaticReplicaConfig>,
    ) -> Result<Self, ReplicaError> {
        Self::bootstrap_with_cache(path, cfg, CacheBudget::default())
    }

    pub fn bootstrap_with_cache(
        path: &Path,
        cfg: impl Into<StaticReplicaConfig>,
        cache: CacheBudget,
    ) -> Result<Self, ReplicaError> {
        let mut cfg = cfg.into();
        let identity = cfg.identity()?;
        create_replica_dir(path)?;
        if fs::read_dir(path)?.next().transpose()?.is_some() {
            return Err(ReplicaError::Invalid(
                "bootstrap requires an empty replica directory; recover existing voters".into(),
            ));
        }
        let store = RaftStore::bootstrap_members(&path.join("raft"), &identity, cfg.conf_state())?;
        Self::from_store(path, cfg, store, cache)
    }

    /// Recover from the last locally persisted commit index. The log may also
    /// contain an uncommitted suffix, which remains available to Raft's leader.
    pub fn reopen(path: &Path, cfg: impl Into<StaticReplicaConfig>) -> Result<Self, ReplicaError> {
        Self::reopen_with_cache(path, cfg, CacheBudget::default())
    }

    pub fn reopen_with_cache(
        path: &Path,
        cfg: impl Into<StaticReplicaConfig>,
        cache: CacheBudget,
    ) -> Result<Self, ReplicaError> {
        let mut cfg = cfg.into();
        let identity = cfg.identity()?;
        let store = RaftStore::reopen(&path.join("raft"), &identity)?;
        let mut replica = Self::from_store(path, cfg, store, cache)?;
        replica.cleanup_after_reopen();
        Ok(replica)
    }

    /// Explicit repair for a lost local checkpoint. Validate the complete
    /// retained Raft log and replay the committed prefix. Replace the previous
    /// recovery pointer only after the new working state has been rebuilt.
    pub fn rebuild_from_log(
        path: &Path,
        cfg: impl Into<StaticReplicaConfig>,
        cache: CacheBudget,
    ) -> Result<Self, ReplicaError> {
        let mut cfg = cfg.into();
        let identity = cfg.identity()?;
        let store = RaftStore::reopen_from_genesis(&path.join("raft"), &identity)?;
        let mut replica = Self::from_store(path, cfg, store, cache)?;
        if replica.raw.store().reclaimed_through()? == 0 {
            replica.raw.store().clear_checkpoint()?;
        } else {
            replica
                .begin_checkpoint()?
                .ok_or_else(|| {
                    ReplicaError::Invalid(
                        "missing rebuilt checkpoint for the reclaimed prefix".into(),
                    )
                })?
                .finish()?;
        }
        replica.cleanup_after_reopen();
        Ok(replica)
    }

    // No other capture can be alive on this disk: its task holds a clone of
    // the exclusively locked Raft store. Cleanup is maintenance. A failed
    // cleanup leaves the verified working state and its active pointer valid.
    fn cleanup_after_reopen(&mut self) {
        let cleanup = || -> Result<(), ReplicaError> {
            Projection::discard_working(&self.root.join("peer-state"), self.config.ledger)?;
            if self.root.join("checkpoints").try_exists()? {
                let files = CheckpointFiles::new(&self.root)?;
                files.recover_stages()?;
                files.collect(self.current_checkpoint()?.as_ref())?;
                self.snapshot_files()?.expire_incoming()?;
            }
            Ok(())
        };
        self.cleanup_warning = cleanup().err().map(|e| e.to_string());
    }

    pub fn checkpoint_cleanup_warning(&self) -> Option<&str> {
        self.cleanup_warning.as_deref()
    }

    pub(crate) fn from_store(
        path: &Path,
        cfg: StaticReplicaConfig,
        store: RaftStore,
        cache: CacheBudget,
    ) -> Result<Self, ReplicaError> {
        if store.initial_state()?.conf_state != cfg.conf_state() {
            return Err(ReplicaError::Invalid(
                "stored membership differs from configuration".into(),
            ));
        }
        // Holding the Raft database lock excludes another owner before touching state.
        let projection = match store.checkpoint()? {
            Some(reference) => {
                let boundary = CheckpointBoundary::new(&cfg, reference.index, reference.term);
                let files = CheckpointFiles::new(path)?;
                let verified = files.load(&reference, &boundary)?;
                Projection::from_checkpoint(
                    &path.join("state"),
                    cfg.ledger,
                    reference.index,
                    cache,
                    |dest| verified.materialize(dest),
                )?
            }
            None => Projection::reset_with_cache(
                &path.join("state"),
                Ledger::new(cfg.ledger, &cfg.asset, cfg.scale),
                cache.account_bytes,
                cache.block_bytes,
            )?,
        };
        Self::from_projection(path, cfg, store, projection, cache)
    }

    pub(crate) fn from_projection(
        path: &Path,
        cfg: StaticReplicaConfig,
        store: RaftStore,
        mut projection: Projection,
        cache: CacheBudget,
    ) -> Result<Self, ReplicaError> {
        let committed = store.committed_index()?;
        if projection.id() != cfg.ledger
            || projection.applied_index() > committed
            || store.initial_state()?.conf_state != cfg.conf_state()
        {
            return Err(ReplicaError::Invalid(
                "recovery state or membership differs from voter".into(),
            ));
        }
        let recovered_from = projection.applied_index();
        while projection.applied_index() < committed {
            let first = projection.applied_index() + 1;
            let entries =
                store.retained_entries(first, committed + 1, Some(MAX_BATCH_BYTES as u64 * 4))?;
            if entries.is_empty() {
                return Err(ReplicaError::Invalid(
                    "missing committed Raft entries".into(),
                ));
            }
            for entry in entries {
                projection.apply(&entry)?;
            }
        }
        projection.clear_cache();
        let options = Config {
            id: cfg.node,
            applied: projection.applied_index(),
            heartbeat_tick: 1,
            election_tick: 10,
            check_quorum: true,
            pre_vote: true,
            // raft-rs 0.7 can merge consecutive prepared appends without checking
            // max_size_per_msg a second time. Keep each bounded append separate.
            batch_append: false,
            // The library budgets only entry bodies. Leave room for protobuf
            // repeated-field tags, lengths and the enclosing Raft fields.
            max_size_per_msg: MAX_RAFT_MESSAGE_BYTES as u64 / 2,
            max_uncommitted_size: MAX_BATCH_BYTES as u64 * 16,
            max_committed_size_per_ready: MAX_BATCH_BYTES as u64 * 4,
            ..Config::default()
        };
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let raw = RawNode::new(&options, store, &logger)?;
        Ok(Self {
            raft_syncs: RaftSyncStats::default(),
            raw,
            projection,
            config: cfg,
            root: std::path::absolute(path)?,
            recovered_from,
            checkpoint_busy: Arc::default(),
            cache_budget: cache,
            incoming_snapshot: None,
            installed_snapshot: None,
            cleanup_warning: None,
            inspection: OnceCell::new(),
            read_states: Vec::new(),
            stopped: false,
        })
    }

    fn healthy(&self) -> Result<(), ReplicaError> {
        if self.stopped {
            Err(ReplicaError::Stopped)
        } else {
            Ok(())
        }
    }

    pub fn campaign(&mut self) -> Result<(), ReplicaError> {
        self.healthy()?;
        if self.is_learner() {
            return Err(ReplicaError::Invalid("learner cannot campaign".into()));
        }
        Ok(self.raw.campaign()?)
    }

    pub fn tick(&mut self) {
        if !self.stopped {
            self.raw.tick();
        }
    }

    /// Deliver a message from a configured peer in this cluster. The transport
    /// must bind the sender and cluster identity to the authenticated connection.
    pub fn step(&mut self, message: Message) -> Result<(), ReplicaError> {
        self.healthy()?;
        if message.to != self.raw.raft.id
            || message.from == self.raw.raft.id
            || self.raw.raft.prs().get(message.from).is_none()
        {
            return Err(ReplicaError::Invalid("invalid peer message route".into()));
        }
        if !self
            .config
            .permits(message.from, message.to, message.get_msg_type())
        {
            return Err(ReplicaError::Invalid(
                "Raft message incompatible with member role".into(),
            ));
        }
        if message.has_snapshot()
            || matches!(
                message.get_msg_type(),
                MessageType::MsgSnapshot | MessageType::MsgPropose
            )
        {
            return Err(ReplicaError::Invalid(
                "snapshot delivery needs a verified staged image; peer proposals are unsupported"
                    .into(),
            ));
        }
        Ok(self.raw.step(message)?)
    }

    pub fn is_leader(&self) -> bool {
        !self.stopped && self.raw.raft.state == StateRole::Leader
    }

    pub fn ledger_id(&self) -> LedgerId {
        self.projection.id()
    }
    pub fn asset(&self) -> &str {
        &self.config.asset
    }
    pub fn scale(&self) -> u8 {
        self.config.scale
    }
    pub fn node_id(&self) -> u64 {
        self.raw.raft.id
    }
    pub fn cluster_id(&self) -> u128 {
        self.config.cluster
    }
    pub fn voters(&self) -> &[u64] {
        &self.config.voters
    }
    pub fn learner_id(&self) -> Option<u64> {
        self.config.learner_id()
    }
    pub fn is_learner(&self) -> bool {
        self.learner_id() == Some(self.node_id())
    }
    pub fn members(&self) -> impl Iterator<Item = u64> + '_ {
        self.config.members()
    }
    pub fn term(&self) -> u64 {
        self.raw.raft.term
    }

    /// Clone a handle that exports only locally durable, committed Raft entries.
    pub fn committed_source(&self) -> CommittedSource {
        CommittedSource(self.raw.store().clone(), self.cluster_id())
    }

    /// Raft can issue a safe read barrier after committing in the leader’s term.
    pub fn can_read_index(&self) -> bool {
        self.is_leader() && self.raw.raft.commit_to_current_term()
    }

    /// Use a unique context for every outstanding quorum barrier. Wait for its
    /// matching read state, local application and independent recovery coverage.
    pub fn request_read_index(&mut self, context: Vec<u8>) -> Result<(), ReplicaError> {
        self.healthy()?;
        if !self.is_leader() {
            return Err(ReplicaError::NotLeader);
        }
        if !self.raw.raft.commit_to_current_term() {
            return Err(ReplicaError::ReadNotReady);
        }
        self.raw.read_index(context);
        Ok(())
    }

    pub fn take_read_states(&mut self) -> Vec<raft::ReadState> {
        std::mem::take(&mut self.read_states)
    }

    /// Last index known to Raft, including proposals awaiting persistence.
    pub fn last_index(&self) -> u64 {
        self.raw.raft.raft_log.last_index()
    }

    pub fn leader_id(&self) -> Option<u64> {
        let leader = self.raw.raft.leader_id;
        (!self.stopped && leader != 0).then_some(leader)
    }

    /// Feed a failed transport delivery back to the single Raft owner.
    pub fn report_unreachable(&mut self, peer: u64) {
        if !self.stopped && peer != self.node_id() && self.members().any(|id| id == peer) {
            self.raw.report_unreachable(peer);
        }
    }

    pub fn propose_batch(&mut self, batch: ProposalBatch) -> Result<(), ReplicaError> {
        self.healthy()?;
        if !self.is_leader() {
            return Err(ReplicaError::NotLeader);
        }
        let bytes = batch.finish(self.ledger_id())?;
        Ok(self.raw.propose(Vec::new(), bytes)?)
    }

    /// Submit one bounded batch. Callers drive consensus to learn what committed.
    pub fn propose(&mut self, commands: &[(RequestId, Command)]) -> Result<(), ReplicaError> {
        self.healthy()?;
        if !self.is_leader() {
            return Err(ReplicaError::NotLeader);
        }
        let batch = encode_batch(self.ledger_id(), commands)?;
        Ok(self.raw.propose(Vec::new(), batch)?)
    }

    /// Persist Ready and LightReady, apply their committed entries in order, and
    /// return outbound messages only after all required storage operations succeed.
    pub fn drain_ready(&mut self) -> Result<Vec<Message>, ReplicaError> {
        self.healthy()?;
        let result = self.persist_and_apply();
        if result.is_err() {
            self.stopped = true;
            self.read_states.clear();
        }
        result
    }

    fn persist_and_apply(&mut self) -> Result<Vec<Message>, ReplicaError> {
        let mut messages = Vec::new();
        let mut read_states = Vec::new();
        while self.raw.has_ready() {
            let mut ready = self.raw.ready();
            let synced = if ready.snapshot().is_empty() {
                self.raw
                    .store()
                    .persist_ready(ready.entries(), ready.hs())?
            } else {
                self.install_ready_snapshot(ready.snapshot(), ready.entries(), ready.hs())?;
                true
            };
            if synced {
                self.raft_syncs.ready_batches = self.raft_syncs.ready_batches.saturating_add(1);
                if !ready.entries().is_empty() {
                    self.raft_syncs.append_batches =
                        self.raft_syncs.append_batches.saturating_add(1);
                    self.raft_syncs.appended_entries = self
                        .raft_syncs
                        .appended_entries
                        .saturating_add(ready.entries().len() as u64);
                }
            }
            let committed = ready.take_committed_entries();
            read_states.extend(ready.take_read_states());
            messages.extend(ready.take_messages());
            messages.extend(ready.take_persisted_messages());
            let mut light = self.raw.advance_append(ready);
            if let Some(index) = light.commit_index()
                && self.raw.store().persist_commit(index)?
            {
                self.raft_syncs.commit_batches = self.raft_syncs.commit_batches.saturating_add(1);
            }
            messages.extend(light.take_messages());
            let durable = self.raw.store().committed_index()?;
            for entry in committed.into_iter().chain(light.take_committed_entries()) {
                if entry.index > durable {
                    return Err(ReplicaError::Invalid(
                        "apply exceeds durable commit index".into(),
                    ));
                }
                self.projection.apply(&entry)?;
                self.inspection.take();
            }
            self.raw.advance_apply_to(self.projection.applied_index());
        }
        self.read_states.extend(read_states);
        Ok(messages)
    }

    pub fn raft_syncs(&self) -> RaftSyncStats {
        self.raft_syncs
    }

    /// Last fully published local index, including Raft no-ops.
    pub fn applied_index(&self) -> u64 {
        self.projection.applied_index()
    }

    /// Materialize the entire local ledger for explicit inspection. Its memory
    /// cost grows with history. Serving paths use the point lookups below.
    pub fn ledger(&self) -> Result<&Ledger, ReplicaError> {
        self.healthy()?;
        if let Some(ledger) = self.inspection.get() {
            return Ok(ledger);
        }
        let genesis = Ledger::new(self.ledger_id(), self.asset(), self.scale());
        let ledger = self.projection.inspect_ledger(genesis)?;
        Ok(self.inspection.get_or_init(|| ledger))
    }

    pub fn account(&self, id: AccountId) -> Result<Option<Account>, ReplicaError> {
        self.healthy()?;
        self.projection.account(id)
    }

    pub fn entry(&self, id: EntryId) -> Result<Option<JournalEntry>, ReplicaError> {
        self.healthy()?;
        self.projection.entry(id)
    }

    pub fn reservation(&self, id: ReservationId) -> Result<Option<Reservation>, ReplicaError> {
        self.healthy()?;
        self.projection.reservation(id)
    }

    pub fn export_decisions(
        &self,
        after: ledger_export::Position,
        through: u64,
    ) -> Result<ledger_export::EventPage, ReplicaError> {
        self.healthy()?;
        self.projection.event_page(after, through)
    }

    pub fn decision(&self, id: RequestId) -> Result<Option<Decision>, ReplicaError> {
        self.healthy()?;
        self.projection.decision(id)
    }

    pub fn outcome_for(
        &self,
        id: RequestId,
        command: &Command,
    ) -> Result<Option<Result<Outcome, Error>>, ReplicaError> {
        Ok(self
            .decision(id)?
            .map(|decision| decision.outcome_for(command)))
    }

    pub fn cache_stats(&self) -> AccountCacheStats {
        self.projection.cache_stats()
    }

    /// Wait for working-state memtables to reach SST files. Recovery still
    /// uses the durable Raft store and its committed index.
    pub fn flush_projection(&self) -> Result<(), ReplicaError> {
        self.healthy()?;
        self.projection.flush()
    }

    /// Index of the physical base actually used to start this voter. Zero
    /// means this instance replayed from genesis.
    pub fn recovered_from(&self) -> u64 {
        self.recovered_from
    }

    pub fn current_checkpoint(&self) -> Result<Option<CheckpointRef>, ReplicaError> {
        self.raw.store().checkpoint()
    }

    /// Capture at a complete applied entry. None means the current checkpoint
    /// already covers this index. Index zero has no physical state to seal.
    pub fn begin_checkpoint(&self) -> Result<Option<CheckpointTask>, ReplicaError> {
        self.prepare_checkpoint()?
            .map(CheckpointCapture::capture)
            .transpose()
    }

    /// Reserve the projection generation without flushing or copying files on the owner.
    /// The worker reads the actual applied boundary from its immutable capture.
    pub fn prepare_checkpoint(&self) -> Result<Option<CheckpointCapture>, ReplicaError> {
        self.healthy()?;
        let guard = CaptureGuard::acquire(&self.checkpoint_busy)?;
        let previous = self.current_checkpoint()?;
        let index = self.applied_index();
        if index == 0 {
            return Err(ReplicaError::Invalid(
                "a checkpoint needs a committed entry".into(),
            ));
        }
        if previous.is_some_and(|p| p.index == index) {
            return Ok(None);
        }
        let store = self.raw.store();
        if index > store.committed_index()? {
            return Err(ReplicaError::Invalid(
                "checkpoint exceeds durable commit".into(),
            ));
        }
        Ok(Some(CheckpointCapture {
            db: self.projection.checkpoint_db(),
            root: self.root.clone(),
            config: self.config.clone(),
            minimum: index,
            previous,
            store: store.clone(),
            guard,
        }))
    }

    /// Hash every file in the active checkpoint, including previously verified
    /// immutable files. Suitable for an offline integrity check.
    pub fn scrub_checkpoint(&self) -> Result<CheckpointRef, ReplicaError> {
        Self::scrub(&self.root, &self.config, self.raw.store())
    }

    /// Check a stopped voter without opening or rebuilding its working state.
    pub fn scrub_stored_checkpoint(
        path: &Path,
        cfg: impl Into<StaticReplicaConfig>,
    ) -> Result<CheckpointRef, ReplicaError> {
        let mut cfg = cfg.into();
        let store = RaftStore::reopen(&path.join("raft"), &cfg.identity()?)?;
        Self::scrub(path, &cfg, &store)
    }

    fn scrub(
        path: &Path,
        cfg: &StaticReplicaConfig,
        store: &RaftStore,
    ) -> Result<CheckpointRef, ReplicaError> {
        let reference = store
            .checkpoint()?
            .ok_or_else(|| ReplicaError::Invalid("no local checkpoint".into()))?;
        let boundary = CheckpointBoundary::new(cfg, reference.index, reference.term);
        CheckpointFiles::new(path)?.scrub(&reference, &boundary)?;
        Ok(reference)
    }

    /// Read counters from the working RocksDB projection at the local applied index.
    pub fn stored_counters(&self, id: AccountId) -> Result<Option<Counters>, ReplicaError> {
        self.healthy()?;
        self.projection.stored_counters(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn config() -> ReplicaConfig {
        ReplicaConfig {
            cluster: 77,
            node: 2,
            voters: vec![1, 2, 3],
            ledger: LedgerId(1),
            asset: "USD".into(),
            scale: 2,
        }
    }

    #[test]
    fn restart_binds_identity_but_accepts_voter_list_reordering() {
        let dir = tempdir().unwrap();
        let cfg = config();
        assert!(Replica::reopen(dir.path(), cfg.clone()).is_err());
        let replica = Replica::bootstrap(dir.path(), cfg.clone()).unwrap();
        assert!(Replica::reopen(dir.path(), cfg.clone()).is_err());
        drop(replica);
        assert!(Replica::bootstrap(dir.path(), cfg.clone()).is_err());
        for wrong in [
            ReplicaConfig {
                cluster: 78,
                ..cfg.clone()
            },
            ReplicaConfig {
                node: 1,
                ..cfg.clone()
            },
            ReplicaConfig {
                voters: vec![1, 2, 4],
                ..cfg.clone()
            },
            ReplicaConfig {
                ledger: LedgerId(2),
                ..cfg.clone()
            },
            ReplicaConfig {
                asset: "EUR".into(),
                ..cfg.clone()
            },
            ReplicaConfig {
                scale: 0,
                ..cfg.clone()
            },
        ] {
            assert!(Replica::reopen(dir.path(), wrong).is_err());
        }
        let reordered = ReplicaConfig {
            voters: vec![3, 1, 2],
            ..cfg
        };
        assert_eq!(
            Replica::reopen(dir.path(), reordered)
                .unwrap()
                .applied_index(),
            0
        );
    }

    #[test]
    fn reopen_rebuilds_a_v1_single_family_projection_from_the_durable_log() {
        use ledger_core::{Bounds, Side};
        use raft::eraftpb::{Entry, HardState};
        let dir = tempdir().unwrap();
        let mut cfg = config();
        let wallet = AccountId::new(cfg.ledger, 42);
        let request = RequestId(73);
        let create = Command::CreateAccount {
            account: wallet,
            code: 7,
            normal: Side::Credit,
            bounds: Bounds::default(),
        };
        let bytes = encode_batch(cfg.ledger, &[(request, create.clone())]).unwrap();
        let store = RaftStore::bootstrap(
            &dir.path().join("raft"),
            &cfg.identity().unwrap(),
            &cfg.voters,
        )
        .unwrap();
        store
            .persist_ready(
                &[Entry {
                    term: 1,
                    index: 1,
                    data: bytes.into(),
                    ..Default::default()
                }],
                Some(&HardState {
                    term: 1,
                    commit: 1,
                    ..Default::default()
                }),
            )
            .unwrap();
        drop(store);

        // The former projection was a single default column family. Its
        // no-WAL frontier and rows may disagree after a crash.
        let path = dir.path().join("state");
        fs::create_dir(&path).unwrap();
        fs::File::create_new(path.join(format!(".ledger-projection-v1-{:032x}", cfg.ledger.0)))
            .unwrap();
        let old = rocksdb::DB::open_default(path.join("data")).unwrap();
        old.put(b"projection/v1/applied", 9u64.to_be_bytes())
            .unwrap();
        drop(old);

        let replica = Replica::reopen(dir.path(), cfg).unwrap();
        assert_eq!(replica.applied_index(), 1);
        assert_eq!(replica.cache_stats().resident_accounts, 0);
        assert_eq!(replica.account(wallet).unwrap().unwrap().code(), 7);
        assert_eq!(
            replica.outcome_for(request, &create).unwrap(),
            Some(Ok(Outcome::AccountCreated(wallet)))
        );
        assert!(replica.inspection.get().is_none());
    }

    #[test]
    fn missing_raft_store_does_not_bootstrap_over_a_surviving_ledger() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("voter");
        drop(Replica::bootstrap(&root, config()).unwrap());
        fs::rename(root.join("raft"), dir.path().join("unavailable")).unwrap();
        let current = root.join("state/data/CURRENT");
        let before = fs::read(&current).unwrap();
        assert!(Replica::reopen(&root, config()).is_err());
        assert!(Replica::bootstrap(&root, config()).is_err());
        assert!(!root.join("raft").exists());
        assert_eq!(fs::read(current).unwrap(), before);
    }

    #[test]
    fn unsupported_committed_format_stops_local_reads_and_replay() {
        use raft::eraftpb::Entry;
        let dir = tempdir().unwrap();
        let mut replica = Replica::bootstrap(dir.path(), config()).unwrap();
        let mut append = Message {
            from: 1,
            to: 2,
            term: 1,
            commit: 1,
            ..Default::default()
        };
        append.set_msg_type(MessageType::MsgAppend);
        append.entries.push(Entry {
            index: 1,
            term: 1,
            data: b"RLCB\x02".to_vec().into(),
            ..Default::default()
        });
        replica.step(append).unwrap();
        assert!(matches!(
            replica.drain_ready(),
            Err(ReplicaError::Invalid(_))
        ));
        assert_eq!(replica.applied_index(), 0);
        assert!(matches!(replica.ledger(), Err(ReplicaError::Stopped)));
        assert!(matches!(replica.drain_ready(), Err(ReplicaError::Stopped)));
        drop(replica);
        assert!(matches!(
            Replica::reopen(dir.path(), config()),
            Err(ReplicaError::Invalid(_))
        ));
    }

    #[test]
    fn invalid_voter_sets_do_not_initialize_a_directory() {
        let root = tempdir().unwrap();
        let path = root.path().join("new");
        for voters in [vec![], vec![2], vec![1, 1, 2], vec![0, 1, 2], vec![1, 3, 4]] {
            let cfg = ReplicaConfig { voters, ..config() };
            assert!(Replica::bootstrap(&path, cfg).is_err());
            assert!(!path.exists());
        }
        for id in [0, 1, 2, 3] {
            assert!(Replica::bootstrap(&path, config().with_learner(id)).is_err());
            assert!(!path.exists());
        }
    }
}
