//! A single-owner Raft voter backed by two embedded RocksDB databases.
//!
//! Drive each voter by delivering peer messages, ticking its logical clock and
//! draining ready work. A successful proposal has entered the local Raft driver.
//! Local reads expose applied state for diagnostics. Client acknowledgments and
//! current reads require an independent durability frontier and a serving layer.

mod account_cache;
mod batch;
mod checkpoint;
mod checkpoint_files;
mod codec;
mod error;
mod export;
mod journal_verifier;
mod projection;
mod raft_store;
mod recovery;
mod replica;

pub use account_cache::AccountCacheStats;
pub use batch::{PreparedCommand, ProposalBatch};
pub use checkpoint::{CheckpointCapture, CheckpointRef, CheckpointTask};
pub use checkpoint_files::transfer::{
    ImportedCheckpoint, SNAPSHOT_CHUNK_BYTES, SNAPSHOT_MAX_MANIFEST, SnapshotFiles, SnapshotRange,
    SnapshotReceiver, SnapshotSource,
};
pub use error::ReplicaError;
pub use journal_verifier::{JournalConfirmation, JournalVerifier, VerificationError};
pub use raft_store::reclaim::ReclaimStats;
pub use raft_store::snapshot::{
    HistoryStage, MAX_REPLAY_BYTES, MAX_REPLAY_ENTRIES, SnapshotHistory, snapshot_replay_candidate,
};
pub use recovery::{CommittedSource, RecoveryStats};
pub use replica::{
    CacheBudget, MAX_RAFT_MESSAGE_BYTES, RaftSyncStats, Replica, ReplicaConfig, StaticReplicaConfig,
};
