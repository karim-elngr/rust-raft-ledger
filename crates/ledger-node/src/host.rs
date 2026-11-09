use std::sync::Arc;
use std::thread;

use ledger_core::{Error, Outcome};
use ledger_replica::{AccountCacheStats, Replica, ReplicaError};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};

use crate::checkpoints::CheckpointPolicy;
use crate::service_mode::Service;
mod admission;
use crate::protection::Protection;
use crate::runtime::Owner;
use crate::transport::{PeerConfig, PeerTransport, SnapshotResources};
pub use admission::NodeHandle;
pub(crate) use admission::{Admission, Control, Submission, Waiter, enqueue};
use ledger_recovery::{Journal, JournalError};

const PEER_INBOX: usize = 32;

pub type Decision = Result<Outcome, Error>;

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("{0}")]
    Invalid(String),
    #[error("replica: {0}")]
    Replica(#[from] ReplicaError),
    #[error("gRPC transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("node I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("independent journal: {0}")]
    Journal(#[from] JournalError),
    #[error("this node has no independent journal configured")]
    Unprotected,
    #[error("this cluster no longer owns the active recovery generation")]
    Fenced,
    #[error("node admission is full")]
    Busy,
    #[error("this voter is a follower (known leader {0:?})")]
    NotLeader(Option<u64>),
    #[error("leader changed after proposal; retry the original request ID")]
    LeadershipChanged,
    #[error("deadline elapsed; retry the original request ID to resolve the outcome")]
    Deadline,
    #[error("node is stopped")]
    Stopped,
    #[error("voter thread terminated unexpectedly")]
    ThreadFailed,
}

#[derive(Clone, Debug)]
pub struct NodeStatus {
    pub node: u64,
    pub generation: u128,
    pub learner_id: Option<u64>,
    pub is_leader: bool,
    pub is_learner: bool,
    pub leader_id: Option<u64>,
    pub term: u64,
    pub applied_index: u64,
    pub account_cache: AccountCacheStats,
    pub checkpoint_index: Option<u64>,
    pub recovered_from: u64,
    /// Last checkpoint this running process installed through Raft Ready.
    pub installed_snapshot: Option<u64>,
    pub last_snapshot_transfer: Option<crate::SnapshotTransferStats>,
    pub snapshot_maintenance_error: Option<String>,
    /// Remote voters and learner, with their confirmed match indexes on the current leader.
    pub peer_matches: Vec<(u64, u64)>,
    pub checkpoint_error: Option<String>,
    pub protected_index: Option<u64>,
    pub protection_error: Option<String>,
    pub raft_syncs: ledger_replica::RaftSyncStats,
    /// Independent journal appends published by this voter's completed verifier rounds.
    pub journal_append_publications: u64,
    #[cfg(feature = "batch-diagnostics")]
    /// Queue time of successfully proposed commands, summed across commands.
    pub proposal_queue_nanos: u128,
    pub proposed_batches: u64,
    pub proposed_commands: u64,
}

/// Embedded voter settings. Batching limits apply to local proposals and do
/// not change the on-disk format or a follower's ability to apply larger entries.
#[derive(Clone, Copy, Debug)]
pub struct NodeOptions {
    pub checkpoints: CheckpointPolicy,
    pub max_commands_per_entry: usize,
}

impl Default for NodeOptions {
    fn default() -> Self {
        Self {
            checkpoints: CheckpointPolicy::default(),
            max_commands_per_entry: ledger_replica::ProposalBatch::MAX_COMMANDS,
        }
    }
}

pub struct Node {
    feed: Option<Arc<dyn ledger_export::LedgerFeed>>,
    handle: NodeHandle,
    stop: watch::Sender<bool>,
    done: watch::Receiver<bool>,
    owner: Option<thread::JoinHandle<Result<(), NodeError>>>,
}

impl Node {
    /// Start a private peer listener for an initialized or reopened replica.
    pub async fn start(
        replica: Replica,
        config: PeerConfig,
        listener: TcpListener,
    ) -> Result<Self, NodeError> {
        Self::start_configured(replica, config, listener, None, CheckpointPolicy::default()).await
    }

    pub async fn start_protected(
        replica: Replica,
        config: PeerConfig,
        listener: TcpListener,
        journal: Journal,
    ) -> Result<Self, NodeError> {
        Self::start_configured(
            replica,
            config,
            listener,
            Some(journal),
            CheckpointPolicy::default(),
        )
        .await
    }

    pub async fn start_configured(
        replica: Replica,
        config: PeerConfig,
        listener: TcpListener,
        journal: Option<Journal>,
        checkpoints: CheckpointPolicy,
    ) -> Result<Self, NodeError> {
        Self::start_with_options(
            replica,
            config,
            listener,
            journal,
            NodeOptions {
                checkpoints,
                ..NodeOptions::default()
            },
        )
        .await
    }

    pub async fn start_with_options(
        replica: Replica,
        config: PeerConfig,
        listener: TcpListener,
        journal: Option<Journal>,
        options: NodeOptions,
    ) -> Result<Self, NodeError> {
        if !(1..=ledger_replica::ProposalBatch::MAX_COMMANDS)
            .contains(&options.max_commands_per_entry)
        {
            return Err(NodeError::Invalid(
                "max_commands_per_entry must be within the Raft batch limit".into(),
            ));
        }
        if journal.is_some() && replica.learner_id().is_some() {
            return Err(NodeError::Invalid(
                "synchronous protection requires the three-voter topology".into(),
            ));
        }
        let Some(journal) = journal else {
            return Self::start_with(replica, config, listener, None, options).await;
        };
        let (replica, verifier) = tokio::task::spawn_blocking(move || {
            let verifier = ledger_replica::JournalVerifier::open(&replica, journal)?;
            Ok::<_, NodeError>((replica, verifier))
        })
        .await
        .map_err(|_| NodeError::ThreadFailed)??;
        let protection = Protection::new(verifier);
        Self::start_with(replica, config, listener, Some(protection), options).await
    }

    async fn start_with(
        replica: Replica,
        config: PeerConfig,
        listener: TcpListener,
        protection: Option<Protection>,
        options: NodeOptions,
    ) -> Result<Self, NodeError> {
        let mut members: Vec<_> = config.peers.iter().map(|p| p.node).collect();
        members.push(config.node);
        members.sort_unstable();
        let mut expected: Vec<_> = replica.members().collect();
        expected.sort_unstable();
        if config.cluster != replica.cluster_id()
            || config.node != replica.node_id()
            || members != expected
        {
            return Err(NodeError::Invalid(
                "peer configuration differs from durable group membership".into(),
            ));
        }
        let service = Service::new(&replica, protection);
        let (handle, sub_rx, control_rx) =
            NodeHandle::channels(replica.ledger_id(), service.mode());
        let feed = replica
            .is_learner()
            .then(|| crate::export::feed(&replica, &handle));
        let (peer_tx, peer_rx) = mpsc::channel(PEER_INBOX);
        let (failure_tx, failure_rx) = mpsc::channel(8);
        let (snapshot_tx, snapshot_rx) = mpsc::channel(8);
        let snapshots = SnapshotResources {
            files: replica.snapshot_files()?,
            history: replica.snapshot_history(),
            events: snapshot_tx,
        };
        let (stop, stop_rx) = watch::channel(false);
        let id = config.node;
        let transport = PeerTransport::start(
            config,
            listener,
            peer_tx,
            failure_tx,
            snapshots,
            stop_rx.clone(),
        )
        .await?;
        let runtime = tokio::runtime::Handle::current();
        let mut driver = Owner::new(
            replica,
            service,
            sub_rx,
            control_rx,
            peer_rx,
            failure_rx,
            snapshot_rx,
        );
        driver.checkpoints.policy = options.checkpoints;
        driver.max_commands_per_entry = options.max_commands_per_entry;
        let (done_tx, done) = watch::channel(false);
        let owner = thread::Builder::new()
            .name(format!("ledger-voter-{id}"))
            .spawn(move || {
                let result = runtime.block_on(async move {
                    let result = driver.run(&transport, stop_rx).await;
                    transport.shutdown().await;
                    if let Some(p) = driver.service.protection_mut() {
                        p.join().await;
                    }
                    driver.checkpoints.join().await;
                    result
                });
                done_tx.send_replace(true);
                result
            })?;
        Ok(Self {
            feed,
            handle,
            stop,
            done,
            owner: Some(owner),
        })
    }

    /// Bounded reads of this learner's applied decisions and original Raft WAL.
    /// Invoke the synchronous feed from a blocking worker thread.
    pub fn learner_feed(&self) -> Option<Arc<dyn ledger_export::LedgerFeed>> {
        self.feed.clone()
    }

    pub fn handle(&self) -> NodeHandle {
        self.handle.clone()
    }

    /// Observe unsolicited listener or voter termination. Call shutdown to join
    /// the owner and retrieve the original error.
    pub async fn wait_stopped(&self) {
        let mut done = self.done.clone();
        let _ = done.wait_for(|finished| *finished).await;
    }

    /// Wait for the voter thread to release both RocksDB directories and sockets.
    pub async fn shutdown(mut self) -> Result<(), NodeError> {
        self.stop.send_replace(true);
        let owner = self.owner.take().expect("node owns its voter thread");
        tokio::task::spawn_blocking(move || owner.join())
            .await
            .map_err(|_| NodeError::ThreadFailed)?
            .map_err(|_| NodeError::ThreadFailed)?
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
