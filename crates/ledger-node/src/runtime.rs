//! One owner drives Raft and performs synchronous RocksDB writes on its own
//! thread. Network workers exchange bounded messages with this owner.
use std::collections::VecDeque;
use std::time::Duration;

use ledger_replica::{ProposalBatch, Replica, ReplicaError};
use raft::eraftpb::{Message, MessageType};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{self, Instant, MissedTickBehavior};

use crate::checkpoints::Checkpoints;
use crate::host::{Control, NodeError, NodeStatus, Submission, Waiter};
use crate::protection::Protection;
use crate::reads::Reads;
use crate::service_mode::Service;
use crate::transport::{PeerTransport, ReceivedSnapshot, SnapshotEvent, SnapshotTransferStats};

const MAX_IN_FLIGHT_BATCHES: usize = 8;
const PEER_MESSAGES_PER_TURN: usize = 32;
const TICK_INTERVAL: Duration = Duration::from_millis(50);
// Cap work spent gathering an entry on the owner thread, even during a burst.
const GATHER_BUDGET: Duration = Duration::from_micros(250);

struct PendingBatch {
    round: u64,
    index: u64,
    term: u64,
    waiters: Vec<Waiter>,
}

struct SnapshotReceipt {
    admitted: oneshot::Sender<bool>,
    installed: Option<SnapshotTransferStats>,
}

pub(crate) struct Owner {
    replica: Replica,
    submissions: mpsc::Receiver<Submission>,
    controls: mpsc::Receiver<Control>,
    inbox: mpsc::Receiver<Message>,
    failures: mpsc::Receiver<u64>,
    snapshots: mpsc::Receiver<SnapshotEvent>,
    last_snapshot_transfer: Option<SnapshotTransferStats>,
    pub(crate) service: Service,
    pub(crate) checkpoints: Checkpoints,
    reads: Reads,
    pending: VecDeque<PendingBatch>,
    carry: Option<Submission>,
    pub(crate) max_commands_per_entry: usize,
    #[cfg(feature = "batch-diagnostics")]
    proposal_queue_nanos: u128,
    proposed_batches: u64,
    proposed_commands: u64,
}

impl Owner {
    pub(crate) fn new(
        replica: Replica,
        service: Service,
        submissions: mpsc::Receiver<Submission>,
        controls: mpsc::Receiver<Control>,
        inbox: mpsc::Receiver<Message>,
        failures: mpsc::Receiver<u64>,
        snapshots: mpsc::Receiver<SnapshotEvent>,
    ) -> Self {
        Self {
            replica,
            submissions,
            controls,
            inbox,
            failures,
            snapshots,
            last_snapshot_transfer: None,
            service,
            checkpoints: Checkpoints::new(0),
            reads: Reads::default(),
            pending: VecDeque::new(),
            carry: None,
            max_commands_per_entry: ProposalBatch::MAX_COMMANDS,
            #[cfg(feature = "batch-diagnostics")]
            proposal_queue_nanos: 0,
            proposed_batches: 0,
            proposed_commands: 0,
        }
    }

    pub(crate) async fn run(
        &mut self,
        transport: &PeerTransport,
        mut stop: watch::Receiver<bool>,
    ) -> Result<(), NodeError> {
        let mut network_stop = transport.stopped();
        self.checkpoints.last_index = self.replica.current_checkpoint()?.map_or(0, |c| c.index);
        self.checkpoints.error = self.replica.checkpoint_cleanup_warning().map(str::to_owned);
        let mut ticks = time::interval(TICK_INTERVAL);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        while !*stop.borrow() {
            let mut snapshot_receipt = None;
            // Select fairly among sources. Gathering a batch never sleeps or waits
            // for peers, so an idle request and a ready heartbeat can both progress.
            tokio::select! {
                _ = stop.changed() => break,
                result = Protection::poll(self.service.protection_mut()) => result?,
                _ = self.checkpoints.poll() => {},
                _ = network_stop.wait_for(|stopped| *stopped) => {
                    if *stop.borrow() { break; }
                    return Err(NodeError::Invalid(transport.failure_reason()
                        .unwrap_or("peer transport stopped").to_owned()));
                }
                _ = ticks.tick() => self.replica.tick(),
                Some(peer) = self.failures.recv() => self.replica.report_unreachable(peer),
                Some(event) = self.snapshots.recv() => { snapshot_receipt = self.snapshot_event(event)?; }
                Some(first) = self.inbox.recv() => {
                    self.step(first)?;
                    for _ in 1..PEER_MESSAGES_PER_TURN {
                        let Ok(next) = self.inbox.try_recv() else { break };
                        self.step(next)?;
                    }
                }
                Some(control) = self.controls.recv() => self.inspect(control, transport)?,
                Some(first) = async {
                    match self.carry.take() {
                        Some(s) => Some(s),
                        None => self.submissions.recv().await,
                    }
                } => self.propose(first)?,
            }
            self.reads.begin(&mut self.replica)?;
            for message in self.replica.drain_ready()? {
                if message.get_msg_type() == MessageType::MsgSnapshot {
                    let term = message.term;
                    let to = message.to;
                    let index = message.get_snapshot().get_metadata().index;
                    match transport.send_snapshot(message) {
                        Ok(lease) => self.checkpoints.pin_snapshot_tail(to, term, index, lease),
                        Err(peer) => self.replica.report_snapshot(peer, term, index, false),
                    }
                } else if let Err(peer) = transport.send(message) {
                    self.replica.report_unreachable(peer);
                }
            }
            if let Some(receipt) = snapshot_receipt {
                if let Some(stats) = receipt.installed {
                    self.checkpoints.last_index = self
                        .replica
                        .installed_snapshot()
                        .expect("installed Ready snapshot");
                    self.last_snapshot_transfer = Some(stats);
                }
                let _ = receipt.admitted.send(true);
            }
            self.resolve()?;
            let read = self
                .reads
                .advance(&mut self.replica, self.service.protection())?;
            if let Some(p) = self.service.protection_mut() {
                let applied = self.replica.applied_index();
                let write = self
                    .pending
                    .iter()
                    .any(|b| !b.waiters.is_empty() && b.index <= applied);
                p.schedule(applied, read || write, self.replica.is_leader());
            }
            self.checkpoints.update_replication_cut(&self.replica)?;
            self.checkpoints.schedule(&self.replica);
        }
        Ok(())
    }

    fn snapshot_event(
        &mut self,
        event: SnapshotEvent,
    ) -> Result<Option<SnapshotReceipt>, NodeError> {
        match event {
            SnapshotEvent::Prepare {
                from,
                term,
                index,
                reply,
            } => {
                let acceptable = index > 0
                    && term >= self.replica.term()
                    && (term > self.replica.term()
                        || self.replica.leader_id().is_none_or(|leader| leader == from));
                let reuse = if acceptable {
                    Ok(self.replica.current_checkpoint()?)
                } else {
                    Err(())
                };
                let _ = reply.send(reuse);
            }
            SnapshotEvent::Received(received) => {
                let ReceivedSnapshot {
                    message,
                    image,
                    history,
                    admitted,
                    stats,
                } = *received;
                match self.replica.step_snapshot(message, image, history) {
                    Ok(selected) => {
                        return Ok(Some(SnapshotReceipt {
                            admitted,
                            installed: selected.then_some(stats),
                        }));
                    }
                    Err(ReplicaError::Invalid(_)) => {
                        let _ = admitted.send(false);
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            SnapshotEvent::Report {
                peer,
                term,
                index,
                success,
                lease,
            } => {
                if self
                    .checkpoints
                    .report_snapshot(peer, index, success, &lease)
                {
                    self.replica.report_snapshot(peer, term, index, success);
                }
            }
        }
        Ok(None)
    }

    fn step(&mut self, message: Message) -> Result<(), NodeError> {
        match self.replica.step(message) {
            Ok(())
            | Err(
                ReplicaError::Invalid(_)
                | ReplicaError::Consensus(
                    raft::Error::StepLocalMsg
                    | raft::Error::StepPeerNotFound
                    | raft::Error::ProposalDropped,
                ),
            ) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn inspect(&mut self, control: Control, transport: &PeerTransport) -> Result<(), NodeError> {
        match control {
            Control::Checkpoint(reply) => self.checkpoints.start(&self.replica, Some(reply)),
            Control::Read(request) => match self.service.protection() {
                Some(p) if p.fenced => {
                    let _ = request.reply.send(Err(NodeError::Fenced));
                }
                None if !self.service.mode().serves_clients() => {
                    let _ = request.reply.send(Err(NodeError::Unprotected));
                }
                _ => self.reads.request(&mut self.replica, *request)?,
            },
            Control::ExportFrontier(reply) => {
                let _ = reply.send(self.replica.applied_index());
            }
            Control::ExportPage {
                after,
                through,
                reply,
            } => {
                let _ = reply.send(self.replica.export_decisions(after, through));
            }
            Control::Status(reply) => {
                let _ = reply.send(NodeStatus {
                    node: self.replica.node_id(),
                    generation: self.replica.cluster_id(),
                    learner_id: self.replica.learner_id(),
                    is_leader: self.replica.is_leader(),
                    is_learner: self.replica.is_learner(),
                    leader_id: self.replica.leader_id(),
                    term: self.replica.term(),
                    applied_index: self.replica.applied_index(),
                    account_cache: self.replica.cache_stats(),
                    checkpoint_index: self.replica.current_checkpoint()?.map(|c| c.index),
                    recovered_from: self.replica.recovered_from(),
                    installed_snapshot: self.replica.installed_snapshot(),
                    last_snapshot_transfer: self.last_snapshot_transfer,
                    snapshot_maintenance_error: transport.snapshot_maintenance_error(),
                    peer_matches: self
                        .replica
                        .members()
                        .filter(|id| *id != self.replica.node_id())
                        .filter_map(|id| {
                            self.replica.matched_index(id).map(|matched| (id, matched))
                        })
                        .collect(),
                    checkpoint_error: self.checkpoints.error.clone(),
                    protected_index: self.service.protection().map(Protection::frontier),
                    protection_error: self.service.protection().and_then(|p| p.last_error.clone()),
                    raft_syncs: self.replica.raft_syncs(),
                    journal_append_publications: self
                        .service
                        .protection()
                        .map_or(0, |p| p.append_publications),
                    #[cfg(feature = "batch-diagnostics")]
                    proposal_queue_nanos: self.proposal_queue_nanos,
                    proposed_batches: self.proposed_batches,
                    proposed_commands: self.proposed_commands,
                });
            }
            Control::Lookup {
                id, command, reply, ..
            } => {
                let _ = reply.send(self.replica.outcome_for(id, &command)?);
            }
        }
        Ok(())
    }

    fn propose(&mut self, first: Submission) -> Result<(), NodeError> {
        if self.service.protection().is_some_and(|p| p.fenced) {
            first.waiter.finish(Err(NodeError::Fenced));
            return Ok(());
        }
        // Even cancelled writes occupy a real Raft index. Bound their total
        // distance beyond independent coverage when the journal is offline.
        let lagged = self.service.protection().is_some_and(|p| {
            self.replica.is_leader()
                && self.replica.last_index().saturating_sub(p.frontier())
                    >= MAX_IN_FLIGHT_BATCHES as u64
        });
        if self.pending.len() >= MAX_IN_FLIGHT_BATCHES || lagged {
            // Retire an admitted item even if heartbeats keep this leader alive
            // while appends are stalled. Otherwise dead callers can pin the queue.
            let error = if first.waiter.deadline <= Instant::now() {
                NodeError::Deadline
            } else {
                NodeError::Busy
            };
            first.waiter.finish(Err(error));
            return Ok(());
        }
        let mut batch = ProposalBatch::new(self.replica.ledger_id());
        let mut waiters = Vec::new();
        let mut item = Some(first);
        #[cfg(feature = "batch-diagnostics")]
        let mut queue_nanos = 0u128;
        let mut gather_until = Instant::now() + GATHER_BUDGET;
        // Inspect at most one batch worth, including expired/cancelled requests.
        for _ in 0..ProposalBatch::MAX_COMMANDS {
            let Some(submission) = item.take().or_else(|| self.submissions.try_recv().ok()) else {
                break;
            };
            if submission.waiter.reply.is_closed() {
                continue;
            }
            if submission.waiter.deadline <= Instant::now() {
                submission.waiter.finish(Err(NodeError::Deadline));
            } else if !self.replica.is_leader() {
                submission
                    .waiter
                    .finish(Err(NodeError::NotLeader(self.replica.leader_id())));
            } else if batch.push(&submission.prepared)? {
                #[cfg(feature = "batch-diagnostics")]
                {
                    queue_nanos += submission.enqueued.elapsed().as_nanos();
                }
                // If a deadline is close, send what we already have immediately.
                let deadline = submission.waiter.deadline;
                gather_until =
                    gather_until.min(deadline.checked_sub(GATHER_BUDGET).unwrap_or(deadline));
                waiters.push(submission.waiter);
                if batch.len() == self.max_commands_per_entry || Instant::now() >= gather_until {
                    break;
                }
            } else {
                // It was admitted as an individual command and fits the next empty batch.
                self.carry = Some(submission);
                break;
            }
        }
        if batch.is_empty() {
            return Ok(());
        }
        let count = batch.len() as u64;
        let before = self.replica.last_index();
        match self.replica.propose_batch(batch) {
            Ok(()) if self.replica.last_index() == before + 1 => {
                #[cfg(feature = "batch-diagnostics")]
                {
                    self.proposal_queue_nanos =
                        self.proposal_queue_nanos.saturating_add(queue_nanos);
                }
                self.proposed_batches += 1;
                self.proposed_commands += count;
                self.pending.push_back(PendingBatch {
                    round: self.service.protection().map_or(0, Protection::next_round),
                    index: before + 1,
                    term: self.replica.term(),
                    waiters,
                });
            }
            Ok(()) | Err(ReplicaError::Consensus(raft::Error::ProposalDropped)) => {
                for waiter in waiters {
                    waiter.finish(Err(NodeError::Busy));
                }
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    fn resolve(&mut self) -> Result<(), NodeError> {
        let now = Instant::now();
        // Even if every caller goes away, keep the entry counted against the
        // in-flight cap until it applies or the leader loses its term.
        for batch in &mut self.pending {
            let mut i = 0;
            while i < batch.waiters.len() {
                if batch.waiters[i].deadline <= now || batch.waiters[i].reply.is_closed() {
                    batch
                        .waiters
                        .swap_remove(i)
                        .finish(Err(NodeError::Deadline));
                } else {
                    i += 1;
                }
            }
        }
        while let Some(first) = self.pending.front() {
            let same_leader = self.replica.is_leader() && self.replica.term() == first.term;
            let fenced = self.service.protection().is_some_and(|p| p.fenced);
            let covered = self
                .service
                .protection()
                .is_none_or(|p| p.covers(first.round, first.index));
            if same_leader && !fenced && (self.replica.applied_index() < first.index || !covered) {
                break;
            }
            let batch = self.pending.pop_front().expect("front was present");
            for waiter in batch.waiters {
                let outcome = if fenced {
                    Err(NodeError::Fenced)
                } else if same_leader {
                    Ok(self
                        .replica
                        .outcome_for(waiter.id, &waiter.command)?
                        .ok_or_else(|| {
                            NodeError::Invalid("published proposal lacks a decision".into())
                        })?)
                } else {
                    Err(NodeError::LeadershipChanged)
                };
                waiter.finish(outcome);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeHandle;
    use ledger_core::{Command, LedgerId, RequestId, ReservationId};
    use ledger_replica::ReplicaConfig;

    #[tokio::test]
    async fn saturated_replication_still_retires_submissions() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = ReplicaConfig {
            cluster: 1,
            node: 1,
            voters: vec![1, 2, 3],
            ledger: LedgerId(5),
            asset: "USD".into(),
            scale: 2,
        };
        let replica = Replica::bootstrap(dir.path(), cfg).unwrap();
        let (handle, submissions, controls) =
            NodeHandle::channels(replica.ledger_id(), crate::ServiceMode::DiagnosticVoter);
        let (_, inbox) = mpsc::channel(2);
        let (_, failures) = mpsc::channel(2);
        let (_, snapshots) = mpsc::channel(2);
        let mut owner = Owner::new(
            replica,
            Service::DiagnosticVoter,
            submissions,
            controls,
            inbox,
            failures,
            snapshots,
        );
        for i in 1..=MAX_IN_FLIGHT_BATCHES {
            owner.pending.push_back(PendingBatch {
                round: 0,
                index: i as u64,
                term: 1,
                waiters: vec![],
            });
        }
        let command = Command::Void {
            reservation: ReservationId(1),
        };
        let (result, ()) = tokio::join!(
            handle.submit_local(
                RequestId(2),
                command,
                Instant::now() + Duration::from_secs(5)
            ),
            async {
                let first = owner.submissions.recv().await.unwrap();
                owner.propose(first).unwrap();
            }
        );
        assert!(matches!(result, Err(NodeError::Busy)));
        assert_eq!(owner.pending.len(), MAX_IN_FLIGHT_BATCHES);
        assert_eq!(owner.replica.last_index(), 0);
    }
}
