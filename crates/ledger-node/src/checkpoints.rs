use std::collections::BTreeMap;
use std::future::pending;
use std::time::Duration;

use ledger_replica::{CheckpointRef, Replica};
use serde::Deserialize;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::NodeError;
use crate::transport::OfferLease;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CheckpointPolicy {
    /// Number of applied Raft entries between local checkpoints. Zero allows
    /// manual checkpoints only.
    pub interval_entries: u64,
    /// Keep this many entries preceding a sealed checkpoint available for
    /// ordinary peer replication. Larger gaps can use a snapshot.
    pub retained_entries: u64,
}

impl Default for CheckpointPolicy {
    fn default() -> Self {
        Self {
            interval_entries: 65_536,
            retained_entries: 8_192,
        }
    }
}

pub(crate) type Reply = oneshot::Sender<Result<CheckpointRef, NodeError>>;

struct SnapshotTail {
    term: u64,
    cut: u64,
    lease: OfferLease,
    catchup: Option<(u64, Instant)>,
}

impl SnapshotTail {
    fn active(&mut self, matched: u64, now: Instant) -> bool {
        let Some((previous, deadline)) = &mut self.catchup else {
            return self.lease.active();
        };
        if *deadline <= now {
            return false;
        }
        if matched > *previous {
            *previous = matched;
            *deadline = now + Duration::from_secs(60);
        }
        true
    }
}

impl Drop for SnapshotTail {
    fn drop(&mut self) {
        self.lease.cancel();
    }
}

pub(crate) struct Checkpoints {
    pub(crate) policy: CheckpointPolicy,
    pub(crate) last_index: u64,
    pub(crate) error: Option<String>,
    retry_after: Instant,
    replication_cut: u64,
    tails: BTreeMap<u64, SnapshotTail>,
    task: Option<JoinHandle<Result<CheckpointRef, ledger_replica::ReplicaError>>>,
    reply: Option<Reply>,
}

impl Checkpoints {
    pub(crate) fn new(last_index: u64) -> Self {
        Self {
            policy: CheckpointPolicy::default(),
            last_index,
            error: None,
            retry_after: Instant::now(),
            replication_cut: 0,
            tails: BTreeMap::new(),
            task: None,
            reply: None,
        }
    }

    pub(crate) fn update_replication_cut(&mut self, replica: &Replica) -> Result<(), NodeError> {
        let mut cut = self.last_index.saturating_sub(self.policy.retained_entries);
        let now = Instant::now();
        self.tails.retain(|&peer, pin| {
            if pin.term != replica.term() {
                return false;
            }
            if let Some(matched) = replica.matched_index(peer) {
                if !pin.active(matched, now) {
                    return false;
                }
                if matched >= pin.cut
                    && matched
                        >= replica
                            .last_index()
                            .saturating_sub(self.policy.retained_entries)
                {
                    return false;
                }
                cut = cut.min(pin.cut.max(matched));
                return true;
            }
            false
        });
        if cut > self.replication_cut {
            replica.compact_replication(cut)?;
            self.replication_cut = cut;
        }
        Ok(())
    }

    pub(crate) fn pin_snapshot_tail(&mut self, peer: u64, term: u64, cut: u64, lease: OfferLease) {
        self.tails.insert(
            peer,
            SnapshotTail {
                term,
                cut,
                lease,
                catchup: None,
            },
        );
    }

    pub(crate) fn report_snapshot(
        &mut self,
        peer: u64,
        index: u64,
        success: bool,
        lease: &OfferLease,
    ) -> bool {
        if let Some(pin) = self.tails.get_mut(&peer) {
            if !pin.lease.same_offer(lease) {
                return false;
            }
            if success {
                // Keep ordinary append available while the receiver confirms
                // the new index and starts replicating its post-snapshot tail.
                pin.catchup = Some((index, Instant::now() + Duration::from_secs(60)));
            } else {
                self.tails.remove(&peer);
            }
        }
        true
    }

    pub(crate) fn schedule(&mut self, replica: &Replica) {
        let period = self.policy.interval_entries;
        if period == 0 || self.task.is_some() || Instant::now() < self.retry_after {
            return;
        }
        // Offset the initial captures of a new three-voter group. Most groups
        // apply similar indexes together and would otherwise flush together.
        let spacing = if self.last_index == 0 {
            let rank = replica
                .members()
                .position(|id| id == replica.node_id())
                .expect("configured local member") as u64;
            period - (period / replica.members().count() as u64) * rank
        } else {
            period
        };
        if replica.applied_index().saturating_sub(self.last_index) >= spacing {
            self.start(replica, None);
        }
    }

    pub(crate) fn start(&mut self, replica: &Replica, reply: Option<Reply>) {
        if self.task.is_some() {
            if let Some(reply) = reply {
                let _ = reply.send(Err(NodeError::Busy));
            }
            return;
        }
        self.reply = reply;
        let result = match replica.prepare_checkpoint() {
            Ok(Some(task)) => {
                self.task = Some(tokio::task::spawn_blocking(move || task.finish()));
                return;
            }
            Ok(None) => replica
                .current_checkpoint()
                .map_err(NodeError::from)
                .and_then(|r| r.ok_or_else(|| NodeError::Invalid("checkpoint disappeared".into()))),
            Err(error) => Err(error.into()),
        };
        self.complete(result);
    }

    fn complete(&mut self, result: Result<CheckpointRef, NodeError>) {
        match &result {
            Ok(reference) => {
                self.last_index = self.last_index.max(reference.index);
                self.error = None;
            }
            Err(e) => {
                self.error = Some(e.to_string());
                self.retry_after = Instant::now() + Duration::from_secs(30);
                eprintln!("Local checkpoint failed: {e}");
            }
        }
        if let Some(reply) = self.reply.take() {
            let _ = reply.send(result);
        }
    }

    pub(crate) async fn poll(&mut self) {
        let Some(task) = self.task.as_mut() else {
            return pending().await;
        };
        let result = task
            .await
            .map_err(|_| NodeError::ThreadFailed)
            .and_then(|r| r.map_err(NodeError::from));
        self.task = None;
        self.complete(result);
    }

    pub(crate) async fn join(&mut self) {
        if self.task.is_some() {
            self.poll().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledger_core::LedgerId;
    use ledger_replica::ReplicaConfig;
    use raft::eraftpb::{Entry, Message, MessageType};

    #[test]
    fn late_worker_completion_preserves_a_newer_installed_snapshot_frontier() {
        let mut checkpoints = Checkpoints::new(100);
        // The owner adopted index 100 after the worker released its generation
        // guard, before choosing the completed JoinHandle in its select loop.
        let previous = CheckpointRef {
            index: 90,
            term: 1,
            digest: [0; 32],
        };
        checkpoints.complete(Ok(previous));
        assert_eq!(checkpoints.last_index, 100);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_offer_reports_and_slow_but_advancing_appends_preserve_the_current_pin() {
        let mut checkpoints = Checkpoints::new(20);
        let old = OfferLease::new();
        let live = OfferLease::new();
        checkpoints.pin_snapshot_tail(2, 3, 20, old.clone());
        checkpoints.pin_snapshot_tail(2, 3, 20, live.clone());
        assert!(!old.active());
        assert!(!checkpoints.report_snapshot(2, 20, false, &old));
        assert!(checkpoints.report_snapshot(2, 20, true, &live));
        let pin = checkpoints.tails.get_mut(&2).unwrap();
        for matched in [21, 22, 23] {
            tokio::time::advance(Duration::from_secs(45)).await;
            assert!(pin.active(matched, Instant::now()));
        }
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!(!pin.active(23, Instant::now()));
    }

    #[test]
    fn waiting_for_a_checkpoint_worker_leaves_the_voter_free_to_process_commits() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let disk = tempfile::tempdir().unwrap();
            let mut voter = Replica::bootstrap(
                disk.path(),
                ReplicaConfig {
                    cluster: 41,
                    node: 2,
                    voters: vec![1, 2, 3],
                    ledger: LedgerId(41),
                    asset: "USD".into(),
                    scale: 2,
                },
            )
            .unwrap();
            let append = |v: &mut Replica, index: u64| {
                let mut message = Message {
                    from: 1,
                    to: 2,
                    term: 1,
                    index: index - 1,
                    log_term: u64::from(index > 1),
                    commit: index,
                    ..Default::default()
                };
                message.set_msg_type(MessageType::MsgAppend);
                message.entries.push(Entry {
                    index,
                    term: 1,
                    ..Default::default()
                });
                v.step(message).unwrap();
                v.drain_ready().unwrap();
                assert_eq!(v.applied_index(), index);
            };
            append(&mut voter, 1);
            let (release, blocked) = std::sync::mpsc::channel::<()>();
            let (started, ready) = oneshot::channel();
            let worker = tokio::task::spawn_blocking(move || {
                let _ = started.send(());
                let _ = blocked.recv();
            });
            ready.await.unwrap();
            let mut checkpoints = Checkpoints::new(0);
            let (reply, result) = oneshot::channel();
            checkpoints.start(&voter, Some(reply));
            append(&mut voter, 2);
            assert!(voter.current_checkpoint().unwrap().is_none());
            drop(release);
            worker.await.unwrap();
            checkpoints.join().await;
            assert_eq!(result.await.unwrap().unwrap().index, 2);
        });
    }
}
