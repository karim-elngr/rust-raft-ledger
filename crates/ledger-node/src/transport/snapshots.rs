//! Peer snapshots use independent HTTP/2 connections. A small offer stays
//! pending while the recipient pulls only its missing file and history ranges.
use super::wire::{
    SnapshotChunk, SnapshotHistory as HistoryPage, SnapshotHistoryRead, SnapshotRead,
    SnapshotReplayProof, SnapshotReplayRead,
};
use super::{
    Channel, MAX_CONTROL_BYTES, MAX_ENVELOPE_BYTES, Message, MessageType, ProtobufMessage,
    RaftPeerClient, Request, Status, StepRequest, WireGroup, stopped,
};
use ledger_replica::{
    CheckpointRef, HistoryStage, ImportedCheckpoint, ReplicaError, SNAPSHOT_CHUNK_BYTES,
    SnapshotFiles, SnapshotHistory, SnapshotSource,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::time::{Instant, sleep_until, timeout};

mod receive;

const CHUNK_TIMEOUT: Duration = Duration::from_secs(30);
const OFFER_IDLE: Duration = Duration::from_secs(24 * 3600);
const DISK_SLOTS: u32 = 6;

/// Successful reverse reads extend the offer's idle deadline. The Raft owner
/// can cancel a superseded offer and its append-tail pin together.
#[derive(Clone)]
pub(crate) struct OfferLease(watch::Sender<Option<Instant>>);

impl OfferLease {
    pub(crate) fn new() -> Self {
        Self(watch::channel(Some(Instant::now() + OFFER_IDLE)).0)
    }

    pub(crate) fn active(&self) -> bool {
        self.0.borrow().is_some_and(|until| until > Instant::now())
    }

    fn activity(&self) {
        self.0.send_if_modified(|deadline| {
            let now = Instant::now();
            if deadline.is_some_and(|until| until > now) {
                *deadline = Some(now + OFFER_IDLE);
                return true;
            }
            false
        });
    }

    pub(crate) fn cancel(&self) {
        self.0.send_replace(None);
    }

    pub(crate) fn same_offer(&self, other: &Self) -> bool {
        self.0.same_channel(&other.0)
    }

    async fn ended(&self) {
        let mut rx = self.0.subscribe();
        loop {
            let until = match *rx.borrow_and_update() {
                Some(until) if until > Instant::now() => until,
                _ => return,
            };
            tokio::select! { _ = sleep_until(until) => {}, _ = rx.changed() => {} }
        }
    }
}

#[derive(Clone)]
struct OfferedSource {
    files: Arc<SnapshotSource>,
    lease: OfferLease,
}

pub(crate) struct SnapshotResources {
    pub(crate) files: SnapshotFiles,
    pub(crate) history: SnapshotHistory,
    pub(crate) events: mpsc::Sender<SnapshotEvent>,
}

/// Payload byte counts for the single successful incoming offer. Frame headers,
/// TLS and earlier interrupted attempts are excluded.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SnapshotTransferStats {
    pub manifest_bytes: u64,
    pub file_bytes: u64,
    /// Bytes already present as verified file content or partial staged ranges.
    pub reused_or_staged_file_bytes: u64,
    pub history_bytes: u64,
    pub history_entries: u64,
    /// Local sealed checkpoint from which this voter reconstructed the offered state.
    pub replay_base_index: Option<u64>,
    /// Encoded proof request and response, including peer routing fields.
    pub replay_proof_bytes: u64,
    /// Protobuf request and response bodies for the reverse snapshot RPCs.
    pub rpc_message_bytes: u64,
}

pub(crate) enum SnapshotEvent {
    Prepare {
        from: u64,
        term: u64,
        index: u64,
        reply: oneshot::Sender<Result<Option<CheckpointRef>, ()>>,
    },
    Received(Box<ReceivedSnapshot>),
    Report {
        peer: u64,
        term: u64,
        index: u64,
        success: bool,
        lease: OfferLease,
    },
}

pub(crate) struct ReceivedSnapshot {
    pub(crate) message: Message,
    pub(crate) image: ImportedCheckpoint,
    pub(crate) history: HistoryStage,
    pub(crate) stats: SnapshotTransferStats,
    pub(crate) admitted: oneshot::Sender<bool>,
}

pub(super) fn reference(message: &Message) -> Result<CheckpointRef, Status> {
    if message.get_msg_type() != MessageType::MsgSnapshot
        || !message.has_snapshot()
        || !message.entries.is_empty()
        || message.compute_size() as usize > MAX_CONTROL_BYTES
    {
        return Err(Status::invalid_argument("invalid snapshot offer"));
    }
    let reference = CheckpointRef::from_snapshot(message.get_snapshot())
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
    if message.term < reference.term {
        return Err(Status::invalid_argument(
            "snapshot term exceeds sender term",
        ));
    }
    Ok(reference)
}

fn storage_error(error: ReplicaError) -> Status {
    Status::failed_precondition(error.to_string())
}

pub(super) struct Snapshots {
    node: u64,
    cluster: WireGroup,
    resources: SnapshotResources,
    channels: BTreeMap<u64, Channel>,
    // One source per destination, present only while its worker is making the
    // matching offer. A peer cannot fetch another recipient's retained image.
    sources: Mutex<BTreeMap<u64, OfferedSource>>,
    incoming: Arc<Semaphore>,
    maintenance_error: Mutex<Option<String>>,
    disk: Arc<Semaphore>,
    stop: watch::Receiver<bool>,
}

impl Snapshots {
    pub(super) fn permits_message(&self, message: &Message) -> bool {
        self.resources
            .history
            .permits_message(message.from, message.to, message.get_msg_type())
    }

    pub(super) fn new(
        node: u64,
        cluster: WireGroup,
        resources: SnapshotResources,
        channels: BTreeMap<u64, Channel>,
        stop: watch::Receiver<bool>,
    ) -> Self {
        Self {
            node,
            cluster,
            resources,
            channels,
            sources: Mutex::default(),
            incoming: Arc::new(Semaphore::new(1)),
            maintenance_error: Mutex::default(),
            disk: Arc::new(Semaphore::new(DISK_SLOTS as usize)),
            stop,
        }
    }

    // The permit lives inside the blocking closure: cancellation cannot admit
    // another disk job over the limit, and shutdown waits for outstanding I/O.
    async fn io<T: Send + 'static>(
        &self,
        f: impl FnOnce() -> Result<T, ReplicaError> + Send + 'static,
    ) -> Result<T, Status> {
        let permit = self
            .disk
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("snapshot disk workers busy"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f().map_err(storage_error)
        })
        .await
        .map_err(|_| Status::internal("snapshot worker stopped"))?
    }

    pub(super) fn maintenance_error(&self) -> Option<String> {
        self.maintenance_error
            .lock()
            .expect("snapshot maintenance status lock")
            .clone()
    }

    pub(super) async fn maintain(self: Arc<Self>) {
        let mut ticks = tokio::time::interval(Duration::from_secs(3600));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let clean = async {
            loop {
                ticks.tick().await; // The first tick runs at startup.
                if self.incoming.available_permits() == 0 {
                    continue;
                }
                let files = self.resources.files.clone();
                let error = match self.io(move || files.expire_incoming()).await {
                    Ok(_) => None,
                    Err(status) if status.code() == tonic::Code::ResourceExhausted => continue,
                    Err(status) => Some(status.message().to_owned()),
                };
                let mut previous = self
                    .maintenance_error
                    .lock()
                    .expect("snapshot maintenance status lock");
                if *previous != error {
                    if let Some(message) = &error {
                        eprintln!("snapshot staging cleanup: {message}");
                    }
                    *previous = error;
                }
            }
        };
        tokio::select! {biased; _ = stopped(self.stop.clone()) => {}, _ = clean => {}}
    }

    pub(super) async fn drain_disk(&self) {
        let _ = self.disk.acquire_many(DISK_SLOTS).await;
    }

    fn source(&self, peer: u64, digest: &[u8]) -> Result<OfferedSource, Status> {
        if digest.len() != 32 {
            return Err(Status::invalid_argument("invalid generation digest"));
        }
        self.sources
            .lock()
            .expect("snapshot source lock")
            .get(&peer)
            .filter(|s| s.files.reference().digest == digest && s.lease.active())
            .cloned()
            .ok_or_else(|| Status::not_found("no matching snapshot offered to this voter"))
    }

    pub(super) async fn read(&self, r: SnapshotRead) -> Result<SnapshotChunk, Status> {
        if r.length == 0 || r.length as usize > SNAPSHOT_CHUNK_BYTES {
            return Err(Status::invalid_argument("invalid snapshot chunk length"));
        }
        let source = self.source(r.from, &r.generation)?;
        if let Some(file) = r.file {
            return self
                .io(move || {
                    Ok(SnapshotChunk {
                        data: source.files.read_file(file, r.offset, r.length)?,
                        end: false,
                    })
                })
                .await
                .inspect(|_| source.lease.activity());
        }
        let offset =
            usize::try_from(r.offset).map_err(|_| Status::out_of_range("manifest offset"))?;
        let rest = source
            .files
            .manifest()
            .get(offset..)
            .ok_or_else(|| Status::out_of_range("manifest offset"))?;
        let count = rest.len().min(r.length as usize);
        source.lease.activity();
        Ok(SnapshotChunk {
            data: rest[..count].to_vec(),
            end: count == rest.len(),
        })
    }

    pub(super) async fn read_history(&self, r: SnapshotHistoryRead) -> Result<HistoryPage, Status> {
        let source = self.source(r.from, &r.generation)?;
        let head = source.files.reference();
        if r.after >= head.index {
            return Err(Status::out_of_range("history outside active snapshot"));
        }
        let history = self.resources.history.clone();
        self.io(move || {
            Ok(HistoryPage {
                entries: history.encoded_entries_after(head, r.after)?,
            })
        })
        .await
        .inspect(|_| source.lease.activity())
    }

    pub(super) async fn read_replay_proof(
        &self,
        r: SnapshotReplayRead,
    ) -> Result<SnapshotReplayProof, Status> {
        let source = self.source(r.from, &r.generation)?;
        let base = CheckpointRef {
            index: r.base_index,
            term: r.base_term,
            // Equivalent state may be sealed into different local files.
            digest: [0; 32],
        };
        let history = self.resources.history.clone();
        self.io(move || {
            let digest = history.replay_proof(source.files.reference(), base)?;
            Ok(SnapshotReplayProof {
                digest: digest.map_or_else(Vec::new, |hash| hash.to_vec()),
            })
        })
        .await
        .inspect(|_| source.lease.activity())
    }

    async fn offer(
        &self,
        peer: u64,
        client: &mut RaftPeerClient<Channel>,
        message: &Message,
        lease: &OfferLease,
    ) -> Result<(), Status> {
        let reference = reference(message)?;
        let files = self.resources.files.clone();
        let source = Arc::new(self.io(move || files.source(reference)).await?);
        let raft = message
            .write_to_bytes()
            .map_err(|_| Status::invalid_argument("snapshot offer encoding"))?;
        self.sources.lock().expect("snapshot source lock").insert(
            peer,
            OfferedSource {
                files: source,
                lease: lease.clone(),
            },
        );
        // Always drop the published pin, including on transport shutdown.
        let _pin = PublishedSource {
            peer,
            sources: &self.sources,
        };
        let request = Request::new(StepRequest {
            version: self.cluster.version(),
            cluster: self.cluster.bytes().to_vec(),
            from: self.node,
            to: peer,
            raft,
        });
        client.offer_snapshot(request).await?;
        Ok(())
    }

    pub(super) async fn worker(
        self: Arc<Self>,
        peer: u64,
        channel: Channel,
        mut queue: mpsc::Receiver<(Message, OfferLease)>,
    ) {
        let mut client = RaftPeerClient::new(channel).max_decoding_message_size(MAX_ENVELOPE_BYTES);
        let work = async {
            while let Some((message, lease)) = queue.recv().await {
                let index = message.get_snapshot().get_metadata().index;
                let success = tokio::select! { biased;
                    _ = lease.ended() => false,
                    result = self.offer(peer, &mut client, &message, &lease) => result.is_ok(),
                };
                if self
                    .resources
                    .events
                    .send(SnapshotEvent::Report {
                        peer,
                        term: message.term,
                        index,
                        success,
                        lease,
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        };
        tokio::select! { biased; _ = stopped(self.stop.clone()) => {}, _ = work => {} }
    }
}

struct PublishedSource<'a> {
    peer: u64,
    sources: &'a Mutex<BTreeMap<u64, OfferedSource>>,
}

impl Drop for PublishedSource<'_> {
    fn drop(&mut self) {
        self.sources
            .lock()
            .expect("snapshot source lock")
            .remove(&self.peer);
    }
}

#[cfg(test)]
mod tests;
