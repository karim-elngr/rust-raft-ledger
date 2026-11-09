use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use protobuf::Message as ProtobufMessage;
use raft::eraftpb::{Message, MessageType};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig,
};
use tonic::{Request, Response, Status};

use crate::NodeError;

mod snapshots;
pub(crate) use snapshots::OfferLease;
pub use snapshots::SnapshotTransferStats;
use snapshots::Snapshots;
pub(crate) use snapshots::{ReceivedSnapshot, SnapshotEvent, SnapshotResources};

mod wire {
    tonic::include_proto!("ledger.peer.v1");
}
use wire::raft_peer_client::RaftPeerClient;
use wire::raft_peer_server::{RaftPeer, RaftPeerServer};
use wire::{
    SnapshotChunk, SnapshotHistory, SnapshotHistoryRead, SnapshotRead, SnapshotReplayProof,
    SnapshotReplayRead, StepReceipt, StepRequest,
};

#[derive(Clone, Copy)]
enum WireGroup {
    ThreeVoters([u8; 16]),
    WithLearner([u8; 32]),
}

impl WireGroup {
    fn version(self) -> u32 {
        match self {
            Self::ThreeVoters(_) => 1,
            Self::WithLearner(_) => 2,
        }
    }
    fn bytes(&self) -> &[u8] {
        match self {
            Self::ThreeVoters(id) => id,
            Self::WithLearner(digest) => digest,
        }
    }
}
use ledger_replica::MAX_RAFT_MESSAGE_BYTES as MAX_MESSAGE_BYTES;
const MAX_CONTROL_BYTES: usize = 64 << 10;
const MAX_ENVELOPE_BYTES: usize = MAX_MESSAGE_BYTES + 128;
const RPC_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
pub struct Peer {
    pub node: u64,
    pub endpoint: String,
    /// The cluster CA must issue this exact server DNS identity exclusively to this voter.
    pub domain: String,
    /// Exact DER leaf certificate that this voter presents when calling our listener.
    pub cert_der: Vec<u8>,
}

#[derive(Clone)]
pub struct PeerConfig {
    pub cluster: u128,
    pub node: u64,
    pub ca_pem: Vec<u8>,
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
    pub peers: Vec<Peer>,
}

fn is_control(kind: MessageType) -> bool {
    use MessageType::*;
    matches!(
        kind,
        MsgHeartbeat
            | MsgHeartbeatResponse
            | MsgRequestVote
            | MsgRequestVoteResponse
            | MsgRequestPreVote
            | MsgRequestPreVoteResponse
    )
}

fn validate_message(message: &Message, bytes: usize) -> Result<(), Status> {
    let kind = message.get_msg_type();
    if kind == MessageType::MsgSnapshot || message.has_snapshot() {
        return Err(Status::unimplemented(
            "snapshot messages require OfferSnapshot",
        ));
    }
    if raft::raw_node::is_local_msg(kind) || kind == MessageType::MsgPropose {
        return Err(Status::invalid_argument("message type is not a peer RPC"));
    }
    if bytes > MAX_MESSAGE_BYTES || (is_control(kind) && bytes > MAX_CONTROL_BYTES) {
        return Err(Status::resource_exhausted(
            "Raft message exceeds its lane limit",
        ));
    }
    Ok(())
}

struct Receiver {
    cluster: WireGroup,
    node: u64,
    pins: BTreeMap<u64, Vec<u8>>,
    inbox: mpsc::Sender<Message>,
    snapshots: Option<Arc<Snapshots>>,
}

impl Receiver {
    fn authenticate(
        &self,
        version: u32,
        cluster: &[u8],
        from: u64,
        to: u64,
        leaf_der: &[u8],
    ) -> Result<(), Status> {
        if self.pins.get(&from).map(Vec::as_slice) != Some(leaf_der) {
            return Err(Status::permission_denied(
                "certificate does not identify this sender",
            ));
        }
        if version != self.cluster.version() || cluster != self.cluster.bytes() || to != self.node {
            return Err(Status::invalid_argument(
                "incompatible version, cluster or recipient",
            ));
        }
        Ok(())
    }

    fn decode(&self, request: &StepRequest, leaf: &[u8], limit: usize) -> Result<Message, Status> {
        self.authenticate(
            request.version,
            &request.cluster,
            request.from,
            request.to,
            leaf,
        )?;
        if request.raft.len() > limit {
            return Err(Status::resource_exhausted("Raft message is too large"));
        }
        let message = Message::parse_from_bytes(&request.raft)
            .map_err(|_| Status::invalid_argument("invalid Raft message encoding"))?;
        if message.from != request.from || message.to != request.to {
            return Err(Status::invalid_argument(
                "Raft route does not match the envelope",
            ));
        }
        if self
            .snapshots
            .as_ref()
            .is_some_and(|s| !s.permits_message(&message))
        {
            return Err(Status::permission_denied(
                "Raft message incompatible with member role",
            ));
        }
        Ok(message)
    }

    fn admit(&self, request: StepRequest, leaf_der: &[u8]) -> Result<(), Status> {
        let message = self.decode(&request, leaf_der, MAX_MESSAGE_BYTES)?;
        validate_message(&message, request.raft.len())?;
        // Acquire the data slot and control reserve atomically so concurrent
        // Append RPCs cannot race each other into consuming the final slots.
        let reserve = if is_control(message.get_msg_type()) {
            0
        } else {
            (self.inbox.max_capacity() / 2).min(8)
        };
        let mut permits = self
            .inbox
            .try_reserve_many(1 + reserve)
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => {
                    Status::resource_exhausted("Raft inbox is full")
                }
                mpsc::error::TrySendError::Closed(_) => Status::unavailable("voter is stopped"),
            })?;
        permits
            .next()
            .expect("reserved at least one slot")
            .send(message);
        Ok(())
    }
}

// Keep the certificate chain alive while checking the leaf, without copying
// the certificate for each chunk or exposing unauthenticated request bodies.
fn with_leaf<T, V>(
    request: Request<T>,
    f: impl FnOnce(T, &[u8]) -> Result<V, Status>,
) -> Result<V, Status> {
    let certs = request
        .peer_certs()
        .ok_or_else(|| Status::unauthenticated("client certificate required"))?;
    let leaf = certs
        .first()
        .ok_or_else(|| Status::unauthenticated("missing client leaf certificate"))?;
    f(request.into_inner(), leaf.as_ref())
}

#[tonic::async_trait]
impl RaftPeer for Receiver {
    async fn step(&self, request: Request<StepRequest>) -> Result<Response<StepReceipt>, Status> {
        with_leaf(request, |request, leaf| self.admit(request, leaf))?;
        Ok(Response::new(StepReceipt {}))
    }

    async fn offer_snapshot(
        &self,
        request: Request<StepRequest>,
    ) -> Result<Response<StepReceipt>, Status> {
        let message = with_leaf(request, |request, leaf| {
            self.decode(&request, leaf, MAX_CONTROL_BYTES)
        })?;
        self.snapshot_service()?.receive(message).await?;
        Ok(Response::new(StepReceipt {}))
    }

    async fn read_snapshot(
        &self,
        request: Request<SnapshotRead>,
    ) -> Result<Response<SnapshotChunk>, Status> {
        let request = with_leaf(request, |r, leaf| {
            self.authenticate(r.version, &r.cluster, r.from, r.to, leaf)?;
            Ok(r)
        })?;
        Ok(Response::new(self.snapshot_service()?.read(request).await?))
    }

    async fn read_snapshot_history(
        &self,
        request: Request<SnapshotHistoryRead>,
    ) -> Result<Response<SnapshotHistory>, Status> {
        let request = with_leaf(request, |r, leaf| {
            self.authenticate(r.version, &r.cluster, r.from, r.to, leaf)?;
            Ok(r)
        })?;
        Ok(Response::new(
            self.snapshot_service()?.read_history(request).await?,
        ))
    }

    async fn read_snapshot_replay(
        &self,
        request: Request<SnapshotReplayRead>,
    ) -> Result<Response<SnapshotReplayProof>, Status> {
        let request = with_leaf(request, |r, leaf| {
            self.authenticate(r.version, &r.cluster, r.from, r.to, leaf)?;
            Ok(r)
        })?;
        Ok(Response::new(
            self.snapshot_service()?.read_replay_proof(request).await?,
        ))
    }
}

impl Receiver {
    fn snapshot_service(&self) -> Result<&Snapshots, Status> {
        self.snapshots
            .as_deref()
            .ok_or_else(|| Status::unavailable("snapshot service stopped"))
    }
}

struct Lanes {
    control: mpsc::Sender<StepRequest>,
    data: mpsc::Sender<StepRequest>,
    snapshot: mpsc::Sender<(Message, OfferLease)>,
}

pub(crate) struct PeerTransport {
    cluster: WireGroup,
    node: u64,
    lanes: BTreeMap<u64, Lanes>,
    stop: watch::Sender<bool>,
    failure: Arc<OnceLock<String>>,
    tasks: JoinSet<()>,
    snapshots: Arc<Snapshots>,
}

async fn stopped(mut signal: watch::Receiver<bool>) {
    let _ = signal.wait_for(|stop| *stop).await;
}

async fn worker(
    channel: Channel,
    peer: u64,
    mut queue: mpsc::Receiver<StepRequest>,
    unreachable: mpsc::Sender<u64>,
    stop: watch::Receiver<bool>,
) {
    let mut client = RaftPeerClient::new(channel).max_encoding_message_size(MAX_ENVELOPE_BYTES);
    let send_loop = async {
        while let Some(envelope) = queue.recv().await {
            let mut request = Request::new(envelope);
            request.set_timeout(RPC_TIMEOUT);
            // Also bound time spent waiting for connection readiness.
            if !matches!(timeout(RPC_TIMEOUT, client.step(request)).await, Ok(Ok(_)))
                && unreachable.send(peer).await.is_err()
            {
                break;
            }
        }
    };
    tokio::select! { biased; _ = stopped(stop) => {}, _ = send_loop => {} }
}

impl PeerTransport {
    pub(crate) async fn start(
        config: PeerConfig,
        listener: TcpListener,
        inbox: mpsc::Sender<Message>,
        unreachable: mpsc::Sender<u64>,
        resources: SnapshotResources,
        shutdown: watch::Receiver<bool>,
    ) -> Result<Self, NodeError> {
        let membership = resources.history.membership();
        let expected: BTreeSet<_> = membership
            .voters
            .iter()
            .chain(&membership.learners)
            .copied()
            .collect();
        let configured: BTreeSet<_> = config
            .peers
            .iter()
            .map(|p| p.node)
            .chain([config.node])
            .collect();
        if expected != configured
            || config.node == 0
            || config.cluster == 0
            || config.peers.len() + 1 != expected.len()
            || inbox.max_capacity() < 2
        {
            return Err(NodeError::Invalid(
                "expected the complete durable group membership and a bounded inbox".into(),
            ));
        }
        let identity = Identity::from_pem(config.cert_pem, config.key_pem);
        let ca = Certificate::from_pem(config.ca_pem);
        let tls = ServerTlsConfig::new()
            .identity(identity.clone())
            .client_ca_root(ca.clone())
            .client_auth_optional(false)
            .timeout(RPC_TIMEOUT);
        let server = Server::builder()
            .tls_config(tls)?
            .concurrency_limit_per_connection(8)
            .max_concurrent_streams(8);
        let mut domains = BTreeSet::new();
        let mut pins = BTreeMap::new();
        let mut channels = Vec::new();
        for peer in config.peers {
            let domain = peer.domain.to_ascii_lowercase();
            if peer.node == 0
                || peer.node == config.node
                || pins.contains_key(&peer.node)
                || peer.cert_der.is_empty()
                || pins.values().any(|cert| cert == &peer.cert_der)
                || domain.is_empty()
                || domain.ends_with('.')
                || domain.contains('*')
                || !domains.insert(domain.clone())
            {
                return Err(NodeError::Invalid(
                    "peer IDs, DNS names and certificate pins must be distinct".into(),
                ));
            }
            let endpoint = Endpoint::from_shared(peer.endpoint)?;
            if endpoint.uri().scheme_str() != Some("https") || endpoint.uri().host().is_none() {
                return Err(NodeError::Invalid(
                    "peer endpoints must be absolute HTTPS URIs".into(),
                ));
            }
            let client_tls = ClientTlsConfig::new()
                .ca_certificate(ca.clone())
                .identity(identity.clone())
                .domain_name(domain);
            let endpoint = endpoint
                .connect_timeout(RPC_TIMEOUT)
                .tls_config(client_tls)?;
            // Separate connections keep bulk HTTP/2 flow control off the Raft lanes.
            let channel = endpoint.clone().connect_lazy();
            // Detect a severed transfer while the offer awaits its remote receipt.
            let bulk = endpoint
                .http2_keep_alive_interval(Duration::from_secs(30))
                .keep_alive_timeout(Duration::from_secs(30))
                .connect_lazy();
            pins.insert(peer.node, peer.cert_der);
            channels.push((peer.node, channel, bulk));
        }
        let cluster = match resources.files.learner_group_digest() {
            Some(digest) => WireGroup::WithLearner(digest),
            None => WireGroup::ThreeVoters(config.cluster.to_be_bytes()),
        };
        let (stop, signal) = watch::channel(false);
        let snapshots = Arc::new(Snapshots::new(
            config.node,
            cluster,
            resources,
            channels
                .iter()
                .map(|(id, _, bulk)| (*id, bulk.clone()))
                .collect(),
            signal.clone(),
        ));
        let service = RaftPeerServer::new(Receiver {
            cluster,
            node: config.node,
            pins,
            inbox,
            snapshots: Some(snapshots.clone()),
        })
        .max_decoding_message_size(MAX_ENVELOPE_BYTES);
        let mut tasks = JoinSet::new();
        tasks.spawn(snapshots.clone().maintain());
        let external_stop = stop.clone();
        let local_signal = signal.clone();
        tasks.spawn(async move {
            tokio::select!{ _ = stopped(shutdown) => { external_stop.send_replace(true); }, _ = stopped(local_signal) => {} }
        });
        let server_stopped = stop.clone();
        let server_signal = signal.clone();
        let failure = Arc::new(OnceLock::new());
        let server_failure = failure.clone();
        let accept_failure = failure.clone();
        let accept_stop = stop.clone();
        // Tonic retries every incoming stream error. A permanently broken
        // listener must instead wake the voter owner and stop this transport.
        let incoming = TcpListenerStream::new(listener).take_while(move |item| {
            if let Err(error) = item {
                use std::io::ErrorKind::*;
                if !matches!(
                    error.kind(),
                    Interrupted | ConnectionAborted | ConnectionReset | WouldBlock
                ) {
                    let _ = accept_failure.set(format!("peer listener accept failed: {error}"));
                    accept_stop.send_replace(true);
                    return false;
                }
            }
            true
        });
        tasks.spawn(async move {
            let mut server = server;
            let result = server
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, stopped(server_signal))
                .await;
            if !*server_stopped.borrow() {
                let reason = match result {
                    Err(error) => format!("peer listener failed: {error:?}")
                        .chars()
                        .take(512)
                        .collect(),
                    Ok(()) => String::from("peer listener ended unexpectedly"),
                };
                let _ = server_failure.set(reason);
            }
            server_stopped.send_replace(true);
        });
        let mut lanes = BTreeMap::new();
        for (peer, channel, bulk) in channels {
            // Per peer, up to 16 small control messages and eight data messages
            // may wait. Each lane has its own in-flight RPC on the shared channel.
            let (control, control_rx) = mpsc::channel(16);
            let (data, data_rx) = mpsc::channel(8);
            tasks.spawn(worker(
                channel.clone(),
                peer,
                control_rx,
                unreachable.clone(),
                signal.clone(),
            ));
            tasks.spawn(worker(
                channel,
                peer,
                data_rx,
                unreachable.clone(),
                signal.clone(),
            ));
            let (snapshot, offers) = mpsc::channel(1);
            tasks.spawn(snapshots.clone().worker(peer, bulk, offers));
            lanes.insert(
                peer,
                Lanes {
                    control,
                    data,
                    snapshot,
                },
            );
        }
        Ok(Self {
            cluster,
            node: config.node,
            lanes,
            stop,
            failure,
            tasks,
            snapshots,
        })
    }

    pub(crate) fn stopped(&self) -> watch::Receiver<bool> {
        self.stop.subscribe()
    }

    pub(crate) fn snapshot_maintenance_error(&self) -> Option<String> {
        self.snapshots.maintenance_error()
    }

    pub(crate) fn failure_reason(&self) -> Option<&str> {
        self.failure.get().map(String::as_str)
    }

    /// Queue without waiting for DNS, the remote peer or its inbox.
    pub(crate) fn send(&self, message: Message) -> Result<(), u64> {
        let to = message.to;
        let lanes = self.lanes.get(&to).ok_or(to)?;
        if *self.stop.borrow() || message.from != self.node {
            return Err(to);
        }
        validate_message(&message, message.compute_size() as usize).map_err(|_| to)?;
        let lane = if is_control(message.get_msg_type()) {
            &lanes.control
        } else {
            &lanes.data
        };
        let permit = lane.try_reserve().map_err(|_| to)?;
        let raft = message.write_to_bytes().map_err(|_| to)?;
        permit.send(StepRequest {
            version: self.cluster.version(),
            cluster: self.cluster.bytes().to_vec(),
            from: self.node,
            to,
            raft,
        });
        Ok(())
    }

    pub(crate) fn send_snapshot(&self, message: Message) -> Result<OfferLease, u64> {
        let peer = message.to;
        if *self.stop.borrow()
            || message.from != self.node
            || snapshots::reference(&message).is_err()
        {
            return Err(peer);
        }
        let lease = OfferLease::new();
        self.lanes
            .get(&peer)
            .ok_or(peer)?
            .snapshot
            .try_send((message, lease.clone()))
            .map_err(|_| peer)?;
        Ok(lease)
    }

    pub(crate) async fn shutdown(mut self) {
        self.stop.send_replace(true);
        // A remote client can keep its HTTP/2 connection open during shutdown.
        // Bound the graceful drain, then cancel and join remaining workers.
        if timeout(RPC_TIMEOUT, async {
            while self.tasks.join_next().await.is_some() {}
        })
        .await
        .is_err()
        {
            self.tasks.shutdown().await;
        }
        self.snapshots.drain_disk().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn heartbeat(from: u64, to: u64) -> Message {
        let mut m = Message {
            from,
            to,
            term: 3,
            ..Default::default()
        };
        m.set_msg_type(MessageType::MsgHeartbeat);
        m
    }

    fn receiver() -> (Receiver, mpsc::Receiver<Message>) {
        let (tx, rx) = mpsc::channel(2);
        (
            Receiver {
                cluster: WireGroup::ThreeVoters(9u128.to_be_bytes()),
                node: 2,
                pins: [(1, vec![0xaa]), (3, vec![0xcc])].into(),
                inbox: tx,
                snapshots: None,
            },
            rx,
        )
    }

    fn envelope() -> StepRequest {
        StepRequest {
            version: 1,
            cluster: 9u128.to_be_bytes().to_vec(),
            from: 1,
            to: 2,
            raft: heartbeat(1, 2).write_to_bytes().unwrap(),
        }
    }

    #[test]
    fn admits_only_the_message_bound_to_the_authenticated_sender_and_recipient() {
        let (receiver, mut inbox) = receiver();
        let valid = envelope();
        receiver.admit(valid.clone(), &[0xaa]).unwrap();
        assert_eq!(inbox.try_recv().unwrap(), heartbeat(1, 2));
        for (request, cert) in [
            (valid.clone(), vec![0xcc]),
            (
                StepRequest {
                    from: 4,
                    ..valid.clone()
                },
                vec![0xaa],
            ),
            (
                StepRequest {
                    version: 2,
                    ..valid.clone()
                },
                vec![0xaa],
            ),
            (
                StepRequest {
                    cluster: vec![0; 16],
                    ..valid.clone()
                },
                vec![0xaa],
            ),
            (
                StepRequest {
                    to: 3,
                    ..valid.clone()
                },
                vec![0xaa],
            ),
            (
                StepRequest {
                    raft: heartbeat(3, 2).write_to_bytes().unwrap(),
                    ..valid.clone()
                },
                vec![0xaa],
            ),
            (
                StepRequest {
                    raft: heartbeat(1, 3).write_to_bytes().unwrap(),
                    ..valid
                },
                vec![0xaa],
            ),
        ] {
            assert!(receiver.admit(request, &cert).is_err());
        }
        assert!(inbox.try_recv().is_err());
    }

    #[test]
    fn rejects_local_control_proposals_snapshots_and_oversized_messages() {
        let (receiver, mut inbox) = receiver();
        for kind in [
            MessageType::MsgHup,
            MessageType::MsgBeat,
            MessageType::MsgUnreachable,
            MessageType::MsgPropose,
            MessageType::MsgSnapshot,
        ] {
            let mut m = heartbeat(1, 2);
            m.set_msg_type(kind);
            let request = StepRequest {
                raft: m.write_to_bytes().unwrap(),
                ..envelope()
            };
            assert!(receiver.admit(request, &[0xaa]).is_err());
        }
        let request = StepRequest {
            raft: vec![0; MAX_MESSAGE_BYTES + 1],
            ..envelope()
        };
        assert!(receiver.admit(request, &[0xaa]).is_err());
        assert!(inbox.try_recv().is_err());
    }

    #[test]
    fn data_saturation_leaves_admission_capacity_for_a_vote_or_heartbeat() {
        let (receiver, mut inbox) = receiver();
        let mut append = heartbeat(1, 2);
        append.set_msg_type(MessageType::MsgAppend);
        let data = StepRequest {
            raft: append.write_to_bytes().unwrap(),
            ..envelope()
        };
        receiver.admit(data.clone(), &[0xaa]).unwrap();
        assert_eq!(
            receiver.admit(data, &[0xaa]).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        receiver.admit(envelope(), &[0xaa]).unwrap();
        assert_eq!(inbox.try_recv().unwrap(), append);
        assert_eq!(inbox.try_recv().unwrap(), heartbeat(1, 2));
    }
}
