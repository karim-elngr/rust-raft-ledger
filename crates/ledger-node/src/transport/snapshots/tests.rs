use super::*;
use crate::transport::wire::StepReceipt;
use crate::transport::wire::raft_peer_server::{RaftPeer, RaftPeerServer};
use crate::transport::{
    Certificate, ClientTlsConfig, Endpoint, Identity, Receiver, Response, Server, ServerTlsConfig,
    TcpListener, TcpListenerStream,
};
use ledger_core::LedgerId;
use ledger_replica::{Replica, ReplicaConfig};
use raft::eraftpb::Entry;
use std::sync::atomic::{AtomicU8, Ordering};
use tempfile::tempdir;

#[tokio::test(start_paused = true)]
async fn progressing_offers_can_outlive_a_day_and_idle_or_cancelled_offers_release_the_pin() {
    let lease = OfferLease::new();
    let monitor = tokio::spawn({
        let lease = lease.clone();
        async move { lease.ended().await }
    });
    for _ in 0..3 {
        tokio::time::advance(OFFER_IDLE / 2).await;
        lease.activity();
        tokio::task::yield_now().await;
        assert!(!monitor.is_finished());
    }
    tokio::time::advance(OFFER_IDLE).await;
    monitor.await.unwrap();
    assert!(!lease.active());
    lease.activity();
    assert!(!lease.active());

    let superseded = OfferLease::new();
    superseded.cancel();
    superseded.ended().await;
    assert!(!superseded.active());
}

#[derive(Default)]
struct Faults {
    // 1 corrupts a manifest, 2 stalls a file, 3/4 corrupt a proof, 5 models an old peer.
    mode: AtomicU8,
    stalled: tokio::sync::Notify,
    files: Mutex<Vec<(u32, u64)>>,
    history: Mutex<Vec<u64>>,
}

struct FaultySource {
    receiver: Receiver,
    faults: Arc<Faults>,
}

#[tonic::async_trait]
impl RaftPeer for FaultySource {
    async fn read_snapshot_replay(
        &self,
        request: Request<SnapshotReplayRead>,
    ) -> Result<Response<SnapshotReplayProof>, Status> {
        let mut response = self.receiver.read_snapshot_replay(request).await?;
        match self.faults.mode.load(Ordering::SeqCst) {
            3 => response.get_mut().digest.truncate(1),
            4 => response.get_mut().digest[0] ^= 1,
            5 => return Err(Status::unimplemented("legacy peer")),
            _ => {}
        }
        Ok(response)
    }
    async fn step(&self, r: Request<StepRequest>) -> Result<Response<StepReceipt>, Status> {
        self.receiver.step(r).await
    }
    async fn offer_snapshot(
        &self,
        r: Request<StepRequest>,
    ) -> Result<Response<StepReceipt>, Status> {
        self.receiver.offer_snapshot(r).await
    }
    async fn read_snapshot(
        &self,
        r: Request<SnapshotRead>,
    ) -> Result<Response<SnapshotChunk>, Status> {
        let file = r.get_ref().file;
        let offset = r.get_ref().offset;
        let mut response = self.receiver.read_snapshot(r).await?;
        if let Some(file) = file {
            let count = {
                let mut requests = self.faults.files.lock().unwrap();
                requests.push((file, offset));
                requests.len()
            };
            if count == 2 && self.faults.mode.load(Ordering::SeqCst) == 2 {
                self.faults.stalled.notify_one();
                return std::future::pending().await; // The test cuts this TCP connection.
            }
        } else if self.faults.mode.load(Ordering::SeqCst) == 1 {
            response.get_mut().data[0] ^= 1;
        }
        Ok(response)
    }
    async fn read_snapshot_history(
        &self,
        r: Request<SnapshotHistoryRead>,
    ) -> Result<Response<HistoryPage>, Status> {
        let after = r.get_ref().after;
        let result = self.receiver.read_snapshot_history(r).await?;
        self.faults.history.lock().unwrap().push(after);
        Ok(result)
    }
}

fn voter(path: &std::path::Path, node: u64, last: u64) -> Replica {
    let mut replica = Replica::bootstrap(
        path,
        ReplicaConfig {
            cluster: 41,
            node,
            voters: vec![1, 2, 3],
            ledger: LedgerId(5),
            asset: "CAD".into(),
            scale: 2,
        },
    )
    .unwrap();
    let mut append = Message {
        from: 3,
        to: node,
        term: 3,
        commit: last,
        ..Default::default()
    };
    append.set_msg_type(MessageType::MsgAppend);
    append.entries.extend((1..=last).map(|index| Entry {
        term: 3,
        index,
        ..Default::default()
    }));
    replica.step(append).unwrap();
    replica.drain_ready().unwrap();
    replica
}

fn message(reference: CheckpointRef) -> Message {
    let mut m = Message {
        from: 1,
        to: 2,
        term: 4,
        ..Default::default()
    };
    m.set_msg_type(MessageType::MsgSnapshot);
    let snap = m.mut_snapshot();
    snap.data = reference.snapshot_token().into();
    let meta = snap.mut_metadata();
    meta.index = reference.index;
    meta.term = reference.term;
    meta.mut_conf_state().voters = vec![1, 2, 3];
    m
}

fn resources(replica: &Replica) -> (SnapshotResources, mpsc::Receiver<SnapshotEvent>) {
    let (events, rx) = mpsc::channel(4);
    (
        SnapshotResources {
            files: replica.snapshot_files().unwrap(),
            history: replica.snapshot_history(),
            events,
        },
        rx,
    )
}

struct TestTls {
    server: ServerTlsConfig,
    client: ClientTlsConfig,
    client_pin: Vec<u8>,
}

fn tls() -> TestTls {
    use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose};
    let mut ca = CertificateParams::default();
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    let issuer = KeyPair::generate().unwrap();
    let ca = ca.self_signed(&issuer).unwrap();
    let issue = |name: &str| {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![name.to_owned()]).unwrap();
        params.distinguished_name.push(DnType::CommonName, name);
        let cert = params.signed_by(&key, &ca, &issuer).unwrap();
        (
            Identity::from_pem(cert.pem(), key.serialize_pem()),
            cert.der().to_vec(),
        )
    };
    let (server, _) = issue("source.test");
    let (client, client_pin) = issue("recipient.test");
    let roots = Certificate::from_pem(ca.pem());
    TestTls {
        server: ServerTlsConfig::new()
            .identity(server)
            .client_ca_root(roots.clone()),
        client: ClientTlsConfig::new()
            .identity(client)
            .ca_certificate(roots)
            .domain_name("source.test"),
        client_pin,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_chunks_resume_after_disconnect_and_only_verified_images_reach_the_owner() {
    let root = tempdir().unwrap();
    let sender = voter(&root.path().join("source"), 1, 76);
    let recipient = voter(&root.path().join("target"), 2, 7);
    let reference = sender
        .begin_checkpoint()
        .unwrap()
        .unwrap()
        .finish()
        .unwrap();
    let offer = message(reference);
    let mut bad = offer.clone();
    bad.mut_snapshot().mut_metadata().index += 1;
    assert!(super::reference(&bad).is_err());
    let group = 41u128.to_be_bytes();
    let (stop, signal) = watch::channel(false);
    let (source, _) = resources(&sender);
    let source = Arc::new(Snapshots::new(
        1,
        WireGroup::ThreeVoters(group),
        source,
        BTreeMap::new(),
        signal.clone(),
    ));
    let pin = Arc::new(source.resources.files.source(reference).unwrap());
    source.sources.lock().unwrap().insert(
        2,
        OfferedSource {
            files: pin,
            lease: OfferLease::new(),
        },
    );
    let tls = tls();
    let (inbox, mut delivered) = mpsc::channel(4);
    let receiver = Receiver {
        cluster: WireGroup::ThreeVoters(group),
        node: 1,
        pins: [(2, tls.client_pin), (3, vec![0xcc])].into(),
        inbox,
        snapshots: Some(source),
    };
    let faults = Arc::new(Faults::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bulk_address = proxy.local_addr().unwrap();
    let (disconnect, cut) = oneshot::channel();
    // Forward encrypted bytes without terminating TLS. Dropping both ends
    // interrupts the in-flight RPC, then a fresh TCP connection carries retry.
    let relay = tokio::spawn(async move {
        let (mut downstream, _) = proxy.accept().await.unwrap();
        let mut upstream = tokio::net::TcpStream::connect(address).await.unwrap();
        tokio::select! { _ = cut => {}, result = tokio::io::copy_bidirectional(&mut downstream,&mut upstream) => panic!("early connection end: {result:?}") }
        drop((upstream, downstream));
        let (mut downstream, _) = proxy.accept().await.unwrap();
        let mut upstream = tokio::net::TcpStream::connect(address).await.unwrap();
        let _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await;
    });
    let connect = |addr| {
        Endpoint::from_shared(format!("https://{addr}"))
            .unwrap()
            .tls_config(tls.client.clone())
            .unwrap()
            .connect_lazy()
    };
    let channel = connect(bulk_address);
    let control = connect(address);
    let server = Server::builder()
        .tls_config(tls.server)
        .unwrap()
        .add_service(RaftPeerServer::new(FaultySource {
            receiver,
            faults: faults.clone(),
        }));
    let server =
        tokio::spawn(server.serve_with_incoming_shutdown(
            TcpListenerStream::new(listener),
            stopped(signal.clone()),
        ));
    let mut caller = RaftPeerClient::new(control);
    let proof = SnapshotReplayRead {
        version: 1,
        cluster: group.to_vec(),
        from: 2,
        to: 1,
        generation: reference.digest.to_vec(),
        base_index: 7,
        base_term: 3,
    };
    for (request, code) in [
        (
            SnapshotReplayRead {
                from: 3,
                ..proof.clone()
            },
            tonic::Code::PermissionDenied,
        ),
        (
            SnapshotReplayRead {
                generation: vec![0; 32],
                ..proof.clone()
            },
            tonic::Code::NotFound,
        ),
    ] {
        assert_eq!(
            caller
                .read_snapshot_replay(request)
                .await
                .unwrap_err()
                .code(),
            code
        );
    }
    assert_eq!(
        caller
            .read_snapshot_replay(proof)
            .await
            .unwrap()
            .into_inner()
            .digest
            .len(),
        32
    );
    let read = SnapshotRead {
        version: 1,
        cluster: group.to_vec(),
        from: 2,
        to: 1,
        generation: reference.digest.to_vec(),
        file: None,
        offset: 0,
        length: 128,
    };
    for (r, code) in [
        (
            SnapshotRead {
                from: 3,
                ..read.clone()
            },
            tonic::Code::PermissionDenied,
        ),
        (
            SnapshotRead {
                cluster: vec![0; 16],
                ..read.clone()
            },
            tonic::Code::InvalidArgument,
        ),
        (
            SnapshotRead {
                to: 2,
                ..read.clone()
            },
            tonic::Code::InvalidArgument,
        ),
        (
            SnapshotRead {
                generation: vec![0; 32],
                ..read.clone()
            },
            tonic::Code::NotFound,
        ),
        (
            SnapshotRead {
                length: SNAPSHOT_CHUNK_BYTES as u32 + 1,
                ..read
            },
            tonic::Code::InvalidArgument,
        ),
    ] {
        assert_eq!(caller.read_snapshot(r).await.unwrap_err().code(), code);
    }

    let (resources, mut events) = resources(&recipient);
    let target = Snapshots::new(
        2,
        WireGroup::ThreeVoters(group),
        resources,
        [(1, channel.clone()), (3, channel)].into(),
        signal,
    );
    let mut foreign_group = offer.clone();
    foreign_group
        .mut_snapshot()
        .mut_metadata()
        .mut_conf_state()
        .voters = vec![1, 2, 9];
    assert_eq!(
        target.receive(foreign_group).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    let downloads = async {
        faults.mode.store(1, Ordering::SeqCst);
        assert_eq!(
            target.receive(offer.clone()).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        faults.mode.store(2, Ordering::SeqCst);
        let cut = target.receive(offer.clone()).await.unwrap_err();
        assert!(
            matches!(
                cut.code(),
                tonic::Code::Unavailable | tonic::Code::Cancelled | tonic::Code::Unknown
            ),
            "{cut}"
        );
        let interrupted = faults.files.lock().unwrap().clone();
        assert_eq!(interrupted.len(), 2);
        faults.mode.store(0, Ordering::SeqCst);
        target.receive(offer.clone()).await.unwrap();
        let reads = faults.files.lock().unwrap();
        assert_eq!(reads[2], interrupted[1]); // Resume at the missing range.
        assert_eq!(reads.iter().filter(|r| **r == interrupted[0]).count(), 1);
        assert_eq!(*faults.history.lock().unwrap().first().unwrap(), 7);
    };
    let heartbeat_during_stall = async {
        faults.stalled.notified().await;
        assert_eq!(
            target.receive(offer.clone()).await.unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        let mut heartbeat = Message {
            from: 2,
            to: 1,
            term: 4,
            ..Default::default()
        };
        heartbeat.set_msg_type(MessageType::MsgHeartbeat);
        let request = StepRequest {
            version: 1,
            cluster: group.to_vec(),
            from: 2,
            to: 1,
            raft: heartbeat.write_to_bytes().unwrap(),
        };
        caller.step(request).await.unwrap();
        assert_eq!(delivered.recv().await.unwrap(), heartbeat);
        disconnect.send(()).unwrap();
    };
    let owner = async {
        for _ in 0..3 {
            let Some(SnapshotEvent::Prepare { reply, .. }) = events.recv().await else {
                panic!("unverified image reached Raft")
            };
            reply.send(Ok(None)).unwrap();
        }
        let Some(SnapshotEvent::Received(received)) = events.recv().await else {
            panic!("missing verified image")
        };
        let ReceivedSnapshot {
            message,
            image,
            history,
            stats,
            admitted,
        } = *received;
        assert_eq!(message, offer);
        assert_eq!(image.source_reference(), reference);
        assert!(
            stats.manifest_bytes > 0
                && stats.file_bytes > 0
                && stats.reused_or_staged_file_bytes > 0
        );
        assert_eq!(stats.history_entries, 76 - 7);
        assert!(stats.history_bytes > 0);
        assert_eq!(history.resume_after().unwrap(), reference.index);
        assert_eq!(recipient.applied_index(), 7); // Transport staged only.
        admitted.send(true).unwrap();
    };
    timeout(Duration::from_secs(20), async {
        tokio::join!(downloads, owner, heartbeat_during_stall)
    })
    .await
    .unwrap();
    drop(target);
    relay.abort();
    let _ = relay.await;
    drop(caller);
    stop.send_replace(true);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn replay_rejects_invalid_proofs_and_uses_files_with_an_older_peer() {
    let root = tempdir().unwrap();
    let group = 41u128.to_be_bytes();
    let sender = voter(&root.path().join("sender"), 1, 16);
    let mut recipient = voter(&root.path().join("recipient"), 2, 4);
    let base = recipient
        .begin_checkpoint()
        .unwrap()
        .unwrap()
        .finish()
        .unwrap();
    let offered = sender
        .begin_checkpoint()
        .unwrap()
        .unwrap()
        .finish()
        .unwrap();
    let (stop, signal) = watch::channel(false);
    let (source, _) = resources(&sender);
    let source = Arc::new(Snapshots::new(
        1,
        WireGroup::ThreeVoters(group),
        source,
        BTreeMap::new(),
        signal.clone(),
    ));
    let file_pin = Arc::new(source.resources.files.source(offered).unwrap());
    source.sources.lock().unwrap().insert(
        2,
        OfferedSource {
            files: file_pin,
            lease: OfferLease::new(),
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tls = tls();
    let channel = Endpoint::from_shared(format!("https://{}", listener.local_addr().unwrap()))
        .unwrap()
        .tls_config(tls.client)
        .unwrap()
        .connect_lazy();
    let (inbox, _rx) = mpsc::channel(4);
    let faults = Arc::new(Faults::default());
    let service = FaultySource {
        receiver: Receiver {
            cluster: WireGroup::ThreeVoters(group),
            node: 1,
            pins: [(2, tls.client_pin)].into(),
            inbox,
            snapshots: Some(source),
        },
        faults: faults.clone(),
    };
    let server = tokio::spawn(
        Server::builder()
            .tls_config(tls.server)
            .unwrap()
            .add_service(RaftPeerServer::new(service))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                stopped(signal.clone()),
            ),
    );
    let (resources, mut events) = resources(&recipient);
    let target = Snapshots::new(
        2,
        WireGroup::ThreeVoters(group),
        resources,
        [(1, channel.clone()), (3, channel)].into(),
        signal,
    );
    let requests = async {
        for (mode, code) in [
            (3, tonic::Code::DataLoss),
            (4, tonic::Code::FailedPrecondition),
        ] {
            faults.mode.store(mode, Ordering::SeqCst);
            assert_eq!(
                target.receive(message(offered)).await.unwrap_err().code(),
                code
            );
            assert!(faults.files.lock().unwrap().is_empty());
        }
        faults.mode.store(5, Ordering::SeqCst);
        target.receive(message(offered)).await.unwrap();
    };
    let owner = async {
        for _ in 0..3 {
            let Some(SnapshotEvent::Prepare { reply, .. }) = events.recv().await else {
                panic!("invalid proof reached Raft");
            };
            reply.send(Ok(Some(base))).unwrap();
        }
        let Some(SnapshotEvent::Received(ready)) = events.recv().await else {
            panic!("legacy physical image missing");
        };
        let ReceivedSnapshot {
            message,
            image,
            history,
            stats,
            admitted,
        } = *ready;
        assert_eq!(stats.replay_base_index, None);
        assert!(stats.replay_proof_bytes > 0 && stats.file_bytes > 0);
        assert_eq!(recipient.applied_index(), base.index);
        assert!(recipient.step_snapshot(message, image, history).unwrap());
        recipient.drain_ready().unwrap();
        assert_eq!(recipient.applied_index(), offered.index);
        admitted.send(true).unwrap();
    };
    timeout(Duration::from_secs(15), async {
        tokio::join!(requests, owner)
    })
    .await
    .unwrap();
    stop.send_replace(true);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn startup_cleanup_reports_a_missing_storage_path_without_stopping_the_voter() {
    let root = tempdir().unwrap();
    let path = root.path().join("member");
    let node = voter(&path, 2, 1);
    let (resources, _) = resources(&node);
    let (stop, signal) = watch::channel(false);
    let snapshots = Arc::new(Snapshots::new(
        2,
        WireGroup::ThreeVoters(41u128.to_be_bytes()),
        resources,
        BTreeMap::new(),
        signal,
    ));
    let directory = path.join("checkpoints");
    let disconnected = root.path().join("disconnected-checkpoints");
    std::fs::rename(&directory, &disconnected).unwrap();
    let task = tokio::spawn(snapshots.clone().maintain());
    timeout(Duration::from_secs(5), async {
        while snapshots.maintenance_error().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(!task.is_finished());
    assert_eq!(node.applied_index(), 1);
    stop.send_replace(true);
    task.await.unwrap();
    snapshots.drain_disk().await;
    std::fs::rename(disconnected, directory).unwrap();
}
