use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use ledger_core::{
    AccountId, Balance, Bounds, Command, Error, Funding, LedgerId, Line, Outcome, RequestId, Side,
};
use ledger_node::{Node, NodeHandle, Peer, PeerConfig};
use ledger_replica::{Replica, ReplicaConfig};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, timeout};

#[path = "network/account_reads.rs"]
mod account_reads;
#[path = "network/api.rs"]
mod api;
#[cfg(unix)]
#[path = "network/cli.rs"]
mod cli;
#[cfg(feature = "batch-diagnostics")]
#[path = "network/group_commit.rs"]
mod group_commit;
#[path = "network/recovery.rs"]
mod recovery;

const BOOK: LedgerId = LedgerId(7);
const CLUSTER: u128 = 42;
const WAIT: Duration = Duration::from_secs(15);

struct Identity {
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
    cert_der: Vec<u8>,
}

fn leaf(domain: &str, ca: &Certificate, issuer_key: &KeyPair) -> Identity {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec![domain.to_owned()]).unwrap();
    params.distinguished_name.push(DnType::CommonName, domain);
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let cert = params.signed_by(&key, ca, issuer_key).unwrap();
    Identity {
        cert_pem: cert.pem().into_bytes(),
        key_pem: key.serialize_pem().into_bytes(),
        cert_der: cert.der().to_vec(),
    }
}

struct Member {
    domain: String,
    identity: Identity,
    addr: SocketAddr,
    listener: Option<TcpListener>,
    node: Option<Node>,
}

struct Cluster {
    directory: TempDir,
    ca: Certificate,
    ca_key: KeyPair,
    members: BTreeMap<u64, Member>,
}

impl Cluster {
    async fn new() -> Self {
        Self::with_ids([1, 2, 3]).await
    }

    async fn with_ids<const N: usize>(ids: [u64; N]) -> Self {
        let ca_key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::default();
        params
            .distinguished_name
            .push(DnType::CommonName, "ledger test CA");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca = params.self_signed(&ca_key).unwrap();
        let mut members = BTreeMap::new();
        for id in ids {
            let domain = format!("voter-{id}.ledger.test");
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            members.insert(
                id,
                Member {
                    identity: leaf(&domain, &ca, &ca_key),
                    domain,
                    addr: listener.local_addr().unwrap(),
                    listener: Some(listener),
                    node: None,
                },
            );
        }
        Self {
            directory: TempDir::new().unwrap(),
            ca,
            ca_key,
            members,
        }
    }

    fn config(&self, id: u64) -> PeerConfig {
        let identity = &self.members[&id].identity;
        PeerConfig {
            cluster: CLUSTER,
            node: id,
            ca_pem: self.ca.pem().into_bytes(),
            cert_pem: identity.cert_pem.clone(),
            key_pem: identity.key_pem.clone(),
            peers: self
                .members
                .iter()
                .filter(|(peer, _)| **peer != id)
                .map(|(&node, member)| Peer {
                    node,
                    endpoint: format!("https://{}", member.addr),
                    domain: member.domain.clone(),
                    cert_der: member.identity.cert_der.clone(),
                })
                .collect(),
        }
    }

    async fn start(&mut self, id: u64, config: PeerConfig) {
        let member = self.members.get_mut(&id).unwrap();
        let path = self.directory.path().join(format!("n{id}"));
        let cfg = ReplicaConfig {
            node: id,
            cluster: CLUSTER,
            ledger: BOOK,
            voters: vec![1, 2, 3],
            asset: "USD".into(),
            scale: 2,
        };
        let (listener, replica) = match member.listener.take() {
            Some(listener) => (listener, Replica::bootstrap(&path, cfg).unwrap()),
            None => (
                TcpListener::bind(member.addr).await.unwrap(),
                Replica::reopen(&path, cfg).unwrap(),
            ),
        };
        member.node = Some(Node::start(replica, config, listener).await.unwrap());
    }

    fn handle(&self, id: u64) -> NodeHandle {
        self.members[&id].node.as_ref().unwrap().handle()
    }

    async fn leader(&self) -> u64 {
        timeout(WAIT, async {
            loop {
                for (&id, member) in &self.members {
                    if let Some(node) = &member.node
                        && node.handle().status().await.unwrap().is_leader
                    {
                        return id;
                    }
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("reachable majority did not elect a leader")
    }

    async fn stop(&mut self, id: u64) {
        let node = self.members.get_mut(&id).unwrap().node.take().unwrap();
        timeout(WAIT, node.shutdown())
            .await
            .expect("node shutdown stalled")
            .unwrap();
    }
}

fn account(n: u128) -> AccountId {
    AccountId::new(BOOK, n)
}

fn create(n: u128, normal: Side) -> Command {
    Command::CreateAccount {
        account: account(n),
        normal,
        code: 1,
        bounds: Bounds {
            min: Some(Balance::positive(0)),
            max: None,
        },
    }
}

fn post(lines: Vec<Line>) -> Command {
    Command::Post {
        funding: Funding::Explicit(lines),
        conditions: vec![],
        corrects: None,
    }
}

async fn submit(handle: &NodeHandle, id: u128, command: Command) -> Result<Outcome, Error> {
    let deadline = Instant::now() + WAIT;
    tokio::time::timeout_at(
        deadline,
        handle.submit_local(RequestId(id), command, deadline),
    )
    .await
    .expect("local submit deadline elapsed")
    .unwrap()
}

async fn await_outcome(
    handle: &NodeHandle,
    id: u128,
    command: &Command,
    expected: &Result<Outcome, Error>,
) {
    timeout(WAIT, async {
        loop {
            if let Some(actual) = handle
                .local_outcome(RequestId(id), command.clone())
                .await
                .unwrap()
            {
                assert_eq!(&actual, expected);
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("voter did not apply the entry");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_quorum_batches_payments_and_replays_them_after_leader_loss() {
    let mut cluster = Cluster::new().await;
    for id in 1..=3 {
        cluster.start(id, cluster.config(id)).await;
    }
    let original = cluster.leader().await;
    let leader = cluster.handle(original);

    // An idle request must flush even if a second request never arrives.
    let before = leader.status().await.unwrap();
    assert_eq!(
        submit(&leader, 1, create(1, Side::Debit)).await,
        Ok(Outcome::AccountCreated(account(1)))
    );
    let idle = leader.status().await.unwrap();
    assert_eq!(idle.proposed_batches - before.proposed_batches, 1);

    let mut burst = JoinSet::new();
    const CALLERS: usize = 48;
    let barrier = Arc::new(Barrier::new(CALLERS));
    for number in 2..2 + CALLERS as u128 {
        let handle = leader.clone();
        let barrier = Arc::clone(&barrier);
        burst.spawn(async move {
            barrier.wait().await;
            assert_eq!(
                submit(&handle, number, create(number, Side::Credit)).await,
                Ok(Outcome::AccountCreated(account(number)))
            );
        });
    }
    while let Some(task) = burst.join_next().await {
        task.unwrap();
    }
    let loaded = leader.status().await.unwrap();
    assert_eq!(
        loaded.proposed_commands - idle.proposed_commands,
        CALLERS as u64
    );
    let batches = loaded.proposed_batches - idle.proposed_batches;
    assert!(
        (1..CALLERS as u64).contains(&batches),
        "{CALLERS} commands used {batches} batches"
    );

    let funding = post(vec![
        Line::debit(account(1), 100),
        Line::credit(account(2), 100),
    ]);
    submit(&leader, 55, funding.clone()).await.unwrap();
    let lines = vec![
        Line::debit(account(2), 80),
        Line::credit(account(3), 77),
        Line::credit(account(4), 3),
    ];
    let payment = post(lines.clone());
    let accepted = submit(&leader, 60, payment.clone()).await;
    assert!(
        matches!(&accepted, Ok(Outcome::Posted(entry)) if entry.id.0 == 2 && entry.lines.as_ref() == lines)
    );
    let rejected = Err(Error::BelowMinimum(account(2)));
    assert_eq!(submit(&leader, 61, payment.clone()).await, rejected);
    cluster.stop(original).await;

    let replacement = cluster.leader().await;
    let successor = cluster.handle(replacement);
    // Fresh funding would make the rejected payment affordable if it were evaluated again.
    let new_entry = submit(&successor, 70, funding.clone()).await;
    assert!(matches!(&new_entry, Ok(Outcome::Posted(entry)) if entry.id.0 == 3));
    assert_eq!(submit(&successor, 60, payment.clone()).await, accepted);
    assert_eq!(submit(&successor, 61, payment.clone()).await, rejected);
    let conflict = Err(Error::IdConflict(RequestId(60)));
    assert_eq!(submit(&successor, 60, funding.clone()).await, conflict);

    cluster.start(original, cluster.config(original)).await;
    for id in 1..=3 {
        let voter = cluster.handle(id);
        await_outcome(&voter, 70, &funding, &new_entry).await;
        await_outcome(&voter, 60, &payment, &accepted).await;
        await_outcome(&voter, 61, &payment, &rejected).await;
        await_outcome(&voter, 60, &funding, &conflict).await;
    }
    for id in 1..=3 {
        cluster.stop(id).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_ca_valid_certificate_with_the_wrong_voter_pin_cannot_form_a_quorum() {
    let mut cluster = Cluster::new().await;
    let mut config = cluster.config(1);
    let peer = config.peers.iter_mut().find(|peer| peer.node == 2).unwrap();
    // Same issuer, same DNS name, different credential for voter 2.
    peer.cert_der = leaf(&peer.domain, &cluster.ca, &cluster.ca_key).cert_der;
    cluster.start(1, config).await;
    cluster.start(2, cluster.config(2)).await;
    let observation = Instant::now() + Duration::from_secs(3);
    while Instant::now() < observation {
        for id in [1, 2] {
            assert!(!cluster.handle(id).status().await.unwrap().is_leader);
        }
        sleep(Duration::from_millis(20)).await;
    }
    cluster.stop(1).await;
    cluster.start(1, cluster.config(1)).await;
    let leader = cluster.handle(cluster.leader().await);
    assert_eq!(
        submit(&leader, 1, create(1, Side::Credit)).await,
        Ok(Outcome::AccountCreated(account(1)))
    );
    cluster.stop(1).await;
    cluster.stop(2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incomplete_peer_handshake_expires_without_stopping_the_listener() {
    let mut cluster = Cluster::new().await;
    cluster.start(1, cluster.config(1)).await;
    let socket = tokio::net::TcpStream::connect(cluster.members[&1].addr)
        .await
        .unwrap();
    timeout(Duration::from_secs(6), async {
        let mut byte = [0; 1];
        loop {
            socket.readable().await.unwrap();
            match socket.try_read(&mut byte) {
                Ok(0) => break,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => break,
                result => panic!("unexpected peer handshake data: {result:?}"),
            }
        }
    })
    .await
    .expect("the peer handshake must have a time limit");
    assert_eq!(cluster.handle(1).status().await.unwrap().node, 1);
    cluster.stop(1).await;
}
