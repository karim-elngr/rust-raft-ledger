use ledger_api::client::{ClientSeed, LedgerClient as RoutingClient};
use ledger_api::v1::{self, ledger_client::LedgerClient as WireClient};
use ledger_node::{ClientServer, ClientTls};
use ledger_recovery::Journal;
use tonic::transport::{
    Certificate as Trust, Channel, ClientTlsConfig, Endpoint, Identity as Credentials,
};
use tonic::{Code, Request, Response, Status};

use super::*;

struct PublicApi {
    servers: BTreeMap<u64, ClientServer>,
    channels: BTreeMap<u64, Channel>,
    endpoints: BTreeMap<u64, Endpoint>,
    routed: RoutingClient,
    ordinary: BTreeMap<u64, Channel>,
    ca_pem: Vec<u8>,
}

impl PublicApi {
    async fn start(cluster: &Cluster, first_seed: u64) -> Self {
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::default();
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "public API test CA");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let ca_pem = ca.pem().into_bytes();
        let service = leaf("backend.ledger.test", &ca, &ca_key);
        let payments = leaf("payments.ledger.test", &ca, &ca_key);
        let mut ordinary = BTreeMap::new();
        let mut seeds = Vec::new();
        let mut channels = BTreeMap::new();
        let mut endpoints = BTreeMap::new();
        let mut servers = BTreeMap::new();
        for id in 1..=3 {
            let domain = format!("api-{id}.ledger.test");
            let server = leaf(&domain, &ca, &ca_key);
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("https://{}", listener.local_addr().unwrap());
            let tls = ClientTls {
                ca_pem: ca_pem.clone(),
                cert_pem: server.cert_pem,
                key_pem: server.key_pem,
            };
            let serving = ClientServer::start_authorized(
                cluster.handle(id),
                listener,
                tls,
                Some(vec![service.cert_der.clone()]),
            )
            .await
            .unwrap();
            let target = Endpoint::from_shared(endpoint.clone()).unwrap();
            let authenticated = target
                .tls_config(
                    ClientTlsConfig::new()
                        .domain_name(domain.clone())
                        .ca_certificate(Trust::from_pem(&ca_pem))
                        .identity(Credentials::from_pem(&service.cert_pem, &service.key_pem)),
                )
                .unwrap();
            ordinary.insert(
                id,
                Endpoint::from_shared(endpoint.clone())
                    .unwrap()
                    .tls_config(
                        ClientTlsConfig::new()
                            .domain_name(domain.clone())
                            .ca_certificate(Trust::from_pem(&ca_pem))
                            .identity(Credentials::from_pem(&payments.cert_pem, &payments.key_pem)),
                    )
                    .unwrap()
                    .connect_lazy(),
            );
            let channel = authenticated.connect().await.unwrap();
            endpoints.insert(id, authenticated);
            servers.insert(id, serving);
            channels.insert(id, channel);
            seeds.push(ClientSeed {
                node: id,
                endpoint,
                domain,
            });
        }
        seeds.sort_by_key(|seed| seed.node != first_seed);
        let routed =
            RoutingClient::connect(seeds, ca_pem.clone(), service.cert_pem, service.key_pem)
                .unwrap();
        Self {
            servers,
            channels,
            endpoints,
            routed,
            ordinary,
            ca_pem,
        }
    }

    fn wire(&self, id: u64) -> WireClient<Channel> {
        WireClient::new(self.channels[&id].clone())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_listener_rejects_a_raft_peer_certificate_at_the_tls_handshake() {
    use std::error::Error;
    use std::io;

    let external = TempDir::new().unwrap();
    let remote = external.path().join("journal");
    Journal::init(&remote, &super::recovery::spec(), CLUSTER).unwrap();
    let mut cluster = super::recovery::original_cluster(&remote).await;
    let voter = 1;
    let mut api = PublicApi::start(&cluster, voter).await;
    let authorized = &api.endpoints[&voter];
    let peer = &cluster.members[&voter].identity;
    let untrusted = Endpoint::from(authorized.uri().clone())
        .tls_config(
            ClientTlsConfig::new()
                .domain_name(format!("api-{voter}.ledger.test"))
                .ca_certificate(Trust::from_pem(&api.ca_pem))
                .identity(Credentials::from_pem(&peer.cert_pem, &peer.key_pem)),
        )
        .unwrap();
    let invalid_entry = v1::GetEntryRequest {
        ledger_id: BOOK.0.to_string(),
        entry_id: 0,
    };

    let validate_with_backend = async || {
        let channel = authorized.connect().await.unwrap();
        let status = WireClient::new(channel)
            .get_entry(invalid_entry.clone())
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
        assert_eq!(status.message(), "journal entry IDs start at one");
    };
    timeout(WAIT, async {
        validate_with_backend().await;
        // TLS 1.3 can finish the client's handshake before the server verifies the
        // client certificate. Some HTTP/2 drivers observe only connection teardown.
        // A retry on a fresh connection can expose the peer's underlying TLS alert.
        for attempt in 1..=8 {
            let status = WireClient::new(untrusted.connect_lazy())
                .get_entry(invalid_entry.clone())
                .await
                .unwrap_err();
            assert_ne!(
                status.code(),
                Code::InvalidArgument,
                "peer certificate reached the ledger: {status:?}"
            );
            let mut cause = status.source();
            let mut rejected_by_ca = false;
            while let Some(error) = cause {
                if let Some(io) = error.downcast_ref::<io::Error>()
                    && io.kind() == io::ErrorKind::InvalidData
                    && io.to_string() == "received fatal alert: UnknownCA"
                {
                    rejected_by_ca = true;
                    break;
                }
                cause = error.source();
            }
            if rejected_by_ca {
                break;
            }
            assert!(
                attempt < 8,
                "no remote unknown-CA TLS alert in {attempt} connections; last result {status:?}"
            );
        }
        validate_with_backend().await;
    })
    .await
    .expect("timed out verifying public listener client authentication");

    for (_, server) in std::mem::take(&mut api.servers) {
        server.shutdown().await.unwrap();
    }
    for id in 1..=3 {
        cluster.stop(id).await;
    }
}

fn wallet(id: u64) -> v1::CreateAccountRequest {
    v1::CreateAccountRequest {
        ledger_id: BOOK.0.to_string(),
        request_id: id.to_string(),
        account_id: id.to_string(),
        code: 1,
        normal: if id == 1 {
            v1::Side::Debit
        } else {
            v1::Side::Credit
        }
        .into(),
        bounds: Some(v1::Bounds {
            min: Some("0".into()),
            max: None,
        }),
    }
}

pub(super) fn line(id: u64, side: v1::Side, amount: &str) -> v1::Line {
    v1::Line {
        account_id: id.to_string(),
        side: side.into(),
        amount: amount.into(),
    }
}

pub(super) fn post_request(id: u64, funding: v1::funding::Kind) -> v1::PostEntryRequest {
    v1::PostEntryRequest {
        ledger_id: BOOK.0.to_string(),
        request_id: id.to_string(),
        funding: Some(v1::Funding {
            kind: Some(funding),
        }),
        ..Default::default()
    }
}

fn deadline() -> Instant {
    Instant::now() + WAIT
}

fn short<T>(value: T) -> Request<T> {
    let mut request = Request::new(value);
    request.set_timeout(Duration::from_millis(600));
    request
}

fn withheld<T: std::fmt::Debug>(result: Result<Response<T>, Status>) {
    let status = result.unwrap_err();
    assert!(
        matches!(
            status.code(),
            Code::DeadlineExceeded | Code::Unavailable | Code::Cancelled
        ),
        "{status}"
    );
}

fn posted(decision: &v1::Decision) -> &v1::JournalEntry {
    match accepted(decision) {
        v1::outcome::Kind::Posted(entry) => entry,
        other => panic!("expected a posted entry, got {other:?}"),
    }
}

fn accepted(decision: &v1::Decision) -> &v1::outcome::Kind {
    match &decision.result {
        Some(v1::decision::Result::Accepted(v1::Outcome { kind: Some(kind) })) => kind,
        other => panic!("expected a successful command, got {other:?}"),
    }
}

async fn protected_wallet(client: &RoutingClient) -> v1::Account {
    client
        .get_account(
            v1::GetAccountRequest {
                ledger_id: BOOK.0.to_string(),
                account_id: "2".into(),
            },
            deadline(),
        )
        .await
        .unwrap()
        .account
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_capture_requires_disposition_and_void_releases_only_remaining_funds() {
    use v1::Side::{Credit, Debit};
    use v1::capture_reservation_request::{Capture, Disposition};
    use v1::outcome::Kind;

    let external = TempDir::new().unwrap();
    let journal = external.path().join("journal");
    Journal::init(&journal, &super::recovery::spec(), CLUSTER).unwrap();
    let mut cluster = super::recovery::original_cluster(&journal).await;
    let api = PublicApi::start(&cluster, cluster.leader().await).await;
    let client = &api.routed;
    for id in [1, 2] {
        assert!(matches!(
            accepted(&client.create_account(wallet(id), deadline()).await.unwrap()),
            Kind::AccountCreated(_)
        ));
    }
    let deposit = post_request(
        3,
        v1::funding::Kind::Explicit(v1::ExplicitFunding {
            lines: vec![line(1, Debit, "100"), line(2, Credit, "100")],
        }),
    );
    assert_eq!(
        posted(&client.post_entry(deposit, deadline()).await.unwrap()).entry_id,
        1
    );
    let bounds = v1::Bounds {
        min: Some("30".into()),
        max: None,
    };
    let set = v1::SetAccountBoundsRequest {
        ledger_id: BOOK.0.to_string(),
        request_id: "4".into(),
        account_id: "2".into(),
        bounds: Some(bounds.clone()),
    };
    assert_eq!(
        accepted(&client.set_account_bounds(set, deadline()).await.unwrap()),
        &Kind::BoundsChanged("2".into())
    );

    let leader = cluster.leader().await;
    let follower = leader % 3 + 1;
    for node in [leader, follower] {
        let mut health = v1::monitor_client::MonitorClient::new(api.ordinary[&node].clone());
        let status = health
            .get_status(v1::GetStatusRequest {
                ledger_id: BOOK.0.to_string(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(status.node_id, node);
        assert_eq!(status.generation, CLUSTER.to_string());
        assert_eq!(status.is_leader, node == leader);
        assert_eq!(status.mode(), v1::ServingMode::ProtectedVoter);
        assert!(status.verified_journal_index.is_some());
        if node == leader {
            assert_eq!(status.peers.len(), 2);
            assert!(
                status
                    .peers
                    .iter()
                    .all(|p| !p.learner && p.node_id != leader)
            );
        }
        assert!(status.exports.is_none());
    }
    let mut payments = WireClient::new(api.ordinary[&leader].clone());
    let denied = payments
        .set_account_bounds(v1::SetAccountBoundsRequest {
            ledger_id: BOOK.0.to_string(),
            request_id: "10000".into(),
            account_id: "2".into(),
            bounds: Some(v1::Bounds::default()),
        })
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);
    assert_eq!(
        payments
            .create_account(wallet(10000))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let correction = v1::PostEntryRequest {
        corrects: Some(1),
        ..Default::default()
    };
    assert_eq!(
        payments.post_entry(correction).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    assert!(
        client
            .get_request_outcome(
                v1::GetRequestOutcomeRequest {
                    ledger_id: BOOK.0.to_string(),
                    request_id: "10000".into()
                },
                deadline()
            )
            .await
            .unwrap()
            .decision
            .is_none()
    );
    let lookup = v1::GetReservationRequest {
        ledger_id: BOOK.0.to_string(),
        reservation_id: "45".into(),
    };
    assert!(
        client
            .get_reservation(lookup.clone(), deadline())
            .await
            .unwrap()
            .reservation
            .is_none()
    );

    let reserve = v1::ReserveEntryRequest {
        ledger_id: BOOK.0.to_string(),
        request_id: "5".into(),
        reservation_id: "45".into(),
        funding: Some(v1::Funding {
            kind: Some(v1::funding::Kind::Explicit(v1::ExplicitFunding {
                lines: vec![line(2, Debit, "70"), line(1, Credit, "70")],
            })),
        }),
        conditions: vec![],
    };
    let reserved = payments
        .reserve_entry(reserve.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        accepted(&reserved),
        &Kind::Reserved(v1::Reserved {
            reservation_id: "45".into(),
            lines: vec![line(2, Debit, "70"), line(1, Credit, "70")],
        })
    );
    let capture = v1::CaptureReservationRequest {
        ledger_id: BOOK.0.to_string(),
        request_id: "6".into(),
        reservation_id: "45".into(),
        capture: Some(Capture::Amount("30".into())),
        disposition: None,
    };
    let rejected = client
        .capture_reservation(capture.clone(), deadline())
        .await
        .unwrap();
    assert!(
        matches!(&rejected.result, Some(v1::decision::Result::Rejected(r)) if r.code == v1::ErrorCode::MissingDisposition as i32)
    );
    let held = protected_wallet(client).await;
    assert_eq!(held.bounds, Some(bounds));
    assert_eq!(held.posted_balance, "100");
    assert_eq!(held.available_balance, "30");
    assert_eq!(held.counters.unwrap().debits_pending, "70");

    let keep = v1::CaptureReservationRequest {
        request_id: "7".into(),
        disposition: Some(Disposition::KeepReserved(v1::Empty {})),
        ..capture.clone()
    };
    let captured = client
        .capture_reservation(keep.clone(), deadline())
        .await
        .unwrap();
    assert_eq!(
        accepted(&captured),
        &Kind::Captured(v1::Captured {
            reservation_id: "45".into(),
            entry: Some(v1::JournalEntry {
                entry_id: 2,
                lines: vec![line(2, Debit, "30"), line(1, Credit, "30")],
                corrects: None,
            }),
        })
    );
    let state = payments
        .get_reservation(lookup.clone())
        .await
        .unwrap()
        .into_inner()
        .reservation
        .unwrap();
    assert_eq!(state.status(), v1::ReservationStatus::Open);
    assert_eq!(
        state.lines,
        vec![line(2, Debit, "70"), line(1, Credit, "70")]
    );
    assert_eq!(state.remaining, ["40", "40"]);
    let partially_captured = protected_wallet(client).await;
    assert_eq!(partially_captured.posted_balance, "70");
    assert_eq!(partially_captured.available_balance, "30");
    assert_eq!(partially_captured.counters.unwrap().debits_pending, "40");

    let void = v1::VoidReservationRequest {
        ledger_id: BOOK.0.to_string(),
        request_id: "8".into(),
        reservation_id: "45".into(),
    };
    let voided = client
        .void_reservation(void.clone(), deadline())
        .await
        .unwrap();
    assert_eq!(accepted(&voided), &Kind::Voided("45".into()));
    assert_eq!(
        client.reserve_entry(reserve, deadline()).await.unwrap(),
        reserved
    );
    assert_eq!(
        client
            .capture_reservation(capture, deadline())
            .await
            .unwrap(),
        rejected
    );
    assert_eq!(
        client.capture_reservation(keep, deadline()).await.unwrap(),
        captured
    );
    assert_eq!(
        client.void_reservation(void, deadline()).await.unwrap(),
        voided
    );
    let state = payments
        .get_reservation(lookup)
        .await
        .unwrap()
        .into_inner()
        .reservation
        .unwrap();
    assert_eq!(state.status(), v1::ReservationStatus::Voided);
    assert_eq!(state.remaining, ["0", "0"]);
    let cleared = protected_wallet(client).await;
    assert_eq!(cleared.posted_balance, "70");
    assert_eq!(cleared.available_balance, "70");
    assert_eq!(
        cleared.counters.unwrap(),
        v1::Counters {
            debits_posted: "30".into(),
            credits_posted: "100".into(),
            debits_pending: "0".into(),
            credits_pending: "0".into(),
        }
    );
    for (id, server) in api.servers {
        server.shutdown().await.unwrap();
        cluster.stop(id).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_client_recovers_an_unknown_payment_through_new_leader_and_protected_reads() {
    use v1::Side::{Credit, Debit};
    let external = TempDir::new().unwrap();
    let remote = external.path().join("journal");
    let disconnected = external.path().join("disconnected");
    Journal::init(&remote, &super::recovery::spec(), CLUSTER).unwrap();
    let mut cluster = super::recovery::original_cluster(&remote).await;
    let old = cluster.leader().await;
    let follower = old % 3 + 1;
    let mut api = PublicApi::start(&cluster, follower).await;
    let mut primary_wire = api.wire(old);
    timeout(WAIT, async {
        while cluster.handle(follower).status().await.unwrap().leader_id != Some(old) {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("follower did not learn its leader");
    let redirect = api
        .wire(follower)
        .create_account(wallet(1))
        .await
        .unwrap_err();
    assert_eq!(redirect.code(), Code::Unavailable);
    assert_eq!(
        redirect
            .metadata()
            .get("ledger-leader-id")
            .unwrap()
            .to_str()
            .unwrap(),
        old.to_string()
    );
    let client = &api.routed;
    for id in 1..=4 {
        let receipt = client.create_account(wallet(id), deadline()).await.unwrap();
        assert!(matches!(
            receipt.result,
            Some(v1::decision::Result::Accepted(_))
        ));
    }

    let deposit = |quantity| {
        post_request(
            5,
            v1::funding::Kind::Explicit(v1::ExplicitFunding {
                lines: vec![
                    line(1, Debit, "12000"),
                    line(2, Credit, quantity),
                    line(3, Credit, "9000"),
                ],
            }),
        )
    };
    let before = cluster
        .handle(old)
        .status()
        .await
        .unwrap()
        .proposed_commands;
    let mut oversized = wallet(42);
    oversized.account_id = "1".repeat(1 << 20);
    assert_eq!(
        primary_wire
            .create_account(oversized)
            .await
            .unwrap_err()
            .code(),
        Code::OutOfRange
    );
    assert_eq!(
        primary_wire
            .post_entry(deposit("03000"))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        cluster
            .handle(old)
            .status()
            .await
            .unwrap()
            .proposed_commands,
        before
    );
    assert_eq!(
        posted(
            &client
                .post_entry(deposit("3000"), deadline())
                .await
                .unwrap()
        )
        .entry_id,
        1
    );

    let payment = |request_id, ordered: [u64; 2]| {
        post_request(
            request_id,
            v1::funding::Kind::Waterfall(v1::Waterfall {
                amount: "8000".into(),
                sources: ordered
                    .map(|id| v1::Source {
                        account_id: id.to_string(),
                        floor: Some(v1::source::Floor::Configured(v1::Empty {})),
                        cap: None,
                    })
                    .to_vec(),
                destinations: vec![line(4, Credit, "8000")],
                shortfall: v1::Shortfall::RequireFull.into(),
            }),
        )
    };
    let first = payment(6, [2, 3]);
    let original = client.post_entry(first.clone(), deadline()).await.unwrap();
    assert_eq!(posted(&original).entry_id, 2);
    assert_eq!(
        posted(&original).lines,
        [
            line(2, Debit, "3000"),
            line(3, Debit, "5000"),
            line(4, Credit, "8000")
        ]
    );
    let retry_rejection = payment(7, [2, 3]);
    let rejection = client
        .post_entry(retry_rejection.clone(), deadline())
        .await
        .unwrap();
    assert!(
        matches!(&rejection.result, Some(v1::decision::Result::Rejected(r)) if r.code == v1::ErrorCode::InsufficientFunding as i32)
    );

    let account = v1::GetAccountRequest {
        ledger_id: BOOK.0.to_string(),
        account_id: "3".into(),
    };
    let entry = v1::GetEntryRequest {
        ledger_id: BOOK.0.to_string(),
        entry_id: 3,
    };
    let lookup = v1::GetRequestOutcomeRequest {
        ledger_id: BOOK.0.to_string(),
        request_id: "8".into(),
    };
    let unknown = post_request(
        8,
        v1::funding::Kind::Explicit(v1::ExplicitFunding {
            lines: vec![line(1, Debit, "8000"), line(3, Credit, "8000")],
        }),
    );
    let prior = cluster.handle(old).status().await.unwrap().applied_index;
    std::fs::rename(&remote, &disconnected).unwrap();
    withheld(primary_wire.post_entry(short(unknown.clone())).await);
    let local = cluster.handle(old).status().await.unwrap();
    assert!(local.applied_index > prior && local.applied_index > local.protected_index.unwrap());
    withheld(primary_wire.post_entry(short(first.clone())).await);
    withheld(primary_wire.get_account(short(account.clone())).await);
    withheld(primary_wire.get_entry(short(entry.clone())).await);
    withheld(
        primary_wire
            .get_reservation(short(v1::GetReservationRequest {
                ledger_id: BOOK.0.to_string(),
                reservation_id: "42".into(),
            }))
            .await,
    );
    withheld(
        primary_wire
            .get_request_outcome(short(lookup.clone()))
            .await,
    );

    // The routing client has the previous leader cached. Its application never received
    // a receipt for request 8, though the original quorum had already applied it.
    api.servers.remove(&old).unwrap().shutdown().await.unwrap();
    cluster.stop(old).await;
    let replacement = cluster.leader().await;
    assert_ne!(replacement, old);
    std::fs::rename(disconnected, &remote).unwrap();
    timeout(WAIT, async {
        loop {
            let status = cluster.handle(replacement).status().await.unwrap();
            if status
                .protected_index
                .is_some_and(|index| index >= local.applied_index)
            {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("replacement leader did not verify the protected prefix after reconnection");
    let recovered = client
        .post_entry(unknown.clone(), deadline())
        .await
        .unwrap();
    assert_eq!(posted(&recovered).entry_id, 3);
    assert_eq!(
        client
            .get_entry(entry, deadline())
            .await
            .unwrap()
            .entry
            .as_ref(),
        Some(posted(&recovered))
    );
    assert_eq!(
        client
            .get_request_outcome(lookup, deadline())
            .await
            .unwrap()
            .decision,
        Some(recovered)
    );
    assert_eq!(
        client
            .get_account(account, deadline())
            .await
            .unwrap()
            .account
            .unwrap()
            .available_balance,
        "12000"
    );
    assert_eq!(
        client.post_entry(first, deadline()).await.unwrap(),
        original
    );
    assert_eq!(
        client
            .post_entry(retry_rejection, deadline())
            .await
            .unwrap(),
        rejection
    );
    let reordered = client
        .post_entry(payment(6, [3, 2]), deadline())
        .await
        .unwrap();
    assert!(
        matches!(reordered.result, Some(v1::decision::Result::Rejected(r)) if r.code == v1::ErrorCode::IdConflict as i32)
    );
    for (id, server) in api.servers {
        server.shutdown().await.unwrap();
        cluster.stop(id).await;
    }
}

#[path = "../../../ledger-api/examples/first_payment/workflow.rs"]
mod first_payment;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_payment_example_runs_twice_with_the_same_receipt() {
    let external = TempDir::new().unwrap();
    let journal = external.path().join("journal");
    Journal::init(&journal, &super::recovery::spec(), CLUSTER).unwrap();
    let mut cluster = super::recovery::original_cluster(&journal).await;
    let api = PublicApi::start(&cluster, cluster.leader().await).await;
    for _ in 0..2 {
        let receipt = first_payment::run(&api.routed, &BOOK.0.to_string())
            .await
            .unwrap();
        assert_eq!(receipt.entry_id, 2);
        assert_eq!(
            receipt.lines,
            vec![
                line(2, v1::Side::Debit, "25"),
                line(3, v1::Side::Credit, "25")
            ]
        );
    }
    for (id, server) in api.servers {
        server.shutdown().await.unwrap();
        cluster.stop(id).await;
    }
}

#[path = "api/load.rs"]
mod load;

#[cfg(unix)]
#[path = "api/capacity.rs"]
mod capacity;

#[path = "api/history.rs"]
mod history;
