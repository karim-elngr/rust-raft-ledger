//! Run with LEDGER_OLD_NODE_BIN set to a frozen pre-upgrade ledger-node binary.
use super::*;
use ledger_api::{
    client::{ClientSeed, LedgerClient},
    v1,
};
use ledger_recovery::Journal;
use tonic::{
    Code,
    transport::{Certificate as Root, Channel, ClientTlsConfig, Endpoint, Identity as Credentials},
};

fn process(binary: &Path, action: &str, config: &Path) -> Process {
    Process(
        ProcessCommand::new(binary)
            .arg(action)
            .arg(config)
            .spawn()
            .unwrap(),
    )
}

fn payment(id: u64, debit: u64, credit: u64, amount: &str) -> v1::PostEntryRequest {
    api::post_request(
        id,
        v1::funding::Kind::Explicit(v1::ExplicitFunding {
            lines: vec![
                api::line(debit, v1::Side::Debit, amount),
                api::line(credit, v1::Side::Credit, amount),
            ],
        }),
    )
}

async fn post(client: &LedgerClient, request: v1::PostEntryRequest, entry: u64) -> v1::Decision {
    let expected = match request.funding.as_ref().unwrap().kind.as_ref().unwrap() {
        v1::funding::Kind::Explicit(value) => value.lines.clone(),
        _ => unreachable!(),
    };
    // During a handoff a former leader can disappear before another voter
    // admits the call. Exercise the backend's same-ID retry contract.
    let deadline = Instant::now() + WAIT;
    let decision = loop {
        match client.post_entry(request.clone(), deadline).await {
            Ok(decision) => break decision,
            Err(status)
                if Instant::now() < deadline
                    && matches!(status.code(), Code::Unavailable | Code::ResourceExhausted) =>
            {
                println!(
                    "rolling retry request={} status={:?}",
                    request.request_id,
                    status.code()
                );
                sleep(Duration::from_millis(25)).await;
            }
            Err(status) => panic!("payment {} did not resolve: {status}", request.request_id),
        }
    };
    assert!(
        matches!(&decision.result, Some(v1::decision::Result::Accepted(v1::Outcome {
        kind: Some(v1::outcome::Kind::Posted(posted)),
    })) if posted.entry_id == entry && posted.lines == expected)
    );
    decision
}

fn lookup(id: u64) -> v1::GetRequestOutcomeRequest {
    v1::GetRequestOutcomeRequest {
        ledger_id: BOOK.0.to_string(),
        request_id: id.to_string(),
    }
}

// The baseline predates Monitor/GetStatus. Before taking another member
// offline, require the restarted process to serve a protected lookup or name
// its elected leader on the mTLS client listener.
async fn baseline_joined(channel: &Channel, node: u64) {
    let mut local = v1::ledger_client::LedgerClient::new(channel.clone());
    timeout(WAIT, async {
        loop {
            let joined = match local.get_request_outcome(lookup(11)).await {
                Ok(reply) => reply.into_inner().decision.is_some(),
                Err(e) => {
                    e.code() == Code::Unavailable && e.metadata().get("ledger-leader-id").is_some()
                }
            };
            if joined {
                println!("rolling baseline_listener_joined={node}");
                return;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("baseline voter did not join the serving group");
}

async fn candidate_caught_up(channel: &Channel, node: u64, through: u64) {
    let mut monitor = v1::monitor_client::MonitorClient::new(channel.clone());
    timeout(WAIT, async {
        loop {
            if let Ok(reply) = monitor
                .get_status(v1::GetStatusRequest {
                    ledger_id: BOOK.0.to_string(),
                })
                .await
            {
                let state = reply.into_inner();
                assert_eq!(state.node_id, node);
                assert_eq!(state.mode(), v1::ServingMode::ProtectedVoter);
                if state.applied_index >= through
                    && state.checkpoint_index.is_some_and(|i| i >= through)
                {
                    println!(
                        "rolling candidate_node={node} applied={} sealed={:?} through={through}",
                        state.applied_index, state.checkpoint_index
                    );
                    return;
                }
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("candidate failed to apply and seal required history");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a frozen old voter binary and local TCP"]
async fn protected_rolling_upgrade_and_rollback() {
    let old = fs::canonicalize(
        std::env::var_os("LEDGER_OLD_NODE_BIN").expect("supply a frozen baseline voter"),
    )
    .unwrap();
    let candidate = fs::canonicalize(
        std::env::var_os("LEDGER_CANDIDATE_NODE_BIN")
            .unwrap_or_else(|| env!("CARGO_BIN_EXE_ledger-node").into()),
    )
    .unwrap();
    assert_ne!(old, candidate);
    println!(
        "rolling baseline={} candidate={}",
        old.display(),
        candidate.display()
    );
    let mut peers = Cluster::new().await;
    let mut public = Cluster::new().await;
    let external = TempDir::new().unwrap();
    let path = external.path().join("journal");
    let configs = write_configs(&peers, Some((&public, &path)));
    let identity = leaf("backend.test", &public.ca, &public.ca_key);
    let roots = public.ca.pem();
    let mut direct = BTreeMap::new();
    let mut seeds = Vec::new();
    for (&node, member) in &public.members {
        let endpoint = format!("https://{}", member.addr);
        let tls = ClientTlsConfig::new()
            .domain_name(member.domain.clone())
            .ca_certificate(Root::from_pem(&roots))
            .identity(Credentials::from_pem(&identity.cert_pem, &identity.key_pem));
        direct.insert(
            node,
            Endpoint::from_shared(endpoint.clone())
                .unwrap()
                .tls_config(tls)
                .unwrap()
                .connect_lazy(),
        );
        seeds.push(ClientSeed {
            node,
            endpoint,
            domain: member.domain.clone(),
        });
    }
    let client = LedgerClient::connect(
        seeds,
        roots.into_bytes(),
        identity.cert_pem,
        identity.key_pem,
    )
    .unwrap();
    process(&old, "journal-init", &configs[&1])
        .wait_success()
        .await;
    let journal = Journal::open(&path, &super::super::recovery::spec()).unwrap();
    let mut running = BTreeMap::new();
    for (&id, config) in &configs {
        process(&old, "init", config).wait_success().await;
        drop(peers.members.get_mut(&id).unwrap().listener.take());
        drop(public.members.get_mut(&id).unwrap().listener.take());
        running.insert(id, process(&old, "run", config));
    }

    for (id, normal) in [(1, v1::Side::Debit), (2, v1::Side::Credit)] {
        let result = client
            .create_account(
                v1::CreateAccountRequest {
                    ledger_id: BOOK.0.to_string(),
                    request_id: id.to_string(),
                    account_id: id.to_string(),
                    normal: normal.into(),
                    bounds: Some(v1::Bounds {
                        min: Some("0".into()),
                        max: None,
                    }),
                    ..Default::default()
                },
                Instant::now() + WAIT,
            )
            .await
            .unwrap();
        assert!(matches!(
            result.result,
            Some(v1::decision::Result::Accepted(_))
        ));
    }
    let original = post(&client, payment(11, 1, 2, "40"), 1).await;
    let refused = client
        .post_entry(payment(12, 2, 1, "60"), Instant::now() + WAIT)
        .await
        .unwrap();
    assert!(
        matches!(&refused.result, Some(v1::decision::Result::Rejected(r)) if r.code == v1::ErrorCode::BelowMinimum as i32)
    );
    let (upgraded, old_leader) = timeout(WAIT, async {
        loop {
            for (&id, channel) in &direct {
                let result = v1::ledger_client::LedgerClient::new(channel.clone())
                    .get_request_outcome(lookup(11))
                    .await;
                if let Err(e) = result
                    && e.code() == Code::Unavailable
                    && let Some(leader) = e.metadata().get("ledger-leader-id")
                {
                    let leader = leader.to_str().unwrap().parse::<u64>().unwrap();
                    assert_ne!(id, leader);
                    return (id, leader);
                }
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("could not identify a baseline follower");
    println!(
        "rolling baseline_leader={old_leader} selected_follower={upgraded} original_request=11 entry=1 rejected_request=12"
    );
    running.remove(&upgraded).unwrap().stop().await;
    let old_pair = post(&client, payment(13, 1, 2, "3"), 2).await;
    running.insert(upgraded, process(&candidate, "run", &configs[&upgraded]));
    candidate_caught_up(&direct[&upgraded], upgraded, journal.head().unwrap().index).await;

    // Exactly two processes remain. Every new protected reply requires both binaries.
    running.remove(&old_leader).unwrap().stop().await;
    let mixed = post(&client, payment(14, 2, 1, "30"), 3).await;
    assert_eq!(
        client
            .post_entry(payment(14, 2, 1, "30"), Instant::now() + WAIT)
            .await
            .unwrap(),
        mixed
    );
    candidate_caught_up(&direct[&upgraded], upgraded, journal.head().unwrap().index).await;
    println!(
        "rolling mixed_majority={:?} request=14 entry=3 retry=identical",
        running.keys().collect::<Vec<_>>()
    );

    // Re-establish the old pair before restarting the candidate on its original disk.
    running.insert(old_leader, process(&old, "run", &configs[&old_leader]));
    baseline_joined(&direct[&old_leader], old_leader).await;
    running.remove(&upgraded).unwrap().stop().await;
    let rollback_gap = post(&client, payment(15, 1, 2, "1"), 4).await;
    process(&old, "checkpoint-scrub", &configs[&upgraded])
        .wait_success()
        .await;
    running.insert(upgraded, process(&old, "run", &configs[&upgraded]));
    baseline_joined(&direct[&upgraded], upgraded).await;
    running.remove(&old_leader).unwrap().stop().await;
    let after = post(&client, payment(16, 2, 1, "4"), 5).await;
    for (id, expected) in [
        (11, original),
        (12, refused),
        (13, old_pair),
        (14, mixed),
        (15, rollback_gap),
        (16, after),
    ] {
        assert_eq!(
            client
                .get_request_outcome(lookup(id), Instant::now() + WAIT)
                .await
                .unwrap()
                .decision,
            Some(expected)
        );
    }
    let balance = client
        .get_account(
            v1::GetAccountRequest {
                ledger_id: BOOK.0.to_string(),
                account_id: "2".into(),
            },
            Instant::now() + WAIT,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(balance.available_balance, "10");
    println!(
        "rolling rollback_majority={:?} reopened_node={upgraded} original_decisions_verified=6 final_entry=5 wallet_balance=10",
        running.keys().collect::<Vec<_>>()
    );
    for (_, voter) in running {
        voter.stop().await;
    }
}
