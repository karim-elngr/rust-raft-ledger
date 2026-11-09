use super::*;
use ledger_api::{
    client::{ClientSeed, LedgerClient},
    v1,
};
use ledger_export_adapters::v1::DecisionEvent;
use ledger_recovery::{Journal, Spec};
use prost::Message as _;
use rdkafka::{
    ClientConfig, Message, Offset, TopicPartitionList,
    consumer::{BaseConsumer, Consumer},
    mocking::MockCluster,
};

fn receive(consumer: &BaseConsumer) -> DecisionEvent {
    tokio::task::block_in_place(|| {
        let deadline = std::time::Instant::now() + WAIT;
        while std::time::Instant::now() < deadline {
            if let Some(Ok(message)) = consumer.poll(Duration::from_millis(100)) {
                let event = DecisionEvent::decode(message.payload().unwrap()).unwrap();
                assert_eq!(message.key().unwrap(), event.event_id.as_bytes());
                return event;
            }
        }
        panic!("no learner event arrived before deadline")
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn learner_cli_archives_and_publishes_after_quorum_ack_and_restart() {
    learner_disaster(PredecessorStop::Graceful).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "process-crash drill terminates the fixture's local voter and learner children"]
async fn learner_process_crash_restores_only_the_archived_receipts() {
    learner_disaster(PredecessorStop::Crash).await;
}

enum PredecessorStop {
    Graceful,
    Crash,
}

async fn stop_old_members(mode: PredecessorStop, mut active: [(u64, Process); 3]) {
    use std::os::unix::process::ExitStatusExt;
    match mode {
        PredecessorStop::Graceful => {
            for (_, process) in active {
                process.stop().await;
            }
        }
        PredecessorStop::Crash => {
            // The third voter is already offline. Signal the entire live majority
            // and learner before waiting for any exit or touching their disks.
            for (id, process) in &mut active {
                assert!(
                    process.0.try_wait().unwrap().is_none(),
                    "member {id} exited early"
                );
            }
            for (_, process) in &mut active {
                process.0.kill().unwrap();
            }
            for (id, mut process) in active {
                let exit = process.wait_exit().await;
                assert_eq!(exit.signal(), Some(9), "member {id}: {exit}");
                eprintln!("predecessor_crash member={id} signal=SIGKILL");
            }
        }
    }
}

async fn learner_disaster(mode: PredecessorStop) {
    use std::io::Write;
    let mut peers = Cluster::with_ids([1, 2, 3, 4]).await;
    let mut public = Cluster::new().await;
    let external = TempDir::new().unwrap();
    let receipt_dir = TempDir::new().unwrap();
    let receipts = receipt_dir.path().join(BOOK.0.to_string());
    fs::create_dir(&receipts).unwrap();
    fs::File::open(receipt_dir.path())
        .unwrap()
        .sync_all()
        .unwrap();
    let mut inventory = fs::File::create_new(receipts.join("request-ids.txt")).unwrap();
    let mut receipt = |id: u128, decision: &v1::Decision| {
        let mut file = fs::File::create_new(receipts.join(id.to_string())).unwrap();
        file.write_all(&decision.encode_to_vec()).unwrap();
        file.sync_all().unwrap();
        fs::File::open(&receipts).unwrap().sync_all().unwrap();
        writeln!(inventory, "{id}").unwrap();
        inventory.sync_all().unwrap();
    };
    let path = external.path().join("archive");
    let configs = write_topology(&peers, Some((&public, &path)), Some(4));
    let topic = format!(
        "learner-cli-{}",
        peers
            .directory
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .trim_start_matches('.')
    );
    let mock;
    let brokers = match std::env::var("LEDGER_TEST_KAFKA_BROKERS") {
        Ok(server) => {
            use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
            let admin: AdminClient<_> = ClientConfig::new()
                .set("bootstrap.servers", &server)
                .create()
                .unwrap();
            let topics = [NewTopic::new(&topic, 1, TopicReplication::Fixed(1))];
            let result = admin
                .create_topics(&topics, &AdminOptions::new())
                .await
                .unwrap();
            assert!(result[0].is_ok(), "{result:?}");
            server
        }
        Err(_) => {
            mock = MockCluster::new(1).unwrap();
            mock.create_topic(&topic, 1, 1).unwrap();
            mock.bootstrap_servers()
        }
    };
    let learner_config = &configs[&4];
    let monitor_socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let monitor_addr = monitor_socket.local_addr().unwrap();
    let source = format!(
        "{}\n[monitor]\nlisten = \"{monitor_addr}\"\nca_pem = \"client-ca.pem\"\ncert_pem = \"api-1.pem\"\nkey_pem = \"api-1.key\"\n",
        fs::read_to_string(learner_config).unwrap()
    );
    let monitor_identity = leaf("operator.test", &public.ca, &public.ca_key);
    let monitor_channel =
        tonic::transport::Endpoint::from_shared(format!("https://{monitor_addr}"))
            .unwrap()
            .tls_config(
                tonic::transport::ClientTlsConfig::new()
                    .domain_name(public.members[&1].domain.clone())
                    .ca_certificate(tonic::transport::Certificate::from_pem(public.ca.pem()))
                    .identity(tonic::transport::Identity::from_pem(
                        monitor_identity.cert_pem,
                        monitor_identity.key_pem,
                    )),
            )
            .unwrap()
            .connect_lazy();
    let mut monitor = v1::monitor_client::MonitorClient::new(monitor_channel.clone());
    let status_request = v1::GetStatusRequest {
        ledger_id: BOOK.0.to_string(),
    };
    drop(monitor_socket);
    let publisher = format!(
        r#"[export.publisher]
kind = "kafka"
brokers = "{brokers}"
topic = "{topic}"
partition = 0
"#
    );
    let sinks = format!(
        r#"{source}
[export.archive]
kind = "posix"
path = "{}"
{publisher}
[checkpoint]
interval_entries = 1
"#,
        path.display()
    );
    fs::write(learner_config, &sinks).unwrap();
    Process::spawn("journal-init", learner_config)
        .wait_success()
        .await;
    for file in configs.values() {
        Process::spawn("init", file).wait_success().await;
    }
    let spec = Spec {
        ledger: BOOK.0,
        asset: "USD".into(),
        scale: 2,
    };
    let journal = Journal::open(&path, &spec).unwrap();
    let cert = leaf("backend.test", &public.ca, &public.ca_key);
    let client = LedgerClient::connect(
        public
            .members
            .iter()
            .map(|(&node, m)| ClientSeed {
                node,
                endpoint: format!("https://{}", m.addr),
                domain: m.domain.clone(),
            })
            .collect(),
        public.ca.pem().into_bytes(),
        cert.cert_pem,
        cert.key_pem,
    )
    .unwrap();
    for m in peers
        .members
        .values_mut()
        .chain(public.members.values_mut())
    {
        drop(m.listener.take());
    }
    fs::write(
        learner_config,
        format!(
            "{source}\n[export.archive]\nkind = \"noop\"\n[export.publisher]\nkind = \"noop\"\n"
        ),
    )
    .unwrap();
    let disabled = Process::spawn("run", learner_config);
    timeout(WAIT, async {
        while tokio::net::TcpStream::connect(peers.members[&4].addr)
            .await
            .is_err()
        {
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("disabled learner did not start");
    disabled.stop().await;
    assert!(!peers.directory.path().join("n4/export").exists());
    fs::write(learner_config, sinks).unwrap();

    // Exercise the minimum voting majority. The third voter and the learner are both offline.
    let first = Process::spawn("run", &configs[&1]);
    let second = Process::spawn("run", &configs[&2]);
    let account = v1::CreateAccountRequest {
        ledger_id: BOOK.0.to_string(),
        request_id: "100".into(),
        account_id: "52".into(),
        code: 27,
        normal: v1::Side::Credit.into(),
        bounds: None,
    };
    let create = |request| client.create_account(request, Instant::now() + WAIT);
    let accepted = create(account.clone()).await.unwrap();
    receipt(100, &accepted);
    assert!(matches!(
        accepted.result,
        Some(v1::decision::Result::Accepted(_))
    ));
    assert_eq!(create(account.clone()).await.unwrap(), accepted);
    assert!(
        client
            .get_account(
                v1::GetAccountRequest {
                    ledger_id: BOOK.0.to_string(),
                    account_id: "52".into()
                },
                Instant::now() + WAIT
            )
            .await
            .unwrap()
            .account
            .is_some()
    );
    let destination = create(v1::CreateAccountRequest {
        request_id: "102".into(),
        account_id: "53".into(),
        ..account.clone()
    })
    .await
    .unwrap();
    receipt(102, &destination);
    let transfer = api::post_request(
        103,
        v1::funding::Kind::Explicit(v1::ExplicitFunding {
            lines: vec![
                api::line(52, v1::Side::Debit, "70"),
                api::line(53, v1::Side::Credit, "70"),
            ],
        }),
    );
    let posted = client
        .post_entry(transfer.clone(), Instant::now() + WAIT)
        .await
        .unwrap();
    receipt(103, &posted);
    assert!(matches!(
        &posted.result,
        Some(v1::decision::Result::Accepted(v1::Outcome {
            kind: Some(v1::outcome::Kind::Posted(_))
        }))
    ));
    assert_eq!(journal.head().unwrap().index, 0);

    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(&topic, 0, Offset::Beginning)
        .unwrap();
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &topic)
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    consumer.assign(&assignment).unwrap();
    let detached = external.path().join("detached");
    fs::rename(&path, &detached).unwrap();
    let learner = Process::spawn("run", learner_config);
    // Local checkpoint creation proves the learner is applying Raft with the archive offline.
    await_seal(&peers.directory.path().join("n4")).await;
    assert_eq!(
        Journal::open(&detached, &spec)
            .unwrap()
            .head()
            .unwrap()
            .index,
        0
    );
    timeout(WAIT, async {
        loop {
            let state = monitor
                .get_status(status_request.clone())
                .await
                .unwrap()
                .into_inner();
            assert_eq!(state.mode(), v1::ServingMode::Learner);
            assert!(!state.is_leader);
            let progress = state.exports.unwrap();
            if progress.archive_error.is_some() {
                assert!(progress.verified_archive_index.is_none());
                assert!(progress.archive_lag_entries.is_none());
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("monitor did not report the detached archive");
    let forbidden = v1::ledger_client::LedgerClient::new(monitor_channel)
        .create_account(account.clone())
        .await
        .unwrap_err();
    assert_eq!(forbidden.code(), tonic::Code::Unimplemented);
    fs::rename(&detached, &path).unwrap();
    let created = receive(&consumer);
    assert_eq!(created.request_id, "100");
    assert_eq!(created.decision, Some(accepted));
    assert!(journal.head().unwrap().index >= created.raft_index);
    let second_account = receive(&consumer);
    assert_eq!(second_account.request_id, "102");
    assert_eq!(second_account.decision, Some(destination));
    let posting = receive(&consumer);
    assert_eq!(posting.request_id, "103");
    assert_eq!(posting.decision, Some(posted));
    assert!(journal.head().unwrap().index >= posting.raft_index);
    timeout(WAIT, async {
        loop {
            let state = monitor
                .get_status(status_request.clone())
                .await
                .unwrap()
                .into_inner();
            let progress = state.exports.unwrap();
            if progress
                .verified_archive_index
                .is_some_and(|i| i >= posting.raft_index)
                && progress
                    .event_index
                    .is_some_and(|i| i >= posting.raft_index)
            {
                assert!(progress.archive_error.is_none());
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("monitor did not report archive and broker recovery");
    learner.stop().await;

    let rejected = create(v1::CreateAccountRequest {
        request_id: "101".into(),
        ..account.clone()
    })
    .await
    .unwrap();
    receipt(101, &rejected);
    // Preserve the known four-receipt inventory before the archive is detached.
    let covered_inventory = receipt_dir.path().join("covered-request-ids.txt");
    fs::copy(receipts.join("request-ids.txt"), &covered_inventory).unwrap();
    fs::File::open(covered_inventory)
        .unwrap()
        .sync_all()
        .unwrap();
    fs::File::open(receipt_dir.path())
        .unwrap()
        .sync_all()
        .unwrap();
    assert!(matches!(
        rejected.result,
        Some(v1::decision::Result::Rejected(_))
    ));
    let learner = Process::spawn("run", learner_config);
    let duplicate_account = receive(&consumer);
    assert_eq!(duplicate_account.request_id, "101");
    assert_eq!(duplicate_account.decision, Some(rejected));
    assert!(duplicate_account.raft_index > posting.raft_index);
    assert!(journal.head().unwrap().index >= duplicate_account.raft_index);
    learner.stop().await;
    let archived = journal.head().unwrap();
    // Publish directly from applied Raft after turning off archive export.
    fs::write(learner_config, format!("{source}\n{publisher}")).unwrap();
    let kafka_only = Process::spawn("run", learner_config);
    // These two client receipts survive on an independent path. Neither live
    // voting disk nor the offline learner will be available to disaster recovery.
    let acknowledged_transfer = client
        .post_entry(
            v1::PostEntryRequest {
                request_id: "104".into(),
                ..transfer
            },
            Instant::now() + WAIT,
        )
        .await
        .unwrap();
    assert!(matches!(
        acknowledged_transfer.result,
        Some(v1::decision::Result::Accepted(_))
    ));
    receipt(104, &acknowledged_transfer);
    let acknowledged_rejection = create(v1::CreateAccountRequest {
        request_id: "105".into(),
        ..account
    })
    .await
    .unwrap();
    assert!(matches!(
        acknowledged_rejection.result,
        Some(v1::decision::Result::Rejected(_))
    ));
    receipt(105, &acknowledged_rejection);
    let mut beyond_cut = std::collections::BTreeSet::new();
    for _ in 0..6 {
        let event = receive(&consumer);
        let id: u128 = event.request_id.parse().unwrap();
        let expected = fs::read(receipts.join(id.to_string())).unwrap();
        assert_eq!(event.decision.unwrap().encode_to_vec(), expected);
        if event.raft_index > archived.index {
            beyond_cut.insert(id);
        }
        if beyond_cut.len() == 2 {
            break;
        }
    }
    assert_eq!(beyond_cut, [104, 105].into());
    assert_eq!(journal.head().unwrap(), archived);
    let interrupted = Instant::now();
    stop_old_members(mode, [(4, kafka_only), (1, first), (2, second)]).await;
    let lost = peers.directory.path().to_owned();
    for id in 1..=4 {
        assert!(lost.join(format!("n{id}/raft")).is_dir());
    }
    peers.directory.close().unwrap();
    assert!(!lost.exists());
    let cut = journal.fence(&journal.head().unwrap()).unwrap();
    restart_from_archive(&journal, &cut, &receipts, &beyond_cut, interrupted).await;
}

fn check_receipts(
    restored: &Replica,
    receipts: &Path,
    beyond_cut: &std::collections::BTreeSet<u128>,
) {
    let mut lost = vec![];
    for id in 100..=105 {
        let bytes = fs::read(receipts.join(id.to_string())).unwrap();
        let expected = v1::Decision::decode(bytes.as_slice()).unwrap();
        match restored.decision(RequestId(id)).unwrap() {
            Some(local) => assert_eq!(ledger_api::adapter::decision(local.outcome), expected),
            None => lost.push(id),
        }
    }
    assert_eq!(lost, [104, 105]);
    assert!(lost.iter().eq(beyond_cut));
    let entry = restored.entry(ledger_core::EntryId(1)).unwrap().unwrap();
    assert_eq!(
        entry.lines.as_ref(),
        &[
            Line::debit(AccountId::new(BOOK, 52), 70),
            Line::credit(AccountId::new(BOOK, 53), 70)
        ]
    );
    assert_eq!(
        restored
            .account(AccountId::new(BOOK, 52))
            .unwrap()
            .unwrap()
            .code(),
        27
    );
}

async fn restart_from_archive(
    journal: &Journal,
    cut: &ledger_recovery::Head,
    receipts: &Path,
    beyond_cut: &std::collections::BTreeSet<u128>,
    interrupted: Instant,
) {
    use tonic::transport::{
        Certificate as Trust, ClientTlsConfig, Endpoint, Identity as Credential,
    };
    const GENERATION: u128 = CLUSTER + 1;
    const VOTERS: [u64; 3] = [11, 12, 13];
    const LEARNER: u64 = 14;
    let members = [11, 12, 13, LEARNER];
    let mut peers = Cluster::with_ids(members).await;
    let mut public = Cluster::with_ids(VOTERS).await;
    let archive = journal.directory();
    let configs = write_generation(&peers, Some((&public, archive)), Some(LEARNER), GENERATION);
    let root = peers.directory.path();
    for id in members {
        let path = &configs[&id];
        let original = fs::read_to_string(path).unwrap();
        let admin = root.join(format!("admin-{id}.toml"));
        fs::write(
            &admin,
            format!("recovery_dir = \"{}\"\n{original}", archive.display()),
        )
        .unwrap();
        assert!(!root.join(format!("n{id}")).exists());
        Process::spawn("restore", &admin).wait_success().await;
        let cfg = ReplicaConfig {
            node: id,
            cluster: GENERATION,
            voters: VOTERS.to_vec(),
            ledger: BOOK,
            asset: "USD".into(),
            scale: 2,
        }
        .with_learner(LEARNER);
        let restored = Replica::reopen(&root.join(format!("n{id}")), cfg).unwrap();
        assert_eq!(restored.applied_index(), cut.index);
        assert_eq!(restored.is_learner(), id == LEARNER);
        check_receipts(&restored, receipts, beyond_cut);
        if id == LEARNER {
            fs::write(
                path,
                format!(
                    "{original}\n[export.archive]\nkind = \"posix\"\npath = \"{}\"\n",
                    archive.display()
                ),
            )
            .unwrap();
        }
    }
    Process::spawn("activate", &root.join("admin-11.toml"))
        .wait_success()
        .await;
    let activated = journal.head().unwrap();
    assert_eq!(
        (activated.cluster, activated.index, activated.fenced),
        (GENERATION, cut.index, false)
    );

    let app = leaf("replacement-client.test", &public.ca, &public.ca_key);
    let channels: BTreeMap<_, _> = public
        .members
        .iter()
        .map(|(&id, m)| {
            let ch = Endpoint::from_shared(format!("https://{}", m.addr))
                .unwrap()
                .tls_config(
                    ClientTlsConfig::new()
                        .domain_name(m.domain.clone())
                        .ca_certificate(Trust::from_pem(public.ca.pem()))
                        .identity(Credential::from_pem(&app.cert_pem, &app.key_pem)),
                )
                .unwrap()
                .connect_lazy();
            (id, ch)
        })
        .collect();
    for member in peers
        .members
        .values_mut()
        .chain(public.members.values_mut())
    {
        drop(member.listener.take());
    }
    let processes: Vec<_> = configs
        .values()
        .map(|path| Process::spawn("run", path))
        .collect();
    let status = |id: u64| {
        let mut monitor = v1::monitor_client::MonitorClient::new(channels[&id].clone());
        async move {
            monitor
                .get_status(v1::GetStatusRequest {
                    ledger_id: BOOK.0.to_string(),
                })
                .await
                .map(tonic::Response::into_inner)
        }
    };
    let leader = timeout(WAIT, async {
        loop {
            for id in VOTERS {
                if let Ok(state) = status(id).await
                    && state.is_leader
                {
                    assert_eq!(state.generation, GENERATION.to_string());
                    assert_eq!(state.mode(), v1::ServingMode::QuorumVoter);
                    return state;
                }
            }
            sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("replacement quorum did not elect a leader");
    let mut client = v1::ledger_client::LedgerClient::new(channels[&leader.node_id].clone());
    let payment = api::post_request(
        106,
        v1::funding::Kind::Explicit(v1::ExplicitFunding {
            lines: vec![
                api::line(52, v1::Side::Debit, "5"),
                api::line(53, v1::Side::Credit, "5"),
            ],
        }),
    );
    let fresh = timeout(WAIT, client.post_entry(payment.clone()))
        .await
        .unwrap()
        .unwrap()
        .into_inner();
    let first_payment = interrupted.elapsed();
    assert!(matches!(
        &fresh.result,
        Some(v1::decision::Result::Accepted(v1::Outcome {
            kind: Some(v1::outcome::Kind::Posted(_))
        }))
    ));
    let sampled = status(leader.node_id).await.unwrap();
    assert!(sampled.is_leader && sampled.term == leader.term && sampled.applied_index > cut.index);
    let through = sampled.applied_index;
    timeout(WAIT, async {
        for id in VOTERS {
            loop {
                let state = status(id).await.unwrap();
                assert_eq!(state.node_id, id);
                assert_eq!(state.generation, GENERATION.to_string());
                assert_eq!(state.mode(), v1::ServingMode::QuorumVoter);
                if state.leader_id == Some(leader.node_id)
                    && state.term == leader.term
                    && state.applied_index >= through
                {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        }
    })
    .await
    .expect("three replacement voting disks did not apply the payment");
    let three_voters = interrupted.elapsed();
    timeout(WAIT, async {
        while journal.head().unwrap().index < through {
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("replacement learner did not archive the fresh payment");
    assert_eq!(
        client.post_entry(payment).await.unwrap().into_inner(),
        fresh
    );
    eprintln!(
        "learner_restore cut_index={} independent_receipts=6 matched=4 lost_accepted=1 lost_rejected=1 kafka_only_beyond_cut=2 restored_members=4 first_payment_ms={} three_voters_ms={} applied_through={} learner_archived_through={}",
        cut.index,
        first_payment.as_millis(),
        three_voters.as_millis(),
        through,
        journal.head().unwrap().index
    );
    if let Some(binary) = std::env::var_os("LEDGER_RECONCILE_BIN") {
        check_external_reconciler(Path::new(&binary), receipts, root, &public, &app);
    }
    for member in processes {
        member.stop().await;
    }
}

fn check_external_reconciler(
    binary: &Path,
    receipts: &Path,
    root: &Path,
    public: &Cluster,
    identity: &Identity,
) {
    fs::write(root.join("reconcile.pem"), &identity.cert_pem).unwrap();
    fs::write(root.join("reconcile.key"), &identity.key_pem).unwrap();
    let run = |book: u128, evidence_root: &Path, code, counts: &str| {
        let output = ProcessCommand::new(binary)
            .arg(book.to_string())
            .arg(evidence_root)
            .arg(root.join("client-ca.pem"))
            .arg(root.join("reconcile.pem"))
            .arg(root.join("reconcile.key"))
            .args(
                public
                    .members
                    .iter()
                    .map(|(id, m)| format!("{id},https://{},{}", m.addr, m.domain)),
            )
            .output()
            .unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        eprintln!("reconcile_exit={}\n{stdout}{stderr}", output.status);
        assert_eq!(output.status.code(), Some(code));
        assert!(
            stdout.contains(counts) || stderr.contains(counts),
            "expected {counts}"
        );
    };
    run(
        BOOK.0,
        receipts.parent().unwrap(),
        1,
        "expected=6 matched=4 missing_acceptances=1 missing_rejections=1 mismatches=0 rpc_errors=0 invalid_evidence=0",
    );

    // The independently recorded four-ID cut is a positive control. Change only
    // disposable copies for the subsequent mismatch and incomplete-evidence checks.
    let controls = TempDir::new().unwrap();
    let matched = controls.path().join(BOOK.0.to_string());
    fs::create_dir(&matched).unwrap();
    fs::copy(
        receipts.parent().unwrap().join("covered-request-ids.txt"),
        matched.join("request-ids.txt"),
    )
    .unwrap();
    for id in 100..=103 {
        fs::copy(receipts.join(id.to_string()), matched.join(id.to_string())).unwrap();
    }
    run(
        BOOK.0,
        controls.path(),
        0,
        "expected=4 matched=4 missing_acceptances=0 missing_rejections=0 mismatches=0 rpc_errors=0 invalid_evidence=0",
    );
    // Both postings were accepted. A changed entry ID still changes the decision.
    fs::copy(receipts.join("104"), matched.join("103")).unwrap();
    run(
        BOOK.0,
        controls.path(),
        1,
        "expected=4 matched=3 missing_acceptances=0 missing_rejections=0 mismatches=1 rpc_errors=0 invalid_evidence=0",
    );
    fs::remove_file(matched.join("100")).unwrap();
    fs::write(matched.join("103"), [0xff]).unwrap();
    run(
        BOOK.0,
        controls.path(),
        1,
        "expected=4 matched=2 missing_acceptances=0 missing_rejections=0 mismatches=0 rpc_errors=0 invalid_evidence=2",
    );
    fs::write(matched.join("request-ids.txt"), "").unwrap();
    run(BOOK.0, controls.path(), 2, "empty receipt inventory");
    let other = BOOK.0 + 1;
    let wrong_ledger = controls.path().join(other.to_string());
    fs::create_dir(&wrong_ledger).unwrap();
    fs::write(wrong_ledger.join("request-ids.txt"), "100\n").unwrap();
    fs::copy(receipts.join("100"), wrong_ledger.join("100")).unwrap();
    run(
        other,
        controls.path(),
        1,
        "expected=1 matched=0 missing_acceptances=0 missing_rejections=0 mismatches=0 rpc_errors=1 invalid_evidence=0",
    );
}
