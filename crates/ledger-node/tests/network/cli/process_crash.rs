//! Abrupt loss of all voter processes with surviving Raft disks and POSIX journal.
use super::*;
use ledger_api::{
    client::{ClientSeed, LedgerClient},
    v1,
};
use ledger_recovery::Journal;
use prost::Message;
use std::{io::Write, os::unix::process::ExitStatusExt};
use tonic::{
    Code,
    transport::{Certificate as Root, Channel, ClientTlsConfig, Endpoint, Identity as Credential},
};

fn voter(binary: &Path, action: &str, config: &Path) -> Process {
    Process(
        ProcessCommand::new(binary)
            .args([action])
            .arg(config)
            .spawn()
            .unwrap(),
    )
}

fn save(root: &Path, id: u64, kind: &str, message: &impl Message) {
    let mut file = fs::File::create_new(root.join(format!("{id}.{kind}"))).unwrap();
    file.write_all(&message.encode_to_vec()).unwrap();
    file.sync_all().unwrap();
    fs::File::open(root).unwrap().sync_all().unwrap();
}

fn load<T: Message + Default>(root: &Path, id: u64, kind: &str) -> T {
    T::decode(
        fs::read(root.join(format!("{id}.{kind}")))
            .unwrap()
            .as_slice(),
    )
    .unwrap()
}

fn transfer(id: u64, debit: u64, credit: u64, amount: &str) -> v1::PostEntryRequest {
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

async fn receipt(sdk: &LedgerClient, id: u64) -> v1::Decision {
    sdk.get_request_outcome(
        v1::GetRequestOutcomeRequest {
            ledger_id: BOOK.0.to_string(),
            request_id: id.to_string(),
        },
        Instant::now() + WAIT,
    )
    .await
    .unwrap()
    .decision
    .unwrap()
}

fn check_posting(request: &v1::PostEntryRequest, posted: &v1::JournalEntry, index: u64) {
    let Some(v1::funding::Kind::Explicit(funding)) =
        request.funding.as_ref().and_then(|f| f.kind.as_ref())
    else {
        panic!("expected explicit payment lines");
    };
    assert_eq!(posted.entry_id, index);
    assert_eq!(posted.lines, funding.lines);
    assert!(posted.corrects.is_none());
}

fn check_payment(request: &v1::PostEntryRequest, decision: &v1::Decision, entry: Option<u64>) {
    if let Some(index) = entry {
        let Some(v1::decision::Result::Accepted(v1::Outcome {
            kind: Some(v1::outcome::Kind::Posted(posted)),
        })) = &decision.result
        else {
            panic!("expected a posted entry, received {decision:?}");
        };
        check_posting(request, posted, index);
    } else {
        assert!(
            matches!(&decision.result, Some(v1::decision::Result::Rejected(r))
            if r.code == v1::ErrorCode::BelowMinimum as i32 && r.context == Some(v1::rejection::Context::Account(v1::AccountRef {
                ledger_id: BOOK.0.to_string(), account_id:"2".into()
            }))),
            "{decision:?}"
        );
    }
}

async fn status(channel: &Channel) -> Option<v1::GetStatusResponse> {
    v1::monitor_client::MonitorClient::new(channel.clone())
        .get_status(v1::GetStatusRequest {
            ledger_id: BOOK.0.to_string(),
        })
        .await
        .ok()
        .map(|r| r.into_inner())
}

async fn leader(members: &BTreeMap<u64, Channel>) -> v1::GetStatusResponse {
    timeout(WAIT, async {
        loop {
            for channel in members.values() {
                if let Some(s) = status(channel).await
                    && s.is_leader
                {
                    return s;
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("protected voters did not elect a leader")
}

fn fingerprint(binary: &Path) -> String {
    let hash = ProcessCommand::new("shasum")
        .args(["-a", "256"])
        .arg(binary)
        .output()
        .or_else(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                ProcessCommand::new("sha256sum").arg(binary).output()
            } else {
                Err(err)
            }
        })
        .unwrap();
    assert!(hash.status.success());
    let hash = String::from_utf8(hash.stdout)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .to_owned();
    assert!(hash.len() == 64 && hash.bytes().all(|v| v.is_ascii_hexdigit()));
    hash
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "release three-voter process crash and same-disk restart drill on loopback"]
async fn protected_all_voter_processes_sigkill_reconcile_inflight() {
    let binary = fs::canonicalize(
        std::env::var_os("LEDGER_CANDIDATE_NODE_BIN")
            .unwrap_or_else(|| env!("CARGO_BIN_EXE_ledger-node").into()),
    )
    .unwrap();
    println!(
        "process_crash binary={} sha256={}",
        binary.display(),
        fingerprint(&binary)
    );
    let mut peers = Cluster::new().await;
    let mut public = Cluster::new().await;
    let journal_dir = TempDir::new().unwrap();
    let client_archive = TempDir::new().unwrap();
    let records = client_archive.path();
    let journal_path = journal_dir.path().join("journal");
    let configs = write_configs(&peers, Some((&public, &journal_path)));
    let cert = leaf("backend.test", &public.ca, &public.ca_key);
    let ca = public.ca.pem();
    let mut seeds = Vec::new();
    let mut monitors = BTreeMap::new();
    let mut disks = BTreeMap::new();
    let mut live = BTreeMap::new();
    voter(&binary, "journal-init", &configs[&1])
        .wait_success()
        .await;
    let independent = Journal::open(&journal_path, &super::super::recovery::spec()).unwrap();
    for (&id, config) in &configs {
        // Exercise reopening through the durable committed log and rebuilding the working projection.
        let raw = fs::read_to_string(config).unwrap();
        assert!(raw.contains("interval_entries = 1"));
        fs::write(
            config,
            raw.replace("interval_entries = 1", "interval_entries = 0"),
        )
        .unwrap();
        voter(&binary, "init", config).wait_success().await;
        let disk = peers.directory.path().join(format!("n{id}"));
        let m = fs::metadata(&disk).unwrap();
        disks.insert(id, (disk, m.dev(), m.ino()));
        let api = &public.members[&id];
        let endpoint = format!("https://{}", api.addr);
        let tls = ClientTlsConfig::new()
            .domain_name(api.domain.clone())
            .ca_certificate(Root::from_pem(&ca))
            .identity(Credential::from_pem(&cert.cert_pem, &cert.key_pem));
        monitors.insert(
            id,
            Endpoint::from_shared(endpoint.clone())
                .unwrap()
                .tls_config(tls)
                .unwrap()
                .connect_lazy(),
        );
        seeds.push(ClientSeed {
            node: id,
            endpoint,
            domain: api.domain.clone(),
        });
        drop(peers.members.get_mut(&id).unwrap().listener.take());
        drop(public.members.get_mut(&id).unwrap().listener.take());
        live.insert(id, voter(&binary, "run", config));
    }
    let sdk = LedgerClient::connect(seeds, ca.into_bytes(), cert.cert_pem, cert.key_pem).unwrap();
    for (id, normal) in [(1, v1::Side::Debit), (2, v1::Side::Credit)] {
        let request = v1::CreateAccountRequest {
            ledger_id: BOOK.0.to_string(),
            request_id: id.to_string(),
            account_id: id.to_string(),
            normal: normal.into(),
            // The wallet alone enforces the minimum on withdrawals.
            bounds: (id == 2).then(|| v1::Bounds {
                min: Some("0".into()),
                max: None,
            }),
            ..Default::default()
        };
        save(records, id, "request", &request);
        let answer = sdk
            .create_account(request, Instant::now() + WAIT)
            .await
            .unwrap();
        assert!(matches!(
            answer.result,
            Some(v1::decision::Result::Accepted(_))
        ));
        save(records, id, "receipt", &answer);
    }
    for (request, entry) in [
        (transfer(10, 1, 2, "100"), Some(1)),
        (transfer(11, 2, 1, "10"), Some(2)),
        (transfer(12, 2, 1, "101"), None),
    ] {
        let id = request.request_id.parse().unwrap();
        save(records, id, "request", &request);
        let answer = sdk
            .post_entry(request.clone(), Instant::now() + WAIT)
            .await
            .unwrap();
        check_payment(&request, &answer, entry);
        save(records, id, "receipt", &answer);
    }
    let original = leader(&monitors).await;
    let cut = independent.head().unwrap().index;
    assert_eq!(cut, original.applied_index);
    println!(
        "process_crash acknowledged_receipts=5 accepted=4 rejected=1 protected_index={cut} old_leader={} term={}",
        original.node_id, original.term
    );

    // Prevent a protected reply, but keep Raft active. Admit one call at a
    // time until each has crossed a distinct local apply boundary. Their
    // client futures remain unresolved throughout the fatal signals.
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(journal_path.join("LOCK"))
        .unwrap();
    lock.lock().unwrap();
    let mut jobs = Vec::new();
    let mut applied = cut;
    for (id, qty) in [(20, "3"), (21, "4"), (22, "200")] {
        let request = transfer(id, 2, 1, qty);
        save(records, id, "request", &request);
        let caller = sdk.clone();
        jobs.push((
            id,
            tokio::spawn(async move {
                caller
                    .post_entry(request, Instant::now() + Duration::from_secs(5))
                    .await
            }),
        ));
        applied = timeout(WAIT, async {
            loop {
                let s = status(&monitors[&original.node_id]).await.unwrap();
                assert!(s.is_leader && s.term == original.term);
                assert_eq!(s.verified_journal_index, Some(cut));
                if s.applied_index > applied {
                    return s.applied_index;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("unacknowledged write failed to reach local applied frontier");
        println!("process_crash pending_request={id} applied_index={applied} verified_index={cut}");
    }
    assert!(jobs.iter().all(|(_, task)| !task.is_finished()));
    for process in live.values_mut() {
        assert!(process.0.try_wait().unwrap().is_none());
        process.0.kill().unwrap();
    }
    for (id, mut process) in std::mem::take(&mut live) {
        let exit = process.wait_exit().await;
        assert_eq!(exit.signal(), Some(9));
        println!("process_crash voter={id} signal=9");
    }
    drop(lock);
    assert_eq!(independent.head().unwrap().index, cut);
    for (id, task) in jobs {
        let error = task
            .await
            .unwrap()
            .expect_err("all three clients must have an unknown result");
        assert!(
            (error.code() == Code::Unknown && error.message() == "transport error")
                || matches!(
                    error.code(),
                    Code::DeadlineExceeded | Code::Unavailable | Code::Cancelled
                ),
            "{error}"
        );
        println!(
            "process_crash pending_request={id} client_status={:?}",
            error.code()
        );
    }

    for (&id, config) in &configs {
        let (path, dev, ino) = &disks[&id];
        let m = fs::metadata(path).unwrap();
        assert_eq!((m.dev(), m.ino()), (*dev, *ino));
        live.insert(id, voter(&binary, "run", config));
    }
    let replacement = leader(&monitors).await;
    assert!(replacement.term > original.term);
    println!(
        "process_crash restarted_same_voter_directories=3 leader={} term={}",
        replacement.node_id, replacement.term
    );
    for id in [1, 2, 10, 11, 12] {
        let saved: v1::Decision = load(records, id, "receipt");
        assert_eq!(receipt(&sdk, id).await, saved);
        let deadline = Instant::now() + WAIT;
        let retried = match id {
            1 | 2 => {
                sdk.create_account(load(records, id, "request"), deadline)
                    .await
            }
            _ => sdk.post_entry(load(records, id, "request"), deadline).await,
        };
        assert_eq!(retried.unwrap(), saved);
    }
    for (id, entry) in [(20, Some(3)), (21, Some(4)), (22, None)] {
        let request: v1::PostEntryRequest = load(records, id, "request");
        let recovered = receipt(&sdk, id).await;
        check_payment(&request, &recovered, entry);
        assert_eq!(
            sdk.post_entry(request, Instant::now() + WAIT)
                .await
                .unwrap(),
            recovered
        );
    }
    let fresh = transfer(23, 2, 1, "2");
    let answer = sdk
        .post_entry(fresh.clone(), Instant::now() + WAIT)
        .await
        .unwrap();
    check_payment(&fresh, &answer, Some(5));
    let mut original_posts = 0;
    for (entry, id) in [(1, 10), (2, 11), (3, 20), (4, 21), (5, 23)] {
        let expected = if id == 23 {
            fresh.clone()
        } else {
            load(records, id, "request")
        };
        let found = sdk
            .get_entry(
                v1::GetEntryRequest {
                    ledger_id: BOOK.0.to_string(),
                    entry_id: entry,
                },
                Instant::now() + WAIT,
            )
            .await
            .unwrap()
            .entry
            .unwrap();
        check_posting(&expected, &found, entry);
        original_posts += 1;
    }
    assert!(
        sdk.get_entry(
            v1::GetEntryRequest {
                ledger_id: BOOK.0.to_string(),
                entry_id: 6
            },
            Instant::now() + WAIT
        )
        .await
        .unwrap()
        .entry
        .is_none()
    );
    for (id, debits, credits) in [(1, "100", "19"), (2, "19", "100")] {
        let account = sdk
            .get_account(
                v1::GetAccountRequest {
                    ledger_id: BOOK.0.to_string(),
                    account_id: id.to_string(),
                },
                Instant::now() + WAIT,
            )
            .await
            .unwrap()
            .account
            .unwrap();
        assert_eq!(account.posted_balance, "81");
        assert_eq!(account.available_balance, "81");
        assert_eq!(
            account.counters,
            Some(v1::Counters {
                debits_posted: debits.into(),
                credits_posted: credits.into(),
                debits_pending: "0".into(),
                credits_pending: "0".into()
            })
        );
    }
    let final_cut = independent.head().unwrap().index;
    timeout(WAIT, async {
        for (&id, channel) in &monitors {
            loop {
                if let Some(s) = status(channel).await
                    && s.node_id == id
                    && s.leader_id == Some(replacement.node_id)
                    && s.applied_index >= final_cut
                {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        }
    })
    .await
    .expect("not all three original voters caught up after restart");
    println!(
        "process_crash saved_receipts_matched=5 identical_acknowledged_retries=5 pending_recovered_before_retry=3 identical_pending_retries=3 unique_postings={original_posts} first_absent_entry=6 final_wallet=81 healthy_voters=3 final_journal_index={final_cut}"
    );
    for (_, process) in live {
        process.stop().await;
    }
}
