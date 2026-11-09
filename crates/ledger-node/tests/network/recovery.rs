use std::path::Path;

use ledger_core::{
    Allocation, CaptureAmount, Counters, Remainder, ReservationId, ReservationStatus, Shortfall,
    Source, SourceFloor, Waterfall,
};
use ledger_node::NodeError;
use ledger_recovery::{Head, Journal, Spec};

use super::*;

const REPLACEMENT: u128 = CLUSTER + 1;
const HOLD: ReservationId = ReservationId(37);
const NEW_VOTERS: [u64; 3] = [11, 12, 13];

pub(super) fn spec() -> Spec {
    Spec {
        ledger: BOOK.0,
        asset: "USD".into(),
        scale: 2,
    }
}

fn cfg(node: u64, cluster: u128, voters: &[u64]) -> ReplicaConfig {
    ReplicaConfig {
        node,
        cluster,
        voters: voters.to_vec(),
        ledger: BOOK,
        asset: "USD".into(),
        scale: 2,
    }
}

async fn launch(
    cluster: &mut Cluster,
    generation: u128,
    voters: Vec<(u64, Replica)>,
    remote: &Path,
) {
    launch_with_options(
        cluster,
        generation,
        voters,
        remote,
        ledger_node::NodeOptions::default(),
    )
    .await
}

async fn launch_with_options(
    cluster: &mut Cluster,
    generation: u128,
    voters: Vec<(u64, Replica)>,
    remote: &Path,
    options: ledger_node::NodeOptions,
) {
    for (id, replica) in voters {
        let mut config = cluster.config(id);
        config.cluster = generation;
        let member = cluster.members.get_mut(&id).unwrap();
        let listener = member.listener.take().unwrap();
        let journal = Journal::open(remote, &spec()).unwrap();
        member.node = Some(
            Node::start_with_options(replica, config, listener, Some(journal), options)
                .await
                .unwrap(),
        );
    }
}

pub(super) async fn original_cluster(remote: &Path) -> Cluster {
    original_cluster_with_options(remote, ledger_node::NodeOptions::default()).await
}

pub(super) async fn original_cluster_with_options(
    remote: &Path,
    options: ledger_node::NodeOptions,
) -> Cluster {
    let mut cluster = Cluster::new().await;
    let mut nodes = Vec::new();
    for id in 1..=3 {
        let path = cluster.directory.path().join(format!("n{id}"));
        nodes.push((
            id,
            Replica::bootstrap(&path, cfg(id, CLUSTER, &[1, 2, 3])).unwrap(),
        ));
    }
    launch_with_options(&mut cluster, CLUSTER, nodes, remote, options).await;
    cluster
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn isolated_follower_prepares_a_restart_anchor_without_publishing_its_local_tail() {
    use ledger_replica::JournalVerifier;
    use raft::eraftpb::{Entry, Message, MessageType};

    let mut cluster = Cluster::new().await;
    let data = cluster.directory.path().join("n2");
    let voter_config = cfg(2, CLUSTER, &[1, 2, 3]);
    let mut voter = Replica::bootstrap(&data, voter_config.clone()).unwrap();
    let through = JournalVerifier::ANCHOR_INTERVAL;
    let mut append = Message {
        from: 1,
        to: 2,
        term: 1,
        commit: through + 1,
        ..Default::default()
    };
    append.set_msg_type(MessageType::MsgAppend);
    append.entries.extend((1..=through + 1).map(|index| Entry {
        index,
        term: 1,
        ..Default::default()
    }));
    voter.step(append).unwrap();
    voter.drain_ready().unwrap();
    assert_eq!(voter.applied_index(), through + 1);

    let remote = cluster.directory.path().join("journal");
    let journal = Journal::init(&remote, &spec(), CLUSTER).unwrap();
    let mut head = journal.head().unwrap();
    let source = voter.committed_source();
    while head.index < through {
        let entries = source.entries_after(head.index, through).unwrap();
        head = journal.append(&head, &entries).unwrap();
    }
    drop(source);
    launch(&mut cluster, CLUSTER, vec![(2, voter)], &remote).await;
    let follower = cluster.handle(2);
    timeout(WAIT, async {
        loop {
            let status = follower.status().await.unwrap();
            assert!(!status.is_leader);
            if status.protected_index == Some(through) {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        follower
            .protected_account(account(1), Instant::now() + WAIT)
            .await,
        Err(NodeError::NotLeader(_))
    ));
    cluster.stop(2).await;
    let restarted = Replica::reopen(&data, voter_config).unwrap();
    assert_eq!(journal.head().unwrap(), head);
    assert_eq!(
        restarted.committed_source().journal_anchor().unwrap(),
        Some(head)
    );
    assert_eq!(
        JournalVerifier::open(&restarted, journal)
            .unwrap()
            .observe()
            .unwrap()
            .compared_entries,
        0
    );
}

async fn protected(handle: &NodeHandle, id: u128, command: &Command) -> Result<Outcome, Error> {
    handle
        .submit_protected(RequestId(id), command.clone(), Instant::now() + WAIT)
        .await
        .unwrap()
}

fn reservation() -> Command {
    Command::Reserve {
        reservation: HOLD,
        funding: Funding::Waterfall(Waterfall {
            amount: 8000,
            sources: [2, 3]
                .map(|id| Source {
                    account: account(id),
                    floor: SourceFloor::Configured,
                    cap: None,
                })
                .to_vec(),
            destinations: vec![
                Line::credit(account(4), 7760),
                Line::credit(account(5), 240),
            ],
            shortfall: Shortfall::RequireFull,
        }),
        conditions: vec![],
    }
}

fn partial_capture() -> Command {
    Command::Capture {
        reservation: HOLD,
        amount: CaptureAmount::Lines(
            [3000, 3000, 5820, 180]
                .into_iter()
                .enumerate()
                .map(|(line, amount)| Allocation {
                    line: line as u32,
                    amount,
                })
                .collect(),
        ),
        remainder: Some(Remainder::KeepReserved),
    }
}

fn counters() -> [(u128, Counters); 5] {
    [
        (
            1,
            Counters {
                debits_posted: 13000,
                ..Counters::default()
            },
        ),
        (
            2,
            Counters {
                credits_posted: 3000,
                debits_posted: 3000,
                ..Counters::default()
            },
        ),
        (
            3,
            Counters {
                credits_posted: 10000,
                debits_posted: 3000,
                debits_pending: 2000,
                ..Counters::default()
            },
        ),
        (
            4,
            Counters {
                credits_posted: 5820,
                credits_pending: 1940,
                ..Counters::default()
            },
        ),
        (
            5,
            Counters {
                credits_posted: 180,
                credits_pending: 60,
                ..Counters::default()
            },
        ),
    ]
}

fn restore_nodes(
    cluster: &Cluster,
    journal: &Journal,
    terminal: &Head,
    base: u64,
) -> Vec<(u64, Replica)> {
    NEW_VOTERS
        .into_iter()
        .map(|id| {
            let path = cluster.directory.path().join(format!("n{id}"));
            let (node, stats) = Replica::restore_with_stats(
                &path,
                cfg(id, REPLACEMENT, &NEW_VOTERS),
                journal,
                terminal,
                Default::default(),
            )
            .unwrap();
            assert_eq!(
                (stats.base_index, stats.replayed_entries),
                (base, terminal.index - base)
            );
            let local = node.committed_source();
            journal
                .verify_since(None, terminal, |entries| {
                    local
                        .verify(entries)
                        .map_err(|error| ledger_recovery::JournalError::Invalid(error.to_string()))
                })
                .unwrap();
            assert_eq!(node.applied_index(), terminal.index);
            assert_eq!(node.cluster_id(), REPLACEMENT);
            assert_eq!(node.node_id(), id);
            let ledger = node.ledger().unwrap();
            assert_eq!(ledger.journal().len(), 2);
            let hold = ledger.reservation(HOLD).unwrap();
            assert_eq!(hold.status, ReservationStatus::Open);
            assert_eq!(hold.remaining, [0, 2000, 1940, 60]);
            for (number, values) in counters() {
                assert_eq!(ledger.account(account(number)).unwrap().counters(), values);
                assert_eq!(node.stored_counters(account(number)).unwrap(), Some(values));
            }
            (id, node)
        })
        .collect()
}

#[path = "recovery/snapshots.rs"]
mod snapshots;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn total_voter_disk_loss_preserves_protected_partial_waterfall_and_retries() {
    use prost::Message;
    use std::{fs, io::Write};
    let receipts = TempDir::new().unwrap();
    let record = |id: u128, result: &Result<Outcome, Error>| {
        let bytes = ledger_api::adapter::decision(result.clone()).encode_to_vec();
        let mut file = fs::File::create_new(receipts.path().join(id.to_string())).unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
    };
    let external = TempDir::new().unwrap();
    let remote = external.path().join("journal");
    let journal = Journal::init(&remote, &spec(), CLUSTER).unwrap();
    let mut source = original_cluster(&remote).await;
    let leader = source.handle(source.leader().await);
    for id in 1..=5 {
        let created = protected(
            &leader,
            id,
            &create(id, if id == 1 { Side::Debit } else { Side::Credit }),
        )
        .await;
        record(id, &created);
        created.unwrap();
    }
    let funding = post(vec![
        Line::debit(account(1), 13000),
        Line::credit(account(2), 3000),
        Line::credit(account(3), 10000),
    ]);
    let funded = protected(&leader, 6, &funding).await;
    record(6, &funded);
    assert!(funded.is_ok());
    let reserve = reservation();
    let capture = partial_capture();
    let rejected = post(vec![
        Line::debit(account(3), 8000),
        Line::credit(account(4), 8000),
    ]);
    let reserved = protected(&leader, 7, &reserve).await;
    record(7, &reserved);
    let reserved_lines = [
        Line::debit(account(2), 3000),
        Line::debit(account(3), 5000),
        Line::credit(account(4), 7760),
        Line::credit(account(5), 240),
    ];
    assert!(
        matches!(&reserved, Ok(Outcome::Reserved { lines, .. }) if lines.as_ref() == reserved_lines)
    );
    let base = journal.head().unwrap();
    let base_index = base.index;
    let mount = journal.clone();
    let stats = tokio::task::spawn_blocking(move || {
        Replica::create_recovery_base(&mount, &base, Default::default())
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!((stats.base_index, stats.replayed_entries), (0, base_index));
    let captured = protected(&leader, 8, &capture).await;
    record(8, &captured);
    let posted_lines = [
        Line::debit(account(2), 3000),
        Line::debit(account(3), 3000),
        Line::credit(account(4), 5820),
        Line::credit(account(5), 180),
    ];
    assert!(
        matches!(&captured, Ok(Outcome::Captured {entry, ..}) if entry.id.0 == 2 && entry.lines.as_ref() == posted_lines)
    );
    let refusal = Err(Error::BelowMinimum(account(3)));
    let denied = protected(&leader, 9, &rejected).await;
    record(9, &denied);
    assert_eq!(denied, refusal);
    fs::File::open(receipts.path()).unwrap().sync_all().unwrap();

    let interrupted = Instant::now();
    for id in 1..=3 {
        source.stop(id).await;
    }
    let lost_root = source.directory.path().to_owned();
    source.directory.close().unwrap();
    assert!(!lost_root.exists());
    drop(journal);

    // Only the independent volume, including its recovery base, survives.
    let recovered = Journal::open(&remote, &spec()).unwrap();
    let terminal = recovered.fence(&recovered.head().unwrap()).unwrap();
    let mut replacement = Cluster::with_ids(NEW_VOTERS).await;
    let replicas = restore_nodes(&replacement, &recovered, &terminal, base_index);
    for (_, voter) in &replicas {
        for id in 1..=9 {
            let bytes = fs::read(receipts.path().join(id.to_string())).unwrap();
            let actual = voter.decision(RequestId(id)).unwrap().unwrap();
            assert_eq!(
                ledger_api::adapter::decision(actual.outcome).encode_to_vec(),
                bytes
            );
        }
    }
    let (_, activated) = replicas[0]
        .1
        .activate_replacement(&recovered, &terminal)
        .unwrap();
    assert_eq!(
        (activated.base_index, activated.replayed_entries),
        (base_index, terminal.index - base_index)
    );
    launch(&mut replacement, REPLACEMENT, replicas, &remote).await;
    let successor_id = replacement.leader().await;
    let successor = replacement.handle(successor_id);
    for (id, command, original) in [
        (6, &funding, funded),
        (7, &reserve, reserved),
        (8, &capture, captured),
        (9, &rejected, refusal),
    ] {
        assert_eq!(
            successor
                .protected_outcome(RequestId(id), command.clone(), Instant::now() + WAIT)
                .await
                .unwrap(),
            Some(original.clone())
        );
        assert_eq!(protected(&successor, id, command).await, original);
    }
    for (number, values) in counters() {
        let view = successor
            .protected_account(account(number), Instant::now() + WAIT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(view.counters(), values);
    }
    let finish = Command::Capture {
        reservation: HOLD,
        amount: CaptureAmount::Full,
        remainder: None,
    };
    let settled = protected(&successor, 10, &finish).await;
    let final_lines = [
        Line::debit(account(3), 2000),
        Line::credit(account(4), 1940),
        Line::credit(account(5), 60),
    ];
    assert!(
        matches!(&settled, Ok(Outcome::Captured {entry,..}) if entry.id.0 == 3 && entry.lines.as_ref() == final_lines)
    );
    let first_payment = interrupted.elapsed();
    let through = recovered.head().unwrap().index;
    timeout(WAIT, async {
        for id in NEW_VOTERS {
            loop {
                let status = replacement.handle(id).status().await.unwrap();
                if !status.is_learner
                    && status.leader_id == Some(successor_id)
                    && status.applied_index >= through
                {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        }
    })
    .await
    .expect("three replacement voters did not apply the new payment");
    eprintln!(
        "protected_restore cut_index={} independent_receipts=9 matched=9 lost=0 first_payment_ms={} three_voters_ms={} applied_through={}",
        terminal.index,
        first_payment.as_millis(),
        interrupted.elapsed().as_millis(),
        through
    );
    assert_eq!(protected(&successor, 10, &finish).await, settled);
    assert_eq!(
        protected(&successor, 11, &finish).await,
        Err(Error::ClosedReservation(HOLD))
    );
    for id in NEW_VOTERS {
        replacement.stop(id).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fencing_stops_protected_reads_retries_and_new_writes_on_the_old_quorum() {
    let external = TempDir::new().unwrap();
    let remote = external.path().join("journal");
    let journal = Journal::init(&remote, &spec(), CLUSTER).unwrap();
    let mut cluster = original_cluster(&remote).await;
    let leader = cluster.handle(cluster.leader().await);
    let first = create(1, Side::Debit);
    protected(&leader, 1, &first).await.unwrap();
    journal.fence(&journal.head().unwrap()).unwrap();
    let deadline = || Instant::now() + Duration::from_secs(2);
    assert!(
        leader
            .protected_account(account(1), deadline())
            .await
            .is_err()
    );
    assert!(
        leader
            .protected_outcome(RequestId(1), first.clone(), deadline())
            .await
            .is_err()
    );
    assert!(
        leader
            .submit_protected(RequestId(1), first, deadline())
            .await
            .is_err()
    );
    assert!(matches!(
        leader
            .submit_protected(RequestId(2), create(2, Side::Credit), deadline())
            .await,
        Err(NodeError::Fenced)
    ));
    assert!(leader.status().await.unwrap().is_leader);
    for id in 1..=3 {
        cluster.stop(id).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unavailable_journal_withholds_outcomes_after_local_application_advances() {
    let external = TempDir::new().unwrap();
    let remote = external.path().join("journal");
    let disconnected = external.path().join("disconnected");
    let journal = Journal::init(&remote, &spec(), CLUSTER).unwrap();
    let mut cluster = original_cluster(&remote).await;
    let leader = cluster.handle(cluster.leader().await);
    let first = create(1, Side::Debit);
    protected(&leader, 1, &first).await.unwrap();
    let prior = leader.status().await.unwrap().applied_index;

    std::fs::rename(&remote, &disconnected).unwrap();
    assert!(journal.head().is_err());
    let second = create(2, Side::Credit);
    let deadline = || Instant::now() + Duration::from_secs(2);
    assert!(
        leader
            .submit_protected(RequestId(2), second.clone(), deadline())
            .await
            .is_err()
    );
    let progress = leader.status().await.unwrap();
    assert!(progress.applied_index > prior);
    assert!(progress.applied_index > progress.protected_index.unwrap());
    // This local image now includes the unacknowledged write. Its durable
    // publication cannot grant independent serving authority to the voter.
    let sealed = timeout(WAIT, leader.checkpoint()).await.unwrap().unwrap();
    assert_eq!(sealed.index, progress.applied_index);
    let status = leader.status().await.unwrap();
    assert_eq!(status.checkpoint_index, Some(sealed.index));
    assert!(status.protected_index.unwrap() < sealed.index);
    assert!(
        leader
            .protected_outcome(RequestId(2), second.clone(), deadline())
            .await
            .is_err()
    );
    assert!(
        leader
            .submit_local(RequestId(2), second.clone(), deadline())
            .await
            .is_err()
    );
    std::fs::rename(disconnected, &remote).unwrap();
    assert_eq!(
        protected(&leader, 2, &second).await,
        Ok(Outcome::AccountCreated(account(2)))
    );
    for id in 1..=3 {
        cluster.stop(id).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn protected_failover_reads_need_a_live_quorum_even_with_a_healthy_journal() {
    let storage = tempfile::tempdir().unwrap();
    let journal = Journal::init(storage.path(), &spec(), CLUSTER).unwrap();
    let mut cluster = original_cluster(storage.path()).await;
    let first = cluster.leader().await;
    let cmd = create(1, Side::Credit);
    let receipt = protected(&cluster.handle(first), 1, &cmd).await;
    cluster.stop(first).await;
    let replacement = cluster.leader().await;
    let handle = cluster.handle(replacement);
    assert_eq!(
        handle
            .protected_outcome(RequestId(1), cmd.clone(), Instant::now() + WAIT)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(protected(&handle, 1, &cmd).await, receipt);
    // An existing offsite receipt alone cannot authorize a new current read.
    let follower = (1..=3)
        .find(|id| *id != first && *id != replacement)
        .unwrap();
    cluster.stop(follower).await;
    assert!(!journal.head().unwrap().fenced);
    let result = handle
        .protected_account(account(1), Instant::now() + Duration::from_secs(2))
        .await;
    assert!(matches!(
        result,
        Err(NodeError::Deadline | NodeError::NotLeader(_) | NodeError::LeadershipChanged)
    ));
    cluster.stop(replacement).await;
}
