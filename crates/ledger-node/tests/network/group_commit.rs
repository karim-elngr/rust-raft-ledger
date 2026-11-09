//! Run with --features=batch-diagnostics --release -- --ignored --nocapture.
use super::*;
use ledger_node::{NodeError, NodeOptions, NodeStatus};
use ledger_recovery::Journal;
use ledger_replica::ProposalBatch;
use std::collections::BTreeSet;

const SERIAL: usize = 48;
const BURST: usize = 384;
const CLIENTS: usize = 24;

// Each phase has two-, sixteen- and sixty-four-line payments. Every sixteenth
// payment uses an empty wallet and must produce the same durable rejection.
fn transfer(sequence: usize) -> (Command, usize, bool) {
    let pairs = [1, 8, 32][sequence % 3];
    let empty = sequence.is_multiple_of(16);
    let payer = account(if empty { 3 } else { 1 });
    let mut lines = Vec::with_capacity(pairs * 2);
    for _ in 0..pairs {
        lines.extend([Line::debit(payer, 1), Line::credit(account(2), 1)]);
    }
    (post(lines), pairs, empty)
}

async fn statuses(cluster: &Cluster, leader: u64) -> Vec<NodeStatus> {
    let through = cluster.handle(leader).status().await.unwrap().applied_index;
    timeout(WAIT, async {
        loop {
            let mut result = Vec::new();
            for id in 1..=3 {
                result.push(cluster.handle(id).status().await.unwrap());
            }
            if result.iter().all(|s| s.applied_index == through) {
                assert!(result[leader as usize - 1].is_leader);
                return result;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("followers did not finish publishing this phase")
}

#[derive(Default)]
struct Results {
    entries: BTreeSet<u64>,
    units: u128,
    rejections: usize,
    refusals: usize,
    latencies: Vec<Duration>,
}

async fn phase(
    cluster: &Cluster,
    journal: &Journal,
    leader: u64,
    limit: usize,
    start: usize,
    count: usize,
    clients: usize,
) -> Results {
    let before = statuses(cluster, leader).await;
    let base = journal.head().unwrap();
    let handle = cluster.handle(leader);
    let start_workers = Arc::new(Barrier::new(clients + 1));
    let mut tasks = JoinSet::new();
    for client in 0..clients {
        let worker = handle.clone();
        let barrier = start_workers.clone();
        tasks.spawn(async move {
            let mut results = Results::default();
            barrier.wait().await;
            for offset in (client..count).step_by(clients) {
                let sequence = start + offset;
                let (command, units, reject) = transfer(sequence);
                let id = RequestId(sequence as u128 + 100);
                let begin = Instant::now();
                let deadline = begin + WAIT;
                let decision = loop {
                    match worker.submit_protected(id, command.clone(), deadline).await {
                        Ok(result) => break result,
                        Err(NodeError::Busy) => {
                            results.refusals += 1;
                            sleep(Duration::from_millis(1)).await;
                        }
                        Err(error) => panic!("payment {sequence}: {error}"),
                    }
                };
                results.latencies.push(begin.elapsed());
                if reject {
                    assert_eq!(decision, Err(Error::BelowMinimum(account(3))));
                    results.rejections += 1;
                } else {
                    let Ok(Outcome::Posted(entry)) = decision else {
                        panic!("payment {sequence}: {decision:?}")
                    };
                    assert!(results.entries.insert(entry.id.0));
                    results.units += units as u128;
                }
            }
            results
        });
    }
    let begin = Instant::now();
    start_workers.wait().await;
    let mut results = Results::default();
    while let Some(result) = tasks.join_next().await {
        let worker = result.unwrap();
        for id in worker.entries {
            assert!(results.entries.insert(id));
        }
        results.units += worker.units;
        results.rejections += worker.rejections;
        results.refusals += worker.refusals;
        results.latencies.extend(worker.latencies);
    }
    let elapsed = begin.elapsed();
    assert_eq!(results.latencies.len(), count);
    assert_eq!(results.entries.len() + results.rejections, count);
    let after = statuses(cluster, leader).await;
    let (b, a) = (&before[leader as usize - 1], &after[leader as usize - 1]);
    let proposals = a.proposed_batches - b.proposed_batches;
    let commands = a.proposed_commands - b.proposed_commands;
    assert_eq!(commands, count as u64);
    if limit == 1 || clients == 1 {
        assert_eq!(proposals, commands);
    }
    let groups = a.journal_append_publications - b.journal_append_publications;
    let mut verified_groups = 0;
    journal
        .verify_since(Some(&base), &journal.head().unwrap(), |_| {
            verified_groups += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(groups, verified_groups);
    let syncs = |s: &NodeStatus| s.raft_syncs.ready_batches + s.raft_syncs.commit_batches;
    let all_syncs: u64 = before
        .iter()
        .zip(&after)
        .map(|(b, a)| syncs(a) - syncs(b))
        .sum();
    let appends = a.raft_syncs.append_batches - b.raft_syncs.append_batches;
    let entries = a.raft_syncs.appended_entries - b.raft_syncs.appended_entries;
    assert_eq!(entries, proposals);
    results.latencies.sort_unstable();
    let p99 = results.latencies[(99 * count).div_ceil(100) - 1].as_secs_f64() * 1000.;
    let queue_us =
        (a.proposal_queue_nanos - b.proposal_queue_nanos) as f64 / commands as f64 / 1000.;
    println!(
        "limit={limit} clients={clients} accepted={} rejected={} busy={} decisions/s={:.1} p99_ms={p99:.3} mean_queue_us={queue_us:.2} proposals={proposals} ready={} light_commit={} append_batches={appends} appended_entries={entries} leader_syncs/decision={:.3} all_voter_syncs/decision={:.3} publications={groups} decisions/proposal={:.2} entries/append={:.2}",
        results.entries.len(),
        results.rejections,
        results.refusals,
        count as f64 / elapsed.as_secs_f64(),
        a.raft_syncs.ready_batches - b.raft_syncs.ready_batches,
        a.raft_syncs.commit_batches - b.raft_syncs.commit_batches,
        (syncs(a) - syncs(b)) as f64 / count as f64,
        all_syncs as f64 / count as f64,
        commands as f64 / proposals as f64,
        entries as f64 / appends as f64
    );
    results
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "matched local group commit diagnostic, including three-voter peer mTLS and independent journal writes"]
async fn protected_group_commit_comparison() {
    for limit in [1, ProposalBatch::MAX_COMMANDS] {
        let root = tempfile::tempdir().unwrap();
        let journal =
            Journal::init(&root.path().join("journal"), &recovery::spec(), CLUSTER).unwrap();
        let mut cluster = recovery::original_cluster_with_options(
            journal.directory(),
            NodeOptions {
                max_commands_per_entry: limit,
                ..NodeOptions::default()
            },
        )
        .await;
        let leader = cluster.leader().await;
        let handle = cluster.handle(leader);
        for (id, side) in [(1, Side::Debit), (2, Side::Credit), (3, Side::Credit)] {
            handle
                .submit_protected(RequestId(id), create(id, side), Instant::now() + WAIT)
                .await
                .unwrap()
                .unwrap();
        }
        let serial = phase(&cluster, &journal, leader, limit, 0, SERIAL, 1).await;
        let burst = phase(&cluster, &journal, leader, limit, SERIAL, BURST, CLIENTS).await;
        assert!(serial.entries.is_disjoint(&burst.entries));
        let units = serial.units + burst.units;
        let balances = handle
            .protected_accounts(
                vec![account(1), account(2), account(3)],
                Instant::now() + WAIT,
            )
            .await
            .unwrap();
        assert_eq!(
            balances[0].as_ref().unwrap().counters().debits_posted,
            units
        );
        assert_eq!(
            balances[1].as_ref().unwrap().counters().credits_posted,
            units
        );
        assert_eq!(balances[2].as_ref().unwrap().counters().debits_posted, 0);
        for id in 1..=3 {
            cluster.stop(id).await;
        }
    }
}
