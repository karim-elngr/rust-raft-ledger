use super::*;
use ledger_node::{CheckpointPolicy, SnapshotTransferStats};
use ledger_replica::{CheckpointRef, JournalVerifier};

const POLICY: CheckpointPolicy = CheckpointPolicy {
    interval_entries: 0,
    retained_entries: 1,
};

async fn start(cluster: &mut Cluster, id: u64, journal: &Journal) {
    let path = cluster.directory.path().join(format!("n{id}"));
    let config = cfg(id, CLUSTER, &[1, 2, 3]);
    let peers = cluster.config(id);
    let member = cluster.members.get_mut(&id).unwrap();
    let (voter, listener) = match member.listener.take() {
        Some(listener) => (Replica::bootstrap(&path, config).unwrap(), listener),
        None => (
            Replica::reopen(&path, config).unwrap(),
            TcpListener::bind(member.addr).await.unwrap(),
        ),
    };
    member.node = Some(
        Node::start_configured(voter, peers, listener, Some(journal.clone()), POLICY)
            .await
            .unwrap(),
    );
}

async fn wait_for_tail(
    follower: NodeHandle,
    through: u64,
    installed: Option<CheckpointRef>,
) -> Option<SnapshotTransferStats> {
    timeout(WAIT, async {
        loop {
            let status = follower.status().await.unwrap();
            if status.applied_index >= through {
                assert_eq!(status.installed_snapshot, installed.map(|r| r.index));
                return status.last_snapshot_transfer;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the follower must reach the leader's committed tail")
}

async fn wait_for_match(leader: &NodeHandle, follower: u64, through: u64) {
    timeout(WAIT, async {
        loop {
            if leader
                .status()
                .await
                .unwrap()
                .peer_matches
                .iter()
                .any(|&(peer, index)| peer == follower && index >= through)
            {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("leader must receive the follower's durable append response");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_voter_installs_a_peer_checkpoint_and_preserves_protected_history() {
    let remote = TempDir::new().unwrap();
    let journal = Journal::init(remote.path(), &spec(), CLUSTER).unwrap();
    let mut cluster = Cluster::new().await;
    for id in 1..=3 {
        start(&mut cluster, id, &journal).await;
    }
    let leader_id = cluster.leader().await;
    let behind = (1..=3).find(|id| *id != leader_id).unwrap();
    let leader = cluster.handle(leader_id);
    cluster.stop(behind).await;
    for id in 1..=5 {
        protected(
            &leader,
            id,
            &create(id, if id == 1 { Side::Debit } else { Side::Credit }),
        )
        .await
        .unwrap();
    }
    let funds = post(vec![
        Line::debit(account(1), 13000),
        Line::credit(account(2), 3000),
        Line::credit(account(3), 10000),
    ]);
    protected(&leader, 6, &funds).await.unwrap();
    let reserved = protected(&leader, 7, &reservation()).await.unwrap();
    let posted = protected(&leader, 8, &partial_capture()).await.unwrap();
    let first = leader.checkpoint().await.unwrap();
    let rejected = post(vec![
        Line::debit(account(3), 8000),
        Line::credit(account(4), 8000),
    ]);
    let refused = protected(&leader, 9, &rejected).await;
    assert_eq!(refused, Err(Error::BelowMinimum(account(3))));
    let mut through = leader.status().await.unwrap().applied_index;
    assert!(through > first.index);
    start(&mut cluster, behind, &journal).await;
    let cold = wait_for_tail(cluster.handle(behind), through, Some(first))
        .await
        .unwrap();
    wait_for_match(&leader, behind, through).await;
    assert!(cold.file_bytes > 0 && cold.history_bytes > 0 && cold.history_entries > 0);

    // A later image shares immutable history with the first. Proposals at and
    // after both cuts continue to receive independent journal confirmation.
    cluster.stop(behind).await;
    for id in 10..=12 {
        protected(&leader, id, &create(id, Side::Credit))
            .await
            .unwrap();
    }
    let second = leader.checkpoint().await.unwrap();
    protected(&leader, 13, &create(13, Side::Credit))
        .await
        .unwrap();
    through = leader.status().await.unwrap().applied_index;
    start(&mut cluster, behind, &journal).await;
    let incremental = wait_for_tail(cluster.handle(behind), through, Some(second))
        .await
        .unwrap();
    // The small working set can fit on either side of the file-overlap threshold.
    match incremental.replay_base_index {
        Some(base) => {
            assert_eq!(base, first.index);
            assert!(incremental.replay_proof_bytes > 0);
            assert_eq!(incremental.file_bytes, 0);
            assert_eq!(incremental.reused_or_staged_file_bytes, 0);
        }
        None => {
            assert!(incremental.reused_or_staged_file_bytes > 0 && incremental.file_bytes > 0);
        }
    }
    assert!(incremental.history_entries > 0 && incremental.history_entries < second.index);
    eprintln!("cold snapshot {cold:?}; shared-base snapshot {incremental:?}");

    // A small gap above the retained replication boundary uses AppendEntries.
    cluster.stop(behind).await;
    protected(&leader, 14, &create(14, Side::Credit))
        .await
        .unwrap();
    through = leader.status().await.unwrap().applied_index;
    start(&mut cluster, behind, &journal).await;
    assert!(
        wait_for_tail(cluster.handle(behind), through, None)
            .await
            .is_none()
    );

    for id in 1..=3 {
        cluster.stop(id).await;
    }
    let path = cluster.directory.path().join(format!("n{behind}"));
    let voter_config = cfg(behind, CLUSTER, &[1, 2, 3]);
    let restored = Replica::reopen(&path, voter_config.clone()).unwrap();
    assert_eq!(restored.recovered_from(), second.index);
    assert_eq!(
        restored.outcome_for(RequestId(7), &reservation()).unwrap(),
        Some(Ok(reserved))
    );
    assert_eq!(
        restored
            .outcome_for(RequestId(8), &partial_capture())
            .unwrap(),
        Some(Ok(posted))
    );
    assert_eq!(
        restored.outcome_for(RequestId(9), &rejected).unwrap(),
        Some(refused)
    );
    assert_eq!(
        restored.reservation(HOLD).unwrap().unwrap().remaining,
        [0, 2000, 1940, 60]
    );
    assert!(
        JournalVerifier::open(&restored, journal)
            .unwrap()
            .observe()
            .unwrap()
            .head
            .index
            >= through
    );
    drop(restored);
    let rebuilt = Replica::rebuild_from_log(&path, voter_config, Default::default()).unwrap();
    for (id, expected) in counters() {
        assert_eq!(
            rebuilt.stored_counters(account(id)).unwrap(),
            Some(expected)
        );
    }
}

#[cfg(unix)]
#[path = "snapshot_load.rs"]
mod snapshot_load;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_nonvoting_learner_installs_an_authenticated_snapshot_then_reopens() {
    let mut cluster = Cluster::with_ids([1, 2, 3, 4]).await;
    let learner = 4;
    // Start only the three voters, with the same fixed membership on disk.
    for id in 1..=3 {
        start_static(&mut cluster, id, learner).await;
    }
    let leader = cluster.handle(cluster.leader().await);
    for id in 1..=5 {
        submit(&leader, id, create(id, Side::Credit)).await.unwrap();
    }
    let cut = leader.checkpoint().await.unwrap();
    let tail = create(6, Side::Debit);
    let decision = submit(&leader, 6, tail.clone()).await;
    let through = leader.status().await.unwrap().applied_index;
    assert!(through > cut.index);

    start_static(&mut cluster, learner, learner).await;
    let handle = cluster.handle(learner);
    let transferred = wait_for_tail(handle.clone(), through, Some(cut))
        .await
        .unwrap();
    assert!(transferred.file_bytes > 0 && transferred.history_entries > 0);
    let status = handle.status().await.unwrap();
    assert!(status.is_learner && !status.is_leader);
    await_outcome(&handle, 6, &tail, &decision).await;
    wait_for_match(&leader, learner, through).await;
    for id in 1..=4 {
        cluster.stop(id).await;
    }
    let path = cluster.directory.path().join("n4");
    let member = Replica::reopen(
        &path,
        cfg(learner, CLUSTER, &[1, 2, 3]).with_learner(learner),
    )
    .unwrap();
    assert_eq!(member.recovered_from(), cut.index);
    assert_eq!(
        member.snapshot_history().membership().learners,
        vec![learner]
    );
    assert_eq!(
        member.outcome_for(RequestId(6), &tail).unwrap(),
        Some(decision)
    );
}

async fn start_static(cluster: &mut Cluster, id: u64, learner: u64) {
    let path = cluster.directory.path().join(format!("n{id}"));
    let peers = cluster.config(id);
    let member = cluster.members.get_mut(&id).unwrap();
    let voters: Vec<_> = (1..=4).filter(|n| *n != learner).collect();
    let replica =
        Replica::bootstrap(&path, cfg(id, CLUSTER, &voters).with_learner(learner)).unwrap();
    member.node = Some(
        Node::start_configured(
            replica,
            peers,
            member.listener.take().unwrap(),
            None,
            POLICY,
        )
        .await
        .unwrap(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_different_learner_roster_cannot_supply_the_second_voter() {
    let mut cluster = Cluster::with_ids([1, 2, 3, 4]).await;
    start_static(&mut cluster, 1, 4).await;
    // Both sides identify 1 and 2 as voters and trust each other's TLS leaf.
    start_static(&mut cluster, 2, 3).await;
    assert!(
        timeout(Duration::from_secs(2), cluster.leader())
            .await
            .is_err()
    );
    for id in [1, 2] {
        assert_eq!(cluster.handle(id).status().await.unwrap().applied_index, 0);
    }
    start_static(&mut cluster, 3, 4).await;
    let leader_id = cluster.leader().await;
    assert_ne!(leader_id, 2);
    let leader = cluster.handle(leader_id);
    submit(&leader, 1, create(1, Side::Debit)).await.unwrap();
    let matches = leader.status().await.unwrap().peer_matches;
    assert!(matches.contains(&(2, 0)));
    assert_eq!(cluster.handle(2).status().await.unwrap().applied_index, 0);
    for id in 1..=3 {
        cluster.stop(id).await;
    }
}
