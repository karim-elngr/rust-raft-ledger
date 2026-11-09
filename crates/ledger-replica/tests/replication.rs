use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use ledger_core::{
    AccountId, Balance, Bounds, Command, Counters, Error, Funding, LedgerId, Line, Outcome,
    RequestId, Shortfall, Side, Source, SourceFloor, Waterfall,
};
use ledger_replica::{Replica, ReplicaConfig};
use tempfile::TempDir;

const LEDGER: LedgerId = LedgerId(41);
const BANK: AccountId = AccountId::new(LEDGER, 1);
const REWARDS: AccountId = AccountId::new(LEDGER, 2);
const CASH: AccountId = AccountId::new(LEDGER, 3);
const MERCHANT: AccountId = AccountId::new(LEDGER, 4);
const PAYMENT: RequestId = RequestId(80);
const SHORTFALL: RequestId = RequestId(81);

type Voters = BTreeMap<u64, Replica>;

fn config(node: u64) -> ReplicaConfig {
    ReplicaConfig {
        cluster: 784,
        node,
        voters: vec![1, 2, 3],
        ledger: LEDGER,
        asset: "USD".into(),
        scale: 2,
    }
}

fn node_path(root: &Path, node: u64) -> PathBuf {
    root.join(format!("n{node}"))
}

fn bootstrap(root: &Path) -> Voters {
    (1..=3)
        .map(|id| {
            let replica = Replica::bootstrap(&node_path(root, id), config(id)).unwrap();
            (id, replica)
        })
        .collect()
}

/// Deliver the messages produced by the real Raft drivers. A removed voter is offline.
fn drive_until(voters: &mut Voters, done: impl Fn(&Voters) -> bool) {
    for _ in 0..1000 {
        let mut messages = Vec::new();
        for voter in voters.values_mut() {
            messages.extend(voter.drain_ready().unwrap());
        }
        let idle = messages.is_empty();
        for message in messages {
            if let Some(destination) = voters.get_mut(&message.to) {
                destination.step(message).unwrap();
            }
        }
        if done(voters) {
            return;
        }
        if idle {
            for voter in voters.values_mut() {
                voter.tick();
            }
        }
    }
    let progress: Vec<_> = voters
        .iter()
        .map(|(id, n)| (id, n.is_leader(), n.applied_index()))
        .collect();
    panic!("Raft cluster did not reach the expected state: (id, leader, applied) = {progress:?}");
}

fn opening() -> Vec<(RequestId, Command)> {
    let mut commands: Vec<_> = [BANK, REWARDS, CASH, MERCHANT]
        .map(|account| {
            let normal = if account == BANK {
                Side::Debit
            } else {
                Side::Credit
            };
            (
                RequestId(account.number),
                Command::CreateAccount {
                    account,
                    normal,
                    code: 1,
                    bounds: Bounds {
                        min: Some(Balance::positive(0)),
                        max: None,
                    },
                },
            )
        })
        .into();
    commands.push((
        RequestId(5),
        Command::Post {
            funding: Funding::Explicit(vec![
                Line::debit(BANK, 13000),
                Line::credit(REWARDS, 3000),
                Line::credit(CASH, 10000),
            ]),
            conditions: vec![],
            corrects: None,
        },
    ));
    commands
}

#[test]
fn lagging_voter_receives_a_large_backlog_in_wire_sized_appends() {
    use protobuf::Message as _;
    use raft::eraftpb::MessageType;

    let root = TempDir::new().unwrap();
    let mut voters = bootstrap(root.path());
    voters.get_mut(&1).unwrap().campaign().unwrap();
    drive_until(&mut voters, |v| v.values().all(|r| r.applied_index() > 0));
    let offline = voters.remove(&3).unwrap();
    let cut = offline.applied_index();
    let command = Command::Post {
        funding: Funding::Explicit(vec![Line::credit(CASH, 1); 1024]),
        conditions: vec![],
        corrects: None,
    };
    for n in 1..=8 {
        let commands = vec![(RequestId(n), command.clone()); 18];
        voters.get_mut(&1).unwrap().propose(&commands).unwrap();
        drive_until(&mut voters, |v| v[&2].applied_index() >= cut + n as u64);
    }
    voters.insert(3, offline);
    let last = voters[&1].applied_index();
    let mut messages = 0;
    let mut total = 0;
    let mut largest = 0;
    for _ in 0..100 {
        voters.get_mut(&1).unwrap().tick();
        let mut pending = Vec::new();
        for voter in voters.values_mut() {
            pending.extend(voter.drain_ready().unwrap());
        }
        for m in pending {
            if m.to == 3 && m.get_msg_type() == MessageType::MsgAppend && !m.entries.is_empty() {
                let bytes = m.write_to_bytes().unwrap().len();
                assert!(
                    bytes <= ledger_replica::MAX_RAFT_MESSAGE_BYTES,
                    "append exceeds peer wire limit: {bytes}"
                );
                messages += 1;
                total += bytes;
                largest = largest.max(bytes);
            }
            voters.get_mut(&m.to).unwrap().step(m).unwrap();
        }
        if voters[&3].applied_index() == last {
            break;
        }
    }
    assert!(messages > 1 && total > ledger_replica::MAX_RAFT_MESSAGE_BYTES);
    eprintln!("catch-up append messages={messages} total_bytes={total} largest_bytes={largest}");
    assert_eq!(voters[&3].applied_index(), last);
    for n in 1..=8 {
        let expected = voters[&1].decision(RequestId(n)).unwrap().unwrap();
        assert_eq!(voters[&3].decision(RequestId(n)).unwrap(), Some(expected));
    }
}

#[test]
fn static_learner_follows_a_new_leader_but_cannot_supply_a_quorum_after_restart() {
    let root = TempDir::new().unwrap();
    let mut members: Voters = (1..=4)
        .map(|id| {
            (
                id,
                Replica::bootstrap(&node_path(root.path(), id), config(id).with_learner(4))
                    .unwrap(),
            )
        })
        .collect();
    assert!(members.get_mut(&4).unwrap().campaign().is_err());
    members.get_mut(&1).unwrap().campaign().unwrap();
    drive_until(&mut members, |m| {
        m[&1].is_leader() && m[&4].applied_index() > 0
    });
    members.get_mut(&1).unwrap().propose(&opening()).unwrap();
    drive_until(&mut members, |m| m[&4].account(BANK).unwrap().is_some());
    let cut = members[&4]
        .begin_checkpoint()
        .unwrap()
        .unwrap()
        .finish()
        .unwrap();
    members.remove(&4);
    let path = node_path(root.path(), 4);
    assert!(Replica::reopen(&path, config(4).with_learner(7)).is_err());
    let mut learner = Replica::reopen(&path, config(4).with_learner(4)).unwrap();
    assert!(learner.is_learner());
    assert_eq!(learner.recovered_from(), cut.index);
    assert!(learner.request_read_index(vec![1]).is_err());
    assert!(learner.campaign().is_err());
    members.insert(4, learner);
    members.remove(&1);
    drive_until(&mut members, |m| m[&2].is_leader() || m[&3].is_leader());
    let leader = if members[&2].is_leader() { 2 } else { 3 };
    drive_until(&mut members, |m| {
        m[&4].applied_index() > cut.index && m[&leader].can_read_index()
    });
    members.remove(&(5 - leader));
    let command = opening().remove(0).1;
    let receipt = RequestId(999);
    members
        .get_mut(&leader)
        .unwrap()
        .propose(&[(receipt, command)])
        .unwrap();
    members
        .get_mut(&leader)
        .unwrap()
        .request_read_index(vec![2])
        .unwrap();
    drive_until(&mut members, |m| !m[&leader].is_leader());
    for member in members.values_mut() {
        assert!(member.decision(receipt).unwrap().is_none());
        assert!(member.take_read_states().is_empty());
        assert!(!member.is_leader());
    }
}

#[test]
fn read_barrier_returns_its_context_after_a_quorum_response() {
    let root = TempDir::new().unwrap();
    let mut voters = bootstrap(root.path());
    assert!(
        voters
            .get_mut(&2)
            .unwrap()
            .request_read_index(b"follower".to_vec())
            .is_err()
    );
    voters.get_mut(&1).unwrap().campaign().unwrap();
    drive_until(&mut voters, |v| {
        v[&1].is_leader() && v[&1].applied_index() > 0
    });
    let leader = voters.get_mut(&1).unwrap();
    let minimum = leader.applied_index();
    leader.request_read_index(b"read-41".to_vec()).unwrap();
    let probes = leader.drain_ready().unwrap();
    assert!(leader.take_read_states().is_empty());
    for probe in probes {
        let follower = voters.get_mut(&probe.to).unwrap();
        follower.step(probe).unwrap();
        for reply in follower.drain_ready().unwrap() {
            voters.get_mut(&reply.to).unwrap().step(reply).unwrap();
        }
    }
    let leader = voters.get_mut(&1).unwrap();
    leader.drain_ready().unwrap();
    let states = leader.take_read_states();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].request_ctx, b"read-41");
    assert!((minimum..=leader.applied_index()).contains(&states[0].index));
    assert!(leader.take_read_states().is_empty());
}

fn payment() -> Command {
    Command::Post {
        funding: Funding::Waterfall(Waterfall {
            amount: 8000,
            sources: [REWARDS, CASH]
                .map(|account| Source {
                    account,
                    floor: SourceFloor::Configured,
                    cap: None,
                })
                .into(),
            destinations: vec![Line::credit(MERCHANT, 8000)],
            shortfall: Shortfall::RequireFull,
        }),
        conditions: vec![],
        corrects: None,
    }
}

#[test]
fn quorum_failover_and_disk_replay_preserve_exact_payment_and_rejection() {
    let root = TempDir::new().unwrap();
    let mut voters = bootstrap(root.path());
    voters.get_mut(&1).unwrap().campaign().unwrap();
    drive_until(&mut voters, |v| v[&1].is_leader());
    voters.get_mut(&1).unwrap().propose(&opening()).unwrap();
    drive_until(&mut voters, |v| {
        v.values().all(|n| n.ledger().unwrap().journal().len() == 1)
    });

    // Competing requests occupy one log entry. Only the first can fund the payment.
    let attempts = [(PAYMENT, payment()), (SHORTFALL, payment())];
    voters.get_mut(&1).unwrap().propose(&attempts).unwrap();
    drive_until(&mut voters, |v| {
        for node in v.values() {
            let book = node.ledger().unwrap();
            assert_eq!(
                book.request_outcome(PAYMENT).is_some(),
                book.request_outcome(SHORTFALL).is_some()
            );
        }
        v[&1].ledger().unwrap().request_outcome(SHORTFALL).is_some()
    });
    let before_crash = voters[&1].ledger().unwrap().clone();
    let crash_index = voters[&1].applied_index();
    let original = before_crash.request_outcome(PAYMENT).unwrap();
    assert!(
        matches!(original, Ok(Outcome::Posted(entry)) if entry.lines.as_ref() ==
        [Line::debit(REWARDS, 3000), Line::debit(CASH, 5000), Line::credit(MERCHANT, 8000)])
    );
    assert_eq!(
        before_crash.request_outcome(SHORTFALL),
        Some(&Err(Error::InsufficientFunding))
    );
    drop(voters.remove(&1).unwrap());

    drive_until(&mut voters, |v| v.values().any(Replica::is_leader));
    let leader = *voters.iter().find(|(_, n)| n.is_leader()).unwrap().0;
    let replenish = (
        RequestId(82),
        Command::Post {
            funding: Funding::Explicit(vec![Line::debit(BANK, 8000), Line::credit(CASH, 8000)]),
            conditions: vec![],
            corrects: None,
        },
    );
    let conflict = (PAYMENT, replenish.1.clone());
    let retry = [
        replenish,
        attempts[1].clone(),
        attempts[0].clone(),
        conflict.clone(),
    ];
    voters.get_mut(&leader).unwrap().propose(&retry).unwrap();
    drive_until(&mut voters, |v| {
        v.values()
            .all(|n| n.ledger().unwrap().request_outcome(RequestId(82)).is_some())
    });
    let expected = voters[&leader].ledger().unwrap().clone();
    assert_eq!(expected.request_outcome(PAYMENT), Some(original));
    assert_eq!(
        expected.outcome_for(PAYMENT, &payment()),
        Some(original.clone())
    );
    assert_eq!(
        expected.outcome_for(PAYMENT, &conflict.1),
        Some(Err(Error::IdConflict(PAYMENT)))
    );
    assert_eq!(
        expected.request_outcome(SHORTFALL),
        Some(&Err(Error::InsufficientFunding))
    );
    assert_eq!(expected.journal().len(), 3);
    assert_eq!(
        expected.account(MERCHANT).unwrap().posted_balance(),
        Balance::positive(8000)
    );
    assert_eq!(
        expected.decisions().len(),
        before_crash.decisions().len() + 1
    );
    assert!(voters.values().all(|v| v.ledger().unwrap() == &expected));

    // Remove the stopped leader's entire rebuildable database. Its Raft store survives.
    let path = node_path(root.path(), 1);
    assert!(path.join("raft/CURRENT").is_file());
    fs::rename(path.join("state"), path.join("discarded-state")).unwrap();
    let restarted = Replica::reopen(&path, config(1)).unwrap();
    assert_eq!(restarted.ledger().unwrap(), &before_crash);
    assert_eq!(restarted.applied_index(), crash_index);
    assert_eq!(
        restarted.stored_counters(CASH).unwrap(),
        Some(Counters {
            credits_posted: 10000,
            debits_posted: 5000,
            ..Counters::default()
        })
    );
    voters.insert(1, restarted);
    drive_until(&mut voters, |v| {
        v.values().all(|n| {
            n.ledger().unwrap() == &expected && n.applied_index() == v[&leader].applied_index()
        })
    });

    let final_index = voters[&leader].applied_index();
    drop(voters);
    for id in 1..=3 {
        let voter = Replica::reopen(&node_path(root.path(), id), config(id)).unwrap();
        assert_eq!(voter.applied_index(), final_index);
        assert_eq!(voter.ledger().unwrap(), &expected);
        for id in [BANK, REWARDS, CASH, MERCHANT] {
            assert_eq!(
                voter.stored_counters(id).unwrap(),
                Some(expected.account(id).unwrap().counters())
            );
        }
    }
}

#[test]
fn an_isolated_voters_persisted_append_is_excluded_from_replay() {
    let root = TempDir::new().unwrap();
    let mut voters = bootstrap(root.path());
    voters.get_mut(&1).unwrap().campaign().unwrap();
    drive_until(&mut voters, |v| v[&1].is_leader());
    voters.get_mut(&1).unwrap().propose(&opening()).unwrap();
    drive_until(&mut voters, |v| {
        v.values().all(|n| n.ledger().unwrap().journal().len() == 1)
    });

    let mut isolated = voters.remove(&1).unwrap();
    drop(voters);
    let before = isolated.ledger().unwrap().clone();
    let applied = isolated.applied_index();
    isolated.propose(&[(PAYMENT, payment())]).unwrap();
    let append = isolated.drain_ready().unwrap();
    assert!(append.iter().any(|message| {
        message.get_msg_type() == raft::eraftpb::MessageType::MsgAppend
            && !message.entries.is_empty()
    }));
    // Drop every replication message before it reaches a second voter.
    drop(append);
    assert_eq!(isolated.ledger().unwrap().request_outcome(PAYMENT), None);
    drop(isolated);

    let restarted = Replica::reopen(&node_path(root.path(), 1), config(1)).unwrap();
    assert_eq!(restarted.applied_index(), applied);
    assert_eq!(restarted.ledger().unwrap(), &before);
}
