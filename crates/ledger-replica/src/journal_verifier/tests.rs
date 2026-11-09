use super::*;
use crate::ReplicaConfig;
use ledger_core::LedgerId;
use ledger_recovery::Spec;
use raft::eraftpb::{Entry, Message, MessageType};
use std::path::Path;

fn config() -> ReplicaConfig {
    ReplicaConfig {
        node: 2,
        voters: vec![1, 2, 3],
        cluster: 71,
        ledger: LedgerId(3),
        asset: "USD".into(),
        scale: 2,
    }
}

fn journal(path: &Path) -> Journal {
    Journal::init(
        path,
        &Spec {
            ledger: 3,
            asset: "USD".into(),
            scale: 2,
        },
        71,
    )
    .unwrap()
}

fn voter(path: &Path, count: u64) -> Replica {
    let mut voter = Replica::bootstrap(path, config()).unwrap();
    let mut append = Message {
        from: 1,
        to: 2,
        term: 1,
        commit: count,
        ..Default::default()
    };
    append.set_msg_type(MessageType::MsgAppend);
    for index in 1..=count {
        append.entries.push(Entry {
            term: 1,
            index,
            ..Default::default()
        });
    }
    voter.step(append).unwrap();
    voter.drain_ready().unwrap();
    assert_eq!(voter.applied_index(), count);
    voter
}

#[test]
fn follower_startup_can_inspect_a_published_generation_while_a_writer_holds_the_lock() {
    let root = tempfile::tempdir().unwrap();
    let journal = journal(&root.path().join("journal"));
    let voter = voter(&root.path().join("voter"), 1);
    let holder = std::fs::File::options()
        .read(true)
        .write(true)
        .open(journal.directory().join("LOCK"))
        .unwrap();
    holder.lock().unwrap();
    let opened = Journal::open(journal.directory(), journal.spec()).unwrap();
    let mut verifier = JournalVerifier::open(&voter, opened).unwrap();
    assert!(matches!(
        verifier.confirm_through(1),
        Err(VerificationError::Journal(JournalError::Io(e))) if e.kind() == std::io::ErrorKind::TimedOut
    ));
    drop(holder);
    assert_eq!(verifier.confirm_through(1).unwrap().head.index, 1);
}

#[test]
fn raft_sync_counters_distinguish_grouped_appends_hard_state_and_message_only_ready() {
    let root = tempfile::tempdir().unwrap();
    let mut node = voter(&root.path().join("voter"), 4);
    let raft = node.raft_syncs();
    assert_eq!(
        (
            raft.ready_batches,
            raft.append_batches,
            raft.appended_entries
        ),
        (1, 1, 4)
    );
    assert_eq!(raft.commit_batches, 0);
    node.drain_ready().unwrap();
    assert_eq!(node.raft_syncs(), raft);

    // Append index 5 without committing it, then commit it via heartbeat.
    // The latter Ready syncs only hard state. A duplicate heartbeat sends a
    // response but needs no new WAL batch.
    let mut append = Message {
        from: 1,
        to: 2,
        term: 1,
        index: 4,
        log_term: 1,
        commit: 4,
        ..Default::default()
    };
    append.set_msg_type(MessageType::MsgAppend);
    append.entries.push(Entry {
        term: 1,
        index: 5,
        ..Default::default()
    });
    node.step(append).unwrap();
    node.drain_ready().unwrap();
    let mut heartbeat = Message {
        from: 1,
        to: 2,
        term: 1,
        commit: 5,
        ..Default::default()
    };
    heartbeat.set_msg_type(MessageType::MsgHeartbeat);
    node.step(heartbeat.clone()).unwrap();
    node.drain_ready().unwrap();
    let committed = node.raft_syncs();
    assert_eq!(node.applied_index(), 5);
    assert_eq!(
        (
            committed.ready_batches,
            committed.commit_batches,
            committed.append_batches,
            committed.appended_entries
        ),
        (3, 0, 2, 5)
    );
    node.step(heartbeat).unwrap();
    node.drain_ready().unwrap();
    assert_eq!(node.raft_syncs(), committed);
}

#[test]
fn counts_local_journal_publications_separately_from_observation_and_failed_rounds() {
    let root = tempfile::tempdir().unwrap();
    let node = voter(&root.path().join("voter"), 4);
    let log = journal(&root.path().join("journal"));
    let mut writer = JournalVerifier::open(&node, log.clone()).unwrap();
    writer.confirm_through(3).unwrap();
    writer.confirm_through(3).unwrap();
    assert_eq!(writer.append_publications(), 1);
    let mut observer = JournalVerifier::open(&node, log.clone()).unwrap();
    observer.observe().unwrap();
    assert_eq!(observer.append_publications(), 0);
    writer.confirm_through(4).unwrap();
    assert_eq!(writer.append_publications(), 2);
    log.fence(&log.head().unwrap()).unwrap();
    assert!(writer.confirm_through(4).is_err());
    assert_eq!(writer.append_publications(), 2);
}

#[test]
fn restarted_verifier_compares_only_the_suffix_and_still_checks_serving_authority() {
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("voter");
    let journal = journal(&root.path().join("journal"));
    let first = voter(&data, 3);
    let entries = first.committed_source().entries_after(0, 3).unwrap();
    let base = journal
        .append(&journal.head().unwrap(), &entries[..2])
        .unwrap();
    let mut worker = JournalVerifier::open(&first, journal.clone()).unwrap();
    let verified = worker.confirm_through(2).unwrap();
    assert_eq!(
        (verified.head, verified.compared_entries),
        (base.clone(), 2)
    );
    assert_eq!(
        first.committed_source().journal_anchor().unwrap(),
        Some(base.clone())
    );
    drop(worker);
    drop(first);

    let tip = journal.append(&base, &entries[2..]).unwrap();
    let restarted = Replica::reopen(&data, config()).unwrap();
    let mut worker = JournalVerifier::open(&restarted, journal.clone()).unwrap();
    let verified = worker.confirm_through(3).unwrap();
    assert_eq!((verified.head, verified.compared_entries), (tip.clone(), 1));
    assert_eq!(worker.confirm_through(3).unwrap().compared_entries, 0);
    journal.fence(&tip).unwrap();
    assert!(matches!(
        worker.confirm_through(3),
        Err(VerificationError::Fenced)
    ));
}

#[test]
fn observer_never_publishes_and_previously_saved_digest_rejects_foreign_history() {
    let root = tempfile::tempdir().unwrap();
    let node = voter(&root.path().join("voter"), 4);
    let source = node.committed_source();
    let a = journal(&root.path().join("journal-a"));
    let entries = source.entries_after(0, 4).unwrap();
    let base = a.append(&a.head().unwrap(), &entries[..2]).unwrap();
    let mut reader = JournalVerifier::open(&node, a.clone()).unwrap();
    assert_eq!(reader.observe().unwrap().head, base);
    assert_eq!(a.head().unwrap(), base);
    assert_eq!(source.journal_anchor().unwrap(), Some(base.clone()));
    let writer = reader.confirm_through(4).unwrap();
    assert_eq!(writer.head.index, 4);
    assert_eq!(source.journal_anchor().unwrap(), Some(base)); // no per-batch anchor sync

    let b = journal(&root.path().join("journal-b"));
    let mut alternate = entries;
    alternate[0].context = b"different committed history".to_vec().into();
    let before = b.append(&b.head().unwrap(), &alternate[..1]).unwrap();
    let mut other = JournalVerifier::open(&node, b.clone()).unwrap();
    assert!(other.confirm_through(4).is_err()); // rolled back behind the saved index
    assert_eq!(b.head().unwrap(), before);
    let same_index = b.append(&before, &alternate[1..2]).unwrap();
    assert!(other.confirm_through(4).is_err());
    let ahead = b.append(&same_index, &alternate[2..]).unwrap();
    assert!(other.confirm_through(4).is_err()); // same tip term/index, different ancestry
    assert_eq!(b.head().unwrap(), ahead);
}

#[test]
fn grouped_publications_advance_the_durable_anchor_at_the_entry_interval() {
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("voter");
    let journal = journal(&root.path().join("journal"));
    let boundary = 1 + JournalVerifier::ANCHOR_INTERVAL;
    let node = voter(&data, boundary + 1);
    let source = node.committed_source();
    let mut worker = JournalVerifier::open(&node, journal.clone()).unwrap();
    let first = worker.confirm_through(1).unwrap().head;
    let mut tip = first.clone();
    while tip.index < boundary - 1 {
        let group = worker.confirm_through(boundary - 1).unwrap();
        assert!(group.head.index > tip.index);
        assert_eq!(group.compared_entries, 0);
        assert_eq!(source.journal_anchor().unwrap(), Some(first.clone()));
        tip = group.head;
    }
    let covered = worker.confirm_through(boundary).unwrap().head;
    assert_eq!(source.journal_anchor().unwrap(), Some(covered.clone()));
    assert_eq!(
        worker.confirm_through(boundary + 1).unwrap().head.index,
        boundary + 1
    );
    assert_eq!(source.journal_anchor().unwrap(), Some(covered));
    let mut fresh = JournalVerifier::open(&node, journal).unwrap();
    assert_eq!(fresh.observe().unwrap().compared_entries, 1);
}

#[test]
fn follower_can_save_the_compared_cut_while_a_leader_extends_the_journal() {
    let root = tempfile::tempdir().unwrap();
    let node = voter(&root.path().join("voter"), 3);
    let journal = journal(&root.path().join("journal"));
    let source = node.committed_source();
    let entries = source.entries_after(0, 3).unwrap();
    journal
        .append(&journal.head().unwrap(), &entries[..2])
        .unwrap();
    let mut observer = JournalVerifier::open(&node, journal.clone()).unwrap();
    let mut serving = JournalVerifier::open(&node, journal.clone()).unwrap();
    let sampled = observer.active_head().unwrap();
    let newer = journal.append(&sampled, &entries[2..]).unwrap();
    // Interleave a concurrent publication after sampling HEAD, before the
    // observer finishes verifying the original cut. No serving request is waiting.
    assert!(matches!(
        serving.confirm_at(sampled.clone(), Some(2)),
        Err(VerificationError::Journal(JournalError::Conflict))
    ));
    assert_eq!(source.journal_anchor().unwrap(), None);
    assert_eq!(
        observer.confirm_at(sampled.clone(), None).unwrap().head,
        sampled
    );
    assert_eq!(source.journal_anchor().unwrap(), Some(sampled));
    assert_eq!(observer.observe().unwrap().head, newer);
}

#[test]
fn competing_verifiers_retry_from_the_winning_durable_head() {
    let root = tempfile::tempdir().unwrap();
    let node = voter(&root.path().join("voter"), 2);
    let journal = journal(&root.path().join("journal"));
    let entries = node.committed_source().entries_after(0, 2).unwrap();
    let mut first = JournalVerifier::open(&node, journal.clone()).unwrap();
    let mut second = JournalVerifier::open(&node, journal.clone()).unwrap();
    let base = journal
        .append(&journal.head().unwrap(), &entries[..1])
        .unwrap();
    first.observe().unwrap();
    let tip = journal.append(&base, &entries[1..]).unwrap();
    assert!(second.observe().is_err());
    let retry = second.observe().unwrap();
    assert_eq!((retry.head, retry.compared_entries), (tip, 1));
}
