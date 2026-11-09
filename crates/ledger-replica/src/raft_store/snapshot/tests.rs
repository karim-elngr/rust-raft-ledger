use super::*;

fn row(index: u64, term: u64) -> Entry {
    Entry {
        index,
        term,
        data: index.to_be_bytes().to_vec().into(),
        ..Default::default()
    }
}
fn hard(term: u64, vote: u64, commit: u64) -> HardState {
    HardState {
        term,
        vote,
        commit,
        ..Default::default()
    }
}
fn seal(index: u64, term: u64, marker: u8) -> CheckpointRef {
    CheckpointRef {
        index,
        term,
        digest: [marker; 32],
    }
}
fn snapshot(store: &RaftStore, cut: CheckpointRef) -> Snapshot {
    let mut image = Snapshot::default();
    image.mut_metadata().index = cut.index;
    image.mut_metadata().term = cut.term;
    image.mut_metadata().set_conf_state(store.conf.clone());
    image.data = cut.snapshot_token().into();
    image
}
fn voter(path: &Path) -> RaftStore {
    RaftStore::bootstrap(path, b"target", &[1, 2, 3]).unwrap()
}

#[test]
fn native_snapshot_retains_nonvoting_membership_after_reopen_and_rejects_role_changes() {
    let dir = tempfile::tempdir().unwrap();
    let membership = ConfState {
        voters: vec![1, 2, 3],
        learners: vec![4],
        ..Default::default()
    };
    let store =
        RaftStore::bootstrap_members(dir.path(), b"member-four", membership.clone()).unwrap();
    store
        .persist_ready(&[row(1, 1)], Some(&hard(1, 1, 1)))
        .unwrap();
    assert!(store.persist_ready(&[], Some(&hard(2, 4, 1))).is_err());
    let reference = seal(1, 1, 8);
    store.publish_checkpoint(reference).unwrap();
    drop(store);
    let store = RaftStore::reopen(dir.path(), b"member-four").unwrap();
    let offer = store.snapshot(1, 4).unwrap();
    assert_eq!(offer.get_metadata().get_conf_state(), &membership);
    let authority = store.snapshot_history();
    use raft::eraftpb::MessageType::*;
    assert!(authority.permits_message(1, 4, MsgSnapshot));
    assert!(authority.permits_message(4, 1, MsgHeartbeatResponse));
    assert!(!authority.permits_message(4, 1, MsgRequestVote));
    assert!(!authority.permits_message(4, 1, MsgAppend));
    assert!(!authority.permits_message(4, 1, MsgSnapshot));
    assert!(!authority.permits_message(1, 4, MsgTimeoutNow));
}

#[test]
fn expiry_discards_only_the_unpinned_matching_stage_and_preserves_adopted_history() {
    let dir = tempfile::tempdir().unwrap();
    let store = voter(dir.path());
    store.persist_ready(&[], Some(&hard(2, 1, 0))).unwrap();
    let history = store.snapshot_history();
    let old = seal(3, 1, 10);
    let current = seal(4, 1, 11);
    let stage = history.begin(old).unwrap();
    stage.append(&[row(1, 1)]).unwrap();
    assert!(!history.discard_stage(old).unwrap());
    drop(stage);
    assert!(history.discard_stage(current).unwrap());
    assert!(store.db.get(archive_key(&old, 1)).unwrap().is_some());
    assert!(history.discard_stage(old).unwrap());
    assert!(store.db.get(archive_key(&old, 1)).unwrap().is_none());
    assert!(store.db.get(STAGED).unwrap().is_none());

    let rows: Vec<_> = (1..=4).map(|i| row(i, 1)).collect();
    let stage = history.begin(current).unwrap();
    stage.append(&rows).unwrap();
    store
        .persist_snapshot_ready(&snapshot(&store, current), current, &stage, &[], None)
        .unwrap();
    drop(stage);
    assert!(history.discard_stage(current).unwrap());
    assert_eq!(store.retained_entries(1, 5, None).unwrap(), rows);
}

#[test]
fn interrupted_transfer_is_invisible_then_ready_atomically_adopts_only_missing_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("voter");
    let store = voter(&db);
    let canonical: Vec<_> = (1..=8)
        .map(|i| {
            row(
                i,
                if i <= 2 {
                    1
                } else if i <= 6 {
                    2
                } else {
                    3
                },
            )
        })
        .collect();
    let mut fork = canonical[..4].to_vec();
    fork[2].data = b"old suffix".to_vec().into();
    store.persist_ready(&fork, Some(&hard(3, 2, 2))).unwrap();
    let remote = seal(6, 2, 42);
    let local = seal(6, 2, 19);
    let stage = store.snapshot_history().begin(remote).unwrap();
    assert_eq!(stage.resume_after().unwrap(), 2);
    stage.append(&canonical[2..4]).unwrap();
    assert!(stage.finish().is_err());
    assert!(store.snapshot_history().begin(seal(8, 3, 43)).is_err());
    let snap = snapshot(&store, remote);
    assert!(
        store
            .persist_snapshot_ready(&snap, local, &stage, &[], Some(&hard(3, 2, 6)))
            .is_err()
    );
    drop(stage);
    drop(store);
    let store = RaftStore::reopen(&db, b"target").unwrap();
    assert_eq!(store.last_index().unwrap(), 4);
    assert_eq!(store.committed_index().unwrap(), 2);
    assert_eq!(store.checkpoint().unwrap(), None);
    assert_eq!(store.retained_entries(1, 5, None).unwrap(), fork);
    let stage = store.snapshot_history().begin(remote).unwrap();
    assert_eq!(stage.resume_after().unwrap(), 4);
    assert!(stage.append(&[row(5, 0)]).is_err());
    assert!(stage.append(&[row(6, 2)]).is_err());
    stage.append(&canonical[4..6]).unwrap();
    stage.finish().unwrap();
    // Foreground consensus advanced by one since transfer began, replacing the fork.
    store
        .persist_ready(&canonical[2..3], Some(&hard(3, 2, 3)))
        .unwrap();
    assert!(
        store
            .persist_snapshot_ready(&snap, local, &stage, &canonical[6..], Some(&hard(3, 1, 7)))
            .is_err()
    );
    assert!(
        store
            .persist_snapshot_ready(&snap, seal(5, 2, 9), &stage, &[], None)
            .is_err()
    );
    let other = voter(&dir.path().join("other"));
    let swapped = other.snapshot_history().begin(remote).unwrap();
    assert!(
        store
            .persist_snapshot_ready(&snap, local, &swapped, &[], None)
            .is_err()
    );
    assert_eq!(store.committed_index().unwrap(), 3);
    assert_eq!(store.checkpoint().unwrap(), None);
    store
        .persist_snapshot_ready(&snap, local, &stage, &canonical[6..], Some(&hard(3, 2, 7)))
        .unwrap();
    drop(stage);
    drop(store);
    let store = RaftStore::reopen(&db, b"target").unwrap();
    assert_eq!(store.first_index().unwrap(), 7);
    assert_eq!(store.term(6).unwrap(), 2);
    assert!(matches!(
        store.term(5),
        Err(raft::Error::Store(StorageError::Compacted))
    ));
    assert!(matches!(
        store.entries(6, 7, None, GetEntriesContext::empty(false)),
        Err(raft::Error::Store(StorageError::Compacted))
    ));
    assert_eq!(store.retained_entries(1, 9, None).unwrap(), canonical);
    assert_eq!(store.initial_state().unwrap().hard_state, hard(3, 2, 7));
    assert_eq!(
        CheckpointRef::from_snapshot(&store.snapshot(6, 3).unwrap()).unwrap(),
        local
    );
    drop(store);
    let repaired = RaftStore::reopen_from_genesis(&db, b"target").unwrap();
    assert_eq!(repaired.first_index().unwrap(), 1);
    assert_eq!(repaired.retained_entries(1, 9, None).unwrap(), canonical);
    repaired.clear_checkpoint().unwrap();
    drop(repaired);
    assert_eq!(
        RaftStore::reopen(&db, b"target")
            .unwrap()
            .entries(1, 9, None, GetEntriesContext::empty(false))
            .unwrap(),
        canonical
    );
}

#[test]
fn successive_remote_seals_retain_earlier_ranges_for_export_and_genesis_audit() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("store");
    let store = voter(&db);
    let rows: Vec<_> = (1..=20).map(|i| row(i, 1)).collect();
    store.persist_ready(&[], Some(&hard(2, 1, 0))).unwrap();
    for (a, b) in [(0usize, 8usize), (8, 15)] {
        let remote = seal(b as u64, 1, b as u8);
        let stage = store.snapshot_history().begin(remote).unwrap();
        stage.append(&rows[a..b]).unwrap();
        store
            .persist_snapshot_ready(&snapshot(&store, remote), remote, &stage, &[], None)
            .unwrap();
    }
    store
        .persist_ready(&rows[15..], Some(&hard(2, 1, 20)))
        .unwrap();
    let tip = seal(20, 1, 80);
    store.publish_checkpoint(tip).unwrap();
    store.compact_replication(18).unwrap();
    store.compact_replication(12).unwrap();
    assert_eq!(store.first_index().unwrap(), 19);
    assert!(store.compact_replication(21).is_err());
    assert_eq!(
        store.snapshot_history().entries_after(tip, 0).unwrap(),
        rows
    );
    let source = crate::CommittedSource(store.clone(), 7);
    source.verify(&rows).unwrap();
    let proof = ledger_recovery::Head {
        cluster: 7,
        index: 12,
        term: 1,
        digest: [5; 32],
        fenced: false,
    };
    source.save_journal_anchor(None, &proof).unwrap();
    assert_eq!(source.journal_anchor().unwrap(), Some(proof));
    drop(source);
    // An ordinary restart only needs the verified latest checkpoint + suffix;
    // scrub of the retained Raft bytes and full repair must expose old damage.
    store.db.delete(archive_key(&seal(8, 1, 8), 3)).unwrap();
    drop(store);
    let latest = RaftStore::reopen(&db, b"target").unwrap();
    assert!(latest.retained_entries(1, 21, None).is_err());
    drop(latest);
    assert!(RaftStore::reopen_from_genesis(&db, b"target").is_err());
}

#[test]
fn corrupt_logical_replication_floor_is_repairable_without_losing_log_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("voter");
    let store = voter(&db);
    store
        .persist_ready(&[row(1, 1), row(2, 1)], Some(&hard(1, 1, 2)))
        .unwrap();
    store.publish_checkpoint(seal(2, 1, 1)).unwrap();
    store.compact_replication(1).unwrap();
    store.db.put(COMPACTED, b"torn").unwrap();
    drop(store);
    assert!(RaftStore::reopen(&db, b"target").is_err());
    let audit = RaftStore::reopen_from_genesis(&db, b"target").unwrap();
    assert_eq!(
        audit.retained_entries(1, 3, None).unwrap(),
        [row(1, 1), row(2, 1)]
    );
    audit.clear_checkpoint().unwrap();
    drop(audit);
    assert_eq!(
        RaftStore::reopen(&db, b"target")
            .unwrap()
            .first_index()
            .unwrap(),
        1
    );
}

#[test]
fn replay_proof_covers_local_gap_resumed_history_and_source_identity() {
    let dir = tempfile::tempdir().unwrap();
    let source = voter(&dir.path().join("source"));
    let db = dir.path().join("receiver");
    let target = seal(7, 2, 55);
    let base = seal(2, 1, 9);
    let rows: Vec<_> = (1..=7)
        .map(|i| row(i, if i <= 3 { 1 } else { 2 }))
        .collect();
    source.persist_ready(&rows, Some(&hard(2, 1, 7))).unwrap();
    let history = source.snapshot_history();
    let proof = history.replay_proof(target, base).unwrap().unwrap();
    assert_eq!(
        history.replay_proof(target, seal(2, 1, 40)).unwrap(),
        Some(proof)
    );
    assert_ne!(
        history.replay_proof(seal(7, 2, 40), base).unwrap(),
        Some(proof)
    );
    assert!(history.replay_proof(target, seal(4, 1, 0)).is_err());
    assert!(history.replay_proof(seal(7, 1, 0), base).is_err());
    assert!(history.replay_proof(target, seal(7, 1, 0)).is_err());
    let receiver = voter(&db);
    receiver
        .persist_ready(&rows[..4], Some(&hard(2, 1, 4)))
        .unwrap();
    let stage = receiver.snapshot_history().begin(target).unwrap();
    stage.append(&rows[4..6]).unwrap();
    assert!(stage.replay_verified(base, proof, |_| Ok(())).is_err());
    drop(stage);
    drop(receiver);
    let receiver = RaftStore::reopen(&db, b"target").unwrap();
    let stage = receiver.snapshot_history().begin(target).unwrap();
    stage.append(&rows[6..]).unwrap();
    let mut applied = vec![];
    assert!(
        stage
            .replay_verified(base, proof, |entry| {
                applied.push(entry.clone());
                Ok(())
            })
            .unwrap()
    );
    assert_eq!(applied, rows[2..]);
    assert_eq!(receiver.committed_index().unwrap(), 4);
    assert!(
        stage
            .replay_verified(base, proof, |_| Err(invalid("private apply failed")))
            .is_err()
    );
    assert_eq!(stage.resume_after().unwrap(), target.index);
    let same = history
        .replay_proof(target, seal(7, 2, 0))
        .unwrap()
        .unwrap();
    let local = source.snapshot_history().begin(target).unwrap();
    assert!(
        local
            .replay_verified(target, same, |_| panic!("base was already applied"))
            .unwrap()
    );
}

#[test]
fn replay_mismatch_invalidates_only_the_unadopted_exact_stage() {
    let dir = tempfile::tempdir().unwrap();
    let source = voter(&dir.path().join("source"));
    let receiver = voter(&dir.path().join("target"));
    let rows: Vec<_> = (1..=4).map(|i| row(i, 1)).collect();
    source.persist_ready(&rows, Some(&hard(1, 1, 4))).unwrap();
    receiver
        .persist_ready(&rows[..2], Some(&hard(1, 1, 2)))
        .unwrap();
    let target = seal(4, 1, 15);
    let base = seal(1, 1, 22);
    let proof = source
        .snapshot_history()
        .replay_proof(target, base)
        .unwrap()
        .unwrap();
    let history = receiver.snapshot_history();
    let old = history.begin(target).unwrap();
    let mut wrong = rows[2..].to_vec();
    wrong[0].data = b"other command".to_vec().into();
    old.append(&wrong).unwrap();
    assert!(old.replay_verified(base, proof, |_| Ok(())).is_err());
    assert_eq!(receiver.committed_index().unwrap(), 2);
    assert!(receiver.db.get(archive_key(&target, 3)).unwrap().is_none());
    let fresh = history.begin(target).unwrap();
    assert_eq!(fresh.resume_after().unwrap(), 2);
    assert!(old.append(&wrong).is_err());
    fresh.append(&rows[2..]).unwrap();
    assert!(fresh.replay_verified(base, proof, |_| Ok(())).unwrap());
    receiver
        .persist_snapshot_ready(&snapshot(&receiver, target), target, &fresh, &[], None)
        .unwrap();
    assert!(fresh.replay_verified(base, proof, |_| Ok(())).is_err());
    assert_eq!(receiver.retained_entries(1, 5, None).unwrap(), rows);
    let again = history.begin(target).unwrap();
    assert!(fresh.finish().is_err());
    assert!(again.finish().is_ok());
}

#[test]
fn proof_hashes_original_protobuf_order_and_rejects_conflicting_committed_overlap() {
    let dir = tempfile::tempdir().unwrap();
    let source = voter(&dir.path().join("source"));
    let receiver = voter(&dir.path().join("receiver"));
    let rows: Vec<_> = (1..=4).map(|i| row(i, 1)).collect();
    source.persist_ready(&rows, Some(&hard(1, 1, 4))).unwrap();
    receiver
        .persist_ready(&rows[..2], Some(&hard(1, 1, 2)))
        .unwrap();
    let target = seal(4, 1, 33);
    let base = seal(1, 1, 42);
    let origin = source.snapshot_history();
    let canonical = origin.replay_proof(target, base).unwrap().unwrap();
    // Same protobuf with index before term. Both stored bytestrings are valid.
    let mut wire = vec![0x18, 1, 0x10, 1, 0x22, 8];
    wire.extend(1u64.to_be_bytes());
    assert_eq!(decode_entry(1, &wire).unwrap(), rows[0]);
    source.db.put(entry_key(1), wire).unwrap();
    assert_ne!(origin.replay_proof(target, base).unwrap(), Some(canonical));
    let stage = receiver.snapshot_history().begin(target).unwrap();
    stage.append(&rows[2..]).unwrap();
    let mut conflicting = rows[2..3].to_vec();
    conflicting[0].context = b"different".to_vec().into();
    receiver
        .persist_ready(&conflicting, Some(&hard(1, 1, 3)))
        .unwrap();
    assert!(stage.replay_verified(base, canonical, |_| Ok(())).is_err());
    assert_eq!(receiver.retained_entries(3, 4, None).unwrap(), conflicting);
}

#[test]
fn replay_budgets_cover_the_base_and_gaps_in_durable_stage_are_detected() {
    assert!(snapshot_replay_candidate(
        seal(1 + MAX_REPLAY_ENTRIES, 2, 4),
        seal(1, 1, 3)
    ));
    assert!(!snapshot_replay_candidate(
        seal(2 + MAX_REPLAY_ENTRIES, 2, 4),
        seal(1, 1, 3)
    ));
    assert!(!snapshot_replay_candidate(
        seal(u64::MAX, 2, 4),
        seal(u64::MAX - 1, 2, 3)
    ));
    let dir = tempfile::tempdir().unwrap();
    let source = voter(&dir.path().join("source"));
    let receiver = voter(&dir.path().join("receiver"));
    let rows: Vec<_> = (1..=33)
        .map(|i| Entry {
            data: vec![99; 1 << 20].into(),
            ..row(i, 1)
        })
        .collect();
    source.persist_ready(&rows, Some(&hard(1, 1, 33))).unwrap();
    // A sender with these bytes would decline replay. Receiving a positive
    // proof for this too-large interval establishes disagreement instead.
    let over = source.snapshot_history().begin(seal(33, 1, 0)).unwrap();
    assert!(
        over.replay_verified(seal(2, 1, 0), [1; 32], |_| panic!("unverified application"))
            .is_err()
    );
    assert!(over.resume_after().is_err());
    assert_eq!(source.committed_index().unwrap(), 33);
    assert!(
        source
            .snapshot_history()
            .replay_proof(seal(33, 1, 0), seal(2, 1, 0))
            .unwrap()
            .is_none()
    );
    let proof = source
        .snapshot_history()
        .replay_proof(seal(4, 1, 0), seal(1, 1, 0))
        .unwrap()
        .unwrap();
    receiver
        .persist_ready(&rows[..1], Some(&hard(1, 1, 1)))
        .unwrap();
    let stage = receiver.snapshot_history().begin(seal(4, 1, 0)).unwrap();
    for entry in &rows[1..4] {
        stage.append(std::slice::from_ref(entry)).unwrap();
    }
    receiver.db.delete(archive_key(&seal(4, 1, 0), 3)).unwrap();
    assert!(
        stage
            .replay_verified(seal(1, 1, 0), proof, |_| Ok(()))
            .is_err()
    );
}

#[test]
fn a_commit_advance_after_successful_replay_checks_interior_rows_before_ready_adopts() {
    let dir = tempfile::tempdir().unwrap();
    let source = voter(&dir.path().join("source"));
    let rows: Vec<_> = (1..=7).map(|i| row(i, 1)).collect();
    source.persist_ready(&rows, Some(&hard(1, 1, 7))).unwrap();
    let target = seal(7, 1, 33);
    let base = seal(1, 1, 21);
    let proof = source
        .snapshot_history()
        .replay_proof(target, base)
        .unwrap()
        .unwrap();
    for conflict in [true, false] {
        let receiver = voter(&dir.path().join(format!("receiver-{conflict}")));
        receiver
            .persist_ready(&rows[..2], Some(&hard(1, 1, 2)))
            .unwrap();
        let stage = receiver.snapshot_history().begin(target).unwrap();
        stage.append(&rows[2..]).unwrap();
        assert!(stage.replay_verified(base, proof, |_| Ok(())).unwrap());
        let mut committed = rows[2..6].to_vec();
        if conflict {
            committed[1].data = b"interior row changed".to_vec().into();
        }
        receiver
            .persist_ready(&committed, Some(&hard(1, 1, 6)))
            .unwrap();
        let result = receiver.persist_snapshot_ready(
            &snapshot(&receiver, target),
            target,
            &stage,
            &[],
            None,
        );
        assert_eq!(result.is_err(), conflict);
        assert_eq!(
            receiver.committed_index().unwrap(),
            if conflict { 6 } else { 7 }
        );
        assert_eq!(receiver.checkpoint().unwrap().is_none(), conflict);
    }
}

#[test]
fn encoded_history_transfer_completes_replay_and_reexports_original_wire_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let sender = voter(&dir.path().join("sender"));
    let receiver = voter(&dir.path().join("receiver"));
    let remote = seal(4, 1, 27);
    let base = seal(1, 1, 9);
    let rows: Vec<_> = (1..=4).map(|i| row(i, 1)).collect();
    sender.persist_ready(&rows, Some(&hard(1, 1, 4))).unwrap();
    receiver
        .persist_ready(&rows[..2], Some(&hard(1, 1, 2)))
        .unwrap();
    let mut legacy = vec![0x18, 3, 0x10, 1, 0x22, 8];
    legacy.extend(3u64.to_be_bytes());
    assert_eq!(decode_entry(3, &legacy).unwrap(), rows[2]);
    assert_ne!(rows[2].write_to_bytes().unwrap(), legacy);
    sender.db.put(entry_key(3), &legacy).unwrap();
    let source = sender.snapshot_history();
    let page = source.encoded_entries_after(remote, 2).unwrap();
    assert_eq!(page[0], legacy);
    let stage = receiver.snapshot_history().begin(remote).unwrap();
    assert!(stage.append_encoded(&[page[0].clone(), vec![255]]).is_err());
    assert_eq!(stage.resume_after().unwrap(), 2);
    stage.append_encoded(&page).unwrap();
    let proof = source.replay_proof(remote, base).unwrap().unwrap();
    let mut applied = vec![];
    assert!(
        stage
            .replay_verified(base, proof, |e| {
                applied.push(e.clone());
                Ok(())
            })
            .unwrap()
    );
    assert_eq!(applied, rows[1..]);
    receiver
        .persist_snapshot_ready(
            &snapshot(&receiver, remote),
            seal(4, 1, 29),
            &stage,
            &[],
            None,
        )
        .unwrap();
    assert_eq!(
        receiver
            .snapshot_history()
            .encoded_entries_after(remote, 2)
            .unwrap(),
        page
    );
    assert_eq!(receiver.raw_bytes(3, 4).unwrap(), legacy);
}
