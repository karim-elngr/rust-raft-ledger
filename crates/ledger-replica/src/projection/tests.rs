use super::*;
use crate::codec::encode_batch;
use ledger_core::{
    Balance, Bounds, CaptureAmount, Command, Funding, LedgerId, Line, Remainder, RequestId,
    ReservationId, Side,
};
use std::fs;
use tempfile::tempdir;

const BOOK: LedgerId = LedgerId(1);

fn genesis() -> Ledger {
    Ledger::new(BOOK, "USD", 2)
}

fn entry(index: u64, commands: &[(RequestId, Command)]) -> Entry {
    Entry {
        index,
        term: 1,
        data: encode_batch(BOOK, commands).unwrap().into(),
        ..Entry::default()
    }
}

#[test]
fn materializes_first_decisions_and_all_accounts_released_by_partial_capture() {
    let dir = tempdir().unwrap();
    let mut state = Projection::reset(dir.path(), genesis()).unwrap();
    let ids = [1, 2, 3].map(|i| AccountId::new(BOOK, i));
    let create: Vec<_> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            (
                RequestId(i as u128 + 1),
                Command::CreateAccount {
                    account: *id,
                    code: 7,
                    normal: Side::Credit,
                    bounds: Bounds {
                        min: Some(Balance::negative(10)),
                        max: Some(Balance::positive(u128::MAX)),
                    },
                },
            )
        })
        .collect();
    state.apply(&entry(1, &create)).unwrap();
    let hold = ReservationId(4);
    let reserve = (
        RequestId(4),
        Command::Reserve {
            reservation: hold,
            funding: Funding::Explicit(vec![
                Line::debit(ids[0], 10),
                Line::credit(ids[1], 6),
                Line::credit(ids[2], 4),
            ]),
            conditions: vec![],
        },
    );
    state.apply(&entry(2, &[reserve.clone(), reserve])).unwrap();
    let capture = (
        RequestId(5),
        Command::Capture {
            reservation: hold,
            amount: CaptureAmount::Lines(vec![
                ledger_core::Allocation { line: 0, amount: 6 },
                ledger_core::Allocation { line: 1, amount: 6 },
            ]),
            remainder: Some(Remainder::Release),
        },
    );
    let rejected = (RequestId(6), Command::Void { reservation: hold });
    state
        .apply(&entry(
            3,
            &[capture.clone(), capture, rejected.clone(), rejected],
        ))
        .unwrap();
    assert_eq!(state.applied_index(), 3);
    let reconstructed = state.inspect_ledger(genesis()).unwrap();
    for id in ids {
        assert_eq!(
            state.account(id).unwrap().as_ref(),
            reconstructed.account(id)
        );
        assert_eq!(
            state.stored_counters(id).unwrap(),
            Some(reconstructed.account(id).unwrap().counters())
        );
    }
    // The third leg has no posting in the capture; releasing its pending credit still changes it.
    assert_eq!(
        state.stored_counters(ids[2]).unwrap(),
        Some(Counters::default())
    );
    assert_eq!(
        state.reservation(hold).unwrap().as_ref(),
        reconstructed.reservation(hold)
    );
    assert_eq!(reconstructed.decisions().len(), 6);
    assert_eq!(
        state.decision(RequestId(6)).unwrap().as_ref(),
        Some(&reconstructed.decisions()[5])
    );
    assert_eq!(
        state.position(REQUESTS, &6u128.to_be_bytes()).unwrap(),
        Some(position(3, 2))
    );
    assert_eq!(
        state.entry(EntryId(1)).unwrap().as_ref(),
        reconstructed.entry(EntryId(1))
    );
    assert_eq!(
        state.db.get(APPLIED).unwrap().as_deref(),
        Some(&3u64.to_be_bytes()[..])
    );

    let noop = Entry {
        index: 4,
        term: 1,
        ..Entry::default()
    };
    state.apply(&noop).unwrap();
    assert_eq!(state.applied_index(), 4);
    assert!(state.apply(&noop).is_err());
    drop(state);
    let clean = Projection::reset(dir.path(), genesis()).unwrap();
    assert_eq!(clean.applied_index(), 0);
    assert_eq!(clean.stored_counters(ids[0]).unwrap(), None);
}

#[test]
fn refuses_unknown_contents_and_symlink_targets() {
    let dir = tempdir().unwrap();
    let sentinel = dir.path().join("proof");
    std::fs::write(&sentinel, "keep").unwrap();
    assert!(Projection::reset(dir.path(), genesis()).is_err());
    #[cfg(unix)]
    {
        let other = tempdir().unwrap();
        let link = other.path().join("projection");
        std::os::unix::fs::symlink(dir.path(), &link).unwrap();
        assert!(Projection::reset(&link, genesis()).is_err());
    }
    assert_eq!(std::fs::read(sentinel).unwrap(), b"keep");
}

#[test]
fn dangling_or_mismatched_indexes_are_storage_errors() {
    let dir = tempdir().unwrap();
    let mut state = Projection::reset(dir.path(), genesis()).unwrap();
    let id = AccountId::new(BOOK, 5);
    let commands = [
        (
            RequestId(4),
            Command::CreateAccount {
                account: id,
                normal: Side::Credit,
                code: 7,
                bounds: Bounds::default(),
            },
        ),
        (
            RequestId(9),
            Command::Void {
                reservation: ReservationId(7),
            },
        ),
    ];
    state.apply(&entry(1, &commands)).unwrap();
    let key = 4u128.to_be_bytes();
    state
        .db
        .put_cf(state.cf(REQUESTS), key, position(1, 1))
        .unwrap();
    assert!(state.decision(RequestId(4)).is_err());
    state
        .db
        .put_cf(state.cf(REQUESTS), key, position(1, 77))
        .unwrap();
    assert!(state.decision(RequestId(4)).is_err());
    assert!(state.inspect_ledger(genesis()).is_err());
    assert!(state.apply(&entry(2, &commands)).is_err());
    assert_eq!(state.applied_index(), 1);
}

#[test]
fn failed_publication_preserves_cache_history_and_index() {
    let dir = tempdir().unwrap();
    let mut state = Projection::reset(dir.path(), genesis()).unwrap();
    let account = AccountId::new(BOOK, 3);
    let open = (
        RequestId(8),
        Command::CreateAccount {
            account,
            code: 3,
            normal: Side::Debit,
            bounds: Bounds::default(),
        },
    );
    state.apply(&entry(1, &[open])).unwrap();
    let before = state.account(account).unwrap().unwrap();
    let change = (
        RequestId(9),
        Command::SetAccountBounds {
            account,
            bounds: Bounds {
                min: Some(Balance::negative(2)),
                max: None,
            },
        },
    );
    let second = entry(2, &[change]);
    state.fail_next_write = true;
    assert!(state.apply(&second).is_err());
    assert_eq!(state.applied_index(), 1);
    assert_eq!(state.account(account).unwrap(), Some(before.clone()));
    assert_eq!(state.decision(RequestId(9)).unwrap(), None);
    assert_eq!(
        state.inspect_ledger(genesis()).unwrap().decisions().len(),
        1
    );
    state.apply(&second).unwrap();
    assert_ne!(state.account(account).unwrap(), Some(before));
    assert_eq!(
        state.inspect_ledger(genesis()).unwrap().decisions().len(),
        2
    );
}

#[test]
fn reopening_after_db_write_before_cache_publication_replays_raft_and_starts_cold() {
    use crate::{Replica, ReplicaConfig, raft_store::RaftStore};
    use raft::eraftpb::HardState;

    let dir = tempdir().unwrap();
    let mut config = ReplicaConfig {
        ledger: BOOK,
        asset: "USD".into(),
        scale: 2,
        cluster: 8,
        node: 1,
        voters: vec![1, 2, 3],
    };
    let identity = config.identity().unwrap();
    let store = RaftStore::bootstrap(&dir.path().join("raft"), &identity, &config.voters).unwrap();
    let id = AccountId::new(BOOK, 7);
    let entries = [
        entry(
            1,
            &[(
                RequestId(1),
                Command::CreateAccount {
                    account: id,
                    code: 4,
                    normal: Side::Debit,
                    bounds: Bounds::default(),
                },
            )],
        ),
        entry(
            2,
            &[(
                RequestId(2),
                Command::SetAccountBounds {
                    account: id,
                    bounds: Bounds {
                        min: Some(Balance::negative(9)),
                        max: None,
                    },
                },
            )],
        ),
    ];
    store
        .persist_ready(
            &entries,
            Some(&HardState {
                term: 1,
                commit: 2,
                ..Default::default()
            }),
        )
        .unwrap();
    let mut state = Projection::reset(&dir.path().join("state"), genesis()).unwrap();
    state.apply(&entries[0]).unwrap();
    state.account(id).unwrap().unwrap();
    state.stop_after_write = true;
    assert!(state.apply(&entries[1]).is_err());
    // The atomic storage batch already contains index 2 and its financial rows.
    assert_eq!(
        state.db.get(APPLIED).unwrap().as_deref(),
        Some(&2u64.to_be_bytes()[..])
    );
    assert_eq!(state.applied_index(), 1);
    state.flush().unwrap();
    drop(state);
    drop(store);

    let replica = Replica::reopen(dir.path(), config).unwrap();
    assert_eq!(replica.applied_index(), 2);
    let stats = replica.cache_stats();
    assert_eq!(
        (stats.resident_accounts, stats.hits, stats.misses),
        (0, 0, 0)
    );
    assert_eq!(
        replica.account(id).unwrap().unwrap().bounds().min,
        Some(Balance::negative(9))
    );
    assert!(
        matches!(replica.decision(RequestId(2)).unwrap().unwrap().outcome,Ok(Outcome::BoundsChanged(account)) if account==id)
    );
    assert_eq!(replica.cache_stats().rocksdb_reads, 1);
}

#[test]
fn same_batch_corrections_and_retries_use_the_first_immutable_decision() {
    let dir = tempdir().unwrap();
    let mut state = Projection::reset_with_cache(dir.path(), genesis(), 0, 0).unwrap();
    let a = AccountId::new(BOOK, 1);
    let b = AccountId::new(BOOK, 2);
    let create = |id| Command::CreateAccount {
        account: id,
        code: 1,
        normal: Side::Credit,
        bounds: Bounds::default(),
    };
    let payment = |n, corrects| Command::Post {
        funding: Funding::Explicit(vec![Line::debit(a, n), Line::credit(b, n)]),
        conditions: vec![],
        corrects,
    };
    let batch = [
        (RequestId(1), create(a)),
        (RequestId(2), create(b)),
        (RequestId(3), payment(4, None)),
        (RequestId(3), payment(18, None)),
        (RequestId(4), payment(1, Some(EntryId(1)))),
        (RequestId(5), payment(1, Some(EntryId(87)))),
    ];
    state.apply(&entry(1, &batch)).unwrap();
    state
        .apply(&entry(
            2,
            &[batch[3].clone(), batch[2].clone(), batch[5].clone()],
        ))
        .unwrap();
    let mut expected = genesis();
    expected.apply_batch(batch);
    assert_eq!(state.inspect_ledger(genesis()).unwrap(), expected);
    assert_eq!(state.last_entry, EntryId(2));
    assert_eq!(
        state.entry(EntryId(2)).unwrap().unwrap().corrects,
        Some(EntryId(1))
    );
    assert_eq!(
        state.position(ENTRIES, &2u64.to_be_bytes()).unwrap(),
        Some(position(1, 4))
    );
    assert_eq!(
        state.position(REQUESTS, &3u128.to_be_bytes()).unwrap(),
        Some(position(1, 2))
    );
    assert_eq!(
        state.decision(RequestId(5)).unwrap().as_ref(),
        Some(&expected.decisions()[4])
    );

    state
        .db
        .put_cf(state.cf(ENTRIES), 2u64.to_be_bytes(), position(1, 2))
        .unwrap();
    assert!(state.entry(EntryId(2)).is_err());
    state
        .db
        .delete_cf(state.cf(ENTRIES), 2u64.to_be_bytes())
        .unwrap();
    assert!(state.entry(EntryId(2)).is_err());
    assert_eq!(state.entry(EntryId(3)).unwrap(), None);
    state
        .db
        .delete_cf(state.cf(HISTORY), position(1, 2))
        .unwrap();
    assert!(state.entry(EntryId(1)).is_err());
    assert!(state.decision(RequestId(3)).is_err());
}

fn snapshot_fixture(path: &Path, seal: &Path) -> Projection {
    let mut db = Projection::reset(path, genesis()).unwrap();
    let accounts = [7, 9].map(|i| AccountId::new(BOOK, i));
    let mut commands: Vec<_> = accounts
        .iter()
        .enumerate()
        .map(|(n, id)| {
            (
                RequestId(11 + n as u128),
                Command::CreateAccount {
                    account: *id,
                    code: 3,
                    normal: Side::Debit,
                    bounds: Bounds::default(),
                },
            )
        })
        .collect();
    commands.push((
        RequestId(13),
        Command::Post {
            funding: Funding::Explicit(vec![
                Line::debit(accounts[0], 3),
                Line::credit(accounts[1], 3),
            ]),
            conditions: vec![],
            corrects: None,
        },
    ));
    db.apply(&entry(1, &commands)).unwrap();
    db.capture(seal).unwrap();
    db
}

fn clone_snapshot(source: &Path, destination: &Path) -> Result<(), ReplicaError> {
    assert!(fs::read_dir(destination)?.next().is_none());
    for item in fs::read_dir(source)? {
        let item = item?;
        let path = item.path();
        let out = destination.join(item.file_name());
        if path.extension().is_some_and(|e| e == "sst" || e == "blob") {
            fs::hard_link(path, out)?;
        } else {
            fs::copy(path, out)?;
        }
    }
    Ok(())
}

#[test]
fn capture_and_install_share_ssts_with_private_writable_metadata() {
    use crate::CacheBudget;
    let root = tempdir().unwrap();
    let seal = root.path().join("sealed");
    let live = root.path().join("live");
    let source = snapshot_fixture(&live, &seal);
    assert!(source.capture(&seal).is_err());
    let image = || -> Vec<_> {
        let mut files: Vec<_> = fs::read_dir(&seal)
            .unwrap()
            .map(|i| {
                let item = i.unwrap();
                (item.file_name(), fs::read(item.path()).unwrap())
            })
            .collect();
        files.sort();
        files
    };
    let initial = image();
    let target = root.path().join("restored");
    let mut restored =
        Projection::from_checkpoint(&target, BOOK, 1, CacheBudget::default(), |dir| {
            clone_snapshot(&seal, dir)
        })
        .unwrap();
    assert_eq!(restored.applied_index(), 1);
    assert_eq!(restored.cache_stats().resident_accounts, 0);
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let sst = fs::read_dir(&seal)
            .unwrap()
            .map(|f| f.unwrap().path())
            .find(|p| p.extension().is_some_and(|e| e == "sst"))
            .unwrap();
        assert_eq!(
            sst.metadata().unwrap().ino(),
            target
                .join("data")
                .join(sst.file_name().unwrap())
                .metadata()
                .unwrap()
                .ino()
        );
        assert_ne!(
            seal.join("CURRENT").metadata().unwrap().ino(),
            target.join("data/CURRENT").metadata().unwrap().ino()
        );
    }
    let id = AccountId::new(BOOK, 7);
    restored
        .apply(&entry(
            2,
            &[(
                RequestId(2),
                Command::SetAccountBounds {
                    account: id,
                    bounds: Bounds {
                        min: Some(Balance::negative(2)),
                        max: None,
                    },
                },
            )],
        ))
        .unwrap();
    restored.flush().unwrap();
    drop(restored);
    assert_eq!(image(), initial);
    assert_eq!(
        source.account(id).unwrap().unwrap().bounds(),
        Bounds::default()
    );
}

#[test]
fn incomplete_install_can_retry_and_wrong_frontier_or_ledger_fails_closed() {
    use crate::CacheBudget;
    let root = tempdir().unwrap();
    let seal = root.path().join("sealed");
    let _db = snapshot_fixture(&root.path().join("source"), &seal);
    let target = root.path().join("restored");
    let budget = CacheBudget::default();
    assert!(
        Projection::from_checkpoint(&target, BOOK, 1, budget, |dir| {
            fs::write(dir.join("CURRENT"), b"partial")?;
            Err(invalid("simulated interruption"))
        })
        .is_err()
    );
    let db =
        Projection::from_checkpoint(&target, BOOK, 1, budget, |dir| clone_snapshot(&seal, dir))
            .unwrap();
    assert_eq!(db.applied_index(), 1);
    drop(db);
    assert!(
        Projection::from_checkpoint(&target, BOOK, 2, budget, |dir| clone_snapshot(&seal, dir))
            .is_err()
    );
    assert!(
        Projection::from_checkpoint(
            &root.path().join("foreign"),
            LedgerId(99),
            1,
            budget,
            |dir| clone_snapshot(&seal, dir)
        )
        .is_err()
    );
    let marker = target.join(format!(".ledger-projection-v1-{:032x}", BOOK.0));
    fs::write(marker, b"bad").unwrap();
    assert!(
        Projection::from_checkpoint(&target, BOOK, 1, budget, |_| panic!(
            "invalid ownership must prevent callback"
        ))
        .is_err()
    );
    assert!(target.join("data/CURRENT").exists());
}

#[test]
fn refuses_unrecognized_working_files_before_cleanup_or_materialization() {
    use crate::CacheBudget;
    let root = tempdir().unwrap();
    let target = root.path().join("active");
    drop(Projection::reset(&target, genesis()).unwrap());
    fs::write(target.join("data/operator-notes"), b"keep").unwrap();
    let current = fs::read(target.join("data/CURRENT")).unwrap();
    assert!(
        Projection::from_checkpoint(&target, BOOK, 1, CacheBudget::default(), |_| panic!(
            "unexpected callback"
        ))
        .is_err()
    );
    assert_eq!(fs::read(target.join("data/CURRENT")).unwrap(), current);
    assert_eq!(
        fs::read(target.join("data/operator-notes")).unwrap(),
        b"keep"
    );
}

#[test]
fn damaged_clone_frontier_or_missing_family_never_replaces_prior_data() {
    use crate::CacheBudget;
    let root = tempdir().unwrap();
    let seal = root.path().join("sealed");
    let _source = snapshot_fixture(&root.path().join("source"), &seal);
    let target = root.path().join("restored");
    let prior = Projection::reset(&target, genesis()).unwrap();
    drop(prior);
    let current = target.join("data/CURRENT");
    let original = fs::read(&current).unwrap();
    for damage in ["cf", "applied", "journal", "pointer"] {
        let result = Projection::from_checkpoint(&target, BOOK, 1, CacheBudget::default(), |dir| {
            clone_snapshot(&seal, dir)?;
            let opts = open::options(0);
            let mut db = DB::open_cf_descriptors(&opts, dir, open::families(&opts))?;
            match damage {
                "cf" => db.drop_cf(REQUESTS)?,
                "applied" => db.put(APPLIED, [1])?,
                "journal" => db.put(LAST_ENTRY, 0u64.to_be_bytes())?,
                _ => db.delete_cf(db.cf_handle(HISTORY).unwrap(), position(1, 2))?,
            }
            Ok(())
        });
        assert!(result.is_err(), "{damage}");
        assert_eq!(fs::read(&current).unwrap(), original);
    }
}

#[cfg(unix)]
#[test]
fn a_metadata_hardlink_is_rejected_before_rocksdb_can_modify_the_seal() {
    use crate::CacheBudget;
    let root = tempdir().unwrap();
    let seal = root.path().join("sealed");
    let _source = snapshot_fixture(&root.path().join("source"), &seal);
    let current = fs::read(seal.join("CURRENT")).unwrap();
    let result = Projection::from_checkpoint(
        &root.path().join("restored"),
        BOOK,
        1,
        CacheBudget::default(),
        |dir| {
            for item in fs::read_dir(&seal)? {
                let item = item?;
                fs::hard_link(item.path(), dir.join(item.file_name()))?;
            }
            Ok(())
        },
    );
    assert!(result.is_err());
    assert_eq!(fs::read(seal.join("CURRENT")).unwrap(), current);
}

#[test]
fn capture_requires_a_committed_cut_and_never_cleans_an_existing_staging_name() {
    let dir = tempdir().unwrap();
    let mut source = Projection::reset(&dir.path().join("state"), genesis()).unwrap();
    let seal = dir.path().join("seal");
    assert!(source.capture(&seal).is_err());
    assert!(!seal.exists());
    source
        .apply(&Entry {
            index: 1,
            term: 1,
            ..Entry::default()
        })
        .unwrap();
    let collision = dir.path().join("seal.tmp");
    fs::create_dir(&collision).unwrap();
    fs::write(collision.join("unrelated"), b"keep").unwrap();
    assert!(source.capture(&seal).is_err());
    assert!(!seal.exists());
    assert_eq!(fs::read(collision.join("unrelated")).unwrap(), b"keep");
}

#[test]
fn foreign_ledger_hold_is_a_storage_error_before_advancing_or_recording_a_rejection() {
    let disk = tempdir().unwrap();
    let mut projection = Projection::reset(disk.path(), genesis()).unwrap();
    let id = ReservationId(17);
    let foreign = LedgerId(2);
    let hold = ledger_core::Reservation {
        id,
        lines: vec![
            Line::debit(AccountId::new(foreign, 1), 5),
            Line::credit(AccountId::new(foreign, 2), 5),
        ]
        .into(),
        remaining: vec![5, 5],
        status: ledger_core::ReservationStatus::Open,
    };
    projection
        .db
        .put_cf(
            projection.cf(RESERVATIONS),
            id.0.to_be_bytes(),
            format().serialize(&hold).unwrap(),
        )
        .unwrap();
    assert!(
        projection
            .apply(&entry(
                1,
                &[(RequestId(9), Command::Void { reservation: id })]
            ))
            .is_err()
    );
    assert_eq!(projection.applied_index(), 0);
    assert!(projection.decision(RequestId(9)).unwrap().is_none());
}

#[test]
fn export_resumes_inside_a_microbatch_and_skips_retries_and_empty_entries() {
    use ledger_export::{MAX_EVENTS, Position as Cursor};
    let dir = tempdir().unwrap();
    let mut projection = Projection::reset(dir.path(), genesis()).unwrap();
    let decisions: Vec<_> = (1..=MAX_EVENTS as u128 + 1)
        .map(|id| {
            (
                RequestId(id),
                Command::Void {
                    reservation: ReservationId(id),
                },
            )
        })
        .collect();
    projection.apply(&entry(1, &decisions)).unwrap();
    projection.apply(&entry(2, &decisions[..1])).unwrap();
    projection
        .apply(&Entry {
            index: 3,
            term: 1,
            ..Default::default()
        })
        .unwrap();
    let first = projection.event_page(Cursor::after_entry(0), 3).unwrap();
    assert_eq!(first.events.len(), MAX_EVENTS);
    assert_eq!(
        first.through,
        Cursor {
            index: 1,
            slot: MAX_EVENTS as u16 - 1
        }
    );
    let next = projection.event_page(first.through, 3).unwrap();
    assert_eq!(next.events.len(), 1);
    assert_eq!(next.events[0].decision.request, decisions.last().unwrap().0);
    assert!(next.events[0].decision.outcome.is_err());
    assert_eq!(next.through, Cursor::after_entry(3));
    assert!(
        projection
            .event_page(next.through, 3)
            .unwrap()
            .events
            .is_empty()
    );
    assert!(projection.event_page(first.through, 4).is_err());
}
