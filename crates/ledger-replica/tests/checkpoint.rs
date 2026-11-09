#[path = "../examples/support/mod.rs"]
mod support;

use ledger_core::{
    AccountId, Bounds, CaptureAmount, Command, EntryId, Error, Funding, Ledger, Line, Remainder,
    RequestId, ReservationId, Side,
};
use ledger_replica::{CacheBudget, Replica};
use support::{LEDGER, commit, config, create, elect};
use tempfile::tempdir;

#[test]
fn a_delayed_checkpoint_worker_seals_the_applied_cut_it_actually_captured() {
    let root = tempdir().unwrap();
    let mut voter = Replica::bootstrap(root.path(), config()).unwrap();
    elect(&mut voter);
    let requested = voter.applied_index();
    let work = voter.prepare_checkpoint().unwrap().unwrap();
    assert!(voter.begin_checkpoint().is_err());
    // Hold all checkpoint disk work until the owner has applied another entry.
    let account = create(77, Side::Credit);
    commit(&mut voter, std::slice::from_ref(&account));
    let cut = work.finish().unwrap();
    assert_eq!(cut.index, requested + 1);
    commit(&mut voter, &[create(78, Side::Debit)]);
    let expected = voter.ledger().unwrap().clone();
    drop(voter);
    let recovered = Replica::reopen(root.path(), config()).unwrap();
    assert_eq!(recovered.recovered_from(), cut.index);
    assert_eq!(recovered.ledger().unwrap(), &expected);
}

#[test]
fn recovered_checkpoint_and_committed_tail_match_full_replay() {
    let dir = tempdir().unwrap();
    let replica_path = dir.path().join("voter");
    let mut voter = Replica::bootstrap(&replica_path, config()).unwrap();
    assert!(voter.begin_checkpoint().is_err());
    elect(&mut voter);
    let [issuer, wallet, shop] = [1, 2, 3].map(|id| AccountId::new(LEDGER, id));
    let funding = |from, to, amount| {
        Funding::Explicit(vec![Line::debit(from, amount), Line::credit(to, amount)])
    };
    let post = |from, to, amount| Command::Post {
        funding: funding(from, to, amount),
        conditions: vec![],
        corrects: None,
    };
    let hold = ReservationId(75);
    let refused = post(wallet, shop, 101);
    let opening = vec![
        create(1, Side::Debit),
        create(2, Side::Credit),
        create(3, Side::Credit),
        (RequestId(4), post(issuer, wallet, 100)),
        (RequestId(5), refused.clone()),
        (
            RequestId(6),
            Command::Reserve {
                reservation: hold,
                funding: funding(wallet, shop, 70),
                conditions: vec![],
            },
        ),
    ];
    let mut expected = Ledger::new(LEDGER, "USD", 2);
    let _ = expected.apply_batch(opening.clone());
    commit(&mut voter, &opening);
    let sealed_at = voter.applied_index();
    let task = voter.begin_checkpoint().unwrap().unwrap();
    assert!(voter.begin_checkpoint().is_err());
    // Only the physical capture required exclusive access to the apply owner.
    // Later committed commands cannot be included in the sealed image.
    let tail = vec![
        (
            RequestId(7),
            Command::Capture {
                reservation: hold,
                amount: CaptureAmount::Amount(30),
                remainder: Some(Remainder::KeepReserved),
            },
        ),
        (RequestId(8), post(issuer, wallet, 120)),
        (RequestId(5), refused.clone()), // Now affordable, still the original rejection.
    ];
    let _ = expected.apply_batch(tail.clone());
    commit(&mut voter, &tail);
    let terminal = voter.applied_index();
    let reference = task.finish().unwrap();
    assert_eq!(reference.index, sealed_at);
    assert!(terminal > sealed_at);
    drop(voter);

    assert_eq!(
        Replica::scrub_stored_checkpoint(&replica_path, config()).unwrap(),
        reference
    );
    let restarted = Replica::reopen(&replica_path, config()).unwrap();
    assert_eq!(restarted.recovered_from(), sealed_at);
    assert_eq!(restarted.applied_index(), terminal);
    assert_eq!(restarted.cache_stats().resident_accounts, 0);
    for id in [issuer, wallet, shop] {
        assert_eq!(
            restarted.account(id).unwrap().as_ref(),
            expected.account(id)
        );
    }
    assert_eq!(
        restarted.reservation(hold).unwrap().unwrap().remaining,
        [40, 40]
    );
    assert_eq!(
        restarted.entry(EntryId(2)).unwrap().as_ref(),
        expected.entry(EntryId(2))
    );
    assert_eq!(
        restarted.outcome_for(RequestId(5), &refused).unwrap(),
        Some(Err(Error::BelowMinimum(wallet)))
    );
    let changed = Command::SetAccountBounds {
        account: wallet,
        bounds: Bounds::default(),
    };
    assert_eq!(
        restarted.outcome_for(RequestId(5), &changed).unwrap(),
        Some(Err(Error::IdConflict(RequestId(5))))
    );
    assert_eq!(restarted.ledger().unwrap(), &expected);

    let newer = restarted
        .begin_checkpoint()
        .unwrap()
        .unwrap()
        .finish()
        .unwrap();
    assert_eq!(newer.index, terminal);
    assert!(restarted.begin_checkpoint().unwrap().is_none());
    drop(restarted);
    let full = Replica::rebuild_from_log(&replica_path, config(), CacheBudget::default()).unwrap();
    assert_eq!(full.recovered_from(), 0);
    assert!(full.current_checkpoint().unwrap().is_none());
    assert_eq!(full.ledger().unwrap(), &expected);
}
