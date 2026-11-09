//! Restore original journal bytes written by the pre-candidate three-voter build.
//! Keep this fixture immutable across codec and projection changes.
use ledger_core::{
    AccountId, Command, EntryId, Error, LedgerId, Line, Outcome, RequestId, ReservationId,
    ReservationStatus,
};
use ledger_recovery::{Journal, Spec};
use ledger_replica::{Replica, ReplicaConfig};
use std::{fs, path::Path};

#[test]
fn previous_build_preserves_postings_waterfall_hold_and_rejection() {
    let original = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/protected-v1");
    let scratch = tempfile::tempdir().unwrap();
    let archive = scratch.path().join("archive");
    fs::create_dir(&archive).unwrap();
    for item in fs::read_dir(original).unwrap() {
        let item = item.unwrap();
        fs::copy(item.path(), archive.join(item.file_name())).unwrap();
    }
    let ledger = LedgerId(7);
    let account = |id| AccountId::new(ledger, id);
    let spec = Spec {
        ledger: 7,
        asset: "USD".into(),
        scale: 2,
    };
    let old = Journal::open(&archive, &spec).unwrap();
    let head = old.head().unwrap();
    assert!(head.fenced && head.cluster == 42);
    let cfg = ReplicaConfig {
        node: 11,
        cluster: 43,
        voters: vec![11, 12, 13],
        ledger,
        asset: spec.asset,
        scale: 2,
    };
    let path = scratch.path().join("voter");
    drop(Replica::restore(&path, cfg.clone(), &old, &head).unwrap());
    let voter = Replica::reopen(&path, cfg).unwrap();
    let held = voter.reservation(ReservationId(37)).unwrap().unwrap();
    assert_eq!(held.status, ReservationStatus::Open);
    assert_eq!(held.remaining, [0, 2000, 1940, 60]);
    assert_eq!(
        held.lines.as_ref(),
        [
            Line::debit(account(2), 3000),
            Line::debit(account(3), 5000),
            Line::credit(account(4), 7760),
            Line::credit(account(5), 240)
        ]
    );
    let reserved = voter.decision(RequestId(7)).unwrap().unwrap();
    assert!(
        matches!(&reserved.outcome, Ok(Outcome::Reserved { lines, .. }) if lines == &held.lines)
    );
    let posted = voter.entry(EntryId(2)).unwrap().unwrap();
    assert_eq!(
        posted.lines.as_ref(),
        [
            Line::debit(account(2), 3000),
            Line::debit(account(3), 3000),
            Line::credit(account(4), 5820),
            Line::credit(account(5), 180)
        ]
    );
    let capture = voter.decision(RequestId(8)).unwrap().unwrap();
    assert!(matches!(capture.outcome, Ok(Outcome::Captured { entry, .. }) if entry == posted));
    let declined = voter.decision(RequestId(9)).unwrap().unwrap();
    assert_eq!(declined.outcome, Err(Error::BelowMinimum(account(3))));
    assert_eq!(
        voter
            .outcome_for(declined.request, &declined.command)
            .unwrap(),
        Some(declined.outcome)
    );
    assert_eq!(
        voter
            .outcome_for(
                RequestId(9),
                &Command::Void {
                    reservation: held.id
                }
            )
            .unwrap(),
        Some(Err(Error::IdConflict(RequestId(9))))
    );
}
