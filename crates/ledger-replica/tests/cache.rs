#[path = "../examples/support/mod.rs"]
mod support;

use ledger_core::{
    AccountId, Balance, Bounds, CaptureAmount, Command, EntryId, Funding, Ledger, Line, Remainder,
    RequestId, ReservationId, Side,
};
use ledger_replica::{CacheBudget, Replica};
use support::{LEDGER, commit, config, create, elect};
use tempfile::tempdir;

const HOLD: ReservationId = ReservationId(17);

fn account(number: u128) -> AccountId {
    AccountId::new(LEDGER, number)
}

fn workload() -> Vec<Vec<(RequestId, Command)>> {
    let transfer = |from, to, amount| {
        Funding::Explicit(vec![
            Line::debit(account(from), amount),
            Line::credit(account(to), amount),
        ])
    };
    let too_low = (
        RequestId(7),
        Command::SetAccountBounds {
            account: account(3),
            bounds: Bounds {
                min: None,
                max: Some(Balance::positive(69)),
            },
        },
    );
    let capture = (
        RequestId(8),
        Command::Capture {
            reservation: HOLD,
            amount: CaptureAmount::Amount(30),
            remainder: Some(Remainder::KeepReserved),
        },
    );
    vec![
        vec![
            create(1, Side::Debit),
            create(2, Side::Credit),
            create(3, Side::Credit),
            (
                RequestId(5),
                Command::Post {
                    funding: transfer(1, 2, 100),
                    conditions: vec![],
                    corrects: None,
                },
            ),
        ],
        vec![
            (
                RequestId(6),
                Command::Reserve {
                    reservation: HOLD,
                    funding: transfer(2, 3, 70),
                    conditions: vec![],
                },
            ),
            too_low.clone(),
            capture.clone(),
            capture,
            (RequestId(8), Command::Void { reservation: HOLD }), // same ID, different command
        ],
        vec![
            (RequestId(9), Command::Void { reservation: HOLD }),
            create(4, Side::Credit),
            (
                RequestId(10),
                Command::Post {
                    funding: transfer(2, 4, 10),
                    conditions: vec![],
                    corrects: Some(EntryId(2)),
                },
            ),
            too_low, // this bound now fits, but the original rejection remains binding
        ],
    ]
}

fn compare(voter: &Replica, expected: &Ledger) {
    for id in 1..=4 {
        let key = account(id);
        assert_eq!(voter.account(key).unwrap().as_ref(), expected.account(key));
        assert_eq!(
            voter.stored_counters(key).unwrap(),
            expected.account(key).map(|a| a.counters())
        );
        // Read the same key again so even the one-slot mode should hit.
        assert_eq!(voter.account(key).unwrap().as_ref(), expected.account(key));
    }
    for id in 1..=4 {
        let id = EntryId(id);
        assert_eq!(voter.entry(id).unwrap().as_ref(), expected.entry(id));
    }
    assert_eq!(
        voter.reservation(HOLD).unwrap().as_ref(),
        expected.reservation(HOLD)
    );
    for id in 1..=10 {
        let request = RequestId(id);
        let decision = expected.decisions().iter().find(|d| d.request == request);
        assert_eq!(voter.decision(request).unwrap().as_ref(), decision);
    }
}

#[test]
fn published_rows_match_the_ordered_ledger_when_disabled_warm_evicted_and_reopened() {
    for account_bytes in [0, 256, 16 << 10] {
        let storage = tempdir().unwrap();
        let budget = CacheBudget {
            account_bytes,
            block_bytes: (1 << 20) - account_bytes,
        };
        let mut voter = Replica::bootstrap_with_cache(storage.path(), config(), budget).unwrap();
        elect(&mut voter);
        let mut reference = Ledger::new(LEDGER, "USD", 2);
        compare(&voter, &reference);
        for batch in workload() {
            let _ = reference.apply_batch(batch.clone());
            commit(&mut voter, &batch);
            compare(&voter, &reference);
            for (request, command) in &batch {
                assert_eq!(
                    voter.outcome_for(*request, command).unwrap(),
                    reference.outcome_for(*request, command)
                );
            }
        }
        let stats = voter.cache_stats();
        assert!(stats.rocksdb_reads > 0);
        assert!(stats.allocated_bytes <= stats.max_bytes);
        if account_bytes == 0 {
            assert_eq!(stats.resident_accounts, 0);
            assert_eq!(stats.hits, 0);
        } else {
            assert!(stats.hits > 0);
            if account_bytes == 256 {
                assert_eq!(stats.slot_capacity, 1);
                assert!(stats.evictions > 0);
            }
        }
        let index = voter.applied_index();
        drop(voter);
        let voter = Replica::reopen_with_cache(storage.path(), config(), budget).unwrap();
        assert_eq!(voter.applied_index(), index);
        let cold = voter.cache_stats();
        assert_eq!((cold.hits, cold.misses, cold.resident_accounts), (0, 0, 0));
        compare(&voter, &reference);
        assert_eq!(voter.ledger().unwrap(), &reference);
    }
}
