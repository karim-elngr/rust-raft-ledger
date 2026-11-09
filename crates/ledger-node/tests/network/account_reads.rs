use ledger_core::{CaptureAmount, Ledger, Remainder, ReservationId};
use ledger_recovery::Journal;

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batched_account_reads_match_one_protected_state_during_writes() {
    let external = TempDir::new().unwrap();
    let remote = external.path().join("journal");
    Journal::init(&remote, &super::recovery::spec(), CLUSTER).unwrap();
    let mut cluster = super::recovery::original_cluster(&remote).await;
    let node = cluster.handle(cluster.leader().await);
    let accounts = [account(2), account(1), account(2), account(53)];
    let snapshot = |reference: &Ledger| {
        accounts
            .iter()
            .map(|id| reference.account(*id).cloned())
            .collect::<Vec<_>>()
    };
    let mut reference = Ledger::new(BOOK, "USD", 2);
    let mut states = vec![snapshot(&reference)];
    let mut commands = vec![create(1, Side::Debit), create(2, Side::Credit)];
    let funding = || vec![Line::debit(account(1), 8), Line::credit(account(2), 8)];
    for turn in 1..=8 {
        let reservation = ReservationId(turn);
        commands.extend([
            post(funding()),
            Command::Reserve {
                reservation,
                funding: Funding::Explicit(funding()),
                conditions: vec![],
            },
            Command::SetAccountBounds {
                account: account(2),
                bounds: Bounds {
                    min: None,
                    max: Some(Balance::positive(1000 + turn)),
                },
            },
            Command::Capture {
                reservation,
                amount: CaptureAmount::Amount(3),
                remainder: Some(Remainder::KeepReserved),
            },
            Command::Void { reservation },
        ]);
    }
    let mut writes = Vec::new();
    for (position, command) in commands.into_iter().enumerate() {
        let id = RequestId(position as u128 + 1);
        let expected = reference.apply(id, command.clone()).unwrap();
        writes.push((id, command, expected));
        states.push(snapshot(&reference));
    }
    let read = || node.protected_accounts(accounts.to_vec(), Instant::now() + WAIT);
    assert_eq!(read().await.unwrap(), states[0]);
    let ((), ()) = tokio::join!(
        async {
            for (id, command, expected) in writes {
                let decision = node
                    .submit_protected(id, command, Instant::now() + WAIT)
                    .await
                    .unwrap();
                assert_eq!(decision.unwrap(), expected);
            }
        },
        async {
            for _ in 0..50 {
                let seen = read().await.unwrap();
                assert!(
                    states.contains(&seen),
                    "read mixed accounts from different committed states"
                );
            }
        }
    );
    assert_eq!(&read().await.unwrap(), states.last().unwrap());
    assert!(node.status().await.unwrap().account_cache.hits > 0);
    for id in 1..=3 {
        cluster.stop(id).await;
    }
}
