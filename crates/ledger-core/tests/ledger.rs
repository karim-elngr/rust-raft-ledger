use ledger_core::*;

const BOOK: LedgerId = LedgerId(1);
const BANK: AccountId = AccountId::new(BOOK, 1);
const REWARDS: AccountId = AccountId::new(BOOK, 2);
const CASH: AccountId = AccountId::new(BOOK, 3);
const MERCHANT: AccountId = AccountId::new(BOOK, 4);

fn opening_commands() -> Vec<(RequestId, Command)> {
    let mut commands: Vec<_> = [
        (BANK, Side::Debit),
        (REWARDS, Side::Credit),
        (CASH, Side::Credit),
        (MERCHANT, Side::Credit),
    ]
    .into_iter()
    .map(|(account, normal)| {
        (
            RequestId(account.number),
            Command::CreateAccount {
                account,
                code: 1,
                normal,
                bounds: Bounds {
                    min: Some(Balance::positive(0)),
                    max: None,
                },
            },
        )
    })
    .collect();
    commands.push((
        RequestId(5),
        post(vec![
            Line::debit(BANK, 800),
            Line::credit(REWARDS, 300),
            Line::credit(CASH, 500),
        ]),
    ));
    commands
}

fn post(lines: Vec<Line>) -> Command {
    Command::Post {
        funding: Funding::Explicit(lines),
        conditions: vec![],
        corrects: None,
    }
}

fn new_book() -> Ledger {
    Ledger::new(BOOK, "USD", 2)
}

fn sources() -> Vec<Source> {
    [REWARDS, CASH]
        .into_iter()
        .map(|account| Source {
            account,
            floor: SourceFloor::Configured,
            cap: None,
        })
        .collect()
}

fn waterfall(amount: u128) -> Funding {
    Funding::Waterfall(Waterfall {
        amount,
        sources: sources(),
        destinations: vec![Line::credit(MERCHANT, amount)],
        shortfall: Shortfall::RequireFull,
    })
}

#[test]
fn replay_preserves_partial_waterfall_capture_and_sticky_retries() {
    let mut commands = opening_commands();
    let hold = ReservationId(70);
    let capture = Command::Capture {
        reservation: hold,
        amount: CaptureAmount::Lines(vec![
            Allocation {
                line: 0,
                amount: 200,
            },
            Allocation {
                line: 1,
                amount: 200,
            },
            Allocation {
                line: 2,
                amount: 400,
            },
        ]),
        remainder: Some(Remainder::KeepReserved),
    };
    commands.extend([
        (
            RequestId(6),
            Command::Reserve {
                reservation: hold,
                funding: waterfall(600),
                conditions: vec![],
            },
        ),
        (
            RequestId(7),
            post(vec![Line::debit(REWARDS, 1), Line::credit(MERCHANT, 1)]),
        ),
        (RequestId(8), capture.clone()),
        (RequestId(8), capture),
        (RequestId(8), Command::Void { reservation: hold }),
    ]);
    let mut ledger = new_book();
    let outcomes = ledger.apply_batch(commands.clone());
    assert!(outcomes[..6].iter().all(Result::is_ok));
    assert_eq!(outcomes[6], Err(Error::BelowMinimum(REWARDS)));
    assert!(
        matches!(&outcomes[7], Ok(Outcome::Captured { entry, .. }) if entry.lines.as_ref() == [Line::debit(REWARDS,200),Line::debit(CASH,200),Line::credit(MERCHANT,400)])
    );
    assert_eq!(outcomes[7], outcomes[8]);
    assert_eq!(outcomes[9], Err(Error::IdConflict(RequestId(8))));
    assert_eq!(ledger.journal().len(), 2);
    assert_eq!(
        ledger.reservation(hold).unwrap().remaining,
        vec![100, 100, 200]
    );
    for (account, expected) in [
        (BANK, (800, 0, 0, 0)),
        (REWARDS, (200, 300, 100, 0)),
        (CASH, (200, 500, 100, 0)),
        (MERCHANT, (0, 400, 0, 200)),
    ] {
        let c = ledger.account(account).unwrap().counters();
        assert_eq!(
            (
                c.debits_posted,
                c.credits_posted,
                c.debits_pending,
                c.credits_pending
            ),
            expected
        );
    }
    let mut replica = new_book();
    assert_eq!(replica.apply_batch(commands), outcomes);
    assert_eq!(replica, ledger);
}

#[test]
fn policy_change_and_rejection_have_stable_request_outcomes() {
    let mut ledger = new_book();
    assert!(
        ledger
            .apply_batch(opening_commands())
            .iter()
            .all(Result::is_ok)
    );
    let payment = post(vec![Line::debit(REWARDS, 400), Line::credit(MERCHANT, 400)]);
    assert_eq!(
        ledger.apply(RequestId(10), payment.clone()),
        Err(Error::BelowMinimum(REWARDS))
    );
    let loosen = Command::SetAccountBounds {
        account: REWARDS,
        bounds: Bounds {
            min: Some(Balance::negative(100)),
            max: None,
        },
    };
    assert!(ledger.apply(RequestId(11), loosen).is_ok());
    assert_eq!(
        ledger.apply(RequestId(10), payment.clone()),
        Err(Error::BelowMinimum(REWARDS))
    );
    assert!(ledger.apply(RequestId(12), payment).is_ok());
    assert_eq!(
        ledger.account(REWARDS).unwrap().posted_balance(),
        Balance::negative(100)
    );
}

#[test]
fn checked_posting_is_atomic_when_the_last_account_fails() {
    let mut ledger = new_book();
    assert!(
        ledger
            .apply_batch(opening_commands())
            .iter()
            .all(Result::is_ok)
    );
    let bound = Command::SetAccountBounds {
        account: MERCHANT,
        bounds: Bounds {
            min: None,
            max: Some(Balance::positive(99)),
        },
    };
    assert!(ledger.apply(RequestId(10), bound).is_ok());
    let before = ledger.account(CASH).unwrap().clone();
    let pay = post(vec![Line::debit(CASH, 100), Line::credit(MERCHANT, 100)]);
    assert_eq!(
        ledger.apply(RequestId(11), pay),
        Err(Error::AboveMaximum(MERCHANT))
    );
    assert_eq!(ledger.account(CASH), Some(&before));
    assert_eq!(ledger.journal().len(), 1);
}
