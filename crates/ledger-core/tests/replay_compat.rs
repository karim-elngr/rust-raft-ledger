//! Expected first failures are part of the V1 command replay contract.
use ledger_core::*;

const BOOK: LedgerId = LedgerId(8);
fn id(n: u128) -> AccountId {
    AccountId::new(BOOK, n)
}
fn waterfall(sources: &[AccountId], merchant: AccountId) -> Funding {
    Funding::Waterfall(Waterfall {
        amount: 1,
        sources: sources
            .iter()
            .map(|&account| Source {
                account,
                floor: SourceFloor::At(Balance::negative(1)),
                cap: None,
            })
            .collect(),
        destinations: vec![Line::credit(merchant, 1)],
        shortfall: Shortfall::RequireFull,
    })
}
fn post(funding: Funding, conditions: Vec<Condition>) -> Command {
    Command::Post {
        funding,
        conditions,
        corrects: None,
    }
}

#[test]
fn legacy_errors_follow_input_order_even_when_rows_are_preloaded_in_account_order() {
    let mut book = Ledger::new(BOOK, "USD", 2);
    let a = id(3);
    let b = id(2);
    let missing = id(1);
    let foreign = AccountId::new(LedgerId(9), 3);
    for (n, who) in [(1, a), (2, b)] {
        book.apply(
            RequestId(n),
            Command::CreateAccount {
                account: who,
                code: 1,
                normal: Side::Credit,
                bounds: Bounds::default(),
            },
        )
        .unwrap();
    }
    let condition = |account, amount| Condition::AvailableAtLeast { account, amount };
    let cases = [
        (
            post(waterfall(&[a, a, foreign], b), vec![]),
            Error::DuplicateSource(a),
        ),
        (
            post(waterfall(&[a, foreign, a], b), vec![]),
            Error::WrongLedger(foreign),
        ),
        (
            post(waterfall(&[missing, a, a], b), vec![]),
            Error::UnknownAccount(missing),
        ),
        (
            post(waterfall(&[a], foreign), vec![]),
            Error::WrongLedger(foreign),
        ),
        (
            post(
                Funding::Explicit(vec![Line::debit(foreign, 3), Line::credit(b, 1)]),
                vec![],
            ),
            Error::Unbalanced,
        ),
        (
            post(
                waterfall(&[a], b),
                vec![
                    condition(a, Balance::positive(1)),
                    condition(foreign, Balance::default()),
                ],
            ),
            Error::ConditionFailed(a),
        ),
        (
            post(
                waterfall(&[a], b),
                vec![
                    condition(foreign, Balance::default()),
                    condition(a, Balance::positive(1)),
                ],
            ),
            Error::InvalidConditionTarget(foreign),
        ),
        (
            Command::Post {
                funding: Funding::Explicit(vec![]),
                corrects: Some(EntryId(0)),
                conditions: vec![],
            },
            Error::UnknownEntry(EntryId(0)),
        ),
    ];
    for (position, (command, expected)) in cases.into_iter().enumerate() {
        let req = RequestId(20 + position as u128);
        assert_eq!(book.apply(req, command.clone()), Err(expected.clone()));
        assert_eq!(book.apply(req, command), Err(expected));
        assert_eq!(
            book.apply(
                req,
                Command::Void {
                    reservation: ReservationId(30)
                }
            ),
            Err(Error::IdConflict(req))
        );
    }
    assert!(book.journal().is_empty());
    let reserved = Command::Reserve {
        reservation: ReservationId(4),
        funding: waterfall(&[a], b),
        conditions: vec![],
    };
    book.apply(RequestId(40), reserved).unwrap();
    let contradictory = Command::Reserve {
        reservation: ReservationId(4),
        funding: Funding::Explicit(vec![]),
        conditions: vec![],
    };
    assert_eq!(
        book.apply(RequestId(41), contradictory),
        Err(Error::DuplicateReservation(ReservationId(4)))
    );
}
