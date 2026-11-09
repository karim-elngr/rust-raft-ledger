use ledger_core::*;

const BOOK: LedgerId = LedgerId(4);
const BANK: AccountId = AccountId::new(BOOK, 1);
const REWARDS: AccountId = AccountId::new(BOOK, 2);
const CASH: AccountId = AccountId::new(BOOK, 3);
const MERCHANT: AccountId = AccountId::new(BOOK, 4);

fn create(account: AccountId, normal: Side) -> Command {
    Command::CreateAccount {
        account,
        normal,
        code: 1,
        bounds: Bounds {
            min: Some(Balance::positive(0)),
            max: None,
        },
    }
}

fn explicit(lines: Vec<Line>) -> Command {
    Command::Post {
        funding: Funding::Explicit(lines),
        conditions: vec![],
        corrects: None,
    }
}

fn funded_book() -> Ledger {
    let mut ledger = Ledger::new(BOOK, "USD", 2);
    for account in [BANK, REWARDS, CASH, MERCHANT] {
        let side = if account == BANK {
            Side::Debit
        } else {
            Side::Credit
        };
        ledger
            .apply(RequestId(account.number), create(account, side))
            .unwrap();
    }
    ledger
        .apply(
            RequestId(5),
            explicit(vec![
                Line::debit(BANK, 13000),
                Line::credit(REWARDS, 4000),
                Line::credit(CASH, 9000),
            ]),
        )
        .unwrap();
    ledger
}

fn reserve(ledger: &mut Ledger, id: ReservationId, lines: Vec<Line>) {
    ledger
        .apply(
            RequestId(6),
            Command::Reserve {
                reservation: id,
                funding: Funding::Explicit(lines),
                conditions: vec![],
            },
        )
        .unwrap();
}

#[test]
fn a_correction_links_to_the_immutable_original_and_validates_the_reference() {
    let mut book = funded_book();
    let original = book.journal()[0].clone();
    let reversal = vec![
        Line::credit(BANK, 13000),
        Line::debit(REWARDS, 4000),
        Line::debit(CASH, 9000),
    ];
    let correct = |id| Command::Post {
        funding: Funding::Explicit(reversal.clone()),
        conditions: vec![],
        corrects: Some(id),
    };
    assert_eq!(
        book.apply(RequestId(6), correct(EntryId(99))),
        Err(Error::UnknownEntry(EntryId(99)))
    );
    let Outcome::Posted(correction) = book.apply(RequestId(7), correct(original.id)).unwrap()
    else {
        panic!("a correction must produce a posted entry");
    };
    assert_ne!(correction.id, original.id);
    assert_eq!(correction.corrects, Some(original.id));
    assert_eq!(correction.lines.as_ref(), reversal);
    assert_eq!(book.entry(original.id), Some(&original));
    assert_eq!(book.entry(correction.id), Some(&correction));
    assert_eq!(
        book.account(BANK).unwrap().posted_balance(),
        Balance::default()
    );
    assert_eq!(book.journal().len(), 2);
}

#[test]
fn account_numbers_and_request_ids_are_local_to_each_ledger() {
    let mut usd = funded_book();
    let eur_id = LedgerId(5);
    let eur_wallet = AccountId::new(eur_id, CASH.number);
    let mut eur = Ledger::new(eur_id, "EUR", 2);
    let shared_request = RequestId(CASH.number);
    assert_eq!(
        eur.apply(shared_request, create(eur_wallet, Side::Credit)),
        Ok(Outcome::AccountCreated(eur_wallet))
    );
    assert_eq!(
        usd.request_outcome(shared_request),
        Some(&Ok(Outcome::AccountCreated(CASH)))
    );
    assert_eq!(
        eur.account(eur_wallet).unwrap().posted_balance(),
        Balance::default()
    );

    let mixed = explicit(vec![Line::debit(CASH, 1), Line::credit(eur_wallet, 1)]);
    assert_eq!(
        usd.apply(RequestId(6), mixed),
        Err(Error::WrongLedger(eur_wallet))
    );
    assert_eq!(usd.journal().len(), 1);
    assert!(usd.account(eur_wallet).is_none());
    assert!(eur.journal().is_empty());
}

#[test]
fn ordered_conditions_run_against_pre_posting_balances_before_account_limits() {
    let mut book = funded_book();
    let conditions = |order: [AccountId; 2]| {
        order
            .map(|account| Condition::AvailableAtLeast {
                account,
                amount: Balance::positive(10000),
            })
            .to_vec()
    };
    // The debit also violates the wallet floor and the merchant would meet its condition after posting.
    let command = |order| Command::Post {
        funding: Funding::Explicit(vec![
            Line::debit(CASH, 10000),
            Line::credit(MERCHANT, 10000),
        ]),
        conditions: conditions(order),
        corrects: None,
    };
    assert_eq!(
        book.apply(RequestId(6), command([MERCHANT, CASH])),
        Err(Error::ConditionFailed(MERCHANT))
    );
    assert_eq!(
        book.apply(RequestId(7), command([CASH, MERCHANT])),
        Err(Error::ConditionFailed(CASH))
    );
    assert_eq!(book.journal().len(), 1);
    assert_eq!(
        book.account(CASH).unwrap().posted_balance(),
        Balance::positive(9000)
    );
}

#[test]
fn competing_waterfalls_retain_the_first_split_and_the_second_shortfall_across_batches() {
    let mut book = funded_book();
    reserve(
        &mut book,
        ReservationId(1),
        vec![Line::debit(REWARDS, 1000), Line::credit(BANK, 1000)],
    );
    let payment = Command::Post {
        funding: Funding::Waterfall(Waterfall {
            amount: 8000,
            sources: [REWARDS, CASH]
                .map(|account| Source {
                    account,
                    floor: SourceFloor::Configured,
                    cap: None,
                })
                .to_vec(),
            destinations: vec![Line::credit(MERCHANT, 8000)],
            shortfall: Shortfall::RequireFull,
        }),
        conditions: vec![],
        corrects: None,
    };
    let (a, b) = (RequestId(7), RequestId(8));
    let first = book.apply_batch([(a, payment.clone()), (b, payment.clone())]);
    let expected = Ok(Outcome::Posted(JournalEntry {
        id: EntryId(2),
        lines: vec![
            Line::debit(REWARDS, 3000),
            Line::debit(CASH, 5000),
            Line::credit(MERCHANT, 8000),
        ]
        .into(),
        corrects: None,
    }));
    assert_eq!(
        first,
        vec![expected.clone(), Err(Error::InsufficientFunding)]
    );
    assert_eq!(
        book.account(CASH).unwrap().available_balance(),
        Balance::positive(4000)
    );
    assert_eq!(
        book.account(REWARDS).unwrap().available_balance(),
        Balance::default()
    );
    let refund = explicit(vec![Line::debit(BANK, 8000), Line::credit(REWARDS, 8000)]);
    book.apply(RequestId(9), refund).unwrap();
    let before_retries = book.clone();
    assert_eq!(
        book.apply_batch([(b, payment.clone()), (a, payment)]),
        vec![Err(Error::InsufficientFunding), expected]
    );
    assert_eq!(book, before_retries);
}

#[test]
fn outstanding_destination_capacity_survives_policy_updates_and_both_capture_modes() {
    let mut book = funded_book();
    let hold = ReservationId(70);
    reserve(
        &mut book,
        hold,
        vec![Line::debit(CASH, 4000), Line::credit(MERCHANT, 4000)],
    );
    let ceiling = |limit| Command::SetAccountBounds {
        account: MERCHANT,
        bounds: Bounds {
            min: Some(Balance::positive(0)),
            max: Some(Balance::positive(limit)),
        },
    };
    assert_eq!(
        book.apply(RequestId(7), ceiling(3999)),
        Err(Error::AboveMaximum(MERCHANT))
    );
    assert_eq!(book.account(MERCHANT).unwrap().bounds().max, None);
    book.apply(RequestId(8), ceiling(4000)).unwrap();
    let extra = explicit(vec![Line::debit(CASH, 1), Line::credit(MERCHANT, 1)]);
    assert_eq!(
        book.apply(RequestId(9), extra.clone()),
        Err(Error::AboveMaximum(MERCHANT))
    );

    for (amount, remainder, posted) in [
        (CaptureAmount::Full, None, 4000),
        (CaptureAmount::Amount(2500), Some(Remainder::Release), 2500),
    ] {
        let mut branch = book.clone();
        branch
            .apply(
                RequestId(10),
                Command::Capture {
                    reservation: hold,
                    amount,
                    remainder,
                },
            )
            .unwrap();
        let merchant = branch.account(MERCHANT).unwrap();
        assert_eq!(merchant.counters().credits_pending, 0);
        assert_eq!(merchant.counters().credits_posted, posted);
        assert_eq!(
            branch.account(CASH).unwrap().available_balance(),
            Balance::positive(9000 - posted)
        );
        assert_eq!(
            branch.reservation(hold).unwrap().status,
            ReservationStatus::Captured
        );
        let new_debit = branch.apply(RequestId(11), extra.clone());
        if posted == 4000 {
            assert_eq!(new_debit, Err(Error::AboveMaximum(MERCHANT)));
        } else {
            assert!(matches!(new_debit, Ok(Outcome::Posted(_))));
        }
    }
}
