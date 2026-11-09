use ledger_core::*;
use std::collections::BTreeMap;

const LEDGER: LedgerId = LedgerId(5);
fn id(n: u128) -> AccountId {
    AccountId::new(LEDGER, n)
}

#[derive(Default)]
struct View {
    accounts: BTreeMap<AccountId, Account>,
    holds: BTreeMap<ReservationId, Reservation>,
    reads: Vec<AccountId>,
    corrections: Vec<EntryId>,
    failed: bool,
}
impl StateView for View {
    type Error = &'static str;
    fn account(&mut self, id: AccountId) -> Result<Option<Account>, Self::Error> {
        self.reads.push(id);
        if self.failed {
            return Err("storage unavailable");
        }
        Ok(self.accounts.get(&id).cloned())
    }
    fn reservation(&mut self, id: ReservationId) -> Result<Option<Reservation>, Self::Error> {
        Ok(self.holds.get(&id).cloned())
    }
    fn entry_exists(&mut self, id: EntryId) -> Result<bool, Self::Error> {
        self.corrections.push(id);
        Ok(id == EntryId(8))
    }
}

fn account(n: u128) -> Account {
    Account::from_parts(
        id(n),
        0,
        Side::Credit,
        Bounds::default(),
        Counters::default(),
    )
    .unwrap()
}

#[test]
fn corrected_post_reads_only_referenced_rows_and_proposes_the_next_entry() {
    let mut view = View {
        accounts: [1, 2, 999].map(|n| (id(n), account(n))).into(),
        ..View::default()
    };
    let command = Command::Post {
        funding: Funding::Explicit(vec![
            Line::debit(id(2), 3),
            Line::credit(id(1), 2),
            Line::credit(id(1), 1),
        ]),
        conditions: vec![Condition::AvailableAtLeast {
            account: id(2),
            amount: Balance::default(),
        }],
        corrects: Some(EntryId(8)),
    };
    let delta = evaluate(LEDGER, EntryId(1000), &command, &mut view)
        .unwrap()
        .unwrap();
    assert_eq!(view.reads, [id(1), id(2)]);
    assert_eq!(view.corrections, [EntryId(8)]);
    assert_eq!(delta.accounts.len(), 2);
    assert_eq!(delta.accounts[&id(1)].counters().credits_posted, 3);
    assert_eq!(delta.accounts[&id(2)].counters().debits_posted, 3);
    let e = delta.entry().unwrap();
    assert_eq!((e.id, e.corrects), (EntryId(1001), Some(EntryId(8))));
    assert_eq!(view.accounts[&id(1)].counters(), Counters::default());
    assert!(matches!(
        evaluate(LEDGER, EntryId(u64::MAX), &command, &mut view),
        Err(EvaluationError::JournalFull)
    ));
}

#[test]
fn storage_faults_cannot_be_mistaken_for_recordable_business_rejections() {
    let mut view = View {
        failed: true,
        ..View::default()
    };
    let create = Command::CreateAccount {
        account: id(1),
        code: 0,
        normal: Side::Debit,
        bounds: Bounds::default(),
    };
    assert!(matches!(
        evaluate(LEDGER, EntryId(0), &create, &mut view),
        Err(EvaluationError::Storage("storage unavailable"))
    ));
    let missing = Command::Void {
        reservation: ReservationId(9),
    };
    assert!(matches!(
        evaluate(LEDGER, EntryId(0), &missing, &mut view),
        Ok(Err(Error::UnknownReservation(ReservationId(9))))
    ));
}

#[cfg(feature = "projection-serde")]
#[test]
fn stored_rows_round_trip_and_invalid_values_or_collection_lengths_are_refused() {
    let a = account(9);
    assert_eq!(
        bincode::deserialize::<Account>(&bincode::serialize(&a).unwrap()).unwrap(),
        a
    );
    let invalid = bincode::serialize(&(
        id(9),
        0u16,
        Side::Credit,
        Bounds::default(),
        Counters {
            credits_posted: u128::MAX,
            credits_pending: 1,
            ..Counters::default()
        },
    ))
    .unwrap();
    assert!(bincode::deserialize::<Account>(&invalid).is_err());
    let minus_zero = bincode::serialize(&(true, 0u128)).unwrap();
    assert!(bincode::deserialize::<Balance>(&minus_zero).is_err());

    let mut ledger = Ledger::new(LEDGER, "USD", 2);
    let make = |n| Command::CreateAccount {
        account: id(n),
        code: 0,
        normal: Side::Credit,
        bounds: Bounds::default(),
    };
    ledger.apply(RequestId(1), make(1)).unwrap();
    ledger.apply(RequestId(2), make(2)).unwrap();
    let book = Command::Reserve {
        reservation: ReservationId(7),
        conditions: vec![],
        funding: Funding::Explicit(vec![Line::debit(id(1), 5), Line::credit(id(2), 5)]),
    };
    ledger.apply(RequestId(3), book).unwrap();
    let original = ledger.reservation(ReservationId(7)).unwrap();
    assert_eq!(
        &bincode::deserialize::<Reservation>(&bincode::serialize(original).unwrap()).unwrap(),
        original
    );
    let mut bad_hold = original.clone();
    bad_hold.remaining.pop();
    assert!(bincode::deserialize::<Reservation>(&bincode::serialize(&bad_hold).unwrap()).is_err());
    for d in ledger.decisions() {
        assert_eq!(
            &bincode::deserialize::<Decision>(&bincode::serialize(d).unwrap()).unwrap(),
            d
        );
    }
    let too_many = Funding::Explicit(vec![Line::debit(id(1), 1); 1025]);
    assert!(bincode::deserialize::<Funding>(&bincode::serialize(&too_many).unwrap()).is_err());
}

#[test]
fn first_decision_checks_normalized_command_identity_on_retry() {
    let original = Command::Void {
        reservation: ReservationId(7),
    };
    let different = Command::Void {
        reservation: ReservationId(8),
    };
    let decision = Decision {
        request: RequestId(101),
        command: original.clone(),
        outcome: Err(Error::UnknownReservation(ReservationId(7))),
    };
    assert_eq!(decision.outcome_for(&original), decision.outcome);
    assert_eq!(
        decision.outcome_for(&different),
        Err(Error::IdConflict(RequestId(101)))
    );
}

#[cfg(feature = "projection-serde")]
#[test]
fn v1_waterfall_rows_accept_twice_the_per_input_limit() {
    let mut ledger = Ledger::new(LEDGER, "USD", 2);
    for n in 0..=1024 {
        ledger
            .apply(
                RequestId(n),
                Command::CreateAccount {
                    account: id(n),
                    code: 1,
                    normal: Side::Credit,
                    bounds: Bounds::default(),
                },
            )
            .unwrap();
    }
    let cmd = Command::Reserve {
        reservation: ReservationId(9),
        funding: Funding::Waterfall(Waterfall {
            amount: 1024,
            sources: (1..=1024)
                .map(|n| Source {
                    account: id(n),
                    floor: SourceFloor::At(Balance::negative(u128::MAX)),
                    cap: Some(1),
                })
                .collect(),
            destinations: vec![Line::credit(id(0), 1); 1024],
            shortfall: Shortfall::RequireFull,
        }),
        conditions: vec![
            Condition::AvailableAtLeast {
                account: id(10),
                amount: Balance::negative(u128::MAX)
            };
            1024
        ],
    };
    ledger.apply(RequestId(2000), cmd).unwrap();
    let reserved = ledger.decisions().last().unwrap();
    let row = bincode::serialize(reserved).unwrap();
    assert_eq!(&bincode::deserialize::<Decision>(&row).unwrap(), reserved);
    let hold = ledger.reservation(ReservationId(9)).unwrap();
    assert_eq!(hold.lines.len(), 2048);
    assert_eq!(
        &bincode::deserialize::<Reservation>(&bincode::serialize(hold).unwrap()).unwrap(),
        hold
    );
    let Outcome::Captured { entry, .. } = ledger
        .apply(
            RequestId(2001),
            Command::Capture {
                reservation: ReservationId(9),
                amount: CaptureAmount::Full,
                remainder: None,
            },
        )
        .unwrap()
    else {
        panic!("expected full capture")
    };
    assert_eq!(entry.lines.len(), 2048);
    assert_eq!(
        bincode::deserialize::<JournalEntry>(&bincode::serialize(&entry).unwrap()).unwrap(),
        entry
    );
    // Reject the declared count even when there are no actual items to decode.
    let oversized = [
        0u32.to_le_bytes().as_slice(),
        u64::MAX.to_le_bytes().as_slice(),
    ]
    .concat();
    let err = bincode::deserialize::<Funding>(&oversized).unwrap_err();
    assert!(err.to_string().contains("item limit"), "{err}");
}

#[test]
fn partial_capture_loads_only_accounts_with_a_pending_change() {
    let mut source = Ledger::new(LEDGER, "USD", 2);
    for n in [1, 2, 3] {
        source
            .apply(
                RequestId(n),
                Command::CreateAccount {
                    account: id(n),
                    code: 0,
                    normal: Side::Credit,
                    bounds: Bounds::default(),
                },
            )
            .unwrap();
    }
    let hold_id = ReservationId(4);
    source
        .apply(
            RequestId(4),
            Command::Reserve {
                reservation: hold_id,
                funding: Funding::Explicit(vec![
                    Line::debit(id(1), 10),
                    Line::credit(id(2), 6),
                    Line::credit(id(3), 4),
                ]),
                conditions: vec![],
            },
        )
        .unwrap();
    let mut view = View {
        accounts: [1, 2, 3]
            .map(|n| (id(n), source.account(id(n)).unwrap().clone()))
            .into(),
        holds: [(hold_id, source.reservation(hold_id).unwrap().clone())].into(),
        ..View::default()
    };
    let charge = Command::Capture {
        reservation: hold_id,
        amount: CaptureAmount::Lines(vec![
            Allocation { line: 0, amount: 6 },
            Allocation { line: 1, amount: 6 },
        ]),
        remainder: Some(Remainder::KeepReserved),
    };
    let changes = evaluate(LEDGER, EntryId(0), &charge, &mut view)
        .unwrap()
        .unwrap();
    assert_eq!(view.reads, [id(1), id(2)]);
    assert_eq!(
        changes.accounts.keys().copied().collect::<Vec<_>>(),
        [id(1), id(2)]
    );
    assert_eq!(changes.reservation.unwrap().remaining, [4, 0, 4]);
}
