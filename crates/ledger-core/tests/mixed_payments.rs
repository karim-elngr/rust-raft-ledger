//! Independent, integer-only wallet oracle for long interleavings of holds,
//! partial settlement, capped waterfalls, rejection and delayed retries.
use ledger_core::*;

const BOOK: LedgerId = LedgerId(131);
const WALLETS: usize = 8;
fn account(i: usize) -> AccountId {
    AccountId::new(BOOK, i as u128 + 1)
}
fn lines(from: usize, to: usize, amount: u128) -> Vec<Line> {
    vec![
        Line::debit(account(from), amount),
        Line::credit(account(to), amount),
    ]
}
fn post(funding: Funding) -> Command {
    Command::Post {
        funding,
        conditions: vec![],
        corrects: None,
    }
}
fn random(state: &mut u64, max: usize) -> usize {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    ((*state >> 32) as usize) % max
}
#[derive(Copy, Clone)]
struct Hold {
    id: ReservationId,
    from: usize,
    to: usize,
    left: u128,
}
fn available(c: Counters) -> u128 {
    c.credits_posted - c.debits_posted - c.debits_pending
}
fn posted(id: &mut u64, lines: Vec<Line>) -> JournalEntry {
    *id += 1;
    JournalEntry {
        id: EntryId(*id),
        lines: lines.into(),
        corrects: None,
    }
}
#[test]
fn mixed_payment_history_matches_an_independent_wallet_model() {
    for seed in [1, 241, 1923] {
        let mut ledger = Ledger::new(BOOK, "USD", 2);
        let mut wallets = [Counters::default(); WALLETS + 1];
        for i in 0..=WALLETS {
            ledger
                .apply(
                    RequestId(i as u128 + 1),
                    Command::CreateAccount {
                        account: account(i),
                        code: 1,
                        normal: if i == WALLETS {
                            Side::Debit
                        } else {
                            Side::Credit
                        },
                        bounds: Bounds {
                            min: Some(Balance::positive(0)),
                            max: None,
                        },
                    },
                )
                .unwrap();
        }
        let mut rng = seed;
        let mut entry = 0;
        let mut open: Vec<Hold> = Vec::new();
        let mut receipts = Vec::new();
        let mut rejected = 0;
        for step in 0..4_096 {
            let request = RequestId(100 + step);
            let i = random(&mut rng, WALLETS);
            let j = (i + 1 + random(&mut rng, WALLETS - 1)) % WALLETS;
            let amount = (1 + random(&mut rng, 250)) as u128;
            let action = random(&mut rng, 7);
            let (command, expected) = match action {
                0 => {
                    let lines = lines(WALLETS, i, amount);
                    wallets[WALLETS].debits_posted += amount;
                    wallets[i].credits_posted += amount;
                    (
                        post(Funding::Explicit(lines.clone())),
                        Ok(Outcome::Posted(posted(&mut entry, lines))),
                    )
                }
                1 | 2 => {
                    let id = ReservationId(step + 1);
                    let lines = lines(i, j, amount);
                    let command = Command::Reserve {
                        reservation: id,
                        funding: Funding::Explicit(lines.clone()),
                        conditions: vec![],
                    };
                    let outcome = if available(wallets[i]) < amount {
                        Err(Error::BelowMinimum(account(i)))
                    } else {
                        wallets[i].debits_pending += amount;
                        wallets[j].credits_pending += amount;
                        open.push(Hold {
                            id,
                            from: i,
                            to: j,
                            left: amount,
                        });
                        Ok(Outcome::Reserved {
                            reservation: id,
                            lines: lines.into(),
                        })
                    };
                    (command, outcome)
                }
                3..=5 if !open.is_empty() => {
                    let index = random(&mut rng, open.len());
                    let hold = open[index];
                    let capture = amount.min(hold.left);
                    let release = if action == 3 || action == 4 {
                        hold.left
                    } else {
                        capture
                    };
                    wallets[hold.from].debits_pending -= release;
                    wallets[hold.to].credits_pending -= release;
                    open[index].left -= release;
                    if open[index].left == 0 {
                        open.swap_remove(index);
                    }
                    if action == 3 {
                        (
                            Command::Void {
                                reservation: hold.id,
                            },
                            Ok(Outcome::Voided(hold.id)),
                        )
                    } else {
                        wallets[hold.from].debits_posted += capture;
                        wallets[hold.to].credits_posted += capture;
                        let command = Command::Capture {
                            reservation: hold.id,
                            amount: CaptureAmount::Amount(capture),
                            remainder: Some(if action == 4 {
                                Remainder::Release
                            } else {
                                Remainder::KeepReserved
                            }),
                        };
                        (
                            command,
                            Ok(Outcome::Captured {
                                reservation: hold.id,
                                entry: posted(&mut entry, lines(hold.from, hold.to, capture)),
                            }),
                        )
                    }
                }
                _ => {
                    let receiver = (0..WALLETS).find(|n| *n != i && *n != j).unwrap();
                    let cap = (1 + random(&mut rng, 180)) as u128;
                    let command = post(Funding::Waterfall(Waterfall {
                        amount,
                        sources: vec![
                            Source {
                                account: account(i),
                                floor: SourceFloor::Configured,
                                cap: Some(cap),
                            },
                            Source {
                                account: account(j),
                                floor: SourceFloor::Configured,
                                cap: None,
                            },
                        ],
                        destinations: vec![Line::credit(account(receiver), amount)],
                        shortfall: Shortfall::RequireFull,
                    }));
                    let first = amount.min(cap).min(available(wallets[i]));
                    let second = amount - first;
                    let outcome = if second > available(wallets[j]) {
                        Err(Error::InsufficientFunding)
                    } else {
                        let mut split = Vec::new();
                        for (wallet, n) in [(i, first), (j, second)] {
                            if n > 0 {
                                wallets[wallet].debits_posted += n;
                                split.push(Line::debit(account(wallet), n));
                            }
                        }
                        wallets[receiver].credits_posted += amount;
                        split.push(Line::credit(account(receiver), amount));
                        Ok(Outcome::Posted(posted(&mut entry, split)))
                    };
                    (command, outcome)
                }
            };
            rejected += usize::from(expected.is_err());
            assert_eq!(
                ledger.apply(request, command.clone()),
                expected,
                "seed {seed}, step {step}"
            );
            receipts.push((request, command, expected));
            if step % 5 == 0 {
                let (id, cmd, outcome) = &receipts[random(&mut rng, receipts.len())];
                assert_eq!(&ledger.apply(*id, cmd.clone()), outcome);
                assert_eq!(ledger.journal().len() as u64, entry);
            }
            for (n, expected) in wallets.iter().enumerate() {
                assert_eq!(
                    ledger.account(account(n)).unwrap().counters(),
                    *expected,
                    "seed {seed}, step {step}, wallet {n}"
                );
            }
        }
        assert!(rejected > 100 && entry > 500);
        for hold in open {
            assert_eq!(
                ledger.reservation(hold.id).unwrap().remaining,
                [hold.left, hold.left]
            );
        }
    }
}
