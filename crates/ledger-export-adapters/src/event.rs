use crate::v1;
use ledger_api::v1 as api;
use ledger_core as core;
use ledger_export::{EventRecord, Identity};

fn side(side: core::Side) -> i32 {
    (match side {
        core::Side::Debit => api::Side::Debit,
        core::Side::Credit => api::Side::Credit,
    }) as i32
}
fn account(id: core::AccountId) -> api::AccountRef {
    api::AccountRef {
        ledger_id: id.ledger.0.to_string(),
        account_id: id.number.to_string(),
    }
}
fn bounds(b: &core::Bounds) -> api::Bounds {
    api::Bounds {
        min: b.min.map(|v| v.to_string()),
        max: b.max.map(|v| v.to_string()),
    }
}
fn line(l: &core::Line) -> v1::Line {
    v1::Line {
        account: Some(account(l.account)),
        side: side(l.side),
        amount: l.amount.to_string(),
    }
}
fn funding(f: &core::Funding) -> v1::Funding {
    use v1::funding::Kind;
    let kind = match f {
        core::Funding::Explicit(lines) => Kind::Explicit(v1::Explicit {
            lines: lines.iter().map(line).collect(),
        }),
        core::Funding::Waterfall(w) => Kind::Waterfall(v1::Waterfall {
            amount: w.amount.to_string(),
            sources: w
                .sources
                .iter()
                .map(|s| v1::Source {
                    account: Some(account(s.account)),
                    cap: s.cap.map(|v| v.to_string()),
                    floor: Some(match s.floor {
                        core::SourceFloor::Configured => {
                            v1::source::Floor::Configured(api::Empty {})
                        }
                        core::SourceFloor::At(v) => v1::source::Floor::At(v.to_string()),
                    }),
                })
                .collect(),
            destinations: w.destinations.iter().map(line).collect(),
            shortfall: match w.shortfall {
                core::Shortfall::RequireFull => api::Shortfall::RequireFull as i32,
            },
        }),
    };
    v1::Funding { kind: Some(kind) }
}
fn conditions(cs: &[core::Condition]) -> Vec<v1::Condition> {
    cs.iter()
        .map(|c| v1::Condition {
            kind: Some(match c {
                core::Condition::AvailableAtLeast { account: a, amount } => {
                    v1::condition::Kind::AvailableAtLeast(v1::AvailableAtLeast {
                        account: Some(account(*a)),
                        amount: amount.to_string(),
                    })
                }
            }),
        })
        .collect()
}

pub(super) fn event(id: &Identity, r: &EventRecord) -> v1::DecisionEvent {
    use v1::decision_event::Command;
    let ledger_id = id.ledger.0.to_string();
    let request_id = r.decision.request.0.to_string();
    let command = match &r.decision.command {
        core::Command::CreateAccount {
            account: a,
            code,
            normal,
            bounds: b,
        } => Command::CreateAccount(v1::CreateAccount {
            account: Some(account(*a)),
            code: u32::from(*code),
            normal: side(*normal),
            bounds: Some(bounds(b)),
        }),
        core::Command::SetAccountBounds {
            account: a,
            bounds: b,
        } => Command::SetBounds(v1::SetBounds {
            account: Some(account(*a)),
            bounds: Some(bounds(b)),
        }),
        core::Command::Post {
            funding: f,
            conditions: c,
            corrects,
        } => Command::Post(v1::Post {
            funding: Some(funding(f)),
            conditions: conditions(c),
            corrects: corrects.map(|v| v.0),
        }),
        core::Command::Reserve {
            reservation,
            funding: f,
            conditions: c,
        } => Command::Reserve(v1::Reserve {
            reservation_id: reservation.0.to_string(),
            funding: Some(funding(f)),
            conditions: conditions(c),
        }),
        core::Command::Capture {
            reservation,
            amount,
            remainder,
        } => {
            use api::capture_reservation_request::{Capture, Disposition};
            let capture = match amount {
                core::CaptureAmount::Full => Capture::Full(api::Empty {}),
                core::CaptureAmount::Amount(v) => Capture::Amount(v.to_string()),
                core::CaptureAmount::Lines(ls) => Capture::Allocations(api::Allocations {
                    lines: ls
                        .iter()
                        .map(|l| api::Allocation {
                            line: l.line,
                            amount: l.amount.to_string(),
                        })
                        .collect(),
                }),
            };
            Command::Capture(api::CaptureReservationRequest {
                ledger_id: ledger_id.clone(),
                request_id: request_id.clone(),
                reservation_id: reservation.0.to_string(),
                capture: Some(capture),
                disposition: remainder.map(|r| match r {
                    core::Remainder::KeepReserved => Disposition::KeepReserved(api::Empty {}),
                    core::Remainder::Release => Disposition::Release(api::Empty {}),
                }),
            })
        }
        core::Command::Void { reservation } => {
            Command::VoidReservation(api::VoidReservationRequest {
                ledger_id: ledger_id.clone(),
                request_id: request_id.clone(),
                reservation_id: reservation.0.to_string(),
            })
        }
    };
    v1::DecisionEvent {
        schema_version: 1,
        event_id: format!("{ledger_id}/{request_id}"),
        ledger_id,
        request_id,
        generation: id.generation.to_string(),
        raft_index: r.position.index,
        slot: u32::from(r.position.slot),
        asset: id.asset.clone(),
        scale: u32::from(id.scale),
        command: Some(command),
        decision: Some(ledger_api::adapter::decision(r.decision.outcome.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1::{decision_event::Command, funding::Kind};
    use ledger_core::{
        AccountId, EntryId, Funding, JournalEntry, LedgerId, Line, RequestId, Shortfall, Source,
        SourceFloor, Waterfall,
    };
    use ledger_export::Position;
    use prost::Message;

    #[test]
    fn waterfall_keeps_original_order_full_width_amounts_and_resolved_postings() {
        let ledger = LedgerId(u128::MAX);
        let account = |n| AccountId::new(ledger, n);
        let original_sources = vec![
            Source {
                account: account(3),
                floor: SourceFloor::Configured,
                cap: Some(8),
            },
            Source {
                account: account(1),
                floor: SourceFloor::At(core::Balance::negative(5)),
                cap: None,
            },
        ];
        let target = Line::credit(account(2), u128::MAX);
        let cmd = core::Command::Post {
            funding: Funding::Waterfall(Waterfall {
                amount: u128::MAX,
                sources: original_sources,
                destinations: vec![target],
                shortfall: Shortfall::RequireFull,
            }),
            conditions: vec![core::Condition::AvailableAtLeast {
                account: account(1),
                amount: core::Balance::negative(1),
            }],
            corrects: Some(EntryId(2)),
        };
        let record = EventRecord {
            position: Position { index: 4, slot: 3 },
            decision: core::Decision {
                request: RequestId(7),
                command: cmd,
                outcome: Ok(core::Outcome::Posted(JournalEntry {
                    id: EntryId(3),
                    lines: vec![
                        Line::debit(account(3), 8),
                        Line::debit(account(1), u128::MAX - 8),
                        target,
                    ]
                    .into(),
                    corrects: Some(EntryId(2)),
                })),
            },
        };
        let identity = Identity {
            ledger,
            generation: 8,
            asset: "JPY".into(),
            scale: 0,
        };
        let wire = v1::DecisionEvent::decode(event(&identity, &record).encode_to_vec().as_slice())
            .unwrap();
        let Some(Command::Post(post)) = wire.command else {
            panic!("missing post")
        };
        let Some(Kind::Waterfall(w)) = post.funding.unwrap().kind else {
            panic!("missing waterfall")
        };
        assert_eq!(w.amount, u128::MAX.to_string());
        assert_eq!(
            w.sources
                .iter()
                .map(|s| s.account.as_ref().unwrap().account_id.as_str())
                .collect::<Vec<_>>(),
            ["3", "1"]
        );
        assert_eq!(w.sources[0].cap.as_deref(), Some("8"));
        assert!(matches!(&w.sources[1].floor,Some(v1::source::Floor::At(v)) if v=="-5"));
        let Some(api::decision::Result::Accepted(outcome)) = wire.decision.unwrap().result else {
            panic!("missing outcome")
        };
        let Some(api::outcome::Kind::Posted(journal)) = outcome.kind else {
            panic!("missing posting")
        };
        assert_eq!(journal.corrects, Some(2));
        assert_eq!(journal.lines.len(), 3);
        assert_eq!(journal.lines[1].amount, (u128::MAX - 8).to_string());
    }
}
