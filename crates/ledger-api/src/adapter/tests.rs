use super::*;
use crate::v1;
use ledger_core as core;
use tonic::Code;

const LEDGER: core::LedgerId = core::LedgerId(7);

fn account_id(number: u128) -> core::AccountId {
    core::AccountId::new(LEDGER, number)
}

#[test]
fn integers_use_canonical_ascii_and_the_entire_signed_magnitude_range() {
    for (s, expected) in [
        ("0".to_owned(), core::Balance::default()),
        (u128::MAX.to_string(), core::Balance::positive(u128::MAX)),
        (
            format!("-{}", u128::MAX),
            core::Balance::negative(u128::MAX),
        ),
    ] {
        let b = parse_balance(&s).unwrap();
        assert_eq!(b, expected);
        assert_eq!(b.to_string(), s);
    }
    for malformed in [
        "",
        " ",
        "01",
        "-0",
        "-01",
        "+1",
        "1 ",
        "0.0",
        "1e2",
        "０",
        "340282366920938463463374607431768211456",
    ] {
        assert_eq!(
            parse_balance(malformed).unwrap_err().code(),
            Code::InvalidArgument
        );
    }
    assert!(parse_id("-1").is_err());
    assert!(check_ledger("8", LEDGER).is_err());
}

#[test]
fn unbounded_rules_and_explicit_zero_are_distinct_and_bad_code_is_malformed() {
    let create = v1::CreateAccountRequest {
        ledger_id: "7".into(),
        request_id: u128::MAX.to_string(),
        account_id: "0".into(),
        code: 65535,
        normal: v1::Side::Credit as i32,
        bounds: Some(v1::Bounds {
            min: Some("0".into()),
            max: None,
        }),
    };
    assert_eq!(
        create.clone().into_command(LEDGER).unwrap(),
        (
            core::RequestId(u128::MAX),
            core::Command::CreateAccount {
                account: account_id(0),
                code: u16::MAX,
                normal: core::Side::Credit,
                bounds: core::Bounds {
                    min: Some(core::Balance::default()),
                    max: None
                }
            }
        )
    );
    let invalid = v1::CreateAccountRequest {
        code: 65536,
        ..create
    };
    assert_eq!(
        invalid.into_command(LEDGER).unwrap_err().code(),
        Code::InvalidArgument
    );
    let set = v1::SetAccountBoundsRequest {
        ledger_id: "7".into(),
        request_id: "6".into(),
        account_id: "4".into(),
        bounds: Some(v1::Bounds::default()),
    };
    assert_eq!(
        set.into_command(LEDGER).unwrap().1,
        core::Command::SetAccountBounds {
            account: account_id(4),
            bounds: core::Bounds::default()
        }
    );
}

fn source(id: u128, floor: v1::source::Floor) -> v1::Source {
    v1::Source {
        account_id: id.to_string(),
        floor: Some(floor),
        cap: None,
    }
}

fn reserve() -> v1::ReserveEntryRequest {
    v1::ReserveEntryRequest {
        ledger_id: "7".into(),
        request_id: "8".into(),
        reservation_id: "2".into(),
        funding: Some(v1::Funding {
            kind: Some(v1::funding::Kind::Waterfall(v1::Waterfall {
                amount: "80".into(),
                sources: vec![
                    source(2, v1::source::Floor::At("-40".into())),
                    v1::Source {
                        cap: Some("0".into()),
                        ..source(1, v1::source::Floor::Configured(v1::Empty {}))
                    },
                ],
                destinations: vec![v1::Line {
                    account_id: "3".into(),
                    side: v1::Side::Credit as i32,
                    amount: "80".into(),
                }],
                shortfall: v1::Shortfall::RequireFull as i32,
            })),
        }),
        conditions: vec![v1::Condition {
            kind: Some(v1::condition::Kind::AvailableAtLeast(
                v1::AvailableAtLeast {
                    account_id: "2".into(),
                    amount: "-15".into(),
                },
            )),
        }],
    }
}

#[test]
fn waterfall_preserves_source_order_floor_instructions_caps_and_conditions() {
    let (id, cmd) = reserve().into_command(LEDGER).unwrap();
    assert_eq!(id, core::RequestId(8));
    assert_eq!(
        cmd,
        core::Command::Reserve {
            reservation: core::ReservationId(2),
            funding: core::Funding::Waterfall(core::Waterfall {
                amount: 80,
                sources: vec![
                    core::Source {
                        account: account_id(2),
                        floor: core::SourceFloor::At(core::Balance::negative(40)),
                        cap: None
                    },
                    core::Source {
                        account: account_id(1),
                        floor: core::SourceFloor::Configured,
                        cap: Some(0)
                    }
                ],
                destinations: vec![core::Line::credit(account_id(3), 80)],
                shortfall: core::Shortfall::RequireFull
            }),
            conditions: vec![core::Condition::AvailableAtLeast {
                account: account_id(2),
                amount: core::Balance::negative(15)
            }]
        }
    );
}

#[test]
fn collection_limits_are_admission_errors_and_empty_lines_are_a_business_decision() {
    let mut post = v1::PostEntryRequest {
        ledger_id: "7".into(),
        request_id: "9".into(),
        funding: Some(v1::Funding {
            kind: Some(v1::funding::Kind::Explicit(v1::ExplicitFunding::default())),
        }),
        conditions: vec![],
        corrects: None,
    };
    let (id, command) = post.clone().into_command(LEDGER).unwrap();
    let mut ledger = core::Ledger::new(LEDGER, "USD", 2);
    let result = decision(ledger.apply(id, command));
    assert!(
        matches!(result.result,Some(v1::decision::Result::Rejected(v1::Rejection{code,context:None})) if code==v1::ErrorCode::TooFewLines as i32)
    );
    post.conditions = vec![v1::Condition::default(); 1025];
    assert_eq!(
        post.into_command(LEDGER).unwrap_err().code(),
        Code::OutOfRange
    );
}

#[test]
fn capture_preserves_allocation_order_and_full_does_not_default_to_keep_or_release() {
    use v1::capture_reservation_request::{Capture, Disposition};
    let capture = v1::CaptureReservationRequest {
        ledger_id: "7".into(),
        request_id: "11".into(),
        reservation_id: "12".into(),
        capture: Some(Capture::Allocations(v1::Allocations {
            lines: vec![
                v1::Allocation {
                    line: 2,
                    amount: "9".into(),
                },
                v1::Allocation {
                    line: 0,
                    amount: "9".into(),
                },
            ],
        })),
        disposition: Some(Disposition::KeepReserved(v1::Empty {})),
    };
    assert_eq!(
        capture.clone().into_command(LEDGER).unwrap().1,
        core::Command::Capture {
            reservation: core::ReservationId(12),
            amount: core::CaptureAmount::Lines(vec![
                core::Allocation { line: 2, amount: 9 },
                core::Allocation { line: 0, amount: 9 }
            ]),
            remainder: Some(core::Remainder::KeepReserved)
        }
    );
    let full = v1::CaptureReservationRequest {
        capture: Some(Capture::Full(v1::Empty {})),
        disposition: None,
        ..capture
    };
    assert_eq!(
        full.into_command(LEDGER).unwrap().1,
        core::Command::Capture {
            reservation: core::ReservationId(12),
            amount: core::CaptureAmount::Full,
            remainder: None
        }
    );
}

#[test]
fn entry_and_rejection_responses_preserve_zero_ids_and_specific_context() {
    use v1::rejection::Context;
    let entry_data = core::JournalEntry {
        id: core::EntryId(u64::MAX),
        lines: vec![
            core::Line::debit(account_id(0), u128::MAX),
            core::Line::credit(account_id(9), u128::MAX),
        ]
        .into(),
        corrects: Some(core::EntryId(0)),
    };
    let got = entry(&entry_data);
    assert_eq!(got.entry_id, u64::MAX);
    assert_eq!(got.corrects, Some(0));
    assert_eq!(got.lines[0].amount, u128::MAX.to_string());
    assert_eq!(got.lines[0].account_id, "0");
    assert_eq!(got.lines[1].side, v1::Side::Credit as i32);
    for (err, code, context) in [
        (
            core::Error::IdConflict(core::RequestId(0)),
            v1::ErrorCode::IdConflict,
            Context::RequestId("0".into()),
        ),
        (
            core::Error::ExcessCapture(0),
            v1::ErrorCode::ExcessCapture,
            Context::Line(0),
        ),
        (
            core::Error::WrongLedger(core::AccountId::new(core::LedgerId(8), 1)),
            v1::ErrorCode::WrongLedger,
            Context::Account(v1::AccountRef {
                ledger_id: "8".into(),
                account_id: "1".into(),
            }),
        ),
    ] {
        assert_eq!(
            decision(Err(err)).result,
            Some(v1::decision::Result::Rejected(v1::Rejection {
                code: code as i32,
                context: Some(context)
            }))
        );
    }
}

#[test]
fn protobuf_field_order_does_not_change_request_identity() {
    use prost::Message;
    // CreateAccountRequest's ledger_id, request_id, account_id and normal,
    // encoded in two wire orders. Both produce the same canonical command.
    let first = b"\x0a\x017\x12\x015\x1a\x013\x28\x02";
    let reordered = b"\x28\x02\x1a\x013\x12\x015\x0a\x017";
    let mut ledger = core::Ledger::new(LEDGER, "USD", 2);
    for bytes in [first, reordered] {
        let wire = v1::CreateAccountRequest::decode(bytes.as_slice()).unwrap();
        let (id, command) = wire.into_command(LEDGER).unwrap();
        assert_eq!(
            ledger.apply(id, command),
            Ok(core::Outcome::AccountCreated(account_id(3)))
        );
    }
    assert_eq!(ledger.decisions().len(), 1);
}

#[test]
fn mutation_choice_presence_and_business_disposition_are_preserved() {
    let mut post = v1::PostEntryRequest {
        ledger_id: "7".into(),
        request_id: "1".into(),
        funding: Some(v1::Funding { kind: None }),
        ..Default::default()
    };
    assert_eq!(
        post.clone().into_command(LEDGER).unwrap_err().code(),
        Code::InvalidArgument
    );
    post.funding = Some(v1::Funding {
        kind: Some(v1::funding::Kind::Explicit(v1::ExplicitFunding {
            lines: vec![v1::Line {
                account_id: "1".into(),
                side: i32::MAX,
                amount: "0".into(),
            }],
        })),
    });
    assert_eq!(
        post.into_command(LEDGER).unwrap_err().code(),
        Code::InvalidArgument
    );

    let mut capture = v1::CaptureReservationRequest {
        ledger_id: "7".into(),
        request_id: "2".into(),
        reservation_id: "3".into(),
        capture: None,
        disposition: None,
    };
    assert_eq!(
        capture.clone().into_command(LEDGER).unwrap_err().code(),
        Code::InvalidArgument
    );
    capture.capture = Some(v1::capture_reservation_request::Capture::Amount("1".into()));
    assert_eq!(
        capture.into_command(LEDGER).unwrap().1,
        core::Command::Capture {
            reservation: core::ReservationId(3),
            amount: core::CaptureAmount::Amount(1),
            remainder: None
        }
    );
    let void = v1::VoidReservationRequest {
        ledger_id: "7".into(),
        request_id: "2".into(),
        reservation_id: "4".into(),
    };
    assert_eq!(
        void.into_command(LEDGER).unwrap(),
        (
            core::RequestId(2),
            core::Command::Void {
                reservation: core::ReservationId(4)
            }
        )
    );
}

#[test]
fn rust_and_cpp_protobuf_round_trip_extreme_balances_and_counters() {
    use prost::Message;
    use std::io::Write;
    use std::process::{Command, Stdio};

    fn protoc(mode: &str, payload: &[u8]) -> Vec<u8> {
        // Use the same protoc executable the Cargo build needs for code generation.
        let mut child = Command::new(option_env!("PROTOC").unwrap_or("protoc"))
            .args([
                &format!("--{mode}=ledger.v1.Account"),
                "--proto_path=proto",
                "ledger/v1/ledger.proto",
            ])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start C++ protobuf codec");
        child.stdin.take().unwrap().write_all(payload).unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }

    let mut ledger = core::Ledger::new(LEDGER, "USD", 2);
    for id in [1, 2] {
        ledger
            .apply(
                core::RequestId(id),
                core::Command::CreateAccount {
                    account: account_id(id),
                    normal: core::Side::Credit,
                    code: 0,
                    bounds: core::Bounds {
                        min: None,
                        max: Some(core::Balance::positive(u128::MAX)),
                    },
                },
            )
            .unwrap();
    }
    ledger
        .apply(
            core::RequestId(3),
            core::Command::Post {
                funding: core::Funding::Explicit(vec![
                    core::Line::debit(account_id(1), u128::MAX),
                    core::Line::credit(account_id(2), u128::MAX),
                ]),
                conditions: vec![],
                corrects: None,
            },
        )
        .unwrap();
    for (id, balance) in [(1, format!("-{}", u128::MAX)), (2, u128::MAX.to_string())] {
        let expected = account(ledger.account(account_id(id)).unwrap());
        let wire = expected.encode_to_vec();
        let text = protoc("decode", &wire);
        let fields = std::str::from_utf8(&text).unwrap();
        assert!(fields.contains(&format!("posted_balance: \"{balance}\"")));
        assert!(fields.contains(&format!("available_balance: \"{balance}\"")));
        let side = if id == 1 {
            "debits_posted"
        } else {
            "credits_posted"
        };
        assert!(fields.contains(&format!("{side}: \"{}\"", u128::MAX)));
        let encoded = protoc("encode", &text);
        assert_eq!(v1::Account::decode(encoded.as_slice()).unwrap(), expected);
    }
}
