use ledger_api::{MAX_RPC_BYTES, adapter::parse_id, v1};
use prost::Message;
use std::{
    collections::BTreeSet,
    error::Error,
    fs::{self, File},
    io::{BufRead, BufReader, Read},
    path::Path,
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const INVENTORY: &str = "request-ids.txt";

fn regular(path: &Path) -> Result<File> {
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(format!("expected a regular file at {}", path.display()).into());
    }
    Ok(File::open(path)?)
}

pub fn inventory(dir: &Path) -> Result<BTreeSet<u128>> {
    if !fs::symlink_metadata(dir)?.is_dir() {
        return Err("expected a per-ledger receipt directory".into());
    }
    let mut source = BufReader::new(regular(&dir.join(INVENTORY))?);
    let mut ids = BTreeSet::new();
    let mut line = String::new();
    // Up to 39 decimal digits and a CRLF, independent of the overall ledger size.
    while source.by_ref().take(42).read_line(&mut line)? != 0 {
        let value = line.strip_suffix('\n').unwrap_or(&line);
        let id = parse_id(value.strip_suffix('\r').unwrap_or(value))?;
        if !ids.insert(id) {
            return Err(format!("duplicate inventory request ID {id}").into());
        }
        line.clear();
    }
    if ids.is_empty() {
        return Err("empty receipt inventory".into());
    }
    for item in fs::read_dir(dir)? {
        let item = item?;
        let name = item.file_name();
        if name == INVENTORY {
            continue;
        }
        let id = parse_id(name.to_str().ok_or("invalid receipt filename")?)?;
        if !ids.contains(&id) || !item.file_type()?.is_file() {
            return Err(format!("unlisted or non-regular receipt {id}").into());
        }
    }
    Ok(ids)
}

pub fn load(dir: &Path, id: u128) -> Result<v1::Decision> {
    let mut bytes = vec![];
    regular(&dir.join(id.to_string()))?
        .take(MAX_RPC_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_RPC_BYTES {
        return Err("receipt exceeds the v1 message limit".into());
    }
    decode(&bytes)
}

fn valid_lines(lines: &[v1::Line]) -> bool {
    // Mirror ledger_core::funding::validate_lines. A legal batch can exceed
    // u128 in aggregate even though every individual amount fits.
    let mut debits = (0usize, 0u128);
    let mut credits = (0usize, 0u128);
    for line in lines {
        let Ok(amount) = parse_id(&line.amount) else {
            return false;
        };
        if amount == 0 || parse_id(&line.account_id).is_err() {
            return false;
        }
        let (carries, low) = match line.side() {
            v1::Side::Debit => &mut debits,
            v1::Side::Credit => &mut credits,
            v1::Side::Unspecified => return false,
        };
        let (sum, carry) = low.overflowing_add(amount);
        *low = sum;
        *carries += usize::from(carry);
    }
    lines.len() >= 2 && debits == credits
}

fn valid_entry(entry: &v1::JournalEntry) -> bool {
    entry.entry_id != 0
        && entry
            .corrects
            .is_none_or(|id| id > 0 && id < entry.entry_id)
        && valid_lines(&entry.lines)
}

fn valid_rejection(rejection: &v1::Rejection) -> bool {
    use v1::{ErrorCode as C, rejection::Context};
    match (rejection.code(), rejection.context.as_ref()) {
        (
            C::DuplicateReservation | C::UnknownReservation | C::ClosedReservation,
            Some(Context::ReservationId(id)),
        ) => parse_id(id).is_ok(),
        (
            C::WrongLedger
            | C::UnknownAccount
            | C::DuplicateAccount
            | C::InvalidBounds
            | C::CounterOverflow
            | C::BelowMinimum
            | C::AboveMaximum
            | C::InvalidConditionTarget
            | C::ConditionFailed
            | C::DuplicateSource
            | C::MissingFloor,
            Some(Context::Account(account)),
        ) => parse_id(&account.ledger_id).is_ok() && parse_id(&account.account_id).is_ok(),
        (C::UnknownEntry, Some(Context::EntryId(_)))
        | (C::ExcessCapture | C::DuplicateAllocation, Some(Context::Line(_)))
        | (
            C::Unbalanced
            | C::ZeroAmount
            | C::TooFewLines
            | C::InvalidWaterfall
            | C::InsufficientFunding
            | C::InvalidCapture
            | C::MissingDisposition,
            None,
        ) => true,
        _ => false,
    }
}

pub fn decode(bytes: &[u8]) -> Result<v1::Decision> {
    use v1::{decision::Result as D, outcome::Kind};
    let decision = v1::Decision::decode(bytes)?;
    // Prost discards unknown fields and collapses duplicate oneof alternatives.
    // Reject the evidence if decoding has elided bytes. Field order can differ.
    if decision.encoded_len() != bytes.len() {
        return Err("unsupported or redundant protobuf fields in receipt".into());
    }
    let valid = match &decision.result {
        Some(D::Accepted(outcome)) => match &outcome.kind {
            Some(Kind::AccountCreated(id) | Kind::BoundsChanged(id) | Kind::Voided(id)) => {
                parse_id(id).is_ok()
            }
            Some(Kind::Posted(entry)) => valid_entry(entry),
            Some(Kind::Reserved(r)) => parse_id(&r.reservation_id).is_ok() && valid_lines(&r.lines),
            Some(Kind::Captured(c)) => {
                parse_id(&c.reservation_id).is_ok()
                    && c.entry
                        .as_ref()
                        .is_some_and(|e| e.corrects.is_none() && valid_entry(e))
            }
            None => false,
        },
        Some(D::Rejected(rejection)) => valid_rejection(rejection),
        None => false,
    };
    if !valid {
        return Err("incomplete or unsupported v1 decision".into());
    }
    Ok(decision)
}

#[cfg(test)]
mod tests {
    use super::*;
    use v1::decision::Result as D;

    #[test]
    fn verifies_balanced_lines_across_u128_carries() {
        let line = |id: u128, side: v1::Side, amount: u128| v1::Line {
            account_id: id.to_string(),
            side: side.into(),
            amount: amount.to_string(),
        };
        let mut lines = vec![
            line(0, v1::Side::Debit, u128::MAX),
            line(1, v1::Side::Debit, 2),
            line(2, v1::Side::Credit, 1),
        ];
        // Same low bits, different carry.
        assert!(!valid_lines(&lines));
        lines.push(line(3, v1::Side::Credit, u128::MAX));
        lines.push(line(4, v1::Side::Credit, 1));
        assert!(valid_lines(&lines));
        assert!(!valid_lines(&lines[..2]));
        let mut entry = v1::JournalEntry {
            entry_id: 2,
            lines,
            corrects: Some(1),
        };
        assert!(valid_entry(&entry));
        let capture = v1::Decision {
            result: Some(D::Accepted(v1::Outcome {
                kind: Some(v1::outcome::Kind::Captured(v1::Captured {
                    reservation_id: "0".into(),
                    entry: Some(entry.clone()),
                })),
            })),
        };
        assert!(decode(&capture.encode_to_vec()).is_err());
        for id in [0, 2, 3] {
            entry.corrects = Some(id);
            assert!(!valid_entry(&entry));
        }
    }

    #[test]
    fn refuses_incomplete_unknown_and_ambiguous_protobuf_receipts() {
        let accepted = v1::Decision {
            result: Some(D::Accepted(v1::Outcome {
                kind: Some(v1::outcome::Kind::AccountCreated("1".into())),
            })),
        }
        .encode_to_vec();
        let rejected = v1::Decision {
            result: Some(D::Rejected(v1::Rejection {
                code: v1::ErrorCode::Unbalanced.into(),
                context: None,
            })),
        }
        .encode_to_vec();
        assert!(decode(&accepted).is_ok());
        assert!(decode(&rejected).is_ok());
        // UnknownEntry(0), with the oneof context before its field-1 code.
        assert!(decode(&[18, 4, 32, 0, 8, 18]).is_ok());
        for bad in [
            vec![],
            vec![10, 0],
            vec![18, 0],
            [accepted.as_slice(), rejected.as_slice()].concat(),
            [accepted.as_slice(), &[24, 1]].concat(),
            vec![255],
        ] {
            assert!(decode(&bad).is_err());
        }
        let missing_context = v1::Decision {
            result: Some(D::Rejected(v1::Rejection {
                code: v1::ErrorCode::DuplicateAccount.into(),
                context: None,
            })),
        };
        assert!(decode(&missing_context.encode_to_vec()).is_err());
        let conflict = v1::Decision {
            result: Some(D::Rejected(v1::Rejection {
                code: v1::ErrorCode::IdConflict.into(),
                context: Some(v1::rejection::Context::RequestId("0".into())),
            })),
        };
        assert!(decode(&conflict.encode_to_vec()).is_err());
        let zero = v1::Decision {
            result: Some(D::Accepted(v1::Outcome {
                kind: Some(v1::outcome::Kind::AccountCreated("0".into())),
            })),
        };
        assert!(decode(&zero.encode_to_vec()).is_ok());
    }
}
