//! Normalize public requests before proposal. Parsing checks the wire contract;
//! the replicated ledger evaluates balances, references and business rules.
use crate::v1;
use ledger_core as core;
use tonic::Status;

mod output;
pub use output::{account, decision, entry, reservation};

#[cfg(test)]
mod tests;

const MAX_ITEMS: usize = 1024;
type Result<T> = std::result::Result<T, Status>;

fn unsigned(s: &str) -> Result<u128> {
    if s.is_empty()
        || s.len() > 39
        || (s.len() > 1 && s.starts_with('0'))
        || !s.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(Status::invalid_argument(
            "expected a canonical unsigned decimal integer",
        ));
    }
    s.parse()
        .map_err(|_| Status::invalid_argument("integer exceeds u128 range"))
}

pub fn parse_id(s: &str) -> Result<u128> {
    unsigned(s)
}

pub fn check_ledger(s: &str, expected: core::LedgerId) -> Result<()> {
    if parse_id(s)? != expected.0 {
        return Err(Status::invalid_argument(
            "ledger_id does not match the served ledger",
        ));
    }
    Ok(())
}

fn parse_balance(s: &str) -> Result<core::Balance> {
    match s.strip_prefix('-') {
        Some("0") => Err(Status::invalid_argument("negative zero is not canonical")),
        Some(magnitude) => Ok(core::Balance::negative(unsigned(magnitude)?)),
        None => Ok(core::Balance::positive(unsigned(s)?)),
    }
}

fn request_id(ledger: &str, request: &str, expected: core::LedgerId) -> Result<core::RequestId> {
    check_ledger(ledger, expected)?;
    Ok(core::RequestId(parse_id(request)?))
}

fn account_id(ledger: core::LedgerId, number: &str) -> Result<core::AccountId> {
    Ok(core::AccountId::new(ledger, parse_id(number)?))
}

fn bounds(value: v1::Bounds) -> Result<core::Bounds> {
    Ok(core::Bounds {
        min: value.min.as_deref().map(parse_balance).transpose()?,
        max: value.max.as_deref().map(parse_balance).transpose()?,
    })
}

fn side(n: i32) -> Result<core::Side> {
    match v1::Side::try_from(n) {
        Ok(v1::Side::Debit) => Ok(core::Side::Debit),
        Ok(v1::Side::Credit) => Ok(core::Side::Credit),
        _ => Err(Status::invalid_argument("side must be DEBIT or CREDIT")),
    }
}

fn items<T, U>(values: Vec<T>, convert: impl FnMut(T) -> Result<U>) -> Result<Vec<U>> {
    if values.len() > MAX_ITEMS {
        return Err(Status::out_of_range("repeated field exceeds 1024 items"));
    }
    values.into_iter().map(convert).collect()
}

fn line(value: v1::Line, ledger: core::LedgerId) -> Result<core::Line> {
    Ok(core::Line {
        account: account_id(ledger, &value.account_id)?,
        side: side(value.side)?,
        amount: unsigned(&value.amount)?,
    })
}

fn source(value: v1::Source, ledger: core::LedgerId) -> Result<core::Source> {
    use v1::source::Floor;
    let floor = match value.floor {
        Some(Floor::Configured(_)) => core::SourceFloor::Configured,
        Some(Floor::At(floor)) => core::SourceFloor::At(parse_balance(&floor)?),
        None => return Err(Status::invalid_argument("source floor choice is required")),
    };
    Ok(core::Source {
        account: account_id(ledger, &value.account_id)?,
        floor,
        cap: value.cap.as_deref().map(unsigned).transpose()?,
    })
}

fn funding(value: Option<v1::Funding>, ledger: core::LedgerId) -> Result<core::Funding> {
    use v1::funding::Kind;
    match value.and_then(|f| f.kind) {
        Some(Kind::Explicit(explicit)) => {
            Ok(core::Funding::Explicit(items(explicit.lines, |l| {
                line(l, ledger)
            })?))
        }
        Some(Kind::Waterfall(w)) => {
            if v1::Shortfall::try_from(w.shortfall) != Ok(v1::Shortfall::RequireFull) {
                return Err(Status::invalid_argument("shortfall must be REQUIRE_FULL"));
            }
            Ok(core::Funding::Waterfall(core::Waterfall {
                amount: unsigned(&w.amount)?,
                sources: items(w.sources, |s| source(s, ledger))?,
                destinations: items(w.destinations, |l| line(l, ledger))?,
                shortfall: core::Shortfall::RequireFull,
            }))
        }
        None => Err(Status::invalid_argument("funding choice is required")),
    }
}

fn conditions(values: Vec<v1::Condition>, ledger: core::LedgerId) -> Result<Vec<core::Condition>> {
    items(values, |value| match value.kind {
        Some(v1::condition::Kind::AvailableAtLeast(condition)) => {
            Ok(core::Condition::AvailableAtLeast {
                account: account_id(ledger, &condition.account_id)?,
                amount: parse_balance(&condition.amount)?,
            })
        }
        None => Err(Status::invalid_argument("condition choice is required")),
    })
}

pub trait Mutation {
    fn into_command(self, expected: core::LedgerId) -> Result<(core::RequestId, core::Command)>;
}

impl Mutation for v1::CreateAccountRequest {
    fn into_command(self, expected: core::LedgerId) -> Result<(core::RequestId, core::Command)> {
        let id = request_id(&self.ledger_id, &self.request_id, expected)?;
        Ok((
            id,
            core::Command::CreateAccount {
                account: account_id(expected, &self.account_id)?,
                code: u16::try_from(self.code)
                    .map_err(|_| Status::invalid_argument("account code exceeds 65535"))?,
                normal: side(self.normal)?,
                bounds: bounds(self.bounds.unwrap_or_default())?,
            },
        ))
    }
}

impl Mutation for v1::SetAccountBoundsRequest {
    fn into_command(self, expected: core::LedgerId) -> Result<(core::RequestId, core::Command)> {
        let id = request_id(&self.ledger_id, &self.request_id, expected)?;
        let policy = self
            .bounds
            .ok_or_else(|| Status::invalid_argument("bounds message is required"))?;
        Ok((
            id,
            core::Command::SetAccountBounds {
                account: account_id(expected, &self.account_id)?,
                bounds: bounds(policy)?,
            },
        ))
    }
}

impl Mutation for v1::PostEntryRequest {
    fn into_command(self, expected: core::LedgerId) -> Result<(core::RequestId, core::Command)> {
        let id = request_id(&self.ledger_id, &self.request_id, expected)?;
        Ok((
            id,
            core::Command::Post {
                funding: funding(self.funding, expected)?,
                conditions: conditions(self.conditions, expected)?,
                corrects: self.corrects.map(core::EntryId),
            },
        ))
    }
}

impl Mutation for v1::ReserveEntryRequest {
    fn into_command(self, expected: core::LedgerId) -> Result<(core::RequestId, core::Command)> {
        let id = request_id(&self.ledger_id, &self.request_id, expected)?;
        Ok((
            id,
            core::Command::Reserve {
                reservation: core::ReservationId(parse_id(&self.reservation_id)?),
                funding: funding(self.funding, expected)?,
                conditions: conditions(self.conditions, expected)?,
            },
        ))
    }
}

impl Mutation for v1::CaptureReservationRequest {
    fn into_command(self, expected: core::LedgerId) -> Result<(core::RequestId, core::Command)> {
        use v1::capture_reservation_request::{Capture, Disposition};
        let id = request_id(&self.ledger_id, &self.request_id, expected)?;
        let amount = match self.capture {
            Some(Capture::Full(_)) => core::CaptureAmount::Full,
            Some(Capture::Amount(s)) => core::CaptureAmount::Amount(unsigned(&s)?),
            Some(Capture::Allocations(a)) => {
                core::CaptureAmount::Lines(items(a.lines, |allocation| {
                    Ok(core::Allocation {
                        line: allocation.line,
                        amount: unsigned(&allocation.amount)?,
                    })
                })?)
            }
            None => return Err(Status::invalid_argument("capture choice is required")),
        };
        let remainder = match self.disposition {
            None | Some(Disposition::RequireFullCapture(_)) => None,
            Some(Disposition::KeepReserved(_)) => Some(core::Remainder::KeepReserved),
            Some(Disposition::Release(_)) => Some(core::Remainder::Release),
        };
        Ok((
            id,
            core::Command::Capture {
                reservation: core::ReservationId(parse_id(&self.reservation_id)?),
                amount,
                remainder,
            },
        ))
    }
}

impl Mutation for v1::VoidReservationRequest {
    fn into_command(self, expected: core::LedgerId) -> Result<(core::RequestId, core::Command)> {
        let id = request_id(&self.ledger_id, &self.request_id, expected)?;
        Ok((
            id,
            core::Command::Void {
                reservation: core::ReservationId(parse_id(&self.reservation_id)?),
            },
        ))
    }
}
