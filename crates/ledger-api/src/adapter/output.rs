use crate::v1;
use ledger_core as core;

fn side(value: core::Side) -> i32 {
    match value {
        core::Side::Debit => v1::Side::Debit as i32,
        core::Side::Credit => v1::Side::Credit as i32,
    }
}

pub fn account(value: &core::Account) -> v1::Account {
    let counters = value.counters();
    let bounds = value.bounds();
    v1::Account {
        ledger_id: value.id().ledger.0.to_string(),
        account_id: value.id().number.to_string(),
        code: u32::from(value.code()),
        normal: side(value.normal()),
        bounds: Some(v1::Bounds {
            min: bounds.min.map(|v| v.to_string()),
            max: bounds.max.map(|v| v.to_string()),
        }),
        counters: Some(v1::Counters {
            debits_posted: counters.debits_posted.to_string(),
            credits_posted: counters.credits_posted.to_string(),
            debits_pending: counters.debits_pending.to_string(),
            credits_pending: counters.credits_pending.to_string(),
        }),
        posted_balance: value.posted_balance().to_string(),
        available_balance: value.available_balance().to_string(),
    }
}

fn lines(values: &[core::Line]) -> Vec<v1::Line> {
    values
        .iter()
        .map(|line| v1::Line {
            account_id: line.account.number.to_string(),
            side: side(line.side),
            amount: line.amount.to_string(),
        })
        .collect()
}

pub fn entry(value: &core::JournalEntry) -> v1::JournalEntry {
    v1::JournalEntry {
        entry_id: value.id.0,
        lines: lines(&value.lines),
        corrects: value.corrects.map(|v| v.0),
    }
}

pub fn reservation(value: &core::Reservation) -> v1::Reservation {
    let status = match value.status {
        core::ReservationStatus::Open => v1::ReservationStatus::Open,
        core::ReservationStatus::Captured => v1::ReservationStatus::Captured,
        core::ReservationStatus::Voided => v1::ReservationStatus::Voided,
    };
    v1::Reservation {
        reservation_id: value.id.0.to_string(),
        status: status as i32,
        lines: lines(&value.lines),
        remaining: value.remaining.iter().map(u128::to_string).collect(),
    }
}

fn outcome(value: core::Outcome) -> v1::Outcome {
    use v1::outcome::Kind;
    let kind = match value {
        core::Outcome::AccountCreated(a) => Kind::AccountCreated(a.number.to_string()),
        core::Outcome::BoundsChanged(a) => Kind::BoundsChanged(a.number.to_string()),
        core::Outcome::Posted(e) => Kind::Posted(entry(&e)),
        core::Outcome::Reserved {
            reservation,
            lines: held,
        } => Kind::Reserved(v1::Reserved {
            reservation_id: reservation.0.to_string(),
            lines: lines(&held),
        }),
        core::Outcome::Captured {
            reservation,
            entry: posted,
        } => Kind::Captured(v1::Captured {
            reservation_id: reservation.0.to_string(),
            entry: Some(entry(&posted)),
        }),
        core::Outcome::Voided(reservation) => Kind::Voided(reservation.0.to_string()),
    };
    v1::Outcome { kind: Some(kind) }
}

fn rejection(error: core::Error) -> v1::Rejection {
    use core::Error as E;
    use v1::{ErrorCode as C, rejection::Context};
    let account_ref = |a: core::AccountId| {
        Some(Context::Account(v1::AccountRef {
            ledger_id: a.ledger.0.to_string(),
            account_id: a.number.to_string(),
        }))
    };
    let reservation_ref = |id: core::ReservationId| Some(Context::ReservationId(id.0.to_string()));
    let (code, context) = match error {
        E::IdConflict(r) => (C::IdConflict, Some(Context::RequestId(r.0.to_string()))),
        E::WrongLedger(a) => (C::WrongLedger, account_ref(a)),
        E::UnknownAccount(a) => (C::UnknownAccount, account_ref(a)),
        E::DuplicateAccount(a) => (C::DuplicateAccount, account_ref(a)),
        E::InvalidBounds(a) => (C::InvalidBounds, account_ref(a)),
        E::CounterOverflow(a) => (C::CounterOverflow, account_ref(a)),
        E::BelowMinimum(a) => (C::BelowMinimum, account_ref(a)),
        E::AboveMaximum(a) => (C::AboveMaximum, account_ref(a)),
        E::InvalidConditionTarget(a) => (C::InvalidConditionTarget, account_ref(a)),
        E::ConditionFailed(a) => (C::ConditionFailed, account_ref(a)),
        E::Unbalanced => (C::Unbalanced, None),
        E::ZeroAmount => (C::ZeroAmount, None),
        E::TooFewLines => (C::TooFewLines, None),
        E::InvalidWaterfall => (C::InvalidWaterfall, None),
        E::DuplicateSource(a) => (C::DuplicateSource, account_ref(a)),
        E::MissingFloor(a) => (C::MissingFloor, account_ref(a)),
        E::InsufficientFunding => (C::InsufficientFunding, None),
        E::UnknownEntry(id) => (C::UnknownEntry, Some(Context::EntryId(id.0))),
        E::DuplicateReservation(id) => (C::DuplicateReservation, reservation_ref(id)),
        E::UnknownReservation(id) => (C::UnknownReservation, reservation_ref(id)),
        E::ClosedReservation(id) => (C::ClosedReservation, reservation_ref(id)),
        E::InvalidCapture => (C::InvalidCapture, None),
        E::MissingDisposition => (C::MissingDisposition, None),
        E::ExcessCapture(i) => (C::ExcessCapture, Some(Context::Line(i))),
        E::DuplicateAllocation(i) => (C::DuplicateAllocation, Some(Context::Line(i))),
    };
    v1::Rejection {
        code: code as i32,
        context,
    }
}

pub fn decision(value: Result<core::Outcome, core::Error>) -> v1::Decision {
    let result = match value {
        Ok(value) => v1::decision::Result::Accepted(outcome(value)),
        Err(error) => v1::decision::Result::Rejected(rejection(error)),
    };
    v1::Decision {
        result: Some(result),
    }
}
