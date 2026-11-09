use std::fmt;

use crate::{AccountId, EntryId, RequestId, ReservationId};

/// Deterministic rejections that may be recorded and returned on retry.
#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    IdConflict(RequestId),
    WrongLedger(AccountId),
    UnknownAccount(AccountId),
    DuplicateAccount(AccountId),
    InvalidBounds(AccountId),
    CounterOverflow(AccountId),
    BelowMinimum(AccountId),
    AboveMaximum(AccountId),
    InvalidConditionTarget(AccountId),
    ConditionFailed(AccountId),
    Unbalanced,
    ZeroAmount,
    TooFewLines,
    InvalidWaterfall,
    DuplicateSource(AccountId),
    MissingFloor(AccountId),
    InsufficientFunding,
    UnknownEntry(EntryId),
    DuplicateReservation(ReservationId),
    UnknownReservation(ReservationId),
    ClosedReservation(ReservationId),
    InvalidCapture,
    MissingDisposition,
    ExcessCapture(u32),
    DuplicateAllocation(u32),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Error::*;
        match self {
            IdConflict(id) => write!(f, "request {} already identifies a different command", id.0),
            WrongLedger(a) => write!(f, "account {a:?} belongs to another ledger"),
            UnknownAccount(a) => write!(f, "unknown account {a:?}"),
            DuplicateAccount(a) => write!(f, "account {a:?} already exists"),
            InvalidBounds(a) => write!(f, "invalid bounds on account {a:?}"),
            CounterOverflow(a) => write!(f, "counter capacity exceeded on account {a:?}"),
            BelowMinimum(a) => write!(f, "minimum balance violated on account {a:?}"),
            AboveMaximum(a) => write!(f, "maximum balance violated on account {a:?}"),
            InvalidConditionTarget(a) => {
                write!(f, "condition references nonparticipating account {a:?}")
            }
            ConditionFailed(a) => {
                write!(f, "insufficient available balance for condition on {a:?}")
            }
            Unbalanced => f.write_str("debits and credits do not balance"),
            ZeroAmount => f.write_str("amount must be positive"),
            TooFewLines => f.write_str("entry requires at least two posting lines"),
            InvalidWaterfall => f.write_str("invalid waterfall instructions"),
            DuplicateSource(a) => write!(f, "duplicate waterfall source {a:?}"),
            MissingFloor(a) => write!(f, "account {a:?} has no configured floor"),
            InsufficientFunding => f.write_str("sources cannot fund the complete payment"),
            UnknownEntry(id) => write!(f, "unknown journal entry {}", id.0),
            DuplicateReservation(id) => write!(f, "reservation {} already exists", id.0),
            UnknownReservation(id) => write!(f, "unknown reservation {}", id.0),
            ClosedReservation(id) => write!(f, "reservation {} is closed", id.0),
            InvalidCapture => f.write_str("invalid capture allocation"),
            MissingDisposition => {
                f.write_str("partial capture requires an explicit remainder disposition")
            }
            ExcessCapture(line) => write!(f, "capture exceeds remaining amount on line {line}"),
            DuplicateAllocation(line) => write!(f, "duplicate capture allocation for line {line}"),
        }
    }
}

impl std::error::Error for Error {}
