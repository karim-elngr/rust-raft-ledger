//! Deterministic, in-memory accounting rules for one ledger.
//!
//! A replica feeds ordered commands to [`Ledger`]. Its return values are local
//! business decisions. Replication and offsite durability govern when a service
//! may acknowledge them to clients.

mod account;
mod command;
mod engine;
mod error;
mod evaluation;
mod funding;
mod id;
mod posting;
mod reservation;
#[cfg(feature = "projection-serde")]
mod stored;

pub use account::{Account, Balance, Bounds, Counters, Side};
pub use command::{Command, Condition, JournalEntry, Outcome};
pub use engine::{Decision, Ledger};
pub use error::Error;
pub use evaluation::{Changes, EvaluationError, StateView, evaluate};
pub use funding::{Funding, Line, Shortfall, Source, SourceFloor, Waterfall};
pub use id::{AccountId, EntryId, LedgerId, RequestId, ReservationId};
pub use reservation::{Allocation, CaptureAmount, Remainder, Reservation, ReservationStatus};
