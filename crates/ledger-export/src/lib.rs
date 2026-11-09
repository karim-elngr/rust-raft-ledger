//! Bounded, asynchronous export of already-applied decisions and original
//! committed log bytes. A delivery cursor belongs to a single destination
//! and ledger generation. Event consumers deduplicate by (ledger, request ID).
//!
//! A learner can publish Kafka-style application events while backup is
//! disabled. In this mode the cursor reports delivery of locally applied
//! decisions. The archive frontier stays absent.
//!
//! ```no_run
//! # use ledger_export::*;
//! # use std::{path::Path,sync::Arc,time::Duration};
//! # fn example(feed:Arc<dyn LedgerFeed>, publisher:Arc<dyn EventPublisher>, directory:&Path)->Result<()> {
//! let exports = Arc::new(ExportWorkers::open(
//!     directory, feed, Some(Arc::new(NoopArchive)), Some(publisher),
//! )?);
//! let tasks = exports.start(Duration::from_millis(250))?;
//! assert_eq!(exports.status().archived_through, None);
//! # tasks.shutdown()?;
//! # Ok(()) }
//! ```
//!
//! With both real sinks enabled, events wait for independently confirmed
//! archive coverage. The event cursor tracks delivery to the broker across
//! archive configuration changes. Enabling or replacing an archive makes
//! new deliveries wait for that archive to catch up. Previously acknowledged
//! broker records keep their original delivery guarantee. With both sinks
//! disabled the exporter starts no threads
//! and creates no cursor files. Each backend call must have a bounded timeout
//! so shutdown can join the workers.
use ledger_core::{Decision, LedgerId};
use serde::{Deserialize, Serialize};

mod cursor;
mod workers;
pub use workers::{ExportStatus, ExportWorkers, RunningWorkers, Step};

pub const MAX_RAW_ENTRIES: usize = 64;
pub const MAX_RAW_BYTES: usize = 8 << 20;
pub const MAX_RAW_ENTRY_BYTES: usize = 4 << 20;
pub const MAX_EVENTS: usize = 64;
pub const MAX_EVENT_BYTES: u64 = 8 << 20;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Identity {
    pub ledger: LedgerId,
    pub generation: u128,
    pub asset: String,
    pub scale: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Position {
    pub index: u64,
    pub slot: u16,
}
impl Position {
    /// All slots, including any empty/no-op entry, have been examined.
    pub const fn after_entry(index: u64) -> Self {
        Self {
            index,
            slot: u16::MAX,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawEntry {
    pub index: u64,
    pub term: u64,
    pub bytes: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventRecord {
    pub position: Position,
    pub decision: Decision,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventPage {
    pub through: Position,
    pub events: Vec<EventRecord>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveHead {
    pub index: u64,
    pub term: u64,
    pub digest: [u8; 32],
}

#[derive(Debug, thiserror::Error)]
pub enum ExportError {
    #[error("invalid export state: {0}")]
    Invalid(String),
    #[error("export temporarily unavailable: {0}")]
    Unavailable(String),
}
pub type Result<T> = std::result::Result<T, ExportError>;

/// One incarnation of a ledger source. Every returned row must belong to the
/// reported identity. Bounds are exclusive at the lower end and inclusive at
/// the upper end. Events are first decisions, ordered by original command slot.
pub trait LedgerFeed: Send + Sync {
    fn identity(&self) -> Result<Identity>;
    fn applied_through(&self) -> Result<u64>;
    fn entries_after(&self, after: u64, through: u64) -> Result<Vec<RawEntry>>;
    fn events_after(&self, after: Position, through: u64) -> Result<EventPage>;
}

pub trait RecoveryArchive: Send + Sync {
    fn enabled(&self) -> bool {
        true
    }
    /// Stable target identifier, unchanged across process restarts.
    fn destination(&self) -> &str;
    /// Return an independently durable, live head in the exact generation.
    fn head(&self, id: &Identity) -> Result<ArchiveHead>;
    /// Compare the expected head, verify every overlapping source row against
    /// the archive byte-for-byte, append the missing suffix, and confirm durability.
    fn append(
        &self,
        id: &Identity,
        expected: &ArchiveHead,
        rows: &[RawEntry],
    ) -> Result<ArchiveHead>;
}

pub trait EventPublisher: Send + Sync {
    fn enabled(&self) -> bool {
        true
    }
    fn destination(&self) -> &str;
    /// Return success only after the destination acknowledged the entire page.
    /// A failure can mean a prefix, or the entire page, is already visible.
    fn publish(&self, id: &Identity, events: &[EventRecord]) -> Result<()>;
}

pub struct NoopArchive;
impl RecoveryArchive for NoopArchive {
    fn enabled(&self) -> bool {
        false
    }
    fn destination(&self) -> &str {
        "disabled"
    }
    fn head(&self, _: &Identity) -> Result<ArchiveHead> {
        Err(ExportError::Unavailable("archive is disabled".into()))
    }
    fn append(&self, id: &Identity, _: &ArchiveHead, _: &[RawEntry]) -> Result<ArchiveHead> {
        self.head(id)
    }
}
pub struct NoopPublisher;
impl EventPublisher for NoopPublisher {
    fn enabled(&self) -> bool {
        false
    }
    fn destination(&self) -> &str {
        "disabled"
    }
    fn publish(&self, _: &Identity, _: &[EventRecord]) -> Result<()> {
        Err(ExportError::Unavailable("publisher is disabled".into()))
    }
}

#[cfg(test)]
mod tests;
