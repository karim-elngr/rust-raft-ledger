//! Read-only adapters over the Raft WAL and deterministic decision history.
use crate::{CommittedSource, ReplicaError};
use ledger_export::RawEntry;

impl CommittedSource {
    pub fn export_entries(&self, after: u64, through: u64) -> Result<Vec<RawEntry>, ReplicaError> {
        self.0
            .committed_page(after, through, |entry, raw| RawEntry {
                index: entry.index,
                term: entry.term,
                bytes: raw.into_vec(),
            })
    }
}
