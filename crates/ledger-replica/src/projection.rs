//! One Raft owner serializes reads and writes to this rebuildable database.
//! Reopening discards the working generation and replays the durable Raft log.
mod checkpoint;
mod delta;
mod export;
mod open;
#[cfg(test)]
mod tests;

use std::path::Path;
use std::sync::Arc;

use crate::account_cache::{AccountCache, AccountCacheStats};
use crate::{ReplicaError, codec::decode_batch};
use bincode::Options as _;
use ledger_core::{
    Account, AccountId, Counters, Decision, EntryId, JournalEntry, Ledger, LedgerId, Outcome,
    RequestId, Reservation, ReservationId,
};
use raft::eraftpb::{Entry, EntryType};
use rocksdb::{ColumnFamily, DB, FlushOptions, IteratorMode, WriteOptions};
use serde::de::DeserializeOwned;

const ACCOUNTS: &str = "accounts";
const RESERVATIONS: &str = "reservations";
const REQUESTS: &str = "requests_by_id";
const ENTRIES: &str = "entries_by_id";
const HISTORY: &str = "decision_history";
const FAMILIES: [&str; 6] = [
    "default",
    ACCOUNTS,
    RESERVATIONS,
    HISTORY,
    REQUESTS,
    ENTRIES,
];
const APPLIED: &[u8] = b"projection/v2/applied";
const LAST_ENTRY: &[u8] = b"projection/v2/last_entry";
const LEDGER_ID: &[u8] = b"projection/v2/ledger_id";
type Position = [u8; 10];

fn invalid(reason: &str) -> ReplicaError {
    ReplicaError::Invalid(reason.to_owned())
}

fn format() -> impl bincode::Options {
    bincode::DefaultOptions::new()
        .with_little_endian()
        .with_fixint_encoding()
        .with_limit(8 << 20)
        .reject_trailing_bytes()
}

// Every first decision has one history row. A posted journal entry lives in its
// decision outcome; both indexes point here. Big endian preserves commit order.
fn position(index: u64, slot: u16) -> Position {
    let mut pos = [0; 10];
    pos[..8].copy_from_slice(&index.to_be_bytes());
    pos[8..].copy_from_slice(&slot.to_be_bytes());
    pos
}

fn journal_in(decision: &Decision) -> Option<&JournalEntry> {
    match &decision.outcome {
        Ok(Outcome::Posted(entry) | Outcome::Captured { entry, .. }) => Some(entry),
        _ => None,
    }
}

pub(crate) struct Projection {
    db: Arc<DB>,
    id: LedgerId,
    cache: AccountCache,
    applied: u64,
    last_entry: EntryId,
    #[cfg(test)]
    fail_next_write: bool,
    #[cfg(test)]
    stop_after_write: bool,
}

impl Projection {
    pub(crate) fn check_private_build_db(path: &Path) -> Result<(), ReplicaError> {
        open::check_directory(path, false)
    }
    #[cfg(test)]
    pub(crate) fn reset(path: &Path, genesis: Ledger) -> Result<Self, ReplicaError> {
        let budget = crate::CacheBudget::default();
        Self::reset_with_cache(path, genesis, budget.account_bytes, budget.block_bytes)
    }

    pub(crate) fn reset_with_cache(
        path: &Path,
        genesis: Ledger,
        cache_bytes: usize,
        block_cache_bytes: usize,
    ) -> Result<Self, ReplicaError> {
        if !genesis.decisions().is_empty() {
            return Err(invalid("projection reset requires a genesis ledger"));
        }
        let id = genesis.id();
        let cache = AccountCache::new(cache_bytes)?;
        let db = open::open(path, id, block_cache_bytes)?;
        Ok(Self::loaded(db, id, cache, 0, EntryId(0)))
    }

    fn loaded(
        db: DB,
        id: LedgerId,
        cache: AccountCache,
        applied: u64,
        last_entry: EntryId,
    ) -> Self {
        Self {
            db: Arc::new(db),
            id,
            cache,
            applied,
            last_entry,
            #[cfg(test)]
            fail_next_write: false,
            #[cfg(test)]
            stop_after_write: false,
        }
    }

    pub(crate) fn id(&self) -> LedgerId {
        self.id
    }
    pub(crate) fn applied_index(&self) -> u64 {
        self.applied
    }
    pub(crate) fn clear_cache(&mut self) {
        self.cache.clear();
    }
    pub(crate) fn cache_stats(&self) -> AccountCacheStats {
        self.cache.stats()
    }

    /// Complete a flush of every projection family. Recovery still replays Raft.
    pub(crate) fn flush(&self) -> Result<(), ReplicaError> {
        let mut options = FlushOptions::default();
        options.set_wait(true);
        self.db
            .flush_cfs_opt(&FAMILIES.map(|name| self.cf(name)), &options)?;
        Ok(())
    }

    fn cf(&self, name: &str) -> &ColumnFamily {
        self.db
            .cf_handle(name)
            .expect("projection opens every declared column family")
    }

    fn read<T: DeserializeOwned>(
        &self,
        family: &str,
        key: &[u8],
    ) -> Result<Option<T>, ReplicaError> {
        self.db
            .get_pinned_cf(self.cf(family), key)?
            .map(|v| format().deserialize(&v).map_err(ReplicaError::from))
            .transpose()
    }

    fn stored_account(&self, id: AccountId) -> Result<Option<Account>, ReplicaError> {
        if id.ledger != self.id {
            return Ok(None);
        }
        let account: Option<Account> = self.read(ACCOUNTS, &id.number.to_be_bytes())?;
        if account.as_ref().is_some_and(|a| a.id() != id) {
            return Err(invalid("account key does not match stored identity"));
        }
        Ok(account)
    }

    pub(crate) fn account(&self, id: AccountId) -> Result<Option<Account>, ReplicaError> {
        if id.ledger != self.id {
            return Ok(None);
        }
        self.cache
            .get_or_load(id, self.applied, || self.stored_account(id))
    }

    pub(crate) fn stored_counters(&self, id: AccountId) -> Result<Option<Counters>, ReplicaError> {
        Ok(self.stored_account(id)?.map(|a| a.counters()))
    }

    pub(crate) fn reservation(
        &self,
        id: ReservationId,
    ) -> Result<Option<Reservation>, ReplicaError> {
        let hold: Option<Reservation> = self.read(RESERVATIONS, &id.0.to_be_bytes())?;
        // StoredReservation already checks that the lines form a nonempty
        // balanced hold in one ledger. Bind that ledger to this projection.
        if hold.as_ref().is_some_and(|h| {
            h.id != id
                || h.lines
                    .first()
                    .is_none_or(|line| line.account.ledger != self.id)
        }) {
            return Err(invalid(
                "reservation row belongs to a different ledger or ID",
            ));
        }
        Ok(hold)
    }

    fn position(&self, family: &str, key: &[u8]) -> Result<Option<Position>, ReplicaError> {
        let Some(bytes) = self.db.get_pinned_cf(self.cf(family), key)? else {
            return Ok(None);
        };
        let pos: Position = bytes
            .as_ref()
            .try_into()
            .map_err(|_| invalid("invalid history index pointer"))?;
        self.validate_position(&pos)?;
        Ok(Some(pos))
    }

    fn validate_position(&self, pos: &Position) -> Result<(), ReplicaError> {
        let index = u64::from_be_bytes(pos[..8].try_into().expect("fixed position width"));
        let slot = u16::from_be_bytes(pos[8..].try_into().expect("fixed position width"));
        if index == 0
            || index > self.applied
            || usize::from(slot) >= crate::ProposalBatch::MAX_COMMANDS
        {
            return Err(invalid("history position outside the applied prefix"));
        }
        Ok(())
    }

    fn at(&self, pos: &Position) -> Result<Decision, ReplicaError> {
        self.read(HISTORY, pos)?
            .ok_or_else(|| invalid("index points to missing decision history"))
    }

    pub(crate) fn decision(&self, id: RequestId) -> Result<Option<Decision>, ReplicaError> {
        let Some(pos) = self.position(REQUESTS, &id.0.to_be_bytes())? else {
            return Ok(None);
        };
        let decision = self.at(&pos)?;
        if decision.request != id {
            return Err(invalid("request index points to another decision"));
        }
        Ok(Some(decision))
    }

    pub(crate) fn entry(&self, id: EntryId) -> Result<Option<JournalEntry>, ReplicaError> {
        let allocated = id.0 != 0 && id.0 <= self.last_entry.0;
        let Some(pos) = self.position(ENTRIES, &id.0.to_be_bytes())? else {
            return if allocated {
                Err(invalid("missing index for an allocated journal entry"))
            } else {
                Ok(None)
            };
        };
        let decision = self.at(&pos)?;
        let entry = journal_in(&decision)
            .filter(|e| allocated && e.id == id)
            .ok_or_else(|| invalid("journal index points to another decision"))?;
        Ok(Some(entry.clone()))
    }

    pub(crate) fn apply(&mut self, entry: &Entry) -> Result<(), ReplicaError> {
        if self.applied.checked_add(1) != Some(entry.index) {
            return Err(invalid("projection requires contiguous committed entries"));
        }
        if entry.get_entry_type() != EntryType::EntryNormal {
            return Err(invalid("unsupported Raft configuration entry"));
        }
        let mut delta = delta::Delta::new(self);
        if !entry.data.is_empty() {
            for (slot, (id, command)) in decode_batch(self.id, &entry.data)?.into_iter().enumerate()
            {
                delta.stage(position(entry.index, slot as u16), id, command)?;
            }
        }
        let (batch, accounts, last_entry) = delta.finish(entry.index)?;
        let mut opts = WriteOptions::default();
        opts.disable_wal(true);
        opts.set_sync(false);
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_write) {
            return Err(invalid("injected projection write failure"));
        }
        self.db.write_opt(batch, &opts)?;
        #[cfg(test)]
        if std::mem::take(&mut self.stop_after_write) {
            return Err(invalid("injected stop after storage publication"));
        }
        for account in accounts.values() {
            self.cache.publish(account, entry.index);
        }
        self.last_entry = last_entry;
        self.applied = entry.index;
        Ok(())
    }

    /// Expensive diagnostic for callers explicitly requesting the whole ledger.
    /// Point reads and command application never build this reference state.
    pub(crate) fn inspect_ledger(&self, mut genesis: Ledger) -> Result<Ledger, ReplicaError> {
        if genesis.id() != self.id || !genesis.decisions().is_empty() {
            return Err(invalid("diagnostic replay requires a matching genesis"));
        }
        for row in self.db.iterator_cf(self.cf(HISTORY), IteratorMode::Start) {
            let (key, bytes) = row?;
            let pos: Position = key
                .as_ref()
                .try_into()
                .map_err(|_| invalid("invalid decision history key"))?;
            self.validate_position(&pos)?;
            let decision: Decision = format().deserialize(&bytes)?;
            if self.position(REQUESTS, &decision.request.0.to_be_bytes())? != Some(pos)
                || genesis.request_outcome(decision.request).is_some()
            {
                return Err(invalid("inconsistent first request history"));
            }
            if let Some(entry) = journal_in(&decision)
                && self.position(ENTRIES, &entry.id.0.to_be_bytes())? != Some(pos)
            {
                return Err(invalid("inconsistent journal entry history"));
            }
            if genesis.apply(decision.request, decision.command) != decision.outcome {
                return Err(invalid("replayed decision differs from stored outcome"));
            }
        }
        if genesis.journal().len() as u64 != self.last_entry.0 {
            return Err(invalid(
                "replayed journal length differs from published frontier",
            ));
        }
        Ok(genesis)
    }
}
