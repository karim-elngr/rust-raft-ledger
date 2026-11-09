//! A direct-mapped table for one projection generation. Only a point read fills
//! a vacant slot. Publication refreshes resident accounts without polluting the
//! read working set during replay or a burst of writes to unrelated accounts.
use std::cell::RefCell;
use std::hash::{DefaultHasher, Hash, Hasher};

use ledger_core::{Account, AccountId};

use crate::ReplicaError;

/// Account values live inline in the slot array. Revisit the budget calculation
/// if Account ever starts owning dynamic memory.
const _: () = assert!(!std::mem::needs_drop::<Account>());

struct Entry {
    account: Account,
    applied: u64,
}

type Slot = Option<Entry>;

/// The byte budget covers the single fixed-size backing allocation, including
/// empty slots and their tags and alignment. The owner has fixed inline metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AccountCacheStats {
    pub max_bytes: usize,
    pub allocated_bytes: usize,
    pub slot_capacity: usize,
    pub resident_accounts: usize,
    pub hits: u64,
    pub misses: u64,
    /// Each miss issues one RocksDB point read, including absent and failed reads.
    pub rocksdb_reads: u64,
    pub evictions: u64,
}

struct State {
    slots: Box<[Slot]>,
    /// The newest projection view observed by a lookup or publication.
    latest: u64,
    stats: AccountCacheStats,
}

impl State {
    fn slot(&self, id: AccountId) -> Option<usize> {
        if self.slots.is_empty() {
            return None;
        }
        let mut hash = DefaultHasher::new();
        id.hash(&mut hash);
        Some((hash.finish() % self.slots.len() as u64) as usize)
    }
}

/// Accessed only by the serialized projection owner. Returned accounts are
/// independent copies, so callers can pin a view without retaining a cache slot.
pub(crate) struct AccountCache(RefCell<State>);

impl AccountCache {
    pub(crate) fn new(max_bytes: usize) -> Result<Self, ReplicaError> {
        let slot_capacity = max_bytes / size_of::<Slot>();
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(slot_capacity)
            .map_err(|e| ReplicaError::Invalid(format!("could not allocate account cache: {e}")))?;
        slots.resize_with(slot_capacity, || None);
        let stats = AccountCacheStats {
            max_bytes,
            allocated_bytes: slot_capacity * size_of::<Slot>(),
            slot_capacity,
            ..Default::default()
        };
        Ok(Self(RefCell::new(State {
            slots: slots.into_boxed_slice(),
            latest: 0,
            stats,
        })))
    }

    /// Read through the caller's RocksDB view at `applied`. Missing accounts and
    /// storage errors leave the table untouched.
    pub(crate) fn get_or_load(
        &self,
        id: AccountId,
        applied: u64,
        read: impl FnOnce() -> Result<Option<Account>, ReplicaError>,
    ) -> Result<Option<Account>, ReplicaError> {
        let slot = {
            let mut state = self.0.borrow_mut();
            state.latest = state.latest.max(applied);
            let slot = state.slot(id);
            if let Some(entry) = slot.and_then(|i| state.slots[i].as_ref())
                && entry.account.id() == id
                && entry.applied <= applied
            {
                let account = entry.account.clone();
                state.stats.hits = state.stats.hits.saturating_add(1);
                return Ok(Some(account));
            }
            state.stats.misses = state.stats.misses.saturating_add(1);
            state.stats.rocksdb_reads = state.stats.misses;
            slot
        };
        let Some(account) = read()? else {
            return Ok(None);
        };
        if account.id() != id {
            return Err(ReplicaError::Invalid(
                "account row does not match its key".into(),
            ));
        }
        if let Some(i) = slot {
            let mut state = self.0.borrow_mut();
            if state.latest == applied {
                match &state.slots[i] {
                    None => state.stats.resident_accounts += 1,
                    Some(other) if other.account.id() != id => {
                        state.stats.evictions = state.stats.evictions.saturating_add(1);
                    }
                    _ => {}
                }
                state.slots[i] = Some(Entry {
                    account: account.clone(),
                    applied,
                });
            }
        }
        Ok(Some(account))
    }

    /// Call for every touched row after its atomic state batch succeeds. Until
    /// then readers may only observe the previously published values.
    pub(crate) fn publish(&self, account: &Account, applied: u64) {
        let mut state = self.0.borrow_mut();
        if applied < state.latest {
            return;
        }
        state.latest = applied;
        let id = account.id();
        if let Some(i) = state.slot(id)
            && let Some(entry) = &mut state.slots[i]
            && entry.account.id() == id
        {
            *entry = Entry {
                account: account.clone(),
                applied,
            };
        }
    }

    pub(crate) fn stats(&self) -> AccountCacheStats {
        self.0.borrow().stats
    }

    pub(crate) fn clear(&mut self) {
        let state = self.0.get_mut();
        state.slots.fill_with(|| None);
        state.latest = 0;
        state.stats = AccountCacheStats {
            max_bytes: state.stats.max_bytes,
            allocated_bytes: state.stats.allocated_bytes,
            slot_capacity: state.stats.slot_capacity,
            ..Default::default()
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledger_core::{Bounds, Command, Ledger, LedgerId, RequestId, Side};

    fn account(ledger: u128, number: u128, code: u16) -> Account {
        let mut book = Ledger::new(LedgerId(ledger), "USD", 2);
        let id = AccountId::new(book.id(), number);
        book.apply(
            RequestId(1),
            Command::CreateAccount {
                account: id,
                code,
                normal: Side::Credit,
                bounds: Bounds::default(),
            },
        )
        .unwrap();
        book.account(id).unwrap().clone()
    }

    #[test]
    fn full_slot_allocation_fits_the_budget_and_clear_reuses_it() {
        let slot = size_of::<Slot>();
        let row = account(1, 2, 3);
        for budget in [0, slot - 1, slot, slot * 5 - 1] {
            let mut cache = AccountCache::new(budget).unwrap();
            let cold = cache.stats();
            assert_eq!(cold.max_bytes, budget);
            assert_eq!(cold.slot_capacity, budget / slot);
            assert_eq!(cold.allocated_bytes, cold.slot_capacity * slot);
            for _ in 0..2 {
                assert_eq!(
                    cache
                        .get_or_load(row.id(), 4, || Ok(Some(row.clone())))
                        .unwrap()
                        .as_ref(),
                    Some(&row)
                );
            }
            let warm = cache.stats();
            let resident = usize::from(cold.slot_capacity > 0);
            assert_eq!(warm.hits, resident as u64);
            assert_eq!(warm.resident_accounts, resident);
            assert_eq!(warm.rocksdb_reads, 2 - warm.hits);
            assert_eq!(warm.misses, warm.rocksdb_reads);
            cache.clear();
            assert_eq!(cache.stats(), cold);
            assert!(
                cache
                    .get_or_load(row.id(), 1, || Ok(None))
                    .unwrap()
                    .is_none()
            );
        }
        assert!(AccountCache::new(usize::MAX).is_err());
    }

    #[test]
    fn collision_checks_the_entire_key_and_missing_or_failed_reads_do_not_evict() {
        let cache = AccountCache::new(size_of::<Slot>()).unwrap();
        let first = account(1, 7, 4);
        let second = account(2, 7, 9);
        for _ in 0..2 {
            assert!(
                cache
                    .get_or_load(first.id(), 3, || Ok(None))
                    .unwrap()
                    .is_none()
            );
        }
        cache
            .get_or_load(first.id(), 3, || Ok(Some(first.clone())))
            .unwrap();
        cache.publish(&second, 4);
        assert!(
            cache
                .get_or_load(second.id(), 4, || Err(ReplicaError::Stopped))
                .is_err()
        );
        assert_eq!(
            cache
                .get_or_load(first.id(), 4, || panic!("still resident"))
                .unwrap(),
            Some(first.clone())
        );
        assert_eq!(cache.stats().evictions, 0);
        cache
            .get_or_load(second.id(), 5, || Ok(Some(second.clone())))
            .unwrap();
        assert_eq!(cache.stats().evictions, 1);
        assert_eq!(cache.stats().resident_accounts, 1);
        assert_eq!(
            cache
                .get_or_load(second.id(), 6, || panic!("second key must hit"))
                .unwrap(),
            Some(second)
        );
        assert_eq!(
            cache
                .get_or_load(first.id(), 6, || Ok(Some(first.clone())))
                .unwrap(),
            Some(first)
        );
        assert_eq!(cache.stats().evictions, 2);
    }

    #[test]
    fn a_pinned_old_read_cannot_observe_or_replace_a_published_new_version() {
        let cache = AccountCache::new(size_of::<Slot>()).unwrap();
        let old = account(1, 1, 4);
        let new = account(1, 1, 9);
        let id = old.id();
        cache.get_or_load(id, 10, || Ok(Some(old.clone()))).unwrap();
        cache.publish(&new, 11);
        assert_eq!(
            cache.get_or_load(id, 10, || Ok(Some(old.clone()))).unwrap(),
            Some(old.clone())
        );
        assert_eq!(
            cache
                .get_or_load(id, 12, || panic!("new snapshot must hit"))
                .unwrap(),
            Some(new)
        );
        assert_eq!(cache.stats().evictions, 0);
    }

    #[test]
    fn filling_a_miss_after_publication_does_not_reintroduce_an_old_row() {
        let cache = AccountCache::new(size_of::<Slot>()).unwrap();
        let old = account(1, 7, 4);
        let new = account(1, 7, 9);
        let id = old.id();
        assert_eq!(
            cache
                .get_or_load(id, 4, || {
                    cache.publish(&new, 5);
                    Ok(Some(old.clone()))
                })
                .unwrap(),
            Some(old)
        );
        assert_eq!(cache.stats().resident_accounts, 0);
        assert_eq!(
            cache.get_or_load(id, 5, || Ok(Some(new.clone()))).unwrap(),
            Some(new)
        );
    }
}
