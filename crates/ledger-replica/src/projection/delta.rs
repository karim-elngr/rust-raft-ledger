use std::collections::{BTreeMap, BTreeSet};

use bincode::Options as _;
use ledger_core::{
    Account, AccountId, Command, Decision, EntryId, EvaluationError, RequestId, Reservation,
    ReservationId, StateView, evaluate,
};
use rocksdb::WriteBatch;

use super::{
    ACCOUNTS, APPLIED, ENTRIES, HISTORY, LAST_ENTRY, Position, Projection, REQUESTS, RESERVATIONS,
    ReplicaError, format, invalid,
};

/// Financial effects and new index rows from this entry only. Concurrent readers
/// cannot observe the WriteBatch, and later commands consult the overlay first.
pub(super) struct Delta<'a> {
    view: &'a Projection,
    accounts: BTreeMap<AccountId, Account>,
    holds: BTreeMap<ReservationId, Reservation>,
    first_requests: BTreeSet<RequestId>,
    last_entry: EntryId,
    writes: WriteBatch,
}

impl<'a> Delta<'a> {
    pub(super) fn new(view: &'a Projection) -> Self {
        Self {
            view,
            accounts: BTreeMap::new(),
            holds: BTreeMap::new(),
            first_requests: BTreeSet::new(),
            last_entry: view.last_entry,
            writes: WriteBatch::default(),
        }
    }

    pub(super) fn stage(
        &mut self,
        pos: Position,
        request: RequestId,
        command: Command,
    ) -> Result<(), ReplicaError> {
        if self.first_requests.contains(&request) || self.view.decision(request)?.is_some() {
            // A duplicate and a conflict both preserve the first immutable row.
            // The response path compares the caller's command with that record.
            return Ok(());
        }
        let outcome = match evaluate(self.view.id, self.last_entry, &command, self) {
            Ok(Ok(changes)) => {
                if let Some(entry) = changes.entry() {
                    self.last_entry = entry.id;
                    self.writes
                        .put_cf(self.view.cf(ENTRIES), entry.id.0.to_be_bytes(), pos);
                }
                self.accounts.extend(changes.accounts);
                if let Some(hold) = changes.reservation {
                    self.holds.insert(hold.id, hold);
                }
                Ok(changes.outcome)
            }
            Ok(Err(business)) => Err(business),
            Err(EvaluationError::Storage(failure)) => return Err(failure),
            Err(EvaluationError::JournalFull) => {
                return Err(invalid("journal entry IDs exhausted"));
            }
        };
        self.writes.put_cf(
            self.view.cf(HISTORY),
            pos,
            format().serialize(&Decision {
                request,
                command,
                outcome,
            })?,
        );
        self.writes
            .put_cf(self.view.cf(REQUESTS), request.0.to_be_bytes(), pos);
        self.first_requests.insert(request);
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        index: u64,
    ) -> Result<(WriteBatch, BTreeMap<AccountId, Account>, EntryId), ReplicaError> {
        for (id, account) in &self.accounts {
            self.writes.put_cf(
                self.view.cf(ACCOUNTS),
                id.number.to_be_bytes(),
                format().serialize(account)?,
            );
        }
        for (id, hold) in &self.holds {
            self.writes.put_cf(
                self.view.cf(RESERVATIONS),
                id.0.to_be_bytes(),
                format().serialize(hold)?,
            );
        }
        if self.view.applied == 0 {
            self.writes
                .put(super::LEDGER_ID, self.view.id.0.to_be_bytes());
        }
        self.writes.put(APPLIED, index.to_be_bytes());
        self.writes.put(LAST_ENTRY, self.last_entry.0.to_be_bytes());
        Ok((self.writes, self.accounts, self.last_entry))
    }
}

impl StateView for Delta<'_> {
    type Error = ReplicaError;

    fn account(&mut self, id: AccountId) -> Result<Option<Account>, ReplicaError> {
        if let Some(account) = self.accounts.get(&id) {
            return Ok(Some(account.clone()));
        }
        self.view.account(id)
    }

    fn reservation(&mut self, id: ReservationId) -> Result<Option<Reservation>, ReplicaError> {
        if let Some(hold) = self.holds.get(&id) {
            return Ok(Some(hold.clone()));
        }
        self.view.reservation(id)
    }

    fn entry_exists(&mut self, id: EntryId) -> Result<bool, ReplicaError> {
        if id.0 > self.view.last_entry.0 {
            return Ok(id.0 <= self.last_entry.0);
        }
        Ok(self.view.entry(id)?.is_some())
    }
}
