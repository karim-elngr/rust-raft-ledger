use crate::{
    Account, AccountId, Command, EntryId, Error, EvaluationError, JournalEntry, LedgerId, Outcome,
    RequestId, Reservation, ReservationId, StateView, evaluate,
};
use std::collections::BTreeMap;
use std::convert::Infallible;

/// The first decision for a request ID, in execution order.
#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decision {
    pub request: RequestId,
    pub command: Command,
    pub outcome: Result<Outcome, Error>,
}

impl Decision {
    pub fn outcome_for(&self, command: &Command) -> Result<Outcome, Error> {
        if &self.command == command {
            self.outcome.clone()
        } else {
            Err(Error::IdConflict(self.request))
        }
    }
}

/// An ordered state machine for a single asset and ledger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ledger {
    id: LedgerId,
    asset: String,
    scale: u8,
    accounts: BTreeMap<AccountId, Account>,
    reservations: BTreeMap<ReservationId, Reservation>,
    journal: Vec<JournalEntry>,
    decisions: Vec<Decision>,
    by_request: BTreeMap<RequestId, usize>,
}

impl Ledger {
    pub fn new(id: LedgerId, asset: impl Into<String>, scale: u8) -> Self {
        Self {
            id,
            asset: asset.into(),
            scale,
            accounts: BTreeMap::new(),
            reservations: BTreeMap::new(),
            journal: Vec::new(),
            decisions: Vec::new(),
            by_request: BTreeMap::new(),
        }
    }

    pub fn id(&self) -> LedgerId {
        self.id
    }
    pub fn asset(&self) -> &str {
        &self.asset
    }
    pub fn scale(&self) -> u8 {
        self.scale
    }
    pub fn account(&self, id: AccountId) -> Option<&Account> {
        self.accounts.get(&id)
    }
    pub fn reservation(&self, id: ReservationId) -> Option<&Reservation> {
        self.reservations.get(&id)
    }
    pub fn journal(&self) -> &[JournalEntry] {
        &self.journal
    }
    pub fn decisions(&self) -> &[Decision] {
        &self.decisions
    }

    pub fn entry(&self, id: EntryId) -> Option<&JournalEntry> {
        let offset = usize::try_from(id.0.checked_sub(1)?).ok()?;
        self.journal.get(offset)
    }

    pub fn request_outcome(&self, id: RequestId) -> Option<&Result<Outcome, Error>> {
        self.by_request
            .get(&id)
            .map(|&offset| &self.decisions[offset].outcome)
    }

    /// Resolve this specific payload against the first decision for its request ID.
    /// An ID-only lookup returns that first decision even if the caller reused
    /// the ID for a different command.
    pub fn outcome_for(
        &self,
        request: RequestId,
        command: &Command,
    ) -> Option<Result<Outcome, Error>> {
        let previous = &self.decisions[*self.by_request.get(&request)?];
        Some(previous.outcome_for(command))
    }

    /// Evaluate once. Both success and rejection survive a matching retry.
    pub fn apply(&mut self, request: RequestId, command: Command) -> Result<Outcome, Error> {
        if let Some(original) = self.outcome_for(request, &command) {
            return original;
        }

        let last_entry = self.journal.last().map_or(EntryId(0), |e| e.id);
        let result = match evaluate(self.id, last_entry, &command, self) {
            Ok(result) => result,
            Err(EvaluationError::Storage(never)) => match never {},
            Err(EvaluationError::JournalFull) => {
                unreachable!("an in-memory journal cannot contain u64::MAX entries")
            }
        };
        let outcome = result.map(|changes| {
            if let Some(entry) = changes.entry() {
                self.journal.push(entry.clone());
            }
            self.accounts.extend(changes.accounts);
            if let Some(hold) = changes.reservation {
                self.reservations.insert(hold.id, hold);
            }
            changes.outcome
        });
        self.by_request.insert(request, self.decisions.len());
        self.decisions.push(Decision {
            request,
            command,
            outcome: outcome.clone(),
        });
        outcome
    }

    /// Execute in the supplied order. A business rejection leaves the next command eligible.
    pub fn apply_batch(
        &mut self,
        commands: impl IntoIterator<Item = (RequestId, Command)>,
    ) -> Vec<Result<Outcome, Error>> {
        commands
            .into_iter()
            .map(|(id, command)| self.apply(id, command))
            .collect()
    }
}

impl StateView for Ledger {
    type Error = Infallible;
    fn account(&mut self, id: AccountId) -> Result<Option<Account>, Self::Error> {
        Ok(Ledger::account(self, id).cloned())
    }
    fn reservation(&mut self, id: ReservationId) -> Result<Option<Reservation>, Self::Error> {
        Ok(Ledger::reservation(self, id).cloned())
    }
    fn entry_exists(&mut self, id: EntryId) -> Result<bool, Self::Error> {
        Ok(self.entry(id).is_some())
    }
}
