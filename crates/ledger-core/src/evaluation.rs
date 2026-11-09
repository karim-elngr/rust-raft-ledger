//! Evaluate one normalized command against a coherent, read-only ledger view.
use std::collections::{BTreeMap, BTreeSet};

use crate::funding::resolve;
use crate::posting::stage;
use crate::reservation::{Resolution, open, plan_capture, plan_void};
use crate::{
    Account, AccountId, Command, Condition, Counters, EntryId, Error, Funding, JournalEntry,
    LedgerId, Line, Outcome, Reservation, ReservationId,
};

/// Each lookup sees the same ledger version, including earlier commands in this
/// Raft batch. A returned row must match its lookup key. Missing rows are None.
pub trait StateView {
    type Error;
    fn account(&mut self, id: AccountId) -> Result<Option<Account>, Self::Error>;
    fn reservation(&mut self, id: ReservationId) -> Result<Option<Reservation>, Self::Error>;
    fn entry_exists(&mut self, id: EntryId) -> Result<bool, Self::Error>;
}

/// Accepted modifications. The caller publishes them atomically with the decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Changes {
    pub accounts: BTreeMap<AccountId, Account>,
    pub reservation: Option<Reservation>,
    pub outcome: Outcome,
}

impl Changes {
    pub fn entry(&self) -> Option<&JournalEntry> {
        match &self.outcome {
            Outcome::Posted(entry) | Outcome::Captured { entry, .. } => Some(entry),
            _ => None,
        }
    }
}

/// Infrastructure failures stop apply. Only the inner business rejection from
/// evaluate is eligible to become a stored request decision.
#[derive(Debug, PartialEq, Eq)]
pub enum EvaluationError<E> {
    Storage(E),
    JournalFull,
}

/// Evaluate a first request without writing to the backing view. The caller owns
/// retry identity. `last_entry` is zero at genesis and advances only on publication.
pub fn evaluate<V: StateView>(
    ledger: LedgerId,
    last_entry: EntryId,
    command: &Command,
    view: &mut V,
) -> Result<Result<Changes, Error>, EvaluationError<V::Error>> {
    if let Command::Post {
        corrects: Some(id), ..
    } = command
        && !view.entry_exists(*id).map_err(EvaluationError::Storage)?
    {
        return Ok(Err(Error::UnknownEntry(*id)));
    }
    let mut resolution = None;
    let mut required = BTreeSet::new();
    match command {
        Command::CreateAccount { account, .. } | Command::SetAccountBounds { account, .. } => {
            required.insert(*account);
        }
        Command::Post { funding, .. } => funding_accounts(funding, &mut required),
        Command::Reserve {
            reservation,
            funding,
            ..
        } => {
            if view
                .reservation(*reservation)
                .map_err(EvaluationError::Storage)?
                .is_some()
            {
                return Ok(Err(Error::DuplicateReservation(*reservation)));
            }
            funding_accounts(funding, &mut required);
        }
        Command::Capture { reservation, .. } | Command::Void { reservation } => {
            let Some(existing) = view
                .reservation(*reservation)
                .map_err(EvaluationError::Storage)?
            else {
                return Ok(Err(Error::UnknownReservation(*reservation)));
            };
            let plan = if let Command::Capture {
                amount, remainder, ..
            } = command
            {
                plan_capture(&existing, amount, *remainder)
            } else {
                plan_void(&existing)
            };
            let plan = match plan {
                Ok(plan) => plan,
                Err(rejection) => return Ok(Err(rejection)),
            };
            // Each posted capture amount is also released from pending.
            required.extend(plan.released.iter().map(|l| l.account));
            resolution = Some(plan);
        }
    }

    let mut accounts = BTreeMap::new();
    for id in required {
        // Foreign accounts are rejected by the ledger. Never look up another
        // ledger's data, even when it uses the same account numbers.
        if id.ledger == ledger
            && let Some(account) = view.account(id).map_err(EvaluationError::Storage)?
        {
            accounts.insert(id, account);
        }
    }
    let state = WorkingSet {
        ledger,
        accounts,
        last_entry,
    };
    match state.execute(command, resolution) {
        Ok(changes) => Ok(Ok(changes)),
        Err(PlanError::Rejected(reason)) => Ok(Err(reason)),
        Err(PlanError::JournalFull) => Err(EvaluationError::JournalFull),
    }
}

fn funding_accounts(funding: &Funding, required: &mut BTreeSet<AccountId>) {
    match funding {
        Funding::Explicit(lines) => required.extend(lines.iter().map(|line| line.account)),
        Funding::Waterfall(w) => {
            required.extend(w.sources.iter().map(|s| s.account));
            required.extend(w.destinations.iter().map(|l| l.account));
        }
    }
}

struct WorkingSet {
    ledger: LedgerId,
    accounts: BTreeMap<AccountId, Account>,
    last_entry: EntryId,
}

enum PlanError {
    Rejected(Error),
    JournalFull,
}

impl From<Error> for PlanError {
    fn from(error: Error) -> Self {
        Self::Rejected(error)
    }
}

impl WorkingSet {
    fn execute(
        &self,
        command: &Command,
        resolution: Option<Resolution>,
    ) -> Result<Changes, PlanError> {
        let mut reservation = None;
        let (accounts, outcome) = match command {
            Command::CreateAccount {
                account,
                code,
                normal,
                bounds,
            } => {
                if account.ledger != self.ledger {
                    return Err(Error::WrongLedger(*account).into());
                }
                if self.accounts.contains_key(account) {
                    return Err(Error::DuplicateAccount(*account).into());
                }
                let new =
                    Account::from_parts(*account, *code, *normal, *bounds, Counters::default())?;
                (
                    BTreeMap::from([(*account, new)]),
                    Outcome::AccountCreated(*account),
                )
            }
            Command::SetAccountBounds { account, bounds } => {
                let mut changed = self.lookup(*account)?.clone();
                changed.bounds = *bounds;
                changed.validate()?;
                (
                    BTreeMap::from([(*account, changed)]),
                    Outcome::BoundsChanged(*account),
                )
            }
            Command::Post {
                funding,
                conditions,
                corrects,
            } => {
                let lines = self.fund(funding, conditions)?;
                let changed = stage(&self.accounts, &lines, &[], &[])?;
                (changed, Outcome::Posted(self.entry(lines, *corrects)?))
            }
            Command::Reserve {
                reservation: id,
                funding,
                conditions,
            } => {
                let lines = self.fund(funding, conditions)?;
                let changed = stage(&self.accounts, &[], &lines, &[])?;
                let hold = open(*id, lines);
                let outcome = Outcome::Reserved {
                    reservation: *id,
                    lines: hold.lines.clone(),
                };
                reservation = Some(hold);
                (changed, outcome)
            }
            Command::Capture {
                reservation: id, ..
            }
            | Command::Void { reservation: id } => {
                return self.resolve(resolution.ok_or(Error::UnknownReservation(*id))?);
            }
        };
        Ok(Changes {
            accounts,
            reservation,
            outcome,
        })
    }

    fn lookup(&self, id: AccountId) -> Result<&Account, Error> {
        if id.ledger != self.ledger {
            return Err(Error::WrongLedger(id));
        }
        self.accounts.get(&id).ok_or(Error::UnknownAccount(id))
    }

    fn fund(&self, funding: &Funding, conditions: &[Condition]) -> Result<Vec<Line>, Error> {
        let lines = resolve(funding, self.ledger, &self.accounts)?;
        if conditions.is_empty() {
            return Ok(lines);
        }
        let mut participants: BTreeSet<_> = lines.iter().map(|line| line.account).collect();
        if let Funding::Waterfall(plan) = funding {
            participants.extend(plan.sources.iter().map(|source| source.account));
        }
        for &Condition::AvailableAtLeast { account, amount } in conditions {
            if !participants.contains(&account) {
                return Err(Error::InvalidConditionTarget(account));
            }
            if self.lookup(account)?.available_balance() < amount {
                return Err(Error::ConditionFailed(account));
            }
        }
        Ok(lines)
    }

    fn resolve(&self, resolution: Resolution) -> Result<Changes, PlanError> {
        let Resolution {
            reservation,
            posted,
            released,
        } = resolution;
        let accounts = stage(&self.accounts, &posted, &[], &released)?;
        let id = reservation.id;
        let outcome = if posted.is_empty() {
            Outcome::Voided(id)
        } else {
            Outcome::Captured {
                reservation: id,
                entry: self.entry(posted, None)?,
            }
        };
        Ok(Changes {
            accounts,
            reservation: Some(reservation),
            outcome,
        })
    }

    fn entry(
        &self,
        lines: Vec<Line>,
        corrects: Option<EntryId>,
    ) -> Result<JournalEntry, PlanError> {
        let id = EntryId(
            self.last_entry
                .0
                .checked_add(1)
                .ok_or(PlanError::JournalFull)?,
        );
        Ok(JournalEntry {
            id,
            lines: lines.into(),
            corrects,
        })
    }
}
