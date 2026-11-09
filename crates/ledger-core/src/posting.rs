use std::collections::BTreeMap;

use crate::{Account, AccountId, Error, Line, Side};

#[derive(Clone, Copy)]
enum Change {
    Release,
    Post,
    Reserve,
}

impl Change {
    fn apply(self, account: &mut Account, line: &Line) -> Result<(), Error> {
        let counters = &mut account.counters;
        let (posted, pending) = match line.side {
            Side::Debit => (&mut counters.debits_posted, &mut counters.debits_pending),
            Side::Credit => (&mut counters.credits_posted, &mut counters.credits_pending),
        };
        match self {
            Self::Release => {
                *pending = pending
                    .checked_sub(line.amount)
                    .ok_or(Error::InvalidCapture)?
            }
            Self::Post => {
                *posted = posted
                    .checked_add(line.amount)
                    .ok_or(Error::CounterOverflow(account.id))?
            }
            Self::Reserve => {
                *pending = pending
                    .checked_add(line.amount)
                    .ok_or(Error::CounterOverflow(account.id))?
            }
        }
        Ok(())
    }
}

pub(crate) fn stage(
    accounts: &BTreeMap<AccountId, Account>,
    posted: &[Line],
    reserved: &[Line],
    released: &[Line],
) -> Result<BTreeMap<AccountId, Account>, Error> {
    let mut changes = BTreeMap::<AccountId, Vec<(Change, &Line)>>::new();
    for (change, lines) in [
        (Change::Release, released),
        (Change::Post, posted),
        (Change::Reserve, reserved),
    ] {
        for line in lines {
            changes
                .entry(line.account)
                .or_default()
                .push((change, line));
        }
    }

    let mut updates = BTreeMap::new();
    let mut first_policy_error = None;
    for (id, changes) in changes {
        let mut account = accounts.get(&id).ok_or(Error::UnknownAccount(id))?.clone();
        for (change, line) in changes {
            change.apply(&mut account, line)?;
        }
        match account.validate() {
            Ok(()) => {}
            Err(error @ Error::CounterOverflow(_)) => return Err(error),
            // Report a policy failure only after every account has passed its capacity checks.
            Err(error) => {
                first_policy_error.get_or_insert(error);
            }
        }
        updates.insert(id, account);
    }
    match first_policy_error {
        Some(error) => Err(error),
        None => Ok(updates),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Balance, Bounds, Counters, LedgerId, Side};

    fn id(number: u128) -> AccountId {
        AccountId::new(LedgerId(1), number)
    }

    fn accounts(counters: [Counters; 3]) -> BTreeMap<AccountId, Account> {
        counters
            .into_iter()
            .enumerate()
            .map(|(i, counters)| {
                let account = Account {
                    id: id(i as u128 + 1),
                    code: 0,
                    normal: Side::Credit,
                    bounds: Bounds {
                        min: Some(Balance::default()),
                        max: None,
                    },
                    counters,
                };
                (account.id, account)
            })
            .collect()
    }

    #[test]
    fn immediate_post_checks_net_balance_but_preserves_gross_totals() {
        let mut state = accounts([Counters::default(); 3]);
        state.get_mut(&id(1)).unwrap().counters.credits_posted = 50;
        let lines = [
            Line::debit(id(1), 60),
            Line::credit(id(1), 20),
            Line::credit(id(2), 40),
        ];
        let updates = stage(&state, &lines, &[], &[]).unwrap();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[&id(1)].counters.debits_posted, 60);
        assert_eq!(updates[&id(1)].counters.credits_posted, 70);
        assert_eq!(updates[&id(2)].counters.credits_posted, 40);
        assert_eq!(
            stage(&state, &[], &lines, &[]),
            Err(Error::BelowMinimum(id(1)))
        );
    }

    #[test]
    fn captures_at_capacity_release_the_pending_amount_before_posting() {
        let mut state = accounts([Counters::default(); 3]);
        state.get_mut(&id(1)).unwrap().counters = Counters {
            debits_posted: u128::MAX - 1,
            credits_posted: u128::MAX,
            debits_pending: 1,
            credits_pending: 0,
        };
        state.get_mut(&id(2)).unwrap().counters.credits_pending = 1;
        let lines = [Line::debit(id(1), 1), Line::credit(id(2), 1)];
        let updates = stage(&state, &lines, &[], &lines).unwrap();
        assert_eq!(updates[&id(1)].counters.debits_posted, u128::MAX);
        assert_eq!(updates[&id(1)].counters.debits_pending, 0);
        assert_eq!(updates[&id(2)].counters.credits_pending, 0);

        let before = state.clone();
        let excess_release = [lines[0], lines[1], lines[1]];
        assert_eq!(
            stage(&state, &lines, &[], &excess_release),
            Err(Error::InvalidCapture)
        );
        assert_eq!(state, before);
    }

    #[test]
    fn overflow_precedes_policy_and_uses_canonical_account_order() {
        let mut state = accounts([Counters::default(); 3]);
        for number in [2, 3] {
            state.get_mut(&id(number)).unwrap().counters.credits_posted = u128::MAX - 1;
            state.get_mut(&id(number)).unwrap().counters.credits_pending = 1;
        }
        let lines = [
            Line::credit(id(3), 2),
            Line::credit(id(2), 1),
            Line::debit(id(1), 3),
        ];
        assert_eq!(
            stage(&state, &lines, &[], &[]),
            Err(Error::CounterOverflow(id(2)))
        );
    }
}
