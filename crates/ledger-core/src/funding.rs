use std::collections::{BTreeMap, BTreeSet};

use crate::{Account, AccountId, Balance, Error, LedgerId, Side};

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Line {
    pub account: AccountId,
    pub side: Side,
    pub amount: u128,
}

impl Line {
    pub const fn debit(account: AccountId, amount: u128) -> Self {
        Self {
            account,
            side: Side::Debit,
            amount,
        }
    }

    pub const fn credit(account: AccountId, amount: u128) -> Self {
        Self {
            account,
            side: Side::Credit,
            amount,
        }
    }
}

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Funding {
    Explicit(
        #[cfg_attr(
            feature = "projection-serde",
            serde(deserialize_with = "crate::stored::inputs")
        )]
        Vec<Line>,
    ),
    Waterfall(Waterfall),
}

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Waterfall {
    pub amount: u128,
    #[cfg_attr(
        feature = "projection-serde",
        serde(deserialize_with = "crate::stored::inputs")
    )]
    pub sources: Vec<Source>,
    #[cfg_attr(
        feature = "projection-serde",
        serde(deserialize_with = "crate::stored::inputs")
    )]
    pub destinations: Vec<Line>,
    pub shortfall: Shortfall,
}

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shortfall {
    RequireFull,
}

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Source {
    pub account: AccountId,
    pub floor: SourceFloor,
    pub cap: Option<u128>,
}

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceFloor {
    Configured,
    At(Balance),
}

pub(crate) fn validate_lines(lines: &[Line]) -> Result<(), Error> {
    if lines.len() < 2 {
        return Err(Error::TooFewLines);
    }
    // An amount can carry at most once, so a usize covers all carries in a slice.
    let mut debits = (0usize, 0u128);
    let mut credits = (0usize, 0u128);
    for line in lines {
        if line.amount == 0 {
            return Err(Error::ZeroAmount);
        }
        let (carries, low) = match line.side {
            Side::Debit => &mut debits,
            Side::Credit => &mut credits,
        };
        let (sum, carry) = low.overflowing_add(line.amount);
        *low = sum;
        *carries += usize::from(carry);
    }
    if debits != credits {
        return Err(Error::Unbalanced);
    }
    Ok(())
}

pub(crate) fn resolve(
    funding: &Funding,
    ledger: LedgerId,
    accounts: &BTreeMap<AccountId, Account>,
) -> Result<Vec<Line>, Error> {
    let account = |id: AccountId| {
        if id.ledger != ledger {
            return Err(Error::WrongLedger(id));
        }
        accounts.get(&id).ok_or(Error::UnknownAccount(id))
    };
    let waterfall = match funding {
        Funding::Explicit(lines) => {
            validate_lines(lines)?;
            for line in lines {
                account(line.account)?;
            }
            return Ok(lines.clone());
        }
        Funding::Waterfall(waterfall) => waterfall,
    };
    if waterfall.amount == 0 {
        return Err(Error::ZeroAmount);
    }
    if waterfall.sources.is_empty() {
        return Err(Error::InvalidWaterfall);
    }
    let destination_side = waterfall
        .destinations
        .first()
        .ok_or(Error::InvalidWaterfall)?
        .side;
    let mut source_ids = BTreeSet::new();
    let mut remaining = waterfall.amount;
    let mut lines = Vec::new();
    for source in &waterfall.sources {
        let payer = account(source.account)?;
        if !source_ids.insert(source.account) {
            return Err(Error::DuplicateSource(source.account));
        }
        let spend_side = payer.normal.opposite();
        if spend_side == destination_side {
            return Err(Error::InvalidWaterfall);
        }
        let floor = match source.floor {
            SourceFloor::Configured => payer
                .bounds
                .min
                .ok_or(Error::MissingFloor(source.account))?,
            SourceFloor::At(floor) => payer.bounds.min.map_or(floor, |min| min.max(floor)),
        };
        let capacity = payer.available_balance().capacity_above(floor);
        let amount = capacity.min(source.cap.unwrap_or(u128::MAX)).min(remaining);
        if amount > 0 {
            lines.push(Line {
                account: source.account,
                side: spend_side,
                amount,
            });
            remaining -= amount;
        }
    }

    let mut unallocated = waterfall.amount;
    for destination in &waterfall.destinations {
        account(destination.account)?;
        if destination.amount == 0 {
            return Err(Error::ZeroAmount);
        }
        if destination.side != destination_side || source_ids.contains(&destination.account) {
            return Err(Error::InvalidWaterfall);
        }
        unallocated = unallocated
            .checked_sub(destination.amount)
            .ok_or(Error::InvalidWaterfall)?;
    }
    if unallocated != 0 {
        return Err(Error::InvalidWaterfall);
    }
    match waterfall.shortfall {
        Shortfall::RequireFull if remaining != 0 => return Err(Error::InsufficientFunding),
        Shortfall::RequireFull => {}
    }
    lines.extend_from_slice(&waterfall.destinations);
    Ok(lines)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Bounds, Counters};

    const LEDGER: LedgerId = LedgerId(7);

    fn id(number: u128) -> AccountId {
        AccountId::new(LEDGER, number)
    }

    fn wallet(number: u128, posted: u128, pending_spend: u128) -> Account {
        Account {
            id: id(number),
            code: 1,
            normal: Side::Credit,
            bounds: Bounds {
                min: Some(Balance::positive(0)),
                max: None,
            },
            counters: Counters {
                credits_posted: posted,
                debits_pending: pending_spend,
                ..Counters::default()
            },
        }
    }

    fn fixture() -> (BTreeMap<AccountId, Account>, Waterfall) {
        let accounts = [wallet(1, 40, 10), wallet(2, 90, 0), wallet(3, 0, 0)]
            .into_iter()
            .map(|a| (a.id, a))
            .collect();
        let sources = [id(1), id(2)]
            .into_iter()
            .map(|account| Source {
                account,
                floor: SourceFloor::Configured,
                cap: None,
            })
            .collect();
        let waterfall = Waterfall {
            amount: 80,
            sources,
            destinations: vec![Line::credit(id(3), 80)],
            shortfall: Shortfall::RequireFull,
        };
        (accounts, waterfall)
    }

    fn allocate(
        waterfall: &Waterfall,
        accounts: &BTreeMap<AccountId, Account>,
    ) -> Result<Vec<Line>, Error> {
        resolve(&Funding::Waterfall(waterfall.clone()), LEDGER, accounts)
    }

    #[test]
    fn validates_exact_totals_beyond_a_single_counter() {
        let max = u128::MAX;
        let balanced = [
            Line::debit(id(1), max),
            Line::credit(id(2), max - 1),
            Line::credit(id(3), 2),
            Line::debit(id(4), 1),
        ];
        assert_eq!(validate_lines(&balanced), Ok(()));
        let wrapped_mismatch = [
            Line::debit(id(1), max),
            Line::debit(id(2), 2),
            Line::credit(id(3), 1),
        ];
        assert_eq!(validate_lines(&wrapped_mismatch), Err(Error::Unbalanced));
        assert_eq!(
            validate_lines(&[Line::debit(id(1), 0), Line::credit(id(2), 0)]),
            Err(Error::ZeroAmount)
        );
        assert_eq!(
            validate_lines(&[Line::debit(id(1), 1)]),
            Err(Error::TooFewLines)
        );
    }

    #[test]
    fn allocates_gross_pending_spend_in_source_order() {
        let (accounts, mut waterfall) = fixture();
        assert_eq!(
            allocate(&waterfall, &accounts),
            Ok(vec![
                Line::debit(id(1), 30),
                Line::debit(id(2), 50),
                Line::credit(id(3), 80)
            ])
        );
        waterfall.sources.reverse();
        assert_eq!(
            allocate(&waterfall, &accounts),
            Ok(vec![Line::debit(id(2), 80), Line::credit(id(3), 80)])
        );
    }

    #[test]
    fn stricter_floor_and_cap_limit_contributions() {
        let (mut accounts, mut waterfall) = fixture();
        accounts.get_mut(&id(1)).unwrap().bounds.min = Some(Balance::positive(10));
        waterfall.sources[0].floor = SourceFloor::At(Balance::negative(10));
        waterfall.sources[1].floor = SourceFloor::At(Balance::positive(30));
        assert_eq!(
            allocate(&waterfall, &accounts),
            Ok(vec![
                Line::debit(id(1), 20),
                Line::debit(id(2), 60),
                Line::credit(id(3), 80)
            ])
        );
        waterfall.sources[1].cap = Some(59);
        assert_eq!(
            allocate(&waterfall, &accounts),
            Err(Error::InsufficientFunding)
        );
    }

    #[test]
    fn accepts_one_unit_from_double_maximum_headroom() {
        let (mut accounts, mut waterfall) = fixture();
        let source = accounts.get_mut(&id(1)).unwrap();
        source.counters = Counters {
            credits_posted: u128::MAX,
            ..Counters::default()
        };
        source.bounds.min = Some(Balance::negative(u128::MAX));
        waterfall.amount = 1;
        waterfall.destinations = vec![Line::credit(id(3), 1)];
        assert_eq!(
            allocate(&waterfall, &accounts),
            Ok(vec![Line::debit(id(1), 1), Line::credit(id(3), 1)])
        );
    }

    #[test]
    fn fully_funded_waterfall_still_validates_later_sources() {
        let (mut accounts, mut waterfall) = fixture();
        waterfall.sources.reverse();
        waterfall.sources[1].account = id(999);
        assert_eq!(
            allocate(&waterfall, &accounts),
            Err(Error::UnknownAccount(id(999)))
        );
        let foreign = AccountId::new(LedgerId(8), 1);
        waterfall.sources[1].account = foreign;
        assert_eq!(
            allocate(&waterfall, &accounts),
            Err(Error::WrongLedger(foreign))
        );
        waterfall.sources[1].account = id(2);
        assert_eq!(
            allocate(&waterfall, &accounts),
            Err(Error::DuplicateSource(id(2)))
        );
        waterfall.sources[1].account = id(3);
        assert_eq!(
            allocate(&waterfall, &accounts),
            Err(Error::InvalidWaterfall)
        );
        waterfall.sources[1].account = id(1);
        accounts.get_mut(&id(1)).unwrap().bounds.min = None;
        assert_eq!(
            allocate(&waterfall, &accounts),
            Err(Error::MissingFloor(id(1)))
        );
        waterfall.sources[1].floor = SourceFloor::At(Balance::positive(0));
        accounts.get_mut(&id(1)).unwrap().normal = Side::Debit;
        assert_eq!(
            allocate(&waterfall, &accounts),
            Err(Error::InvalidWaterfall)
        );
    }

    #[test]
    fn validates_all_destination_sides_and_target_sum() {
        let (accounts, mut waterfall) = fixture();
        waterfall.destinations = vec![Line::credit(id(3), 79), Line::debit(id(3), 1)];
        assert_eq!(
            allocate(&waterfall, &accounts),
            Err(Error::InvalidWaterfall)
        );
        waterfall.destinations = vec![Line::credit(id(3), u128::MAX), Line::credit(id(3), 81)];
        assert_eq!(
            allocate(&waterfall, &accounts),
            Err(Error::InvalidWaterfall)
        );
        waterfall.destinations = vec![Line::credit(id(999), 80)];
        assert_eq!(
            allocate(&waterfall, &accounts),
            Err(Error::UnknownAccount(id(999)))
        );
    }

    #[test]
    fn explicit_lines_preserve_order_and_require_accounts_in_the_ledger() {
        let (accounts, _) = fixture();
        let lines = vec![Line::credit(id(3), 4), Line::debit(id(2), 4)];
        assert_eq!(
            resolve(&Funding::Explicit(lines.clone()), LEDGER, &accounts),
            Ok(lines)
        );
        let lines = vec![Line::debit(id(99), 1), Line::credit(id(2), 1)];
        assert_eq!(
            resolve(&Funding::Explicit(lines), LEDGER, &accounts),
            Err(Error::UnknownAccount(id(99)))
        );
    }
}
