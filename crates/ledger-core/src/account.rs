use crate::{AccountId, Error};
use std::cmp::Ordering;
use std::fmt;

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Debit,
    Credit,
}

impl Side {
    pub const fn opposite(self) -> Self {
        match self {
            Self::Debit => Self::Credit,
            Self::Credit => Self::Debit,
        }
    }
}

/// A signed balance whose magnitude can use the entire unsigned counter range.
#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[cfg_attr(feature = "projection-serde", serde(try_from = "StoredBalance"))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Balance {
    negative: bool,
    magnitude: u128,
}

#[cfg(feature = "projection-serde")]
#[derive(serde::Deserialize)]
struct StoredBalance {
    negative: bool,
    magnitude: u128,
}

#[cfg(feature = "projection-serde")]
impl TryFrom<StoredBalance> for Balance {
    type Error = &'static str;
    fn try_from(value: StoredBalance) -> Result<Self, Self::Error> {
        if value.negative && value.magnitude == 0 {
            return Err("invalid stored negative zero");
        }
        Ok(Self {
            negative: value.negative,
            magnitude: value.magnitude,
        })
    }
}

impl Balance {
    pub const fn is_negative(self) -> bool {
        self.negative
    }

    pub const fn magnitude(self) -> u128 {
        self.magnitude
    }

    pub const fn positive(magnitude: u128) -> Self {
        Self {
            negative: false,
            magnitude,
        }
    }

    pub const fn negative(magnitude: u128) -> Self {
        Self {
            negative: magnitude != 0,
            magnitude,
        }
    }

    fn difference(increases: u128, decreases: u128) -> Self {
        if increases >= decreases {
            Self::positive(increases - decreases)
        } else {
            Self::negative(decreases - increases)
        }
    }

    /// Spending capacity, capped at the largest quantity a single source can contribute.
    pub fn capacity_above(self, floor: Self) -> u128 {
        match (self.negative, floor.negative) {
            (false, false) => self.magnitude.saturating_sub(floor.magnitude),
            (true, true) => floor.magnitude.saturating_sub(self.magnitude),
            (false, true) => self.magnitude.saturating_add(floor.magnitude),
            (true, false) => 0,
        }
    }
}

impl Ord for Balance {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.negative, other.negative) {
            (false, false) => self.magnitude.cmp(&other.magnitude),
            (true, true) => other.magnitude.cmp(&self.magnitude),
            (false, true) => Ordering::Greater,
            (true, false) => Ordering::Less,
        }
    }
}

impl PartialOrd for Balance {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Balance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.negative {
            formatter.write_str("-")?;
        }
        write!(formatter, "{}", self.magnitude)
    }
}

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Bounds {
    pub min: Option<Balance>,
    pub max: Option<Balance>,
}

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    pub debits_posted: u128,
    pub credits_posted: u128,
    pub debits_pending: u128,
    pub credits_pending: u128,
}

impl Counters {
    fn posted_and_pending(self, side: Side) -> (u128, u128) {
        match side {
            Side::Debit => (self.debits_posted, self.debits_pending),
            Side::Credit => (self.credits_posted, self.credits_pending),
        }
    }
}

/// The ledger constructs accounts and checks their counter and policy invariants.
///
/// ```compile_fail,E0616
/// use ledger_core::{Account, Counters};
/// fn replace_counters(mut account: Account) {
///     account.counters = Counters::default();
/// }
/// ```
#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(try_from = "StoredAccount")
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Account {
    pub(crate) id: AccountId,
    pub(crate) code: u16,
    pub(crate) normal: Side,
    pub(crate) bounds: Bounds,
    pub(crate) counters: Counters,
}

#[cfg(feature = "projection-serde")]
#[derive(serde::Deserialize)]
struct StoredAccount {
    id: AccountId,
    code: u16,
    normal: Side,
    bounds: Bounds,
    counters: Counters,
}

#[cfg(feature = "projection-serde")]
impl TryFrom<StoredAccount> for Account {
    type Error = Error;
    fn try_from(row: StoredAccount) -> Result<Self, Error> {
        Self::from_parts(row.id, row.code, row.normal, row.bounds, row.counters)
    }
}

impl Account {
    /// Reconstruct a checked account from its stored row and ledger-scoped key.
    pub fn from_parts(
        id: AccountId,
        code: u16,
        normal: Side,
        bounds: Bounds,
        counters: Counters,
    ) -> Result<Self, Error> {
        let account = Self {
            id,
            code,
            normal,
            bounds,
            counters,
        };
        account.validate()?;
        Ok(account)
    }

    pub fn id(&self) -> AccountId {
        self.id
    }

    pub fn code(&self) -> u16 {
        self.code
    }

    pub fn normal(&self) -> Side {
        self.normal
    }

    pub fn bounds(&self) -> Bounds {
        self.bounds
    }

    pub fn counters(&self) -> Counters {
        self.counters
    }

    pub fn posted_balance(&self) -> Balance {
        let (increases, _) = self.counters.posted_and_pending(self.normal);
        let (decreases, _) = self.counters.posted_and_pending(self.normal.opposite());
        Balance::difference(increases, decreases)
    }

    pub fn available_balance(&self) -> Balance {
        let (increases, _) = self.counters.posted_and_pending(self.normal);
        let (decreases, pending_decreases) =
            self.counters.posted_and_pending(self.normal.opposite());
        let total_decreases = decreases
            .checked_add(pending_decreases)
            .expect("validated account has capacity to post all pending decreases");
        Balance::difference(increases, total_decreases)
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if matches!((self.bounds.min, self.bounds.max), (Some(min), Some(max)) if min > max) {
            return Err(Error::InvalidBounds(self.id));
        }

        let (increases, pending_increases) = self.counters.posted_and_pending(self.normal);
        let (decreases, pending_decreases) =
            self.counters.posted_and_pending(self.normal.opposite());
        let reserved_increases = increases
            .checked_add(pending_increases)
            .ok_or(Error::CounterOverflow(self.id))?;
        let reserved_decreases = decreases
            .checked_add(pending_decreases)
            .ok_or(Error::CounterOverflow(self.id))?;
        if self
            .bounds
            .min
            .is_some_and(|floor| Balance::difference(increases, reserved_decreases) < floor)
        {
            return Err(Error::BelowMinimum(self.id));
        }
        if self
            .bounds
            .max
            .is_some_and(|ceiling| Balance::difference(reserved_increases, decreases) > ceiling)
        {
            return Err(Error::AboveMaximum(self.id));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LedgerId;

    fn account(normal: Side, counters: Counters) -> Account {
        Account {
            id: AccountId {
                ledger: LedgerId(1),
                number: 7,
            },
            code: 2,
            normal,
            bounds: Bounds::default(),
            counters,
        }
    }

    #[test]
    fn accounts_expose_read_only_fields_and_decimal_balances() {
        let counters = Counters {
            debits_posted: u128::MAX,
            ..Counters::default()
        };
        let wallet = account(Side::Credit, counters);
        assert_eq!(
            wallet.id(),
            AccountId {
                ledger: LedgerId(1),
                number: 7
            }
        );
        assert_eq!(wallet.code(), 2);
        assert_eq!(wallet.normal(), Side::Credit);
        assert_eq!(wallet.bounds(), Bounds::default());
        assert_eq!(wallet.counters(), counters);
        assert_eq!(
            wallet.available_balance().to_string(),
            format!("-{}", u128::MAX)
        );
        assert_eq!(
            Balance::positive(u128::MAX).to_string(),
            u128::MAX.to_string()
        );
        assert_eq!(Balance::negative(0).to_string(), "0");
    }

    #[test]
    fn sign_and_spending_capacity_cover_the_full_range() {
        let max = u128::MAX;
        let zero = Balance::default();
        assert_eq!(Balance::negative(0), zero);
        assert_eq!(Balance::positive(0), zero);
        let ordered = [
            Balance::negative(max),
            Balance::negative(1),
            zero,
            Balance::positive(1),
            Balance::positive(max),
        ];
        assert!(ordered.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            Balance::positive(max).capacity_above(Balance::negative(max)),
            max
        );
        assert_eq!(
            Balance::negative(max - 1).capacity_above(Balance::negative(max)),
            1
        );
        assert_eq!(
            Balance::positive(max).capacity_above(Balance::positive(max - 1)),
            1
        );
        assert_eq!(
            Balance::negative(max).capacity_above(Balance::positive(max)),
            0
        );
        assert_eq!(Balance::positive(2).capacity_above(Balance::negative(5)), 7);
    }

    #[test]
    fn posted_and_available_are_exact_on_both_normal_sides() {
        let max = u128::MAX;
        let counters = Counters {
            credits_posted: max,
            debits_pending: max,
            ..Counters::default()
        };
        let wallet = account(Side::Credit, counters);
        assert_eq!(wallet.normal.opposite(), Side::Debit);
        assert_eq!(wallet.posted_balance(), Balance::positive(max));
        assert_eq!(wallet.available_balance(), Balance::default());
        let asset = account(Side::Debit, counters);
        assert_eq!(asset.normal.opposite(), Side::Credit);
        assert_eq!(asset.posted_balance(), Balance::negative(max));
        assert_eq!(asset.available_balance(), Balance::negative(max));
    }

    #[test]
    fn enforce_pending_decreases_and_increases_independently() {
        let counters = Counters {
            credits_posted: 40,
            debits_pending: 30,
            credits_pending: 20,
            ..Counters::default()
        };
        let mut wallet = account(Side::Credit, counters);
        wallet.bounds = Bounds {
            min: Some(Balance::positive(10)),
            max: Some(Balance::positive(60)),
        };
        assert_eq!(wallet.validate(), Ok(()));
        wallet.bounds.min = Some(Balance::positive(11));
        wallet.bounds.max = Some(Balance::positive(59));
        assert_eq!(wallet.validate(), Err(Error::BelowMinimum(wallet.id)));
        wallet.bounds.min = None;
        assert_eq!(wallet.validate(), Err(Error::AboveMaximum(wallet.id)));

        let mut asset = account(Side::Debit, counters);
        asset.bounds = Bounds {
            min: Some(Balance::negative(60)),
            max: Some(Balance::negative(10)),
        };
        assert_eq!(asset.validate(), Ok(()));
        asset.bounds.min = Some(Balance::negative(59));
        assert_eq!(asset.validate(), Err(Error::BelowMinimum(asset.id)));
        asset.bounds.min = None;
        asset.bounds.max = Some(Balance::negative(11));
        assert_eq!(asset.validate(), Err(Error::AboveMaximum(asset.id)));
    }

    #[test]
    fn reject_inverted_bounds_and_counter_capacity_overflow() {
        let mut wallet = account(Side::Credit, Counters::default());
        wallet.bounds = Bounds {
            min: Some(Balance::positive(1)),
            max: Some(Balance::default()),
        };
        assert_eq!(wallet.validate(), Err(Error::InvalidBounds(wallet.id)));
        wallet.bounds = Bounds::default();
        for counters in [
            Counters {
                debits_posted: u128::MAX,
                debits_pending: 1,
                ..Counters::default()
            },
            Counters {
                credits_posted: u128::MAX,
                credits_pending: 1,
                ..Counters::default()
            },
        ] {
            wallet.counters = counters;
            assert_eq!(wallet.validate(), Err(Error::CounterOverflow(wallet.id)));
        }
    }
}
