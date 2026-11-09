use std::sync::Arc;

use crate::{Error, Line, ReservationId};

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReservationStatus {
    Open,
    Captured,
    Voided,
}

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[cfg_attr(feature = "projection-serde", serde(try_from = "StoredReservation"))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reservation {
    pub id: ReservationId,
    pub lines: Arc<[Line]>,
    pub remaining: Vec<u128>,
    pub status: ReservationStatus,
}

#[cfg(feature = "projection-serde")]
#[derive(serde::Deserialize)]
struct StoredReservation {
    id: ReservationId,
    #[serde(deserialize_with = "crate::stored::lines")]
    lines: Arc<[Line]>,
    #[serde(deserialize_with = "crate::stored::amounts")]
    remaining: Vec<u128>,
    status: ReservationStatus,
}

#[cfg(feature = "projection-serde")]
impl TryFrom<StoredReservation> for Reservation {
    type Error = &'static str;
    fn try_from(row: StoredReservation) -> Result<Self, Self::Error> {
        crate::funding::validate_lines(&row.lines)
            .map_err(|_| "invalid original reservation lines")?;
        if row.remaining.len() != row.lines.len()
            || row.lines.iter().zip(&row.remaining).any(|(line, n)| {
                *n > line.amount || line.account.ledger != row.lines[0].account.ledger
            })
        {
            return Err("invalid reservation remainder");
        }
        match row.status {
            ReservationStatus::Open => {
                let outstanding = lines_with_amounts(&row.lines, &row.remaining);
                crate::funding::validate_lines(&outstanding)
                    .map_err(|_| "invalid open reservation remainder")?;
            }
            ReservationStatus::Voided | ReservationStatus::Captured => {
                if row.remaining.iter().any(|n| *n != 0) {
                    return Err("closed reservation has a remainder");
                }
            }
        }
        Ok(Self {
            id: row.id,
            lines: row.lines,
            remaining: row.remaining,
            status: row.status,
        })
    }
}

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Allocation {
    /// Zero-based position in the original reservation.
    pub line: u32,
    pub amount: u128,
}

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CaptureAmount {
    Full,
    Amount(u128),
    Lines(
        #[cfg_attr(
            feature = "projection-serde",
            serde(deserialize_with = "crate::stored::inputs")
        )]
        Vec<Allocation>,
    ),
}

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Remainder {
    KeepReserved,
    Release,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Resolution {
    pub(crate) reservation: Reservation,
    pub(crate) posted: Vec<Line>,
    /// Total deductions from pending counters, including amounts moved to posted.
    pub(crate) released: Vec<Line>,
}

pub(crate) fn open(id: ReservationId, lines: impl Into<Arc<[Line]>>) -> Reservation {
    let lines = lines.into();
    let remaining = lines.iter().map(|line| line.amount).collect();
    Reservation {
        id,
        lines,
        remaining,
        status: ReservationStatus::Open,
    }
}

fn ensure_open(hold: &Reservation) -> Result<(), Error> {
    if hold.status != ReservationStatus::Open {
        return Err(Error::ClosedReservation(hold.id));
    }
    Ok(())
}

fn lines_with_amounts(lines: &[Line], amounts: &[u128]) -> Vec<Line> {
    let mut result = Vec::new();
    for (line, &amount) in lines.iter().zip(amounts) {
        if amount != 0 {
            result.push(Line { amount, ..*line });
        }
    }
    result
}

fn capture_amounts(hold: &Reservation, request: &CaptureAmount) -> Result<Vec<u128>, Error> {
    match request {
        CaptureAmount::Full => Ok(hold.remaining.clone()),
        CaptureAmount::Amount(amount) => {
            if hold.lines.len() != 2 {
                return Err(Error::InvalidCapture);
            }
            if *amount == 0 {
                return Err(Error::ZeroAmount);
            }
            for (i, &remaining) in hold.remaining.iter().enumerate() {
                if *amount > remaining {
                    return Err(Error::ExcessCapture(i as u32));
                }
            }
            Ok(vec![*amount; 2])
        }
        CaptureAmount::Lines(allocations) => {
            let mut amounts = vec![0; hold.lines.len()];
            for allocation in allocations {
                let i = allocation.line as usize;
                let amount = amounts.get_mut(i).ok_or(Error::InvalidCapture)?;
                if *amount != 0 {
                    return Err(Error::DuplicateAllocation(allocation.line));
                }
                if allocation.amount == 0 {
                    return Err(Error::ZeroAmount);
                }
                if allocation.amount > hold.remaining[i] {
                    return Err(Error::ExcessCapture(allocation.line));
                }
                *amount = allocation.amount;
            }
            Ok(amounts)
        }
    }
}

pub(crate) fn plan_capture(
    hold: &Reservation,
    amount: &CaptureAmount,
    remainder: Option<Remainder>,
) -> Result<Resolution, Error> {
    ensure_open(hold)?;
    let amounts = capture_amounts(hold, amount)?;
    let posted = lines_with_amounts(&hold.lines, &amounts);
    crate::funding::validate_lines(&posted)?;

    let full = amounts == hold.remaining;
    if !full && remainder.is_none() {
        return Err(Error::MissingDisposition);
    }

    let mut reservation = hold.clone();
    let released = if full || remainder == Some(Remainder::Release) {
        reservation.status = ReservationStatus::Captured;
        reservation.remaining.fill(0);
        lines_with_amounts(&hold.lines, &hold.remaining)
    } else {
        for (remaining, amount) in reservation.remaining.iter_mut().zip(amounts) {
            *remaining -= amount;
        }
        posted.clone()
    };
    Ok(Resolution {
        reservation,
        posted,
        released,
    })
}

pub(crate) fn plan_void(hold: &Reservation) -> Result<Resolution, Error> {
    ensure_open(hold)?;
    let released = lines_with_amounts(&hold.lines, &hold.remaining);
    let mut reservation = hold.clone();
    reservation.remaining.fill(0);
    reservation.status = ReservationStatus::Voided;
    Ok(Resolution {
        reservation,
        posted: Vec::new(),
        released,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AccountId, LedgerId, Side};

    fn lines(quantities: &[u128]) -> Vec<Line> {
        quantities
            .iter()
            .enumerate()
            .map(|(i, &amount)| Line {
                account: AccountId::new(LedgerId(1), i as u128 + 1),
                side: if i == 0 { Side::Debit } else { Side::Credit },
                amount,
            })
            .collect()
    }

    fn split(allocations: &[(u32, u128)]) -> CaptureAmount {
        CaptureAmount::Lines(
            allocations
                .iter()
                .map(|&(line, amount)| Allocation { line, amount })
                .collect(),
        )
    }

    #[test]
    fn opening_preserves_line_ids_and_initial_amounts() {
        let hold = open(ReservationId(1), lines(&[10000, 9700, 300]));
        assert_eq!(hold.id, ReservationId(1));
        assert_eq!(hold.status, ReservationStatus::Open);
        assert_eq!(hold.lines.as_ref(), lines(&[10000, 9700, 300]));
        assert_eq!(hold.remaining, vec![10000, 9700, 300]);
    }

    #[test]
    fn partial_capture_keeps_a_balanced_remainder_for_full_capture() {
        let hold = open(ReservationId(1), lines(&[10000, 9700, 300]));
        let capture = split(&[(2, 210), (0, 7000), (1, 6790)]);
        let first = plan_capture(&hold, &capture, Some(Remainder::KeepReserved)).unwrap();
        assert_eq!(first.posted, lines(&[7000, 6790, 210]));
        assert_eq!(first.released, first.posted);
        assert_eq!(first.reservation.remaining, vec![3000, 2910, 90]);
        assert_eq!(first.reservation.status, ReservationStatus::Open);
        assert_eq!(first.reservation.lines, hold.lines);
        let second = plan_capture(&first.reservation, &CaptureAmount::Full, None).unwrap();
        assert_eq!(second.posted, lines(&[3000, 2910, 90]));
        assert_eq!(second.released, second.posted);
        assert_eq!(second.reservation.remaining, vec![0, 0, 0]);
        assert_eq!(second.reservation.status, ReservationStatus::Captured);
    }

    #[test]
    fn partial_release_closes_hold_and_removes_all_remaining_pending() {
        let hold = open(ReservationId(2), lines(&[10000, 9700, 300]));
        let capture = split(&[(0, 7000), (1, 7000)]);
        let result = plan_capture(&hold, &capture, Some(Remainder::Release)).unwrap();
        assert_eq!(result.posted, lines(&[7000, 7000]));
        assert_eq!(result.released, hold.lines.as_ref());
        assert_eq!(result.reservation.remaining, vec![0, 0, 0]);
        assert_eq!(result.reservation.status, ReservationStatus::Captured);
        assert_eq!(
            plan_void(&result.reservation),
            Err(Error::ClosedReservation(hold.id))
        );
    }

    #[test]
    fn void_after_capture_releases_only_outstanding_lines() {
        let hold = open(ReservationId(3), lines(&[100, 70, 30]));
        let capture = split(&[(0, 70), (1, 70)]);
        let partial = plan_capture(&hold, &capture, Some(Remainder::KeepReserved)).unwrap();
        let result = plan_void(&partial.reservation).unwrap();
        assert!(result.posted.is_empty());
        let outstanding = lines(&[30, 0, 30]);
        assert_eq!(result.released, vec![outstanding[0], outstanding[2]]);
        assert_eq!(result.reservation.remaining, vec![0, 0, 0]);
        assert_eq!(result.reservation.status, ReservationStatus::Voided);
        assert_eq!(
            plan_capture(&result.reservation, &CaptureAmount::Full, None),
            Err(Error::ClosedReservation(hold.id))
        );
    }

    #[test]
    fn allocation_validation_is_line_specific_and_preserves_hold() {
        let hold = open(ReservationId(4), lines(&[10000, 9700, 300]));
        for (capture, error) in [
            (CaptureAmount::Amount(7000), Error::InvalidCapture),
            (
                split(&[(0, 7000), (1, 6600), (2, 400)]),
                Error::ExcessCapture(2),
            ),
            (
                split(&[(0, 70), (1, 10), (1, 60)]),
                Error::DuplicateAllocation(1),
            ),
            (split(&[(0, 70), (1, 69)]), Error::Unbalanced),
            (split(&[(0, 70), (8, 70)]), Error::InvalidCapture),
            (split(&[(0, 70), (1, 70), (2, 0)]), Error::ZeroAmount),
        ] {
            assert_eq!(
                plan_capture(&hold, &capture, Some(Remainder::Release)),
                Err(error)
            );
        }
        assert_eq!(hold.remaining, vec![10000, 9700, 300]);
    }

    #[test]
    fn two_line_amount_requires_disposition_only_if_partial() {
        let hold = open(ReservationId(5), lines(&[u128::MAX, u128::MAX]));
        assert_eq!(
            plan_capture(&hold, &CaptureAmount::Amount(0), None),
            Err(Error::ZeroAmount)
        );
        assert_eq!(
            plan_capture(&hold, &CaptureAmount::Amount(1), None),
            Err(Error::MissingDisposition)
        );
        let partial = plan_capture(
            &hold,
            &CaptureAmount::Amount(1),
            Some(Remainder::KeepReserved),
        )
        .unwrap();
        assert_eq!(partial.posted, lines(&[1, 1]));
        let full = plan_capture(
            &partial.reservation,
            &CaptureAmount::Amount(u128::MAX - 1),
            None,
        )
        .unwrap();
        assert_eq!(full.reservation.status, ReservationStatus::Captured);
        assert_eq!(full.posted, lines(&[u128::MAX - 1, u128::MAX - 1]));
    }
}
