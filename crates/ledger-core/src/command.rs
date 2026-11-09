use std::sync::Arc;

use crate::{
    AccountId, Balance, Bounds, CaptureAmount, EntryId, Funding, Line, Remainder, ReservationId,
    Side,
};

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Condition {
    AvailableAtLeast { account: AccountId, amount: Balance },
}

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    CreateAccount {
        account: AccountId,
        code: u16,
        normal: Side,
        bounds: Bounds,
    },
    SetAccountBounds {
        account: AccountId,
        bounds: Bounds,
    },
    Post {
        funding: Funding,
        #[cfg_attr(
            feature = "projection-serde",
            serde(deserialize_with = "crate::stored::inputs")
        )]
        conditions: Vec<Condition>,
        corrects: Option<EntryId>,
    },
    Reserve {
        reservation: ReservationId,
        funding: Funding,
        #[cfg_attr(
            feature = "projection-serde",
            serde(deserialize_with = "crate::stored::inputs")
        )]
        conditions: Vec<Condition>,
    },
    Capture {
        reservation: ReservationId,
        amount: CaptureAmount,
        remainder: Option<Remainder>,
    },
    Void {
        reservation: ReservationId,
    },
}

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalEntry {
    pub id: EntryId,
    #[cfg_attr(
        feature = "projection-serde",
        serde(deserialize_with = "crate::stored::lines")
    )]
    pub lines: Arc<[Line]>,
    pub corrects: Option<EntryId>,
}

#[cfg_attr(
    feature = "projection-serde",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    AccountCreated(AccountId),
    BoundsChanged(AccountId),
    Posted(JournalEntry),
    Reserved {
        reservation: ReservationId,
        #[cfg_attr(
            feature = "projection-serde",
            serde(deserialize_with = "crate::stored::lines")
        )]
        lines: Arc<[Line]>,
    },
    Captured {
        reservation: ReservationId,
        entry: JournalEntry,
    },
    Voided(ReservationId),
}
