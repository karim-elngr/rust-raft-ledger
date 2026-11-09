//! Verification of one independent journal against one voter's committed Raft
//! prefix. A saved head is evidence about immutable history. Each confirmation
//! still consults the live journal to establish serving authority.
use ledger_recovery::{Head, Journal, JournalError};

use crate::{CommittedSource, Replica, ReplicaError};

#[derive(Debug, thiserror::Error)]
pub enum VerificationError {
    #[error("this cluster no longer owns the active recovery generation")]
    Fenced,
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Replica(#[from] ReplicaError),
}

/// Result of one fresh comparison or publication. Locally sourced entries
/// appended by this operation do not need to be compared a second time.
#[derive(Debug)]
pub struct JournalConfirmation {
    pub head: Head,
    pub compared_entries: u64,
}

pub struct JournalVerifier {
    append_publications: u64,
    source: CommittedSource,
    journal: Journal,
    cluster: u128,
    verified: Option<Head>,
    saved: Option<Head>,
}

impl JournalVerifier {
    /// Bound extra local syncs during normal operation. A full segment may cross
    /// the threshold. The first nonempty confirmed prefix is saved immediately.
    pub const ANCHOR_INTERVAL: u64 = 1_024;

    pub fn open(voter: &Replica, journal: Journal) -> Result<Self, VerificationError> {
        let spec = journal.spec();
        if spec.ledger != voter.ledger_id().0
            || spec.asset != voter.asset()
            || spec.scale != voter.scale()
        {
            return Err(ReplicaError::Invalid(
                "journal ledger definition differs from voter".into(),
            )
            .into());
        }
        let source = voter.committed_source();
        let saved = source.journal_anchor()?;
        let verified = match &saved {
            Some(head) => Some(head.clone()),
            None => source.0.import_proof()?.map(|proof| proof.fence),
        };
        let (generation, fenced) = journal.observed_generation()?;
        if generation != voter.cluster_id() || fenced {
            return Err(VerificationError::Fenced);
        }
        Ok(Self {
            append_publications: 0,
            source,
            journal,
            cluster: voter.cluster_id(),
            saved,
            verified,
        })
    }

    /// Successful append publications by this verifier, excluding observations
    /// and repeated confirmations of an already published head.
    pub fn append_publications(&self) -> u64 {
        self.append_publications
    }

    fn active_head(&self) -> Result<Head, VerificationError> {
        let head = self.journal.head()?;
        if head.cluster != self.cluster || head.fenced {
            return Err(VerificationError::Fenced);
        }
        Ok(head)
    }

    /// A follower can verify independently published history without extending
    /// the journal. This prepares its next leader term or restart.
    pub fn observe(&mut self) -> Result<JournalConfirmation, VerificationError> {
        self.confirm_at(self.active_head()?, None)
    }

    /// Verify the current head and publish at most one bounded batch of new
    /// locally committed entries. The returned index can be below the target.
    pub fn confirm_through(
        &mut self,
        target: u64,
    ) -> Result<JournalConfirmation, VerificationError> {
        self.confirm_at(self.active_head()?, Some(target))
    }

    fn confirm_at(
        &mut self,
        mut head: Head,
        target: Option<u64>,
    ) -> Result<JournalConfirmation, VerificationError> {
        let changed = self.verified.as_ref() != Some(&head);
        let compared_entries = if changed {
            let compared = self.compare_suffix(&head)?;
            // Remember authenticated history even if a concurrent publication
            // wins the final comparison. A retry must still verify its ancestry.
            self.verified = Some(head.clone());
            compared
        } else {
            0
        };
        if let Some(target) = target.filter(|target| *target > head.index) {
            let suffix = self.source.encoded_after(head.index, target)?;
            if suffix.is_empty() {
                return Err(
                    ReplicaError::Invalid("missing durable committed suffix".into()).into(),
                );
            }
            let frames: Vec<_> = suffix.iter().map(Vec::as_slice).collect();
            head = self.journal.append_encoded(&head, &frames)?;
            self.append_publications = self.append_publications.saturating_add(1);
            self.verified = Some(head.clone());
        } else if changed {
            let current = self.active_head()?;
            // A follower can retain a fully checked historical prefix even when
            // the leader keeps publishing faster than the follower can scan.
            // A serving confirmation must cover the exact head it checked.
            if current != head && (target.is_some() || current.index <= head.index) {
                return Err(JournalError::Conflict.into());
            }
        }
        self.save_anchor_if_due(&head)?;
        Ok(JournalConfirmation {
            head,
            compared_entries,
        })
    }

    fn compare_suffix(&self, head: &Head) -> Result<u64, VerificationError> {
        if self.verified.as_ref().is_some_and(|base| {
            head.index < base.index || (head.index == base.index && !base.fenced)
        }) {
            return Err(JournalError::Invalid(
                "journal rolled back or replaced a verified head".into(),
            )
            .into());
        }
        // A lagging voter can cheaply defer a scan it cannot yet complete.
        if self.source.term(head.index)? != head.term {
            return Err(JournalError::Invalid(
                "journal tip term differs from committed Raft term".into(),
            )
            .into());
        }
        let mut compared = 0;
        self.journal
            .verify_since_encoded(self.verified.as_ref(), head, |group| {
                self.source
                    .verify_encoded(group)
                    .map_err(|e| JournalError::Invalid(e.to_string()))?;
                compared += group.entries().len() as u64;
                Ok(())
            })?;
        Ok(compared)
    }

    fn save_anchor_if_due(&mut self, head: &Head) -> Result<(), ReplicaError> {
        if head.index == 0
            || self
                .saved
                .as_ref()
                .is_some_and(|prior| head.index.saturating_sub(prior.index) < Self::ANCHOR_INTERVAL)
        {
            return Ok(());
        }
        match self.source.save_journal_anchor(self.saved.as_ref(), head) {
            Ok(()) => {
                self.saved = Some(head.clone());
                Ok(())
            }
            Err(ReplicaError::JournalAnchorChanged) => {
                // A different verifier won this local compare-and-set. Restart
                // the next comparison from its actual head to prove ancestry.
                self.saved = self.source.journal_anchor()?;
                self.verified.clone_from(&self.saved);
                Err(ReplicaError::JournalAnchorChanged)
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests;
