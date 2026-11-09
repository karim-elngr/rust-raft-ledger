use super::*;
use crate::ReclaimStats;
use ledger_recovery::Journal;

impl Replica {
    /// Offline maintenance. Authenticate both recovery sources before handing
    /// old raw history to the independent repository and compacting local SSTs.
    pub fn reclaim_log(&self, journal: &Journal) -> Result<ReclaimStats, ReplicaError> {
        self.healthy()?;
        if self.learner_id().is_some() {
            return Err(ReplicaError::Invalid(
                "learner topology retains raw Raft history locally".into(),
            ));
        }
        let _guard = CaptureGuard::acquire(&self.checkpoint_busy)?;
        let spec = journal.spec();
        if spec.ledger != self.config.ledger.0
            || spec.asset != self.config.asset
            || spec.scale != self.config.scale
        {
            return Err(ReplicaError::Invalid(
                "independent history ledger definition differs".into(),
            ));
        }
        let checkpoint = self.scrub_checkpoint()?;
        self.raw
            .store()
            .reclaim(journal, self.config.cluster, checkpoint)
    }
}
