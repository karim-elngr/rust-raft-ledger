//! Reads invoked by the learner's blocking export workers. Only the Raft owner
//! samples the live projection. Archive I/O and Kafka delivery run elsewhere.
use std::sync::Arc;

use ledger_export::{EventPage, ExportError, Identity, LedgerFeed, Position, RawEntry, Result};
use ledger_replica::{CommittedSource, Replica};
use tokio::sync::oneshot;

use crate::host::{Control, NodeHandle, enqueue};

struct LearnerFeed {
    identity: Identity,
    committed: CommittedSource,
    node: NodeHandle,
}

pub(crate) fn feed(replica: &Replica, node: &NodeHandle) -> Arc<dyn LedgerFeed> {
    Arc::new(LearnerFeed {
        identity: Identity {
            ledger: replica.ledger_id(),
            generation: replica.cluster_id(),
            asset: replica.asset().into(),
            scale: replica.scale(),
        },
        committed: replica.committed_source(),
        node: node.clone(),
    })
}

impl LearnerFeed {
    fn query<T>(&self, make: impl FnOnce(oneshot::Sender<T>) -> Control) -> Result<T> {
        let (tx, rx) = oneshot::channel();
        enqueue(&self.node.controls, make(tx))
            .map_err(|e| ExportError::Unavailable(e.to_string()))?;
        rx.blocking_recv()
            .map_err(|_| ExportError::Unavailable("learner stopped".into()))
    }
}

impl LedgerFeed for LearnerFeed {
    fn identity(&self) -> Result<Identity> {
        Ok(self.identity.clone())
    }
    fn applied_through(&self) -> Result<u64> {
        self.query(Control::ExportFrontier)
    }
    fn entries_after(&self, after: u64, through: u64) -> Result<Vec<RawEntry>> {
        self.committed
            .export_entries(after, through)
            .map_err(|e| ExportError::Invalid(e.to_string()))
    }
    fn events_after(&self, after: Position, through: u64) -> Result<EventPage> {
        self.query(|reply| Control::ExportPage {
            after,
            through,
            reply,
        })?
        .map_err(|e| ExportError::Invalid(e.to_string()))
    }
}
