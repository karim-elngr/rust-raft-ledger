//! Operational observations are local to the addressed member and remain
//! available on a follower and during independent export outages.
use std::sync::Arc;

use ledger_api::{adapter, v1};
use ledger_export::{ExportStatus, ExportWorkers, Position};
use tonic::{Request, Response, Status};

use crate::client_service::{deadline, rpc_status};
use crate::{NodeHandle, ServiceMode};

pub(crate) struct Monitor {
    pub node: NodeHandle,
    pub exports: Option<Arc<ExportWorkers>>,
}

fn progress(applied: u64, state: ExportStatus) -> v1::ExportProgress {
    let fully_scanned = state.event_cursor.map(|position| {
        if position == Position::after_entry(position.index) {
            position.index
        } else {
            position.index.saturating_sub(1)
        }
    });
    v1::ExportProgress {
        saved_archive_index: state.archive_cursor,
        verified_archive_index: state.archived_through,
        archive_lag_entries: state
            .archived_through
            .map(|index| applied.saturating_sub(index)),
        event_index: state.event_cursor.map(|cursor| cursor.index),
        event_slot: state.event_cursor.map(|cursor| u32::from(cursor.slot)),
        event_lag_entries: fully_scanned.map(|index| applied.saturating_sub(index)),
        archive_error: state.archive_error,
        event_error: state.event_error,
    }
}

#[tonic::async_trait]
impl v1::monitor_server::Monitor for Monitor {
    async fn get_status(
        &self,
        r: Request<v1::GetStatusRequest>,
    ) -> Result<Response<v1::GetStatusResponse>, Status> {
        let deadline = deadline(&r)?;
        let ledger = self.node.ledger_id();
        adapter::check_ledger(&r.get_ref().ledger_id, ledger)?;
        let exports = self.exports.as_ref().map(|workers| workers.status());
        let state = tokio::time::timeout_at(deadline, self.node.status())
            .await
            .map_err(|_| Status::deadline_exceeded("member status deadline elapsed"))?
            .map_err(rpc_status)?;
        let mode = match self.node.service_mode() {
            ServiceMode::ProtectedVoter => v1::ServingMode::ProtectedVoter,
            ServiceMode::QuorumVoter => v1::ServingMode::QuorumVoter,
            ServiceMode::Learner => v1::ServingMode::Learner,
            ServiceMode::DiagnosticVoter => v1::ServingMode::DiagnosticVoter,
        };
        Ok(Response::new(v1::GetStatusResponse {
            ledger_id: ledger.0.to_string(),
            generation: state.generation.to_string(),
            node_id: state.node,
            mode: mode as i32,
            is_leader: state.is_leader,
            leader_id: state.leader_id,
            term: state.term,
            applied_index: state.applied_index,
            checkpoint_index: state.checkpoint_index,
            verified_journal_index: state.protected_index,
            protection_error: state.protection_error,
            checkpoint_error: state.checkpoint_error,
            snapshot_maintenance_error: state.snapshot_maintenance_error,
            peers: state
                .peer_matches
                .into_iter()
                .map(|(node_id, matched_index)| v1::PeerProgress {
                    node_id,
                    learner: state.learner_id == Some(node_id),
                    matched_index,
                })
                .collect(),
            exports: exports.map(|x| progress(state.applied_index, x)),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_partial_event_entry_and_an_unverified_archive_remain_outstanding() {
        let sample = progress(
            9,
            ExportStatus {
                archive_cursor: Some(8),
                archived_through: None,
                event_cursor: Some(Position { index: 9, slot: 1 }),
                ..Default::default()
            },
        );
        assert_eq!(sample.saved_archive_index, Some(8));
        assert_eq!(sample.archive_lag_entries, None);
        assert_eq!(sample.event_lag_entries, Some(1));
        let sample = progress(
            9,
            ExportStatus {
                archived_through: Some(7),
                event_cursor: Some(Position::after_entry(9)),
                ..Default::default()
            },
        );
        assert_eq!(sample.archive_lag_entries, Some(2));
        assert_eq!(sample.event_lag_entries, Some(0));
    }
}
