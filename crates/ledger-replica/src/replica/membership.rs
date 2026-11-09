use super::{ReplicaConfig, ReplicaError};
use raft::eraftpb::{ConfState, MessageType};
use std::ops::Deref;

/// Three fixed voters and an optional permanent non-voting replica. Membership
/// is chosen once for a generation and is checked again on every reopen.
#[derive(Clone, Debug)]
pub struct StaticReplicaConfig {
    replica: ReplicaConfig,
    learner: Option<u64>,
}

impl ReplicaConfig {
    pub fn with_learner(self, learner: u64) -> StaticReplicaConfig {
        StaticReplicaConfig {
            replica: self,
            learner: Some(learner),
        }
    }

    #[cfg(test)]
    pub(crate) fn identity(&mut self) -> Result<Vec<u8>, ReplicaError> {
        self.voters.sort_unstable();
        StaticReplicaConfig::from(self.clone()).identity()
    }
}

impl From<ReplicaConfig> for StaticReplicaConfig {
    fn from(replica: ReplicaConfig) -> Self {
        Self {
            replica,
            learner: None,
        }
    }
}
impl From<&ReplicaConfig> for StaticReplicaConfig {
    fn from(replica: &ReplicaConfig) -> Self {
        replica.clone().into()
    }
}
impl From<&StaticReplicaConfig> for StaticReplicaConfig {
    fn from(replica: &StaticReplicaConfig) -> Self {
        replica.clone()
    }
}
impl Deref for StaticReplicaConfig {
    type Target = ReplicaConfig;
    fn deref(&self) -> &Self::Target {
        &self.replica
    }
}

impl StaticReplicaConfig {
    pub fn learner_id(&self) -> Option<u64> {
        self.learner
    }
    pub fn is_learner(&self) -> bool {
        self.learner == Some(self.node)
    }
    pub fn members(&self) -> impl Iterator<Item = u64> + '_ {
        self.voters.iter().copied().chain(self.learner)
    }
    pub(crate) fn conf_state(&self) -> ConfState {
        ConfState {
            voters: self.voters.clone(),
            learners: self.learner.into_iter().collect(),
            ..Default::default()
        }
    }
    pub(crate) fn identity(&mut self) -> Result<Vec<u8>, ReplicaError> {
        self.replica.voters.sort_unstable();
        if self.cluster == 0 || self.asset.trim().is_empty() {
            return Err(ReplicaError::Invalid(
                "a cluster ID and asset are required".into(),
            ));
        }
        if self.voters.len() != 3
            || self.voters[0] == 0
            || self.voters.windows(2).any(|ids| ids[0] == ids[1])
            || self
                .learner
                .is_some_and(|id| id == 0 || self.voters.contains(&id))
            || !self.members().any(|id| id == self.node)
        {
            return Err(ReplicaError::Invalid("expected three distinct voters and an optional distinct learner, including the local node".into()));
        }
        let fields = (
            self.cluster,
            self.node,
            &self.voters,
            self.ledger.0,
            &self.asset,
            self.scale,
        );
        Ok(match self.learner {
            Some(learner) => bincode::serialize(&(b"ledger-member-v2", fields, learner))?,
            None => bincode::serialize(&(b"ledger-voter-v1", fields))?,
        })
    }
    pub(crate) fn permits(&self, from: u64, to: u64, kind: MessageType) -> bool {
        role_permits(self.learner, from, to, kind)
    }
}

pub(crate) fn role_permits(learner: Option<u64>, from: u64, to: u64, kind: MessageType) -> bool {
    use MessageType::*;
    if learner == Some(from) {
        matches!(kind, MsgAppendResponse | MsgHeartbeatResponse)
    } else if learner == Some(to) {
        matches!(kind, MsgAppend | MsgHeartbeat | MsgSnapshot)
    } else {
        true
    }
}
