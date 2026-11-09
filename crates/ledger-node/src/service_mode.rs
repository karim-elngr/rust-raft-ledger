//! The member's role and acknowledgement contract, fixed at startup.
use crate::protection::Protection;
use ledger_replica::Replica;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceMode {
    DiagnosticVoter,
    ProtectedVoter,
    QuorumVoter,
    Learner,
}

impl ServiceMode {
    pub fn serves_clients(self) -> bool {
        matches!(self, Self::ProtectedVoter | Self::QuorumVoter)
    }
}

/// A protected owner always has a live verifier. Handles receive a copy of the
/// mode and cannot change the owner's read or acknowledgement policy.
pub(crate) enum Service {
    DiagnosticVoter,
    ProtectedVoter(Box<Protection>),
    QuorumVoter,
    Learner,
}

impl Service {
    pub(crate) fn new(replica: &Replica, protection: Option<Protection>) -> Self {
        if let Some(worker) = protection {
            Self::ProtectedVoter(Box::new(worker))
        } else if replica.is_learner() {
            Self::Learner
        } else if replica.learner_id().is_some() {
            Self::QuorumVoter
        } else {
            Self::DiagnosticVoter
        }
    }

    pub(crate) fn mode(&self) -> ServiceMode {
        match self {
            Self::DiagnosticVoter => ServiceMode::DiagnosticVoter,
            Self::ProtectedVoter(_) => ServiceMode::ProtectedVoter,
            Self::QuorumVoter => ServiceMode::QuorumVoter,
            Self::Learner => ServiceMode::Learner,
        }
    }

    pub(crate) fn protection(&self) -> Option<&Protection> {
        match self {
            Self::ProtectedVoter(worker) => Some(worker),
            _ => None,
        }
    }

    pub(crate) fn protection_mut(&mut self) -> Option<&mut Protection> {
        match self {
            Self::ProtectedVoter(worker) => Some(worker),
            _ => None,
        }
    }
}
