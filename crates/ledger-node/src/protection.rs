//! The voter keeps processing Raft messages while its worker checks the independent
//! journal. Serving requires a confirmation started after the request arrived.
use std::time::Duration;

use ledger_replica::{JournalConfirmation, JournalVerifier, VerificationError};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::NodeError;

const RETRY_DELAY: Duration = Duration::from_millis(100);
const FOLLOWER_INTERVAL: Duration = Duration::from_secs(5);

impl From<VerificationError> for NodeError {
    fn from(error: VerificationError) -> Self {
        match error {
            VerificationError::Fenced => Self::Fenced,
            VerificationError::Journal(error) => Self::Journal(error),
            VerificationError::Replica(error) => Self::Replica(error),
        }
    }
}

struct Round {
    verifier: JournalVerifier,
    serving: bool,
    result: Result<JournalConfirmation, VerificationError>,
}

pub(crate) struct Protection {
    verifier: Option<JournalVerifier>,
    task: Option<JoinHandle<Round>>,
    frontier: u64,
    pub(crate) append_publications: u64,
    started: u64,
    confirmed: Option<(u64, u64)>,
    retry_at: Instant,
    next_observation: Instant,
    pub(crate) fenced: bool,
    pub(crate) last_error: Option<String>,
}

impl Protection {
    pub(crate) fn new(verifier: JournalVerifier) -> Self {
        Self {
            verifier: Some(verifier),
            task: None,
            frontier: 0,
            append_publications: 0,
            started: 0,
            confirmed: None,
            retry_at: Instant::now(),
            next_observation: Instant::now(),
            fenced: false,
            last_error: None,
        }
    }

    pub(crate) fn frontier(&self) -> u64 {
        self.frontier
    }
    pub(crate) fn next_round(&self) -> u64 {
        self.started + 1
    }
    pub(crate) fn covers(&self, round: u64, index: u64) -> bool {
        !self.fenced
            && self
                .confirmed
                .is_some_and(|(last_round, covered)| last_round >= round && covered >= index)
    }

    pub(crate) fn schedule(&mut self, applied: u64, needs_confirmation: bool, leader: bool) {
        let now = Instant::now();
        if self.fenced || self.task.is_some() || now < self.retry_at {
            return;
        }
        if leader {
            if applied <= self.frontier && !needs_confirmation {
                return;
            }
        } else {
            if now < self.next_observation
                || applied.saturating_sub(self.frontier) < JournalVerifier::ANCHOR_INTERVAL
            {
                return;
            }
            self.next_observation = now + FOLLOWER_INTERVAL;
        }
        let mut verifier = self.verifier.take().expect("idle journal verifier");
        self.started += 1;
        self.task = Some(tokio::task::spawn_blocking(move || {
            let result = if leader {
                verifier.confirm_through(applied)
            } else {
                verifier.observe()
            };
            Round {
                verifier,
                serving: leader,
                result,
            }
        }));
    }

    pub(crate) async fn poll(state: Option<&mut Self>) -> Result<(), NodeError> {
        let Some(this) = state else {
            return std::future::pending().await;
        };
        let Some(task) = &mut this.task else {
            return std::future::pending().await;
        };
        let round = task.await.map_err(|_| NodeError::ThreadFailed)?;
        this.task = None;
        this.append_publications = round.verifier.append_publications();
        this.verifier = Some(round.verifier);
        match round.result {
            Ok(confirmation) => {
                this.frontier = confirmation.head.index;
                if round.serving {
                    this.confirmed = Some((this.started, this.frontier));
                }
                this.last_error = None;
            }
            Err(error) => {
                this.fenced = matches!(error, VerificationError::Fenced);
                this.last_error = Some(error.to_string());
                this.retry_at = Instant::now() + RETRY_DELAY;
            }
        }
        Ok(())
    }

    // Wait for any issued disk operation and release the durable log handle.
    pub(crate) async fn join(&mut self) {
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
