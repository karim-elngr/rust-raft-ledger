use ledger_core::{
    Account, AccountId, Command, EntryId, JournalEntry, RequestId, Reservation, ReservationId,
};
use ledger_replica::{Replica, ReplicaError};
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::host::{Admission, Decision, NodeError};
use crate::protection::Protection;

pub(crate) enum Query {
    Outcome(RequestId, Command),
    Account(AccountId),
    Accounts(Vec<AccountId>),
    Entry(EntryId),
    Reservation(ReservationId),
    Receipt(RequestId),
}

pub(crate) enum Value {
    Outcome(Option<Decision>),
    Account(Option<Account>),
    Accounts(Vec<Option<Account>>),
    Entry(Option<JournalEntry>),
    Reservation(Option<Reservation>),
}

impl Query {
    fn sample(&self, replica: &Replica) -> Result<Value, ReplicaError> {
        Ok(match self {
            Self::Outcome(id, cmd) => Value::Outcome(replica.outcome_for(*id, cmd)?),
            Self::Account(id) => Value::Account(replica.account(*id)?),
            Self::Accounts(ids) => Value::Accounts(
                ids.iter()
                    .map(|id| replica.account(*id))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Reservation(id) => Value::Reservation(replica.reservation(*id)?),
            Self::Entry(id) => Value::Entry(replica.entry(*id)?),
            Self::Receipt(id) => Value::Outcome(replica.decision(*id)?.map(|d| d.outcome)),
        })
    }
}

pub(crate) struct ReadRequest {
    pub query: Query,
    pub reply: oneshot::Sender<Result<Value, NodeError>>,
    pub deadline: Instant,
    pub _admission: Admission,
}

enum Phase {
    Failed(NodeError),
    Queued,
    Waiting,
    Barrier(u64),
    Pinned {
        index: u64,
        round: u64,
        value: Value,
    },
}

struct Read {
    request: ReadRequest,
    term: u64,
    phase: Phase,
}

#[derive(Default)]
pub(crate) struct Reads {
    sequence: u64,
    barrier: Option<(u64, Vec<u8>)>,
    pending: Vec<Read>,
}

impl Reads {
    pub(crate) fn request(
        &mut self,
        replica: &mut Replica,
        request: ReadRequest,
    ) -> Result<(), NodeError> {
        if !replica.is_leader() {
            let _ = request
                .reply
                .send(Err(NodeError::NotLeader(replica.leader_id())));
            return Ok(());
        }
        if request.deadline <= Instant::now() || request.reply.is_closed() {
            let _ = request.reply.send(Err(NodeError::Deadline));
            return Ok(());
        }
        self.pending.push(Read {
            term: replica.term(),
            request,
            phase: Phase::Queued,
        });
        self.begin(replica)
    }

    // Reads that arrive after a barrier was issued get a later quorum round.
    // Keep at most one context inside Raft, even if all waiting callers cancel.
    pub(crate) fn begin(&mut self, replica: &mut Replica) -> Result<(), NodeError> {
        if self.barrier.is_some()
            || !self
                .pending
                .iter()
                .any(|r| matches!(r.phase, Phase::Queued))
            || !replica.can_read_index()
        {
            return Ok(());
        }
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| NodeError::Invalid("read sequence exhausted".into()))?;
        let term = replica.term();
        let context = [term.to_be_bytes(), self.sequence.to_be_bytes()].concat();
        replica.request_read_index(context.clone())?;
        self.barrier = Some((term, context));
        for read in &mut self.pending {
            if read.term == term && matches!(read.phase, Phase::Queued) {
                read.phase = Phase::Waiting;
            }
        }
        Ok(())
    }

    pub(crate) fn advance(
        &mut self,
        replica: &mut Replica,
        protection: Option<&Protection>,
    ) -> Result<bool, NodeError> {
        if self
            .barrier
            .as_ref()
            .is_some_and(|(term, _)| !replica.is_leader() || *term != replica.term())
        {
            self.barrier = None;
        }
        for state in replica.take_read_states() {
            if self
                .barrier
                .as_ref()
                .is_some_and(|(_, context)| *context == state.request_ctx)
            {
                self.barrier = None;
                for read in &mut self.pending {
                    if matches!(read.phase, Phase::Waiting) {
                        read.phase = Phase::Barrier(state.index);
                    }
                }
            }
        }
        let mut needs_confirmation = false;
        let applied = replica.applied_index();
        let term = replica.term();
        let leader = replica.is_leader();
        let now = Instant::now();
        for read in &mut self.pending {
            if read.request.reply.is_closed() {
                continue;
            }
            let error = if now >= read.request.deadline {
                Some(NodeError::Deadline)
            } else if protection.is_some_and(|p| p.fenced) {
                Some(NodeError::Fenced)
            } else if !leader || term != read.term {
                Some(NodeError::LeadershipChanged)
            } else {
                None
            };
            if let Some(e) = error {
                read.phase = Phase::Failed(e);
                continue;
            }
            if matches!(read.phase,Phase::Barrier(index) if applied >= index) {
                read.phase = Phase::Pinned {
                    index: applied,
                    round: protection.map_or(0, Protection::next_round),
                    // The Raft owner is the sole projection writer. It cannot
                    // publish another index while copying this view.
                    value: read.request.query.sample(replica)?,
                };
            }
        }
        let finished = self.pending.extract_if(.., |read| {
            if read.request.reply.is_closed() || matches!(read.phase, Phase::Failed(_)) {
                return true;
            }
            if let Phase::Pinned { index, round, .. } = &read.phase {
                if protection.is_none_or(|p| p.covers(*round, *index)) {
                    return true;
                }
                needs_confirmation = true;
            }
            false
        });
        for read in finished {
            let result = match read.phase {
                Phase::Failed(error) => Err(error),
                Phase::Pinned { value, .. } => Ok(value),
                _ => continue, // caller already dropped its response channel
            };
            let _ = read.request.reply.send(result);
        }
        Ok(needs_confirmation)
    }
}
