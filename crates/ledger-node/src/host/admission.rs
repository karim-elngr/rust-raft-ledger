use ledger_core::{Account, AccountId, Command, EntryId, JournalEntry, LedgerId, RequestId};
use ledger_replica::{CheckpointRef, PreparedCommand, ReplicaError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::{Instant, timeout_at};

use super::Decision;
use crate::checkpoints;
use crate::reads::{Query, ReadRequest, Value};
use crate::{NodeError, NodeStatus, ServiceMode};
use std::sync::Arc;
const MAX_REQUESTS: usize = 512;
const MAX_REQUEST_BYTES: usize = 8 << 20;
const CONTROL_QUEUE: usize = 64;

pub(crate) struct Admission {
    _slot: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

pub(crate) struct Waiter {
    pub id: RequestId,
    pub command: Command,
    pub deadline: Instant,
    pub reply: oneshot::Sender<Result<Decision, NodeError>>,
    _admission: Admission,
}

impl Waiter {
    pub fn finish(self, outcome: Result<Decision, NodeError>) {
        let _ = self.reply.send(outcome);
    }
}

pub(crate) struct Submission {
    #[cfg(feature = "batch-diagnostics")]
    pub enqueued: Instant,
    pub prepared: PreparedCommand,
    pub waiter: Waiter,
}

pub(crate) enum Control {
    Checkpoint(checkpoints::Reply),
    Read(Box<ReadRequest>),
    ExportFrontier(oneshot::Sender<u64>),
    ExportPage {
        after: ledger_export::Position,
        through: u64,
        reply: oneshot::Sender<Result<ledger_export::EventPage, ReplicaError>>,
    },
    Status(oneshot::Sender<NodeStatus>),
    Lookup {
        id: RequestId,
        command: Command,
        reply: oneshot::Sender<Option<Decision>>,
        _admission: Admission,
    },
}

pub(crate) fn enqueue<T>(sender: &mpsc::Sender<T>, message: T) -> Result<(), NodeError> {
    sender.try_send(message).map_err(|err| match err {
        mpsc::error::TrySendError::Closed(_) => NodeError::Stopped,
        mpsc::error::TrySendError::Full(_) => NodeError::Busy,
    })
}

/// Access to a running voter. Protected nodes gate all monetary results on
/// the independently durable committed prefix.
#[derive(Clone)]
pub struct NodeHandle {
    ledger: LedgerId,
    mode: ServiceMode,
    slots: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    submissions: mpsc::Sender<Submission>,
    pub(crate) controls: mpsc::Sender<Control>,
}

impl NodeHandle {
    pub fn ledger_id(&self) -> LedgerId {
        self.ledger
    }
    pub fn service_mode(&self) -> ServiceMode {
        self.mode
    }
    pub fn is_protected(&self) -> bool {
        self.mode == ServiceMode::ProtectedVoter
    }

    pub(crate) fn channels(
        ledger: LedgerId,
        mode: ServiceMode,
    ) -> (Self, mpsc::Receiver<Submission>, mpsc::Receiver<Control>) {
        let (submissions, sub_rx) = mpsc::channel(MAX_REQUESTS);
        let (controls, control_rx) = mpsc::channel(CONTROL_QUEUE);
        (
            Self {
                ledger,
                mode,
                slots: Arc::new(Semaphore::new(MAX_REQUESTS)),
                bytes: Arc::new(Semaphore::new(MAX_REQUEST_BYTES)),
                submissions,
                controls,
            },
            sub_rx,
            control_rx,
        )
    }

    fn prepare(
        &self,
        id: RequestId,
        command: &Command,
    ) -> Result<(PreparedCommand, Admission), NodeError> {
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| NodeError::Busy)?;
        let prepared = PreparedCommand::new(self.ledger, id, command)?;
        // The versioned command codec has a hard 1 MiB bound per entry.
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(prepared.encoded_len() as u32)
            .map_err(|_| NodeError::Busy)?;
        Ok((
            prepared,
            Admission {
                _slot: slot,
                _bytes: bytes,
            },
        ))
    }

    /// Submit one command and wait for its Raft entry to apply on this leader.
    /// Losing the connection, leadership or a deadline can leave it committed.
    pub async fn submit_local(
        &self,
        id: RequestId,
        command: Command,
        deadline: Instant,
    ) -> Result<Decision, NodeError> {
        if deadline <= Instant::now() {
            return Err(NodeError::Deadline);
        }
        let (prepared, admission) = self.prepare(id, &command)?;
        let (reply, rx) = oneshot::channel();
        enqueue(
            &self.submissions,
            Submission {
                #[cfg(feature = "batch-diagnostics")]
                enqueued: Instant::now(),
                prepared,
                waiter: Waiter {
                    id,
                    command,
                    deadline,
                    reply,
                    _admission: admission,
                },
            },
        )?;
        timeout_at(deadline, rx)
            .await
            .map_err(|_| NodeError::Deadline)?
            .map_err(|_| NodeError::Stopped)?
    }

    /// Return a business decision only after local publication and a fresh
    /// confirmation from the active independent recovery journal.
    pub async fn submit_protected(
        &self,
        id: RequestId,
        command: Command,
        deadline: Instant,
    ) -> Result<Decision, NodeError> {
        if !self.is_protected() {
            return Err(NodeError::Unprotected);
        }
        self.submit_local(id, command, deadline).await
    }

    async fn read(
        &self,
        query: Query,
        deadline: Instant,
        admission: Admission,
    ) -> Result<Value, NodeError> {
        let (reply, rx) = oneshot::channel();
        enqueue(
            &self.controls,
            Control::Read(Box::new(ReadRequest {
                query,
                reply,
                deadline,
                _admission: admission,
            })),
        )?;
        timeout_at(deadline, rx)
            .await
            .map_err(|_| NodeError::Deadline)?
            .map_err(|_| NodeError::Stopped)?
    }

    pub async fn protected_outcome(
        &self,
        id: RequestId,
        command: Command,
        deadline: Instant,
    ) -> Result<Option<Decision>, NodeError> {
        if !self.is_protected() {
            return Err(NodeError::Unprotected);
        }
        if deadline <= Instant::now() {
            return Err(NodeError::Deadline);
        }
        let (_, admission) = self.prepare(id, &command)?;
        match self
            .read(Query::Outcome(id, command), deadline, admission)
            .await?
        {
            Value::Outcome(outcome) => Ok(outcome),
            _ => unreachable!("outcome query returns outcome"),
        }
    }

    async fn read_id(&self, query: Query, deadline: Instant) -> Result<Value, NodeError> {
        if !self.mode.serves_clients() {
            return Err(NodeError::Unprotected);
        }
        if deadline <= Instant::now() {
            return Err(NodeError::Deadline);
        }
        let bytes = match &query {
            Query::Accounts(ids) => {
                32 * u32::try_from(ids.len().max(1)).expect("bounded account list")
            }
            _ => 32,
        };
        let admission = Admission {
            _slot: self
                .slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| NodeError::Busy)?,
            _bytes: self
                .bytes
                .clone()
                .try_acquire_many_owned(bytes)
                .map_err(|_| NodeError::Busy)?,
        };
        self.read(query, deadline, admission).await
    }

    pub async fn protected_account(
        &self,
        id: AccountId,
        deadline: Instant,
    ) -> Result<Option<Account>, NodeError> {
        if !self.is_protected() {
            return Err(NodeError::Unprotected);
        }
        self.current_account(id, deadline).await
    }

    pub(crate) async fn current_account(
        &self,
        id: AccountId,
        deadline: Instant,
    ) -> Result<Option<Account>, NodeError> {
        match self.read_id(Query::Account(id), deadline).await? {
            Value::Account(account) => Ok(account),
            _ => unreachable!("account query returns an account"),
        }
    }

    pub(crate) async fn current_reservation(
        &self,
        id: ledger_core::ReservationId,
        deadline: Instant,
    ) -> Result<Option<ledger_core::Reservation>, NodeError> {
        match self.read_id(Query::Reservation(id), deadline).await? {
            Value::Reservation(reservation) => Ok(reservation),
            _ => unreachable!("reservation query returns a reservation"),
        }
    }

    pub async fn protected_entry(
        &self,
        id: EntryId,
        deadline: Instant,
    ) -> Result<Option<JournalEntry>, NodeError> {
        if !self.is_protected() {
            return Err(NodeError::Unprotected);
        }
        self.current_entry(id, deadline).await
    }

    pub(crate) async fn current_entry(
        &self,
        id: EntryId,
        deadline: Instant,
    ) -> Result<Option<JournalEntry>, NodeError> {
        match self.read_id(Query::Entry(id), deadline).await? {
            Value::Entry(entry) => Ok(entry),
            _ => unreachable!("entry query returns a journal entry"),
        }
    }

    /// Read balances and policies from one protected applied view. Results
    /// preserve caller order, duplicate IDs and missing accounts.
    pub async fn protected_accounts(
        &self,
        ids: Vec<AccountId>,
        deadline: Instant,
    ) -> Result<Vec<Option<Account>>, NodeError> {
        if !self.is_protected() {
            return Err(NodeError::Unprotected);
        }
        if ids.len() > 1024 {
            return Err(NodeError::Replica(ReplicaError::SizeLimit(
                "account list exceeds 1024 IDs",
            )));
        }
        match self.read_id(Query::Accounts(ids), deadline).await? {
            Value::Accounts(accounts) => Ok(accounts),
            _ => unreachable!("accounts query returns accounts"),
        }
    }

    /// Look up the original decision under a request ID, including a durable rejection.
    pub async fn protected_receipt(
        &self,
        id: RequestId,
        deadline: Instant,
    ) -> Result<Option<Decision>, NodeError> {
        if !self.is_protected() {
            return Err(NodeError::Unprotected);
        }
        self.current_receipt(id, deadline).await
    }

    pub(crate) async fn current_receipt(
        &self,
        id: RequestId,
        deadline: Instant,
    ) -> Result<Option<Decision>, NodeError> {
        match self.read_id(Query::Receipt(id), deadline).await? {
            Value::Outcome(outcome) => Ok(outcome),
            _ => unreachable!("receipt query returns a decision"),
        }
    }

    /// Diagnostic nodes expose local state. Protected nodes use a quorum read
    /// and require a fresh journal confirmation.
    pub async fn local_outcome(
        &self,
        id: RequestId,
        command: Command,
    ) -> Result<Option<Decision>, NodeError> {
        if self.is_protected() {
            return self
                .protected_outcome(
                    id,
                    command,
                    Instant::now() + std::time::Duration::from_secs(5),
                )
                .await;
        }
        let (_, admission) = self.prepare(id, &command)?;
        let (reply, rx) = oneshot::channel();
        enqueue(
            &self.controls,
            Control::Lookup {
                id,
                command,
                reply,
                _admission: admission,
            },
        )?;
        rx.await.map_err(|_| NodeError::Stopped)
    }

    pub async fn status(&self) -> Result<NodeStatus, NodeError> {
        let (reply, rx) = oneshot::channel();
        enqueue(&self.controls, Control::Status(reply))?;
        rx.await.map_err(|_| NodeError::Stopped)
    }

    /// Seal the current local applied view. A voter of either role can do this.
    pub async fn checkpoint(&self) -> Result<CheckpointRef, NodeError> {
        let (reply, rx) = oneshot::channel();
        enqueue(&self.controls, Control::Checkpoint(reply))?;
        rx.await.map_err(|_| NodeError::Stopped)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledger_core::{Balance, Bounds, Side};

    fn command() -> Command {
        Command::CreateAccount {
            account: ledger_core::AccountId::new(LedgerId(1), 8),
            code: 3,
            normal: Side::Credit,
            bounds: Bounds {
                min: Some(Balance::default()),
                max: None,
            },
        }
    }

    #[tokio::test]
    async fn expired_requests_never_enter_the_proposal_queue() {
        let (handle, mut submissions, _controls) =
            NodeHandle::channels(LedgerId(1), ServiceMode::DiagnosticVoter);
        let result = handle
            .submit_local(RequestId(1), command(), Instant::now())
            .await;
        assert!(matches!(result, Err(NodeError::Deadline)));
        assert!(matches!(
            submissions.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn large_commands_exhaust_bytes_before_request_slots() {
        use ledger_core::{AccountId, Funding, Line};
        let (handle, _, _) = NodeHandle::channels(LedgerId(1), ServiceMode::DiagnosticVoter);
        let account = AccountId::new(LedgerId(1), 7);
        let command = Command::Post {
            funding: Funding::Explicit(vec![Line::debit(account, 10); 1024]),
            conditions: vec![],
            corrects: None,
        };
        let mut held = Vec::new();
        let first = handle.prepare(RequestId(1), &command).unwrap();
        let capacity = MAX_REQUEST_BYTES / first.0.encoded_len();
        assert!(capacity < MAX_REQUESTS);
        held.push(first);
        for _ in 1..capacity {
            held.push(handle.prepare(RequestId(1), &command).unwrap());
        }
        assert!(matches!(
            handle.prepare(RequestId(1), &command),
            Err(NodeError::Busy)
        ));
        drop(held.pop());
        assert!(handle.prepare(RequestId(1), &command).is_ok());
    }

    #[test]
    fn in_flight_commands_keep_their_admission_budget() {
        let (handle, _, _) = NodeHandle::channels(LedgerId(1), ServiceMode::DiagnosticVoter);
        let mut held = Vec::new();
        for _ in 0..MAX_REQUESTS {
            held.push(handle.prepare(RequestId(1), &command()).unwrap());
        }
        assert!(matches!(
            handle.prepare(RequestId(2), &command()),
            Err(NodeError::Busy)
        ));
        drop(held.pop());
        assert!(handle.prepare(RequestId(2), &command()).is_ok());
    }
}
