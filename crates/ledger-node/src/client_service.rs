//! The backend service uses a separate TLS listener and the acknowledgement
//! contract selected at startup. A successful RPC may carry a business rejection.
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ledger_api::MAX_RPC_BYTES;
use ledger_api::adapter::{self, Mutation};
use ledger_api::v1::{
    self,
    ledger_server::{Ledger, LedgerServer},
};
use ledger_core::{AccountId, EntryId, RequestId, ReservationId};
use ledger_export::ExportWorkers;
use ledger_replica::ReplicaError;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};
use tower::ServiceBuilder;
use tower::limit::GlobalConcurrencyLimitLayer;

use crate::{NodeError, NodeHandle};

const MAX_CALL_TIME: Duration = Duration::from_secs(10);

pub struct ClientTls {
    /// Trust root for backend client certificates authorized to use this ledger.
    pub ca_pem: Vec<u8>,
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

fn budget<T>(request: &Request<T>) -> Result<Duration, Status> {
    let Some(raw) = request.metadata().get("grpc-timeout") else {
        return Ok(MAX_CALL_TIME);
    };
    let invalid = || Status::invalid_argument("invalid grpc-timeout");
    let bytes = raw.as_bytes();
    let (unit, num) = bytes.split_last().ok_or_else(invalid)?;
    if num.is_empty() || num.len() > 8 || !num.iter().all(u8::is_ascii_digit) {
        return Err(invalid());
    }
    let n = num.iter().fold(0u64, |v, b| v * 10 + u64::from(*b - b'0'));
    let value = match unit {
        b'H' => Duration::from_secs(n * 3600),
        b'M' => Duration::from_secs(n * 60),
        b'S' => Duration::from_secs(n),
        b'm' => Duration::from_millis(n),
        b'u' => Duration::from_micros(n),
        b'n' => Duration::from_nanos(n),
        _ => return Err(invalid()),
    };
    Ok(value.min(MAX_CALL_TIME))
}

pub(crate) fn rpc_status(error: NodeError) -> Status {
    match error {
        NodeError::NotLeader(leader) => {
            let mut status = Status::unavailable("leader required");
            if let Some(id) = leader {
                status.metadata_mut().insert(
                    "ledger-leader-id",
                    id.to_string().parse().expect("decimal integer metadata"),
                );
            }
            status
        }
        NodeError::Replica(ReplicaError::SizeLimit(reason)) => Status::out_of_range(reason),
        NodeError::Busy => Status::resource_exhausted("ledger admission limit reached"),
        NodeError::Deadline => Status::deadline_exceeded("outcome unknown after deadline"),
        NodeError::Fenced => Status::unavailable("voter no longer has serving authority"),
        NodeError::Unprotected => {
            Status::failed_precondition("independent commit protection is required")
        }
        NodeError::LeadershipChanged
        | NodeError::Stopped
        | NodeError::ThreadFailed
        | NodeError::Transport(_)
        | NodeError::Journal(_)
        | NodeError::Io(_) => {
            Status::unavailable("ledger unavailable; retry with the original request ID")
        }
        NodeError::Invalid(_) | NodeError::Replica(_) => {
            Status::internal("ledger could not serve this request")
        }
    }
}

struct Service {
    node: NodeHandle,
    // Omission preserves the single trusted backend role of existing deployments.
    admin_pins: Option<Vec<Vec<u8>>>,
}

pub(crate) fn deadline<T>(request: &Request<T>) -> Result<Instant, Status> {
    let remaining = budget(request)?;
    if remaining.is_zero() {
        return Err(Status::deadline_exceeded("deadline elapsed"));
    }
    Ok(Instant::now() + remaining)
}

impl Service {
    fn require_admin<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let Some(pins) = &self.admin_pins else {
            return Ok(());
        };
        let certs = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("client certificate required"))?;
        if certs
            .first()
            .is_some_and(|leaf| pins.iter().any(|pin| pin.as_slice() == leaf.as_ref()))
        {
            Ok(())
        } else {
            Err(Status::permission_denied(
                "account administration permission required",
            ))
        }
    }

    async fn mutate<M: Mutation>(
        &self,
        request: Request<M>,
    ) -> Result<Response<v1::Decision>, Status> {
        let deadline = deadline(&request)?;
        let (id, command) = request.into_inner().into_command(self.node.ledger_id())?;
        let outcome = self
            .node
            .submit_local(id, command, deadline)
            .await
            .map_err(rpc_status)?;
        Ok(Response::new(adapter::decision(outcome)))
    }
}

#[tonic::async_trait]
impl Ledger for Service {
    async fn create_account(
        &self,
        r: Request<v1::CreateAccountRequest>,
    ) -> Result<Response<v1::Decision>, Status> {
        self.require_admin(&r)?;
        self.mutate(r).await
    }
    async fn set_account_bounds(
        &self,
        r: Request<v1::SetAccountBoundsRequest>,
    ) -> Result<Response<v1::Decision>, Status> {
        self.require_admin(&r)?;
        self.mutate(r).await
    }
    async fn post_entry(
        &self,
        r: Request<v1::PostEntryRequest>,
    ) -> Result<Response<v1::Decision>, Status> {
        if r.get_ref().corrects.is_some() {
            self.require_admin(&r)?;
        }
        self.mutate(r).await
    }
    async fn reserve_entry(
        &self,
        r: Request<v1::ReserveEntryRequest>,
    ) -> Result<Response<v1::Decision>, Status> {
        self.mutate(r).await
    }
    async fn capture_reservation(
        &self,
        r: Request<v1::CaptureReservationRequest>,
    ) -> Result<Response<v1::Decision>, Status> {
        self.mutate(r).await
    }
    async fn void_reservation(
        &self,
        r: Request<v1::VoidReservationRequest>,
    ) -> Result<Response<v1::Decision>, Status> {
        self.mutate(r).await
    }

    async fn get_account(
        &self,
        r: Request<v1::GetAccountRequest>,
    ) -> Result<Response<v1::GetAccountResponse>, Status> {
        let deadline = deadline(&r)?;
        let input = r.into_inner();
        adapter::check_ledger(&input.ledger_id, self.node.ledger_id())?;
        let id = AccountId::new(self.node.ledger_id(), adapter::parse_id(&input.account_id)?);
        let account = self
            .node
            .current_account(id, deadline)
            .await
            .map_err(rpc_status)?;
        Ok(Response::new(v1::GetAccountResponse {
            account: account.as_ref().map(adapter::account),
        }))
    }

    async fn get_reservation(
        &self,
        r: Request<v1::GetReservationRequest>,
    ) -> Result<Response<v1::GetReservationResponse>, Status> {
        let deadline = deadline(&r)?;
        let input = r.into_inner();
        adapter::check_ledger(&input.ledger_id, self.node.ledger_id())?;
        let id = ReservationId(adapter::parse_id(&input.reservation_id)?);
        let value = self
            .node
            .current_reservation(id, deadline)
            .await
            .map_err(rpc_status)?;
        Ok(Response::new(v1::GetReservationResponse {
            reservation: value.as_ref().map(adapter::reservation),
        }))
    }

    async fn get_entry(
        &self,
        r: Request<v1::GetEntryRequest>,
    ) -> Result<Response<v1::GetEntryResponse>, Status> {
        let deadline = deadline(&r)?;
        let input = r.into_inner();
        adapter::check_ledger(&input.ledger_id, self.node.ledger_id())?;
        if input.entry_id == 0 {
            return Err(Status::invalid_argument("journal entry IDs start at one"));
        }
        let entry = self
            .node
            .current_entry(EntryId(input.entry_id), deadline)
            .await
            .map_err(rpc_status)?;
        Ok(Response::new(v1::GetEntryResponse {
            entry: entry.as_ref().map(adapter::entry),
        }))
    }

    async fn get_request_outcome(
        &self,
        r: Request<v1::GetRequestOutcomeRequest>,
    ) -> Result<Response<v1::GetRequestOutcomeResponse>, Status> {
        let deadline = deadline(&r)?;
        let input = r.into_inner();
        adapter::check_ledger(&input.ledger_id, self.node.ledger_id())?;
        let id = RequestId(adapter::parse_id(&input.request_id)?);
        let outcome = self
            .node
            .current_receipt(id, deadline)
            .await
            .map_err(rpc_status)?;
        Ok(Response::new(v1::GetRequestOutcomeResponse {
            decision: outcome.map(adapter::decision),
        }))
    }
}

pub struct ClientServer {
    stop: watch::Sender<bool>,
    done: watch::Receiver<bool>,
    failure: Arc<OnceLock<String>>,
    task: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl ClientServer {
    pub async fn start(
        node: NodeHandle,
        listener: TcpListener,
        tls: ClientTls,
    ) -> Result<Self, NodeError> {
        if !node.is_protected() {
            return Err(NodeError::Unprotected);
        }
        Self::start_authorized(node, listener, tls, None).await
    }

    /// Serve a voting member of a static learner topology. Success means
    /// committed and applied. Independent archive progress is asynchronous.
    pub async fn start_quorum(
        node: NodeHandle,
        listener: TcpListener,
        tls: ClientTls,
    ) -> Result<Self, NodeError> {
        if node.service_mode() != crate::ServiceMode::QuorumVoter {
            return Err(NodeError::Invalid(
                "quorum API requires a voter in a learner topology".into(),
            ));
        }
        Self::start_authorized(node, listener, tls, None).await
    }

    /// Restrict account creation, bounds changes and correction postings to these
    /// exact DER leaf certificates. All client identities must also pass mutual TLS.
    pub async fn start_authorized(
        node: NodeHandle,
        listener: TcpListener,
        tls: ClientTls,
        admin_pins: Option<Vec<Vec<u8>>>,
    ) -> Result<Self, NodeError> {
        if !node.service_mode().serves_clients() {
            return Err(NodeError::Unprotected);
        }
        let ledger = Service {
            node: node.clone(),
            admin_pins,
        };
        Self::listen(node, listener, tls, Some(ledger), None).await
    }

    /// A voter can expose status to a separate operator CA without registering
    /// any ledger methods on that listener.
    pub async fn start_voter_monitor(
        node: NodeHandle,
        listener: TcpListener,
        tls: ClientTls,
    ) -> Result<Self, NodeError> {
        if node.service_mode() == crate::ServiceMode::Learner {
            return Err(NodeError::Invalid("voter monitor requires a voter".into()));
        }
        Self::listen(node, listener, tls, None, None).await
    }

    /// Serve operational reads on a learner using its export workers' local observations.
    pub async fn start_monitor(
        node: NodeHandle,
        listener: TcpListener,
        tls: ClientTls,
        exports: Arc<ExportWorkers>,
    ) -> Result<Self, NodeError> {
        if node.service_mode() != crate::ServiceMode::Learner {
            return Err(NodeError::Invalid(
                "the export monitor requires a learner".into(),
            ));
        }
        Self::listen(node, listener, tls, None, Some(exports)).await
    }

    async fn listen(
        node: NodeHandle,
        listener: TcpListener,
        tls: ClientTls,
        ledger: Option<Service>,
        exports: Option<Arc<ExportWorkers>>,
    ) -> Result<Self, NodeError> {
        let tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(tls.cert_pem, tls.key_pem))
            .client_ca_root(Certificate::from_pem(tls.ca_pem))
            .client_auth_optional(false)
            .timeout(MAX_CALL_TIME);
        let admission = ServiceBuilder::new()
            .load_shed()
            .layer(GlobalConcurrencyLimitLayer::new(128))
            .into_inner();
        let mut server = Server::builder()
            .tls_config(tls)?
            .layer(admission)
            .timeout(MAX_CALL_TIME)
            .concurrency_limit_per_connection(32)
            .max_concurrent_streams(32);
        let service = ledger.map(|ledger| {
            LedgerServer::new(ledger)
                .max_decoding_message_size(MAX_RPC_BYTES)
                .max_encoding_message_size(MAX_RPC_BYTES)
        });
        let monitor =
            v1::monitor_server::MonitorServer::new(crate::monitor::Monitor { node, exports })
                .max_decoding_message_size(MAX_RPC_BYTES)
                .max_encoding_message_size(MAX_RPC_BYTES);
        let (stop, mut stop_rx) = watch::channel(false);
        let (done_tx, done) = watch::channel(false);
        let failure = Arc::new(OnceLock::new());
        let accept_failure = failure.clone();
        let accept_done = done_tx.clone();
        let incoming = TcpListenerStream::new(listener).take_while(move |item| {
            if let Err(err) = item {
                use std::io::ErrorKind::*;
                if !matches!(
                    err.kind(),
                    Interrupted | ConnectionAborted | ConnectionReset | WouldBlock
                ) {
                    let _ = accept_failure.set(err.to_string());
                    accept_done.send_replace(true);
                    return false;
                }
            }
            true
        });
        let task = tokio::spawn(async move {
            let result = server
                .add_service(monitor)
                .add_optional_service(service)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = stop_rx.wait_for(|stop| *stop).await;
                })
                .await;
            done_tx.send_replace(true);
            result
        });
        Ok(Self {
            stop,
            done,
            failure,
            task: Some(task),
        })
    }

    pub async fn wait_stopped(&self) {
        let mut done = self.done.clone();
        let _ = done.wait_for(|v| *v).await;
    }

    pub async fn shutdown(mut self) -> Result<(), NodeError> {
        self.stop.send_replace(true);
        let mut task = self.task.take().expect("server task");
        match timeout(Duration::from_secs(2), &mut task).await {
            Ok(joined) => joined.map_err(|_| NodeError::ThreadFailed)??,
            Err(_) => {
                task.abort();
                let _ = task.await;
            }
        }
        if let Some(reason) = self.failure.get() {
            return Err(NodeError::Invalid(format!(
                "client listener failed: {reason}"
            )));
        }
        Ok(())
    }
}

impl Drop for ClientServer {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[test]
    fn request_deadline_understands_grpc_units_and_caps_long_calls() {
        let mut req = Request::new(());
        req.metadata_mut()
            .insert("grpc-timeout", "100m".parse().unwrap());
        assert_eq!(budget(&req).unwrap(), Duration::from_millis(100));
        req.metadata_mut()
            .insert("grpc-timeout", "99999999H".parse().unwrap());
        assert_eq!(budget(&req).unwrap(), MAX_CALL_TIME);
        req.metadata_mut()
            .insert("grpc-timeout", "123456789S".parse().unwrap());
        assert_eq!(budget(&req).unwrap_err().code(), Code::InvalidArgument);
    }

    #[test]
    fn follower_hint_is_a_node_id_and_business_errors_stay_out_of_transport_status() {
        let status = rpc_status(NodeError::NotLeader(Some(41)));
        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(status.metadata().get("ledger-leader-id").unwrap(), "41");
        assert_eq!(rpc_status(NodeError::Busy).code(), Code::ResourceExhausted);
        assert_eq!(rpc_status(NodeError::Fenced).code(), Code::Unavailable);
    }
}
