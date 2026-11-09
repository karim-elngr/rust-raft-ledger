//! A generated gRPC client with deadline-bounded routing across configured seeds.
use std::{
    collections::BTreeSet,
    error::Error,
    fmt,
    future::Future,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::time::{self, Instant};
use tonic::{
    Code, Request, Response, Status,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity},
};

use crate::v1;

const DISCOVERY_ATTEMPT: Duration = Duration::from_secs(2);
// Match the maximum unary RPC budget accepted by the public ledger service.
const MAX_ATTEMPT: Duration = Duration::from_secs(10);
const FIRST_BACKOFF: Duration = Duration::from_millis(20);
const MAX_BACKOFF: Duration = Duration::from_millis(250);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientSeed {
    pub node: u64,
    /// HTTPS origin for this public service.
    pub endpoint: String,
    /// The server name to verify in the seed's certificate.
    pub domain: String,
}

struct Target {
    node: u64,
    endpoint: Endpoint,
    channel: OnceLock<Channel>,
}

impl Target {
    fn client(&self) -> v1::ledger_client::LedgerClient<Channel> {
        let channel = self.channel.get_or_init(|| self.endpoint.connect_lazy());
        v1::ledger_client::LedgerClient::new(channel.clone())
            .max_encoding_message_size(crate::MAX_RPC_BYTES)
            .max_decoding_message_size(crate::MAX_RPC_BYTES)
    }
}

struct Routes {
    targets: Vec<Target>,
    preferred: AtomicUsize,
}

/// Clones share reusable channels and the last successful seed. Construct the
/// client offline; run calls on a Tokio runtime with I/O and time enabled.
/// Each retry carries the exact request supplied by the caller. A deadline can
/// leave a write's outcome unknown. Retry that write using its original ID.
/// An unavailable seed or expired attempt can route the same request to another
/// seed. The first pass shares time across seeds; later attempts can wait up
/// to ten seconds for independent commit protection, within the caller's deadline.
/// Reads and first-attempt admission overload return `RESOURCE_EXHAUSTED`.
/// If an earlier write attempt may have committed, a terminal RPC error at the
/// next seed leaves the outcome unknown. The method returns `UNAVAILABLE`;
/// retry the original request ID and payload or look up its receipt.
#[derive(Clone)]
pub struct LedgerClient {
    routes: Arc<Routes>,
}

impl fmt::Debug for LedgerClient {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("LedgerClient")
            .field("seed_count", &self.routes.targets.len())
            .finish_non_exhaustive()
    }
}

fn retryable(status: &Status) -> bool {
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded => true,
        Code::Cancelled => {
            // Tonic represents its own transport timer as CANCELLED. A remote
            // cancellation with identical text carries no such local error.
            let mut error = status.source();
            while let Some(cause) = error {
                if cause.is::<tonic::TimeoutExpired>() {
                    return true;
                }
                error = cause.source();
            }
            false
        }
        _ => false,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CallKind {
    Mutation,
    Read,
}

macro_rules! unary_methods {
    ($kind:ident; $(($method:ident, $request:ident, $response:ident)),+ $(,)?) => {
        $(
            pub async fn $method(&self, request: v1::$request, deadline: Instant)
                -> Result<v1::$response, Status>
            {
                if prost::Message::encoded_len(&request) > crate::MAX_RPC_BYTES {
                    return Err(Status::out_of_range("request exceeds the v1 message limit"));
                }
                self.call(CallKind::$kind, request, deadline, |target, request| {
                    let mut client = target.client();
                    async move { client.$method(request).await }
                }).await
            }
        )+
    };
}

impl LedgerClient {
    /// Validate the seed origins and mTLS material without dialing the servers.
    pub fn connect(
        seeds: Vec<ClientSeed>,
        ca_pem: Vec<u8>,
        cert_pem: Vec<u8>,
        key_pem: Vec<u8>,
    ) -> Result<Self, Status> {
        if seeds.is_empty() || ca_pem.is_empty() || cert_pem.is_empty() || key_pem.is_empty() {
            return Err(Status::invalid_argument(
                "supply seeds, server CA and client identity",
            ));
        }
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(ca_pem))
            .identity(Identity::from_pem(cert_pem, key_pem));
        let mut targets = Vec::with_capacity(seeds.len());
        let mut ids = BTreeSet::new();
        for seed in seeds {
            if seed.node == 0 || !ids.insert(seed.node) {
                return Err(Status::invalid_argument(
                    "seed node IDs must be nonzero and unique",
                ));
            }
            let endpoint = Endpoint::from_shared(seed.endpoint)
                .map_err(|_| Status::invalid_argument("invalid seed endpoint"))?;
            let uri = endpoint.uri();
            if uri.scheme_str() != Some("https")
                || uri.authority().is_none_or(|a| a.as_str().contains('@'))
                || !matches!(uri.path(), "" | "/")
                || uri.query().is_some()
                || seed.domain.trim().is_empty()
            {
                return Err(Status::invalid_argument(
                    "supply an HTTPS origin and TLS server name for each seed",
                ));
            }
            let endpoint = endpoint
                .connect_timeout(DISCOVERY_ATTEMPT)
                .tls_config(tls.clone().domain_name(seed.domain))
                .map_err(|_| Status::invalid_argument("invalid client TLS configuration"))?;
            targets.push(Target {
                node: seed.node,
                endpoint,
                channel: OnceLock::new(),
            });
        }
        Ok(Self::from_targets(targets))
    }

    fn from_targets(targets: Vec<Target>) -> Self {
        Self {
            routes: Arc::new(Routes {
                targets,
                preferred: AtomicUsize::new(0),
            }),
        }
    }

    async fn call<Q: Clone, R, F, Fut>(
        &self,
        kind: CallKind,
        payload: Q,
        deadline: Instant,
        mut invoke: F,
    ) -> Result<R, Status>
    where
        F: FnMut(&Target, Request<Q>) -> Fut,
        Fut: Future<Output = Result<Response<R>, Status>>,
    {
        let targets = &self.routes.targets;
        let mut current = self.routes.preferred.load(Ordering::Relaxed);
        let mut tried = vec![false; targets.len()];
        let mut remaining = targets.len();
        let mut backoff = FIRST_BACKOFF;
        let mut attempt_cap = DISCOVERY_ATTEMPT;
        let mut earlier_attempt_uncertain = false;
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(Status::deadline_exceeded("ledger call deadline elapsed"));
            }
            let budget = if attempt_cap == DISCOVERY_ATTEMPT {
                // During discovery reserve some time for every untried seed.
                let share = (deadline - now) / u32::try_from(remaining).unwrap_or(u32::MAX);
                share.clamp(Duration::from_millis(1), attempt_cap)
            } else {
                // Every seed has been tried. Give a slow leader time to finish
                // one protected call within the original operation deadline.
                attempt_cap
            };
            let attempt_deadline = deadline.min(now + budget);
            let mut request = Request::new(payload.clone());
            request.set_timeout(attempt_deadline.saturating_duration_since(Instant::now()));
            let result =
                time::timeout_at(attempt_deadline, invoke(&targets[current], request)).await;
            if Instant::now() >= deadline {
                return Err(Status::deadline_exceeded("ledger call deadline elapsed"));
            }
            let hint = match result {
                Ok(Ok(response)) => {
                    self.routes.preferred.store(current, Ordering::Relaxed);
                    return Ok(response.into_inner());
                }
                Ok(Err(status)) if !retryable(&status) => {
                    if kind == CallKind::Mutation && earlier_attempt_uncertain {
                        return Err(Status::unavailable(format!(
                            "write outcome unknown after an earlier attempt (last status {:?}); retry with the original request ID and payload",
                            status.code()
                        )));
                    }
                    return Err(status);
                }
                Ok(Err(status)) if status.code() == Code::Unavailable => {
                    // The public service attaches a leader ID only to its
                    // pre-admission follower redirect. Other transport failures
                    // may have happened after the server committed the write.
                    let hint = status
                        .metadata()
                        .get("ledger-leader-id")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok());
                    earlier_attempt_uncertain |= hint.is_none();
                    hint
                }
                _ => {
                    earlier_attempt_uncertain = true;
                    None
                }
            };
            tried[current] = true;
            remaining -= 1;
            let hinted = hint.and_then(|node| targets.iter().position(|t| t.node == node));
            if remaining == 0 {
                // Complete a round before retrying any endpoint; back off once
                // per exhausted round, including a single-seed configuration.
                time::sleep_until(deadline.min(Instant::now() + backoff)).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                attempt_cap = (attempt_cap * 2).min(MAX_ATTEMPT);
                remaining = targets.len();
                tried.fill(false);
                current = hinted.unwrap_or((current + 1) % targets.len());
            } else {
                current = hinted.filter(|&index| !tried[index]).unwrap_or_else(|| {
                    let mut next = (current + 1) % targets.len();
                    while tried[next] {
                        next = (next + 1) % targets.len();
                    }
                    next
                });
            }
        }
    }

    unary_methods!(Mutation;
        (create_account, CreateAccountRequest, Decision),
        (set_account_bounds, SetAccountBoundsRequest, Decision),
        (post_entry, PostEntryRequest, Decision),
        (reserve_entry, ReserveEntryRequest, Decision),
        (capture_reservation, CaptureReservationRequest, Decision),
        (void_reservation, VoidReservationRequest, Decision),
    );

    unary_methods!(Read;
        (get_account, GetAccountRequest, GetAccountResponse),
        (get_entry, GetEntryRequest, GetEntryResponse),
        (get_reservation, GetReservationRequest, GetReservationResponse),
        (
            get_request_outcome,
            GetRequestOutcomeRequest,
            GetRequestOutcomeResponse
        ),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::{pending, ready};

    fn client(ids: &[u64]) -> LedgerClient {
        LedgerClient::from_targets(
            ids.iter()
                .map(|&node| Target {
                    node,
                    endpoint: Endpoint::from_static("https://seed.invalid"),
                    channel: OnceLock::new(),
                })
                .collect(),
        )
    }

    fn follower(leader: u64) -> Status {
        let mut err = Status::unavailable("follower");
        err.metadata_mut()
            .insert("ledger-leader-id", leader.to_string().parse().unwrap());
        err
    }

    #[test]
    fn configuration_requires_numbered_https_seeds_and_a_client_identity() {
        let seed = ClientSeed {
            node: 1,
            endpoint: "https://127.0.0.1:7001".into(),
            domain: "pay.test".into(),
        };
        for seeds in [
            vec![],
            vec![ClientSeed {
                node: 0,
                ..seed.clone()
            }],
            vec![ClientSeed {
                endpoint: "http://pay.test".into(),
                ..seed.clone()
            }],
            vec![ClientSeed {
                endpoint: "https://pay.test/other".into(),
                ..seed.clone()
            }],
        ] {
            assert_eq!(
                LedgerClient::connect(seeds, vec![1], vec![2], vec![3])
                    .unwrap_err()
                    .code(),
                Code::InvalidArgument
            );
        }
        assert_eq!(
            LedgerClient::connect(vec![seed], vec![1], vec![2], vec![])
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stale_leader_hint_rotates_to_an_untried_seed_with_the_original_body() {
        let client = client(&[10, 20, 30]);
        let command = v1::PostEntryRequest {
            ledger_id: "11".into(),
            request_id: "24".into(),
            corrects: Some(87),
            ..Default::default()
        };
        let mut tried = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(8);
        let decision = client
            .call(
                CallKind::Mutation,
                command.clone(),
                deadline,
                |seed, request| {
                    assert_eq!(request.get_ref(), &command);
                    assert!(request.metadata().contains_key("grpc-timeout"));
                    tried.push(seed.node);
                    ready(match seed.node {
                        10 => Err(follower(30)),
                        30 => Err(follower(10)),
                        _ => Ok(Response::new(7)),
                    })
                },
            )
            .await
            .unwrap();
        assert_eq!(decision, 7);
        assert_eq!(tried, [10, 30, 20]);

        let next = client
            .call(CallKind::Read, (), deadline, |seed, _| {
                ready(Ok(Response::new(seed.node)))
            })
            .await
            .unwrap();
        assert_eq!(next, 20);
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_hint_never_selects_an_unconfigured_target() {
        let client = client(&[10, 20]);
        let mut tried = vec![];
        client
            .call(
                CallKind::Read,
                (),
                Instant::now() + Duration::from_secs(1),
                |seed, _| {
                    tried.push(seed.node);
                    ready(if seed.node == 10 {
                        Err(follower(999))
                    } else {
                        Ok(Response::new(()))
                    })
                },
            )
            .await
            .unwrap();
        assert_eq!(tried, [10, 20]);
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_attempt_yields_to_another_seed_and_overall_deadline_is_final() {
        let client = client(&[1, 2]);
        let mut count = 0;
        let start = Instant::now();
        let outcome = client
            .call(
                CallKind::Read,
                (),
                start + DISCOVERY_ATTEMPT * 3,
                |seed, _| {
                    count += 1;
                    let node = seed.node;
                    async move {
                        if node == 1 {
                            pending::<()>().await;
                        }
                        Ok(Response::new(node))
                    }
                },
            )
            .await
            .unwrap();
        assert_eq!((count, outcome), (2, 2));
        assert!(Instant::now() - start <= DISCOVERY_ATTEMPT + Duration::from_millis(1));

        let deadline = Instant::now() + Duration::from_millis(50);
        let mut attempts = 0;
        let err = client
            .call(CallKind::Read, (), deadline, |_, _| {
                attempts += 1;
                ready(Err::<Response<()>, _>(Status::unavailable("disconnected")))
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::DeadlineExceeded);
        assert!(attempts < 20);
        assert_eq!(Instant::now(), deadline);
    }

    #[tokio::test(start_paused = true)]
    async fn discovery_is_bounded_and_a_slow_protected_leader_can_finish() {
        let client = client(&[1, 2, 3]);
        let start = Instant::now();
        let deadline = start + Duration::from_secs(9);
        let mut attempts = Vec::new();
        let returned = client
            .call(CallKind::Mutation, (), deadline, |target, _| {
                let node = target.node;
                attempts.push((node, Instant::now() - start));
                async move {
                    match node {
                        1 => pending().await, // dead seed
                        2 => {
                            // Fresh offsite durability confirmation for every attempt.
                            time::sleep(Duration::from_secs(3)).await;
                            Ok(Response::new(42))
                        }
                        _ => Err(follower(2)),
                    }
                }
            })
            .await
            .unwrap();
        assert_eq!(returned, 42);
        assert_eq!(
            attempts.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            [1, 2, 3, 2]
        );
        assert_eq!(attempts[1].1, Duration::from_secs(2));
        assert!(Instant::now() < deadline);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_first_seed_cannot_consume_a_short_deadline() {
        let client = client(&[1, 2]);
        let start = Instant::now();
        let budget = Duration::from_millis(100);
        let answer = client
            .call(CallKind::Read, (), start + budget, |target, _| {
                let node = target.node;
                async move {
                    if node == 1 {
                        pending::<()>().await;
                    }
                    Ok(Response::new(node))
                }
            })
            .await
            .unwrap();
        assert_eq!(answer, 2);
        assert!(Instant::now() < start + budget);
    }

    #[tokio::test(start_paused = true)]
    async fn lost_write_reply_followed_by_overload_preserves_unknown_outcome() {
        let client = client(&[1, 2]);
        let payment = v1::PostEntryRequest {
            ledger_id: "1".into(),
            request_id: "17".into(),
            ..Default::default()
        };
        let err = client
            .call(
                CallKind::Mutation,
                payment.clone(),
                Instant::now() + Duration::from_secs(5),
                |seed, request| {
                    assert_eq!(request.get_ref(), &payment);
                    let node = seed.node;
                    async move {
                        if node == 1 {
                            // The payment could have committed before the reply went missing.
                            return pending().await;
                        }
                        Err::<Response<()>, _>(Status::resource_exhausted("full"))
                    }
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unavailable);
        assert!(err.message().contains("outcome unknown"));
        assert!(err.message().contains("original request ID"));
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_error_at_a_later_seed_preserves_an_unknown_write() {
        for refused in [
            Status::permission_denied("certificate rotated"),
            Status::out_of_range("size limit"),
            Status::invalid_argument("seed serves a different ledger"),
            Status::unauthenticated("new seed rejected the certificate"),
        ] {
            let client = client(&[1, 2]);
            let result = client
                .call(
                    CallKind::Mutation,
                    (),
                    Instant::now() + Duration::from_secs(3),
                    |seed, _| {
                        let error = if seed.node == 1 {
                            Status::unavailable("reply lost")
                        } else {
                            refused.clone()
                        };
                        ready(Err::<Response<()>, _>(error))
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(result.code(), Code::Unavailable);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn report_admission_overload_for_reads_and_writes_with_only_known_redirects() {
        let client = client(&[1, 2]);
        for (kind, first_error, expected) in [
            (
                CallKind::Read,
                Status::deadline_exceeded("timed out"),
                Code::ResourceExhausted,
            ),
            (CallKind::Mutation, follower(2), Code::ResourceExhausted),
            (
                CallKind::Mutation,
                Status::unavailable("connection lost"),
                Code::Unavailable,
            ),
        ] {
            let result = client
                .call(
                    kind,
                    (),
                    Instant::now() + Duration::from_secs(3),
                    |seed, _| {
                        let err = if seed.node == 1 {
                            first_error.clone()
                        } else {
                            Status::resource_exhausted("full")
                        };
                        ready(Err::<Response<()>, _>(err))
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(result.code(), expected);
        }
        let first_overload = client
            .call(
                CallKind::Mutation,
                (),
                Instant::now() + Duration::from_secs(3),
                |_, _| ready(Err::<Response<()>, _>(Status::resource_exhausted("full"))),
            )
            .await
            .unwrap_err();
        assert_eq!(first_overload.code(), Code::ResourceExhausted);
    }

    #[tokio::test(start_paused = true)]
    async fn overload_and_application_cancellation_are_terminal() {
        let client = client(&[1, 2]);
        for err in [
            Status::resource_exhausted("full"),
            Status::cancelled("Timeout expired"),
        ] {
            let mut count = 0;
            let result = client
                .call(
                    CallKind::Read,
                    (),
                    Instant::now() + Duration::from_secs(1),
                    |_, _| {
                        count += 1;
                        ready(Err::<Response<()>, _>(err.clone()))
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(result.code(), err.code());
            assert_eq!(count, 1);
        }
        let mut count = 0;
        let answer = client
            .call(
                CallKind::Read,
                (),
                Instant::now() + Duration::from_secs(1),
                |_, _| {
                    count += 1;
                    ready(Ok(Response::new(v1::Decision {
                        result: Some(v1::decision::Result::Rejected(v1::Rejection::default())),
                    })))
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            answer.result,
            Some(v1::decision::Result::Rejected(_))
        ));
        assert_eq!(count, 1);
        assert!(retryable(&Status::from_error(Box::new(
            tonic::TimeoutExpired(())
        ))));
    }
    #[tokio::test]
    async fn oversized_requests_are_rejected_before_cloning_or_dialing_a_seed() {
        let sdk = LedgerClient::from_targets(vec![Target {
            node: 1,
            endpoint: Endpoint::from_static("http://127.0.0.1:1"),
            channel: OnceLock::new(),
        }]);
        let request = v1::GetAccountRequest {
            ledger_id: "1".into(),
            account_id: "1".repeat(1 << 20),
        };
        let result = sdk
            .get_account(request, Instant::now() + Duration::from_millis(200))
            .await;
        assert_eq!(result.unwrap_err().code(), Code::OutOfRange);
        assert!(sdk.routes.targets[0].channel.get().is_none());
    }
}
