//! Check a recorded real-time history against an independent two-wallet model.
//! Two competing payments cannot both fit. Either can win the concurrent race.
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

const IDS: [u64; 2] = [40, 41];
const AMOUNTS: [u64; 2] = [7, 5];

#[derive(Clone, Copy, Debug)]
enum Operation {
    Pay(usize),
    Receipt(usize),
    Balance(u64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Answer {
    Posted(u64),
    BelowMinimum,
    Missing,
    Balance(u64),
    Unknown,
}

#[derive(Clone, Copy, Debug)]
struct Call {
    op: Operation,
    start: u64,
    end: u64,
    answer: Answer,
}

struct Trace {
    clock: Instant,
    next: AtomicU64,
}
impl Trace {
    fn event(&self, message: impl std::fmt::Display) -> u64 {
        let order = self.next.fetch_add(1, Ordering::SeqCst);
        println!(
            "history_event order={order} us={} {message}",
            self.clock.elapsed().as_micros()
        );
        order
    }
}

fn decision(wire: v1::Decision, i: usize) -> Answer {
    match wire.result {
        Some(v1::decision::Result::Accepted(v1::Outcome {
            kind: Some(v1::outcome::Kind::Posted(entry)),
        })) => {
            let amount = AMOUNTS[i].to_string();
            assert_eq!(
                entry.lines,
                [
                    line(2, v1::Side::Debit, &amount),
                    line(3, v1::Side::Credit, &amount)
                ]
            );
            Answer::Posted(entry.entry_id)
        }
        Some(v1::decision::Result::Rejected(rejection)) => {
            assert_eq!(rejection.code, v1::ErrorCode::BelowMinimum as i32);
            assert_eq!(
                rejection.context,
                Some(v1::rejection::Context::Account(v1::AccountRef {
                    ledger_id: BOOK.0.to_string(),
                    account_id: "2".into(),
                }))
            );
            Answer::BelowMinimum
        }
        other => panic!("unexpected payment decision {other:?}"),
    }
}

async fn observe(client: &RoutingClient, trace: &Trace, op: Operation, budget: Duration) -> Call {
    let start = trace.event(format_args!("invoke {op:?}"));
    let until = Instant::now() + budget;
    let result = match op {
        Operation::Pay(i) => {
            let amount = AMOUNTS[i].to_string();
            let request = post_request(
                IDS[i],
                v1::funding::Kind::Explicit(v1::ExplicitFunding {
                    lines: vec![
                        line(2, v1::Side::Debit, &amount),
                        line(3, v1::Side::Credit, &amount),
                    ],
                }),
            );
            client
                .post_entry(request, until)
                .await
                .map(|value| decision(value, i))
        }
        Operation::Receipt(i) => client
            .get_request_outcome(
                v1::GetRequestOutcomeRequest {
                    ledger_id: BOOK.0.to_string(),
                    request_id: IDS[i].to_string(),
                },
                until,
            )
            .await
            .map(|r| r.decision.map_or(Answer::Missing, |d| decision(d, i))),
        Operation::Balance(id) => client
            .get_account(
                v1::GetAccountRequest {
                    ledger_id: BOOK.0.to_string(),
                    account_id: id.to_string(),
                },
                until,
            )
            .await
            .map(|r| Answer::Balance(r.account.unwrap().available_balance.parse().unwrap())),
    };
    let answer = result.unwrap_or_else(|e| {
        assert!(
            matches!(
                e.code(),
                Code::DeadlineExceeded | Code::Unavailable | Code::Cancelled
            ),
            "{e}"
        );
        trace.event(format_args!("unknown {op:?} code={:?}", e.code()));
        Answer::Unknown
    });
    let end = trace.event(format_args!("return {op:?} {answer:?}"));
    Call {
        op,
        start,
        end,
        answer,
    }
}

#[derive(Clone, Copy, Debug)]
struct Model {
    customer: u64,
    receipts: [Option<Answer>; 2],
}

impl Model {
    fn apply(&mut self, op: Operation) -> Answer {
        match op {
            Operation::Pay(i) => {
                if let Some(prior) = self.receipts[i] {
                    return prior;
                }
                let answer = if self.customer < AMOUNTS[i] {
                    Answer::BelowMinimum
                } else {
                    self.customer -= AMOUNTS[i];
                    // Funding is entry 1. Only one of these competing payments fits.
                    Answer::Posted(2)
                };
                self.receipts[i] = Some(answer);
                answer
            }
            Operation::Receipt(i) => self.receipts[i].unwrap_or(Answer::Missing),
            Operation::Balance(2) => Answer::Balance(self.customer),
            Operation::Balance(3) => Answer::Balance(10 - self.customer),
            Operation::Balance(other) => panic!("unexpected wallet {other}"),
        }
    }
}

// Enumerate legal serializations. Completed-before-invoked calls retain their
// real-time order. An uncertain call can be skipped or can take effect; later
// receipt reads and same-ID results determine which explanations survive.
fn history_matches(calls: &[Call], model: Model, placed: u32) -> bool {
    if placed.count_ones() as usize == calls.len() {
        return true;
    }
    for (i, call) in calls.iter().enumerate() {
        if placed & (1 << i) != 0
            || calls
                .iter()
                .enumerate()
                .any(|(j, other)| placed & (1 << j) == 0 && other.end < call.start)
        {
            continue;
        }
        let marked = placed | (1 << i);
        let mut next = model;
        let expected = next.apply(call.op);
        if call.answer == Answer::Unknown && history_matches(calls, model, marked) {
            return true;
        }
        if (call.answer == Answer::Unknown || call.answer == expected)
            && history_matches(calls, next, marked)
        {
            return true;
        }
    }
    false
}

#[test]
fn model_rejects_double_spends_and_completed_before_read_violations() {
    let start = Model {
        customer: 10,
        receipts: [None; 2],
    };
    let pay = Call {
        op: Operation::Pay(0),
        start: 0,
        end: 2,
        answer: Answer::Posted(2),
    };
    let other = Call {
        op: Operation::Pay(1),
        start: 1,
        end: 3,
        answer: Answer::Posted(2),
    };
    assert!(!history_matches(&[pay, other], start, 0));
    let read = Call {
        op: Operation::Balance(2),
        start: 3,
        end: 4,
        answer: Answer::Balance(10),
    };
    assert!(!history_matches(&[pay, read], start, 0));
    assert!(history_matches(&[pay, Call { start: 1, ..read }], start, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_wallet_history_survives_journal_outage_and_leader_replacement() {
    use Operation::{Balance, Pay, Receipt};
    let external = TempDir::new().unwrap();
    let remote = external.path().join("journal");
    let detached = external.path().join("detached");
    Journal::init(&remote, &super::super::recovery::spec(), CLUSTER).unwrap();
    let mut cluster = super::super::recovery::original_cluster(&remote).await;
    let old = cluster.leader().await;
    let mut api = PublicApi::start(&cluster, old).await;
    let client = api.routed.clone();
    for id in 1..=3 {
        accepted(&client.create_account(wallet(id), deadline()).await.unwrap());
    }
    let deposit = post_request(
        10,
        v1::funding::Kind::Explicit(v1::ExplicitFunding {
            lines: vec![
                line(1, v1::Side::Debit, "10"),
                line(2, v1::Side::Credit, "10"),
            ],
        }),
    );
    assert_eq!(
        posted(&client.post_entry(deposit, deadline()).await.unwrap()).entry_id,
        1
    );
    let trace = Trace {
        clock: Instant::now(),
        next: AtomicU64::new(0),
    };
    trace.event(format_args!("schedule=two-wallet-v1 old_leader={old} request_ids={IDS:?} amounts={AMOUNTS:?} initial_customer=10 initial_merchant=0"));
    let mut calls = vec![observe(&client, &trace, Balance(2), WAIT).await];
    assert_eq!(calls[0].answer, Answer::Balance(10));
    let prior = cluster.handle(old).status().await.unwrap();
    std::fs::rename(&remote, &detached).unwrap();
    trace.event("archive_detached");
    let outage = Duration::from_secs(3);
    let (a, b, read, ()) = tokio::join!(
        observe(&client, &trace, Pay(0), outage),
        observe(&client, &trace, Pay(1), outage),
        observe(&client, &trace, Balance(3), outage),
        async {
            timeout(WAIT, async {
                loop {
                    let status = cluster.handle(old).status().await.unwrap();
                    let new_batches = status.proposed_batches - prior.proposed_batches;
                    if status.proposed_commands >= prior.proposed_commands + 2
                        && status.applied_index >= prior.applied_index + new_batches
                        && status.applied_index > status.protected_index.unwrap()
                    {
                        trace.event(format_args!(
                            "quorum_applied index={} covered={:?} batches={new_batches}",
                            status.applied_index, status.protected_index
                        ));
                        break;
                    }
                    sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("both concurrent originals must reach the quorum before leader loss");
            cluster.stop(old).await;
            api.servers.remove(&old).unwrap().shutdown().await.unwrap();
            let replacement = cluster.leader().await;
            assert_ne!(old, replacement);
            trace.event(format_args!("leader_replaced new_leader={replacement}"));
        }
    );
    assert!([a, b, read].iter().all(|r| r.answer == Answer::Unknown));
    calls.extend([a, b, read]);
    std::fs::rename(&detached, &remote).unwrap();
    trace.event("archive_restored");
    let (ra, rb) = tokio::join!(
        observe(&client, &trace, Receipt(0), WAIT),
        observe(&client, &trace, Receipt(1), WAIT)
    );
    for row in [ra, rb] {
        assert!(matches!(
            row.answer,
            Answer::Posted(_) | Answer::BelowMinimum
        ));
    }
    calls.extend([ra, rb]);
    let (a, b, balance) = tokio::join!(
        observe(&client, &trace, Pay(0), WAIT),
        observe(&client, &trace, Pay(1), WAIT),
        observe(&client, &trace, Balance(2), WAIT)
    );
    assert_eq!(a.answer, ra.answer);
    assert_eq!(b.answer, rb.answer);
    calls.extend([
        a,
        b,
        balance,
        observe(&client, &trace, Balance(3), WAIT).await,
    ]);
    assert!(calls[4..].iter().all(|c| c.answer != Answer::Unknown));
    assert!(
        history_matches(
            &calls,
            Model {
                customer: 10,
                receipts: [None; 2]
            },
            0
        ),
        "no legal history: {calls:?}"
    );
    trace.event(format_args!(
        "verified calls={} wallet={:?} merchant={:?}",
        calls.len(),
        balance.answer,
        calls.last().unwrap().answer
    ));
    for (id, server) in api.servers {
        server.shutdown().await.unwrap();
        cluster.stop(id).await;
    }
}
