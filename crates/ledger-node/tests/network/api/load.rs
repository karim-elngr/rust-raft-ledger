//! Opt-in public mTLS workload. Run alone with --release --ignored --nocapture.
//! LEDGER_LOAD_CLIENTS, LEDGER_LOAD_CALLS, LEDGER_LOAD_RATE, LEDGER_LOAD_SECONDS
//! and LEDGER_LOAD_MAX_OUTSTANDING control caller count, closed-loop payments per
//! caller, open-loop arrivals per second, duration and in-flight limit.
//! LEDGER_LOAD_RETRY_BUSY=0 surfaces the first terminal admission refusal and
//! flags the phase for review, reproducing the un-retried diagnostic.
use super::*;
use ledger_node::NodeStatus;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::process::Command as Process;
use std::sync::OnceLock;
use tokio::sync::oneshot;

const WALLETS: usize = 32;
const SOURCE: u64 = 2;
const DEST: u64 = SOURCE + WALLETS as u64;
const CALL_TIMEOUT: Duration = Duration::from_secs(5);

struct Options {
    clients: usize,
    calls: usize,
    rate: usize,
    seconds: usize,
    limit: usize,
}
impl Options {
    fn get() -> Self {
        fn env(key: &str, default: usize, max: usize) -> usize {
            let n = std::env::var(key)
                .map_or(default, |v| v.parse().expect("positive benchmark option"));
            assert!((1..=max).contains(&n), "{key} outside 1..={max}");
            n
        }
        Self {
            clients: env("LEDGER_LOAD_CLIENTS", 32, 256),
            calls: env("LEDGER_LOAD_CALLS", 64, 4096),
            rate: env("LEDGER_LOAD_RATE", 300, 100_000),
            seconds: env("LEDGER_LOAD_SECONDS", 4, 3600),
            limit: env("LEDGER_LOAD_MAX_OUTSTANDING", 64, 1024),
        }
    }
}
fn grpc<T>(v: T) -> Request<T> {
    let mut r = Request::new(v);
    r.set_timeout(CALL_TIMEOUT);
    r
}
fn retry_busy() -> bool {
    static POLICY: OnceLock<bool> = OnceLock::new();
    *POLICY.get_or_init(
        || match std::env::var("LEDGER_LOAD_RETRY_BUSY").as_deref() {
            Err(_) | Ok("1") => true,
            Ok("0") => false,
            Ok(_) => panic!("LEDGER_LOAD_RETRY_BUSY must be 0 or 1"),
        },
    )
}
async fn send_post(
    mut client: WireClient<Channel>,
    req: v1::PostEntryRequest,
) -> Result<Response<v1::Decision>, Status> {
    client.post_entry(grpc(req)).await
}
async fn read_account(
    mut client: WireClient<Channel>,
    id: u64,
) -> Result<Response<v1::GetAccountResponse>, Status> {
    client.get_account(grpc(lookup(id))).await
}
fn lookup(id: u64) -> v1::GetAccountRequest {
    v1::GetAccountRequest {
        ledger_id: BOOK.0.to_string(),
        account_id: id.to_string(),
    }
}
fn payment(phase: u64, seq: usize, w: usize) -> v1::PostEntryRequest {
    post_request(
        phase * 10_000_000_000 + seq as u64,
        v1::funding::Kind::Explicit(v1::ExplicitFunding {
            lines: vec![
                line(SOURCE + w as u64, v1::Side::Debit, "1"),
                line(DEST + w as u64, v1::Side::Credit, "1"),
            ],
        }),
    )
}

async fn fund(
    client: &mut WireClient<Channel>,
    phase: u64,
    amounts: &[u64; WALLETS],
) -> (v1::PostEntryRequest, v1::Decision) {
    let total: u64 = amounts.iter().sum();
    let mut lines = vec![line(1, v1::Side::Debit, &total.to_string())];
    for (w, &v) in amounts.iter().enumerate().filter(|(_, v)| **v > 0) {
        lines.push(line(SOURCE + w as u64, v1::Side::Credit, &v.to_string()));
    }
    let req = post_request(
        1_000_000_000 + phase,
        v1::funding::Kind::Explicit(v1::ExplicitFunding { lines }),
    );
    let result = client
        .post_entry(grpc(req.clone()))
        .await
        .unwrap()
        .into_inner();
    posted(&result);
    (req, result)
}
#[derive(Clone, Copy)]
enum Op {
    Payment,
    Read,
    Retry,
}
struct Sample {
    latency: Duration,
    done: Instant,
}
#[derive(Default)]
struct Samples {
    times: [Vec<Sample>; 3],
    errors: HashMap<Code, u64>,
    rpc_attempts: [usize; 3],
    busy: [usize; 3],
    first_busy: [usize; 3],
    posted: Vec<(usize, u64)>,
    rejected: usize,
    dropped: usize,
}
impl Samples {
    async fn call<T, F: Future<Output = Result<Response<T>, Status>>>(
        &mut self,
        op: Op,
        arrival: Instant,
        mut rpc: impl FnMut() -> F,
    ) -> Option<T> {
        // RESOURCE_EXHAUSTED is a bounded admission refusal. Keep one logical
        // operation and its original deadline/identity across wire attempts.
        let deadline = arrival + CALL_TIMEOUT;
        let mut first = true;
        let result = tokio::time::timeout_at(deadline, async {
            loop {
                self.rpc_attempts[op as usize] += 1;
                match rpc().await {
                    Err(status) if status.code() == Code::ResourceExhausted => {
                        self.busy[op as usize] += 1;
                        self.first_busy[op as usize] += usize::from(first);
                        if !retry_busy() {
                            break Err(status);
                        }
                        first = false;
                        sleep(Duration::from_millis(5)).await;
                    }
                    other => break other,
                }
            }
        })
        .await;
        let end = Instant::now();
        self.times[op as usize].push(Sample {
            latency: end - arrival,
            done: end,
        });
        match result {
            Ok(Ok(resp)) => Some(resp.into_inner()),
            other => {
                let code = match other {
                    Ok(Err(e)) => e.code(),
                    Err(_) => Code::DeadlineExceeded,
                    _ => unreachable!(),
                };
                *self.errors.entry(code).or_default() += 1;
                None
            }
        }
    }
    fn merge(&mut self, mut other: Self) {
        for (to, from) in self.times.iter_mut().zip(&mut other.times) {
            to.append(from);
        }
        for i in 0..3 {
            self.rpc_attempts[i] += other.rpc_attempts[i];
            self.busy[i] += other.busy[i];
            self.first_busy[i] += other.first_busy[i];
        }
        for (e, n) in other.errors {
            *self.errors.entry(e).or_default() += n;
        }
        self.posted.append(&mut other.posted);
        self.rejected += other.rejected;
        self.dropped += other.dropped;
    }
    fn payment(&mut self, d: &v1::Decision, w: usize) {
        match &d.result {
            Some(v1::decision::Result::Accepted(_)) => {
                let entry = posted(d);
                assert_eq!(
                    entry.lines,
                    [
                        line(SOURCE + w as u64, v1::Side::Debit, "1"),
                        line(DEST + w as u64, v1::Side::Credit, "1")
                    ]
                );
                self.posted.push((w, entry.entry_id));
            }
            Some(v1::decision::Result::Rejected(r)) => {
                assert_eq!(r.code, v1::ErrorCode::BelowMinimum as i32);
                self.rejected += 1;
            }
            None => panic!("missing decision"),
        }
    }
    fn report(
        &self,
        name: &str,
        elapsed: Duration,
        before: &NodeStatus,
        after: &NodeStatus,
        remote: u64,
        max_lag: u64,
    ) {
        for (op, (label, set)) in ["payment", "quorum_read", "id_retry"]
            .into_iter()
            .zip(&self.times)
            .enumerate()
        {
            let mut us = set
                .iter()
                .map(|s| s.latency.as_micros())
                .collect::<Vec<_>>();
            us.sort_unstable();
            let q = |p: usize| {
                if us.is_empty() {
                    0
                } else {
                    us[(us.len() * p).div_ceil(100) - 1]
                }
            };
            println!(
                "latency,{name},{label},{},{:.1},{:.3},{:.3},{:.3},{},{},{}",
                us.len(),
                us.len() as f64 / elapsed.as_secs_f64(),
                q(50) as f64 / 1000.,
                q(95) as f64 / 1000.,
                q(99) as f64 / 1000.,
                self.rpc_attempts[op],
                self.first_busy[op],
                self.busy[op]
            );
        }
        println!(
            "phase,{name},{:.3},{},{},{},{},{},{},{},{},{},{},{max_lag},{},{}",
            elapsed.as_secs_f64(),
            self.posted.len(),
            self.rejected,
            self.errors.values().sum::<u64>(),
            self.dropped,
            after.proposed_commands - before.proposed_commands,
            after.proposed_batches - before.proposed_batches,
            before.applied_index,
            after.applied_index,
            after.protected_index.unwrap(),
            remote,
            after.account_cache.rocksdb_reads - before.account_cache.rocksdb_reads,
            after.account_cache.hits - before.account_cache.hits
        );
        println!(
            "raft_syncs,{name},{},{},{},{},{}",
            after.raft_syncs.ready_batches - before.raft_syncs.ready_batches,
            after.raft_syncs.commit_batches - before.raft_syncs.commit_batches,
            after.raft_syncs.append_batches - before.raft_syncs.append_batches,
            after.raft_syncs.appended_entries - before.raft_syncs.appended_entries,
            after.journal_append_publications - before.journal_append_publications
        );
        if !self.errors.is_empty() {
            eprintln!("transport_errors,{name},{:?}", self.errors);
        }
    }
}

// Independent, low-frequency local status sampling gives an observed maximum
// protection lag. Each timed account query still crosses the public quorum gate.
async fn observe(handle: NodeHandle, mut finish: oneshot::Receiver<()>) -> u64 {
    let mut max = 0;
    loop {
        tokio::select! { _=&mut finish=>return max,
            _=sleep(Duration::from_millis(20))=>{
                let s=handle.status().await.unwrap();
                max=max.max(s.applied_index.saturating_sub(s.protected_index.unwrap()));
            }
        }
    }
}

async fn closed(
    clients: &[WireClient<Channel>],
    handle: &NodeHandle,
    journal: &Journal,
    phase: u64,
    hot: bool,
    opts: &Options,
    started: Option<oneshot::Sender<()>>,
) -> Samples {
    let name = ["", "healthy", "hot_limited", "one_down", "catchup"][phase as usize];
    let before = handle.status().await.unwrap();
    let gate = Arc::new(Barrier::new(opts.clients + 1));
    let mut workers = JoinSet::new();
    for lane in 0..opts.clients {
        let client = clients[lane % clients.len()].clone();
        let (clients, calls) = (opts.clients, opts.calls);
        let gate = gate.clone();
        workers.spawn(async move {
            let w = if hot { 0 } else { lane % WALLETS };
            let mut records = Samples::default();
            gate.wait().await;
            for turn in 0..calls {
                let req = payment(phase, turn * clients + lane, w);
                let result = records
                    .call(Op::Payment, Instant::now(), || {
                        send_post(client.clone(), req.clone())
                    })
                    .await;
                if let Some(d) = &result {
                    records.payment(d, w);
                }
                if turn % 4 == 0
                    && let Some(response) = records
                        .call(Op::Read, Instant::now(), || {
                            read_account(client.clone(), SOURCE + w as u64)
                        })
                        .await
                {
                    assert!(
                        response
                            .account
                            .unwrap()
                            .posted_balance
                            .parse::<u64>()
                            .is_ok()
                    );
                }
                if turn % 8 == 0
                    && let Some(original) = result
                    && let Some(second) = records
                        .call(Op::Retry, Instant::now(), || {
                            send_post(client.clone(), req.clone())
                        })
                        .await
                {
                    assert_eq!(second, original);
                }
            }
            records
        });
    }
    let (finished, rx) = oneshot::channel();
    let monitor = tokio::spawn(observe(handle.clone(), rx));
    let t = Instant::now();
    gate.wait().await;
    if let Some(signal) = started {
        let _ = signal.send(());
    }
    let mut records = Samples::default();
    while let Some(lane) = workers.join_next().await {
        records.merge(lane.unwrap());
    }
    let elapsed = t.elapsed();
    let _ = finished.send(());
    let max_lag = monitor.await.unwrap();
    let after = handle.status().await.unwrap();
    let remote = journal.head().unwrap().index;
    records.report(name, elapsed, &before, &after, remote, max_lag);
    assert!(records.errors.is_empty(), "{name}: {:?}", records.errors);
    let n = opts.clients * opts.calls;
    let accepted = if hot { n * 7 / 8 } else { n };
    assert_eq!(
        (records.posted.len(), records.rejected),
        (accepted, n - accepted)
    );
    assert_eq!(
        after.proposed_commands - before.proposed_commands,
        (n + opts.clients * opts.calls.div_ceil(8)) as u64
    );
    assert!(
        remote >= after.protected_index.unwrap()
            && after.protected_index.unwrap() >= after.applied_index
    );
    records
}

async fn offered(
    clients: &[WireClient<Channel>],
    handle: &NodeHandle,
    journal: &Journal,
    opts: &Options,
    duplicate: (v1::PostEntryRequest, v1::Decision),
) -> Samples {
    let before = handle.status().await.unwrap();
    let (finished, rx) = oneshot::channel();
    let monitor = tokio::spawn(observe(handle.clone(), rx));
    let duplicate = Arc::new(duplicate);
    let mut workers = JoinSet::new();
    let mut records = Samples::default();
    let start = Instant::now();
    let n = opts.rate * opts.seconds;
    for seq in 0..n {
        let due = start + Duration::from_nanos((seq as u64 * 1_000_000_000) / opts.rate as u64);
        tokio::time::sleep_until(due).await;
        while let Some(done) = workers.try_join_next() {
            records.merge(done.unwrap());
        }
        if workers.len() >= opts.limit {
            records.dropped += 1;
            continue;
        }
        let client = clients[seq % clients.len()].clone();
        let duplicate = duplicate.clone();
        workers.spawn(async move {
            let mut r = Samples::default();
            let w = (seq / 8 + seq % 8) % WALLETS;
            match seq % 8 {
                0 => {
                    if let Some(a) = r
                        .call(Op::Read, due, || {
                            read_account(client.clone(), SOURCE + w as u64)
                        })
                        .await
                    {
                        assert!(a.account.is_some());
                    }
                }
                1 => {
                    if let Some(d) = r
                        .call(Op::Retry, due, || {
                            send_post(client.clone(), duplicate.0.clone())
                        })
                        .await
                    {
                        assert_eq!(d, duplicate.1);
                    }
                }
                _ => {
                    if let Some(d) = r
                        .call(Op::Payment, due, || {
                            send_post(client.clone(), payment(5, seq, w))
                        })
                        .await
                    {
                        r.payment(&d, w);
                    }
                }
            }
            r
        });
    }
    while let Some(done) = workers.join_next().await {
        records.merge(done.unwrap());
    }
    let elapsed = start.elapsed();
    let _ = finished.send(());
    let after = handle.status().await.unwrap();
    records.report(
        "scheduled",
        elapsed,
        &before,
        &after,
        journal.head().unwrap().index,
        monitor.await.unwrap(),
    );
    assert_eq!(
        records.times.iter().map(Vec::len).sum::<usize>() + records.dropped,
        n
    );
    // Report all admission drops separately. Ambiguous outcomes require review
    // before this benchmark's exact post-count reconciliation can pass.
    assert!(records.errors.is_empty(), "scheduled {:?}", records.errors);
    assert_eq!(records.rejected, 0);
    records
}

fn admin(cluster: &Cluster, node: u64, journal: &std::path::Path, verb: &str) -> String {
    use std::fmt::Write;
    let cfg = cluster.config(node);
    let mut toml = format!(
        "cluster_id = \"{CLUSTER}\"\nledger_id = \"{}\"\nnode_id = {node}\nasset = \"USD\"\nscale = 2\nlisten = \"{}\"\ndata_dir = \"n{node}\"\nrecovery_dir = {:?}\n[tls]\nca_pem = \"absent\"\ncert_pem = \"absent\"\nkey_pem = \"absent\"\n",
        BOOK.0, cluster.members[&node].addr, journal
    );
    for peer in cfg.peers {
        writeln!(
            toml,
            "[[peers]]\nnode_id = {}\nendpoint = {:?}\ndomain = {:?}\ncert_der = \"absent\"",
            peer.node, peer.endpoint, peer.domain
        )
        .unwrap();
    }
    let cfg = cluster.directory.path().join("admin.toml");
    std::fs::write(&cfg, toml).unwrap();
    let out = Process::new(env!("CARGO_BIN_EXE_ledger-node"))
        .arg(verb)
        .arg(cfg)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{verb}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

async fn restart(
    cluster: &mut Cluster,
    node: u64,
    remote: &std::path::Path,
    target: u64,
    go: oneshot::Receiver<()>,
) -> Instant {
    go.await.unwrap();
    let t = Instant::now();
    let path = cluster.directory.path().join(format!("n{node}"));
    let cfg = ReplicaConfig {
        node,
        cluster: CLUSTER,
        voters: vec![1, 2, 3],
        ledger: BOOK,
        asset: "USD".into(),
        scale: 2,
    };
    let voter = tokio::task::spawn_blocking(move || Replica::reopen(&path, cfg).unwrap())
        .await
        .unwrap();
    let reopened = voter.applied_index();
    let peers = cluster.config(node);
    let member = cluster.members.get_mut(&node).unwrap();
    let listener = TcpListener::bind(member.addr).await.unwrap();
    member.node = Some(
        Node::start_protected(
            voter,
            peers,
            listener,
            Journal::open(remote, &super::super::recovery::spec()).unwrap(),
        )
        .await
        .unwrap(),
    );
    let ready = Instant::now();
    let handle = cluster.handle(node);
    let state = timeout(WAIT, async {
        loop {
            let s = handle.status().await.unwrap();
            if s.applied_index >= target {
                break s;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let end = Instant::now();
    println!(
        "catchup_start_index={reopened} target={target} observed={} reopen_ms={:.2} reopen_plus_catchup_ms={:.2} installed_snapshot={:?}",
        state.applied_index,
        (ready - t).as_secs_f64() * 1000.,
        (end - t).as_secs_f64() * 1000.,
        state.installed_snapshot
    );
    end
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "local public mTLS payment, quorum read and retry load suite"]
async fn public_protected_load_suite() {
    let opts = Options::get();
    println!(
        "load_options clients={} calls_per_client={} scheduled_rate={} scheduled_seconds={} max_outstanding={}",
        opts.clients, opts.calls, opts.rate, opts.seconds, opts.limit
    );
    println!(
        "latency,phase,operation,calls,calls_per_second,p50_ms,p95_ms,p99_ms,wire_attempts,first_resource_exhausted,all_resource_exhausted"
    );
    println!(
        "raft_syncs,phase,leader_ready_batches,leader_light_commit_batches,leader_append_batches,leader_appended_entries,leader_journal_append_publications"
    );
    println!(
        "phase,name,seconds,accepted_new_payments,business_rejections,transport_errors,scheduler_drops,proposed_commands,proposed_batches,initial_applied,final_applied,protected_index,journal_index,max_sampled_lag,account_db_reads,account_cache_hits"
    );
    let external = TempDir::new().unwrap();
    let remote = external.path().join("journal");
    let journal = Journal::init(&remote, &super::super::recovery::spec(), CLUSTER).unwrap();
    let mut cluster = super::super::recovery::original_cluster(&remote).await;
    let leader_id = cluster.leader().await;
    let follower = if leader_id == 1 { 2 } else { 1 };
    let leader = cluster.handle(leader_id);
    let mut api = PublicApi::start(&cluster, leader_id).await;
    let mut client = api.wire(leader_id);
    assert!(opts.calls * opts.clients >= 8 && opts.rate * opts.seconds >= 8);
    let mut clients = Vec::new();
    for _ in 0..opts.clients.min(8) {
        clients.push(WireClient::new(
            api.endpoints[&leader_id].connect().await.unwrap(),
        ));
    }
    println!(
        "independent_public_tls_connections={} retry_first_resource_exhausted={}",
        clients.len(),
        retry_busy()
    );
    for id in 1..DEST + WALLETS as u64 {
        accepted(
            &client
                .create_account(grpc(wallet(id)))
                .await
                .unwrap()
                .into_inner(),
        );
    }
    let mut credits = [0; WALLETS];
    let mut debits = [0; WALLETS];
    let mut unique = HashSet::new();
    let mut offline_target = 0;
    for phase in 1..=5 {
        let mut amounts = [0; WALLETS];
        match phase {
            2 => amounts[0] = (opts.clients * opts.calls * 7 / 8) as u64,
            5 => {
                for s in 0..opts.rate * opts.seconds {
                    if s % 8 > 1 {
                        amounts[(s / 8 + s % 8) % WALLETS] += 1;
                    }
                }
            }
            _ => {
                for lane in 0..opts.clients {
                    amounts[lane % WALLETS] += opts.calls as u64;
                }
            }
        }
        let duplicate = fund(&mut client, phase, &amounts).await;
        for (i, v) in amounts.iter().enumerate() {
            credits[i] += v;
        }
        let records = if phase == 5 {
            offered(&clients, &leader, &journal, &opts, duplicate).await
        } else if phase == 4 {
            let (tx, rx) = oneshot::channel();
            let (records, caught) = tokio::join!(
                closed(&clients, &leader, &journal, phase, false, &opts, Some(tx)),
                restart(&mut cluster, follower, &remote, offline_target, rx)
            );
            println!(
                "catchup_overlap_completed_public_calls={}",
                records
                    .times
                    .iter()
                    .flatten()
                    .filter(|s| s.done <= caught)
                    .count()
            );
            records
        } else {
            closed(&clients, &leader, &journal, phase, phase == 2, &opts, None).await
        };
        for (w, id) in records.posted {
            debits[w] += 1;
            assert!(unique.insert(id));
        }
        if phase == 2 {
            let cut = journal.head().unwrap();
            let target = cluster.handle(follower);
            timeout(WAIT, async {
                while target.status().await.unwrap().applied_index < cut.index {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert!(target.checkpoint().await.unwrap().index >= cut.index);
            println!(
                "{}",
                admin(&cluster, follower, &remote, "journal-checkpoint").trim()
            );
            api.servers
                .remove(&follower)
                .unwrap()
                .shutdown()
                .await
                .unwrap();
            cluster.stop(follower).await;
            let reclaimed = admin(&cluster, follower, &remote, "reclaim");
            assert!(
                !reclaimed
                    .split_whitespace()
                    .any(|field| field == "reclaimed_entries=0"),
                "{reclaimed}"
            );
            println!("{}", reclaimed.trim());
        }
        if phase == 3 {
            offline_target = journal.head().unwrap().index;
        }
    }
    let mut ids = BTreeSet::new();
    for w in 0..WALLETS {
        for (id, debit, credit) in [
            (SOURCE + w as u64, debits[w], credits[w]),
            (DEST + w as u64, 0, debits[w]),
        ] {
            let a = client
                .get_account(grpc(lookup(id)))
                .await
                .unwrap()
                .into_inner()
                .account
                .unwrap();
            assert_eq!(a.posted_balance, (credit - debit).to_string());
            assert_eq!(
                a.counters,
                Some(v1::Counters {
                    debits_posted: debit.to_string(),
                    credits_posted: credit.to_string(),
                    debits_pending: "0".into(),
                    credits_pending: "0".into()
                })
            );
            ids.insert(id);
        }
    }
    assert_eq!(ids.len(), 2 * WALLETS);
    let bank = client
        .get_account(grpc(lookup(1)))
        .await
        .unwrap()
        .into_inner()
        .account
        .unwrap();
    assert_eq!(bank.posted_balance, credits.iter().sum::<u64>().to_string());
    assert_eq!(debits.iter().sum::<u64>(), unique.len() as u64);
    println!(
        "reconciled_unique_payment_entries={} verified_accounts={}",
        unique.len(),
        2 * WALLETS + 1
    );
    for (_, server) in api.servers {
        server.shutdown().await.unwrap();
    }
    for id in 1..=3 {
        cluster.stop(id).await;
    }
}
