//! Local protected-payment and mTLS peer catch-up drill. Run alone:
//! cargo test -p ledger-node --test network snapshot_catchup_under_payment_load -- --ignored --nocapture
use super::*;
use std::collections::HashSet;
use std::os::unix::fs::MetadataExt;
use std::sync::atomic::{AtomicBool, Ordering};

const CLIENTS: usize = 32;
const FIXTURE_CLIENTS: usize = 96;
const INITIAL_POSTS: usize = 8192;
const MORE_POSTS: usize = 2048;
const PHASE_STRIDE: u128 = 1 << 64;
const LAYOUT_ROUNDS: usize = 10;
const POSTS_PER_ROUND: usize = 128;

fn payment() -> Command {
    post(vec![
        Line::debit(account(1), 1),
        Line::credit(account(2), 1),
    ])
}

async fn add_posts(leader: &NodeHandle, phase: u128, count: usize) {
    let mut workers = JoinSet::new();
    for lane in 0..FIXTURE_CLIENTS {
        let leader = leader.clone();
        workers.spawn(async move {
            let command = payment();
            for i in (lane..count).step_by(FIXTURE_CLIENTS) {
                assert!(matches!(
                    protected(&leader, phase * PHASE_STRIDE + i as u128, &command).await,
                    Ok(Outcome::Posted(_))
                ));
            }
        });
    }
    while let Some(result) = workers.join_next().await {
        result.unwrap();
    }
}

struct Sample {
    latency: Duration,
    end: Instant,
}
type Workers = JoinSet<(usize, Vec<Sample>)>;

// Each caller keeps one write in flight. A transfer samples every call started
// before stop, including late responses, so the slow tail is not censored.
fn clients(
    leader: &NodeHandle,
    phase: u128,
    stop: Arc<AtomicBool>,
    limits: Option<&[usize]>,
) -> Workers {
    let mut workers = JoinSet::new();
    for lane in 0..CLIENTS {
        let stop = stop.clone();
        let leader = leader.clone();
        let limit = limits.map_or(usize::MAX, |counts| counts[lane]);
        workers.spawn(async move {
            let mut samples = Vec::new();
            let command = payment();
            while samples.len() < limit && !stop.load(Ordering::Acquire) {
                let id = phase * PHASE_STRIDE + (lane + CLIENTS * samples.len()) as u128;
                let before = Instant::now();
                let result = protected(&leader, id, &command).await;
                let end = Instant::now();
                assert!(matches!(result, Ok(Outcome::Posted(_))));
                samples.push(Sample {
                    latency: end - before,
                    end,
                });
            }
            (lane, samples)
        });
    }
    workers
}
async fn drain(mut workers: Workers) -> Vec<Vec<Sample>> {
    let mut out: Vec<_> = (0..CLIENTS).map(|_| Vec::new()).collect();
    while let Some(worker) = workers.join_next().await {
        let (lane, samples) = worker.unwrap();
        out[lane] = samples;
    }
    out
}
fn latency(samples: &[Vec<Sample>]) -> (f64, f64) {
    let mut millis: Vec<_> = samples
        .iter()
        .flatten()
        .map(|s| s.latency.as_secs_f64() * 1000.)
        .collect();
    millis.sort_by(f64::total_cmp);
    let rank = |percent: usize| millis[(percent * millis.len()).div_ceil(100) - 1];
    (rank(50), rank(99))
}
// POSIX allocated bytes attributed to unique file inodes in this voter tree.
// File names may disappear during RocksDB flush/rename. A sample is a lower
// bound on the peak, and filesystem allocation outside the tree is excluded.
fn allocated(path: &Path) -> u64 {
    fn walk(path: &Path, seen: &mut HashSet<(u64, u64)>) -> std::io::Result<u64> {
        let meta = match std::fs::symlink_metadata(path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        };
        if !seen.insert((meta.dev(), meta.ino())) {
            return Ok(0);
        }
        let mut size = meta.blocks() * 512;
        if meta.is_dir() {
            let listing = match std::fs::read_dir(path) {
                Ok(r) => r,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
                Err(e) => return Err(e),
            };
            for item in listing {
                match item {
                    Ok(it) => size += walk(&it.path(), seen)?,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(size)
    }
    walk(path, &mut HashSet::new()).unwrap()
}

fn sample_allocation(
    path: std::path::PathBuf,
    before: u64,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<u64> {
    tokio::spawn(async move {
        let mut peak = before;
        while !stop.load(Ordering::Acquire) {
            let directory = path.clone();
            peak = peak.max(
                tokio::task::spawn_blocking(move || allocated(&directory))
                    .await
                    .unwrap(),
            );
            sleep(Duration::from_millis(100)).await;
        }
        peak.max(
            tokio::task::spawn_blocking(move || allocated(&path))
                .await
                .unwrap(),
        )
    })
}

// A byte-for-byte oracle independent of the transfer manifest and its counters.
// Only the small, sealed test fixture is read this way; this is outside timing.
struct Inventory {
    index: u64,
    tables: Vec<Vec<u8>>,
    manifest_bytes: u64,
    bytes: u64,
}

impl Inventory {
    fn read(dir: &Path, seal: CheckpointRef) -> Self {
        use std::fmt::Write as _;
        let mut name = String::from("gen-");
        for byte in seal.digest {
            write!(&mut name, "{byte:02x}").unwrap();
        }
        let generation = dir.join("checkpoints").join(name);
        let manifest_bytes = std::fs::metadata(generation.join("manifest"))
            .unwrap()
            .len();
        let mut tables = Vec::new();
        let mut bytes = 0;
        for file in std::fs::read_dir(generation.join("db")).unwrap() {
            let file = file.unwrap();
            bytes += file.metadata().unwrap().len();
            if file
                .path()
                .extension()
                .is_some_and(|s| s == "sst" || s == "blob")
            {
                tables.push(std::fs::read(file.path()).unwrap());
            }
        }
        assert!(!tables.is_empty());
        Self {
            index: seal.index,
            tables,
            manifest_bytes,
            bytes,
        }
    }

    fn reusable(&self, receiver: &Self) -> u64 {
        self.tables
            .iter()
            .filter(|t| receiver.tables.contains(t))
            .map(|t| t.len() as u64)
            .sum()
    }
}

// Give the returning voter an independently rebuilt projection, then let real
// RocksDB background compactions run under its own flush schedule. All writes
// still arrive by mTLS Raft replication and receive journal protection.
async fn independent_layout(
    cluster: &mut Cluster,
    journal: &Journal,
    leader: &NodeHandle,
    behind: u64,
) -> Inventory {
    let dir = cluster.directory.path().join(format!("n{behind}"));
    let voter =
        Replica::rebuild_from_log(&dir, cfg(behind, CLUSTER, &[1, 2, 3]), Default::default())
            .unwrap();
    voter.begin_checkpoint().unwrap().unwrap().finish().unwrap();
    drop(voter);
    start(cluster, behind, journal).await;
    let follower = cluster.handle(behind);
    let log = dir.join("state/data/LOG");
    let compactions = || {
        std::fs::read_to_string(&log)
            .unwrap()
            .matches("\"compaction_finished\"")
            .count()
    };
    let before = compactions();
    for round in 0..LAYOUT_ROUNDS {
        add_posts(leader, 10 + round as u128, POSTS_PER_ROUND).await;
        let through = leader.status().await.unwrap().applied_index;
        timeout(WAIT, async {
            while follower.status().await.unwrap().applied_index < through {
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        follower.checkpoint().await.unwrap();
    }
    timeout(WAIT, async {
        while compactions() <= before {
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("receiver must complete an independent projection compaction");
    let completed_compactions = compactions() - before;
    assert!(matches!(
        protected(leader, 25 * PHASE_STRIDE, &payment()).await,
        Ok(Outcome::Posted(_))
    ));
    let sender_seal = leader.checkpoint().await.unwrap();
    timeout(WAIT, async {
        while follower.status().await.unwrap().applied_index < sender_seal.index {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let receiver_seal = follower.checkpoint().await.unwrap();
    assert_eq!(
        (sender_seal.index, sender_seal.term),
        (receiver_seal.index, receiver_seal.term)
    );
    let leader_id = leader.status().await.unwrap().node;
    let source = Inventory::read(
        &cluster.directory.path().join(format!("n{leader_id}")),
        sender_seal,
    );
    let receiver = Inventory::read(&dir, receiver_seal);
    assert!(source.tables.iter().any(|t| !receiver.tables.contains(t)));
    assert!(receiver.tables.iter().any(|t| !source.tables.contains(t)));
    let receipts = [RequestId(PHASE_STRIDE), RequestId(10 * PHASE_STRIDE)];
    let expected = {
        let mut result = Vec::new();
        for id in receipts {
            result.push(
                leader
                    .protected_outcome(id, payment(), Instant::now() + WAIT)
                    .await
                    .unwrap(),
            );
        }
        result
    };
    wait_for_match(leader, behind, sender_seal.index).await;
    cluster.stop(behind).await;
    let voter = Replica::reopen(&dir, cfg(behind, CLUSTER, &[1, 2, 3])).unwrap();
    assert_eq!(voter.recovered_from(), receiver_seal.index);
    for (id, expected) in receipts.into_iter().zip(expected) {
        assert!(matches!(expected, Some(Ok(Outcome::Posted(_)))));
        assert_eq!(voter.outcome_for(id, &payment()).unwrap(), expected);
    }
    eprintln!(
        "independent layout at cut {}: {} receiver compactions, {} sender SSTs, {} receiver SSTs, {} identical SST bytes",
        sender_seal.index,
        completed_compactions,
        source.tables.len(),
        receiver.tables.len(),
        source.reusable(&receiver)
    );
    receiver
}

async fn phase(
    cluster: &mut Cluster,
    journal: &Journal,
    leader: &NodeHandle,
    behind: u64,
    id: u128,
    name: &str,
    receiver: Option<&Inventory>,
) -> (usize, Inventory) {
    let seal = leader.checkpoint().await.unwrap();
    // The target includes an ordinary Raft tail above the checkpoint.
    let marker_id = id * PHASE_STRIDE;
    let marker = protected(leader, marker_id, &payment()).await;
    assert!(matches!(marker, Ok(Outcome::Posted(_))));
    let target = leader.status().await.unwrap().applied_index;
    let dir = cluster.directory.path().join(format!("n{behind}"));
    let disk_before = allocated(&dir);
    let opening = Instant::now();
    start(cluster, behind, journal).await;
    let started = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let clients_in_flight = clients(leader, id + 1, stop.clone(), None);
    let sampler = sample_allocation(dir.clone(), disk_before, stop.clone());
    let transfer = wait_for_tail(cluster.handle(behind), target, Some(seal))
        .await
        .expect("snapshot transfer statistics");
    let end = Instant::now();
    stop.store(true, Ordering::Release);
    let samples = drain(clients_in_flight).await;
    let allocated_peak = sampler.await.unwrap();
    let during = samples.iter().flatten().filter(|s| s.end <= end).count();
    assert!(
        during > 0,
        "need a protected acknowledgement while catch-up is in progress"
    );
    assert!(transfer.history_entries > 0);
    let counts: Vec<_> = samples.iter().map(Vec::len).collect();
    let n: usize = counts.iter().sum();
    wait_for_match(leader, behind, leader.status().await.unwrap().applied_index).await;
    cluster.stop(behind).await;
    let first = RequestId(PHASE_STRIDE);
    let original = leader
        .protected_outcome(first, payment(), Instant::now() + WAIT)
        .await
        .unwrap();
    assert!(matches!(original, Some(Ok(Outcome::Posted(_)))));
    let follower = Replica::reopen(&dir, cfg(behind, CLUSTER, &[1, 2, 3])).unwrap();
    assert_eq!(follower.recovered_from(), seal.index);
    assert_eq!(
        follower
            .outcome_for(RequestId(marker_id), &payment())
            .unwrap(),
        Some(marker)
    );
    assert_eq!(follower.outcome_for(first, &payment()).unwrap(), original);
    if name == "independent" {
        // These real payments committed while this voter was offline, inside
        // the rebuilt interval. The Raft-tail marker above covers a later cut.
        for offset in [0, MORE_POSTS - 1] {
            let id = RequestId(20 * PHASE_STRIDE + offset as u128);
            let expected = leader
                .protected_outcome(id, payment(), Instant::now() + WAIT)
                .await
                .unwrap();
            assert!(matches!(expected, Some(Ok(Outcome::Posted(_)))));
            assert_eq!(follower.outcome_for(id, &payment()).unwrap(), expected);
        }
    }
    let installed = Inventory::read(&dir, follower.current_checkpoint().unwrap().unwrap());
    // Recount the exact Raft bytes independently of the transport. Both modes
    // keep the full committed history available after installation/reopen.
    let mut after = seal.index - transfer.history_entries;
    let mut canonical_bytes = 0;
    let committed = follower.committed_source();
    while after < seal.index {
        let page = committed.entries_after(after, seal.index).unwrap();
        after = page.last().unwrap().index;
        canonical_bytes += page
            .iter()
            .map(|e| u64::from(protobuf::Message::compute_size(e)))
            .sum::<u64>();
    }
    assert_eq!(transfer.history_bytes, canonical_bytes);
    drop(committed);
    drop(follower);
    let leader_id = leader.status().await.unwrap().node;
    let source = Inventory::read(
        &cluster.directory.path().join(format!("n{leader_id}")),
        seal,
    );
    assert_eq!(transfer.manifest_bytes, source.manifest_bytes);
    let legacy_reuse = receiver.map_or(0, |r| source.reusable(r));
    let legacy_files = source.bytes - legacy_reuse;
    let legacy_total = source.manifest_bytes + legacy_files + canonical_bytes;
    let payload = transfer.manifest_bytes
        + transfer.file_bytes
        + transfer.history_bytes
        + transfer.replay_proof_bytes;
    assert!(transfer.rpc_message_bytes >= payload);
    if name == "independent" {
        let base = receiver.unwrap().index;
        assert_eq!(transfer.replay_base_index, Some(base));
        assert_eq!(transfer.history_entries, seal.index - base);
        assert_eq!(
            (transfer.file_bytes, transfer.reused_or_staged_file_bytes),
            (0, 0)
        );
        assert!(transfer.replay_proof_bytes > 0);
        // Even the complete optimized reverse RPC bodies are smaller than the
        // original mode's data alone for precisely this offer and history.
        assert!(transfer.rpc_message_bytes < legacy_total);
    } else {
        assert_eq!(transfer.replay_base_index, None);
        assert_eq!(transfer.file_bytes, legacy_files);
        assert_eq!(transfer.reused_or_staged_file_bytes, legacy_reuse);
        assert!(transfer.file_bytes > 0);
        assert_eq!(name == "shared", legacy_reuse > 0);
    }
    let baseline_stop = Arc::new(AtomicBool::new(false));
    let control_sampler = sample_allocation(dir, 0, baseline_stop.clone());
    let control_start = Instant::now();
    let baseline = drain(clients(
        leader,
        id + 2,
        Arc::new(AtomicBool::new(false)),
        Some(&counts),
    ))
    .await;
    let control_elapsed = control_start.elapsed();
    baseline_stop.store(true, Ordering::Release);
    control_sampler.await.unwrap();
    assert_eq!(counts, baseline.iter().map(Vec::len).collect::<Vec<_>>());
    let (p50, p99) = latency(&samples);
    let (base50, base99) = latency(&baseline);
    eprintln!(
        "{name},{},{target},{},{},{},{},{},{},{:.1},{:.1},{n},{during},{p50:.2},{p99:.2},{base50:.2},{base99:.2},{:.1},{disk_before},{allocated_peak},{legacy_files},{legacy_total},{payload},{},{},{}",
        seal.index,
        transfer.manifest_bytes,
        transfer.file_bytes,
        transfer.reused_or_staged_file_bytes,
        transfer.history_bytes,
        transfer.history_entries,
        CLIENTS,
        (started - opening).as_secs_f64() * 1000.,
        (end - started).as_secs_f64() * 1000.,
        control_elapsed.as_secs_f64() * 1000.,
        transfer.replay_proof_bytes,
        transfer.rpc_message_bytes,
        transfer
            .replay_base_index
            .map_or_else(String::new, |index| index.to_string())
    );
    (1 + 2 * n, installed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "multi-megabyte local mTLS snapshot and foreground latency measurement"]
async fn snapshot_catchup_under_payment_load() {
    let remote = TempDir::new().unwrap();
    let journal = Journal::init(remote.path(), &spec(), CLUSTER).unwrap();
    let mut cluster = Cluster::new().await;
    for i in 1..=3 {
        start(&mut cluster, i, &journal).await;
    }
    let leader_id = cluster.leader().await;
    let leader = cluster.handle(leader_id);
    let behind = (1..=3).find(|i| *i != leader_id).unwrap();
    cluster.stop(behind).await;
    for (id, side) in [(1, Side::Debit), (2, Side::Credit)] {
        assert_eq!(
            protected(&leader, id, &create(id, side)).await,
            Ok(Outcome::AccountCreated(account(id)))
        );
    }
    add_posts(&leader, 1, INITIAL_POSTS).await;
    eprintln!(
        "phase,seal_index,target_index,manifest_bytes,file_bytes,reused_file_bytes,history_bytes,history_entries,clients,follower_reopen_ms,catchup_ms,foreground_calls,completed_in_window,catchup_p50_ms,catchup_p99_ms,baseline_p50_ms,baseline_p99_ms,baseline_ms,receiver_alloc_before,receiver_alloc_peak,legacy_same_offer_file_bytes,legacy_same_offer_payload_bytes,measured_payload_bytes,replay_proof_bytes,reverse_rpc_message_bytes,replay_base_index"
    );
    let (cold, prior) = phase(&mut cluster, &journal, &leader, behind, 2, "cold", None).await;
    add_posts(&leader, 5, MORE_POSTS).await;
    let (shared, _) = phase(
        &mut cluster,
        &journal,
        &leader,
        behind,
        6,
        "shared",
        Some(&prior),
    )
    .await;
    let receiver = independent_layout(&mut cluster, &journal, &leader, behind).await;
    add_posts(&leader, 20, MORE_POSTS).await;
    let (compacted, _) = phase(
        &mut cluster,
        &journal,
        &leader,
        behind,
        21,
        "independent",
        Some(&receiver),
    )
    .await;
    let expected = (1
        + INITIAL_POSTS
        + 2 * MORE_POSTS
        + LAYOUT_ROUNDS * POSTS_PER_ROUND
        + cold
        + shared
        + compacted) as u128;
    let total = leader
        .protected_account(account(1), Instant::now() + WAIT)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(total.counters().debits_posted, expected);
    for id in 1..=3 {
        if id != behind {
            cluster.stop(id).await;
        }
    }
}
