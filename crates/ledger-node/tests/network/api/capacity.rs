//! Opt-in fixed-arrival SDK workload for both acknowledgement contracts.
use super::*;
use ledger_export::ExportWorkers;
use ledger_export_adapters::PosixArchive;
use ledger_node::CheckpointPolicy;
use ledger_replica::{CacheBudget, StaticReplicaConfig};
use std::{collections::HashSet, fs, path::Path};
use tokio::sync::oneshot;

const LANES: u64 = 32;
const EMPTY: u64 = LANES + 2;

fn option(name: &str, default: u64, maximum: u64) -> u64 {
    let n = std::env::var(name).map_or(default, |s| s.parse().unwrap());
    assert!((1..=maximum).contains(&n), "{name}");
    n
}

async fn cluster(learner: bool, journal: &Journal) -> Cluster {
    let mut cluster = if learner {
        Cluster::with_ids([1, 2, 3, 4]).await
    } else {
        Cluster::new().await
    };
    for id in 1..=if learner { 4 } else { 3 } {
        let cfg = ReplicaConfig {
            node: id,
            cluster: CLUSTER,
            ledger: BOOK,
            voters: vec![1, 2, 3],
            asset: "USD".into(),
            scale: 2,
        };
        let cfg: StaticReplicaConfig = if learner {
            cfg.with_learner(4)
        } else {
            cfg.into()
        };
        let path = cluster.directory.path().join(format!("n{id}"));
        let replica = Replica::bootstrap_with_cache(
            &path,
            cfg,
            CacheBudget {
                account_bytes: 2048,
                ..Default::default()
            },
        )
        .unwrap();
        let peers = cluster.config(id);
        let member = cluster.members.get_mut(&id).unwrap();
        member.node = Some(
            Node::start_configured(
                replica,
                peers,
                member.listener.take().unwrap(),
                (!learner).then(|| journal.clone()),
                CheckpointPolicy {
                    interval_entries: 512,
                    retained_entries: 256,
                },
            )
            .await
            .unwrap(),
        );
    }
    cluster
}

// Count allocated 512-byte blocks once per inode, including hardlinked seals.
// A concurrent compaction can remove a directory entry between readdir and stat.
fn disk_bytes(roots: &[std::path::PathBuf]) -> u64 {
    use std::os::unix::fs::MetadataExt;
    fn visit(p: &Path, seen: &mut HashSet<(u64, u64)>) -> std::io::Result<u64> {
        let m = fs::symlink_metadata(p)?;
        if !seen.insert((m.dev(), m.ino())) {
            return Ok(0);
        }
        let mut n = m.blocks() * 512;
        if m.is_dir() {
            for entry in fs::read_dir(p)? {
                match entry.and_then(|e| visit(&e.path(), seen)) {
                    Ok(bytes) => n += bytes,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(n)
    }
    let mut seen = HashSet::new();
    roots.iter().map(|p| visit(p, &mut seen).unwrap()).sum()
}

struct Completed {
    seq: u64,
    lane: u64,
    rejected: bool,
    id: Option<u64>,
    duplicate: bool,
    busy: usize,
    latency: Duration,
}

struct Failed {
    request: String,
    code: Code,
    busy: usize,
    had_first_reply: bool,
}

async fn call(client: RoutingClient, seq: u64, due: Instant) -> Result<Completed, Failed> {
    let limit = due + Duration::from_secs(10);
    let lane = if seq.is_multiple_of(2) {
        2
    } else {
        2 + (seq / 2) % LANES
    };
    let rejected = seq % 8 == 7;
    let lines = vec![
        line(if rejected { EMPTY } else { 1 }, v1::Side::Debit, "1"),
        line(lane, v1::Side::Credit, "1"),
    ];
    let req = post_request(
        10_000 + seq,
        v1::funding::Kind::Explicit(v1::ExplicitFunding { lines }),
    );
    let duplicate = seq.is_multiple_of(8);
    let mut busy = 0;
    let mut original = None;
    let mut latency = Duration::ZERO;
    for attempt in 0..=u8::from(duplicate) {
        let result = loop {
            match client.post_entry(req.clone(), limit).await {
                Err(e) if e.code() == Code::ResourceExhausted && Instant::now() < limit => {
                    busy += 1;
                    sleep(Duration::from_millis(5)).await;
                }
                Ok(decision) => break decision,
                Err(status) => {
                    return Err(Failed {
                        request: req.request_id,
                        code: status.code(),
                        busy,
                        had_first_reply: original.is_some(),
                    });
                }
            }
        };
        if attempt == 0 {
            original = Some(result);
            latency = Instant::now() - due;
        } else {
            assert_eq!(Some(result), original);
        }
    }
    let decision = original.unwrap();
    let id = if rejected {
        assert!(
            matches!(decision.result, Some(v1::decision::Result::Rejected(ref r)) if r.code == v1::ErrorCode::BelowMinimum as i32)
        );
        None
    } else {
        Some(posted(&decision).entry_id)
    };
    Ok(Completed {
        seq,
        lane,
        rejected,
        id,
        duplicate,
        busy,
        latency,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "release SDK traffic and disk-growth drill on local TCP"]
async fn sdk_capacity_and_storage() {
    let mode = std::env::var("LEDGER_QUAL_MODE").unwrap_or_else(|_| "protected".into());
    assert!(matches!(mode.as_str(), "protected" | "learner"));
    let observation = std::env::var("LEDGER_QUAL_OBSERVER").unwrap_or_else(|_| "active".into());
    assert!(matches!(observation.as_str(), "active" | "passive"));
    let passive = observation == "passive";
    let rate = option("LEDGER_QUAL_RATE", 300, 100_000);
    let seconds = option("LEDGER_QUAL_SECONDS", 10, 3600);
    let bound = option("LEDGER_QUAL_OUTSTANDING", 64, 1024) as usize;
    let external = TempDir::new().unwrap();
    let archive = external.path().join("journal");
    let journal = Journal::init(&archive, &super::super::recovery::spec(), CLUSTER).unwrap();
    let mut cluster = cluster(mode == "learner", &journal).await;
    let workers = cluster.members.get(&4).map(|m| {
        let feed = m.node.as_ref().unwrap().learner_feed().unwrap();
        let dir = cluster.directory.path().join("n4");
        let sink = PosixArchive::for_replica(&archive, &dir, &feed.identity().unwrap()).unwrap();
        Arc::new(
            ExportWorkers::open(&dir.join("export"), feed, Some(Arc::new(sink)), None).unwrap(),
        )
    });
    let running = workers
        .as_ref()
        .map(|workers| workers.start(Duration::from_millis(100)).unwrap());
    let leader = cluster.leader().await;
    let handle = cluster.handle(leader);
    let api = PublicApi::start(&cluster, if leader == 1 { 2 } else { 1 }).await;
    for id in 1..=EMPTY {
        accepted(
            &api.routed
                .create_account(wallet(id), deadline())
                .await
                .unwrap(),
        );
    }

    let roots = vec![
        cluster.directory.path().to_owned(),
        external.path().to_owned(),
    ];
    let initial_scan = Instant::now();
    let baseline = disk_bytes(&roots);
    let initial_scan = initial_scan.elapsed();
    let final_roots = roots.clone();
    let (stop, mut stopped) = oneshot::channel();
    let observer = handle.clone();
    let view = journal.clone();
    let sample = tokio::spawn(async move {
        let mut peak = baseline;
        let mut max_lag = None;
        let mut samples = 0;
        let mut total_sample_time = Duration::ZERO;
        let mut max_sample_time = Duration::ZERO;
        let started = Instant::now();
        loop {
            let begin = Instant::now();
            let head = if passive {
                None
            } else {
                let journal = view.clone();
                Some(
                    tokio::task::spawn_blocking(move || journal.head().unwrap().index)
                        .await
                        .unwrap(),
                )
            };
            let head_time = begin.elapsed();
            let state = observer.status().await.unwrap();
            let archived = if passive {
                match &workers {
                    Some(workers) => workers.status().archived_through,
                    None => state.protected_index,
                }
            } else {
                head
            };
            let disk_start = Instant::now();
            let size = if passive {
                None
            } else {
                let roots = roots.clone();
                let size = tokio::task::spawn_blocking(move || disk_bytes(&roots))
                    .await
                    .unwrap();
                peak = peak.max(size);
                Some(size)
            };
            let disk_time = disk_start.elapsed();
            let sample_time = begin.elapsed();
            let local = state.applied_index;
            let lag = archived.map(|index| local.saturating_sub(index));
            max_lag = max_lag.max(lag);
            samples += 1;
            total_sample_time += sample_time;
            max_sample_time = max_sample_time.max(sample_time);
            let number = |n: Option<u64>| n.map_or_else(|| "NA".into(), |v| v.to_string());
            println!(
                "sample,seconds={:.2},disk_bytes={},applied={local},archived={},lag={},sample_ms={:.3},journal_ms={:.3},disk_scan_ms={:.3}",
                started.elapsed().as_secs_f64(),
                number(size),
                number(archived),
                number(lag),
                sample_time.as_secs_f64() * 1000.,
                head_time.as_secs_f64() * 1000.,
                disk_time.as_secs_f64() * 1000.
            );
            tokio::select! { _ = &mut stopped => break, _ = sleep(Duration::from_secs(1)) => {} }
        }
        (peak, max_lag, samples, total_sample_time, max_sample_time)
    });
    let before = handle.status().await.unwrap();
    let start = Instant::now();
    let mut jobs = JoinSet::new();
    let mut responses = vec![];
    let mut dropped = 0;
    for seq in 0..rate * seconds {
        let due = start + Duration::from_nanos(seq * 1_000_000_000 / rate);
        tokio::time::sleep_until(due).await;
        while let Some(result) = jobs.try_join_next() {
            responses.push(result.unwrap());
        }
        if jobs.len() == bound {
            dropped += 1;
            continue;
        }
        jobs.spawn(call(api.routed.clone(), seq, due));
    }
    while let Some(result) = jobs.join_next().await {
        responses.push(result.unwrap());
    }
    let elapsed = start.elapsed();
    let mut completed = Vec::new();
    let mut failures = 0;
    for response in responses {
        match response {
            Ok(row) => completed.push(row),
            Err(failed) => {
                failures += 1;
                let current = api
                    .routed
                    .get_request_outcome(
                        v1::GetRequestOutcomeRequest {
                            ledger_id: BOOK.0.to_string(),
                            request_id: failed.request.clone(),
                        },
                        deadline(),
                    )
                    .await
                    .map(|r| r.decision.is_some())
                    .map_err(|e| e.code());
                println!(
                    "terminal request={} status={:?} busy_resubmits={} had_first_reply={} receipt_present={current:?}",
                    failed.request, failed.code, failed.busy, failed.had_first_reply
                );
            }
        }
    }
    println!(
        "delivery offered={} completed={} sender_drops={dropped} terminal_errors={failures}",
        rate * seconds,
        completed.len()
    );
    assert_eq!(
        failures, 0,
        "ambiguous or failed calls invalidate this trial; see per-ID receipts"
    );
    let after = handle.status().await.unwrap();
    let (verified, final_head_reads) = timeout(WAIT, async {
        let mut reads = 0;
        loop {
            reads += 1;
            let index = journal.head().unwrap().index;
            if index >= after.applied_index {
                break (index, reads);
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("archive failed to reach last acknowledged write");
    stop.send(()).unwrap();
    let (peak, max_lag, samples, sample_time, max_sample_time) = sample.await.unwrap();
    let final_scan = Instant::now();
    let final_disk = disk_bytes(&final_roots);
    let final_scan = final_scan.elapsed();
    let peak = if passive {
        "NA".into()
    } else {
        peak.max(final_disk).to_string()
    };
    let max_lag = max_lag.map_or_else(|| "NA".into(), |v| v.to_string());
    let reads = if passive { 0 } else { samples };
    println!(
        "observer mode={observation} samples={samples} periodic_journal_head_reads={reads} periodic_disk_scans={reads} sum_sample_ms={:.3} max_sample_ms={:.3} endpoint_disk_scans=2 initial_scan_ms={:.3} final_scan_ms={:.3} final_journal_head_reads={final_head_reads} independently_verified_index={verified}",
        sample_time.as_secs_f64() * 1000.,
        max_sample_time.as_secs_f64() * 1000.,
        initial_scan.as_secs_f64() * 1000.,
        final_scan.as_secs_f64() * 1000.
    );
    let mut entry_ids = HashSet::new();
    let mut counts = [0; LANES as usize];
    let (mut rejected, mut retries, mut busy) = (0, 0, 0);
    for row in &completed {
        if let Some(id) = row.id {
            assert!(
                entry_ids.insert(id),
                "duplicate posting for request {}",
                row.seq
            );
            counts[(row.lane - 2) as usize] += 1;
        }
        rejected += usize::from(row.rejected);
        retries += usize::from(row.duplicate);
        busy += row.busy;
    }
    for (offset, count) in counts.iter().enumerate() {
        let a = api
            .routed
            .get_account(
                v1::GetAccountRequest {
                    ledger_id: BOOK.0.to_string(),
                    account_id: (2 + offset).to_string(),
                },
                deadline(),
            )
            .await
            .unwrap()
            .account
            .unwrap();
        assert_eq!(a.posted_balance, count.to_string());
    }
    assert_eq!(completed.len() + dropped, (rate * seconds) as usize);
    assert_eq!(entry_ids.len() + rejected, completed.len());
    completed.sort_unstable_by_key(|x| x.latency);
    let q = |p: usize| {
        completed[(completed.len() * p).div_ceil(100) - 1]
            .latency
            .as_secs_f64()
            * 1000.
    };
    println!(
        "sdk_capacity mode={mode} rate={rate} seconds={seconds} offered={} completed={} posted={} rejected={rejected} same_id_replays={retries} busy_resubmits={busy} drops={dropped} decisions_per_s={:.1} p50_ms={:.2} p95_ms={:.2} p99_ms={:.2} initial_disk_bytes={baseline} peak_sampled_disk_bytes={peak} final_disk_bytes={final_disk} max_sampled_archive_lag={max_lag} cache_hits={} cache_db_reads={} last_checkpoint={:?}",
        rate * seconds,
        completed.len(),
        entry_ids.len(),
        completed.len() as f64 / elapsed.as_secs_f64(),
        q(50usize),
        q(95),
        q(99),
        after.account_cache.hits - before.account_cache.hits,
        after.account_cache.rocksdb_reads - before.account_cache.rocksdb_reads,
        after.checkpoint_index
    );
    for (_, server) in api.servers {
        server.shutdown().await.unwrap();
    }
    if let Some(running) = running {
        tokio::task::spawn_blocking(move || running.shutdown())
            .await
            .unwrap()
            .unwrap();
    }
    for id in cluster.members.keys().copied().collect::<Vec<_>>() {
        cluster.stop(id).await;
    }
}
