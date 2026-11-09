//! Compare local account point reads with equal combined account and RocksDB
//! block-cache budgets. Run with `cargo run -p ledger-replica --release --example account_cache`.
//! RPC, Raft writes, setup, SST flushes, and one warm-up traversal are outside
//! the timer. The operating system's file cache remains enabled.
mod support;

use std::fs;
use std::hint::black_box;
use std::path::Path;
use std::time::Instant;

use ledger_core::{AccountId, Side};
use ledger_replica::{CacheBudget, ProposalBatch, Replica};
use support::{LEDGER, commit, config, create, elect};
use tempfile::tempdir;

const WALLETS: usize = 32_768;
const READS: usize = 131_072;
const TOTAL_BUDGET: usize = 1 << 20;

fn fill(voter: &mut Replica) {
    for start in (0..WALLETS).step_by(ProposalBatch::MAX_COMMANDS) {
        let end = (start + ProposalBatch::MAX_COMMANDS).min(WALLETS);
        let commands = (start..end)
            .map(|i| create(i as u128 + 1, Side::Credit))
            .collect::<Vec<_>>();
        commit(voter, &commands);
    }
    voter.flush_projection().unwrap();
}

fn sst_bytes(root: &Path) -> u64 {
    fs::read_dir(root.join("state/data"))
        .unwrap()
        .map(|file| {
            let file = file.unwrap();
            if file
                .path()
                .extension()
                .is_some_and(|extension| extension == "sst")
            {
                file.metadata().unwrap().len()
            } else {
                0
            }
        })
        .sum()
}

fn measure(voter: &Replica, budget: CacheBudget, working_set: usize, sst: u64) {
    // The coprime stride visits every member once per cycle, in identical order
    // across cache modes. Even the first traversal goes through the live projection.
    let read = |i| {
        let number = 1 + (i * 4051usize) % working_set;
        let id = AccountId::new(LEDGER, number as u128);
        let account = voter.account(id).unwrap().expect("seeded account");
        black_box(account);
    };
    for i in 0..working_set {
        read(i);
    }
    let before = voter.cache_stats();
    let mut samples = Vec::with_capacity(READS);
    let run = Instant::now();
    for i in 0..READS {
        let start = Instant::now();
        read(i);
        samples.push(start.elapsed().as_nanos() as u64);
    }
    let elapsed = run.elapsed();
    let after = voter.cache_stats();
    let hits = after.hits - before.hits;
    let disk_lookups = after.rocksdb_reads - before.rocksdb_reads;
    assert_eq!(hits + disk_lookups, READS as u64);
    samples.sort_unstable();
    let percentile = |pct: usize| samples[(READS * pct).div_ceil(100) - 1];
    println!(
        "{},{},{},{},{},{},{},{},{:.2},{},{},{},{},{:.0}",
        budget.account_bytes,
        budget.block_bytes,
        after.allocated_bytes,
        after.slot_capacity,
        working_set,
        READS,
        sst,
        hits,
        100.0 * hits as f64 / READS as f64,
        disk_lookups,
        after.evictions - before.evictions,
        percentile(50),
        percentile(99),
        READS as f64 / elapsed.as_secs_f64()
    );
}

fn main() {
    println!(
        "account_budget_B,block_budget_B,account_backing_B,slots,working_set,reads,SST_B,hits,hit_pct,rocksdb_gets,evictions,p50_ns,p99_ns,reads_per_s"
    );
    for account_bytes in [0, 128 << 10] {
        let dir = tempdir().unwrap();
        let budget = CacheBudget {
            account_bytes,
            block_bytes: TOTAL_BUDGET - account_bytes,
        };
        let mut voter = Replica::bootstrap_with_cache(dir.path(), config(), budget).unwrap();
        elect(&mut voter);
        fill(&mut voter);
        let sst = sst_bytes(dir.path());
        assert!(sst > 0, "the measured projection must have flushed SSTs");
        for working_set in [16, WALLETS] {
            measure(&voter, budget, working_set, sst);
        }
    }
}
