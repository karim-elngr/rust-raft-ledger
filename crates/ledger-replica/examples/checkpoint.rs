//! Run with `cargo run --locked -p ledger-replica --example checkpoint`.
//! A real RocksDB voter, simulated peer acknowledgments and a temporary local
//! directory. The first account is debit-normal and funds one equal posting
//! to every other account. File inventory hashes run after capture/publication.
//! Reopening from the checkpoint runs before rebuilding the same log from
//! genesis; both use the warm OS cache. These timings exclude client/network I/O.
mod support;

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use ledger_core::{AccountId, Command, Funding, Line, Outcome, RequestId, Side};
use ledger_replica::{CacheBudget, ProposalBatch, Replica};
use sha2::{Digest, Sha256};
use support::{LEDGER, commit, config, create, elect};

const INITIAL: usize = 32_768;
const BETWEEN: usize = 1_024;
const TAIL: usize = 768;

const PAYMENT_ID_BASE: u128 = 1 << 64;

fn add(voter: &mut Replica, first: usize, count: usize) {
    let source = AccountId::new(LEDGER, 1);
    let batch_accounts = ProposalBatch::MAX_COMMANDS / 2;
    for start in (first..first + count).step_by(batch_accounts) {
        let mut commands = Vec::with_capacity(ProposalBatch::MAX_COMMANDS);
        for i in start..(start + batch_accounts).min(first + count) {
            let n = i as u128;
            if n == 1 {
                commands.push(create(n, Side::Debit));
                continue;
            }
            commands.push(create(n, Side::Credit));
            commands.push((
                RequestId(PAYMENT_ID_BASE + n),
                Command::Post {
                    funding: Funding::Explicit(vec![
                        Line::debit(source, 1),
                        Line::credit(AccountId::new(LEDGER, n), 1),
                    ]),
                    conditions: vec![],
                    corrects: None,
                },
            ));
        }
        commit(voter, &commands);
    }
}

struct Table {
    digest: [u8; 32],
    bytes: u64,
    #[cfg(unix)]
    file_id: (u64, u64),
}

struct Capture {
    index: u64,
    pause: Duration,
    seal: Duration,
    db_bytes: u64,
    // Inventory is collected after both timers stop. Match name and SHA-256
    // to measure immutable payload already present in the preceding image.
    tables: BTreeMap<String, Table>,
}

fn capture(voter: &Replica, root: &Path) -> Result<Capture, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let task = voter.begin_checkpoint()?.expect("new committed entries");
    let pause = start.elapsed();
    let finish = Instant::now();
    let reference = task.finish()?;
    let seal = finish.elapsed();
    let mut name = String::from("gen-");
    for byte in reference.digest {
        use std::fmt::Write;
        write!(name, "{byte:02x}")?;
    }
    let mut db_bytes = 0;
    let mut tables = BTreeMap::new();
    for item in fs::read_dir(root.join("checkpoints").join(name).join("db"))? {
        let item = item?;
        let metadata = item.metadata()?;
        let size = metadata.len();
        db_bytes += size;
        if item
            .path()
            .extension()
            .is_some_and(|s| s == "sst" || s == "blob")
        {
            let mut digest = Sha256::new();
            io::copy(&mut File::open(item.path())?, &mut digest)?;
            tables.insert(
                item.file_name().into_string().expect("RocksDB file name"),
                Table {
                    digest: digest.finalize().into(),
                    bytes: size,
                    #[cfg(unix)]
                    file_id: {
                        use std::os::unix::fs::MetadataExt;
                        (metadata.dev(), metadata.ino())
                    },
                },
            );
        }
    }
    Ok(Capture {
        index: reference.index,
        pause,
        seal,
        db_bytes,
        tables,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let mut voter = Replica::bootstrap(root.path(), config())?;
    elect(&mut voter);
    add(&mut voter, 1, INITIAL);
    let first = capture(&voter, root.path())?;
    add(&mut voter, 1 + INITIAL, BETWEEN);
    let second = capture(&voter, root.path())?;
    add(&mut voter, 1 + INITIAL + BETWEEN, TAIL);

    println!(
        "capture,index,owner_pause_ms,seal_publish_ms,image_db_bytes,sst_files,sst_bytes,matching_prior_files,matching_prior_bytes,new_sst_bytes"
    );
    for (stage, image, prior) in [(1, &first, None), (2, &second, Some(&first))] {
        let sst_bytes: u64 = image.tables.values().map(|t| t.bytes).sum();
        let mut matching = 0;
        let mut matching_bytes = 0;
        #[cfg(unix)]
        let mut same_inodes = 0;
        for (name, table) in &image.tables {
            if let Some(old) = prior.and_then(|c| c.tables.get(name))
                && old.bytes == table.bytes
                && old.digest == table.digest
            {
                matching += 1;
                matching_bytes += table.bytes;
                #[cfg(unix)]
                if old.file_id == table.file_id {
                    same_inodes += 1;
                }
            }
        }
        println!(
            "{stage},{},{:.3},{:.3},{},{},{sst_bytes},{matching},{matching_bytes},{}",
            image.index,
            image.pause.as_secs_f64() * 1000.0,
            image.seal.as_secs_f64() * 1000.0,
            image.db_bytes,
            image.tables.len(),
            sst_bytes - matching_bytes
        );
        #[cfg(unix)]
        if stage == 2 {
            println!("shared_prior_sst_inodes={same_inodes}");
        }
    }
    let final_index = voter.applied_index();
    let count = INITIAL + BETWEEN + TAIL;
    let last_request = RequestId(PAYMENT_ID_BASE + count as u128);
    let last_account = AccountId::new(LEDGER, count as u128);
    let expected = voter.decision(last_request)?.expect("tail decision");
    assert!(
        matches!(&expected.outcome, Ok(Outcome::Posted(entry)) if entry.id.0 == (count - 1) as u64)
    );
    drop(voter);

    let start = Instant::now();
    let restored = Replica::reopen(root.path(), config())?;
    let fast = start.elapsed();
    assert_eq!(restored.recovered_from(), second.index);
    assert_eq!(restored.applied_index(), final_index);
    assert_eq!(restored.decision(last_request)?.as_ref(), Some(&expected));
    let account = restored.account(last_account)?.expect("tail account");
    drop(restored);
    let start = Instant::now();
    let replayed = Replica::rebuild_from_log(root.path(), config(), CacheBudget::default())?;
    let full = start.elapsed();
    assert_eq!(replayed.recovered_from(), 0);
    assert_eq!(replayed.applied_index(), final_index);
    assert_eq!(replayed.account(last_account)?.as_ref(), Some(&account));
    assert_eq!(replayed.decision(last_request)?.as_ref(), Some(&expected));

    println!("accounts,postings,base_index,tail_entries,committed_index,reopen_ms,full_rebuild_ms");
    println!(
        "{count},{},{},{},{final_index},{:.3},{:.3}",
        count - 1,
        second.index,
        final_index - second.index,
        fast.as_secs_f64() * 1000.0,
        full.as_secs_f64() * 1000.0
    );
    Ok(())
}
