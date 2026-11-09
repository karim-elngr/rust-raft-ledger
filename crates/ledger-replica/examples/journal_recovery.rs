//! Independent-journal disaster recovery on a local temporary filesystem.
//! Build ledger-node and this example in the same profile:
//! cargo build --locked -p ledger-node
//! cargo run --locked -p ledger-replica --example journal_recovery
//! Optional arguments select account counts (default: 4096, 32768). Both the
//! journal and the replacement disks use the same temporary filesystem. The
//! Each size runs the same journal bytes with and without a base, alternating
//! which restores first. The reported CLI timings exclude fixture setup and
//! verification. A separate column reports advance preparation of the base.
mod support;

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as Process;
use std::time::{Duration, Instant};

use ledger_core::{
    AccountId, Allocation, CaptureAmount, Command, EntryId, Error as BusinessError, Funding, Line,
    Outcome, Remainder, RequestId, ReservationId, ReservationStatus, Shortfall, Side, Source,
    SourceFloor, Waterfall,
};
use ledger_recovery::{Head, Journal, JournalError, Spec};
use ledger_replica::{JournalVerifier, ProposalBatch, Replica, ReplicaConfig};
use protobuf::Message as _;
use sha2::{Digest, Sha256};
use support::{LEDGER, commit, config, create, elect};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const PAYMENT: u128 = 1 << 64;
const SPECIAL: u128 = 1 << 96;
const HOLD: ReservationId = ReservationId(41);
const SEGMENT_ENTRIES: u64 = 8;

fn id(n: u128) -> AccountId {
    AccountId::new(LEDGER, n)
}

fn dataset(v: &mut Replica, n: usize) -> u64 {
    let per_entry = ProposalBatch::MAX_COMMANDS / 2;
    for start in (1..=n).step_by(per_entry) {
        let mut batch = Vec::with_capacity(per_entry * 2);
        for i in start..(start + per_entry).min(n + 1) {
            let i = i as u128;
            if i == 1 {
                batch.push(create(i, Side::Debit));
                continue;
            }
            batch.push(create(i, Side::Credit));
            batch.push((
                RequestId(PAYMENT + i),
                Command::Post {
                    funding: Funding::Explicit(vec![
                        Line::debit(id(1), 100),
                        Line::credit(id(i), 100),
                    ]),
                    conditions: vec![],
                    corrects: None,
                },
            ));
        }
        commit(v, &batch);
    }
    let reserve = Command::Reserve {
        reservation: HOLD,
        funding: Funding::Waterfall(Waterfall {
            amount: 130,
            sources: [2, 3]
                .map(|n| Source {
                    account: id(n),
                    floor: SourceFloor::Configured,
                    cap: None,
                })
                .into(),
            destinations: vec![Line::credit(id(n as u128), 130)],
            shortfall: Shortfall::RequireFull,
        }),
        conditions: vec![],
    };
    let capture = Command::Capture {
        reservation: HOLD,
        amount: CaptureAmount::Lines(vec![
            Allocation {
                line: 0,
                amount: 40,
            },
            Allocation {
                line: 2,
                amount: 40,
            },
        ]),
        remainder: Some(Remainder::KeepReserved),
    };
    let rejected = Command::Void {
        reservation: ReservationId(999),
    };
    commit(v, &[(RequestId(SPECIAL), reserve.clone())]);
    let base = v.applied_index(); // The base contains the funded, open reservation.
    commit(v, &[(RequestId(SPECIAL + 1), capture)]);
    // An identical retry and conflicting reuse must preserve the original record.
    commit(
        v,
        &[
            (RequestId(SPECIAL + 2), rejected.clone()),
            (RequestId(SPECIAL), reserve),
            (RequestId(SPECIAL), rejected),
        ],
    );
    base
}

// Read the same point indexes a serving voter uses. Verify the logical rows
// outside the timing window, including every real journal line and receipt.
fn fingerprint(v: &Replica, n: usize) -> Result<[u8; 32]> {
    let mut digest = Sha256::new();
    for i in 1..=n as u128 {
        bincode::serialize_into(&mut digest, &v.account(id(i))?.expect("funded account"))?;
        bincode::serialize_into(
            &mut digest,
            &v.decision(RequestId(i))?.expect("account receipt"),
        )?;
        if i > 1 {
            bincode::serialize_into(
                &mut digest,
                &v.decision(RequestId(PAYMENT + i))?
                    .expect("payment receipt"),
            )?;
        }
        bincode::serialize_into(
            &mut digest,
            &v.entry(EntryId(i as u64))?.expect("posted entry"),
        )?;
    }
    assert!(v.entry(EntryId(n as u64 + 1))?.is_none());
    let hold = v.reservation(HOLD)?.expect("partially captured hold");
    assert_eq!(hold.status, ReservationStatus::Open);
    assert_eq!(hold.remaining, [60, 30, 90]);
    bincode::serialize_into(&mut digest, &hold)?;
    for i in SPECIAL..=SPECIAL + 2 {
        let decision = v.decision(RequestId(i))?.expect("special receipt");
        match i - SPECIAL {
            0 => assert!(
                matches!(&decision.outcome,Ok(Outcome::Reserved {lines,..}) if lines.as_ref()==[Line::debit(id(2),100),Line::debit(id(3),30),Line::credit(id(n as u128),130)])
            ),
            1 => assert!(
                matches!(&decision.outcome,Ok(Outcome::Captured {entry,..}) if entry.id==EntryId(n as u64))
            ),
            _ => assert_eq!(
                decision.outcome,
                Err(BusinessError::UnknownReservation(ReservationId(999)))
            ),
        }
        bincode::serialize_into(&mut digest, &decision)?;
    }
    let conflict = v.outcome_for(
        RequestId(SPECIAL),
        &Command::Void {
            reservation: ReservationId(999),
        },
    )?;
    assert_eq!(
        conflict,
        Some(Err(BusinessError::IdConflict(RequestId(SPECIAL))))
    );
    Ok(digest.finalize().into())
}

fn new_config(node: u64) -> ReplicaConfig {
    ReplicaConfig {
        cluster: 2,
        node: node + 10,
        voters: vec![11, 12, 13],
        ..config()
    }
}

fn cli_config(root: &Path, cfg: &ReplicaConfig, data: &str) -> Result<PathBuf> {
    let path = root.join(format!("{}-{}.toml", cfg.cluster, cfg.node));
    // Recovery CLI commands use the journal and local data. TLS keys and sockets
    // are only required by the serving process, which is outside this measurement.
    let mut text = format!(
        r#"cluster_id = "{}"
ledger_id = "{}"
node_id = {}
asset = "USD"
scale = 2
listen = "127.0.0.1:{}"
data_dir = "{data}"
recovery_dir = "journal"
[tls]
ca_pem = "unused"
cert_pem = "unused"
key_pem = "unused"
"#,
        cfg.cluster,
        LEDGER.0,
        cfg.node,
        7100 + cfg.node
    );
    for &peer in &cfg.voters {
        if peer != cfg.node {
            use std::fmt::Write;
            writeln!(
                text,
                r#"[[peers]]
node_id = {peer}
endpoint = "https://127.0.0.1:{}"
domain = "v{peer}.example"
cert_der = "unused""#,
                7100 + peer
            )?;
        }
    }
    fs::write(&path, text)?;
    Ok(path)
}

struct CliRun {
    elapsed: Duration,
    output: String,
}

fn cli(binary: &Path, action: &str, cfg: &Path) -> Result<CliRun> {
    let start = Instant::now();
    let result = Process::new(binary).arg(action).arg(cfg).output()?;
    let elapsed = start.elapsed();
    let output = format!(
        "{}{}",
        String::from_utf8(result.stdout)?,
        String::from_utf8(result.stderr)?
    );
    if !result.status.success() {
        return Err(format!("{action}: {output}").into());
    }
    Ok(CliRun { elapsed, output })
}

impl CliRun {
    fn counter(&self, key: &str) -> Result<u64> {
        self.output
            .split_whitespace()
            .filter_map(|field| field.split_once('='))
            .find(|(name, _)| *name == key)
            .ok_or_else(|| format!("CLI omitted {key}: {}", self.output))?
            .1
            .parse()
            .map_err(Into::into)
    }
}

// Logical regular-file bytes. Shared physical blocks, directory entries, and
// the filesystem cache are outside this measurement.
fn dir_bytes(dir: &Path) -> Result<u64> {
    let mut bytes = 0;
    for item in fs::read_dir(dir)? {
        let entry = item?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            bytes += dir_bytes(&entry.path())?;
        } else if kind.is_file() {
            bytes += entry.metadata()?.len();
        }
    }
    Ok(bytes)
}

#[derive(PartialEq, Eq, Debug)]
struct History {
    digest: [u8; 32],
    bytes: u64,
}

fn raw_history(voter: &Replica, through: u64) -> Result<History> {
    let mut digest = Sha256::new();
    let mut bytes = 0u64;
    let mut after = 0;
    let source = voter.committed_source();
    while after < through {
        let entries = source.entries_after(after, through)?;
        if entries.is_empty() {
            return Err("missing local committed bytes".into());
        }
        for entry in entries {
            let encoded = entry.write_to_bytes()?;
            let len = encoded.len() as u64;
            digest.update(len.to_le_bytes());
            digest.update(encoded);
            bytes += len;
            after = entry.index;
        }
    }
    Ok(History {
        digest: digest.finalize().into(),
        bytes,
    })
}

fn observe(
    voter: &Replica,
    journal: &Journal,
    head: &Head,
    entries: u64,
) -> Result<(Duration, u64)> {
    let started = Instant::now();
    let confirmation = JournalVerifier::open(voter, journal.clone())?.observe()?;
    let elapsed = started.elapsed();
    assert_eq!(&confirmation.head, head);
    assert_eq!(confirmation.compared_entries, entries);
    Ok((elapsed, confirmation.compared_entries))
}

struct Fixture {
    n: usize,
    ledger: [u8; 32],
    history: History,
    prefix_bytes: u64,
    head: Head,
    segments: u64,
}

struct Timings {
    build: Duration,
    compare: Duration,
    full: (Duration, u64),
    equal: (Duration, u64),
    suffix: (Duration, u64),
}

fn ms(elapsed: Duration) -> f64 {
    elapsed.as_secs_f64() * 1000.
}

fn drill(
    binary: &Path,
    root: &Path,
    journal: &Journal,
    data: &Fixture,
    base_index: u64,
    timings: &Timings,
) -> Result<()> {
    let n = data.n;
    let used_base = base_index != 0;
    let mode = if used_base { "base" } else { "full" };
    let old = cli_config(root, &config(), "gone")?;
    let fence = cli(binary, "fence", &old)?.elapsed;
    let journal_dir = root.join("journal");
    let journal_bytes = dir_bytes(&journal_dir)?;
    let files = journal_dir.join("recovery-images-v1");
    let image_bytes = if used_base { dir_bytes(&files)? } else { 0 };
    let mut restore = Vec::new();
    let mut replayed = Vec::new();
    let mut targets = Vec::new();
    for node in 1..=3 {
        let target = cli_config(root, &new_config(node), &format!("new-{node}"))?;
        let run = cli(binary, "restore", &target)?;
        assert_eq!(run.counter("base_index")?, base_index);
        replayed.push(run.counter("replayed_entries")?);
        assert_eq!(replayed.last(), Some(&(data.head.index - base_index)));
        restore.push(run.elapsed);
        targets.push(target);
    }
    let activate = cli(binary, "activate", &targets[0])?;
    let activation_compared = activate.counter("compared_entries")?;
    assert_eq!(
        (activate.counter("base_index")?, activation_compared),
        (base_index, data.head.index - base_index)
    );
    let total = fence + restore.iter().sum::<Duration>() + activate.elapsed;
    let live = journal.head()?;
    assert_eq!(
        (live.cluster, live.index, live.term, live.fenced),
        (2, data.head.index, data.head.term, false)
    );
    let mut reopens = Vec::new();
    for node in 1..=3 {
        let start = Instant::now();
        let restored = Replica::reopen(&root.join(format!("new-{node}")), new_config(node))?;
        reopens.push(start.elapsed());
        assert_eq!(
            (restored.recovered_from(), restored.applied_index()),
            (live.index, live.index)
        );
        // Compare to both fingerprints captured before destroying the voter disk.
        assert_eq!(fingerprint(&restored, n)?, data.ledger);
        assert_eq!(raw_history(&restored, live.index)?, data.history);
    }
    reopens.sort();
    let build = if used_base { ms(timings.build) } else { 0. };
    println!(
        "{n},{n},{},{mode},{},{},{journal_bytes},{image_bytes},{base_index},{},{},{},{activation_compared},{build:.1},{:.1},{:.1},{:.1},{:.1},{:.1},{:.1},{:.1},{:.1},{:.2},{},{:.2},{},{:.2},{}",
        2 * n + 2,
        live.index,
        data.segments,
        data.history.bytes,
        data.prefix_bytes,
        replayed[0],
        ms(fence),
        ms(restore[0]),
        ms(restore[1]),
        ms(restore[2]),
        ms(activate.elapsed),
        ms(total),
        ms(reopens[1]),
        ms(timings.compare),
        ms(timings.full.0),
        timings.full.1,
        ms(timings.equal.0),
        timings.equal.1,
        ms(timings.suffix.0),
        timings.suffix.1
    );
    Ok(())
}

fn run(binary: &Path, n: usize, base_first: bool) -> Result<()> {
    if !(4..=10_000_000).contains(&n) {
        return Err("account count must be 4..=10000000".into());
    }
    let workspace = tempfile::tempdir()?;
    let full_dir = tempfile::tempdir_in(workspace.path())?;
    let base_dir = tempfile::tempdir_in(workspace.path())?;
    let source_dir = tempfile::tempdir_in(workspace.path())?;
    let mut voter = Replica::bootstrap(source_dir.path(), config())?;
    elect(&mut voter);
    let base_index = dataset(&mut voter, n);
    let final_index = voter.applied_index();
    let expected = fingerprint(&voter, n)?;
    let history = raw_history(&voter, final_index)?;
    let prefix_bytes = raw_history(&voter, base_index)?.bytes;
    let spec = Spec {
        ledger: LEDGER.0,
        asset: "USD".into(),
        scale: 2,
    };
    let full_journal = Journal::init(&full_dir.path().join("journal"), &spec, 1)?;
    let journal = Journal::init(&base_dir.path().join("journal"), &spec, 1)?;
    let mut head = journal.head()?;
    let source = voter.committed_source();
    let mut segments = 0;
    while head.index < base_index {
        let entries =
            source.entries_after(head.index, base_index.min(head.index + SEGMENT_ENTRIES))?;
        let full = full_journal.append(&head, &entries)?;
        head = journal.append(&head, &entries)?;
        assert_eq!(head, full);
        segments += 1;
    }
    let cfg = cli_config(base_dir.path(), &config(), "gone")?;
    let built = cli(binary, "journal-checkpoint", &cfg)?;
    assert_eq!(
        (
            built.counter("base_index")?,
            built.counter("replayed_entries")?
        ),
        (0, base_index)
    );
    let compare = Instant::now();
    journal.visit(&head, |entries| {
        source
            .verify(entries)
            .map_err(|e| JournalError::Invalid(e.to_string()))
    })?;
    let compare = compare.elapsed();
    let tail = source.entries_after(base_index, final_index)?;
    drop(source);
    let full = observe(&voter, &journal, &head, base_index)?;
    drop(voter);
    let restarted = Replica::reopen(source_dir.path(), config())?;
    let equal = observe(&restarted, &journal, &head, 0)?;
    drop(restarted);
    let copied = full_journal.append(&head, &tail)?;
    head = journal.append(&head, &tail)?;
    assert_eq!(head, copied);
    segments += 1;
    let restarted = Replica::reopen(source_dir.path(), config())?;
    let suffix = observe(&restarted, &journal, &head, final_index - base_index)?;
    drop(restarted);
    source_dir.close()?;
    let data = Fixture {
        n,
        head,
        ledger: expected,
        history,
        prefix_bytes,
        segments,
    };
    let timings = Timings {
        build: built.elapsed,
        compare,
        full,
        equal,
        suffix,
    };
    let mut scenarios = [
        (full_dir.path(), &full_journal, 0),
        (base_dir.path(), &journal, base_index),
    ];
    if base_first {
        scenarios.reverse();
    }
    for (root, journal, base_index) in scenarios {
        drill(binary, root, journal, &data, base_index, &timings)?;
    }
    Ok(())
}

fn main() -> Result<()> {
    let exe = std::env::current_exe()?;
    let binary = exe
        .parent()
        .and_then(Path::parent)
        .ok_or("missing Cargo profile path")?
        .join(format!("ledger-node{}", std::env::consts::EXE_SUFFIX));
    if !binary.is_file() {
        return Err("build ledger-node in the same Cargo profile first".into());
    }
    let sizes = std::env::args()
        .skip(1)
        .map(|s| s.parse::<usize>())
        .collect::<std::result::Result<Vec<_>, _>>()?;
    println!(
        "accounts,posted_entries,unique_decisions,mode,raft_entries,data_segments,recovery_directory_logical_bytes,image_logical_bytes,base_index,retained_history_bytes,source_prefix_bytes_at_common_cut,restore_replayed_entries_per_voter,activation_compared_entries,base_build_ms,fence_ms,restore1_ms,restore2_ms,restore3_ms,activate_ms,fence_to_activation_ms,median_reopen_ms,unanchored_forward_compare_ms,full_attach_ms,full_compared_entries,equal_attach_ms,equal_compared_entries,tail_attach_ms,tail_compared_entries"
    );
    let sizes = if sizes.is_empty() {
        vec![4096, 32768]
    } else {
        sizes
    };
    for (index, n) in sizes.into_iter().enumerate() {
        run(&binary, n, index % 2 == 1)?;
    }
    Ok(())
}
