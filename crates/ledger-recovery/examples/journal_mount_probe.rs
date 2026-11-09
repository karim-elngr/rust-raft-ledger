//! Build on each host with `cargo build --locked -p ledger-recovery --example journal_mount_probe`.
//! On host A run `journal_mount_probe holder /existing/shared/scratch`. On host B
//! run `journal_mount_probe observer /its/path/to/the/same/scratch RUN_ID` using
//! the ID printed by A. Use the deployment's mount options and service account.
//! Each handshake waits up to five minutes. Record both exit statuses and logs.
//! Both processes must pass. Repeat with the hosts reversed to test both lock
//! directions. The printed disposable child can be removed after both exit.

use std::error::Error;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use ledger_recovery::{Head, Journal, JournalError, Spec};
use raft::eraftpb::Entry;

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const LAST: u64 = 32;
const USAGE: &str =
    "usage: journal_mount_probe holder EXISTING_SCRATCH_DIR | observer EXISTING_SCRATCH_DIR RUN_ID";

fn scratch(path: &Path) -> Result<PathBuf> {
    let root = fs::canonicalize(path)?;
    if !root.is_dir() {
        return Err("scratch path must be an existing directory".into());
    }
    for ancestor in root.ancestors() {
        if ancestor.join("LOCK").try_exists()? && ancestor.join("HEAD").try_exists()? {
            return Err("scratch directory is inside a recovery journal".into());
        }
    }
    Ok(root)
}

fn spec(id: u128) -> Spec {
    Spec {
        ledger: id,
        asset: "MOUNT-PROBE".into(),
        scale: 0,
    }
}

fn entry(id: u128, index: u64) -> Entry {
    Entry {
        index,
        term: 1,
        data: format!("journal mount probe {id:032x} entry {index}")
            .into_bytes()
            .into(),
        ..Default::default()
    }
}

fn verified(journal: &Journal, id: u128, index: u64) -> Result<Head> {
    let head = journal.head()?;
    if (head.cluster, head.index, head.fenced) != (id, index, false) {
        return Err(
            format!("unexpected published head at expected index {index}: {head:?}").into(),
        );
    }
    let mut seen = 0;
    journal.visit(&head, |entries| {
        for e in entries {
            seen += 1;
            if *e != entry(id, seen) {
                return Err(JournalError::Invalid(format!("unexpected entry at {seen}")));
            }
        }
        Ok(())
    })?;
    if seen != index {
        return Err(format!("expected {index} entries, verified {seen}").into());
    }
    Ok(head)
}

fn signal(run: &Path, name: &str) -> Result<()> {
    File::create_new(run.join(name))?.sync_all()?;
    File::open(run)?.sync_all()?;
    Ok(())
}

fn wait(run: &Path, name: &str, mut sample: impl FnMut() -> Result<()>) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(300);
    let marker = run.join(name);
    loop {
        sample()?;
        if marker.try_exists()? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for {name} in {}", run.display()).into());
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn holder(root: &Path) -> Result<()> {
    let mut nonce = [0; 16];
    File::open("/dev/urandom")?.read_exact(&mut nonce)?;
    let id = u128::from_be_bytes(nonce);
    let run = root.join(format!("ledger-mount-probe-{id:032x}"));
    fs::create_dir(&run)?;
    File::open(root)?.sync_all()?;
    let path = run.join("journal");
    let journal = Journal::init(&path, &spec(id), id)?;
    let mut tip = journal.head()?;
    let lock = File::options()
        .read(true)
        .write(true)
        .open(path.join("LOCK"))?;
    lock.try_lock()?;
    println!("holder ready run_id={id:032x} path={}", run.display());
    wait(&run, "contended", || Ok(()))?;
    drop(lock);
    for index in 1..=LAST {
        tip = journal.append(&tip, &[entry(id, index)])?;
        if index == LAST / 2 {
            signal(&run, "midpoint")?;
            wait(&run, "midpoint-verified", || Ok(()))?;
        }
    }
    signal(&run, "published")?;
    wait(&run, "replied", || Ok(()))?;
    let head = verified(&journal, id, LAST + 1)?;
    println!(
        "holder passed observer publication and full replay index={}",
        head.index
    );
    Ok(())
}

fn observer(root: &Path, token: &str) -> Result<()> {
    let id = u128::from_str_radix(token, 16)?;
    if id == 0 || token != format!("{id:032x}") {
        return Err("run ID must be 32 lowercase hexadecimal digits from the holder".into());
    }
    let run = root.join(format!("ledger-mount-probe-{token}"));
    let path = run.join("journal");
    for dir in [&run, &path] {
        if !fs::symlink_metadata(dir)?.file_type().is_dir() {
            return Err("expected the holder's disposable directory".into());
        }
    }
    let spec = spec(id);
    let journal = Journal::open(&path, &spec)?;
    if journal.observed_generation()? != (id, false) {
        return Err("journal does not belong to this run".into());
    }
    signal(&run, "observer-claimed")?;
    match journal.head() {
        Err(JournalError::Io(e))
            if e.kind() == io::ErrorKind::TimedOut
                && e.to_string().contains("recovery journal lock timed out") => {}
        other => {
            return Err(
                format!("expected timeout on the holder's journal lock, got {other:?}").into(),
            );
        }
    }
    println!("observer confirmed exclusive lock run_id={token}");
    signal(&run, "contended")?;
    let mut samples = 0;
    for (marker, index) in [("midpoint", LAST / 2), ("published", LAST)] {
        wait(&run, marker, || {
            // Journal::open checks the unlocked HEAD checksum and its immutable segment.
            Journal::open(&path, &spec)?;
            samples += 1;
            Ok(())
        })?;
        let head = verified(&journal, id, index)?;
        println!("observer verified published index={index} full_replay={index}");
        if index == LAST {
            journal.append(&head, &[entry(id, LAST + 1)])?;
        } else {
            signal(&run, "midpoint-verified")?;
        }
    }
    signal(&run, "replied")?;
    println!(
        "observer passed unlocked_head_and_segment_samples={samples} reply_index={}",
        LAST + 1
    );
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    match args.as_slice() {
        [cmd, root] if cmd == "holder" => holder(&scratch(root.as_ref())?),
        [cmd, root, token] if cmd == "observer" => observer(
            &scratch(root.as_ref())?,
            token.to_str().ok_or("expected hexadecimal run ID")?,
        ),
        _ => Err(USAGE.into()),
    }
}
