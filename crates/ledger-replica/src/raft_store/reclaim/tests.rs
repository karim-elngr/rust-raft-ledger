use super::*;
use crate::{CacheBudget, Replica, ReplicaConfig, codec::encode_batch};
use ledger_core::{AccountId, Bounds, Command, LedgerId, RequestId, Side};
use ledger_recovery::Head;
use tempfile::TempDir;

struct Fixture {
    dir: TempDir,
    journal: Journal,
    config: ReplicaConfig,
    rows: Vec<Entry>,
    earlier: Head,
    first: Head,
    tip: Head,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let disk = dir.path().join("voter");
        let spec = Spec {
            ledger: 9,
            asset: "USD".into(),
            scale: 2,
        };
        let journal = Journal::init(&dir.path().join("journal"), &spec, 17).unwrap();
        let config = ReplicaConfig {
            cluster: 17,
            node: 1,
            voters: vec![1, 2, 3],
            ledger: LedgerId(9),
            asset: spec.asset.clone(),
            scale: 2,
        };
        drop(Replica::bootstrap(&disk, config.clone()).unwrap());
        let store =
            RaftStore::reopen(&disk.join("raft"), &config.clone().identity().unwrap()).unwrap();
        let mut seed = 5u64;
        let rows: Vec<_> = (1..=20u64)
            .map(|i| {
                let account = AccountId::new(config.ledger, i.into());
                let cmd = Command::CreateAccount {
                    account,
                    code: 7,
                    normal: Side::Credit,
                    bounds: Bounds::default(),
                };
                // Incompressible, legal opaque Raft contexts make the physical SST
                // size change visible independently of the tiny account commands.
                let context: Vec<_> = (0..65536)
                    .map(|_| {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        seed as u8
                    })
                    .collect();
                Entry {
                    index: i,
                    term: 1,
                    data: encode_batch(config.ledger, &[(RequestId(i.into()), cmd)])
                        .unwrap()
                        .into(),
                    context: context.into(),
                    ..Default::default()
                }
            })
            .collect();
        store
            .persist_ready(
                &rows,
                Some(&HardState {
                    term: 1,
                    vote: 1,
                    commit: 20,
                    ..Default::default()
                }),
            )
            .unwrap();
        drop(store);
        let earlier = journal
            .append(&journal.head().unwrap(), &rows[..7])
            .unwrap();
        let first = journal.append(&earlier, &rows[7..15]).unwrap();
        Replica::create_recovery_base(&journal, &first, CacheBudget::default()).unwrap();
        let tip = journal.append(&first, &rows[15..]).unwrap();
        let voter = Replica::reopen(&disk, config.clone()).unwrap();
        voter.begin_checkpoint().unwrap().unwrap().finish().unwrap();
        drop(voter);
        Self {
            dir,
            journal,
            config,
            rows,
            earlier,
            first,
            tip,
        }
    }
    fn disk(&self) -> PathBuf {
        self.dir.path().join("voter")
    }
    fn voter(&self) -> Replica {
        Replica::reopen(&self.disk(), self.config.clone()).unwrap()
    }
    fn raw(&self) -> RaftStore {
        RaftStore::reopen(
            &self.disk().join("raft"),
            &self.config.clone().identity().unwrap(),
        )
        .unwrap()
    }
}

#[test]
fn physical_bytes_shrink_and_a_replacement_base_preserves_exact_reads_repair_and_votes() {
    let f = Fixture::new();
    let sst_bytes = || {
        std::fs::read_dir(f.disk().join("raft"))
            .unwrap()
            .map(Result::unwrap)
            .filter(|d| d.path().extension().is_some_and(|s| s == "sst"))
            .map(|d| d.metadata().unwrap().len())
            .sum::<u64>()
    };
    f.raw().db.flush().unwrap();
    let before_on_disk = sst_bytes();
    let voter = f.voter();
    let stats = voter.reclaim_log(&f.journal).unwrap();
    assert_eq!((stats.through, stats.reclaimed_entries), (15, 14));
    assert!(stats.reclaimed_bytes > 896_000);
    assert!(
        stats.live_sst_bytes_before > stats.live_sst_bytes_after + 800_000,
        "{stats:?}"
    );
    drop(voter);
    let after_on_disk = sst_bytes();
    assert!(before_on_disk > after_on_disk + 800_000);
    eprintln!(
        "reclamation measured {stats:?}, on-disk .sst bytes {before_on_disk} -> {after_on_disk}"
    );
    let raw = f.raw();
    assert!(raw.db.get(entry_key(1)).unwrap().is_none());
    assert!(raw.db.get(entry_key(14)).unwrap().is_none());
    assert!(raw.db.get(entry_key(15)).unwrap().is_some());
    assert_eq!(raw.first_index().unwrap(), 16);
    assert_eq!(raw.term(15).unwrap(), 1);
    assert_eq!(raw.initial_state().unwrap().hard_state.vote, 1);
    assert_eq!(raw.retained_entries(2, 20, None).unwrap(), f.rows[1..19]);
    drop(raw);
    // The newest repository pointer changes while this voter is offline. Its
    // pinned exact predecessor remains readable and fully rebuildable.
    Replica::create_recovery_base(&f.journal, &f.tip, CacheBudget::default()).unwrap();
    let voter = f.voter();
    voter.committed_source().verify(&f.rows).unwrap();
    let second = voter.reclaim_log(&f.journal).unwrap();
    assert_eq!((second.through, second.reclaimed_entries), (20, 5));
    assert_eq!(voter.reclaim_log(&f.journal).unwrap().reclaimed_entries, 0);
    drop(voter);
    f.raw().db.put(CHECKPOINT, b"torn pointer").unwrap();
    assert!(Replica::reopen(&f.disk(), f.config.clone()).is_err());
    let rebuilt =
        Replica::rebuild_from_log(&f.disk(), f.config.clone(), CacheBudget::default()).unwrap();
    assert_eq!(rebuilt.applied_index(), 20);
    assert_eq!(
        rebuilt
            .account(AccountId::new(f.config.ledger, 1))
            .unwrap()
            .unwrap()
            .code(),
        7
    );
    rebuilt.committed_source().verify(&f.rows).unwrap();
    drop(rebuilt);
    let offline = f.dir.path().join("after-repair-offline");
    std::fs::rename(f.journal.directory(), &offline).unwrap();
    assert_eq!(f.voter().recovered_from(), 20);
    std::fs::rename(offline, f.journal.directory()).unwrap();
}

#[test]
fn crashes_on_both_sides_of_the_pointer_switch_leave_local_restart_independent() {
    let f = Fixture::new();
    let voter = f.voter();
    fault::NEXT.set(Some(fault::Point::BeforeSwitch));
    assert!(voter.reclaim_log(&f.journal).is_err());
    drop(voter);
    let raw = f.raw();
    assert_eq!(raw.reclaimed_through().unwrap(), 0);
    assert!(raw.db.get(entry_key(1)).unwrap().is_some());
    drop(raw);
    let voter = f.voter();
    fault::NEXT.set(Some(fault::Point::AfterSwitch));
    assert!(voter.reclaim_log(&f.journal).is_err());
    drop(voter);
    let mount = f.journal.directory();
    let offline = f.dir.path().join("offline");
    std::fs::rename(mount, &offline).unwrap();
    let voter = f.voter();
    assert_eq!(voter.recovered_from(), 20);
    let source = voter.committed_source();
    assert!(source.entries_after(0, 1).is_err());
    assert_eq!(source.journal_anchor().unwrap(), Some(f.first.clone()));
    assert_eq!(source.term(15).unwrap(), f.first.term);
    assert_eq!(source.entries_after(14, 20).unwrap(), f.rows[14..]);
    drop(source);
    drop(voter);
    assert!(
        Replica::rebuild_from_log(&f.disk(), f.config.clone(), CacheBudget::default()).is_err()
    );
    std::fs::rename(offline, mount).unwrap();
    let voter = f.voter();
    assert_eq!(voter.reclaim_log(&f.journal).unwrap().through, 15);
    voter.committed_source().verify(&f.rows).unwrap();
}

#[test]
fn reclaim_advances_an_authenticated_older_anchor_and_protected_restart_only_reads_tail() {
    let f = Fixture::new();
    let voter = f.voter();
    let source = voter.committed_source();
    source.save_journal_anchor(None, &f.earlier).unwrap();
    voter.reclaim_log(&f.journal).unwrap();
    drop(source);
    drop(voter);
    let images = f.journal.directory().join("recovery-images-v1");
    let offline = f.dir.path().join("images-offline");
    std::fs::rename(&images, &offline).unwrap();
    let voter = f.voter();
    let mut verifier = crate::JournalVerifier::open(&voter, f.journal.clone()).unwrap();
    let evidence = verifier.observe().unwrap();
    assert_eq!(evidence.head, f.tip);
    assert_eq!(evidence.compared_entries, f.tip.index - f.first.index);
    assert!(voter.committed_source().entries_after(0, 1).is_err());
    std::fs::rename(offline, images).unwrap();
}

#[test]
fn reclaim_retains_a_newer_anchor_and_refuses_a_saved_fork() {
    for wrong_fork in [false, true] {
        let f = Fixture::new();
        let voter = f.voter();
        let mut saved = f.tip.clone();
        if wrong_fork {
            saved.digest[0] ^= 1;
        }
        let source = voter.committed_source();
        source.save_journal_anchor(None, &saved).unwrap();
        assert_eq!(voter.reclaim_log(&f.journal).is_err(), wrong_fork);
        assert_eq!(source.journal_anchor().unwrap(), Some(saved));
        drop(source);
        drop(voter);
        let raw = f.raw();
        assert_eq!(
            raw.reclaimed_through().unwrap(),
            if wrong_fork { 0 } else { f.first.index }
        );
    }
}

#[test]
fn partial_adopted_ranges_are_physically_pruned_and_their_matching_entry_remains_local() {
    let f = Fixture::new();
    let seal = f.raw().checkpoint().unwrap().unwrap();
    let path = f.dir.path().join("received-raft");
    let identity = f.config.clone().identity().unwrap();
    let store = RaftStore::bootstrap(&path, &identity, &f.config.voters).unwrap();
    let hard = HardState {
        term: 1,
        vote: 1,
        commit: 2,
        ..Default::default()
    };
    store.persist_ready(&f.rows[..2], Some(&hard)).unwrap();
    let mut offer = Snapshot {
        data: seal.snapshot_token().into(),
        ..Default::default()
    };
    offer.mut_metadata().index = seal.index;
    offer.mut_metadata().term = seal.term;
    offer.mut_metadata().set_conf_state(store.conf.clone());
    let stage = store.snapshot_history().begin(seal).unwrap();
    stage.append(&f.rows[2..]).unwrap();
    stage.finish().unwrap();
    store
        .persist_snapshot_ready(&offer, seal, &stage, &[], None)
        .unwrap();
    drop(stage);
    assert!(
        store
            .db
            .get(archive::archive_key(&seal, 3))
            .unwrap()
            .is_some()
    );
    let stats = store.reclaim(&f.journal, 17, seal).unwrap();
    assert_eq!(stats.reclaimed_entries, 14);
    assert!(
        store
            .db
            .get(archive::archive_key(&seal, 14))
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .db
            .get(archive::archive_key(&seal, 15))
            .unwrap()
            .is_some()
    );
    assert!(store.db.get(archive::range_key(3)).unwrap().is_none());
    assert!(store.db.get(archive::range_key(15)).unwrap().is_some());
    let history = store.snapshot_history();
    assert_eq!(history.entries_after(seal, 0).unwrap(), f.rows);
    let source = crate::CommittedSource(store.clone(), 17);
    source.save_journal_anchor(None, &f.first).unwrap();
    assert_eq!(source.journal_anchor().unwrap(), Some(f.first));
}

#[test]
fn raw_mismatch_preserves_local_rows_and_prior_floor() {
    let f = Fixture::new();
    let raw = f.raw();
    // Same index and term, independently decodable bytes, different command context.
    let mut mismatch = f.rows[2].clone();
    mismatch.context = vec![4; 100].into();
    raw.db
        .put(entry_key(3), mismatch.write_to_bytes().unwrap())
        .unwrap();
    drop(raw);
    assert!(f.voter().reclaim_log(&f.journal).is_err());
    let raw = f.raw();
    assert_eq!(raw.reclaimed_through().unwrap(), 0);
    assert!(raw.db.get(entry_key(1)).unwrap().is_some());
    raw.db
        .put(entry_key(3), f.rows[2].write_to_bytes().unwrap())
        .unwrap();
    drop(raw);
    // Corrupt an archive file after publication. A fresh image hash must fail
    // even when the journal itself is healthy and its independent head agrees.
    let base = f.journal.recovery_base().unwrap().unwrap();
    let image = open_image(&f.journal, &base).unwrap();
    let raft_dir = image.verified_raft_dir().unwrap();
    drop(image);
    let file = std::fs::read_dir(raft_dir)
        .unwrap()
        .map(Result::unwrap)
        .find(|d| d.path().extension().is_some_and(|s| s == "sst"))
        .unwrap();
    use std::io::Write;
    std::fs::OpenOptions::new()
        .write(true)
        .open(file.path())
        .unwrap()
        .write_all(b"wrong")
        .unwrap();
    assert!(f.voter().reclaim_log(&f.journal).is_err());
    let raw = f.raw();
    assert_eq!(raw.reclaimed_through().unwrap(), 0);
    assert_eq!(raw.retained_entries(1, 21, None).unwrap(), f.rows);
}
