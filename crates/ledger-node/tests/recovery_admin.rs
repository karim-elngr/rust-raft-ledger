use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use ledger_recovery::{Journal, Spec};
use raft::eraftpb::Entry;
use tempfile::tempdir;

fn config(dir: &Path, cluster: u128, node: u64, data: &str, remote: &str) -> PathBuf {
    let path = dir.join(format!("{data}-{remote}-{cluster}.toml"));
    let text = format!(
        r#"
cluster_id = "{cluster}"
ledger_id = "5"
node_id = {node}
asset = "USD"
scale = 2
listen = "127.0.0.1:7101"
data_dir = "{data}"
recovery_dir = "{remote}"
[tls]
ca_pem = "ca.pem"
cert_pem = "node.pem"
key_pem = "node.key"
[[peers]]
node_id = {}
endpoint = "https://127.0.0.1:7102"
domain = "peer-a.test"
cert_der = "a.der"
[[peers]]
node_id = {}
endpoint = "https://127.0.0.1:7103"
domain = "peer-b.test"
cert_der = "b.der"
"#,
        node + 1,
        node + 2
    );
    fs::write(&path, text).unwrap();
    path
}

fn credential_files(dir: &Path) {
    for name in ["ca.pem", "node.pem", "node.key", "a.der", "b.der"] {
        fs::write(dir.join(name), name).unwrap();
    }
}

fn invoke(verb: &str, path: &Path, success: bool) -> String {
    let result = Command::new(env!("CARGO_BIN_EXE_ledger-node"))
        .args([verb.as_ref(), path.as_os_str()])
        .output()
        .unwrap();
    assert_eq!(
        result.status.success(),
        success,
        "{verb} {path:?}: {}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    String::from_utf8(if success {
        result.stdout
    } else {
        result.stderr
    })
    .unwrap()
}

fn seed(dir: &Path, tag: &[u8]) -> Journal {
    let journal = Journal::open(
        dir,
        &Spec {
            ledger: 5,
            asset: "USD".into(),
            scale: 2,
        },
    )
    .unwrap();
    let prefix = [1, 2].map(|index| Entry {
        index,
        term: 4,
        context: tag.to_vec().into(),
        ..Entry::default()
    });
    journal.append(&journal.head().unwrap(), &prefix).unwrap();
    journal
}

#[cfg(unix)]
#[test]
fn journal_init_rejects_a_redirect_into_replica_storage_before_writing() {
    use std::os::unix::fs::symlink;
    let root = tempdir().unwrap();
    let data = root.path().join("data");
    fs::create_dir(&data).unwrap();
    symlink(&data, root.path().join("remote")).unwrap();
    let cfg = config(root.path(), 7, 1, "data", "remote");
    assert!(invoke("journal-init", &cfg, false).contains("separate directory trees"));
    assert!(fs::read_dir(&data).unwrap().next().is_none());

    let separate = root.path().join("separate");
    fs::create_dir(&separate).unwrap();
    fs::remove_file(root.path().join("remote")).unwrap();
    symlink(&separate, root.path().join("remote")).unwrap();
    invoke("journal-init", &cfg, true);
    assert!(separate.join("HEAD").is_file());
}

#[cfg(unix)]
#[test]
fn legacy_journal_alias_under_the_data_dir_can_point_to_a_separate_tree() {
    use std::os::unix::fs::symlink;
    let root = tempdir().unwrap();
    let separate = root.path().join("separate");
    fs::create_dir(&separate).unwrap();
    let cfg = config(root.path(), 7, 1, "voter", "journal");
    let raw = fs::read_to_string(&cfg).unwrap();
    fs::write(
        &cfg,
        raw.replace(
            "recovery_dir = \"journal\"",
            "recovery_dir = \"voter/remote\"",
        ),
    )
    .unwrap();
    fs::create_dir(root.path().join("voter")).unwrap();
    symlink(&separate, root.path().join("voter/remote")).unwrap();
    invoke("journal-init", &cfg, true);
    assert!(separate.join("HEAD").is_file());
    invoke("journal-head", &cfg, true);
}

#[test]
fn administration_refuses_wrong_generations_and_activates_only_the_exact_imported_log() {
    let temp = tempdir().unwrap();
    let dir = temp.path();
    credential_files(dir);
    let original = config(dir, 7, 1, "old", "journal");
    let next = config(dir, 3, 11, "replacement", "journal");
    invoke("init", &original, false);
    invoke("journal-head", &original, false);
    assert!(!dir.join("old").exists());
    let genesis = invoke("journal-init", &original, true);
    assert_eq!(invoke("journal-head", &original, true), genesis);
    invoke("journal-init", &original, false);
    invoke("run", &original, false);
    assert!(!dir.join("old").exists());
    invoke("init", &original, true);
    invoke("init", &original, false);
    for name in ["ca.pem", "node.pem", "node.key", "a.der", "b.der"] {
        fs::remove_file(dir.join(name)).unwrap();
    }

    let journal = seed(&dir.join("journal"), b"source history");
    let active = journal.head().unwrap();
    assert!(
        invoke("journal-head", &original, true).contains("cluster=7 index=2 term=4 fenced=false")
    );
    invoke("fence", &next, false);
    invoke("restore", &next, false);
    assert_eq!(journal.head().unwrap(), active);
    invoke("fence", &original, true);
    let fenced = journal.head().unwrap();
    assert!(fenced.fenced);
    invoke("activate", &original, false);
    invoke("activate", &next, false);

    // A different pair of no-ops has the same last index and term. Only the
    // per-entry bytes distinguish this impostor from the authoritative prefix.
    let other_source = config(dir, 7, 1, "unused", "other");
    invoke("journal-init", &other_source, true);
    seed(&dir.join("other"), b"different history");
    invoke("fence", &other_source, true);
    let other_voter = config(dir, 3, 11, "divergent", "other");
    invoke("restore", &other_voter, true);
    let impostor = config(dir, 3, 11, "divergent", "journal");
    invoke("activate", &impostor, false);
    assert_eq!(journal.head().unwrap(), fenced);

    invoke("restore", &next, true);
    invoke("restore", &next, false);
    let activated = invoke("activate", &next, true);
    assert!(activated.contains("cluster=3 index=2 term=4 fenced=false"));
    assert_eq!(invoke("journal-head", &next, true), activated);
    invoke("fence", &original, false);
    assert_eq!(invoke("journal-head", &next, true), activated);
}

#[test]
fn journal_scrub_reports_the_verified_active_or_fenced_cut_and_detects_interior_damage() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), 7, 1, "missing-voter", "journal");
    let path = dir.path().join("journal");
    invoke("journal-init", &cfg, true);
    let journal = seed(&path, b"first batch");
    let first = journal.head().unwrap();
    journal
        .append(
            &first,
            &[Entry {
                index: 3,
                term: 4,
                ..Default::default()
            }],
        )
        .unwrap();
    let active = invoke("journal-head", &cfg, true);
    assert_eq!(invoke("journal-scrub", &cfg, true), active);
    let fenced = invoke("fence", &cfg, true);
    assert_eq!(invoke("journal-scrub", &cfg, true), fenced);

    use std::fmt::Write;
    let mut name = String::from("segment-");
    for byte in first.digest {
        write!(name, "{byte:02x}").unwrap();
    }
    fs::write(path.join(name), b"damaged historical group").unwrap();
    assert_eq!(invoke("journal-head", &cfg, true), fenced);
    assert!(invoke("journal-scrub", &cfg, false).contains("checksum mismatch"));
    assert!(!dir.path().join("missing-voter").exists());
}

#[test]
fn recovery_validates_storage_layout_while_voter_initialization_and_run_require_credentials() {
    let temp = tempdir().unwrap();
    credential_files(temp.path());
    let same = config(temp.path(), 7, 1, "shared", "shared");
    assert!(invoke("journal-init", &same, false).contains("separate directory trees"));
    assert!(!temp.path().join("shared").exists());
    fs::remove_file(temp.path().join("node.key")).unwrap();
    let separate = config(temp.path(), 7, 1, "voter", "external");
    invoke("journal-init", &separate, true);
    for action in ["init", "run"] {
        assert!(invoke(action, &separate, false).contains("tls.key_pem"));
    }
    assert!(!temp.path().join("voter").exists());
}

#[test]
fn recovery_checkpoint_builds_live_from_the_journal_and_advances_by_the_tail() {
    let dir = tempdir().unwrap();
    let original = config(dir.path(), 7, 1, "voter", "journal");
    let next = config(dir.path(), 8, 11, "replacement", "journal");
    invoke("journal-init", &original, true);
    invoke("journal-checkpoint", &original, false); // Empty journal.
    let journal = seed(&dir.path().join("journal"), b"same prefix");
    let source = ledger_replica::Replica::bootstrap(
        &dir.path().join("voter"),
        ledger_replica::ReplicaConfig {
            cluster: 7,
            node: 1,
            voters: vec![1, 2, 3],
            ledger: ledger_core::LedgerId(5),
            asset: "USD".into(),
            scale: 2,
        },
    )
    .unwrap();
    // This voter owns both its RocksDB locks. Its empty ledger disagrees with
    // the independent journal and no TLS credentials were ever created.
    assert_eq!(source.applied_index(), 0);
    invoke("journal-checkpoint", &next, false);
    let sampled = invoke("journal-head", &original, true);
    assert!(
        invoke("journal-checkpoint", &original, true).contains("base_index=0 replayed_entries=2")
    );
    assert_eq!(invoke("journal-head", &original, true), sampled);
    assert!(
        invoke("journal-checkpoint", &original, true).contains("base_index=2 replayed_entries=0")
    );
    journal
        .append(
            &journal.head().unwrap(),
            &[Entry {
                index: 3,
                term: 4,
                ..Default::default()
            }],
        )
        .unwrap();
    assert!(
        invoke("journal-checkpoint", &original, true).contains("base_index=2 replayed_entries=1")
    );
    journal
        .append(
            &journal.head().unwrap(),
            &[Entry {
                index: 4,
                term: 5,
                ..Default::default()
            }],
        )
        .unwrap();
    let fenced = invoke("fence", &original, true);
    invoke("journal-checkpoint", &original, false); // A build requires a live cut.
    assert_eq!(invoke("journal-head", &original, true), fenced);
    drop(source);
    fs::remove_dir_all(dir.path().join("voter")).unwrap();
    assert!(invoke("restore", &next, true).contains("base_index=3 replayed_entries=1"));
    let active = invoke("activate", &next, true);
    assert_eq!(active, invoke("journal-head", &next, true));
}

#[test]
fn full_checkpoint_repair_replaces_a_damaged_image_from_the_complete_journal() {
    let temp = tempdir().unwrap();
    let source = config(temp.path(), 7, 1, "old", "journal");
    let new = config(temp.path(), 8, 11, "new", "journal");
    invoke("journal-init", &source, true);
    let journal = seed(&temp.path().join("journal"), b"original");
    invoke("journal-checkpoint", &source, true);
    let image = fs::read_dir(journal.directory().join("recovery-images-v1"))
        .unwrap()
        .map(Result::unwrap)
        .find(|e| e.path().join("manifest").is_file())
        .unwrap()
        .path();
    fs::write(image.join("manifest"), b"damaged").unwrap();
    invoke("journal-checkpoint", &source, false);
    let prior = journal.head().unwrap();
    assert!(
        invoke("journal-checkpoint-from-journal", &source, true)
            .contains("base_index=0 replayed_entries=2")
    );
    assert_eq!(journal.head().unwrap(), prior);
    assert_eq!(fs::read(image.join("manifest")).unwrap(), b"damaged");
    assert!(
        invoke("journal-checkpoint", &source, true).contains("base_index=2 replayed_entries=0")
    );
    invoke("fence", &source, true);
    assert!(invoke("restore", &new, true).contains("base_index=2 replayed_entries=0"));
    invoke("activate", &new, true);
}

#[test]
fn damaged_or_divergent_recovery_base_keeps_the_fence_and_destination_intact() {
    let temp = tempdir().unwrap();
    let root = temp.path();
    let source = config(root, 7, 1, "gone", "journal");
    let other = config(root, 7, 1, "gone", "alternate");
    let voter = config(root, 8, 11, "restored", "journal");
    let fresh = config(root, 8, 11, "fresh", "journal");
    for (cfg, path, tag) in [
        (&source, "journal", b"source".as_slice()),
        (&other, "alternate", b"other".as_slice()),
    ] {
        invoke("journal-init", cfg, true);
        let j = seed(&root.join(path), tag);
        invoke("journal-checkpoint", cfg, true);
        j.append(
            &j.head().unwrap(),
            &[Entry {
                index: 3,
                term: 5,
                context: b"identical tail".to_vec().into(),
                ..Default::default()
            }],
        )
        .unwrap();
    }
    let fenced = invoke("fence", &source, true);
    invoke("restore", &voter, true); // This voter was seeded from the authentic original.
    let journal = root.join("journal");
    let generation = fs::read_dir(journal.join("recovery-images-v1"))
        .unwrap()
        .map(Result::unwrap)
        .find(|entry| entry.path().join("manifest").is_file())
        .expect("published image")
        .path();
    let manifest = generation.join("manifest");
    let saved = fs::read(&manifest).unwrap();
    fs::write(&manifest, b"torn manifest").unwrap();
    invoke("restore", &fresh, false);
    assert!(!root.join("fresh").exists());
    fs::write(&manifest, saved).unwrap();

    // Swap every part of the base, including its checksummed binding. Both
    // journals have the same ledger, source generation, term and final index.
    fn move_base(from: &Path, to: &Path) {
        for entry in fs::read_dir(from).unwrap().map(Result::unwrap) {
            let name = entry.file_name();
            if name == "BASE"
                || name == "recovery-images-v1"
                || name.to_string_lossy().starts_with("base-")
            {
                fs::rename(entry.path(), to.join(name)).unwrap();
            }
        }
    }
    let parked = tempfile::tempdir_in(root).unwrap();
    move_base(&journal, parked.path());
    move_base(&root.join("alternate"), &journal);
    invoke("restore", &fresh, false);
    invoke("activate", &voter, false);
    assert!(!root.join("fresh").exists());
    assert_eq!(invoke("journal-head", &source, true), fenced);
    move_base(&journal, &root.join("alternate"));
    move_base(parked.path(), &journal);
    invoke("restore", &fresh, true);
    invoke("activate", &voter, true);
}

#[test]
fn stopped_voter_reclaims_only_a_sealed_independently_recoverable_prefix() {
    use ledger_replica::{Replica, ReplicaConfig};
    use raft::eraftpb::{Message, MessageType};
    let temp = tempdir().unwrap();
    let root = temp.path();
    let cfg = config(root, 7, 1, "voter", "journal");
    invoke("journal-init", &cfg, true);
    let identity = ReplicaConfig {
        cluster: 7,
        node: 1,
        voters: vec![1, 2, 3],
        ledger: ledger_core::LedgerId(5),
        asset: "USD".into(),
        scale: 2,
    };
    let disk = root.join("voter");
    let mut replica = Replica::bootstrap(&disk, identity.clone()).unwrap();
    let records: Vec<_> = (1..=20)
        .map(|index| Entry {
            index,
            term: 2,
            context: vec![index as u8; 256].into(),
            ..Default::default()
        })
        .collect();
    let mut append = Message {
        from: 2,
        to: 1,
        term: 2,
        commit: 20,
        ..Default::default()
    };
    append.set_msg_type(MessageType::MsgAppend);
    append.entries.extend(records.iter().cloned());
    replica.step(append).unwrap();
    replica.drain_ready().unwrap();
    let external = root.join("journal");
    let spec = Spec {
        ledger: 5,
        asset: "USD".into(),
        scale: 2,
    };
    let journal = Journal::open(&external, &spec).unwrap();
    let base = journal
        .append(&journal.head().unwrap(), &records[..12])
        .unwrap();
    invoke("reclaim", &cfg, false); // Voter owns the DB lock.
    drop(replica);
    invoke("reclaim", &cfg, false); // No checkpoint or independent base.
    invoke("journal-checkpoint", &cfg, true);
    invoke("reclaim", &cfg, false); // Still lacking a local sealed checkpoint.
    invoke("checkpoint", &cfg, true);
    let tip = journal.append(&base, &records[12..]).unwrap();
    let reclaimed = invoke("reclaim", &cfg, true);
    let count = |name: &str| {
        reclaimed
            .split_whitespace()
            .find_map(|f| f.strip_prefix(name))
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    assert_eq!(count("through="), 12);
    assert!((1..=12).contains(&count("reclaimed_entries=")));
    assert!(count("reclaimed_bytes=") > 0);
    assert_eq!(journal.head().unwrap(), tip);
    let moved = root.join("unmounted");
    fs::rename(&external, &moved).unwrap();
    let reopened = Replica::reopen(&disk, identity.clone()).unwrap();
    assert_eq!(reopened.applied_index(), 20);
    drop(reopened);
    fs::rename(moved, &external).unwrap();
    let repaired = Replica::rebuild_from_log(&disk, identity, Default::default()).unwrap();
    repaired.committed_source().verify(&records).unwrap();
    drop(repaired);
    let replacement = config(root, 8, 11, "fresh", "journal");
    invoke("fence", &cfg, true);
    assert!(invoke("restore", &replacement, true).contains("base_index=12 replayed_entries=8"));
    invoke("activate", &replacement, true);
}

#[test]
fn explicitly_restoring_from_journal_survives_image_damage_but_requires_every_command() {
    use ledger_core::{AccountId, Bounds, Command, LedgerId, RequestId, Side};
    use ledger_replica::{Replica, ReplicaConfig};

    let temp = tempdir().unwrap();
    let root = temp.path();
    let source = config(root, 7, 1, "old", "journal");
    let next = config(root, 8, 11, "new", "journal");
    let rescue = config(root, 8, 12, "rescue", "journal");
    let journal_path = root.join("journal");
    invoke("journal-init", &source, true);
    let journal = Journal::open(
        &journal_path,
        &Spec {
            ledger: 5,
            asset: "USD".into(),
            scale: 2,
        },
    )
    .unwrap();
    let cfg = ReplicaConfig {
        cluster: 7,
        node: 1,
        voters: vec![1, 2, 3],
        ledger: LedgerId(5),
        asset: "USD".into(),
        scale: 2,
    };
    let mut leader = Replica::bootstrap(&root.join("old"), cfg.clone()).unwrap();
    let mut peer = Replica::bootstrap(
        &root.join("peer"),
        ReplicaConfig {
            node: 2,
            ..cfg.clone()
        },
    )
    .unwrap();
    leader.campaign().unwrap();
    let drive = |leader: &mut Replica, peer: &mut Replica, target: u64| {
        for _ in 0..32 {
            for message in leader
                .drain_ready()
                .unwrap()
                .into_iter()
                .filter(|m| m.to == 2)
            {
                peer.step(message).unwrap();
            }
            for message in peer
                .drain_ready()
                .unwrap()
                .into_iter()
                .filter(|m| m.to == 1)
            {
                leader.step(message).unwrap();
            }
            if leader.applied_index() >= target {
                return;
            }
        }
        panic!("quorum did not commit {target}");
    };
    drive(&mut leader, &mut peer, 1);
    let request = RequestId(41);
    let create = Command::CreateAccount {
        account: AccountId::new(cfg.ledger, 17),
        code: 8,
        normal: Side::Credit,
        bounds: Bounds::default(),
    };
    leader.propose(&[(request, create.clone())]).unwrap();
    drive(&mut leader, &mut peer, 2);
    let receipt = leader.outcome_for(request, &create).unwrap().unwrap();
    assert!(receipt.is_ok());
    let prefix = leader.committed_source().entries_after(0, 2).unwrap();
    let base = journal.append(&journal.head().unwrap(), &prefix).unwrap();
    invoke("journal-checkpoint", &source, true);
    let tail = Entry {
        index: 3,
        term: base.term,
        ..Default::default()
    };
    journal.append(&base, std::slice::from_ref(&tail)).unwrap();
    drop((leader, peer));
    for dir in ["old", "peer"] {
        fs::remove_dir_all(root.join(dir)).unwrap();
    }
    let fenced = invoke("fence", &source, true);

    let manifest = fs::read_dir(journal_path.join("recovery-images-v1"))
        .unwrap()
        .map(Result::unwrap)
        .map(|e| e.path().join("manifest"))
        .find(|p| p.is_file())
        .unwrap();
    fs::write(manifest, b"damaged optional image").unwrap();
    invoke("restore", &next, false);
    assert!(
        invoke("restore-from-journal", &next, true).contains("base_index=0 replayed_entries=3")
    );

    fs::write(journal_path.join("BASE"), b"malformed optional pointer").unwrap();
    invoke("restore", &rescue, false);
    let segment = journal_path.join(format!(
        "segment-{}",
        base.digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    ));
    let original = fs::read(&segment).unwrap();
    fs::remove_file(&segment).unwrap();
    invoke("restore-from-journal", &rescue, false);
    fs::write(&segment, b"corrupted original command bytes").unwrap();
    invoke("restore-from-journal", &rescue, false);
    invoke("activate", &next, false);
    assert_eq!(invoke("journal-head", &source, true), fenced);
    assert!(!root.join("rescue").exists());
    fs::write(&segment, original).unwrap();
    assert!(
        invoke("restore-from-journal", &rescue, true).contains("base_index=0 replayed_entries=3")
    );
    let recovered = Replica::reopen(
        &root.join("rescue"),
        ReplicaConfig {
            cluster: 8,
            node: 12,
            voters: vec![12, 13, 14],
            ..cfg
        },
    )
    .unwrap();
    assert_eq!(
        recovered.outcome_for(request, &create).unwrap(),
        Some(receipt)
    );
    recovered.committed_source().verify(&prefix).unwrap();
    recovered.committed_source().verify(&[tail]).unwrap();
    drop(recovered);
    assert!(invoke("activate", &rescue, true).contains("cluster=8 index=3"));
}
