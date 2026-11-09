use super::*;
use crate::RecoveryBase;
use tempfile::tempdir;

fn open(path: &Path, cluster: u128) -> Journal {
    Journal::init(
        path,
        &Spec {
            ledger: 9,
            asset: "CAD".into(),
            scale: 2,
        },
        cluster,
    )
    .unwrap()
}
fn append(j: &Journal, prior: &Head, tag: u8) -> Head {
    j.append(
        prior,
        &[Entry {
            index: prior.index + 1,
            term: 1,
            data: vec![tag].into(),
            ..Default::default()
        }],
    )
    .unwrap()
}
fn build(j: &Journal, at: &Head, marker: u8) -> RecoveryBase {
    let prepared = j.prepare_base(at, |_| Ok(())).unwrap();
    j.publish_base(&prepared, [marker; 32]).unwrap()
}
#[test]
fn repair_of_a_damaged_pointer_needs_the_complete_history_and_the_same_pointer_at_publication() {
    let dir = tempdir().unwrap();
    let j = open(dir.path(), 7);
    let first = append(&j, &j.head().unwrap(), 1);
    let cut = append(&j, &first, 2);
    let pointer = dir.path().join("BASE");
    fs::write(&pointer, b"torn").unwrap();
    assert!(j.recovery_base().is_err());
    assert!(j.prepare_base_repair(&first, |_| Ok(())).is_err());
    let mut seen = vec![];
    let ticket = j
        .prepare_base_repair(&cut, |group| {
            seen.extend(group.entries().iter().map(|e| e.index));
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, [1, 2]);
    fs::write(&pointer, b"changed").unwrap();
    assert!(matches!(
        j.publish_base(&ticket, [1; 32]),
        Err(JournalError::Conflict)
    ));
    let ticket = j.prepare_base_repair(&cut, |_| Ok(())).unwrap();
    let installed = j.publish_base(&ticket, [2; 32]).unwrap();
    assert_eq!(j.recovery_base().unwrap(), Some(installed));
    fs::remove_file(object_path(dir.path(), &first.digest)).unwrap();
    assert!(j.prepare_base_repair(&cut, |_| Ok(())).is_err());
}

#[test]
fn publish_allows_concurrent_payments_and_fast_restore_authenticates_only_tail() {
    let dir = tempdir().unwrap();
    let journal = open(dir.path(), 7);
    let h = append(&journal, &journal.head().unwrap(), 1);
    let prepared = journal
        .prepare_base(&h, |group| {
            assert_eq!(group[0].index, 1);
            append(&journal, &h, 2);
            Ok(())
        })
        .unwrap();
    let base = journal.publish_base(&prepared, [9; 32]).unwrap();
    assert_eq!(base.source, h);
    assert_eq!(journal.publish_base(&prepared, [9; 32]).unwrap(), base);
    let fenced = journal.fence(&journal.head().unwrap()).unwrap();
    fs::remove_file(object_path(dir.path(), &h.digest)).unwrap();
    let mut seen = vec![];
    journal
        .visit_replacement_from_base(&base, &fenced, 8, |group| {
            seen.push(group[0].index);
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, [2]);
    assert!(journal.visit(&fenced, |_| Ok(())).is_err());
    assert_eq!(
        journal
            .activate_from_base_with(&base, &fenced, 8, |g| {
                assert_eq!(g[0].index, 2);
                Ok(())
            })
            .unwrap()
            .cluster,
        8
    );
}

#[test]
fn incremental_ancestry_remembers_generations_that_never_appended() {
    let dir = tempdir().unwrap();
    let j = open(dir.path(), u128::MAX);
    let h = append(&j, &j.head().unwrap(), 1);
    let base = build(&j, &h, 1);
    let f1 = j.fence(&h).unwrap();
    let next = j.activate(&f1, 42).unwrap();
    let f2 = j.fence(&next).unwrap();
    let current = j.activate(&f2, 7).unwrap();
    let end = append(&j, &current, 2);
    let _newer = append(&j, &end, 3);
    fs::remove_file(object_path(dir.path(), &h.digest)).unwrap();
    let mut applied = vec![];
    let fresh = j
        .prepare_base_since(&base, &end, |group| {
            applied.push(group[0].index);
            Ok(())
        })
        .unwrap();
    assert_eq!(applied, [2]);
    let base = j.publish_base(&fresh, [2; 32]).unwrap();
    for older in [f1, next, f2, current] {
        fs::remove_file(object_path(dir.path(), &older.digest)).unwrap();
    }
    let frozen = j.fence(&j.head().unwrap()).unwrap();
    for id in [u128::MAX, 42, 7, 0] {
        assert!(
            j.visit_replacement_from_base(&base, &frozen, id, |_| panic!("invalid replacement ID"))
                .is_err()
        );
    }
    let mut replay = vec![];
    j.visit_replacement_from_base(&base, &frozen, 9, |g| {
        replay.push(g[0].index);
        Ok(())
    })
    .unwrap();
    assert_eq!(replay, [3]);
    assert!(
        j.activate_from_base_with(&base, &frozen, 9, |_| Err(invalid("different local bytes")))
            .is_err()
    );
    assert_eq!(j.head().unwrap(), frozen);
    j.activate_from_base_with(&base, &frozen, 9, |_| Ok(()))
        .unwrap();
}

#[test]
fn a_checksum_valid_base_from_an_unpublished_fork_cannot_restore_or_activate() {
    let a = tempdir().unwrap();
    let b = tempdir().unwrap();
    let j = open(a.path(), 3);
    let other = open(b.path(), 3);
    let official = append(&j, &j.head().unwrap(), 1);
    let rogue = append(&other, &other.head().unwrap(), 2);
    let foreign = build(&other, &rogue, 12);
    assert!(
        j.prepare_base(&rogue, |_| panic!("foreign source"))
            .is_err()
    );
    let wrong_ticket = other.prepare_base(&rogue, |_| Ok(())).unwrap();
    assert!(j.publish_base(&wrong_ticket, [9; 32]).is_err());
    for item in fs::read_dir(b.path()).unwrap() {
        let item = item.unwrap();
        let name = item.file_name();
        if name == "BASE" || name.to_str().unwrap().starts_with("base-") {
            fs::copy(item.path(), a.path().join(name)).unwrap();
        }
    }
    assert!(
        j.prepare_base_since(&foreign, &official, |_| panic!("displaced base"))
            .is_err()
    );
    let f = j.fence(&official).unwrap();
    assert!(
        j.visit_replacement_from_base(&foreign, &f, 4, |_| panic!("fork"))
            .is_err()
    );
    assert!(
        j.activate_from_base_with(&foreign, &f, 4, |_| Ok(()))
            .is_err()
    );
    assert_eq!(j.head().unwrap(), f);
    j.visit_replacement(&f, 4, |_| Ok(())).unwrap();
}

#[test]
fn exact_base_cas_and_interruptions_preserve_published_authority() {
    let dir = tempdir().unwrap();
    let j = open(dir.path(), 99);
    let h = append(&j, &j.head().unwrap(), 1);
    let old = build(&j, &h, 1);
    let top = append(&j, &h, 2);
    let ticket = j.prepare_base_since(&old, &top, |_| Ok(())).unwrap();
    interruption::inject(interruption::Stage::BaseObjectSynced);
    assert!(matches!(
        j.publish_base(&ticket, [2; 32]),
        Err(JournalError::Io(_))
    ));
    assert_eq!(j.recovery_base().unwrap(), Some(old));
    interruption::inject(interruption::Stage::BaseRenamed);
    assert!(matches!(
        j.publish_base(&ticket, [2; 32]),
        Err(JournalError::Io(_))
    ));
    let live = j.publish_base(&ticket, [2; 32]).unwrap();
    assert_eq!(j.recovery_base().unwrap(), Some(live));
    assert!(matches!(
        j.publish_base(&ticket, [3; 32]),
        Err(JournalError::Conflict)
    ));
    let fresh = append(&j, &top, 3);
    let ticket = j.prepare_base(&fresh, |_| Ok(())).unwrap();
    let prior = j.recovery_base().unwrap();
    let f = j.fence(&fresh).unwrap();
    assert!(matches!(
        j.publish_base(&ticket, [3; 32]),
        Err(JournalError::Conflict)
    ));
    j.activate(&f, 101).unwrap();
    assert!(matches!(
        j.publish_base(&ticket, [3; 32]),
        Err(JournalError::Conflict)
    ));
    assert_eq!(j.recovery_base().unwrap(), prior);
}

#[test]
fn concurrent_base_publication_chooses_one_manifest_and_rejects_damage() {
    let dir = tempdir().unwrap();
    let j = open(dir.path(), 99);
    let h = append(&j, &j.head().unwrap(), 1);
    let tickets = [
        j.prepare_base(&h, |_| Ok(())).unwrap(),
        j.prepare_base(&h, |_| Ok(())).unwrap(),
    ];
    let barrier = std::sync::Barrier::new(2);
    let wins = std::thread::scope(|scope| {
        let jobs: Vec<_> = tickets
            .into_iter()
            .enumerate()
            .map(|(i, t)| {
                let b = &barrier;
                let j = &j;
                scope.spawn(move || {
                    b.wait();
                    j.publish_base(&t, [i as u8; 32])
                })
            })
            .collect();
        jobs.into_iter()
            .map(|w| w.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(wins.iter().filter(|w| w.is_ok()).count(), 1);
    let base = j.recovery_base().unwrap().unwrap();
    let pt = fs::read(dir.path().join("BASE")).unwrap();
    let digest = &pt[40..72];
    let file = fs::read_dir(dir.path())
        .unwrap()
        .map(Result::unwrap)
        .find(|e| {
            e.file_name().to_str().unwrap()
                == format!(
                    "base-{}",
                    digest
                        .iter()
                        .map(|v| format!("{v:02x}"))
                        .collect::<String>()
                )
        })
        .unwrap();
    fs::write(file.path(), b"damaged").unwrap();
    assert!(j.recovery_base().is_err());
    let f = j.fence(&h).unwrap();
    assert!(
        j.visit_replacement_from_base(&base, &f, 7, |_| Ok(()))
            .is_err()
    );
    fs::write(dir.path().join("BASE"), b"torn").unwrap();
    assert!(j.recovery_base().is_err());
    j.visit_replacement(&f, 7, |_| Ok(())).unwrap();
}
