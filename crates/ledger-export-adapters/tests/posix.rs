use ledger_core::LedgerId;
use ledger_export::{Identity, RawEntry, RecoveryArchive};
use ledger_export_adapters::PosixArchive;
use ledger_recovery::{Journal, Spec};
use tempfile::tempdir;

fn row(index: u8) -> RawEntry {
    RawEntry {
        index: u64::from(index),
        term: 1,
        bytes: vec![0x18, index, 0x10, 1],
    }
}

#[test]
fn lazy_open_exact_overlap_and_fenced_generation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("journal");
    let id = Identity {
        ledger: LedgerId(8),
        generation: 2,
        asset: "EUR".into(),
        scale: 2,
    };
    let sink = PosixArchive::open(&path, &id).unwrap();
    assert!(sink.head(&id).is_err());
    let j = Journal::init(
        &path,
        &Spec {
            ledger: 8,
            asset: "EUR".into(),
            scale: 2,
        },
        2,
    )
    .unwrap();
    let genesis = sink.head(&id).unwrap();
    let first = sink.append(&id, &genesis, &[row(1), row(2)]).unwrap();
    let reopened = PosixArchive::open(&path, &id).unwrap();
    assert_eq!(reopened.append(&id, &first, &[row(1)]).unwrap(), first);
    let mut reordered = row(2);
    reordered.bytes = vec![0x10, 1, 0x18, 2];
    assert!(reopened.append(&id, &first, &[reordered]).is_err());
    assert!(reopened.append(&id, &genesis, &[row(3)]).is_err());
    let tip = reopened.append(&id, &first, &[row(2), row(3)]).unwrap();
    assert_eq!(tip.index, 3);
    assert!(reopened.append(&id, &tip, &[row(5)]).is_err());
    let mut wrong = row(4);
    wrong.term = 2;
    assert!(reopened.append(&id, &tip, &[wrong]).is_err());
    let mut cursor = j.encoded_cursor(&j.head().unwrap(), 1).unwrap();
    for i in 1..=3 {
        assert_eq!(cursor.next_entry().unwrap().unwrap(), row(i).bytes);
    }
    j.fence(&j.head().unwrap()).unwrap();
    assert!(reopened.head(&id).is_err());
    assert!(reopened.append(&id, &tip, &[row(4)]).is_err());
}

#[cfg(unix)]
#[test]
fn learner_retries_after_a_dangling_or_redirected_mount() {
    use std::{fs, os::unix::fs::symlink};
    let root = tempdir().unwrap();
    let data = root.path().join("replica");
    let mount = root.path().join("mount");
    let separate = root.path().join("separate");
    fs::create_dir(&data).unwrap();
    fs::create_dir(&separate).unwrap();
    let spec = Spec {
        ledger: 8,
        asset: "EUR".into(),
        scale: 2,
    };
    let id = Identity {
        ledger: LedgerId(8),
        generation: 2,
        asset: "EUR".into(),
        scale: 2,
    };
    let inside = Journal::init(&data.join("journal"), &spec, 2).unwrap();
    Journal::init(&separate.join("journal"), &spec, 2).unwrap();
    symlink("missing", &mount).unwrap();
    let sink = PosixArchive::for_replica(&mount.join("journal"), &data, &id).unwrap();
    assert!(sink.head(&id).is_err());
    assert!(PosixArchive::resolve_separate_path(&mount.join("journal"), &data).is_err());
    fs::remove_file(&mount).unwrap();
    symlink(&separate, &mount).unwrap();
    let first = sink
        .append(&id, &sink.head(&id).unwrap(), &[row(1)])
        .unwrap();
    fs::remove_file(&mount).unwrap();
    symlink(&data, &mount).unwrap();
    assert!(sink.append(&id, &first, &[row(2)]).is_err());
    assert_eq!(inside.head().unwrap().index, 0);
    fs::remove_file(&mount).unwrap();
    symlink(&separate, &mount).unwrap();
    assert_eq!(sink.append(&id, &first, &[row(2)]).unwrap().index, 2);
}
