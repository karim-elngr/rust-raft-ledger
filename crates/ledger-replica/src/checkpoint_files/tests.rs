use super::*;
use std::fs;
use tempfile::tempdir;

fn boundary(index: u64) -> CheckpointBoundary {
    CheckpointBoundary {
        cluster: 7,
        ledger: 10,
        asset: "USD".into(),
        scale: 2,
        voters: vec![1, 2, 3],
        learner: None,
        index,
        term: 2,
    }
}
fn stage_image(files: &CheckpointFiles) -> TempDir {
    let stage = files.stage().unwrap();
    let db = stage.path().join("db");
    fs::create_dir(&db).unwrap();
    fs::write(db.join("CURRENT"), b"MANIFEST-000003\n").unwrap();
    fs::write(db.join("MANIFEST-000003"), b"metadata").unwrap();
    stage
}
#[test]
fn reuses_only_the_same_immutable_inode_and_keeps_working_metadata_private() {
    let dir = tempdir().unwrap();
    let files = CheckpointFiles::new(dir.path()).unwrap();
    let s1 = stage_image(&files);
    fs::write(s1.path().join("db/000001.sst"), b"contents").unwrap();
    let a = files.seal(s1, &boundary(40), None).unwrap();
    let s2 = stage_image(&files);
    fs::hard_link(
        files.generation(&a).join("db/000001.sst"),
        s2.path().join("db/000001.sst"),
    )
    .unwrap();
    let (b, reused) = files.seal_counted(s2, &boundary(42), Some(&a)).unwrap();
    assert_eq!(reused, 1);
    let s3 = stage_image(&files);
    fs::write(s3.path().join("db/000001.sst"), b"replaced").unwrap();
    let (c, reused) = files.seal_counted(s3, &boundary(44), Some(&b)).unwrap();
    assert_eq!(reused, 0);
    let verified = files.load(&c, &boundary(44)).unwrap();
    let working = dir.path().join("working");
    verified.materialize(&working).unwrap();
    fs::write(working.join("MANIFEST-000003"), b"changed").unwrap();
    assert_eq!(
        fs::read(files.generation(&c).join("db/MANIFEST-000003")).unwrap(),
        b"metadata"
    );
    assert_eq!(fs::read(working.join("000001.sst")).unwrap(), b"replaced");
    assert!(!files.retire(&c).unwrap()); // retained while a verified reader exists
    drop(verified);
    assert!(files.retire(&c).unwrap());
    files.scrub(&b, &boundary(42)).unwrap();
}

#[test]
fn rejects_damaged_files_wrong_boundary_symlinks_and_unknown_contents() {
    let dir = tempdir().unwrap();
    let f = CheckpointFiles::new(dir.path()).unwrap();
    let stage = stage_image(&f);
    fs::write(stage.path().join("db/000002.blob"), b"immutable data").unwrap();
    let r = f.seal(stage, &boundary(2), None).unwrap();
    assert!(f.load(&r, &boundary(3)).is_err());
    let path = f.generation(&r);
    fs::write(path.join("unexpected"), b"retain").unwrap();
    assert!(f.load(&r, &boundary(2)).is_err());
    assert!(f.retire(&r).is_err());
    assert!(path.join("unexpected").exists());
    fs::remove_file(path.join("unexpected")).unwrap();
    fs::write(path.join("db/000002.blob"), b"different").unwrap();
    assert!(f.load(&r, &boundary(2)).is_err());
    assert!(f.scrub(&r, &boundary(2)).is_err());
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let s = stage_image(&f);
        symlink(path.join("db/000002.blob"), s.path().join("db/000003.sst")).unwrap();
        assert!(f.seal(s, &boundary(4), None).is_err());
    }
}

#[test]
fn startup_only_removes_marked_abandoned_captures_with_known_flat_files() {
    let voter = tempdir().unwrap();
    let files = CheckpointFiles::new(voter.path()).unwrap();
    let abandoned = files.stage().unwrap().keep();
    let db = abandoned.join("db.tmp");
    fs::create_dir(&db).unwrap();
    fs::write(db.join("000053.sst"), b"large stranded table").unwrap();
    let scratch = abandoned.join("replay-db");
    fs::create_dir(&scratch).unwrap();
    for name in ["000054.sst", "LOG", "LOCK"] {
        fs::write(scratch.join(name), []).unwrap();
    }
    fs::write(abandoned.join("metadata-copy-v1"), b"partial metadata").unwrap();
    let unknown = files.stage().unwrap().keep();
    fs::create_dir(unknown.join("db")).unwrap();
    fs::write(unknown.join("db/retain.me"), b"user data").unwrap();
    let unknown_replay = files.stage().unwrap().keep();
    fs::create_dir(unknown_replay.join("replay-db")).unwrap();
    fs::write(unknown_replay.join("replay-db/retain.me"), b"user data").unwrap();
    let unowned = files.root.join(".stage-A1B2C3");
    fs::create_dir(&unowned).unwrap();
    fs::create_dir(unowned.join("db")).unwrap();
    fs::write(unowned.join("db/000053.sst"), b"not ours").unwrap();
    files.recover_stages().unwrap();
    assert!(!abandoned.exists());
    assert!(unknown.join("db/retain.me").is_file());
    assert!(unknown_replay.join("replay-db/retain.me").is_file());
    assert!(unowned.join("db/000053.sst").is_file());
}

#[test]
fn collection_skips_the_active_and_pinned_generations_and_resumes_partial_retirement() {
    let voter = tempdir().unwrap();
    let files = CheckpointFiles::new(voter.path()).unwrap();
    let first = files
        .seal(stage_image(&files), &boundary(10), None)
        .unwrap();
    let second = files
        .seal(stage_image(&files), &boundary(11), Some(&first))
        .unwrap();
    let held = files.load(&first, &boundary(10)).unwrap();
    files.collect(Some(&second)).unwrap();
    assert!(files.generation(&first).exists());
    drop(held);
    let tomb = files
        .root
        .join(format!(".retiring-{}", digest_name(&first.digest)));
    fs::rename(files.generation(&first), &tomb).unwrap();
    fs::remove_file(tomb.join("db/CURRENT")).unwrap();
    let orphan = files
        .seal(stage_image(&files), &boundary(12), Some(&second))
        .unwrap();
    let bogus = files.root.join(digest_name(&[99; 32]));
    fs::create_dir(&bogus).unwrap();
    fs::write(bogus.join("user-data"), b"keep").unwrap();
    files.collect(Some(&second)).unwrap();
    assert!(!tomb.exists());
    assert!(!files.generation(&orphan).exists());
    assert!(bogus.join("user-data").exists());
    files.scrub(&second, &boundary(11)).unwrap();
}

#[cfg(unix)]
#[test]
fn full_scrub_hashes_immutable_bytes_even_when_local_file_identity_is_unchanged() {
    let voter = tempdir().unwrap();
    let f = CheckpointFiles::new(voter.path()).unwrap();
    let stage = stage_image(&f);
    fs::write(stage.path().join("db/000001.sst"), b"original").unwrap();
    let r = f.seal(stage, &boundary(1), None).unwrap();
    let path = f.generation(&r).join("db/000001.sst");
    let mtime = fs::metadata(&path).unwrap().modified().unwrap();
    fs::write(&path, b"tampered").unwrap();
    let mutating = fs::OpenOptions::new().write(true).open(&path).unwrap();
    mutating
        .set_times(fs::FileTimes::new().set_modified(mtime))
        .unwrap();
    // Simulate damage beneath filesystem mtime accounting. The forced path
    // must compare bytes even when all trusted metadata still matches.
    assert!(f.scrub(&r, &boundary(1)).is_err());
    // A normal write that changes the provenance is detected at load/seal.
    let changed = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);
    mutating
        .set_times(fs::FileTimes::new().set_modified(changed))
        .unwrap();
    assert!(f.load(&r, &boundary(1)).is_err());
    let next = stage_image(&f);
    fs::hard_link(path, next.path().join("db/000001.sst")).unwrap();
    assert!(f.seal(next, &boundary(2), Some(&r)).is_err());
}

#[test]
fn rejects_duplicate_path_traversal_and_missing_manifest_dependencies() {
    let voter = tempdir().unwrap();
    let f = CheckpointFiles::new(voter.path()).unwrap();
    let stage = stage_image(&f);
    fs::write(stage.path().join("db/000001.sst"), b"one").unwrap();
    fs::write(stage.path().join("db/000002.sst"), b"two").unwrap();
    let r = f.seal(stage, &boundary(1), None).unwrap();
    let gen_path = f.generation(&r);
    let bytes = fs::read(gen_path.join("manifest")).unwrap();
    let offset = bytes.windows(10).position(|s| s == b"000002.sst").unwrap();
    for bad in [b"000001.sst", b"../bad.sst"] {
        let mut forged = bytes.clone();
        forged[offset..offset + 10].copy_from_slice(bad);
        assert!(Manifest::parse(&forged).is_err());
    }
    fs::remove_file(gen_path.join("db/000002.sst")).unwrap();
    assert!(f.load(&r, &boundary(1)).is_err());
    f.collect(None).unwrap();
    assert!(gen_path.join("db/000001.sst").exists());
}

#[cfg(test)]
mod transfer_tests {
    use super::*;
    use crate::{ReplicaConfig, codec::encode_batch, projection::Projection};
    use ledger_core::{AccountId, Bounds, Command, Ledger, LedgerId, RequestId, Side};
    use raft::eraftpb::Entry;

    fn config(node: u64) -> ReplicaConfig {
        ReplicaConfig {
            cluster: 7,
            ledger: LedgerId(10),
            node,
            asset: "USD".into(),
            scale: 2,
            voters: vec![1, 2, 3],
        }
    }
    fn image(files: &CheckpointFiles) -> CheckpointRef {
        let working = tempdir().unwrap();
        let cfg = config(1);
        let mut db = Projection::reset(working.path(), Ledger::new(cfg.ledger, "USD", 2)).unwrap();
        let account = AccountId::new(cfg.ledger, 123);
        let command = Command::CreateAccount {
            account,
            code: 72,
            normal: Side::Debit,
            bounds: Bounds::default(),
        };
        db.apply(&Entry {
            index: 1,
            term: 2,
            data: encode_batch(cfg.ledger, &[(RequestId(43), command)])
                .unwrap()
                .into(),
            ..Default::default()
        })
        .unwrap();
        let stage = files.stage().unwrap();
        db.capture(&stage.path().join("db")).unwrap();
        files.seal(stage, &boundary(1), None).unwrap()
    }

    #[test]
    fn moves_a_real_projection_between_voters_and_retains_both_pins_during_gc() {
        use crate::{CacheBudget, CheckpointRef, Replica, SnapshotFiles};
        let sender = tempdir().unwrap();
        let receiver = tempdir().unwrap();
        let voter = Replica::bootstrap(receiver.path(), config(2)).unwrap();
        let second = CheckpointFiles::new(receiver.path()).unwrap();
        let first = CheckpointFiles::new(sender.path()).unwrap();
        let token = image(&first);
        assert_eq!(
            CheckpointRef::from_snapshot_token(&token.snapshot_token()).unwrap(),
            token
        );
        assert!(CheckpointRef::from_snapshot_token(&token.snapshot_token()[1..]).is_err());
        let a = SnapshotFiles::new(sender.path(), config(1))
            .unwrap()
            .source(token)
            .unwrap();
        let b = voter.snapshot_files().unwrap();
        let mut transfer = b.receive(token, a.manifest(), None).unwrap();
        assert!(b.receive(token, a.manifest(), None).is_err());
        first.collect(None).unwrap();
        while let Some(r) = transfer.next_range().unwrap() {
            transfer
                .write_chunk(r, &a.read_file(r.file, r.offset, r.length).unwrap())
                .unwrap();
        }
        let received = transfer.finish().unwrap();
        assert_eq!(received.source_reference(), token);
        let local = received.reference();
        assert_ne!(local.digest, token.digest);
        second.collect(None).unwrap();
        assert!(received.belongs_to(receiver.path(), config(2)).unwrap());
        assert!(!received.belongs_to(sender.path(), config(1)).unwrap());
        let state = tempdir().unwrap();
        let book = config(2).ledger;
        let restored =
            Projection::from_checkpoint(state.path(), book, 1, CacheBudget::default(), |p| {
                received.materialize(p)
            })
            .unwrap();
        assert_eq!(
            restored
                .account(AccountId::new(book, 123))
                .unwrap()
                .unwrap()
                .code(),
            72
        );
        let inbox = receiver.path().join("checkpoints/incoming-v1");
        assert!(
            !inbox.exists(),
            "the completed workspace must release every table"
        );
        drop(received);
        drop(b);
        drop(voter);
        let reopened = Replica::reopen(receiver.path(), config(2)).unwrap();
        let b = reopened.snapshot_files().unwrap();
        assert!(
            second.generation(&local).exists(),
            "restart GC preserves the fresh exact-retry seal"
        );
        // Admission receipt lost across restart. Exact retry uses zero network ranges.
        let mut again = b.receive(token, a.manifest(), None).unwrap();
        assert!(again.next_range().unwrap().is_none());
        let pinned = again.finish().unwrap();
        assert_eq!(pinned.reference(), local);
        let tomorrow = std::time::SystemTime::now() + std::time::Duration::from_secs(86401);
        assert!(!b.expire_at(tomorrow).unwrap());
        assert!(second.generation(&local).exists());
        drop(pinned);
        assert!(b.expire_at(tomorrow).unwrap());
        assert!(!second.generation(&local).exists());
    }

    #[test]
    fn resumes_inside_a_file_rejects_bad_data_and_skips_identical_local_tables() {
        use crate::{SNAPSHOT_CHUNK_BYTES, SnapshotFiles};
        let sender = tempdir().unwrap();
        let receiver = tempdir().unwrap();
        let first = CheckpointFiles::new(sender.path()).unwrap();
        let second = CheckpointFiles::new(receiver.path()).unwrap();
        let s = stage_image(&first);
        let data = vec![37; 2 * SNAPSHOT_CHUNK_BYTES + 33];
        fs::write(s.path().join("db/000001.sst"), &data).unwrap();
        let token = first.seal(s, &boundary(23), None).unwrap();
        let a = SnapshotFiles::new(sender.path(), config(1))
            .unwrap()
            .source(token)
            .unwrap();
        let b = SnapshotFiles::new(receiver.path(), config(2)).unwrap();
        let mut wrong = config(2);
        wrong.cluster += 1;
        assert!(
            SnapshotFiles::new(receiver.path(), &wrong)
                .unwrap()
                .receive(token, a.manifest(), None)
                .is_err()
        );
        let mut transfer = b.receive(token, a.manifest(), None).unwrap();
        let r = transfer.next_range().unwrap().unwrap();
        assert_eq!(r.offset, 0);
        transfer.write_chunk(r, &data[..r.length as usize]).unwrap();
        drop(transfer);
        let mut transfer = b.receive(token, a.manifest(), None).unwrap();
        let next = transfer.next_range().unwrap().unwrap();
        assert_eq!(next.offset, r.length.into());
        assert!(transfer.write_chunk(r, &data[..r.length as usize]).is_err());
        let mut wrong_bytes = a.read_file(next.file, next.offset, next.length).unwrap();
        wrong_bytes[0] ^= 1;
        transfer.write_chunk(next, &wrong_bytes).unwrap();
        let last = transfer.next_range().unwrap().unwrap();
        let end = transfer.write_chunk(
            last,
            &a.read_file(last.file, last.offset, last.length).unwrap(),
        );
        assert!(end.is_err() || transfer.next_range().is_err());
        drop(transfer);
        let old = stage_image(&second);
        fs::write(old.path().join("db/999.sst"), data).unwrap();
        let prior = second.seal(old, &boundary(12), None).unwrap();
        let mut transfer = b.receive(token, a.manifest(), Some(prior)).unwrap();
        let next = transfer.next_range().unwrap().unwrap();
        assert_ne!(
            next.file, 0,
            "identical immutable contents under a different local filename"
        );
        while let Some(r) = transfer.next_range().unwrap() {
            transfer
                .write_chunk(r, &a.read_file(r.file, r.offset, r.length).unwrap())
                .unwrap();
        }
        assert!(
            transfer.finish().is_err(),
            "invalid RocksDB metadata must be rejected before Raft"
        );
    }
    #[test]
    fn superseded_offer_reuses_only_identical_staged_files_and_partial_ranges() {
        use crate::{SNAPSHOT_CHUNK_BYTES, SnapshotFiles};
        let sender = tempdir().unwrap();
        let receiver = tempdir().unwrap();
        let first = CheckpointFiles::new(sender.path()).unwrap();
        let offers = SnapshotFiles::new(sender.path(), config(1)).unwrap();
        let target = SnapshotFiles::new(receiver.path(), config(2)).unwrap();
        let source = |index, prefix: &str, content: u8| {
            let stage = stage_image(&first);
            fs::write(stage.path().join("db").join(prefix), b"completed table").unwrap();
            fs::write(
                stage.path().join("db/000005.sst"),
                vec![content; SNAPSHOT_CHUNK_BYTES + 123],
            )
            .unwrap();
            let cut = first.seal(stage, &boundary(index), None).unwrap();
            offers.source(cut).unwrap()
        };
        let first = source(22, "000001.sst", 5);
        let second = source(23, "000002.sst", 5);
        let changed = source(24, "000002.sst", 6);
        let mut transfer = target
            .receive(first.reference(), first.manifest(), None)
            .unwrap();
        for _ in 0..2 {
            let r = transfer.next_range().unwrap().unwrap();
            transfer
                .write_chunk(r, &first.read_file(r.file, r.offset, r.length).unwrap())
                .unwrap();
        }
        drop(transfer);
        let mut transfer = target
            .receive(second.reference(), second.manifest(), None)
            .unwrap();
        let next = transfer.next_range().unwrap().unwrap();
        assert_eq!((next.file, next.offset), (1, SNAPSHOT_CHUNK_BYTES as u64));
        drop(transfer);
        let mut transfer = target
            .receive(changed.reference(), changed.manifest(), None)
            .unwrap();
        let next = transfer.next_range().unwrap().unwrap();
        assert_eq!((next.file, next.offset), (1, 0));
    }

    #[test]
    fn validates_wire_bounds_before_staging_and_preserves_unowned_inbox_contents() {
        use crate::{SNAPSHOT_CHUNK_BYTES, SnapshotFiles};
        let sender = tempdir().unwrap();
        let receiver = tempdir().unwrap();
        let f = CheckpointFiles::new(sender.path()).unwrap();
        let stage = stage_image(&f);
        fs::write(stage.path().join("db/001.blob"), b"blob data").unwrap();
        let token = f.seal(stage, &boundary(44), None).unwrap();
        let source = SnapshotFiles::new(sender.path(), config(1))
            .unwrap()
            .source(token)
            .unwrap();
        let dest = SnapshotFiles::new(receiver.path(), config(3)).unwrap();
        for (file, offset, len) in [
            (0, 0, 0),
            (0, u64::MAX, 1),
            (0, 0, SNAPSHOT_CHUNK_BYTES as u32 + 1),
            (u32::MAX, 0, 1),
        ] {
            assert!(source.read_file(file, offset, len).is_err());
        }
        let mut offered = Manifest::parse(source.manifest()).unwrap();
        offered.files.get_mut("001.blob").unwrap().size = u64::MAX;
        let forged = offered.bytes().unwrap();
        let digest = Sha256::digest(&forged).into();
        assert!(dest.receive(token, &forged, None).is_err());
        assert!(matches!(
            dest.receive(CheckpointRef { digest, ..token }, &forged, None),
            Err(ReplicaError::SizeLimit(_))
        ));
        let receipt = receiver.path().join("checkpoints/incoming-v1");
        assert!(!receipt.exists());
        let transfer = dest.receive(token, source.manifest(), None).unwrap();
        assert_eq!(
            transfer.total_file_bytes(),
            (b"blob data".len() + b"MANIFEST-000003\n".len() + b"metadata".len()) as u64
        );
        drop(transfer);
        fs::write(receipt.join("db/777.sst"), b"preexisting").unwrap();
        assert!(dest.receive(token, source.manifest(), None).is_err());
        assert_eq!(
            fs::read(receipt.join("db/777.sst")).unwrap(),
            b"preexisting"
        );
    }
    #[test]
    fn expiration_keeps_active_recent_and_unknown_workspaces_and_reclaims_idle_data() {
        use crate::SnapshotFiles;
        use std::time::{Duration, SystemTime};
        let sender = tempdir().unwrap();
        let dest = tempdir().unwrap();
        let owner = CheckpointFiles::new(sender.path()).unwrap();
        let stage = stage_image(&owner);
        fs::write(stage.path().join("db/01.sst"), vec![0; 300_000]).unwrap();
        let token = owner.seal(stage, &boundary(3), None).unwrap();
        let source = SnapshotFiles::new(sender.path(), config(1))
            .unwrap()
            .source(token)
            .unwrap();
        let replica = crate::Replica::bootstrap(dest.path(), config(2)).unwrap();
        let target = replica.snapshot_files().unwrap();
        let mut transfer = target.receive(token, source.manifest(), None).unwrap();
        let r = transfer.next_range().unwrap().unwrap();
        transfer
            .write_chunk(r, &source.read_file(r.file, r.offset, r.length).unwrap())
            .unwrap();
        let tomorrow = SystemTime::now() + Duration::from_secs(86401);
        assert!(!target.expire_at(tomorrow).unwrap(), "live receiver lease");
        drop(transfer);
        assert!(!target.expire_incoming().unwrap(), "recent partial");
        let inbox = dest.path().join("checkpoints/incoming-v1");
        // A long download refreshed the partial file without touching either
        // directory or the initial offer. Use its last write as activity.
        let old = SystemTime::now() - Duration::from_secs(3 * 86400);
        for path in [
            inbox.join("manifest"),
            inbox.join("db"),
            inbox.join("db.tmp"),
        ] {
            File::open(path).unwrap().set_modified(old).unwrap();
        }
        assert!(!target.expire_incoming().unwrap());
        fs::write(inbox.join("db/555.sst"), b"unexpected user file").unwrap();
        assert!(target.expire_at(tomorrow).is_err());
        assert!(inbox.join("db.tmp/01.sst").exists());
        fs::remove_file(inbox.join("db/555.sst")).unwrap();
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(sender.path(), inbox.join("db/01.sst")).unwrap();
            assert!(target.expire_at(tomorrow).is_err());
            fs::remove_file(inbox.join("db/01.sst")).unwrap();
        }
        let bridge = replica.snapshot_history().begin(token).unwrap();
        assert!(
            !target.expire_at(tomorrow).unwrap(),
            "an active exact history stage defers file cleanup"
        );
        drop(bridge);
        assert!(target.expire_at(tomorrow).unwrap());
        assert!(!inbox.exists());
        let mut restarted = target.receive(token, source.manifest(), None).unwrap();
        assert_eq!(restarted.next_range().unwrap().unwrap().offset, 0);
        drop(restarted);

        // Simulate interruption after atomic detachment and some per-file replacements.
        let tomb = dest.path().join("checkpoints/.retiring-incoming-v1");
        fs::rename(&inbox, &tomb).unwrap();
        fs::remove_file(tomb.join("db.tmp/01.sst")).unwrap();
        assert!(target.expire_incoming().unwrap());
        assert!(!tomb.exists());
    }
}
