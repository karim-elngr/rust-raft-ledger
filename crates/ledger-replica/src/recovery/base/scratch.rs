//! Reclaim a crashed builder's private RocksDB scratch only after recognizing
//! its marker and every surviving name. The outer registry lock excludes live builds.
use super::*;
use crate::checkpoint_files::{create_synced, db_names, directory, regular, small, sync};
use std::ffi::OsStr;

const OWNER: &str = "build-owner-v1";
const PREFIX: &str = ".build-";
fn marker(journal: &Journal, path: &Path) -> Result<Vec<u8>, ReplicaError> {
    let name = path
        .file_name()
        .and_then(OsStr::to_str)
        .ok_or_else(|| invalid("invalid build path"))?;
    Ok(bincode::serialize(&(
        b"ledger/recovery-builder/v1",
        journal.spec(),
        name,
    ))?)
}
pub(super) fn create(root: &Path, journal: &Journal) -> Result<tempfile::TempDir, ReplicaError> {
    let work = tempfile::Builder::new().prefix(PREFIX).tempdir_in(root)?;
    create_synced(&work.path().join(OWNER), &marker(journal, work.path())?)?;
    sync(work.path())?;
    sync(root)?;
    Ok(work)
}
fn recognized(work: &Path, journal: &Journal) -> Result<(), ReplicaError> {
    let expected = marker(journal, work)?;
    if small(&work.join(OWNER), expected.len())? != expected {
        return Err(invalid("unrecognized build owner"));
    }
    for child in fs::read_dir(work)? {
        let child = child?;
        let path = child.path();
        match child.file_name().to_str() {
            Some(OWNER) => (),
            Some("raft") => Projection::check_private_build_db(&path)?,
            Some(
                "raw-checkpoint"
                | "raw-checkpoint.tmp"
                | "state-checkpoint"
                | "state-checkpoint.tmp",
            ) => {
                db_names(&path)?;
            }
            Some("state") => crate::recovery::staging::check_state(&path, journal)?,
            Some("image") => {
                directory(&path)?;
                for item in fs::read_dir(path)? {
                    let item = item?;
                    match item.file_name().to_str() {
                        Some("raft" | "state") => {
                            db_names(&item.path())?;
                        }
                        Some("manifest" | "pin") => {
                            regular(&item.path())?;
                        }
                        _ => return Err(invalid("unknown private image content")),
                    }
                }
            }
            _ => return Err(invalid("unknown content in recovery build directory")),
        }
    }
    Ok(())
}
pub(super) fn recover(root: &Path, journal: &Journal) -> Result<(), ReplicaError> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry
            .file_name()
            .to_str()
            .is_some_and(|n| n.starts_with(PREFIX))
            || !entry.file_type()?.is_dir()
        {
            continue;
        }
        let work = entry.path();
        match recognized(&work, journal) {
            Ok(()) => (),
            Err(ReplicaError::Invalid(_)) => continue,
            Err(ReplicaError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        }
        // Keep the ownership marker until every recognized child has been unlinked.
        for entry in fs::read_dir(&work)? {
            let entry = entry?;
            if entry.file_name() != OWNER {
                fs::remove_dir_all(entry.path())?;
            }
        }
        sync(&work)?;
        fs::remove_file(work.join(OWNER))?;
        fs::remove_dir(&work)?;
        sync(root)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn interrupted_builder_is_reclaimed_only_if_all_surviving_files_are_recognized() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = Journal::init(
            &tmp.path().join("journal"),
            &ledger_recovery::Spec {
                ledger: 7,
                asset: "USD".into(),
                scale: 2,
            },
            4,
        )
        .unwrap();
        let root = journal.directory().join(IMAGES);
        fs::create_dir(&root).unwrap();
        let interrupted = create(&root, &journal).unwrap().keep();
        fs::create_dir(interrupted.join("raft")).unwrap();
        fs::write(
            interrupted.join("raft/000013.sst"),
            b"private incomplete build",
        )
        .unwrap();
        let unknown = create(&root, &journal).unwrap().keep();
        fs::write(unknown.join("operator-note"), b"keep").unwrap();
        recover(&root, &journal).unwrap();
        assert!(!interrupted.exists());
        assert_eq!(fs::read(unknown.join("operator-note")).unwrap(), b"keep");
    }
}
