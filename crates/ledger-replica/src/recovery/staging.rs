//! A per-destination restore workspace. The marker's file description owns
//! the work until atomic publication. A crashed writer releases the lease.
use super::*;
use crate::checkpoint_files::{
    CheckpointFiles, create_synced, directory, exact_names, regular, small, sync,
};
use crate::projection::Projection;
use sha2::{Digest, Sha256};
use std::path::PathBuf;

const OWNER: &str = "restore-owner-v1";

fn invalid(why: &str) -> ReplicaError {
    ReplicaError::Invalid(why.into())
}

pub(super) fn check_state(path: &Path, journal: &Journal) -> Result<(), ReplicaError> {
    let marker = format!(".ledger-projection-v1-{:032x}", journal.spec().ledger);
    directory(path)?;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        match entry.file_name().to_str() {
            Some(n) if n == marker && regular(&entry.path())?.metadata()?.len() == 0 => (),
            Some("data" | "checkpoint-install-v1") => {
                Projection::check_private_build_db(&entry.path())?
            }
            _ => return Err(invalid("unknown private working projection content")),
        }
    }
    Ok(())
}

fn discard(work: &Path, journal: &Journal) -> Result<(), ReplicaError> {
    directory(work)?;
    for entry in fs::read_dir(work)? {
        let entry = entry?;
        match entry.file_name().to_str() {
            Some("raft") => Projection::check_private_build_db(&entry.path())?,
            Some("state") => check_state(&entry.path(), journal)?,
            Some("checkpoints") => {
                let checkpoints = CheckpointFiles::new(work)?;
                checkpoints.recover_stages()?;
                checkpoints.collect(None)?;
                // Collection preserves unknown files and foreign generations.
                exact_names(&entry.path(), &[".ledger-checkpoints-v1"])?;
            }
            _ => return Err(invalid("unknown file in restore staging directory")),
        }
    }
    fs::remove_dir_all(work)?;
    Ok(())
}

pub(super) struct RestoreStage {
    pub work: PathBuf,
    _lease: File,
}

impl RestoreStage {
    pub fn open(
        parent: &Path,
        destination: &Path,
        journal: &Journal,
    ) -> Result<Self, ReplicaError> {
        let name = destination
            .file_name()
            .ok_or_else(|| invalid("invalid restore destination"))?;
        let marker = bincode::serialize(&(
            b"ledger/restore/v1",
            name.as_encoded_bytes(),
            journal.spec(),
        ))?;
        let root = parent.join(format!(".ledger-restore-v1-{:x}", Sha256::digest(&marker)));
        match fs::create_dir(&root) {
            Ok(()) => {
                create_synced(&root.join(OWNER), &marker)?;
                sync(&root)?;
                sync(parent)?;
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => directory(&root)?,
            Err(e) => return Err(e.into()),
        }
        if small(&root.join(OWNER), marker.len())? != marker {
            return Err(invalid("unrecognized restore workspace"));
        }
        let lease = regular(&root.join(OWNER))?;
        lease
            .try_lock()
            .map_err(|e| invalid(&format!("cannot acquire restore workspace: {e}")))?;
        let work = root.join("voter");
        for entry in fs::read_dir(&root)? {
            match entry?.file_name().to_str() {
                Some(OWNER | "voter") => (),
                _ => return Err(invalid("unknown file in restore workspace")),
            }
        }
        match fs::symlink_metadata(&work) {
            Ok(_) => discard(&work, journal)?,
            Err(e) if e.kind() == ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        }
        fs::create_dir(&work)?;
        sync(&root)?;
        Ok(Self {
            work,
            _lease: lease,
        })
    }
}
