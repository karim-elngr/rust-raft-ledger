//! A voter that physically discards a prefix keeps a durable pin even when it
//! is offline. Builders and pin updates serialize independently of journal writes.
use super::*;
use crate::checkpoint_files::{directory, regular, small, sync};
use sha2::{Digest, Sha256};
use std::io::{ErrorKind, Write};

pub(super) fn lock(root: &Path) -> Result<File, ReplicaError> {
    directory(root)?;
    let path = root.join("BUILDER-LOCK");
    let file = match fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)
    {
        Ok(f) => {
            f.sync_all()?;
            sync(root)?;
            f
        }
        Err(e) if e.kind() == ErrorKind::AlreadyExists => {
            if regular(&path)?.metadata()?.len() != 0 {
                return Err(invalid("unrecognized recovery builder lock"));
            }
            fs::OpenOptions::new().read(true).write(true).open(path)?
        }
        Err(e) => return Err(e.into()),
    };
    file.try_lock()
        .map_err(|e| invalid(&format!("cannot acquire recovery image registry: {e}")))?;
    Ok(file)
}
fn mkdir(path: &Path) -> Result<(), ReplicaError> {
    match fs::create_dir(path) {
        Ok(()) => sync(path.parent().expect("registry child")),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => directory(path),
        Err(e) => Err(e.into()),
    }
}
fn refs(root: &Path, manifest: &[u8; 32]) -> PathBuf {
    generation(&root.join("pins-v1"), manifest)
}
fn pin_name(root: &Path, base: &RecoveryBase, identity: &[u8]) -> PathBuf {
    generation(
        &refs(root, &base.manifest),
        &Sha256::digest(identity).into(),
    )
}

pub(crate) fn pin_archive(
    journal: &Journal,
    base: &RecoveryBase,
    identity: &[u8],
) -> Result<(), ReplicaError> {
    let root = journal.directory().join(IMAGES);
    let _guard = lock(&root)?;
    if identity.is_empty()
        || journal.recovery_base().map_err(recovery_error)?.as_ref() != Some(base)
    {
        return Err(invalid("cannot pin a superseded or unbound recovery image"));
    }
    let _verified = open_image(journal, base)?;
    mkdir(&root.join("pins-v1"))?;
    let dir = refs(&root, &base.manifest);
    mkdir(&dir)?;
    let path = pin_name(&root, base, identity);
    let bytes = bincode::serialize(base)?;
    if path.try_exists()? {
        if small(&path, 512)? != bytes {
            return Err(invalid("invalid persistent recovery image pin"));
        }
    } else {
        let mut tmp = tempfile::NamedTempFile::new_in(&dir)?;
        tmp.write_all(&bytes)?;
        tmp.as_file().sync_all()?;
        tmp.persist_noclobber(&path).map_err(|e| e.error)?;
    }
    sync(&dir)
}
pub(crate) fn release_archive(
    journal: &Journal,
    base: &RecoveryBase,
    identity: &[u8],
) -> Result<(), ReplicaError> {
    let root = journal.directory().join(IMAGES);
    let _guard = lock(&root)?;
    let path = pin_name(&root, base, identity);
    if path.try_exists()? {
        if small(&path, 512)? != bincode::serialize(base)? {
            return Err(invalid("mismatched durable archive pin"));
        }
        fs::remove_file(&path)?;
        sync(path.parent().expect("pin directory"))?;
    }
    Ok(())
}

pub(super) fn collect(root: &Path, active: &[u8; 32]) -> Result<(), ReplicaError> {
    let pins = root.join("pins-v1");
    if pins.try_exists()? {
        directory(&pins)?;
    }
    for item in fs::read_dir(root)? {
        let item = item?;
        let name = item.file_name();
        let Some(name) = name.to_str() else { continue };
        let name = name.strip_prefix(".retiring-base-").unwrap_or(name);
        if name.len() != 64
            || !name
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            continue;
        }
        let digest = std::array::from_fn(|i| {
            u8::from_str_radix(&name[2 * i..2 * i + 2], 16).expect("checked hex")
        });
        if &digest == active {
            continue;
        }
        let dir = refs(root, &digest);
        if dir.try_exists()? {
            directory(&dir)?;
            if fs::read_dir(&dir)?.next().transpose()?.is_some() {
                continue;
            }
        }
        // A superseded corrupt image may still contain operator evidence.
        // The retirement code verifies all names and contents before unlinking.
        crate::checkpoint_files::keep_unrecognized(
            base_image::retire(&generation(root, &digest), digest).map(|_| ()),
        )?;
        if dir.try_exists()? {
            fs::remove_dir(dir)?;
            sync(&pins)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledger_recovery::Spec;
    use raft::eraftpb::Entry;

    #[test]
    fn offline_voter_pins_keep_their_exact_image_across_new_publications() {
        let root = tempfile::tempdir().unwrap();
        let spec = Spec {
            ledger: 1,
            asset: "USD".into(),
            scale: 2,
        };
        let journal = Journal::init(root.path(), &spec, 1).unwrap();
        let append = |head: &Head| {
            journal
                .append(
                    head,
                    &[Entry {
                        index: head.index + 1,
                        term: 1,
                        ..Default::default()
                    }],
                )
                .unwrap()
        };
        let first = append(&journal.head().unwrap());
        Replica::create_recovery_base(&journal, &first, CacheBudget::default()).unwrap();
        let old = journal.recovery_base().unwrap().unwrap();
        pin_archive(&journal, &old, b"voter one").unwrap();
        pin_archive(&journal, &old, b"voter two").unwrap();
        let next = append(&first);
        Replica::create_recovery_base(&journal, &next, CacheBudget::default()).unwrap();
        assert!(pin_archive(&journal, &old, b"stale request").is_err());
        release_archive(&journal, &old, b"voter one").unwrap();
        Replica::create_recovery_base(&journal, &next, CacheBudget::default()).unwrap();
        open_image(&journal, &old).unwrap().scrub().unwrap();
        let reader = open_image(&journal, &old).unwrap();
        release_archive(&journal, &old, b"voter two").unwrap();
        Replica::create_recovery_base(&journal, &next, CacheBudget::default()).unwrap();
        reader.scrub().unwrap();
        drop(reader);
        Replica::create_recovery_base(&journal, &next, CacheBudget::default()).unwrap();
        assert!(!generation(&journal.directory().join(IMAGES), &old.manifest).exists());
    }
}
