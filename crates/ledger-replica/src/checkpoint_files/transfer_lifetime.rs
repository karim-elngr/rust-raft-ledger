//! One leased receive slot and a small exact-retry receipt. Released inboxes
//! are detached before unlinking. An interrupted removal can be resumed at
//! startup without guessing which files belong to a peer manifest.
use super::*;
use std::time::{Duration, SystemTime};

const RECEIPT: &str = "incoming-receipt-v1";
const RETIRED: &str = ".retiring-incoming-v1";
const IDLE: Duration = Duration::from_secs(24 * 60 * 60);
const RECEIPT_TAG: &[u8] = b"ledger/incoming-receipt/v1\0";
const RECEIPT_SIZE: usize = RECEIPT_TAG.len() + 2 * TOKEN_SIZE + 32;

fn exists(path: &Path) -> Result<bool, ReplicaError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}
fn expired(then: SystemTime, now: SystemTime) -> bool {
    now.duration_since(then).is_ok_and(|age| age >= IDLE)
}

// The same exact manifest proves ownership of every surviving filename in a
// partially drained retirement. No content is removed until all names pass.
pub(super) struct InboxInventory {
    pub(super) reference: CheckpointRef,
    manifest: Manifest,
    complete: BTreeSet<String>,
    partial: BTreeSet<String>,
}

impl InboxInventory {
    pub(super) fn carry_files(
        &self,
        prior: &Path,
        stage: &Path,
        next: &Manifest,
    ) -> Result<(), ReplicaError> {
        let mut complete = BTreeMap::new();
        for name in &self.complete {
            let r = &self.manifest.files[name];
            complete.insert((r.size, r.hash), name);
        }
        for (name, row) in &next.files {
            if let Some(old) = complete.get(&(row.size, row.hash)) {
                fs::hard_link(prior.join("db").join(old), stage.join("db").join(name))?;
            } else if self.partial.contains(name)
                && self
                    .manifest
                    .files
                    .get(name)
                    .is_some_and(|old| (old.size, old.hash) == (row.size, row.hash))
            {
                // Match partials one-to-one. Two mutable names must never share
                // an inode after the old slot is retired.
                fs::hard_link(
                    prior.join("db.tmp").join(name),
                    stage.join("db.tmp").join(name),
                )?;
            }
        }
        Ok(())
    }
}

fn inspect(dir: &Path, files: &SnapshotFiles) -> Result<InboxInventory, ReplicaError> {
    exact_names(dir, &["pin", "manifest", "db", "db.tmp"])?;
    if small(&dir.join("pin"), PIN.len())? != PIN {
        return Err(invalid("unowned snapshot inbox"));
    }
    let meta = small(&dir.join("manifest"), MAX_MANIFEST + TOKEN_SIZE)?;
    let (token, bytes) = meta
        .split_at_checked(TOKEN_SIZE)
        .ok_or_else(|| invalid("short incoming manifest"))?;
    let reference = CheckpointRef::from_snapshot_token(token)?;
    let manifest = authenticate(reference, bytes)?;
    if manifest.boundary != files.boundary(reference) {
        return Err(invalid("incoming snapshot belongs to another group"));
    }
    let owned_names = |name| {
        let names = db_names(&dir.join(name))?;
        if names.iter().any(|n| !manifest.files.contains_key(n)) {
            return Err(invalid("unknown file in snapshot inbox"));
        }
        Ok(names)
    };
    Ok(InboxInventory {
        reference,
        complete: owned_names("db")?,
        partial: owned_names("db.tmp")?,
        manifest,
    })
}

fn receipt(root: &Path) -> Result<Option<(CheckpointRef, CheckpointRef)>, ReplicaError> {
    let path = root.join(RECEIPT);
    if !exists(&path)? {
        return Ok(None);
    }
    let bytes = small(&path, RECEIPT_SIZE)?;
    if bytes.len() != RECEIPT_SIZE || !bytes.starts_with(RECEIPT_TAG) {
        return Err(invalid("invalid incoming receipt"));
    }
    let (body, checksum) = bytes.split_at(RECEIPT_SIZE - 32);
    if &Sha256::digest(body)[..] != checksum {
        return Err(invalid("damaged incoming receipt"));
    }
    let (a, b) = body[RECEIPT_TAG.len()..].split_at(TOKEN_SIZE);
    let remote = CheckpointRef::from_snapshot_token(a)?;
    let local = CheckpointRef::from_snapshot_token(b)?;
    if (remote.index, remote.term) != (local.index, local.term) {
        return Err(invalid("invalid incoming receipt boundary"));
    }
    Ok(Some((remote, local)))
}

pub(in crate::checkpoint_files) fn fresh_receipt(
    root: &Path,
) -> Result<Option<Hash>, ReplicaError> {
    let path = root.join(RECEIPT);
    let read = || -> Result<Option<Hash>, ReplicaError> {
        let Some((_, local)) = receipt(root)? else {
            return Ok(None);
        };
        Ok(
            (!expired(regular(&path)?.metadata()?.modified()?, SystemTime::now()))
                .then_some(local.digest),
        )
    };
    match read() {
        Err(ReplicaError::Invalid(_)) => Ok(None),
        Err(ReplicaError::Io(e)) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        result => result,
    }
}
impl SnapshotFiles {
    pub(super) fn refresh_activity(&self, retained: bool) -> Result<(), ReplicaError> {
        let path = if retained {
            self.files.root.join(RECEIPT)
        } else {
            self.files.root.join(INBOX).join("manifest")
        };
        let f = regular(&path)?;
        f.set_modified(SystemTime::now())?;
        f.sync_all()?;
        Ok(())
    }
    pub(super) fn inbox_reference(&self) -> Result<Option<CheckpointRef>, ReplicaError> {
        Ok(self.inbox_inventory()?.map(|old| old.reference))
    }

    pub(super) fn inbox_inventory(&self) -> Result<Option<InboxInventory>, ReplicaError> {
        let dir = self.files.root.join(INBOX);
        if !exists(&dir)? {
            return Ok(None);
        }
        Ok(Some(inspect(&dir, self)?))
    }

    // Caller holds ROOT_MARKER exclusively. Preserve the authenticated
    // manifest and both directories until all data-file removals are synced.
    fn finish_retired_inbox(&self) -> Result<bool, ReplicaError> {
        let tomb = self.files.root.join(RETIRED);
        if !exists(&tomb)? {
            return Ok(false);
        }
        let old = inspect(&tomb, self)?;
        for (dir, names) in [
            (tomb.join("db"), old.complete),
            (tomb.join("db.tmp"), old.partial),
        ] {
            for name in names {
                fs::remove_file(dir.join(name))?;
            }
            sync(&dir)?;
        }
        // The remaining scaffold uses the ordinary, exclusively owned stage
        // layout. A crash during final unlink is recovered by recover_stages.
        let empty = tempfile::Builder::new()
            .prefix(".stage-")
            .tempdir_in(&self.files.root)?;
        let path = empty.keep();
        fs::remove_dir(&path)?;
        fs::rename(&tomb, &path)?;
        sync(&self.files.root)?;
        self.files.discard_stage(&path)?;
        Ok(true)
    }

    pub(super) fn release_inbox(&self, expected: CheckpointRef) -> Result<(), ReplicaError> {
        if self.inbox_reference()? != Some(expected) {
            return Err(invalid("snapshot receive slot changed"));
        }
        self.finish_retired_inbox()?;
        fs::rename(self.files.root.join(INBOX), self.files.root.join(RETIRED))?;
        sync(&self.files.root)?;
        self.finish_retired_inbox()?;
        Ok(())
    }

    pub(super) fn save_receipt(
        &self,
        remote: CheckpointRef,
        local: CheckpointRef,
    ) -> Result<(), ReplicaError> {
        // Decode any previous receipt before replacing it. Unrecognized data
        // at this fixed name belongs to the operator.
        receipt(&self.files.root)?;
        let stage = self.files.stage()?;
        let meta = stage.path().join("manifest");
        let mut bytes = RECEIPT_TAG.to_vec();
        bytes.extend(remote.snapshot_token());
        bytes.extend(local.snapshot_token());
        bytes.extend(Sha256::digest(&bytes));
        create_synced(&meta, &bytes)?;
        fs::rename(meta, self.files.root.join(RECEIPT))?;
        sync(&self.files.root)
    }

    pub(super) fn retained(
        &self,
        remote: CheckpointRef,
        offered: &Manifest,
    ) -> Result<Option<ImportedCheckpoint>, ReplicaError> {
        let Some((original, local)) = receipt(&self.files.root)? else {
            return Ok(None);
        };
        if original != remote {
            return Ok(None);
        }
        let pin = match self.files.load(&local, &self.boundary(local)) {
            Ok(pin) => pin,
            Err(ReplicaError::Io(e)) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(ReplicaError::Invalid(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        // The receipt alone never authorizes attaching different data to an offer.
        if offered.files.len() != pin.manifest.files.len()
            || offered.files.iter().any(|(name, r)| {
                pin.manifest
                    .files
                    .get(name)
                    .is_none_or(|local| local.hash != r.hash || local.size != r.size)
            })
        {
            return Err(invalid("snapshot receipt content differs from its offer"));
        }
        Projection::validate_checkpoint_dir(
            &pin.db,
            LedgerId(offered.boundary.ledger),
            remote.index,
        )?;
        self.refresh_activity(true)?;
        Ok(Some(ImportedCheckpoint {
            local,
            source: remote,
            pin,
            root: self.files.root.clone(),
        }))
    }

    /// Reclaims a marked, idle receive workspace after 24 hours. The transfer
    /// lock is nonblocking. A live receiver or pinned generation stays usable.
    /// Run on a blocking worker at startup and periodically while serving.
    pub fn expire_incoming(&self) -> Result<bool, ReplicaError> {
        self.expire_at(SystemTime::now())
    }

    pub(crate) fn expire_at(&self, now: SystemTime) -> Result<bool, ReplicaError> {
        let lease = regular(&self.files.root.join(ROOT_MARKER))?;
        match lease.try_lock() {
            Ok(()) => (),
            Err(TryLockError::WouldBlock) => return Ok(false),
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
        let changed = self.expire_locked(now)?;
        // A replaced receipt, or a crash before the receipt write, can leave
        // an ordinary generation without a receipt to drive its retirement.
        if let Some((store, busy)) = &self.voter
            && let Ok(_guard) = CaptureGuard::acquire(busy)
        {
            self.files.collect(store.checkpoint()?.as_ref())?;
        }
        Ok(changed)
    }

    pub(super) fn expire_locked(&self, now: SystemTime) -> Result<bool, ReplicaError> {
        let mut changed = self.finish_retired_inbox()?;
        let inbox = self.files.root.join(INBOX);
        if exists(&inbox)? {
            directory(&inbox)?;
            // Accepting an offer refreshes its manifest timestamp. Completing
            // an object updates a directory; writes to a partial update the file.
            let mut last = regular(&inbox.join("manifest"))?.metadata()?.modified()?;
            if expired(last, now) {
                for name in ["db", "db.tmp"] {
                    let dir = inbox.join(name);
                    directory(&dir)?;
                    last = last.max(fs::metadata(&dir)?.modified()?);
                }
                if expired(last, now) {
                    let partial = inbox.join("db.tmp");
                    for name in db_names(&partial)? {
                        last = last.max(regular(&partial.join(name))?.metadata()?.modified()?);
                    }
                }
            }
            if expired(last, now)
                && let Some(r) = self.inbox_reference()?
            {
                if let Some((store, _)) = &self.voter
                    && !store.snapshot_history().discard_stage(r)?
                {
                    return Ok(changed);
                }
                self.release_inbox(r)?;
                changed = true;
            }
        }
        let receipt_path = self.files.root.join(RECEIPT);
        if exists(&receipt_path)?
            && expired(regular(&receipt_path)?.metadata()?.modified()?, now)
            && let Some((source, local)) = receipt(&self.files.root)?
        {
            if let Some((store, busy)) = &self.voter {
                let Ok(_guard) = CaptureGuard::acquire(busy) else {
                    return Ok(changed);
                };
                if !store.snapshot_history().discard_stage(source)? {
                    return Ok(changed);
                }
                // A damaged hint must never retire the active generation.
                if !store
                    .checkpoint()?
                    .is_some_and(|active| active.digest == local.digest)
                    && exists(&self.files.generation(&local))?
                    && !self.files.retire_generation(&local.digest, false)?
                {
                    return Ok(changed);
                }
            }
            fs::remove_file(receipt_path)?;
            sync(&self.files.root)?;
            changed = true;
        }
        Ok(changed)
    }
}
