//! The token binds the sender's complete manifest. Its inode stamps only
//! identify files on the sender. A receiver uses content hashes, then seals its
//! own manifest with local provenance before making an image available to Raft.
use super::*;
use crate::{
    StaticReplicaConfig, checkpoint::CaptureGuard, projection::Projection, raft_store::RaftStore,
};
use ledger_core::LedgerId;
use std::io::{Seek, SeekFrom};
use std::sync::{Arc, atomic::AtomicBool};

pub const SNAPSHOT_CHUNK_BYTES: usize = 256 << 10;
pub const SNAPSHOT_MAX_MANIFEST: usize = MAX_MANIFEST;
// One received image per voter. No preallocation from peer-provided lengths.
const MAX_IMAGE_BYTES: u64 = 16 << 40;
const TOKEN_MAGIC: &[u8; 8] = b"LGRSNAP1";
const TOKEN_SIZE: usize = 8 + 8 + 8 + 32;
const INBOX: &str = "incoming-v1";
#[path = "transfer_lifetime.rs"]
pub(super) mod lifetime;
#[path = "transfer_replay.rs"]
mod replay;

impl CheckpointRef {
    /// Fixed-width Raft snapshot data; file contents travel out of band.
    pub fn snapshot_token(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(TOKEN_SIZE);
        bytes.extend(TOKEN_MAGIC);
        bytes.extend(self.index.to_be_bytes());
        bytes.extend(self.term.to_be_bytes());
        bytes.extend(self.digest);
        bytes
    }

    pub fn from_snapshot_token(bytes: &[u8]) -> Result<Self, ReplicaError> {
        if bytes.len() != TOKEN_SIZE || !bytes.starts_with(TOKEN_MAGIC) {
            return Err(invalid("invalid Raft snapshot token"));
        }
        let reference = Self {
            index: u64::from_be_bytes(bytes[8..16].try_into().expect("fixed field")),
            term: u64::from_be_bytes(bytes[16..24].try_into().expect("fixed field")),
            digest: bytes[24..].try_into().expect("fixed field"),
        };
        if reference.index == 0 || reference.term == 0 {
            return Err(invalid("snapshot requires a non-genesis cut"));
        }
        Ok(reference)
    }

    /// Checks the Raft envelope's cut before any file transfer or handoff.
    pub fn from_snapshot(snapshot: &raft::eraftpb::Snapshot) -> Result<Self, ReplicaError> {
        let reference = Self::from_snapshot_token(&snapshot.data)?;
        let metadata = snapshot.get_metadata();
        if (reference.index, reference.term) != (metadata.index, metadata.term) {
            return Err(invalid(
                "Raft snapshot metadata differs from its image token",
            ));
        }
        Ok(reference)
    }
}

#[derive(Clone)]
pub struct SnapshotFiles {
    files: CheckpointFiles,
    scope: CheckpointBoundary,
    voter: Option<(RaftStore, Arc<AtomicBool>)>,
}

impl SnapshotFiles {
    /// Identity shared by all four members of a learner cohort. Includes the
    /// roles and ledger definition so a misconfigured member cannot vote or
    /// supply replication responses to peers from a different configuration.
    /// Three-voter groups retain their original cluster-ID-only protocol.
    pub fn learner_group_digest(&self) -> Option<[u8; 32]> {
        let scope = &self.scope;
        let learner = scope.learner?;
        let mut hash = Sha256::new();
        hash.update(b"ledger-peer/learner-group-v2\0");
        hash.update(scope.cluster.to_be_bytes());
        hash.update(scope.ledger.to_be_bytes());
        hash.update([scope.scale]);
        for voter in &scope.voters {
            hash.update(voter.to_be_bytes());
        }
        hash.update(learner.to_be_bytes());
        hash.update(scope.asset.as_bytes());
        Some(hash.finalize().into())
    }

    pub(crate) fn new(
        root: &Path,
        cfg: impl Into<StaticReplicaConfig>,
    ) -> Result<Self, ReplicaError> {
        Ok(Self {
            files: CheckpointFiles::new(root)?,
            scope: CheckpointBoundary::new(cfg, 0, 0),
            voter: None,
        })
    }

    pub(crate) fn with_voter(mut self, store: RaftStore, busy: Arc<AtomicBool>) -> Self {
        self.voter = Some((store, busy));
        self
    }

    fn boundary(&self, reference: CheckpointRef) -> CheckpointBoundary {
        CheckpointBoundary {
            index: reference.index,
            term: reference.term,
            ..self.scope.clone()
        }
    }

    pub fn source(&self, reference: CheckpointRef) -> Result<SnapshotSource, ReplicaError> {
        let pin = self.files.load(&reference, &self.boundary(reference))?;
        check_inventory(&pin.manifest)?;
        let manifest = pin.manifest.bytes()?;
        let files = pin
            .manifest
            .files
            .iter()
            .map(|(name, row)| (name.clone(), row.size))
            .collect();
        Ok(SnapshotSource {
            reference,
            manifest,
            files,
            pin,
        })
    }

    /// Opens or resumes the single persistent transfer slot. A different offer
    /// replaces only files claimed by the authenticated previous inventory.
    /// This is blocking disk work and belongs on a transfer worker.
    pub fn receive(
        &self,
        reference: CheckpointRef,
        manifest: &[u8],
        reuse: Option<CheckpointRef>,
    ) -> Result<SnapshotReceiver, ReplicaError> {
        let incoming = authenticate(reference, manifest)?;
        if incoming.boundary != self.boundary(reference) {
            return Err(invalid("snapshot belongs to another group or boundary"));
        }
        let lease = regular(&self.files.root.join(ROOT_MARKER))?;
        lease.try_lock().map_err(|err| match err {
            TryLockError::WouldBlock => invalid("a snapshot receive is already in progress"),
            TryLockError::Error(err) => err.into(),
        })?;
        self.expire_locked(std::time::SystemTime::now())?;
        // Pin the previous exact completion before collecting unadopted generations.
        let retained = self.retained(reference, &incoming)?;
        // Last attempt may have sealed an image subsequently discarded by
        // Raft. Keep other readers' pins and exclude concurrent pointer swaps.
        if let Some((store, busy)) = &self.voter
            && let Ok(_guard) = CaptureGuard::acquire(busy)
        {
            self.files.collect(store.checkpoint()?.as_ref())?;
        }
        let inbox = if retained.is_some() {
            if self.inbox_reference()? == Some(reference) {
                self.release_inbox(reference)?;
            }
            self.files.root.join(INBOX)
        } else {
            self.prepare_inbox(reference, manifest, &incoming)?
        };
        let local = match reuse.filter(|_| retained.is_none()) {
            Some(previous) => match self.files.load(&previous, &self.boundary(previous)) {
                Ok(prior) => Some(prior),
                Err(ReplicaError::Invalid(_)) => None,
                Err(ReplicaError::Io(e)) if e.kind() == io::ErrorKind::NotFound => None,
                Err(e) => return Err(e),
            },
            None => None,
        };
        let mut candidates = BTreeMap::new();
        if let Some(prior) = &local {
            for (name, row) in &prior.manifest.files {
                if immutable(name) {
                    candidates.insert((row.size, row.hash), prior.db.join(name));
                }
            }
        }
        let total = incoming.files.values().map(|row| row.size).sum();
        let matched: u64 = incoming
            .files
            .iter()
            .filter(|(name, row)| immutable(name) && candidates.contains_key(&(row.size, row.hash)))
            .map(|(_, row)| row.size)
            .sum();
        let replay_base = match (&self.voter, reuse, &local) {
            (Some((store, _)), Some(base), Some(_))
                if replay::prefer(base, reference, matched, total)
                    && store.checkpoint()? == Some(base)
                    && fs::read_dir(inbox.join("db"))?
                        .next()
                        .transpose()?
                        .is_none()
                    && fs::read_dir(inbox.join("db.tmp"))?
                        .next()
                        .transpose()?
                        .is_none() =>
            {
                Some(base)
            }
            _ => None,
        };
        Ok(SnapshotReceiver {
            files: self.clone(),
            retained,
            last_activity: std::time::Instant::now(),
            reference,
            db: inbox.join("db"),
            partial: inbox.join("db.tmp"),
            total,
            replay_base,
            records: incoming.files.into_iter().collect(),
            cursor: 0,
            pending: None,
            candidates,
            reuse_pin: local,
            _lease: lease,
        })
    }

    fn prepare_inbox(
        &self,
        reference: CheckpointRef,
        manifest: &[u8],
        incoming: &Manifest,
    ) -> Result<PathBuf, ReplicaError> {
        let inbox = self.files.root.join(INBOX);
        let prior = self.inbox_inventory()?;
        if prior.as_ref().is_some_and(|old| old.reference == reference) {
            self.refresh_activity(false)?;
            return Ok(inbox);
        }
        // The initial manifest and its directories appear together after
        // rename. Startup can clean an interrupted temporary stage.
        let stage = self.files.stage()?;
        let meta = stage.path().join("manifest");
        let mut file = File::create_new(&meta)?;
        file.write_all(&reference.snapshot_token())?;
        file.write_all(manifest)?;
        file.sync_all()?;
        for name in ["db", "db.tmp"] {
            let db = stage.path().join(name);
            fs::create_dir(&db)?;
        }
        if let Some(old) = &prior {
            old.carry_files(&inbox, stage.path(), incoming)?;
        }
        sync(&stage.path().join("db"))?;
        sync(&stage.path().join("db.tmp"))?;
        sync(stage.path())?;
        if let Some(old) = prior {
            if let Some((store, _)) = &self.voter
                && !store.snapshot_history().discard_stage(old.reference)?
            {
                return Err(invalid("superseded snapshot history still in use"));
            }
            self.release_inbox(old.reference)?;
        }
        fs::rename(stage.path(), &inbox)?;
        sync(&self.files.root)?;
        Ok(inbox)
    }
}

fn authenticate(reference: CheckpointRef, bytes: &[u8]) -> Result<Manifest, ReplicaError> {
    if bytes.len() > MAX_MANIFEST || <Hash>::from(Sha256::digest(bytes)) != reference.digest {
        return Err(invalid("invalid snapshot manifest length or digest"));
    }
    let manifest = Manifest::parse(bytes)?;
    if manifest.boundary.index != reference.index || manifest.boundary.term != reference.term {
        return Err(invalid(
            "snapshot token and manifest describe different cuts",
        ));
    }
    check_inventory(&manifest)?;
    Ok(manifest)
}

fn check_inventory(manifest: &Manifest) -> Result<(), ReplicaError> {
    if manifest.boundary.index == 0
        || manifest.boundary.term == 0
        || manifest
            .files
            .get("CURRENT")
            .is_none_or(|r| r.size == 0 || r.size > 64)
        || manifest
            .files
            .keys()
            .filter(|n| n.starts_with("MANIFEST-"))
            .count()
            != 1
    {
        return Err(invalid("snapshot lacks a valid projection inventory"));
    }
    manifest
        .files
        .values()
        .try_fold(0u64, |n, r| {
            n.checked_add(r.size).filter(|sum| *sum <= MAX_IMAGE_BYTES)
        })
        .ok_or(ReplicaError::SizeLimit(
            "snapshot exceeds the transfer disk budget",
        ))?;
    Ok(())
}

pub struct SnapshotSource {
    reference: CheckpointRef,
    manifest: Vec<u8>,
    files: Vec<(String, u64)>,
    pin: VerifiedCheckpoint,
}
impl SnapshotSource {
    pub fn reference(&self) -> CheckpointRef {
        self.reference
    }
    pub fn manifest(&self) -> &[u8] {
        &self.manifest
    }
    pub fn read_file(&self, file: u32, offset: u64, length: u32) -> Result<Vec<u8>, ReplicaError> {
        let (name, size) = self
            .files
            .get(file as usize)
            .ok_or_else(|| invalid("unknown snapshot object"))?;
        if length == 0
            || length as usize > SNAPSHOT_CHUNK_BYTES
            || offset
                .checked_add(length.into())
                .is_none_or(|end| end > *size)
        {
            return Err(invalid("invalid snapshot read range"));
        }
        let mut source = regular(&self.pin.db.join(name))?;
        source.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0; length as usize];
        source.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotRange {
    pub file: u32,
    pub offset: u64,
    pub length: u32,
}

pub struct SnapshotReceiver {
    retained: Option<ImportedCheckpoint>,
    last_activity: std::time::Instant,
    files: SnapshotFiles,
    reference: CheckpointRef,
    db: PathBuf,
    partial: PathBuf,
    total: u64,
    records: Vec<(String, Record)>,
    cursor: usize,
    pending: Option<SnapshotRange>,
    candidates: BTreeMap<(u64, Hash), PathBuf>,
    reuse_pin: Option<VerifiedCheckpoint>,
    replay_base: Option<CheckpointRef>,
    _lease: File,
}

impl SnapshotReceiver {
    /// Extends the resume window during the command-history phase, when no
    /// checkpoint files are being modified. Call after each durable batch.
    pub fn record_activity(&mut self) -> Result<(), ReplicaError> {
        if self.last_activity.elapsed() >= std::time::Duration::from_secs(60) {
            self.files.refresh_activity(self.retained.is_some())?;
            self.last_activity = std::time::Instant::now();
        }
        Ok(())
    }

    pub fn total_file_bytes(&self) -> u64 {
        self.total
    }

    /// Skips complete verified objects (including locally reused tables).
    /// On reopen, resumes at the length recovered from the filesystem. A torn
    /// or corrupt partial write can only cause that object to be retransmitted.
    pub fn next_range(&mut self) -> Result<Option<SnapshotRange>, ReplicaError> {
        self.replay_base = None;
        if self.retained.is_some() {
            return Ok(None);
        }
        if self.pending.is_some() {
            return Ok(self.pending);
        }
        while let Some((name, row)) = self.records.get_mut(self.cursor) {
            let name = name.as_str();
            let path = self.db.join(name);
            let part = self.partial.join(name);
            if path.try_exists()? {
                row.stamp = check_received(&self.db, name, row)?;
                // A crash between directory syncs of the cross-directory rename
                // may leave both names. The verified complete object wins.
                if part.try_exists()? {
                    regular(&part)?;
                    fs::remove_file(&part)?;
                    sync(&self.partial)?;
                }
                self.cursor += 1;
                continue;
            }
            let size = if part.try_exists()? {
                regular(&part)?.metadata()?.len()
            } else {
                if immutable(name)
                    && let Some(prior) = self.candidates.get(&(row.size, row.hash))
                    && fs::hard_link(prior, &path).is_ok()
                {
                    sync(&self.db)?;
                    match check_received(&self.db, name, row) {
                        Ok(stamp) => {
                            row.stamp = stamp;
                            self.cursor += 1;
                            continue;
                        }
                        Err(ReplicaError::Invalid(_)) => {
                            self.candidates.remove(&(row.size, row.hash));
                        }
                        Err(e) => return Err(e),
                    }
                }
                create_synced(&part, &[])?;
                sync(&self.partial)?;
                0
            };
            if size < row.size {
                let range = SnapshotRange {
                    file: self.cursor as u32,
                    offset: size,
                    length: (row.size - size).min(SNAPSHOT_CHUNK_BYTES as u64) as u32,
                };
                self.pending = Some(range);
                return Ok(self.pending);
            }
            row.stamp = check_received(&self.partial, name, row)?;
            fs::rename(part, path)?;
            sync(&self.db)?;
            sync(&self.partial)?;
            self.cursor += 1;
        }
        Ok(None)
    }

    pub fn write_chunk(&mut self, range: SnapshotRange, bytes: &[u8]) -> Result<(), ReplicaError> {
        if self.pending != Some(range) || bytes.len() != range.length as usize {
            return Err(invalid("unexpected snapshot range or length"));
        }
        let path = self.partial.join(&self.records[self.cursor].0);
        let file = regular(&path)?;
        if file.metadata()?.len() != range.offset {
            return Err(invalid("incoming snapshot object offset changed"));
        }
        let mut out = fs::OpenOptions::new().append(true).open(&path)?;
        // On I/O failure the next call reconciles the actual on-disk length.
        self.pending = None;
        out.write_all(bytes)?;
        out.sync_data()?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<ImportedCheckpoint, ReplicaError> {
        if let Some(pin) = self.retained.take() {
            return Ok(pin);
        }
        if self.next_range()?.is_some() {
            return Err(invalid("snapshot transfer is incomplete"));
        }
        let stage = self.files.files.stage()?;
        let db = stage.path().join("db");
        fs::create_dir(&db)?;
        // Same filesystem. Sealing privately copies all mutable RocksDB files.
        for (name, _) in &self.records {
            fs::hard_link(self.db.join(name), db.join(name))?;
        }
        let boundary = self.files.boundary(self.reference);
        Projection::validate_checkpoint_dir(&db, LedgerId(boundary.ledger), boundary.index)?;
        // All remote stamps were replaced during full-byte verification. Keep
        // the inbox leased so sealing can reuse those exact immutable inodes.
        let received = VerifiedCheckpoint {
            db: self.db,
            manifest: Manifest {
                boundary: boundary.clone(),
                files: self.records.into_iter().collect(),
            },
            _pin: self._lease,
        };
        let (local, pin, _) =
            self.files
                .files
                .seal_generation(stage, &boundary, None, Some(&received))?;
        self.files.save_receipt(self.reference, local)?;
        self.files.release_inbox(self.reference)?;
        Ok(ImportedCheckpoint {
            local,
            source: self.reference,
            pin,
            root: self.files.files.root.clone(),
        })
    }
}

// Hash all bytes including local reuse. Remote inode stamps are never a
// verification shortcut, even when their numbers happen to match local ones.
fn check_received(dir: &Path, name: &str, row: &Record) -> Result<Stamp, ReplicaError> {
    let path = dir.join(name);
    let mut file = regular(&path)?;
    let before = file.metadata()?;
    let stamp = Stamp::of(&before);
    if before.len() != row.size
        || digest_stream(&mut file, io::sink())? != (row.size, row.hash)
        || Stamp::of(&file.metadata()?) != stamp
    {
        fs::remove_file(path)?;
        sync(dir)?;
        return Err(invalid("incoming snapshot object failed verification"));
    }
    Ok(stamp)
}

/// Keeps local image files pinned through the subsequent Raft installation.
pub struct ImportedCheckpoint {
    local: CheckpointRef,
    source: CheckpointRef,
    pin: VerifiedCheckpoint,
    root: PathBuf,
}
impl ImportedCheckpoint {
    pub fn reference(&self) -> CheckpointRef {
        self.local
    }
    pub fn source_reference(&self) -> CheckpointRef {
        self.source
    }
    pub(crate) fn belongs_to(
        &self,
        voter_root: &Path,
        cfg: impl Into<StaticReplicaConfig>,
    ) -> Result<bool, ReplicaError> {
        Ok(
            fs::canonicalize(voter_root)?.join("checkpoints") == self.root
                && self.pin.manifest.boundary
                    == CheckpointBoundary::new(cfg, self.local.index, self.local.term),
        )
    }
    pub(crate) fn materialize(&self, fresh_dir: &Path) -> Result<(), ReplicaError> {
        self.pin.materialize(fresh_dir)
    }
}
