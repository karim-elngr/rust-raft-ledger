//! Local, sealed RocksDB generations. The voter owns this directory exclusively.
//! Immutable SST/blob fast verification trusts the local filesystem to preserve
//! inode identity, length and mtime for unmodified data. Full scrub also catches
//! silent media damage or a writer that deliberately restores timestamps.
use crate::{
    ReplicaError,
    checkpoint::{CheckpointBoundary, CheckpointRef},
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, TryLockError},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
use tempfile::TempDir;
mod manifest;
pub(crate) mod transfer;
use manifest::{Hash, Manifest, Record, Stamp};
pub(crate) use manifest::{MAX_FILES, MAX_MANIFEST, file_name, immutable};
#[cfg(test)]
mod tests;

const ROOT_MARKER: &str = ".ledger-checkpoints-v1";
const PIN: &[u8] = b"ledger/checkpoint-stage/v1\n";

fn invalid(text: &str) -> ReplicaError {
    ReplicaError::Invalid(text.into())
}
pub(crate) fn directory(path: &Path) -> Result<(), ReplicaError> {
    if !fs::symlink_metadata(path)?.is_dir() {
        return Err(invalid("expected real checkpoint directory"));
    }
    Ok(())
}
pub(crate) fn regular(path: &Path) -> Result<File, ReplicaError> {
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(invalid("unexpected checkpoint file type"));
    }
    let file = File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("checkpoint file changed type"));
    }
    Ok(file)
}
pub(crate) fn small(path: &Path, limit: usize) -> Result<Vec<u8>, ReplicaError> {
    let file = regular(path)?;
    if file.metadata()?.len() > limit as u64 {
        return Err(invalid("oversized checkpoint metadata"));
    }
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(invalid("oversized checkpoint metadata"));
    }
    Ok(bytes)
}
pub(crate) fn sync(path: &Path) -> Result<(), ReplicaError> {
    File::open(path)?.sync_all()?;
    Ok(())
}
pub(crate) fn create_synced(path: &Path, bytes: &[u8]) -> Result<(), ReplicaError> {
    let mut f = File::create_new(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}
pub(crate) fn digest_stream(
    source: &mut impl Read,
    mut destination: impl Write,
) -> Result<(u64, Hash), ReplicaError> {
    let mut hash = Sha256::new();
    let mut buf = [0; 64 << 10];
    let mut total = 0;
    loop {
        let n = source.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hash.update(&buf[..n]);
        destination.write_all(&buf[..n])?;
        total += n as u64;
    }
    Ok((total, hash.finalize().into()))
}
pub(crate) fn db_names(db: &Path) -> Result<BTreeSet<String>, ReplicaError> {
    directory(db)?;
    let mut names = BTreeSet::new();
    for item in fs::read_dir(db)? {
        let item = item?;
        let name = item
            .file_name()
            .into_string()
            .map_err(|_| invalid("non-UTF8 checkpoint filename"))?;
        if !item.file_type()?.is_file() || !file_name(&name) {
            return Err(invalid("unexpected checkpoint content"));
        }
        names.insert(name);
        if names.len() > MAX_FILES {
            return Err(invalid("too many checkpoint files"));
        }
    }
    Ok(names)
}
pub(crate) fn inventory(db: &Path) -> Result<BTreeSet<String>, ReplicaError> {
    let names = db_names(db)?;
    let current = small(&db.join("CURRENT"), 256)?;
    let current = std::str::from_utf8(&current)
        .ok()
        .and_then(|s| s.strip_suffix('\n'))
        .ok_or_else(|| invalid("invalid RocksDB CURRENT"))?;
    if !current.starts_with("MANIFEST-")
        || !names.contains(current)
        || names.iter().filter(|n| n.starts_with("MANIFEST-")).count() != 1
    {
        return Err(invalid(
            "checkpoint must contain its exact current RocksDB manifest",
        ));
    }
    Ok(names)
}
pub(crate) fn exact_names(dir: &Path, expected: &[&str]) -> Result<(), ReplicaError> {
    directory(dir)?;
    let mut count = 0;
    for item in fs::read_dir(dir)? {
        let name = item?.file_name();
        if !expected.iter().any(|v| name == *v) {
            return Err(invalid("unexpected generation contents"));
        }
        count += 1;
    }
    if count != expected.len() {
        return Err(invalid("incomplete checkpoint generation"));
    }
    Ok(())
}
fn digest_name(hash: &Hash) -> String {
    use std::fmt::Write;
    let mut name = String::from("gen-");
    for byte in hash {
        write!(&mut name, "{byte:02x}").expect("string write");
    }
    name
}
fn decode_name(name: &str) -> Option<Hash> {
    let tail = name.strip_prefix("gen-")?;
    if tail.len() != 64
        || !tail
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    let mut hash = [0; 32];
    for (i, b) in hash.iter_mut().enumerate() {
        *b = u8::from_str_radix(&tail[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(hash)
}

#[derive(Clone)]
pub(crate) struct CheckpointFiles {
    root: PathBuf,
}

impl CheckpointFiles {
    pub(crate) fn new(voter_root: &Path) -> Result<Self, ReplicaError> {
        directory(voter_root)?;
        let root = fs::canonicalize(voter_root)?.join("checkpoints");
        match fs::create_dir(&root) {
            Ok(()) => sync(voter_root)?,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => directory(&root)?,
            Err(e) => return Err(e.into()),
        }
        let owner = root.join(ROOT_MARKER);
        match fs::symlink_metadata(&owner) {
            Ok(m) if m.is_file() && m.len() == 0 => (),
            Err(e)
                if e.kind() == io::ErrorKind::NotFound && fs::read_dir(&root)?.next().is_none() =>
            {
                create_synced(&owner, &[])?;
                sync(&root)?;
            }
            _ => return Err(invalid("unowned checkpoint directory")),
        }
        Ok(Self { root })
    }
    pub(crate) fn stage(&self) -> Result<TempDir, ReplicaError> {
        let stage = tempfile::Builder::new()
            .prefix(".stage-")
            .tempdir_in(&self.root)?;
        create_synced(&stage.path().join("pin"), PIN)?;
        sync(stage.path())?;
        sync(&self.root)?;
        Ok(stage)
    }
    fn generation(&self, r: &CheckpointRef) -> PathBuf {
        self.root.join(digest_name(&r.digest))
    }

    #[cfg(test)]
    pub(crate) fn seal(
        &self,
        stage: TempDir,
        boundary: &CheckpointBoundary,
        previous: Option<&CheckpointRef>,
    ) -> Result<CheckpointRef, ReplicaError> {
        Ok(self.seal_pinned(stage, boundary, previous)?.0)
    }
    #[cfg(test)]
    fn seal_counted(
        &self,
        stage: TempDir,
        boundary: &CheckpointBoundary,
        previous: Option<&CheckpointRef>,
    ) -> Result<(CheckpointRef, usize), ReplicaError> {
        let (reference, _, reused) = self.seal_generation(stage, boundary, previous, None)?;
        Ok((reference, reused))
    }
    pub(crate) fn seal_pinned(
        &self,
        stage: TempDir,
        boundary: &CheckpointBoundary,
        previous: Option<&CheckpointRef>,
    ) -> Result<(CheckpointRef, VerifiedCheckpoint), ReplicaError> {
        let (reference, pin, _) = self.seal_generation(stage, boundary, previous, None)?;
        Ok((reference, pin))
    }
    fn seal_generation(
        &self,
        stage: TempDir,
        boundary: &CheckpointBoundary,
        previous: Option<&CheckpointRef>,
        received: Option<&VerifiedCheckpoint>,
    ) -> Result<(CheckpointRef, VerifiedCheckpoint, usize), ReplicaError> {
        if stage.path().parent() != Some(self.root.as_path())
            || !stage
                .path()
                .file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|s| s.starts_with(".stage-"))
        {
            return Err(invalid("unowned checkpoint stage"));
        }
        exact_names(stage.path(), &["pin", "db"])?;
        if small(&stage.path().join("pin"), PIN.len())? != PIN {
            return Err(invalid("unowned checkpoint stage"));
        }
        let old = previous.map(|r| self.pinned_manifest(r)).transpose()?;
        let predecessor = received.or(old.as_ref());
        if let Some(prior) = predecessor {
            let b = &prior.manifest.boundary;
            if b.cluster != boundary.cluster
                || b.ledger != boundary.ledger
                || b.scale != boundary.scale
                || b.asset != boundary.asset
                || b.voters != boundary.voters
                || b.learner != boundary.learner
                || b.index > boundary.index
                || b.term > boundary.term
            {
                return Err(invalid("incompatible predecessor checkpoint"));
            }
        }
        let db = stage.path().join("db");
        let mut files = BTreeMap::new();
        let mut reused = 0;
        for name in inventory(&db)? {
            let path = db.join(&name);
            let mut f = regular(&path)?;
            let meta = f.metadata()?;
            let size = meta.len();
            let stamp = Stamp::of(&meta);
            let is_immutable = immutable(&name);
            let hash = if is_immutable {
                // A surviving predecessor link prevents misleading filename or
                // recycled inode matches. New or copied files take the hash path.
                let linked = if let Some(prior) = predecessor
                    && let Some(row) = prior.manifest.files.get(&name)
                {
                    let old_path = prior.db.join(&name);
                    let old_meta = regular(&old_path)?.metadata()?;
                    stamp.same_inode(Stamp::of(&old_meta)).then_some(row)
                } else {
                    None
                };
                let hash = if let Some(row) = linked
                    && row.size == size
                    && stamp.matches(row.stamp)
                {
                    reused += 1;
                    row.hash
                } else {
                    let (len, hash) = digest_stream(&mut f, io::sink())?;
                    if len != size || linked.is_some_and(|row| row.hash != hash) {
                        return Err(invalid("immutable checkpoint file changed in place"));
                    }
                    hash
                };
                let end = f.metadata()?;
                if end.len() != size || Stamp::of(&end) != stamp {
                    return Err(invalid("checkpoint file changed while sealing"));
                }
                f.sync_all()?;
                hash
            } else {
                // RocksDB may hard-link OPTIONS or an inactive WAL. The sealed
                // metadata must be private even if the live DB later rewrites it.
                let tmp_path = stage.path().join("metadata-copy-v1");
                let mut temp = File::create_new(&tmp_path)?;
                let (len, hash) = digest_stream(&mut f, &mut temp)?;
                let end = f.metadata()?;
                if len != size || end.len() != size || Stamp::of(&end) != stamp {
                    return Err(invalid("checkpoint metadata changed while sealing"));
                }
                temp.sync_all()?;
                drop(temp);
                fs::rename(tmp_path, &path)?;
                hash
            };
            if received.is_some_and(|v| {
                v.manifest
                    .files
                    .get(&name)
                    .is_none_or(|row| row.size != size || row.hash != hash)
            }) {
                return Err(invalid(
                    "locally sealed object differs from the verified snapshot offer",
                ));
            }
            files.insert(
                name,
                Record {
                    size,
                    hash,
                    stamp: if is_immutable {
                        stamp
                    } else {
                        Stamp::default()
                    },
                },
            );
        }
        if received.is_some_and(|v| v.manifest.files.len() != files.len()) {
            return Err(invalid("incomplete local seal of snapshot offer"));
        }
        let manifest = Manifest {
            boundary: boundary.clone(),
            files,
        };
        let bytes = manifest.bytes()?;
        // Validate even caller-supplied boundary fields before publication.
        Manifest::parse(&bytes)?;
        let reference = CheckpointRef {
            index: boundary.index,
            term: boundary.term,
            digest: Sha256::digest(&bytes).into(),
        };
        create_synced(&stage.path().join("manifest"), &bytes)?;
        sync(&db)?;
        sync(stage.path())?;
        let target = self.generation(&reference);
        // Acquire the pin before the rename exposes this generation to GC.
        let pin = regular(&stage.path().join("pin"))?;
        pin.lock_shared()?;
        let verified = match fs::symlink_metadata(&target) {
            Ok(_) => self.load(&reference, boundary)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                match fs::rename(stage.path(), &target) {
                    Ok(()) => VerifiedCheckpoint {
                        db: target.join("db"),
                        manifest,
                        _pin: pin,
                    },
                    Err(_) if target.is_dir() => self.load(&reference, boundary)?,
                    Err(e) => return Err(e.into()),
                }
            }
            Err(e) => return Err(e.into()),
        };
        sync(&self.root)?;
        Ok((reference, verified, reused))
    }

    fn pinned_manifest(
        &self,
        reference: &CheckpointRef,
    ) -> Result<VerifiedCheckpoint, ReplicaError> {
        let generation = self.generation(reference);
        directory(&generation)?;
        let pin = regular(&generation.join("pin"))?;
        pin.lock_shared()?;
        // GC could have won the lock, unlinked this pin, and a repeat import
        // published the same digest while we waited. Keep the named inode pinned.
        #[cfg(unix)]
        if !Stamp::of(&pin.metadata()?)
            .same_inode(Stamp::of(&regular(&generation.join("pin"))?.metadata()?))
        {
            return Err(invalid("checkpoint was replaced while acquiring its pin"));
        }
        let manifest = read_manifest(&generation, &reference.digest)?;
        if manifest.boundary.index != reference.index || manifest.boundary.term != reference.term {
            return Err(invalid("invalid checkpoint reference boundary"));
        }
        Ok(VerifiedCheckpoint {
            db: generation.join("db"),
            manifest,
            _pin: pin,
        })
    }
    pub(crate) fn load(
        &self,
        reference: &CheckpointRef,
        expected: &CheckpointBoundary,
    ) -> Result<VerifiedCheckpoint, ReplicaError> {
        let verified = self.pinned_manifest(reference)?;
        if &verified.manifest.boundary != expected {
            return Err(invalid("checkpoint ledger, membership or cut mismatch"));
        }
        verified.verify(false)?;
        Ok(verified)
    }
    pub(crate) fn scrub(
        &self,
        reference: &CheckpointRef,
        expected: &CheckpointBoundary,
    ) -> Result<(), ReplicaError> {
        let verified = self.pinned_manifest(reference)?;
        if &verified.manifest.boundary != expected {
            return Err(invalid("checkpoint boundary mismatch"));
        }
        verified.verify(true)
    }
    #[cfg(test)]
    fn retire(&self, reference: &CheckpointRef) -> Result<bool, ReplicaError> {
        self.retire_generation(&reference.digest, false)
    }
    fn retire_generation(
        &self,
        digest: &Hash,
        preserve_receipt: bool,
    ) -> Result<bool, ReplicaError> {
        let generation = self.root.join(digest_name(digest));
        directory(&generation)?;
        let pin = regular(&generation.join("pin"))?;
        match pin.try_lock() {
            Ok(()) => (),
            Err(TryLockError::WouldBlock) => return Ok(false),
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
        // An exact completion may be retried after restart. Read its hint only
        // after taking this generation's exclusive pin, so a concurrent seal
        // cannot publish a fresh receipt between the check and retirement.
        if preserve_receipt && transfer::lifetime::fresh_receipt(&self.root)? == Some(*digest) {
            return Ok(false);
        }
        let manifest = read_manifest(&generation, digest)?;
        let verified = VerifiedCheckpoint {
            db: generation.join("db"),
            manifest,
            _pin: pin,
        };
        verified.verify(false)?;
        // Detach the complete generation atomically before removing any file.
        // Recovery may resume a partially emptied tombstone using its authenticated inventory.
        let retired = self.root.join(format!(".retiring-{}", digest_name(digest)));
        if retired.try_exists()? {
            self.finish_retirement(&retired, digest)?;
        }
        fs::rename(&generation, &retired)?;
        sync(&self.root)?;
        self.finish_retirement(&retired, digest)?;
        Ok(true)
    }

    fn finish_retirement(&self, dir: &Path, digest: &Hash) -> Result<(), ReplicaError> {
        directory(dir)?;
        let mut db = false;
        let mut pin = false;
        let mut manifest = false;
        for item in fs::read_dir(dir)? {
            let item = item?;
            match item.file_name().to_str() {
                Some("db") if item.file_type()?.is_dir() => db = true,
                Some("pin")
                    if item.file_type()?.is_file() && small(&item.path(), PIN.len())? == PIN =>
                {
                    pin = true
                }
                Some("manifest") if item.file_type()?.is_file() => manifest = true,
                _ => return Err(invalid("unknown files in retiring checkpoint")),
            }
        }
        if !manifest {
            // After dropping its last proof of ownership, leave an interrupted
            // empty directory for offline inspection rather than guessing.
            return Err(invalid("retiring checkpoint has no ownership manifest"));
        }
        let records = authenticated_manifest(dir, digest)?;
        if db {
            let path = dir.join("db");
            let remaining = db_names(&path)?;
            for name in &remaining {
                let row = records
                    .files
                    .get(name)
                    .ok_or_else(|| invalid("unrecognized retiring checkpoint file"))?;
                verify_file(&path, name, row, false)?;
            }
            for name in remaining {
                fs::remove_file(path.join(name))?;
            }
            fs::remove_dir(path)?;
        }
        if pin {
            fs::remove_file(dir.join("pin"))?;
        }
        sync(dir)?;
        fs::remove_file(dir.join("manifest"))?;
        fs::remove_dir(dir)?;
        sync(&self.root)
    }

    /// Only a newly opened voter holding the Raft store lock may recover stages.
    /// No capture or background finalizer can be running at that point.
    pub(crate) fn recover_stages(&self) -> Result<(), ReplicaError> {
        // An off-thread receiver can outlive the owner's shutdown. It uses
        // a transient sealing stage while holding this lock for the transfer.
        let lease = regular(&self.root.join(ROOT_MARKER))?;
        match lease.try_lock() {
            Ok(()) => (),
            Err(TryLockError::WouldBlock) => return Ok(()),
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
        for item in fs::read_dir(&self.root)? {
            let item = item?;
            let name = item.file_name();
            if !item.file_type()?.is_dir()
                || !name.to_str().is_some_and(|n| n.starts_with(".stage-"))
            {
                continue;
            }
            keep_unrecognized(self.discard_stage(&item.path()))?;
        }
        Ok(())
    }
    fn discard_stage(&self, dir: &Path) -> Result<(), ReplicaError> {
        if small(&dir.join("pin"), PIN.len())? != PIN {
            return Err(invalid("unknown capture stage"));
        }
        // Check the complete tree before unlinking anything. RocksDB creates
        // db.tmp first, then renames to db. Our metadata copy uses one fixed name.
        let mut dirs = Vec::new();
        let mut replay = None;
        let mut metadata = Vec::new();
        for item in fs::read_dir(dir)? {
            let item = item?;
            let kind = item.file_type()?;
            match item.file_name().to_str() {
                Some("db" | "db.tmp") if kind.is_dir() => {
                    let path = item.path();
                    let names = db_names(&path)?;
                    dirs.push((path, names));
                }
                Some("replay-db") if kind.is_dir() => {
                    let path = item.path();
                    crate::projection::Projection::check_private_build_db(&path)?;
                    replay = Some(path);
                }
                Some("pin") if kind.is_file() => (),
                Some("metadata-copy-v1" | "manifest") if kind.is_file() => {
                    metadata.push(item.path())
                }
                _ => return Err(invalid("unknown file in abandoned checkpoint stage")),
            }
        }
        if let Some(path) = replay {
            fs::remove_dir_all(path)?;
        }
        for (db, names) in dirs {
            for name in names {
                fs::remove_file(db.join(name))?;
            }
            fs::remove_dir(db)?;
        }
        for path in metadata {
            fs::remove_file(path)?;
        }
        sync(dir)?;
        fs::remove_file(dir.join("pin"))?;
        fs::remove_dir(dir)?;
        sync(&self.root)
    }

    /// Caller holds exclusive voter publication authority. Run after the active
    /// pointer sync or on exclusive reopen, never between seal and pointer sync.
    /// Unrecognized or damaged contents are preserved. Only recover_stages
    /// can discard an exclusively owned, interrupted capture.
    pub(crate) fn collect(&self, active: Option<&CheckpointRef>) -> Result<(), ReplicaError> {
        for item in fs::read_dir(&self.root)? {
            let item = item?;
            if !item.file_type()?.is_dir() {
                continue;
            }
            let basename = item.file_name();
            let Some(name) = basename.to_str() else {
                continue;
            };
            let result = if let Some(digest) = decode_name(name) {
                if active.is_some_and(|r| r.digest == digest) {
                    continue;
                }
                self.retire_generation(&digest, true).map(|_| ())
            } else if let Some(digest) = name.strip_prefix(".retiring-").and_then(decode_name) {
                self.finish_retirement(&item.path(), &digest)
            } else {
                continue;
            };
            // A plausible directory name alone confers no ownership. Leave
            // damaged and unrelated contents intact, including partial stages.
            keep_unrecognized(result)?;
        }
        Ok(())
    }
}

pub(crate) fn keep_unrecognized(result: Result<(), ReplicaError>) -> Result<(), ReplicaError> {
    match result {
        Ok(()) | Err(ReplicaError::Invalid(_)) => Ok(()),
        Err(ReplicaError::Io(e))
            if matches!(
                e.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::UnexpectedEof
            ) =>
        {
            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn read_manifest(gen_dir: &Path, hash: &Hash) -> Result<Manifest, ReplicaError> {
    exact_names(gen_dir, &["db", "manifest", "pin"])?;
    if small(&gen_dir.join("pin"), PIN.len())? != PIN {
        return Err(invalid("invalid checkpoint pin file"));
    }
    authenticated_manifest(gen_dir, hash)
}
fn authenticated_manifest(gen_dir: &Path, hash: &Hash) -> Result<Manifest, ReplicaError> {
    let bytes = small(&gen_dir.join("manifest"), MAX_MANIFEST)?;
    if <Hash>::from(Sha256::digest(&bytes)) != *hash {
        return Err(invalid("checkpoint manifest digest mismatch"));
    }
    Manifest::parse(&bytes)
}

fn verify_file(db: &Path, name: &str, record: &Record, full: bool) -> Result<(), ReplicaError> {
    let mut file = regular(&db.join(name))?;
    let meta = file.metadata()?;
    if meta.len() != record.size {
        return Err(invalid("checkpoint file length mismatch"));
    }
    if (full || !immutable(name) || !record.stamp.matches(Stamp::of(&meta)))
        && digest_stream(&mut file, io::sink())? != (record.size, record.hash)
    {
        return Err(invalid("checkpoint file digest mismatch"));
    }
    Ok(())
}

pub(crate) struct VerifiedCheckpoint {
    db: PathBuf,
    manifest: Manifest,
    _pin: File,
}
impl VerifiedCheckpoint {
    fn verify(&self, full: bool) -> Result<(), ReplicaError> {
        let names = inventory(&self.db)?;
        if !names.iter().eq(self.manifest.files.keys()) {
            return Err(invalid("checkpoint file inventory differs from manifest"));
        }
        for (name, record) in &self.manifest.files {
            verify_file(&self.db, name, record, full)?;
        }
        Ok(())
    }

    pub(crate) fn materialize(&self, fresh_dir: &Path) -> Result<(), ReplicaError> {
        let parent = fresh_dir
            .parent()
            .ok_or_else(|| invalid("invalid working checkpoint directory"))?;
        directory(parent)?;
        match fs::symlink_metadata(fresh_dir) {
            Ok(m) if m.is_dir() && fs::read_dir(fresh_dir)?.next().is_none() => (),
            Ok(_) => {
                return Err(invalid(
                    "checkpoint materialization requires a fresh directory",
                ));
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => fs::create_dir(fresh_dir)?,
            Err(e) => return Err(e.into()),
        }
        // Working owns atomic installation. Leave only recognizable RocksDB
        // names in its unpublished directory if a copy is interrupted.
        for (name, record) in &self.manifest.files {
            let src = self.db.join(name);
            let dst = fresh_dir.join(name);
            let mut file = regular(&src)?;
            if immutable(name) && fs::hard_link(&src, &dst).is_ok() {
                let mut linked = regular(&dst)?;
                let meta = linked.metadata()?;
                if meta.len() != record.size {
                    return Err(invalid("linked checkpoint size changed"));
                }
                if !record.stamp.matches(Stamp::of(&meta))
                    && digest_stream(&mut linked, io::sink())? != (record.size, record.hash)
                {
                    return Err(invalid("linked checkpoint digest mismatch"));
                }
            } else {
                let mut private = File::create_new(&dst)?;
                if digest_stream(&mut file, &mut private)? != (record.size, record.hash) {
                    return Err(invalid("copied checkpoint digest mismatch"));
                }
                private.sync_all()?;
            }
        }
        sync(fresh_dir)?;
        sync(parent)
    }
}

#[cfg(test)]
mod replay_tests;
