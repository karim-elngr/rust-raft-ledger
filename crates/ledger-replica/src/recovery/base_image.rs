//! Voter-neutral physical recovery image. The caller creates the two checkpoints
//! from its private canonical journal replay, binds the returned manifest digest
//! to a journal cut, and publishes the directory. A consumer always makes two
//! private writable databases and verifies every copied byte.
use crate::{
    ReplicaError,
    checkpoint_files::{
        MAX_FILES, MAX_MANIFEST, create_synced, db_names, digest_stream, directory, exact_names,
        file_name, immutable, inventory, regular, small, sync,
    },
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, TryLockError},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

type Hash = [u8; 32];
const TREES: [&str; 2] = ["raft", "state"];
const MAGIC: &[u8] =
    b"ledger-recovery-base-image/v1\0raft-raw/v1\0projection/v2\0rocksdb/10.4.2-bytewise\0sha256\0";
const PIN: &[u8] = b"ledger/recovery-base-image/pin/v1\n";
const MAX_BYTES: u64 = 64 << 40;
struct Row {
    size: u64,
    hash: Hash,
}
type Tree = BTreeMap<String, Row>;
struct Manifest([Tree; 2]);

fn invalid(reason: &str) -> ReplicaError {
    ReplicaError::Invalid(reason.into())
}
fn add_size(total: &mut u64, size: u64) -> Result<(), ReplicaError> {
    *total = total
        .checked_add(size)
        .filter(|n| *n <= MAX_BYTES)
        .ok_or(ReplicaError::SizeLimit(
            "recovery base exceeds 64 TiB across both databases",
        ))?;
    Ok(())
}

impl Manifest {
    // Tree order is fixed: committed raw Raft, then ledger projection. Each row
    // is u8 name length, ASCII basename, u64 LE byte length, and SHA-256.
    fn encode(&self) -> Result<Vec<u8>, ReplicaError> {
        let mut bytes = MAGIC.to_vec();
        for tree in &self.0 {
            bytes.extend((tree.len() as u32).to_le_bytes());
            for (name, row) in tree {
                if bytes.len() + 1 + name.len() + 8 + 32 > MAX_MANIFEST {
                    return Err(ReplicaError::SizeLimit(
                        "recovery base manifest exceeds its budget",
                    ));
                }
                bytes.push(name.len() as u8);
                bytes.extend(name.as_bytes());
                bytes.extend(row.size.to_le_bytes());
                bytes.extend(row.hash);
            }
        }
        Ok(bytes)
    }
    fn parse(bytes: &[u8]) -> Result<Self, ReplicaError> {
        if bytes.len() > MAX_MANIFEST {
            return Err(invalid("oversized recovery base manifest"));
        }
        fn field<const N: usize>(input: &mut &[u8]) -> Result<[u8; N], ReplicaError> {
            let mut bytes = [0; N];
            input.read_exact(&mut bytes)?;
            Ok(bytes)
        }
        let mut input = bytes
            .strip_prefix(MAGIC)
            .ok_or_else(|| invalid("incompatible recovery base image format"))?;
        let mut trees = [Tree::new(), Tree::new()];
        let mut total = 0;
        for tree in &mut trees {
            let count = u32::from_le_bytes(field(&mut input)?) as usize;
            if count == 0 || count > MAX_FILES || count > input.len() / 42 {
                return Err(invalid("invalid recovery base inventory count"));
            }
            for _ in 0..count {
                let [length] = field(&mut input)?;
                let (name, tail) = input
                    .split_at_checked(length as usize)
                    .ok_or_else(|| invalid("short recovery base filename"))?;
                input = tail;
                let name = std::str::from_utf8(name)
                    .map_err(|_| invalid("invalid recovery base filename"))?;
                if !file_name(name)
                    || tree
                        .last_key_value()
                        .is_some_and(|(previous, _)| previous.as_str() >= name)
                {
                    return Err(invalid("invalid or unordered recovery base filename"));
                }
                let size = u64::from_le_bytes(field(&mut input)?);
                add_size(&mut total, size)?;
                tree.insert(
                    name.to_owned(),
                    Row {
                        size,
                        hash: field(&mut input)?,
                    },
                );
            }
        }
        if !input.is_empty() {
            return Err(invalid("trailing recovery base manifest data"));
        }
        Ok(Self(trees))
    }
}

// The original length bounds all I/O even if a source changes under a reader.
fn hash_to(mut file: File, size: u64, to: impl Write) -> Result<Hash, ReplicaError> {
    let before = file.metadata()?;
    if before.len() != size {
        return Err(invalid("recovery base file length mismatch"));
    }
    let (copied, hash) = digest_stream(&mut (&mut file).take(size), to)?;
    let after = file.metadata()?;
    if copied != size || after.len() != size || before.modified()? != after.modified()? {
        return Err(invalid("recovery base file changed while reading"));
    }
    Ok(hash)
}

fn copy_private(source: &Path, destination: &Path, size: u64) -> Result<Hash, ReplicaError> {
    let input = regular(source)?;
    let mut output = File::create_new(destination)?;
    let hash = hash_to(input, size, &mut output)?;
    output.sync_all()?;
    Ok(hash)
}

// The caller owns the private, unpublished output directory; no existing files
// can be overwritten. Both sources are stable RocksDB Checkpoint API outputs.
pub(super) fn seal(
    image_dir: &Path,
    raft_checkpoint: &Path,
    state_checkpoint: &Path,
) -> Result<Hash, ReplicaError> {
    exact_names(image_dir, &[])?;
    let sources = [raft_checkpoint, state_checkpoint];
    let mut manifest = Manifest([Tree::new(), Tree::new()]);
    let mut total = 0;
    for (source, tree) in sources.iter().zip(&mut manifest.0) {
        for name in inventory(source)? {
            let size = regular(&source.join(&name))?.metadata()?.len();
            add_size(&mut total, size)?;
            tree.insert(
                name,
                Row {
                    size,
                    hash: [0; 32],
                },
            );
        }
    }
    // Check the manifest budget before copying large objects.
    let _ = manifest.encode()?;
    create_synced(&image_dir.join("pin"), PIN)?;
    let pin = regular(&image_dir.join("pin"))?;
    pin.lock()?;
    for ((source, tree), kind) in sources.iter().zip(&mut manifest.0).zip(TREES) {
        let target = image_dir.join(kind);
        fs::create_dir(&target)?;
        for (name, row) in tree {
            let from = source.join(name);
            let to = target.join(name);
            // These immutable links can share a *private build* inode on the
            // journal volume. Neither restored voter ever shares an image inode.
            row.hash = if immutable(name) && fs::hard_link(&from, &to).is_ok() {
                let linked = regular(&to)?;
                linked.sync_all()?;
                hash_to(linked, row.size, io::sink())?
            } else {
                copy_private(&from, &to, row.size)?
            };
        }
        inventory(&target)?;
        sync(&target)?;
    }
    let bytes = manifest.encode()?;
    let hash = Sha256::digest(&bytes).into();
    create_synced(&image_dir.join("manifest"), &bytes)?;
    sync(image_dir)?;
    Ok(hash)
}

pub(crate) struct PortableImage {
    root: PathBuf,
    manifest: Manifest,
    _pin: File,
}
impl PortableImage {
    // The caller authenticates expected against its independent journal. An
    // owner deleting a published image must acquire the exclusive file lock.
    pub(crate) fn open(root: &Path, expected: Hash) -> Result<Self, ReplicaError> {
        directory(root)?;
        let root = fs::canonicalize(root)?;
        let lock = root.join("pin");
        let pin = regular(&lock)?;
        pin.lock_shared()?;
        validate_pin(&root, &pin)?;
        exact_names(&root, &["pin", "manifest", "raft", "state"])?;
        let manifest = read_manifest(&root, expected)?;
        let image = Self {
            root,
            manifest,
            _pin: pin,
        };
        for index in 0..TREES.len() {
            image.check_inventory(index)?;
        }
        Ok(image)
    }
    fn check_inventory(&self, index: usize) -> Result<(), ReplicaError> {
        let source = self.root.join(TREES[index]);
        let records = &self.manifest.0[index];
        if !inventory(&source)?.iter().eq(records.keys()) {
            return Err(invalid(
                "recovery base tree contents differ from its manifest",
            ));
        }
        for (name, row) in records {
            if regular(&source.join(name))?.metadata()?.len() != row.size {
                return Err(invalid("recovery base file length mismatch"));
            }
        }
        Ok(())
    }
    // Called from privately staged voter directories only. A failed copy can
    // leave files there; publication of either DB is the caller's responsibility.
    pub(super) fn materialize_raft(&self, destination: &Path) -> Result<(), ReplicaError> {
        self.verify_tree(0, Some(destination))
    }
    pub(super) fn materialize_state(&self, destination: &Path) -> Result<(), ReplicaError> {
        self.verify_tree(1, Some(destination))
    }
    /// The archive can only be opened read-only, with diagnostics directed
    /// elsewhere. The caller must hold this image pin until that DB closes.
    pub(crate) fn verified_raft_dir(&self) -> Result<PathBuf, ReplicaError> {
        self.verify_tree(0, None)?;
        Ok(self.root.join(TREES[0]))
    }
    pub(super) fn scrub(&self) -> Result<(), ReplicaError> {
        self.verify_tree(0, None)?;
        self.verify_tree(1, None)
    }
    fn verify_tree(&self, index: usize, destination: Option<&Path>) -> Result<(), ReplicaError> {
        self.check_inventory(index)?;
        let src = self.root.join(TREES[index]);
        if let Some(path) = destination {
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            let name = path
                .file_name()
                .ok_or_else(|| invalid("invalid recovery base destination"))?;
            directory(parent)?;
            if fs::canonicalize(parent)?.join(name).starts_with(&self.root) {
                return Err(invalid("recovery output must be outside the sealed base"));
            }
            match fs::symlink_metadata(path) {
                Ok(_) => exact_names(path, &[])?,
                Err(e) if e.kind() == io::ErrorKind::NotFound => fs::create_dir(path)?,
                Err(e) => return Err(e.into()),
            }
            sync(parent)?;
        }
        for (name, row) in &self.manifest.0[index] {
            let input = src.join(name);
            let hash = match destination {
                Some(path) => copy_private(&input, &path.join(name), row.size)?,
                None => hash_to(regular(&input)?, row.size, io::sink())?,
            };
            if hash != row.hash {
                return Err(invalid("recovery base object digest mismatch"));
            }
        }
        if let Some(path) = destination {
            sync(path)?;
        }
        Ok(())
    }
}

fn read_manifest(dir: &Path, expected: Hash) -> Result<Manifest, ReplicaError> {
    let bytes = small(&dir.join("manifest"), MAX_MANIFEST)?;
    if <Hash>::from(Sha256::digest(&bytes)) != expected {
        return Err(invalid("recovery base manifest digest mismatch"));
    }
    Manifest::parse(&bytes)
}

fn exists(path: &Path) -> Result<bool, ReplicaError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}

fn tombstone(parent: &Path, digest: Hash) -> PathBuf {
    use std::fmt::Write;
    let mut name = String::from(".retiring-base-");
    for byte in digest {
        write!(&mut name, "{byte:02x}").expect("string write");
    }
    parent.join(name)
}

// Return only authenticated names. Verify every remaining object before any
// unlink, including in a half-drained tombstone. No local inode-stamp shortcut
// applies to an independent archive.
fn verified_removals(
    dir: &Path,
    manifest: &Manifest,
    partial: bool,
) -> Result<Vec<(&'static str, BTreeSet<String>)>, ReplicaError> {
    let mut remaining = Vec::new();
    for (kind, tree) in TREES.into_iter().zip(&manifest.0) {
        let path = dir.join(kind);
        if partial && !exists(&path)? {
            continue;
        }
        let names = if partial {
            db_names(&path)?
        } else {
            inventory(&path)?
        };
        if !partial && names.len() != tree.len() {
            return Err(invalid("incomplete recovery base inventory"));
        }
        for name in &names {
            let row = tree
                .get(name)
                .ok_or_else(|| invalid("unowned file in recovery base"))?;
            if hash_to(regular(&path.join(name))?, row.size, io::sink())? != row.hash {
                return Err(invalid("unknown content in retiring recovery base"));
            }
        }
        remaining.push((kind, names));
    }
    Ok(remaining)
}

// A single catalog owner serializes publication and cleanup. A detached image
// keeps its manifest until all authenticated table and metadata unlinks have
// reached disk. The final crash window can leave an empty directory only.
fn retire_one(dir: &Path, tomb: &Path, digest: Hash) -> Result<bool, ReplicaError> {
    directory(dir)?;
    let detached = dir == tomb;
    if detached && !exists(&dir.join("pin"))? {
        if fs::read_dir(dir)?.next().is_none() {
            // No surviving ownership metadata; retain an empty unknown name.
            return Ok(false);
        }
        exact_names(dir, &["manifest"])?;
        read_manifest(dir, digest)?;
    } else {
        let pin = regular(&dir.join("pin"))?;
        match pin.try_lock() {
            Ok(()) => (),
            Err(TryLockError::WouldBlock) => return Ok(false),
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
        validate_pin(dir, &pin)?;
        if detached {
            // Both data directories may already have been fully removed.
            for item in fs::read_dir(dir)? {
                if !matches!(
                    item?.file_name().to_str(),
                    Some("pin" | "manifest" | "raft" | "state")
                ) {
                    return Err(invalid("unknown top-level file in retiring recovery base"));
                }
            }
        } else {
            exact_names(dir, &["pin", "manifest", "raft", "state"])?;
        }
        let manifest = read_manifest(dir, digest)?;
        let remaining = verified_removals(dir, &manifest, detached)?;
        if !detached {
            fs::rename(dir, tomb)?;
            sync(tomb.parent().expect("tombstone has a parent"))?;
        }
        for (kind, names) in remaining {
            let data = tomb.join(kind);
            for name in names {
                fs::remove_file(data.join(name))?;
            }
            sync(&data)?;
            fs::remove_dir(data)?;
        }
        sync(tomb)?;
        fs::remove_file(tomb.join("pin"))?;
        sync(tomb)?;
    }
    fs::remove_file(tomb.join("manifest"))?;
    fs::remove_dir(tomb)?;
    sync(tomb.parent().expect("tombstone has a parent"))?;
    Ok(true)
}

/// Caller has excluded active and durable cold-history dependencies under the
/// catalog lock. Busy readers and unowned data are preserved. Calling again
/// for an absent original path resumes its digest-named sibling tombstone.
pub(super) fn retire(path: &Path, digest: Hash) -> Result<bool, ReplicaError> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    directory(parent)?;
    let tomb = tombstone(parent, digest);
    let mut removed = false;
    if exists(&tomb)? {
        removed = retire_one(&tomb, &tomb, digest)?;
        if !removed {
            return Ok(false);
        }
    }
    if exists(path)? {
        return retire_one(path, &tomb, digest);
    }
    Ok(removed)
}

fn validate_pin(dir: &Path, pin: &File) -> Result<(), ReplicaError> {
    let lock = dir.join("pin");
    // A collector could have retired and replaced the named generation while
    // this reader waited. Do not hold a lock on an unlinked inode.
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let held = pin.metadata()?;
        let named = regular(&lock)?.metadata()?;
        if (held.dev(), held.ino()) != (named.dev(), named.ino()) {
            return Err(invalid("recovery base replaced while acquiring its pin"));
        }
    }
    if small(&lock, PIN.len())? != PIN {
        return Err(invalid("unrecognized recovery base pin"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{codec::encode_batch, projection::Projection};
    use ledger_core::{AccountId, Bounds, Command, Ledger, LedgerId, RequestId, Side};
    use raft::eraftpb::Entry;
    use rocksdb::{DB, Options, checkpoint::Checkpoint};
    use tempfile::tempdir;

    #[test]
    fn exports_a_real_rocksdb_pair_and_gives_each_voter_private_writable_files() {
        let root = tempdir().unwrap();
        let path = root.path();
        let raw = DB::open_default(path.join("raw")).unwrap();
        raw.put(b"entry-1", b"original committed bytes").unwrap();
        raw.flush().unwrap();
        let raft_cut = path.join("raft-cut");
        Checkpoint::new(&raw)
            .unwrap()
            .create_checkpoint(&raft_cut)
            .unwrap();

        let id = LedgerId(42);
        let account = AccountId::new(id, 1);
        let command = Command::CreateAccount {
            account,
            code: 7,
            normal: Side::Credit,
            bounds: Bounds::default(),
        };
        let mut state =
            Projection::reset(&path.join("working"), Ledger::new(id, "USD", 2)).unwrap();
        state
            .apply(&Entry {
                index: 1,
                term: 2,
                data: encode_batch(id, &[(RequestId(6), command)]).unwrap().into(),
                ..Default::default()
            })
            .unwrap();
        let state_cut = path.join("state-cut");
        state.capture(&state_cut).unwrap();
        let base = path.join("base");
        fs::create_dir(&base).unwrap();
        let digest = seal(&base, &raft_cut, &state_cut).unwrap();
        let image = PortableImage::open(&base, digest).unwrap();
        let first = path.join("restored-raft");
        let second = path.join("restored-state");
        fs::create_dir(&second).unwrap();
        image.materialize_raft(&first).unwrap();
        image.materialize_state(&second).unwrap();
        Projection::validate_checkpoint_dir(&second, id, 1).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            for (tree, destination) in [("raft", &first), ("state", &second)] {
                for item in fs::read_dir(base.join(tree)).unwrap() {
                    let item = item.unwrap();
                    assert_ne!(
                        item.metadata().unwrap().ino(),
                        fs::metadata(destination.join(item.file_name()))
                            .unwrap()
                            .ino()
                    );
                }
            }
        }
        let voter = DB::open(&Options::default(), &first).unwrap();
        assert_eq!(
            voter.get(b"entry-1").unwrap().unwrap(),
            b"original committed bytes"
        );
        voter.put(b"entry-1", b"changed locally").unwrap();
        drop(voter);
        image.scrub().unwrap();
        let again = path.join("second-voter");
        image.materialize_raft(&again).unwrap();
        assert_eq!(
            DB::open(&Options::default(), again)
                .unwrap()
                .get(b"entry-1")
                .unwrap()
                .unwrap(),
            b"original committed bytes"
        );
    }
    fn sample(root: &Path) -> (PathBuf, Hash) {
        let original = root.join("original");
        fs::create_dir(&original).unwrap();
        fs::write(original.join("CURRENT"), b"MANIFEST-0001\n").unwrap();
        fs::write(original.join("MANIFEST-0001"), b"metadata").unwrap();
        fs::write(original.join("0001.sst"), b"12345678").unwrap();
        fs::write(original.join("0002.sst"), b"ABCDEFGH").unwrap();
        let base = root.join("gen");
        fs::create_dir(&base).unwrap();
        let digest = seal(&base, &original, &original).unwrap();
        (base, digest)
    }

    #[test]
    fn verifies_each_consumer_copy_and_refuses_unknown_content_or_mutable_aliasing() {
        let d = tempdir().unwrap();
        let (base, digest) = sample(d.path());
        assert!(PortableImage::open(&base, [1; 32]).is_err());
        let image = PortableImage::open(&base, digest).unwrap();
        assert!(image.materialize_raft(&base.join("wrong")).is_err());
        let output = d.path().join("occupied");
        fs::create_dir(&output).unwrap();
        fs::write(output.join("keep"), b"user data").unwrap();
        assert!(image.materialize_state(&output).is_err());
        assert_eq!(fs::read(output.join("keep")).unwrap(), b"user data");
        assert_eq!(
            image.verified_raft_dir().unwrap(),
            fs::canonicalize(base.join("raft")).unwrap()
        );
        fs::write(base.join("raft/0002.sst"), b"corrupt!").unwrap();
        assert!(image.materialize_raft(&d.path().join("bad-copy")).is_err());
        assert!(image.verified_raft_dir().is_err());
        assert!(image.scrub().is_err());
        drop(image);
        assert!(retire(&base, digest).is_err());
        assert!(base.join("raft/0001.sst").exists());
    }

    #[test]
    fn checks_manifest_bounds_and_paths_before_using_names() {
        let d = tempdir().unwrap();
        let (base, digest) = sample(d.path());
        let mut bytes = fs::read(base.join("manifest")).unwrap();
        let pos = bytes.windows(8).position(|s| s == b"0002.sst").unwrap();
        for bad in [b"../a.sst", b"0001.sst"] {
            let mut malformed = bytes.clone();
            malformed[pos..pos + 8].copy_from_slice(bad);
            assert!(Manifest::parse(&malformed).is_err());
        }
        bytes[pos + 8..pos + 16].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(Manifest::parse(&bytes).is_err());
        fs::write(base.join("raft/unknown"), b"keep").unwrap();
        assert!(PortableImage::open(&base, digest).is_err());
        assert!(retire(&base, digest).is_err());
        assert!(base.join("raft/unknown").exists());
        #[cfg(unix)]
        {
            fs::remove_file(base.join("raft/unknown")).unwrap();
            fs::remove_file(base.join("raft/0001.sst")).unwrap();
            std::os::unix::fs::symlink(
                d.path().join("original/0001.sst"),
                base.join("raft/0001.sst"),
            )
            .unwrap();
            assert!(PortableImage::open(&base, digest).is_err());
            assert!(retire(&base, digest).is_err());
        }
    }

    #[test]
    fn honors_multiple_reader_pins_and_resumes_detached_partial_retirement() {
        let d = tempdir().unwrap();
        let (base, digest) = sample(d.path());
        let first = PortableImage::open(&base, digest).unwrap();
        let other = PortableImage::open(&base, digest).unwrap();
        assert!(!retire(&base, digest).unwrap());
        drop(first);
        assert!(!retire(&base, digest).unwrap());
        drop(other);
        let tomb = tombstone(d.path(), digest);
        fs::rename(&base, &tomb).unwrap();
        fs::remove_file(tomb.join("raft/CURRENT")).unwrap();
        fs::remove_file(tomb.join("state/0001.sst")).unwrap();
        fs::write(tomb.join("state/777.sst"), b"unrecognized").unwrap();
        assert!(retire(&base, digest).is_err());
        assert!(tomb.join("raft/0001.sst").exists());
        fs::remove_file(tomb.join("state/777.sst")).unwrap();
        assert!(retire(&base, digest).unwrap());
        assert!(!tomb.exists());
        assert!(!retire(&base, digest).unwrap());
    }
}
