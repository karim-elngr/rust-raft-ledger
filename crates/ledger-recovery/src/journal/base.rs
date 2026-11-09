//! The BASE pointer is independent of HEAD. Only a replay of an observed live
//! journal cut can issue a publication ticket. Appends do not invalidate that
//! ticket. A fence, replacement, or another base publication does.
use super::*;
use crate::format::hash;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const POINTER: &[u8; 8] = b"LDGRbpt1";
const BINDING: &[u8; 8] = b"LDGRbas1";
// At most 1 MiB for generation IDs. Genesis-only recovery stays available
// even if a ledger outlives this optional image format's generation capacity.
const MAX_IDS: usize = 65536;
const MAX_BINDING: usize = (MAX_IDS * 16) + 512;
const POINTER_BYTES: usize = 104;

/// Exact publication whose image bytes must match the manifest digest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecoveryBase {
    pub source: Head,
    pub manifest: [u8; 32],
}

/// Held only by a builder that finished authenticating and replaying its source.
/// Publishing asserts that the supplied digest identifies the resulting durable
/// image. Image creation and verification belong to the trusted recovery writer.
pub struct PreparedBase {
    directory: PathBuf,
    spec: Hash,
    source: Head,
    expected_pointer: Option<Vec<u8>>,
    generations: BTreeSet<u128>,
}

fn object_name(dir: &Path, digest: &Hash) -> PathBuf {
    use std::fmt::Write as _;
    let mut name = String::from("base-");
    for byte in digest {
        write!(name, "{byte:02x}").expect("write to string");
    }
    dir.join(name)
}
fn read_regular(path: &Path, limit: usize) -> Result<Vec<u8>, JournalError> {
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(invalid("recovery base metadata is not a regular file"));
    }
    read_bounded(path, limit)
}
struct Binding {
    base: RecoveryBase,
    generations: BTreeSet<u128>,
}
fn encode(spec: Hash, base: &RecoveryBase, ids: &BTreeSet<u128>) -> Result<Vec<u8>, JournalError> {
    let mut bytes = bincode::serialize(&(BINDING, spec, base, ids.len() as u32))?;
    for id in ids {
        bytes.extend(id.to_le_bytes());
    }
    Ok(bytes)
}
fn decode(spec: Hash, bytes: &[u8], digest: &Hash) -> Result<Binding, JournalError> {
    if hash(bytes) != *digest {
        return Err(invalid("recovery base binding digest mismatch"));
    }
    let mut input = io::Cursor::new(bytes);
    let (tag, scope, base, count): ([u8; 8], Hash, RecoveryBase, u32) =
        bincode::deserialize_from(&mut input)?;
    let rest = &bytes[input.position() as usize..];
    let count = count as usize;
    if tag != *BINDING
        || scope != spec
        || count == 0
        || count > MAX_IDS
        || rest.len() != count * 16
        || base.source.cluster == 0
        || base.source.fenced
        || base.source.index == 0
        || base.source.term == 0
    {
        return Err(invalid("invalid recovery base binding"));
    }
    let mut generations = BTreeSet::new();
    for chunk in rest.as_chunks::<16>().0 {
        let id = u128::from_le_bytes(*chunk);
        if id == 0 || generations.last().is_some_and(|p| *p >= id) {
            return Err(invalid("invalid recovery base generation table"));
        }
        generations.insert(id);
    }
    if !generations.contains(&base.source.cluster) {
        return Err(invalid("base omits its source generation"));
    }
    Ok(Binding { base, generations })
}

impl Journal {
    pub fn directory(&self) -> &Path {
        &self.dir
    }

    fn base_record(&self) -> Result<Option<Vec<u8>>, JournalError> {
        match read_regular(&self.dir.join("BASE"), POINTER_BYTES) {
            Ok(v) => Ok(Some(v)),
            Err(JournalError::Io(e)) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
    fn parse_base_pointer(&self, bytes: &[u8]) -> Result<Hash, JournalError> {
        if bytes.len() != POINTER_BYTES
            || &bytes[..8] != POINTER
            || bytes[8..40] != self.spec_hash
            || hash(&bytes[..72]) != bytes[72..]
        {
            return Err(invalid("incompatible recovery base pointer"));
        }
        Ok(bytes[40..72].try_into().expect("fixed digest"))
    }
    fn base_pointer(&self) -> Result<Option<Hash>, JournalError> {
        self.base_record()?
            .as_deref()
            .map(|b| self.parse_base_pointer(b))
            .transpose()
    }
    fn read_binding(&self, digest: &Hash) -> Result<Binding, JournalError> {
        decode(
            self.spec_hash,
            &read_regular(&object_name(&self.dir, digest), MAX_BINDING)?,
            digest,
        )
    }

    /// Read the published image identity. The replacement APIs also authenticate
    /// its exact ancestry relative to the requested fence before permitting use.
    pub fn recovery_base(&self) -> Result<Option<RecoveryBase>, JournalError> {
        let pointer = {
            let _guard = self.lock("recovery_base")?;
            let pointer = self.base_pointer()?;
            sync_dir(&self.dir)?;
            pointer
        };
        pointer
            .map(|p| self.read_binding(&p).map(|b| b.base))
            .transpose()
    }

    /// Prove the source is an exact ancestor of an observed live head in the
    /// same generation. Writers remain free throughout verification and replay.
    pub fn prepare_base(
        &self,
        source: &Head,
        mut visitor: impl FnMut(&[Entry]) -> Result<(), JournalError>,
    ) -> Result<PreparedBase, JournalError> {
        self.prepare_base_encoded(None, source, |s| visitor(&s.entries))
    }
    /// The builder has verified and materialized the prior image. Replay only
    /// the suffix, extending its authenticated generation table across activation.
    pub fn prepare_base_since(
        &self,
        base: &RecoveryBase,
        source: &Head,
        mut visitor: impl FnMut(&[Entry]) -> Result<(), JournalError>,
    ) -> Result<PreparedBase, JournalError> {
        self.prepare_base_encoded(Some(base), source, |s| visitor(&s.entries))
    }
    pub fn prepare_base_encoded(
        &self,
        prior: Option<&RecoveryBase>,
        source: &Head,
        visitor: impl FnMut(&Segment) -> Result<(), JournalError>,
    ) -> Result<PreparedBase, JournalError> {
        self.prepare_image(prior, source, false, visitor)
    }

    /// Rebuild from the entire authenticated history at the current head.
    /// A damaged previous image, binding or bounded pointer may be superseded.
    /// A concurrently published replacement pointer always invalidates the ticket.
    pub fn prepare_base_repair(
        &self,
        source: &Head,
        visitor: impl FnMut(&Segment) -> Result<(), JournalError>,
    ) -> Result<PreparedBase, JournalError> {
        self.prepare_image(None, source, true, visitor)
    }

    fn prepare_image(
        &self,
        prior: Option<&RecoveryBase>,
        source: &Head,
        repair: bool,
        visitor: impl FnMut(&Segment) -> Result<(), JournalError>,
    ) -> Result<PreparedBase, JournalError> {
        let (observed, expected_pointer) = {
            let _guard = self.lock("prepare_base")?;
            if source.fenced || source.index == 0 {
                return Err(invalid("recovery image needs a nonempty live source"));
            }
            let observed = self.read_head()?;
            if observed.fenced
                || observed.cluster != source.cluster
                || observed.index < source.index
                || (repair && observed != *source)
            {
                return Err(JournalError::Conflict);
            }
            let pointer = self.base_record()?;
            sync_dir(&self.dir)?;
            (observed, pointer)
        };
        self.walk_back(Some(source), &observed, None, |_, _| Ok(()))?;
        let prior_pointer = if repair {
            None
        } else {
            expected_pointer
                .as_deref()
                .map(|b| self.parse_base_pointer(b))
                .transpose()?
        };
        let mut generations = match prior {
            Some(prior) => self.expected_binding(prior_pointer, prior)?.generations,
            None => {
                if let Some(digest) = prior_pointer {
                    let prior = self.read_binding(&digest)?.base;
                    if prior.source.index > source.index {
                        return Err(invalid("recovery base would regress"));
                    }
                }
                BTreeSet::new()
            }
        };
        self.replay_tracking(
            prior.map(|b| &b.source),
            source,
            None,
            |id| {
                generations.insert(id);
                if generations.len() > MAX_IDS {
                    return Err(invalid("recovery base generation table limit exceeded"));
                }
                Ok(())
            },
            visitor,
        )?;
        Ok(PreparedBase {
            directory: self.dir.clone(),
            spec: self.spec_hash,
            source: source.clone(),
            expected_pointer,
            generations,
        })
    }
    fn expected_binding(
        &self,
        pointer: Option<Hash>,
        expected: &RecoveryBase,
    ) -> Result<Binding, JournalError> {
        let binding = self.read_binding(&pointer.ok_or(JournalError::Conflict)?)?;
        if &binding.base != expected {
            return Err(JournalError::Conflict);
        }
        Ok(binding)
    }

    /// The manifest and all its objects must already be durable on the recovery
    /// volume. Under LOCK compare only small records. Every permitted HEAD write
    /// within a live generation is an append; fenced generation IDs cannot recur.
    pub fn publish_base(
        &self,
        prepared: &PreparedBase,
        manifest: Hash,
    ) -> Result<RecoveryBase, JournalError> {
        if self.dir != prepared.directory || self.spec_hash != prepared.spec {
            return Err(invalid("base ticket belongs to a different journal"));
        }
        let base = RecoveryBase {
            source: prepared.source.clone(),
            manifest,
        };
        let bytes = encode(self.spec_hash, &base, &prepared.generations)?;
        let digest = hash(&bytes);
        let path = object_name(&self.dir, &digest);
        match stage(&self.dir, &bytes)?.persist_noclobber(&path) {
            Ok(_) => (),
            Err(e) if e.error.kind() == io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e.error.into()),
        }
        if read_regular(&path, MAX_BINDING)? != bytes {
            return Err(invalid("immutable base binding readback failed"));
        }
        sync_dir(&self.dir)?;
        #[cfg(test)]
        interruption::check(interruption::Stage::BaseObjectSynced)?;
        let mut pointer = Vec::with_capacity(POINTER_BYTES);
        pointer.extend(POINTER);
        pointer.extend(self.spec_hash);
        pointer.extend(digest);
        pointer.extend(hash(&pointer));
        let staged_pointer = stage(&self.dir, &pointer)?;
        let _guard = self.lock("publish_base")?;
        let current = self.read_head()?;
        if current.fenced
            || current.cluster != base.source.cluster
            || current.index < base.source.index
            || (current.index == base.source.index && current != base.source)
        {
            return Err(JournalError::Conflict);
        }
        let installed = self.base_record()?;
        if installed.as_deref() != Some(pointer.as_slice()) {
            if installed != prepared.expected_pointer {
                return Err(JournalError::Conflict);
            }
            staged_pointer
                .persist(self.dir.join("BASE"))
                .map_err(|e| e.error)?;
            #[cfg(test)]
            interruption::check(interruption::Stage::BaseRenamed)?;
        }
        sync_dir(&self.dir)?;
        Ok(base)
    }
    fn replacement_binding(
        &self,
        base: &RecoveryBase,
        fenced: &Head,
        new_cluster: u128,
    ) -> Result<(), JournalError> {
        let pointer = {
            let _guard = self.lock("replacement_binding")?;
            if self.read_head()? != *fenced {
                return Err(JournalError::Conflict);
            }
            if !fenced.fenced || new_cluster == 0 {
                return Err(invalid(
                    "base restore requires a current fence and nonzero replacement",
                ));
            }
            let ptr = self.base_pointer()?;
            sync_dir(&self.dir)?;
            ptr
        };
        if self
            .expected_binding(pointer, base)?
            .generations
            .contains(&new_cluster)
        {
            return Err(invalid(
                "replacement cluster ID already occurs in recovery base",
            ));
        }
        Ok(())
    }
    /// Import original protobuf bytes after verifying the published fence and
    /// replacement identity. An optional independently checked image covers the prefix.
    pub fn visit_replacement_encoded(
        &self,
        base: Option<&RecoveryBase>,
        fenced: &Head,
        new_cluster: u128,
        visitor: impl FnMut(&Segment) -> Result<(), JournalError>,
    ) -> Result<(), JournalError> {
        match base {
            Some(base) => self.replacement_binding(base, fenced, new_cluster)?,
            None => self.require_fence(fenced, new_cluster)?,
        }
        self.replay_tracking(
            base.map(|b| &b.source),
            fenced,
            Some(new_cluster),
            |_| Ok(()),
            visitor,
        )?;
        self.require_head(fenced)
    }

    pub fn activate_encoded_with(
        &self,
        base: Option<&RecoveryBase>,
        fenced: &Head,
        new_cluster: u128,
        mut verify: impl FnMut(&Segment) -> Result<(), JournalError>,
    ) -> Result<Head, JournalError> {
        match base {
            Some(base) => self.replacement_binding(base, fenced, new_cluster)?,
            None => self.require_fence(fenced, new_cluster)?,
        }
        self.walk_segments(
            base.map(|b| &b.source),
            fenced,
            Some(new_cluster),
            |_, s| {
                if !s.entries.is_empty() {
                    verify(s)?;
                }
                Ok(())
            },
        )?;
        self.compare_publish(fenced, Change::Activate(new_cluster), &[])
    }

    /// The caller first verifies every imported image byte against base.manifest.
    /// Only an exact current binding, uninterrupted ancestry, unused generation
    /// and unchanged authoritative fence allow publication of the resulting voter.
    pub fn visit_replacement_from_base(
        &self,
        base: &RecoveryBase,
        fenced: &Head,
        new_cluster: u128,
        mut visitor: impl FnMut(&[Entry]) -> Result<(), JournalError>,
    ) -> Result<(), JournalError> {
        self.visit_replacement_encoded(Some(base), fenced, new_cluster, |group| {
            visitor(group.entries())
        })
    }
    /// Caller has verified durable local evidence of restoring this exact base.
    /// Compare all later Raft bytes (newest groups first) before the fence CAS.
    pub fn activate_from_base_with(
        &self,
        base: &RecoveryBase,
        fenced: &Head,
        new_cluster: u128,
        mut verify: impl FnMut(&[Entry]) -> Result<(), JournalError>,
    ) -> Result<Head, JournalError> {
        self.activate_encoded_with(Some(base), fenced, new_cluster, |group| {
            verify(group.entries())
        })
    }
}
