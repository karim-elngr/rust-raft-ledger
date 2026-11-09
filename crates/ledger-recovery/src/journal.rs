mod base;
mod encoded;
pub use base::{PreparedBase, RecoveryBase};
pub use encoded::EncodedCursor;

use std::{
    fs::{self, File, OpenOptions, TryLockError},
    io::{self, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use raft::eraftpb::Entry;
use tempfile::NamedTempFile;
use thiserror::Error;

use crate::format::{
    Change, Hash, Head, MAX_ENTRIES, MAX_HEAD_BYTES, MAX_SEGMENT_BYTES, Segment, Spec, decode_head,
    decode_segment, encode_head, encode_segment, invalid, spec_hash,
};

#[derive(Debug, Error)]
pub enum JournalError {
    #[error("invalid recovery journal: {0}")]
    Invalid(String),
    #[error("recovery journal head changed")]
    Conflict,
    #[error("recovery journal I/O: {0}")]
    Io(#[from] io::Error),
    #[error("recovery journal format: {0}")]
    Format(#[from] bincode::Error),
    #[error("recovery journal entry: {0}")]
    Entry(#[from] protobuf::ProtobufError),
}

/// Each operation opens its own lock file description. The volume must honor
/// POSIX advisory locks across hosts, atomic same-directory rename and fsync.
/// Lock contention times out after two seconds. The mount and storage service
/// must also bound the time spent performing filesystem I/O, including fsync.
#[derive(Clone, Debug)]
pub struct Journal {
    dir: PathBuf,
    spec: Spec,
    spec_hash: Hash,
    trace_locks: bool,
}

struct JournalLock {
    file: Option<File>,
    timing: Option<(&'static str, Duration, Instant)>,
}

impl Drop for JournalLock {
    fn drop(&mut self) {
        drop(self.file.take());
        if let Some((operation, waited, acquired)) = self.timing {
            let held = acquired.elapsed();
            let _ = writeln!(
                io::stderr().lock(),
                "journal_lock pid={} operation={operation} wait_us={} hold_us={}",
                std::process::id(),
                waited.as_micros(),
                held.as_micros()
            );
        }
    }
}

fn trace_locks() -> bool {
    std::env::var_os("LEDGER_JOURNAL_LOCK_TRACE").as_deref() == Some(std::ffi::OsStr::new("1"))
}

pub(crate) fn object_path(directory: &Path, digest: &Hash) -> PathBuf {
    use std::fmt::Write;
    let mut name = String::from("segment-");
    for byte in digest {
        write!(name, "{byte:02x}").expect("writing a string");
    }
    directory.join(name)
}

fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>, JournalError> {
    let file = File::open(path)?;
    let len = file.metadata()?.len();
    if len > limit as u64 {
        return Err(invalid("oversized recovery journal file"));
    }
    let mut bytes = Vec::with_capacity(len as usize);
    file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != len {
        return Err(invalid("recovery journal file changed during read"));
    }
    Ok(bytes)
}

fn acquire_lock(file: &File, operation: &str) -> io::Result<()> {
    const WAIT: Duration = Duration::from_secs(2);
    const RETRY: Duration = Duration::from_millis(8);
    let start = Instant::now();
    let deadline = start + WAIT;
    while Instant::now() < deadline {
        match file.try_lock() {
            Ok(()) => return Ok(()),
            Err(TryLockError::WouldBlock) => {}
            Err(TryLockError::Error(err)) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(TryLockError::Error(err)) => return Err(err),
        }
        thread::sleep(RETRY.min(deadline.saturating_duration_since(Instant::now())));
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "recovery journal lock timed out pid={} operation={operation} wait_us={}",
            std::process::id(),
            start.elapsed().as_micros()
        ),
    ))
}

fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn stage(dir: &Path, bytes: &[u8]) -> io::Result<NamedTempFile> {
    let mut tmp = NamedTempFile::new_in(dir)?;
    tmp.write_all(bytes)?;
    tmp.as_file().sync_all()?;
    Ok(tmp)
}

impl Journal {
    /// Initialize an empty, pre-mounted directory or create its last component.
    /// Initialization interrupted before genesis publication leaves the directory
    /// closed to reinitialization, preventing accidental rollback of a live ledger.
    pub fn init(path: &Path, spec: &Spec, cluster: u128) -> Result<Self, JournalError> {
        let digest = spec_hash(spec)?;
        if cluster == 0 {
            return Err(invalid("zero recovery cluster"));
        }
        match fs::create_dir(path) {
            Ok(()) => (),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e.into()),
        }
        let dir = fs::canonicalize(path)?;
        let parent = dir
            .parent()
            .ok_or_else(|| invalid("invalid recovery directory"))?;
        sync_dir(parent)?;
        if fs::read_dir(&dir)?.next().transpose()?.is_some() {
            return Err(invalid("recovery directory already in use"));
        }
        let journal = Self {
            dir,
            spec: spec.clone(),
            spec_hash: digest,
            trace_locks: trace_locks(),
        };
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(journal.dir.join("LOCK"))?;
        acquire_lock(&lock, "init")?;
        lock.sync_all()?;
        sync_dir(&journal.dir)?;
        journal.publish(
            None,
            Change::Genesis {
                spec: spec.clone(),
                cluster,
            },
            &[],
        )?;
        Ok(journal)
    }

    pub fn open(path: &Path, spec: &Spec) -> Result<Self, JournalError> {
        let journal = Self {
            dir: fs::canonicalize(path)?,
            spec: spec.clone(),
            spec_hash: spec_hash(spec)?,
            trace_locks: trace_locks(),
        };
        // HEAD is replaced by atomic rename after its immutable object is synced.
        // Opening a handle needs to inspect the format and access to LOCK. The
        // first serving operation independently confirms durability under LOCK.
        journal.lock_file()?;
        let head = journal.read_head()?;
        journal.segment(&head)?;
        Ok(journal)
    }

    pub fn spec(&self) -> &Spec {
        &self.spec
    }

    fn lock_file(&self) -> io::Result<File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.dir.join("LOCK"))
    }

    fn lock(&self, operation: &'static str) -> Result<JournalLock, JournalError> {
        let file = self.lock_file()?;
        let start = self.trace_locks.then(Instant::now);
        acquire_lock(&file, operation)?;
        let timing = start.map(|start| {
            let acquired = Instant::now();
            (operation, acquired.duration_since(start), acquired)
        });
        Ok(JournalLock {
            file: Some(file),
            timing,
        })
    }

    fn read_head(&self) -> Result<Head, JournalError> {
        let bytes = read_bounded(&self.dir.join("HEAD"), MAX_HEAD_BYTES)?;
        decode_head(self.spec_hash, &bytes)
    }

    /// Inspect generation ownership during startup while a publisher may hold
    /// LOCK. Only `head` or a successful publication can confirm durable coverage.
    pub fn observed_generation(&self) -> Result<(u128, bool), JournalError> {
        let head = self.read_head()?;
        Ok((head.cluster, head.fenced))
    }

    /// Confirm the small authoritative head. A caller retrying after an ambiguous
    /// final directory sync obtains an independent durability confirmation here.
    pub fn head(&self) -> Result<Head, JournalError> {
        let _guard = self.lock("head")?;
        let head = self.read_head()?;
        sync_dir(&self.dir)?;
        Ok(head)
    }

    pub fn append(&self, expected: &Head, entries: &[Entry]) -> Result<Head, JournalError> {
        if entries.is_empty() || entries.len() > MAX_ENTRIES {
            return Err(invalid("invalid journal entry count"));
        }
        self.compare_publish(expected, Change::Append(entries.len() as u16), entries)
    }

    pub fn fence(&self, expected: &Head) -> Result<Head, JournalError> {
        self.compare_publish(expected, Change::Fence, &[])
    }

    /// Verify that the current fence covers an intact prefix and the proposed
    /// identity has never served this ledger. This check does not reserve the ID.
    pub fn check_replacement(&self, fenced: &Head, new_cluster: u128) -> Result<(), JournalError> {
        self.require_fence(fenced, new_cluster)?;
        self.walk_back(None, fenced, Some(new_cluster), |_, _| Ok(()))?;
        self.require_head(fenced)
    }

    fn require_head(&self, expected: &Head) -> Result<(), JournalError> {
        if self.head()? != *expected {
            return Err(JournalError::Conflict);
        }
        Ok(())
    }

    fn require_fence(&self, fenced: &Head, new_cluster: u128) -> Result<(), JournalError> {
        self.require_head(fenced)?;
        if !fenced.fenced || new_cluster == 0 {
            return Err(invalid(
                "replacement requires a current fence and a nonzero cluster ID",
            ));
        }
        Ok(())
    }

    pub fn activate(&self, fenced: &Head, new_cluster: u128) -> Result<Head, JournalError> {
        self.activate_with(fenced, new_cluster, |_| Ok(()))
    }

    /// Verify each append against the candidate voter without a second journal
    /// traversal. The verifier receives groups newest first, with entries in each
    /// group ordered by ascending Raft index. It must support random access.
    /// Only a successful complete verification permits the final locked fence CAS.
    pub fn activate_with(
        &self,
        fenced: &Head,
        new_cluster: u128,
        mut verify: impl FnMut(&[Entry]) -> Result<(), JournalError>,
    ) -> Result<Head, JournalError> {
        self.activate_encoded_with(None, fenced, new_cluster, |group| verify(group.entries()))
    }

    /// Verify the complete history and freshness of the replacement ID together,
    /// then visit ordered entries for import. Keep imported state private until
    /// success. Check the fence again when publishing a staged voter.
    pub fn visit_replacement(
        &self,
        fenced: &Head,
        new_cluster: u128,
        mut visitor: impl FnMut(&[Entry]) -> Result<(), JournalError>,
    ) -> Result<(), JournalError> {
        self.visit_replacement_encoded(None, fenced, new_cluster, |group| visitor(group.entries()))
    }

    fn compare_publish(
        &self,
        expected: &Head,
        change: Change,
        entries: &[Entry],
    ) -> Result<Head, JournalError> {
        let operation = match change {
            Change::Append(_) => "append",
            Change::Fence => "fence",
            Change::Activate(_) => "activate",
            Change::Genesis { .. } => "init",
        };
        let _guard = self.lock(operation)?;
        if self.read_head()? != *expected {
            return Err(JournalError::Conflict);
        }
        self.publish(Some(expected), change, entries)
    }

    // The caller holds LOCK from the comparison until both publication syncs finish.
    fn publish(
        &self,
        prior: Option<&Head>,
        change: Change,
        entries: &[Entry],
    ) -> Result<Head, JournalError> {
        let (head, bytes) = encode_segment(self.spec_hash, prior, change, entries)?;
        self.publish_object(head, bytes)
    }

    // Caller holds the journal lock.
    fn publish_object(&self, head: Head, bytes: Vec<u8>) -> Result<Head, JournalError> {
        let object = object_path(&self.dir, &head.digest);
        match stage(&self.dir, &bytes)?.persist_noclobber(&object) {
            Ok(_) => (),
            Err(e) if e.error.kind() == io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e.error.into()),
        }
        // Recover a matching orphan left by an interrupted publication. Never
        // replace a pre-existing object under its digest.
        if read_bounded(&object, MAX_SEGMENT_BYTES)? != bytes {
            return Err(invalid("immutable recovery segment readback failed"));
        }
        sync_dir(&self.dir)?;
        #[cfg(test)]
        interruption::check(interruption::Stage::ObjectSynced)?;
        let head_bytes = encode_head(self.spec_hash, &head)?;
        stage(&self.dir, &head_bytes)?
            .persist(self.dir.join("HEAD"))
            .map_err(|e| e.error)?;
        #[cfg(test)]
        interruption::check(interruption::Stage::HeadRenamed)?;
        sync_dir(&self.dir)?;
        Ok(head)
    }

    fn segment(&self, head: &Head) -> Result<Segment, JournalError> {
        let bytes = read_bounded(&object_path(&self.dir, &head.digest), MAX_SEGMENT_BYTES)?;
        decode_segment(self.spec_hash, head, bytes)
    }

    /// Verify the complete published history, then replay bounded entry groups
    /// from genesis. Consumers must publish a restored view only after success.
    pub fn visit(
        &self,
        cut: &Head,
        visitor: impl FnMut(&[Entry]) -> Result<(), JournalError>,
    ) -> Result<(), JournalError> {
        self.replay(None, cut, visitor)
    }

    /// Validate and replay exactly (base, cut]. The caller has previously
    /// verified base. Both endpoints must be on the published branch.
    pub fn visit_since(
        &self,
        base: &Head,
        cut: &Head,
        visitor: impl FnMut(&[Entry]) -> Result<(), JournalError>,
    ) -> Result<(), JournalError> {
        self.replay(Some(base), cut, visitor)
    }

    /// Authenticate (base, cut] newest group first, or the whole prefix for
    /// None. Entries within each group retain Raft order. A supplied base is
    /// a previously verified head. Only a successful return proves both
    /// endpoints belong to the published chain and all comparisons passed.
    pub fn verify_since(
        &self,
        base: Option<&Head>,
        cut: &Head,
        mut verify: impl FnMut(&[Entry]) -> Result<(), JournalError>,
    ) -> Result<(), JournalError> {
        self.verify_since_encoded(base, cut, |group| verify(group.entries()))
    }

    /// Authenticate the published ancestry and compare original encoded entries
    /// in bounded groups, with the newest group first.
    pub fn verify_since_encoded(
        &self,
        base: Option<&Head>,
        cut: &Head,
        mut verify: impl FnMut(&Segment) -> Result<(), JournalError>,
    ) -> Result<(), JournalError> {
        self.require_published_cut(cut)?;
        self.walk_segments(base, cut, None, |_, group| {
            if !group.entries.is_empty() {
                verify(group)?;
            }
            Ok(())
        })
    }

    fn replay(
        &self,
        base: Option<&Head>,
        cut: &Head,
        visitor: impl FnMut(&[Entry]) -> Result<(), JournalError>,
    ) -> Result<(), JournalError> {
        self.require_published_cut(cut)?;
        self.replay_forward(base, cut, None, visitor)
    }

    fn require_published_cut(&self, cut: &Head) -> Result<(), JournalError> {
        // Old cuts remain usable across appends and generation transitions.
        // Never accept a fully written object left behind before its HEAD rename.
        let mut cursor = self.head()?;
        while cursor != *cut {
            if cursor.index < cut.index {
                return Err(invalid("cut is outside published journal history"));
            }
            cursor = self
                .segment(&cursor)?
                .prior
                .ok_or_else(|| invalid("unpublished journal cut"))?;
        }
        Ok(())
    }

    fn walk_back(
        &self,
        base: Option<&Head>,
        cut: &Head,
        excluded_cluster: Option<u128>,
        mut visit_append: impl FnMut(&Head, &[Entry]) -> Result<(), JournalError>,
    ) -> Result<(), JournalError> {
        self.walk_segments(base, cut, excluded_cluster, |head, entries| {
            if entries.entries.is_empty() {
                Ok(())
            } else {
                visit_append(head, &entries.entries)
            }
        })
    }

    fn walk_segments(
        &self,
        base: Option<&Head>,
        cut: &Head,
        excluded_cluster: Option<u128>,
        mut visit: impl FnMut(&Head, &Segment) -> Result<(), JournalError>,
    ) -> Result<(), JournalError> {
        let mut cursor = cut.clone();
        while base != Some(&cursor) {
            if excluded_cluster == Some(cursor.cluster) {
                return Err(invalid(
                    "replacement cluster ID already occurs in journal history",
                ));
            }
            if base.is_some_and(|base| base.index > cursor.index) {
                return Err(invalid("base is outside requested journal history"));
            }
            let segment = self.segment(&cursor)?;
            visit(&cursor, &segment)?;
            match segment.prior {
                Some(prior) => cursor = prior,
                None if base.is_none() => break,
                None => return Err(invalid("base is outside requested journal history")),
            }
        }
        Ok(())
    }

    fn replay_forward(
        &self,
        base: Option<&Head>,
        cut: &Head,
        excluded_cluster: Option<u128>,
        mut visitor: impl FnMut(&[Entry]) -> Result<(), JournalError>,
    ) -> Result<(), JournalError> {
        self.replay_tracking(
            base,
            cut,
            excluded_cluster,
            |_| Ok(()),
            |s| visitor(&s.entries),
        )
    }

    fn replay_tracking(
        &self,
        base: Option<&Head>,
        cut: &Head,
        excluded_cluster: Option<u128>,
        mut generation: impl FnMut(u128) -> Result<(), JournalError>,
        mut visitor: impl FnMut(&Segment) -> Result<(), JournalError>,
    ) -> Result<(), JournalError> {
        if base == Some(cut) {
            return Ok(());
        }
        // Authenticate every reverse link before invoking the first import callback.
        // The local spool holds one fixed-width head per append, independent of payload size.
        let mut order = tempfile::tempfile()?;
        let width = bincode::serialized_size(cut)?;
        let mut end = 0u64;
        {
            let mut spool = BufWriter::new(&mut order);
            self.walk_segments(base, cut, excluded_cluster, |head, entries| {
                generation(head.cluster)?;
                if entries.entries.is_empty() {
                    return Ok(());
                }
                bincode::serialize_into(&mut spool, head)?;
                end = end
                    .checked_add(width)
                    .ok_or_else(|| invalid("replay order too large"))?;
                Ok(())
            })?;
            spool.flush()?;
        }
        let mut descriptor = vec![0; width as usize];
        while end > 0 {
            end -= width;
            order.seek(SeekFrom::Start(end))?;
            order.read_exact(&mut descriptor)?;
            let cursor: Head = bincode::deserialize(&descriptor)?;
            visitor(&self.segment(&cursor)?)?;
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod interruption {
    use std::{cell::Cell, io};

    #[derive(Clone, Copy, Eq, PartialEq)]
    pub(crate) enum Stage {
        ObjectSynced,
        HeadRenamed,
        BaseObjectSynced,
        BaseRenamed,
    }

    thread_local! {
        static NEXT: Cell<Option<Stage>> = const { Cell::new(None) };
    }

    pub(crate) fn inject(stage: Stage) {
        NEXT.set(Some(stage));
    }

    pub(super) fn check(stage: Stage) -> io::Result<()> {
        if NEXT.get() == Some(stage) {
            NEXT.set(None);
            return Err(io::Error::other("interrupted journal publication"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod base_tests;

#[cfg(test)]
mod encoded_tests;
