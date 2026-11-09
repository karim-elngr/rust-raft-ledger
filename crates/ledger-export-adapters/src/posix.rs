use ledger_export::{ArchiveHead, ExportError, Identity, RawEntry, RecoveryArchive, Result};
use ledger_recovery::{
    EncodedCursor, Head, Journal, JournalError, MAX_ENTRIES, MAX_ENTRY_BYTES, MAX_SEGMENT_BYTES,
    Spec,
};
use protobuf::Message;
use raft::eraftpb::Entry;
use std::{
    fs, io,
    path::{Component, Path, PathBuf},
    sync::Mutex,
};

fn storage_path(path: &Path, resolve_links: bool) -> io::Result<PathBuf> {
    if resolve_links {
        match fs::canonicalize(path) {
            Ok(real) => return Ok(real),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    let mut out = PathBuf::new();
    for component in std::path::absolute(path)?.components() {
        match component {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            part => {
                out.push(part);
                if resolve_links {
                    match fs::canonicalize(&out) {
                        Ok(real) => out = real,
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {
                            // A missing leaf can be initialized later. A dangling
                            // ancestor has an unknown physical destination.
                            match fs::symlink_metadata(&out) {
                                Err(m) if m.kind() == io::ErrorKind::NotFound => {}
                                Ok(_) => return Err(e),
                                Err(m) => return Err(m),
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }
    }
    Ok(out)
}

fn check_separation(path: &Path, data: &Path, resolve_links: bool) -> Result<PathBuf> {
    let resolve = |p: &Path| {
        storage_path(p, resolve_links)
            .map_err(|e| ExportError::Unavailable(format!("cannot resolve storage path: {e}")))
    };
    let archive = resolve(path)?;
    let data = resolve(data)?;
    if archive.starts_with(&data) || data.starts_with(&archive) {
        return Err(ExportError::Invalid(
            "archive and data_dir must be separate directory trees".into(),
        ));
    }
    Ok(archive)
}

fn convert(err: JournalError) -> ExportError {
    match err {
        JournalError::Conflict | JournalError::Io(_) => ExportError::Unavailable(err.to_string()),
        _ => ExportError::Invalid(err.to_string()),
    }
}
fn tip(h: &Head) -> ArchiveHead {
    ArchiveHead {
        index: h.index,
        term: h.term,
        digest: h.digest,
    }
}

struct Scan {
    head: ArchiveHead,
    next: u64,
    cursor: EncodedCursor,
}
#[derive(Default)]
struct State {
    journal: Option<(PathBuf, Journal)>,
    scan: Option<Scan>,
}
/// Reads and writes the same checksum-addressed chain as the recovery CLI.
pub struct PosixArchive {
    path: PathBuf,
    replica_dir: Option<PathBuf>,
    destination: String,
    identity: Identity,
    state: Mutex<State>,
}
impl PosixArchive {
    /// Defers mounting/opening the pre-initialized journal until a worker poll.
    pub fn open(path: &Path, identity: &Identity) -> Result<Self> {
        let path = std::path::absolute(path).map_err(|e| ExportError::Invalid(e.to_string()))?;
        let destination = format!(
            "posix:{}",
            path.to_str()
                .ok_or_else(|| ExportError::Invalid("archive path is not UTF-8".into()))?
        );
        Ok(Self {
            path,
            replica_dir: None,
            destination,
            identity: identity.clone(),
            state: Mutex::default(),
        })
    }

    /// Configure a learner's archive without accessing either storage tree.
    pub fn for_replica(path: &Path, data: &Path, identity: &Identity) -> Result<Self> {
        Self::check_configured_paths(path, data)?;
        let mut archive = Self::open(path, identity)?;
        archive.replica_dir =
            Some(std::path::absolute(data).map_err(|e| ExportError::Invalid(e.to_string()))?);
        Ok(archive)
    }

    /// Check configured storage paths before starting workers. This comparison
    /// is lexical so an inaccessible mount cannot delay the Raft runtime.
    pub fn check_configured_paths(path: &Path, data: &Path) -> Result<()> {
        check_separation(path, data, false).map(|_| ())
    }

    /// Resolve storage paths on the archive worker or before an offline CLI
    /// operation. The returned path is suitable for opening or initializing
    /// the archive, including a new leaf under a mounted directory.
    pub fn resolve_separate_path(path: &Path, data: &Path) -> Result<PathBuf> {
        check_separation(path, data, true)
    }
    fn state(&self) -> Result<std::sync::MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| ExportError::Unavailable("archive worker interrupted".into()))
    }
    fn active(&self, s: &mut State, id: &Identity) -> Result<(Journal, Head)> {
        if *id != self.identity {
            return Err(ExportError::Invalid("archive identity mismatch".into()));
        }
        let path = match &self.replica_dir {
            Some(data) => Self::resolve_separate_path(&self.path, data)?,
            None => self.path.clone(),
        };
        // A mount can move while this worker is alive. Re-open and revalidate
        // the journal identity when a legitimate alias points elsewhere.
        if s.journal.as_ref().is_none_or(|(old, _)| *old != path) {
            s.scan = None;
            let journal = Journal::open(
                &path,
                &Spec {
                    ledger: id.ledger.0,
                    asset: id.asset.clone(),
                    scale: id.scale,
                },
            )
            .map_err(convert)?;
            s.journal = Some((path, journal));
        }
        let journal = s.journal.as_ref().expect("open journal").1.clone();
        let head = journal.head().map_err(convert)?;
        if head.fenced || head.cluster != id.generation {
            return Err(ExportError::Invalid(
                "archive generation is fenced or different".into(),
            ));
        }
        Ok((journal, head))
    }
}
impl RecoveryArchive for PosixArchive {
    fn destination(&self) -> &str {
        &self.destination
    }
    fn head(&self, id: &Identity) -> Result<ArchiveHead> {
        let mut state = self.state()?;
        self.active(&mut state, id).map(|(_, h)| tip(&h))
    }
    fn append(
        &self,
        id: &Identity,
        expected: &ArchiveHead,
        rows: &[RawEntry],
    ) -> Result<ArchiveHead> {
        let mut state = self.state()?;
        let (journal, mut head) = self.active(&mut state, id)?;
        if tip(&head) != *expected {
            return Err(convert(JournalError::Conflict));
        }
        let Some(first) = rows.first() else {
            return Ok(*expected);
        };
        if rows.len() > MAX_ENTRIES
            || rows
                .iter()
                .try_fold(0usize, |n, row| n.checked_add(row.bytes.len()))
                .is_none_or(|n| n > ledger_export::MAX_RAW_BYTES)
            || first.index == 0
            || first.index > head.index.saturating_add(1)
            || rows
                .windows(2)
                .any(|w| w[0].index.checked_add(1) != Some(w[1].index) || w[0].term > w[1].term)
        {
            return Err(ExportError::Invalid(
                "noncontiguous or oversized archive page".into(),
            ));
        }
        for row in rows {
            if row.bytes.len() > MAX_ENTRY_BYTES || row.term == 0 {
                return Err(ExportError::Invalid(
                    "unsupported archive entry size or term".into(),
                ));
            }
            let entry = Entry::parse_from_bytes(&row.bytes)
                .map_err(|e| ExportError::Invalid(e.to_string()))?;
            if (entry.index, entry.term) != (row.index, row.term) {
                return Err(ExportError::Invalid(
                    "entry fields disagree with original bytes".into(),
                ));
            }
        }
        let overlap = rows.partition_point(|r| r.index <= head.index);
        if overlap > 0 {
            let mut scan = match state.scan.take() {
                Some(s) if s.head == *expected && s.next == first.index => s,
                _ => Scan {
                    head: *expected,
                    next: first.index,
                    cursor: journal
                        .encoded_cursor(&head, first.index)
                        .map_err(convert)?,
                },
            };
            for row in &rows[..overlap] {
                if scan.cursor.next_entry().map_err(convert)?.as_deref() != Some(&row.bytes) {
                    return Err(ExportError::Invalid(format!(
                        "archived Raft entry differs at index {}",
                        row.index
                    )));
                }
                scan.next = scan.next.saturating_add(1);
            }
            state.scan = Some(scan);
        }
        if overlap == rows.len() {
            if journal.head().map_err(convert)? != head {
                return Err(convert(JournalError::Conflict));
            }
        } else {
            state.scan = None;
            // Source pages and the journal both bound payloads, but the latter
            // also accounts for its header and per-row length prefix.
            let mut suffix = &rows[overlap..];
            while !suffix.is_empty() {
                let mut available = MAX_SEGMENT_BYTES - 512;
                let chunk = suffix
                    .iter()
                    .take_while(|r| {
                        if let Some(rest) = available.checked_sub(4 + r.bytes.len()) {
                            available = rest;
                            true
                        } else {
                            false
                        }
                    })
                    .map(|r| r.bytes.as_slice())
                    .collect::<Vec<_>>();
                head = journal.append_encoded(&head, &chunk).map_err(convert)?;
                suffix = &suffix[chunk.len()..];
            }
        }
        Ok(tip(&head))
    }
}
