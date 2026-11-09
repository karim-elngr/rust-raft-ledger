//! Process-local exclusive ownership, checksummed encoding and POSIX durable
//! rename. A separate cursor is retained for every destination and port.
use super::*;
use bincode::Options;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read, Write},
    path::{Path, PathBuf},
};

const MAGIC: &[u8; 8] = b"LDEXCUR1";
const MAX_CURSOR: u64 = 32 * 1024;

fn unavailable(e: std::io::Error) -> ExportError {
    ExportError::Unavailable(e.to_string())
}
fn bad(s: &str) -> ExportError {
    ExportError::Invalid(s.into())
}

#[derive(Serialize, Deserialize)]
struct Record {
    identity: Identity,
    destination: [u8; 32],
    at: Position,
    witness: Option<ArchiveHead>,
    local_boundary: Option<[u8; 32]>,
}

pub(super) struct Cursor {
    directory: PathBuf,
    path: PathBuf,
    _lock: File,
    record: Record,
}

fn ensure_dir(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(bad("cursor path must be a directory")),
        Err(e) if e.kind() == ErrorKind::NotFound => {
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            ensure_dir(parent)?;
            match fs::create_dir(path) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::AlreadyExists => ensure_dir(path)?,
                Err(e) => return Err(unavailable(e)),
            }
            File::open(parent)
                .and_then(|f| f.sync_all())
                .map_err(unavailable)
        }
        Err(e) => Err(unavailable(e)),
    }
}
fn require_regular(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(m) if m.is_file() => Ok(true),
        Ok(_) => Err(bad("expected a regular export cursor or lock")),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
        Err(e) => Err(unavailable(e)),
    }
}
impl Cursor {
    pub(super) fn open(
        directory: &Path,
        identity: &Identity,
        kind: &str,
        dest: &str,
    ) -> Result<Self> {
        if dest.is_empty() || dest.len() > 4096 {
            return Err(bad("invalid export destination identifier"));
        }
        ensure_dir(directory)?;
        let mut hash = Sha256::new();
        hash.update(b"ledger-export/destination/v1\0");
        hash.update(kind.as_bytes());
        hash.update([0]);
        hash.update(dest.as_bytes());
        let destination: [u8; 32] = hash.finalize().into();
        let key = destination
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let path = directory.join(format!("{kind}-{key}.cursor"));
        let lock_path = directory.join(format!("{kind}-{key}.lock"));
        require_regular(&lock_path)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(unavailable)?;
        lock.try_lock().map_err(|e| {
            ExportError::Unavailable(format!("cannot acquire export cursor lock: {e}"))
        })?;
        let initial = Record {
            identity: identity.clone(),
            destination,
            at: Position::after_entry(0),
            witness: None,
            local_boundary: None,
        };
        let record = if require_regular(&path)? {
            let f = File::open(&path).map_err(unavailable)?;
            let mut bytes = Vec::new();
            f.take(MAX_CURSOR + 1)
                .read_to_end(&mut bytes)
                .map_err(unavailable)?;
            if bytes.len() as u64 > MAX_CURSOR || bytes.len() < 40 || !bytes.starts_with(MAGIC) {
                return Err(bad("unrecognized export cursor"));
            }
            let (body, checksum) = bytes.split_at(bytes.len() - 32);
            if &Sha256::digest(body)[..] != checksum {
                return Err(bad("export cursor checksum mismatch"));
            }
            let r: Record = bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_limit(MAX_CURSOR)
                .reject_trailing_bytes()
                .deserialize(&body[8..])
                .map_err(|_| bad("invalid export cursor encoding"))?;
            if r.identity != *identity || r.destination != destination {
                return Err(bad(
                    "cursor ledger, generation, asset, scale or destination mismatch",
                ));
            }
            if !r.at.valid()
                || (kind == "archive"
                    && (r.at.slot != u16::MAX
                        || r.witness.is_some_and(|h| h.index < r.at.index)
                        || (r.at.index > 0 && r.witness.is_none())
                        || (r.at.index > 0) != r.local_boundary.is_some()))
                || (kind == "events" && (r.witness.is_some() || r.local_boundary.is_some()))
            {
                return Err(bad("invalid export cursor position or witness"));
            }
            r
        } else {
            initial
        };
        let cursor = Self {
            directory: directory.to_owned(),
            path,
            _lock: lock,
            record,
        };
        if !require_regular(&cursor.path)? {
            cursor.write(&cursor.record)?;
        }
        Ok(cursor)
    }
    pub(super) fn position(&self) -> Position {
        self.record.at
    }
    pub(super) fn witness(&self) -> Option<ArchiveHead> {
        self.record.witness
    }
    pub(super) fn local_boundary(&self) -> Option<[u8; 32]> {
        self.record.local_boundary
    }
    fn write(&self, r: &Record) -> Result<()> {
        let mut bytes = MAGIC.to_vec();
        bytes.extend(bincode::serialize(r).map_err(|_| bad("invalid export cursor data"))?);
        if bytes.len() + 32 > MAX_CURSOR as usize {
            return Err(bad("export cursor exceeds size limit"));
        }
        bytes.extend_from_slice(&Sha256::digest(&bytes));
        let mut tmp = tempfile::NamedTempFile::new_in(&self.directory).map_err(unavailable)?;
        tmp.write_all(&bytes).map_err(unavailable)?;
        tmp.as_file().sync_all().map_err(unavailable)?;
        tmp.persist(&self.path).map_err(|e| unavailable(e.error))?;
        File::open(&self.directory)
            .and_then(|f| f.sync_all())
            .map_err(unavailable)
    }
    pub(super) fn save(
        &mut self,
        at: Position,
        witness: Option<ArchiveHead>,
        local_boundary: Option<[u8; 32]>,
    ) -> Result<()> {
        if at < self.record.at {
            return Err(bad("export cursor would move backwards"));
        }
        let next = Record {
            identity: self.record.identity.clone(),
            destination: self.record.destination,
            at,
            witness,
            local_boundary,
        };
        self.write(&next)?;
        self.record = next;
        Ok(())
    }
}
