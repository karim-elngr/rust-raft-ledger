use std::fs::{self, File};
use std::io::{ErrorKind, Read};
use std::path::Path;
use std::sync::Arc;

use ledger_core::{EntryId, LedgerId};
use rocksdb::{DB, IteratorMode, checkpoint::Checkpoint};

use super::{
    APPLIED, AccountCache, ENTRIES, LAST_ENTRY, LEDGER_ID, Projection, ReplicaError, invalid, open,
};
use crate::{CacheBudget, ProposalBatch};

fn check_current(path: &Path) -> Result<(), ReplicaError> {
    // Keep RocksDB's first metadata lookup inside this verified directory.
    let current_file = File::open(path.join("CURRENT"))?;
    if current_file.metadata()?.len() > 64 {
        return Err(invalid("oversized RocksDB CURRENT file"));
    }
    let mut current = String::new();
    current_file.take(64).read_to_string(&mut current)?;
    let name = current
        .strip_suffix('\n')
        .and_then(|s| s.strip_prefix("MANIFEST-"))
        .filter(|n| {
            !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) && n.parse::<u64>().is_ok()
        })
        .ok_or_else(|| invalid("invalid RocksDB CURRENT manifest name"))?;
    if !fs::symlink_metadata(path.join(format!("MANIFEST-{name}")))?.is_file() {
        return Err(invalid("invalid RocksDB manifest file"));
    }
    Ok(())
}

fn read_marker<const N: usize>(db: &DB, key: &[u8]) -> Result<[u8; N], ReplicaError> {
    db.get_pinned(key)?
        .and_then(|bytes| bytes.as_ref().try_into().ok())
        .ok_or_else(|| invalid("missing or invalid checkpoint frontier"))
}

fn frontiers(
    db: &DB,
    id: LedgerId,
    expected_index: Option<u64>,
) -> Result<(u64, EntryId), ReplicaError> {
    let applied = u64::from_be_bytes(read_marker(db, APPLIED)?);
    let last = u64::from_be_bytes(read_marker(db, LAST_ENTRY)?);
    if applied == 0
        || expected_index.is_some_and(|expected| expected != applied)
        || u128::from_be_bytes(read_marker(db, LEDGER_ID)?) != id.0
        || last > applied.saturating_mul(ProposalBatch::MAX_COMMANDS as u64)
    {
        return Err(invalid(
            "checkpoint frontiers or ledger identity do not match",
        ));
    }
    let entries = db
        .cf_handle(ENTRIES)
        .ok_or_else(|| invalid("missing checkpoint journal index"))?;
    let highest = db
        .iterator_cf(entries, IteratorMode::End)
        .next()
        .transpose()?;
    match highest {
        None if last == 0 => Ok((applied, EntryId(0))),
        Some((key, pointer)) if last != 0 && key.as_ref() == last.to_be_bytes() => {
            let pos: super::Position = pointer
                .as_ref()
                .try_into()
                .map_err(|_| invalid("invalid checkpoint journal pointer"))?;
            let index = u64::from_be_bytes(pos[..8].try_into().expect("fixed position width"));
            let slot = u16::from_be_bytes(pos[8..].try_into().expect("fixed position width"));
            if index == 0 || index > applied || usize::from(slot) >= ProposalBatch::MAX_COMMANDS {
                return Err(invalid(
                    "checkpoint journal pointer outside the applied prefix",
                ));
            }
            let history = db
                .cf_handle(super::HISTORY)
                .expect("opened all projection families");
            let bytes = db
                .get_pinned_cf(history, pos)?
                .ok_or_else(|| invalid("missing checkpoint journal history"))?;
            let decision: ledger_core::Decision =
                bincode::Options::deserialize(super::format(), &bytes)?;
            if super::journal_in(&decision).is_none_or(|entry| entry.id.0 != last) {
                return Err(invalid("checkpoint entry index points to another decision"));
            }
            Ok((applied, EntryId(last)))
        }
        _ => Err(invalid(
            "checkpoint journal frontier differs from stored entry index",
        )),
    }
}

fn read_only(
    path: &Path,
    id: LedgerId,
    expected_index: Option<u64>,
) -> Result<(u64, EntryId), ReplicaError> {
    check_current(path)?;
    let logs = tempfile::tempdir()?;
    let mut options = open::options(0);
    // Read-only RocksDB may emit diagnostic logs. Keep the image byte-stable.
    options.set_db_log_dir(logs.path());
    open::check_families(path, &options)?;
    let db = DB::open_cf_descriptors_read_only(&options, path, open::families(&options), false)?;
    frontiers(&db, id, expected_index)
}

impl Projection {
    pub(crate) fn validate_checkpoint_dir(
        path: &Path,
        id: LedgerId,
        index: u64,
    ) -> Result<(), ReplicaError> {
        read_only(path, id, Some(index)).map(|_| ())
    }

    /// Opens a private writable clone for a bounded snapshot replay. The sealed
    /// source remains pinned and only immutable tables may be linked to it.
    pub(crate) fn open_replay_copy(
        path: &Path,
        id: LedgerId,
        index: u64,
    ) -> Result<Self, ReplicaError> {
        open::check_directory(path, true)?;
        check_current(path)?;
        let options = open::options(0);
        open::check_families(path, &options)?;
        let db = DB::open_cf_descriptors(&options, path, open::families(&options))?;
        let (_, last) = frontiers(&db, id, Some(index))?;
        Ok(Self::loaded(db, id, AccountCache::new(0)?, index, last))
    }

    pub(crate) fn working_root(&self) -> &Path {
        self.db
            .path()
            .parent()
            .expect("working RocksDB is under a state directory")
    }

    pub(crate) fn discard_working(path: &Path, id: LedgerId) -> Result<(), ReplicaError> {
        if path.try_exists()? {
            open::Working::prepare(path, id)?.discard_data()?;
        }
        Ok(())
    }

    /// Capture an all-family image at a published non-genesis boundary. The
    /// caller seals and authenticates the resulting files before publication.
    pub(crate) fn capture(&self, destination: &Path) -> Result<(), ReplicaError> {
        if self.applied == 0 {
            return Err(invalid("cannot capture an empty Raft prefix"));
        }
        let cut = Self::capture_db(&self.db, self.id, destination)?;
        if cut != (self.applied, self.last_entry) {
            return Err(invalid("sealed frontier differs from projection"));
        }
        Ok(())
    }

    pub(crate) fn checkpoint_db(&self) -> Arc<DB> {
        self.db.clone()
    }

    pub(crate) fn capture_db(
        db: &DB,
        id: LedgerId,
        destination: &Path,
    ) -> Result<(u64, EntryId), ReplicaError> {
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let name = destination
            .file_name()
            .ok_or_else(|| invalid("invalid checkpoint destination"))?;
        let destination = fs::canonicalize(parent)?.join(name);
        let state = db
            .path()
            .parent()
            .ok_or_else(|| invalid("invalid projection database path"))?;
        if destination.starts_with(fs::canonicalize(state)?) {
            return Err(invalid("checkpoint must be outside working state"));
        }
        // RocksDB internally clears <destination>.tmp before capturing. Never
        // let that clean up a path that the caller already owns.
        let mut temp = destination.as_os_str().to_owned();
        temp.push(".tmp");
        for target in [destination.as_path(), Path::new(&temp)] {
            match fs::symlink_metadata(target) {
                Err(err) if err.kind() == ErrorKind::NotFound => (),
                Ok(_) => {
                    return Err(invalid(
                        "checkpoint destination or staging name already exists",
                    ));
                }
                Err(err) => return Err(err.into()),
            }
        }
        // The pinned RocksDB binding requests a flush of all column families.
        // Atomic flush and the single WriteBatch per entry keep the frontier in
        // the image consistent even while its owner applies subsequent entries.
        Checkpoint::new(db)?.create_checkpoint(&destination)?;
        open::check_directory(&destination, false)?;
        read_only(&destination, id, None)
    }

    /// Materialize writes only flat RocksDB files into the supplied empty
    /// directory. Immutable tables/blob files may share inodes with the seal.
    /// Mutable files must be private. A failed or interrupted install is
    /// discarded on the next attempt; the Raft store remains authoritative.
    pub(crate) fn from_checkpoint(
        state_path: &Path,
        id: LedgerId,
        expected_index: u64,
        cache: CacheBudget,
        materialize: impl FnOnce(&Path) -> Result<(), ReplicaError>,
    ) -> Result<Self, ReplicaError> {
        if expected_index == 0 {
            return Err(invalid("checkpoint requires a nonzero Raft index"));
        }
        let decoded = AccountCache::new(cache.account_bytes)?;
        let layout = open::Working::prepare(state_path, id)?;
        fs::create_dir(&layout.incoming)?;
        materialize(&layout.incoming)?;
        open::check_directory(&layout.incoming, true)?;
        let (_, last) = read_only(&layout.incoming, id, Some(expected_index))?;
        // Validate the private clone’s schema and frontiers before replacement.
        layout.discard_data()?;
        fs::rename(&layout.incoming, &layout.data)?;
        File::open(state_path)?.sync_all()?;

        let options = open::options(cache.block_bytes);
        let db = DB::open_cf_descriptors(&options, &layout.data, open::families(&options))?;
        if frontiers(&db, id, Some(expected_index))? != (expected_index, last) {
            return Err(invalid(
                "installed journal frontier differs from checkpoint",
            ));
        }
        Ok(Self::loaded(db, id, decoded, expected_index, last))
    }
}
