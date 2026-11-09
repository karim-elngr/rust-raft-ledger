//! Offline handoff of duplicated voter-local Raft history to an exact,
//! persistently pinned independent image. The checkpoint boundary row remains
//! local; old raw reads open the pinned image only on demand.
use super::*;
use crate::recovery::{
    base::{open_image, pin_archive, release_archive},
    base_image::PortableImage,
};
use bincode::Options as _;
use ledger_recovery::{Journal, RecoveryBase, Spec};
use rocksdb::{BottommostLevelCompaction, CompactOptions};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

pub(super) const COLD: &[u8] = b"reclaimed-history/v1";
const RELEASE: &[u8] = b"reclaim-release/v1";
const MAX_RECORD: usize = 32 << 10;

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct ReclaimStats {
    /// Replication floor. Its matching entry stays local.
    pub through: u64,
    pub reclaimed_entries: u64,
    /// Encoded entries handed off by this invocation, before SST compression.
    pub reclaimed_bytes: u64,
    pub live_sst_bytes_before: u64,
    pub live_sst_bytes_after: u64,
}
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
struct Location {
    directory: PathBuf,
    spec: Spec,
    base: RecoveryBase,
}
impl Location {
    fn shares_pin(&self, other: &Self) -> Result<bool, ReplicaError> {
        if self.base.manifest != other.base.manifest {
            return Ok(false);
        }
        if self.directory == other.directory {
            return Ok(true);
        }
        // Different mount paths may name the same POSIX image registry. Pins
        // are keyed by manifest digest and voter identity, not by mount path.
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let a = std::fs::metadata(self.directory.join("recovery-images-v1"))?;
            let b = std::fs::metadata(other.directory.join("recovery-images-v1"))?;
            Ok((a.dev(), a.ino()) == (b.dev(), b.ino()))
        }
        #[cfg(not(unix))]
        {
            Ok(std::fs::canonicalize(&self.directory)? == std::fs::canonicalize(&other.directory)?)
        }
    }
    fn journal(&self) -> Result<Journal, ReplicaError> {
        Journal::open(&self.directory, &self.spec)
            .map_err(|e| invalid(format!("independent history: {e}")))
    }
    fn open(&self) -> Result<Reader, ReplicaError> {
        let journal = self.journal()?;
        let image = open_image(&journal, &self.base)?;
        let path = image.verified_raft_dir()?;
        let logs = tempfile::tempdir()?;
        let mut options = db_options(false);
        options.set_db_log_dir(logs.path());
        let db = DB::open_for_read_only(&options, path, false)?;
        RaftStore::validate_neutral(&db, &self.spec, &self.base.source)?;
        Ok(Reader {
            db,
            _image: image,
            _logs: logs,
        })
    }
}
pub(super) struct Reader {
    pub(super) db: DB,
    _image: PortableImage,
    _logs: tempfile::TempDir,
}
pub(super) struct Cold {
    location: Location,
    reader: Mutex<Option<Arc<Reader>>>,
}
impl Cold {
    pub(super) fn through(&self) -> u64 {
        self.location.base.source.index
    }
    pub(super) fn reader(&self) -> Result<Arc<Reader>, ReplicaError> {
        let mut open = self
            .reader
            .lock()
            .map_err(|_| invalid("independent history reader lock poisoned"))?;
        if let Some(reader) = &*open {
            return Ok(reader.clone());
        }
        let reader = Arc::new(self.location.open()?);
        *open = Some(reader.clone());
        Ok(reader)
    }
}
#[derive(Default)]
pub(super) struct ColdState {
    current: RwLock<Option<Arc<Cold>>>,
    writer: Mutex<()>,
    uncertain: std::sync::atomic::AtomicBool,
}
impl ColdState {
    pub(super) fn with_view<'a>(
        &self,
        db: &'a DB,
    ) -> Result<(Option<Arc<Cold>>, rocksdb::Snapshot<'a>), ReplicaError> {
        let active = self
            .current
            .read()
            .map_err(|_| invalid("cold history lock poisoned"))?;
        Ok((active.clone(), db.snapshot()))
    }
    pub(super) fn term(&self) -> Result<Option<u64>, ReplicaError> {
        Ok(self.current()?.map(|c| c.location.base.source.term))
    }
    pub(super) fn load(db: &DB, identity: &[u8]) -> Result<Self, ReplicaError> {
        let current = read_location(db, COLD, identity)?.map(|location| {
            Arc::new(Cold {
                location,
                reader: Mutex::default(),
            })
        });
        Ok(Self {
            current: RwLock::new(current),
            writer: Mutex::default(),
            uncertain: Default::default(),
        })
    }
    pub(super) fn current(&self) -> Result<Option<Arc<Cold>>, ReplicaError> {
        Ok(self
            .current
            .read()
            .map_err(|_| invalid("cold history lock poisoned"))?
            .clone())
    }
    pub(super) fn through(&self) -> Result<u64, ReplicaError> {
        Ok(self.current()?.map_or(0, |c| c.through()))
    }
}
fn checksum(key: &[u8], identity: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(key);
    h.update(Sha256::digest(identity));
    h.update(bytes);
    h.finalize().into()
}
fn signed(key: &[u8], identity: &[u8], loc: &Location) -> Result<Vec<u8>, ReplicaError> {
    let mut bytes = bincode::serialize(loc)?;
    if bytes.len() + 32 > MAX_RECORD {
        return Err(invalid("reclamation dependency exceeds storage limit"));
    }
    bytes.extend(checksum(key, identity, &bytes));
    Ok(bytes)
}
fn read_location(db: &DB, key: &[u8], identity: &[u8]) -> Result<Option<Location>, ReplicaError> {
    let Some(bytes) = db.get_pinned(key)? else {
        return Ok(None);
    };
    if !(33..=MAX_RECORD).contains(&bytes.len()) {
        return Err(invalid("invalid reclamation record length"));
    }
    let (body, sum) = bytes.split_at(bytes.len() - 32);
    if checksum(key, identity, body) != sum {
        return Err(invalid(
            "invalid reclamation dependency checksum or identity",
        ));
    }
    let loc: Location = bincode::options()
        .with_fixint_encoding()
        .with_limit(MAX_RECORD as u64)
        .reject_trailing_bytes()
        .deserialize(body)?;
    let source = &loc.base.source;
    if !loc.directory.is_absolute()
        || source.fenced
        || source.cluster == 0
        || source.index == 0
        || source.index == u64::MAX
        || source.term == 0
    {
        return Err(invalid("invalid independent history dependency"));
    }
    Ok(Some(loc))
}

impl RaftStore {
    pub(crate) fn reclaimed_through(&self) -> Result<u64, ReplicaError> {
        self.cold.through()
    }
    fn reconcile_release(&self, identity: &[u8]) -> Result<(), ReplicaError> {
        if let Some(loc) = read_location(&self.db, RELEASE, identity)? {
            let active = self.cold.current()?;
            if !active
                .as_ref()
                .map(|c| c.location.shares_pin(&loc))
                .transpose()?
                .unwrap_or(false)
            {
                release_archive(&loc.journal()?, &loc.base, identity)?;
            }
            let mut batch = WriteBatch::default();
            batch.delete(RELEASE);
            write_sync(&self.db, batch)?;
        }
        Ok(())
    }
    fn live_sst_bytes(&self) -> Result<u64, ReplicaError> {
        self.db
            .property_int_value("rocksdb.live-sst-files-size")?
            .ok_or_else(|| invalid("RocksDB did not report the size of live SST files"))
    }

    /// The image was rebuilt from this published prefix and every included
    /// local raw byte was just compared. Preserve that evidence at the one
    /// boundary row that remains local, before removing the older rows.
    fn preserve_anchor(
        &self,
        journal: &Journal,
        cluster: u128,
        source: &ledger_recovery::Head,
    ) -> Result<(), ReplicaError> {
        let prior = self.journal_anchor(cluster)?;
        if let Some(saved) = &prior {
            if saved == source {
                return Ok(());
            }
            let (earlier, later) = if saved.index < source.index {
                (saved, source)
            } else {
                (source, saved)
            };
            journal
                .verify_since(Some(earlier), later, |_| Ok(()))
                .map_err(|e| {
                    invalid(format!(
                        "saved journal anchor is outside base ancestry: {e}"
                    ))
                })?;
            if saved.index >= source.index {
                return Ok(());
            }
        }
        if source.cluster == cluster {
            self.save_journal_anchor(cluster, prior.as_ref(), source)?;
        }
        Ok(())
    }

    // Check exact canonical bytes, even when the previous handoff now serves
    // part of the range. No vote, commit, or image pointer is changed on failure.
    fn compare_archive(
        &self,
        reader: &Reader,
        through: u64,
        old: u64,
    ) -> Result<u64, ReplicaError> {
        let archived = self.metadata()?.archived;
        let mut size = 0u64;
        let mut term = 0;
        let mut entries = reader.db.iterator(IteratorMode::Start);
        for index in 1..=through {
            let (key, bytes) = entries
                .next()
                .transpose()?
                .ok_or_else(|| invalid("incomplete independent raw history"))?;
            if key.as_ref() != entry_key(index)
                || bytes.as_ref() != self.raw_bytes(index, archived)?
            {
                return Err(invalid(
                    "independent Raft bytes differ from local committed history",
                ));
            }
            let row = decode_entry(index, &bytes)?;
            if row.term < term {
                return Err(invalid("independent history term regressed"));
            }
            term = row.term;
            if index >= old.max(1) && index < through {
                size = size
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| invalid("reclaimed history size overflow"))?;
            }
        }
        Ok(size)
    }

    pub(crate) fn reclaim(
        &self,
        journal: &Journal,
        cluster: u128,
        checkpoint: CheckpointRef,
    ) -> Result<ReclaimStats, ReplicaError> {
        let _writer = self
            .cold
            .writer
            .lock()
            .map_err(|_| invalid("reclamation writer lock poisoned"))?;
        if self
            .cold
            .uncertain
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(invalid(
                "reopen voter to resolve an ambiguous reclaim WAL write",
            ));
        }
        let identity = required(&self.db, b"identity/v1")?;
        self.reconcile_release(&identity)?;
        let base = journal
            .recovery_base()
            .map_err(|e| invalid(e.to_string()))?
            .ok_or_else(|| invalid("no independently published recovery base"))?;
        let head = journal.head().map_err(|e| invalid(e.to_string()))?;
        if head.fenced
            || head.cluster != cluster
            || base.source.index > checkpoint.index
            || self.checkpoint()? != Some(checkpoint)
            || base.source.index > self.committed_index()?
        {
            return Err(invalid(
                "reclamation requires the active voter and a covering sealed local checkpoint",
            ));
        }
        journal
            .verify_since(Some(&base.source), &head, |_| Ok(()))
            .map_err(|e| invalid(e.to_string()))?;
        let loc = Location {
            directory: journal.directory().to_owned(),
            spec: journal.spec().clone(),
            base,
        };
        let prior = self.cold.current()?;
        let old = prior.as_ref().map_or(0, |c| c.through());
        let through = loc.base.source.index;
        if through < old {
            return Err(invalid("reclamation cannot regress"));
        }
        if let Some(previous) = &prior
            && previous.location != loc
        {
            journal
                .verify_since(
                    Some(&previous.location.base.source),
                    &loc.base.source,
                    |_| Ok(()),
                )
                .map_err(|e| invalid(format!("independent base lineage differs: {e}")))?;
        }
        self.db.flush()?;
        let before = self.live_sst_bytes()?;
        let mut size = 0;
        if prior.as_ref().is_none_or(|c| c.location != loc) {
            // If an interruption happens before the WAL switch, the next run
            // can release this prospective pin. Never drop the serving pin.
            let mut intent = WriteBatch::default();
            intent.put(RELEASE, signed(RELEASE, &identity, &loc)?);
            write_sync(&self.db, intent)?;
            pin_archive(journal, &loc.base, &identity)?;
            let reader = Arc::new(loc.open()?);
            size = self.compare_archive(&reader, through, old)?;
            self.preserve_anchor(journal, cluster, &loc.base.source)?;
            let live = journal.head().map_err(|e| invalid(e.to_string()))?;
            if live.fenced || live.cluster != cluster {
                return Err(invalid("voter lost journal authority during reclamation"));
            }
            #[cfg(test)]
            fault::check(fault::Point::BeforeSwitch)?;
            let mut meta = self
                .meta
                .write()
                .map_err(|_| invalid("Raft metadata lock poisoned"))?;
            let mut current = self
                .cold
                .current
                .write()
                .map_err(|_| invalid("cold history lock poisoned"))?;
            if meta.checkpoint != Some(checkpoint) {
                return Err(invalid("local checkpoint changed during reclamation"));
            }
            let mut batch = WriteBatch::default();
            batch.put(COLD, signed(COLD, &identity, &loc)?);
            if let Some(previous) = &prior {
                batch.put(RELEASE, signed(RELEASE, &identity, &previous.location)?);
            } else {
                batch.delete(RELEASE);
            }
            let floor = meta.compacted.max(through);
            batch.put(snapshot::COMPACTED, floor.to_be_bytes());
            if through > 1 {
                batch.delete_range(entry_key(1), entry_key(through));
            }
            if let Err(error) = write_sync(&self.db, batch) {
                // The new pin may already back the durable local pointer. It
                // can only be reconciled after reopening the WAL on this disk.
                self.cold
                    .uncertain
                    .store(true, std::sync::atomic::Ordering::Release);
                return Err(error);
            }
            *current = Some(Arc::new(Cold {
                location: loc,
                reader: Mutex::new(Some(reader)),
            }));
            meta.compacted = floor;
        } else {
            // A prior invocation may have stopped immediately after its WAL
            // switch. Recheck the independent copy before finishing removal
            // of any still-local adopted ranges or running manual compaction.
            pin_archive(journal, &loc.base, &identity)?;
            loc.open()?;
            self.preserve_anchor(journal, cluster, &loc.base.source)?;
        }
        #[cfg(test)]
        fault::check(fault::Point::AfterSwitch)?;
        drop(prior);
        // Each adopted range lives under its own digest. Clean one range per
        // synced batch; the durable handoff already supplies the whole prefix.
        self.sweep_adopted(through)?;
        self.db.flush()?;
        let mut compact = CompactOptions::default();
        compact.set_bottommost_level_compaction(BottommostLevelCompaction::Force);
        self.db
            .compact_range_opt::<&[u8], &[u8]>(None, None, &compact);
        if self
            .db
            .property_int_value("rocksdb.background-errors")?
            .unwrap_or(0)
            != 0
        {
            return Err(invalid(
                "RocksDB reported an error during history compaction",
            ));
        }
        self.reconcile_release(&identity)?;
        Ok(ReclaimStats {
            through,
            reclaimed_entries: through.saturating_sub(old.max(1)),
            reclaimed_bytes: size,
            live_sst_bytes_before: before,
            live_sst_bytes_after: self.live_sst_bytes()?,
        })
    }
    fn sweep_adopted(&self, through: u64) -> Result<(), ReplicaError> {
        loop {
            let mut meta = self
                .meta
                .write()
                .map_err(|_| invalid("Raft metadata lock poisoned"))?;
            let Some((key, value)) = self
                .db
                .iterator(IteratorMode::From(
                    &archive::range_key(0),
                    Direction::Forward,
                ))
                .next()
                .transpose()?
            else {
                return Ok(());
            };
            if key.first() != Some(&archive::RANGE) {
                return Ok(());
            }
            let start = archive::range_start(&key)?;
            if start >= through {
                return Ok(());
            }
            let range = archive::range_value(&key, &value)?;
            let end = through.min(range.index + 1);
            let mut batch = WriteBatch::default();
            batch.delete_range(
                archive::archive_key(&range, start),
                archive::archive_key(&range, end),
            );
            batch.delete(&key);
            if end <= range.index {
                batch.put(archive::range_key(end), &value);
            }
            write_sync(&self.db, batch)?;
            meta.archived = archive::last_range(&self.db)?;
        }
    }
}

#[cfg(test)]
mod fault {
    use std::cell::Cell;
    #[derive(Clone, Copy, PartialEq, Eq)]
    pub(super) enum Point {
        BeforeSwitch,
        AfterSwitch,
    }
    thread_local! {pub(super) static NEXT:Cell<Option<Point>>=const{Cell::new(None)};}
    pub(super) fn check(point: Point) -> Result<(), crate::ReplicaError> {
        if NEXT.get() == Some(point) {
            NEXT.set(None);
            return Err(std::io::Error::other("interrupted raw history reclamation").into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
