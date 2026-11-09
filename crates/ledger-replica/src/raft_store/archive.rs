//! A received snapshot keeps the exact committed Raft bytes that preceded it.
//! Staged rows are unreachable until a Ready transaction publishes their range.
use super::*;
use rocksdb::Snapshot as View;

pub(super) const RANGE: u8 = 1;
const ARCHIVE: u8 = 2;

pub(super) fn range_key(start: u64) -> [u8; 9] {
    let mut key = entry_key(start);
    key[0] = RANGE;
    key
}

pub(super) fn archive_key(ref_: &CheckpointRef, index: u64) -> [u8; 41] {
    let mut key = [0; 41];
    key[0] = ARCHIVE;
    key[1..33].copy_from_slice(&ref_.digest);
    key[33..].copy_from_slice(&index.to_be_bytes());
    key
}

// Find the single physical key range holding the requested part of the
// logical log. The live database and its adopted ranges form one history.
pub(super) fn location(
    view: &View<'_>,
    index: u64,
    high: u64,
    archived: u64,
) -> Result<(Vec<u8>, u64), ReplicaError> {
    if index > archived {
        return Ok((entry_key(index).to_vec(), high));
    }
    let start = range_key(index);
    if let Some((key, bytes)) = view
        .iterator(IteratorMode::From(&start, Direction::Reverse))
        .next()
        .transpose()?
        && key.first() == Some(&RANGE)
    {
        let ref_ = range_value(&key, &bytes)?;
        if ref_.index >= index {
            return Ok((archive_key(&ref_, index).to_vec(), high.min(ref_.index + 1)));
        }
    }
    let next = view
        .iterator(IteratorMode::From(&start, Direction::Forward))
        .next()
        .transpose()?
        .filter(|(key, _)| key.first() == Some(&RANGE))
        .map(|(key, _)| range_start(&key))
        .transpose()?
        .unwrap_or(high);
    Ok((entry_key(index).to_vec(), high.min(next)))
}
pub(super) fn range_start(key: &[u8]) -> Result<u64, ReplicaError> {
    Ok(u64::from_be_bytes(key[1..].try_into().map_err(|_| {
        invalid("invalid adopted history range key")
    })?))
}
pub(super) fn range_value(key: &[u8], value: &[u8]) -> Result<CheckpointRef, ReplicaError> {
    let ref_ = CheckpointRef::from_snapshot_token(value)?;
    let start = range_start(key)?;
    if start == 0 || start > ref_.index || ref_.index == u64::MAX {
        return Err(invalid("invalid adopted history range"));
    }
    Ok(ref_)
}
pub(super) fn last_range(db: &DB) -> Result<u64, ReplicaError> {
    let key = range_key(u64::MAX);
    let found = db
        .iterator(IteratorMode::From(&key, Direction::Reverse))
        .next()
        .transpose()?;
    match found {
        Some((key, bytes)) if key.first() == Some(&RANGE) => Ok(range_value(&key, &bytes)?.index),
        _ => Ok(0),
    }
}

impl RaftStore {
    pub(super) fn raw_bytes(&self, index: u64, archived: u64) -> Result<Vec<u8>, ReplicaError> {
        // Capture the dependency and the matching local RocksDB sequence
        // together. A concurrently completed handoff may tombstone local rows.
        let (active, view) = self.cold.with_view(&self.db)?;
        if let Some(cold) = active
            && index < cold.through()
        {
            return cold
                .reader()?
                .db
                .get(entry_key(index))?
                .ok_or_else(|| invalid("missing independently retained Raft entry"));
        }
        let (key, _) = location(&view, index, index + 1, archived)?;
        view.get(key)?
            .ok_or_else(|| invalid(format!("missing retained Raft entry {index}")))
    }
    pub(crate) fn retained_term(&self, index: u64) -> raft::Result<u64> {
        self.log_term(index, false)
    }
    pub(crate) fn retained_entries(
        &self,
        low: u64,
        high: u64,
        max: Option<u64>,
    ) -> raft::Result<Vec<Entry>> {
        self.log_entries(low, high, max, false)
    }
    pub(super) fn log_entries(
        &self,
        low: u64,
        high: u64,
        max: Option<u64>,
        replication: bool,
    ) -> raft::Result<Vec<Entry>> {
        self.log_rows(low..high, max, replication, u64::MAX, |entry, _| entry)
    }

    pub(super) fn log_rows<T>(
        &self,
        range: std::ops::Range<u64>,
        max: Option<u64>,
        replication: bool,
        max_entry_size: u64,
        mut output: impl FnMut(Entry, Box<[u8]>) -> T,
    ) -> raft::Result<Vec<T>> {
        let (low, high) = (range.start, range.end);
        let meta = self.metadata().map_err(storage_error)?;
        if low == 0 || (replication && low <= meta.compacted) {
            return Err(StorageError::Compacted.into());
        }
        if low > high || high > meta.last + 1 {
            return Err(StorageError::Unavailable.into());
        }
        let (archived, hard_term, checkpoint) = (meta.archived, meta.hard.term, meta.checkpoint);
        let view = self.db.snapshot();
        let cold = self.cold.current().map_err(storage_error)?;
        drop(meta);
        let mut rows = Vec::new();
        let mut size = 0u64;
        let mut term = 0;
        let mut i = low;
        let mut read =
            |view: &View<'_>, mut key: Vec<u8>, start: u64, end: u64| -> raft::Result<bool> {
                let mut iter = view.iterator(IteratorMode::From(&key, Direction::Forward));
                let offset = key.len() - 8;
                for index in start..end {
                    let (found, bytes) = iter
                        .next()
                        .ok_or_else(|| storage_error(invalid("incomplete retained Raft log")))?
                        .map_err(|e| storage_error(e.into()))?;
                    key[offset..].copy_from_slice(&index.to_be_bytes());
                    if *found != key[..] {
                        return Err(storage_error(invalid("missing retained Raft entry")));
                    }
                    size = size.saturating_add(bytes.len() as u64);
                    if !rows.is_empty() && size > max.unwrap_or(u64::MAX) {
                        return Ok(false);
                    }
                    if bytes.len() as u64 > max_entry_size {
                        return Err(storage_error(invalid(
                            "retained entry exceeds snapshot transfer limit",
                        )));
                    }
                    let row = decode_entry(index, &bytes).map_err(storage_error)?;
                    if row.term < term || !term_fits(index, row.term, hard_term, checkpoint) {
                        return Err(storage_error(invalid("invalid retained Raft entry term")));
                    }
                    term = row.term;
                    rows.push(output(row, bytes));
                }
                Ok(true)
            };
        if let Some(cold) = cold
            && i < cold.through()
            && i < high
        {
            let end = high.min(cold.through());
            let reader = cold.reader().map_err(storage_error)?;
            if !read(&reader.db.snapshot(), entry_key(i).to_vec(), i, end)? {
                return Ok(rows);
            }
            i = end;
        }
        while i < high {
            let (key, end) = location(&view, i, high, archived).map_err(storage_error)?;
            if !read(&view, key, i, end)? {
                break;
            }
            i = end;
        }
        Ok(rows)
    }
    pub(super) fn log_term(&self, index: u64, replication: bool) -> raft::Result<u64> {
        let meta = self.metadata().map_err(storage_error)?;
        if replication && index < meta.compacted {
            return Err(StorageError::Compacted.into());
        }
        if index > meta.last {
            return Err(StorageError::Unavailable.into());
        }
        if index == meta.last {
            return Ok(meta.last_term);
        }
        if index == 0 {
            return Ok(0);
        }
        let (archived, current_term, checkpoint) = (meta.archived, meta.hard.term, meta.checkpoint);
        if index <= meta.hard.commit {
            drop(meta); // Committed bytes cannot change while a cold image opens.
        }
        let bytes = self.raw_bytes(index, archived).map_err(storage_error)?;
        let term = decode_entry(index, &bytes).map_err(storage_error)?.term;
        if !term_fits(index, term, current_term, checkpoint) {
            return Err(storage_error(invalid("inconsistent retained Raft term")));
        }
        Ok(term)
    }
}
