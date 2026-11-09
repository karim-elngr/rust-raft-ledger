use super::*;
use crate::format::encode_raw_append;

/// Sequential access to the exact length-framed protobuf payloads of a published
/// prefix. The temporary index holds one fixed-width head per visited segment.
pub struct EncodedCursor {
    journal: Journal,
    order: File,
    end: u64,
    descriptor: Vec<u8>,
    from: u64,
    current: Option<Segment>,
    slot: usize,
}

impl Journal {
    pub fn append_encoded(&self, expected: &Head, rows: &[&[u8]]) -> Result<Head, JournalError> {
        let (head, bytes) = encode_raw_append(self.spec_hash, expected, rows)?;
        let _guard = self.lock("append_encoded")?;
        if self.read_head()? != *expected {
            return Err(JournalError::Conflict);
        }
        self.publish_object(head, bytes)
    }

    pub fn encoded_cursor(&self, cut: &Head, first: u64) -> Result<EncodedCursor, JournalError> {
        if first == 0 {
            return Err(invalid("Raft index is one-based"));
        }
        self.require_published_cut(cut)?;
        let mut order = tempfile::tempfile()?;
        let mut head = cut.clone();
        {
            let mut spool = BufWriter::new(&mut order);
            while head.index >= first {
                let segment = self.segment(&head)?;
                if !segment.entries.is_empty() {
                    bincode::serialize_into(&mut spool, &head)?;
                }
                head = segment
                    .prior
                    .ok_or_else(|| invalid("missing predecessor in encoded journal"))?;
            }
            spool.flush()?;
        }
        let end = order.stream_position()?;
        let width = bincode::serialized_size(cut)? as usize;
        Ok(EncodedCursor {
            journal: self.clone(),
            order,
            end,
            descriptor: vec![0; width],
            from: first,
            current: None,
            slot: 0,
        })
    }
}

impl EncodedCursor {
    pub fn next_entry(&mut self) -> Result<Option<Vec<u8>>, JournalError> {
        loop {
            if let Some(group) = &self.current
                && let Some(entry) = group.entries.get(self.slot)
            {
                if entry.index != self.from {
                    return Err(invalid("encoded journal index discontinuity"));
                }
                let bytes = group.original(self.slot).to_owned();
                self.slot += 1;
                self.from = self.from.saturating_add(1);
                return Ok(Some(bytes));
            }
            if self.end == 0 {
                return Ok(None);
            }
            self.end -= self.descriptor.len() as u64;
            self.order.seek(SeekFrom::Start(self.end))?;
            self.order.read_exact(&mut self.descriptor)?;
            let head = bincode::deserialize(&self.descriptor)?;
            let group = self.journal.segment(&head)?;
            self.slot = group
                .entries
                .partition_point(|entry| entry.index < self.from);
            self.current = Some(group);
        }
    }
}
