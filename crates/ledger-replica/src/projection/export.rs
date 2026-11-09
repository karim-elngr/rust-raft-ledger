use super::*;
use ledger_export::{EventPage, EventRecord, MAX_EVENT_BYTES, MAX_EVENTS, Position as Cursor};
use rocksdb::{Direction, ReadOptions};

impl Projection {
    /// Copy one bounded page while the Raft owner holds the applied view. A
    /// scan frontier can cover no-ops and duplicate slots with no history row.
    pub(crate) fn event_page(
        &self,
        after: Cursor,
        through: u64,
    ) -> Result<EventPage, ReplicaError> {
        if through > self.applied || after.index > through {
            return Err(invalid("event scan exceeds applied history"));
        }
        let start = position(after.index, after.slot);
        let mut options = ReadOptions::default();
        options.set_iterate_upper_bound(position(through, u16::MAX));
        let mut events = Vec::new();
        let mut byte_count = 0u64;
        let mut scanned = Cursor::after_entry(through);
        for item in self.db.iterator_cf_opt(
            self.cf(HISTORY),
            options,
            IteratorMode::From(&start, Direction::Forward),
        ) {
            let (key, value) = item?;
            let key: Position = key
                .as_ref()
                .try_into()
                .map_err(|_| invalid("invalid decision position"))?;
            self.validate_position(&key)?;
            if key == start {
                continue;
            }
            let size = value.len() as u64;
            if size > MAX_EVENT_BYTES {
                return Err(ReplicaError::SizeLimit(
                    "decision exceeds export page limit",
                ));
            }
            if events.len() == MAX_EVENTS || byte_count + size > MAX_EVENT_BYTES {
                scanned = events
                    .last()
                    .map(|e: &EventRecord| e.position)
                    .ok_or_else(|| invalid("decision scan made no progress"))?;
                break;
            }
            byte_count += size;
            events.push(EventRecord {
                position: Cursor {
                    index: u64::from_be_bytes(key[..8].try_into().expect("index width")),
                    slot: u16::from_be_bytes(key[8..].try_into().expect("slot width")),
                },
                decision: format().deserialize(&value)?,
            });
        }
        Ok(EventPage {
            through: scanned,
            events,
        })
    }
}
