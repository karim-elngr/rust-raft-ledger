use super::*;
use crate::cursor::Cursor;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::{
    Arc, Condvar, Mutex, MutexGuard,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MAX_SCAN_ENTRIES: u64 = 256;
const LAST_COMMAND_SLOT: u16 = 255;

impl Position {
    pub(crate) fn valid(self) -> bool {
        self == Self::after_entry(0)
            || (self.index > 0 && (self.slot <= LAST_COMMAND_SLOT || self.slot == u16::MAX))
    }
}
fn invalid(reason: &str) -> ExportError {
    ExportError::Invalid(reason.into())
}
fn lock<T>(value: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    value.lock().map_err(|_| invalid("export lock poisoned"))
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Step {
    Disabled,
    Idle,
    Advanced,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExportStatus {
    /// Saved, previously validated archive cursor. Independent coverage for
    /// this run is reported separately after a live check of the archive.
    pub archive_cursor: Option<u64>,
    pub archived_through: Option<u64>,
    pub event_cursor: Option<Position>,
    pub archive_error: Option<String>,
    pub event_error: Option<String>,
}

struct ArchiveProgress {
    cursor: Cursor,
    seen: Option<ArchiveHead>,
    checked: u64,
}

pub struct ExportWorkers {
    id: Identity,
    feed: Arc<dyn LedgerFeed>,
    archive: Option<(Arc<dyn RecoveryArchive>, Mutex<ArchiveProgress>)>,
    events: Option<(Arc<dyn EventPublisher>, Mutex<Cursor>)>,
    status: Mutex<ExportStatus>,
    running: AtomicBool,
}

impl ExportWorkers {
    pub fn open(
        directory: &Path,
        feed: Arc<dyn LedgerFeed>,
        archive: Option<Arc<dyn RecoveryArchive>>,
        events: Option<Arc<dyn EventPublisher>>,
    ) -> Result<Self> {
        let id = feed.identity()?;
        if id.generation == 0 || id.asset.trim().is_empty() || id.asset.len() > 16 << 10 {
            return Err(invalid("invalid export ledger specification"));
        }
        let mut status = ExportStatus::default();
        let archive = archive
            .filter(|s| s.enabled())
            .map(|sink| {
                let cursor = Cursor::open(directory, &id, "archive", sink.destination())?;
                status.archive_cursor = Some(cursor.position().index);
                Ok((
                    sink,
                    Mutex::new(ArchiveProgress {
                        cursor,
                        seen: None,
                        checked: 0,
                    }),
                ))
            })
            .transpose()?;
        let events = events
            .filter(|s| s.enabled())
            .map(|sink| {
                let cursor = Cursor::open(directory, &id, "events", sink.destination())?;
                status.event_cursor = Some(cursor.position());
                Ok((sink, Mutex::new(cursor)))
            })
            .transpose()?;
        Ok(Self {
            id,
            feed,
            archive,
            events,
            status: Mutex::new(status),
            running: false.into(),
        })
    }

    pub fn status(&self) -> ExportStatus {
        self.status
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
    fn check_identity(&self) -> Result<()> {
        if self.feed.identity()? != self.id {
            return Err(invalid("source generation or ledger specification changed"));
        }
        Ok(())
    }

    /// Verify or publish at most one bounded raw page. A fresh attach never
    /// trusts a previously populated same-generation archive by position alone.
    pub fn archive_step(&self) -> Result<Step> {
        let Some((archive, state)) = &self.archive else {
            return Ok(Step::Disabled);
        };
        let mut state = lock(state)?;
        let result = self.archive_page(archive.as_ref(), &mut state);
        let mut status = lock(&self.status)?;
        match &result {
            Ok(_) => {
                status.archive_cursor = Some(state.cursor.position().index);
                status.archived_through = Some(state.checked);
                status.archive_error = None;
            }
            Err(e) => {
                status.archived_through = None;
                status.archive_error = Some(e.to_string().chars().take(512).collect());
            }
        }
        result
    }

    fn archive_page(
        &self,
        archive: &dyn RecoveryArchive,
        state: &mut ArchiveProgress,
    ) -> Result<Step> {
        self.check_identity()?;
        let applied = self.feed.applied_through()?;
        let head = archive.head(&self.id)?;
        if (head.index == 0) != (head.term == 0) {
            return Err(invalid("invalid archive head term"));
        }
        let prior = state.seen.or_else(|| state.cursor.witness());
        if let Some(old) = prior
            && old != head
            && (head.index <= old.index || head.term < old.term)
        {
            return Err(invalid(
                "remote archive rolled back or replaced a verified head",
            ));
        }
        if state.seen != Some(head) {
            // A matching persisted witness authenticates the saved archive
            // prefix. An independently advanced tip is verified from genesis.
            lock(&self.status)?.archived_through = None;
            let saved = state.cursor.position().index;
            state.checked = if state.cursor.witness() == Some(head) && saved > 0 {
                if applied < saved {
                    return Err(ExportError::Unavailable(
                        "source still catching up to saved archive cursor".into(),
                    ));
                }
                let last = self.feed.entries_after(saved - 1, saved)?;
                validate_raw(&last, saved - 1, saved)?;
                if state.cursor.local_boundary() != Some(boundary_hash(&last[0])) {
                    return Err(invalid(
                        "saved archive boundary differs from current committed source",
                    ));
                }
                saved
            } else {
                0
            };
            state.seen = Some(head);
        }
        if applied < state.checked {
            return Err(ExportError::Unavailable(
                "source still catching up to saved archive cursor".into(),
            ));
        }
        let through = applied.min(state.checked.saturating_add(MAX_RAW_ENTRIES as u64));
        if state.checked == through {
            if state.cursor.witness() != Some(head)
                && state.checked >= state.cursor.position().index
            {
                state.cursor.save(
                    Position::after_entry(state.checked),
                    Some(head),
                    state.cursor.local_boundary(),
                )?;
            }
            return Ok(Step::Idle);
        }
        let entries = self.feed.entries_after(state.checked, through)?;
        validate_raw(&entries, state.checked, through)?;
        self.check_identity()?;
        let last = entries.last().expect("validated nonempty page");
        let next = archive.append(&self.id, &head, &entries)?;
        if next.index != head.index.max(last.index)
            || (last.index <= head.index && next != head)
            || (last.index > head.index && next.term != last.term)
        {
            return Err(invalid("archive did not confirm the requested page"));
        }
        if last.index >= state.cursor.position().index {
            state.cursor.save(
                Position::after_entry(last.index),
                Some(next),
                Some(boundary_hash(last)),
            )?;
        }
        state.seen = Some(next);
        state.checked = last.index;
        Ok(Step::Advanced)
    }

    /// With both sinks enabled the publisher may only use independently
    /// confirmed rows. With archive disabled it publishes locally applied rows.
    pub fn event_step(&self) -> Result<Step> {
        let Some((broker, cursor)) = &self.events else {
            return Ok(Step::Disabled);
        };
        let mut cursor = lock(cursor)?;
        let result = self.event_page(broker.as_ref(), &mut cursor);
        let mut status = lock(&self.status)?;
        status.event_cursor = Some(cursor.position());
        status.event_error = result
            .as_ref()
            .err()
            .map(|e| e.to_string().chars().take(512).collect());
        result
    }

    fn event_page(&self, broker: &dyn EventPublisher, cursor: &mut Cursor) -> Result<Step> {
        self.check_identity()?;
        let after = cursor.position();
        let mut through = self
            .feed
            .applied_through()?
            .min(after.index.saturating_add(MAX_SCAN_ENTRIES));
        if self.archive.is_some() {
            let Some(covered) = lock(&self.status)?.archived_through else {
                return Ok(Step::Idle);
            };
            through = through.min(covered);
        }
        if after >= Position::after_entry(through) {
            return Ok(Step::Idle);
        }
        let page = self.feed.events_after(after, through)?;
        if !page.through.valid()
            || page.through <= after
            || page.through > Position::after_entry(through)
            || page.events.len() > MAX_EVENTS
        {
            return Err(invalid("event page exceeds requested cursor or row limits"));
        }
        let mut previous = after;
        let mut bytes = 0u64;
        for event in &page.events {
            let pos = event.position;
            if pos <= previous
                || pos > page.through
                || pos.index == 0
                || pos.slot > LAST_COMMAND_SLOT
            {
                return Err(invalid("invalid event page ordering"));
            }
            let size = bincode::serialized_size(&event.decision)
                .map_err(|_| invalid("cannot measure event size"))?;
            bytes = bytes.saturating_add(size);
            if bytes > MAX_EVENT_BYTES {
                return Err(invalid("event page exceeds byte limit"));
            }
            previous = pos;
        }
        self.check_identity()?;
        if !page.events.is_empty() {
            broker.publish(&self.id, &page.events)?;
        }
        cursor.save(page.through, None, None)?;
        Ok(Step::Advanced)
    }

    /// Spawn one OS worker per enabled destination. A blocked publisher
    /// cannot stall the archive, source consensus or its financial decisions.
    pub fn start(self: &Arc<Self>, idle: Duration) -> Result<RunningWorkers> {
        if idle.is_zero() {
            return Err(invalid("export idle interval must be positive"));
        }
        if self
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(invalid("export workers already running"));
        }
        let mut running = RunningWorkers {
            workers: Arc::clone(self),
            stop: Arc::default(),
            threads: Vec::new(),
            joined: false,
        };
        for (enabled, archive, name) in [
            (self.archive.is_some(), true, "ledger-archive"),
            (self.events.is_some(), false, "ledger-events"),
        ] {
            if !enabled {
                continue;
            }
            let worker = Arc::clone(self);
            let stop = Arc::clone(&running.stop);
            let handle = thread::Builder::new()
                .name(name.into())
                .spawn(move || {
                    loop {
                        let (flag, wake) = &*stop;
                        if *flag.lock().unwrap_or_else(|p| p.into_inner()) {
                            break;
                        }
                        let step = if archive {
                            worker.archive_step()
                        } else {
                            worker.event_step()
                        };
                        if !matches!(step, Ok(Step::Advanced)) {
                            let stopper = flag.lock().unwrap_or_else(|p| p.into_inner());
                            drop(wake.wait_timeout_while(stopper, idle, |stopped| !*stopped));
                        }
                    }
                })
                .map_err(|e| ExportError::Unavailable(e.to_string()))?;
            running.threads.push(handle);
        }
        Ok(running)
    }
}

fn boundary_hash(row: &RawEntry) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"ledger-export/local-boundary/v1\0");
    hash.update(row.index.to_be_bytes());
    hash.update(row.term.to_be_bytes());
    hash.update(&row.bytes);
    hash.finalize().into()
}

fn validate_raw(rows: &[RawEntry], after: u64, through: u64) -> Result<()> {
    if rows.is_empty() || rows.len() > MAX_RAW_ENTRIES {
        return Err(invalid("empty or oversized raw page"));
    }
    let mut previous = after;
    let mut term = 0;
    let mut bytes = 0usize;
    for row in rows {
        bytes = bytes.saturating_add(row.bytes.len());
        if previous.checked_add(1) != Some(row.index)
            || row.index > through
            || row.term == 0
            || row.term < term
            || row.bytes.is_empty()
            || row.bytes.len() > MAX_RAW_ENTRY_BYTES
            || bytes > MAX_RAW_BYTES
        {
            return Err(invalid(
                "raw page exceeds bounds or has a gap or invalid term",
            ));
        }
        previous = row.index;
        term = row.term;
    }
    Ok(())
}

pub struct RunningWorkers {
    workers: Arc<ExportWorkers>,
    stop: Arc<(Mutex<bool>, Condvar)>,
    threads: Vec<JoinHandle<()>>,
    joined: bool,
}
impl RunningWorkers {
    fn stop_and_join(&mut self) -> Result<()> {
        if self.joined {
            return Ok(());
        }
        self.joined = true;
        let (flag, wake) = &*self.stop;
        *flag.lock().unwrap_or_else(|p| p.into_inner()) = true;
        wake.notify_all();
        let mut ok = true;
        for thread in self.threads.drain(..) {
            ok &= thread.join().is_ok();
        }
        self.workers.running.store(false, Ordering::Release);
        if ok {
            Ok(())
        } else {
            Err(invalid("an export worker stopped unexpectedly"))
        }
    }
    pub fn shutdown(mut self) -> Result<()> {
        self.stop_and_join()
    }
}
impl Drop for RunningWorkers {
    fn drop(&mut self) {
        let _ = self.stop_and_join();
    }
}
