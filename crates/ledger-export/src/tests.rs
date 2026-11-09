use super::*;
use ledger_core::{Command, Error, LedgerId, RequestId, ReservationId};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

struct Feed {
    generation: AtomicU64,
    through: AtomicU64,
}
impl Feed {
    fn new(n: u64) -> Self {
        Self {
            generation: 42.into(),
            through: n.into(),
        }
    }
}
fn row(index: u64) -> RawEntry {
    RawEntry {
        index,
        term: 1,
        bytes: index.to_be_bytes().to_vec(),
    }
}
impl LedgerFeed for Feed {
    fn identity(&self) -> Result<Identity> {
        Ok(Identity {
            ledger: LedgerId(9),
            generation: self.generation.load(Ordering::Relaxed) as u128,
            asset: "USD".into(),
            scale: 2,
        })
    }
    fn applied_through(&self) -> Result<u64> {
        Ok(self.through.load(Ordering::Relaxed))
    }
    fn entries_after(&self, after: u64, through: u64) -> Result<Vec<RawEntry>> {
        Ok((after + 1..=through.min(after + 2)).map(row).collect())
    }
    fn events_after(&self, after: Position, through: u64) -> Result<EventPage> {
        let mut events = Vec::new();
        for index in after.index.max(1)..=through {
            let position = Position { index, slot: 0 };
            if index % 2 == 1 && position > after {
                events.push(EventRecord {
                    position,
                    decision: ledger_core::Decision {
                        request: RequestId(index as u128),
                        command: Command::Void {
                            reservation: ReservationId(1),
                        },
                        outcome: Err(Error::UnknownReservation(ReservationId(1))),
                    },
                });
                return Ok(EventPage {
                    events,
                    through: position,
                });
            }
        }
        Ok(EventPage {
            events,
            through: Position::after_entry(through),
        })
    }
}

#[derive(Default)]
struct Archive(Mutex<Vec<RawEntry>>, AtomicBool);
fn tip(rows: &[RawEntry]) -> ArchiveHead {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    for r in rows {
        hash.update(&r.bytes);
    }
    ArchiveHead {
        index: rows.len() as u64,
        term: rows.last().map_or(0, |r| r.term),
        digest: hash.finalize().into(),
    }
}
impl RecoveryArchive for Archive {
    fn destination(&self) -> &str {
        "independent"
    }
    fn head(&self, _: &Identity) -> Result<ArchiveHead> {
        Ok(tip(&self.0.lock().unwrap()))
    }
    fn append(
        &self,
        _: &Identity,
        expected: &ArchiveHead,
        rows: &[RawEntry],
    ) -> Result<ArchiveHead> {
        if self.1.load(Ordering::Relaxed) {
            return Err(ExportError::Unavailable("archive offline".into()));
        }
        let mut data = self.0.lock().unwrap();
        assert_eq!(&tip(&data), expected);
        for r in rows {
            let offset = r.index as usize - 1;
            if offset < data.len() {
                if data[offset] != *r {
                    return Err(ExportError::Invalid("fork".into()));
                }
            } else {
                data.push(r.clone())
            }
        }
        Ok(tip(&data))
    }
}
#[derive(Default)]
struct Broker(Mutex<Vec<Position>>, AtomicBool);
impl EventPublisher for Broker {
    fn destination(&self) -> &str {
        "events"
    }
    fn publish(&self, _: &Identity, events: &[EventRecord]) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .extend(events.iter().map(|r| r.position));
        if self.1.swap(false, Ordering::Relaxed) {
            return Err(ExportError::Unavailable("uncertain receipt".into()));
        }
        Ok(())
    }
}

#[test]
fn archive_first_noops_and_ambiguous_delivery_resume_independently() {
    let dir = tempfile::tempdir().unwrap();
    let feed = Arc::new(Feed::new(4));
    let archive = Arc::new(Archive::default());
    let broker = Arc::new(Broker::default());
    let workers = ExportWorkers::open(
        dir.path(),
        feed.clone(),
        Some(archive.clone()),
        Some(broker.clone()),
    )
    .unwrap();
    assert_eq!(workers.event_step().unwrap(), Step::Idle);
    workers.archive_step().unwrap();
    assert_eq!(workers.status().archived_through, Some(2));
    broker.1.store(true, Ordering::Relaxed);
    assert!(workers.event_step().is_err());
    workers.archive_step().unwrap();
    assert_eq!(workers.status().archived_through, Some(4));
    drop(workers);
    let workers = ExportWorkers::open(
        dir.path(),
        feed.clone(),
        Some(archive),
        Some(broker.clone()),
    )
    .unwrap();
    assert_eq!(workers.event_step().unwrap(), Step::Idle);
    workers.archive_step().unwrap();
    for _ in 0..4 {
        workers.event_step().unwrap();
    }
    assert_eq!(
        broker
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|p| p.index)
            .collect::<Vec<_>>(),
        [1, 1, 3]
    );
    assert_eq!(
        workers.status().event_cursor,
        Some(Position::after_entry(4))
    );
    drop(workers);
    feed.generation.store(43, Ordering::Relaxed);
    assert!(ExportWorkers::open(dir.path(), feed, None, Some(broker)).is_err());
}

#[test]
fn explicit_disabled_archive_allows_applied_events_without_claiming_durability() {
    let dir = tempfile::tempdir().unwrap();
    let feed = Arc::new(Feed::new(1));
    let workers = ExportWorkers::open(
        dir.path(),
        feed.clone(),
        Some(Arc::new(NoopArchive)),
        Some(Arc::new(NoopPublisher)),
    )
    .unwrap();
    assert_eq!(workers.archive_step().unwrap(), Step::Disabled);
    assert_eq!(workers.event_step().unwrap(), Step::Disabled);
    assert_eq!(workers.status().archived_through, None);
    assert_eq!(workers.status().event_cursor, None);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    let broker = Arc::new(Broker::default());
    let workers =
        ExportWorkers::open(dir.path(), feed, Some(Arc::new(NoopArchive)), Some(broker)).unwrap();
    assert_eq!(workers.event_step().unwrap(), Step::Advanced);
    assert_eq!(workers.status().archived_through, None);
}

#[test]
fn enabling_an_archive_resumes_the_broker_after_its_previous_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let feed = Arc::new(Feed::new(3));
    let broker = Arc::new(Broker::default());
    let exports =
        ExportWorkers::open(directory.path(), feed.clone(), None, Some(broker.clone())).unwrap();
    for _ in 0..2 {
        assert_eq!(exports.event_step().unwrap(), Step::Advanced);
    }
    let saved = Position { index: 3, slot: 0 };
    assert_eq!(exports.status().event_cursor, Some(saved));
    assert_eq!(exports.status().archived_through, None);
    drop(exports);

    feed.through.store(5, Ordering::Relaxed);
    let exports = ExportWorkers::open(
        directory.path(),
        feed,
        Some(Arc::new(Archive::default())),
        Some(broker.clone()),
    )
    .unwrap();
    assert_eq!(exports.event_step().unwrap(), Step::Idle);
    assert_eq!(exports.archive_step().unwrap(), Step::Advanced);
    assert_eq!(exports.status().archived_through, Some(2));
    assert_eq!(exports.event_step().unwrap(), Step::Idle);
    assert_eq!(exports.status().event_cursor, Some(saved));
    assert_eq!(exports.archive_step().unwrap(), Step::Advanced);
    assert_eq!(exports.status().archived_through, Some(4));
    assert_eq!(exports.event_step().unwrap(), Step::Advanced);
    assert_eq!(
        exports.status().event_cursor,
        Some(Position::after_entry(4))
    );
    assert_eq!(broker.0.lock().unwrap().len(), 2);
    assert_eq!(exports.archive_step().unwrap(), Step::Advanced);
    assert_eq!(exports.event_step().unwrap(), Step::Advanced);
    let delivered: Vec<_> = broker.0.lock().unwrap().iter().map(|p| p.index).collect();
    assert_eq!(delivered, [1, 3, 5]);
}

#[test]
fn existing_archive_must_match_source_before_its_progress_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let archive = Arc::new(Archive::default());
    let feed = Arc::new(Feed::new(3));
    *archive.0.lock().unwrap() = vec![
        row(1),
        row(2),
        RawEntry {
            bytes: vec![9],
            ..row(3)
        },
    ];
    let workers = ExportWorkers::open(dir.path(), feed, Some(archive), None).unwrap();
    workers.archive_step().unwrap();
    assert_eq!(workers.status().archived_through, Some(2));
    assert!(workers.archive_step().is_err());
    assert!(workers.status().archived_through.is_none());
}

#[test]
fn a_blocked_publisher_cannot_stop_archive_progress() {
    use std::{
        sync::mpsc,
        time::{Duration, Instant},
    };
    struct Paused {
        entered: mpsc::Sender<()>,
        release: Mutex<Option<mpsc::Receiver<()>>>,
    }
    impl EventPublisher for Paused {
        fn destination(&self) -> &str {
            "paused"
        }
        fn publish(&self, _: &Identity, _: &[EventRecord]) -> Result<()> {
            if let Some(rx) = self.release.lock().unwrap().take() {
                self.entered.send(()).unwrap();
                rx.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            Ok(())
        }
    }
    let root = tempfile::tempdir().unwrap();
    let feed = Arc::new(Feed::new(1));
    let (entered, started) = mpsc::channel();
    let (unpause, release) = mpsc::channel();
    let worker = Arc::new(
        ExportWorkers::open(
            root.path(),
            feed.clone(),
            Some(Arc::new(Archive::default())),
            Some(Arc::new(Paused {
                entered,
                release: Mutex::new(Some(release)),
            })),
        )
        .unwrap(),
    );
    let task = worker.start(Duration::from_millis(1)).unwrap();
    assert!(worker.start(Duration::from_millis(1)).is_err());
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    feed.through.store(5, Ordering::Relaxed);
    let until = Instant::now() + Duration::from_secs(3);
    while worker.status().archived_through != Some(5) {
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(worker.status().event_cursor, Some(Position::after_entry(0)));
    unpause.send(()).unwrap();
    task.shutdown().unwrap();
    let second = worker.start(Duration::from_millis(5)).unwrap();
    second.shutdown().unwrap();
}

#[test]
fn changed_remote_tip_cannot_inherit_the_old_witness_or_release_events() {
    let root = tempfile::tempdir().unwrap();
    let feed = Arc::new(Feed::new(2));
    let archive = Arc::new(Archive::default());
    let worker =
        ExportWorkers::open(root.path(), feed.clone(), Some(archive.clone()), None).unwrap();
    worker.archive_step().unwrap();
    drop(worker);
    feed.through.store(4, Ordering::Relaxed);
    let mut rewritten = vec![row(1), row(2), row(3), row(4)];
    rewritten[1].bytes = vec![77];
    *archive.0.lock().unwrap() = rewritten;
    let broker = Arc::new(Broker::default());
    let worker = ExportWorkers::open(
        root.path(),
        feed,
        Some(archive.clone()),
        Some(broker.clone()),
    )
    .unwrap();
    assert!(worker.archive_step().is_err());
    assert!(worker.status().archived_through.is_none());
    assert_eq!(worker.event_step().unwrap(), Step::Idle);
    assert!(broker.0.lock().unwrap().is_empty());
    *archive.0.lock().unwrap() = vec![row(1)];
    assert!(matches!(
        worker.archive_step(),
        Err(ExportError::Invalid(_))
    ));
}

#[test]
fn a_cursor_is_exclusive_bounded_and_fails_closed_after_damage() {
    let root = tempfile::tempdir().unwrap();
    let feed = Arc::new(Feed::new(1));
    let broker = Arc::new(Broker::default());
    let worker =
        ExportWorkers::open(root.path(), feed.clone(), None, Some(broker.clone())).unwrap();
    assert!(ExportWorkers::open(root.path(), feed.clone(), None, Some(broker.clone())).is_err());
    worker.event_step().unwrap();
    drop(worker);
    let record = std::fs::read_dir(root.path())
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|x| x == "cursor"))
        .unwrap();
    let mut data = std::fs::read(&record).unwrap();
    data[10] ^= 7;
    std::fs::write(&record, &data).unwrap();
    assert!(matches!(
        ExportWorkers::open(root.path(), feed, None, Some(broker)),
        Err(ExportError::Invalid(_))
    ));
    assert_eq!(std::fs::read(record).unwrap(), data);
}

#[test]
fn malformed_feed_pages_cannot_advance_either_destination() {
    struct Broken(Feed);
    impl LedgerFeed for Broken {
        fn identity(&self) -> Result<Identity> {
            self.0.identity()
        }
        fn applied_through(&self) -> Result<u64> {
            Ok(4)
        }
        fn entries_after(&self, _: u64, _: u64) -> Result<Vec<RawEntry>> {
            Ok(vec![row(2), row(4)])
        }
        fn events_after(&self, after: Position, through: u64) -> Result<EventPage> {
            let mut page = self.0.events_after(after, through)?;
            page.through = Position::after_entry(through + 1);
            Ok(page)
        }
    }
    let root = tempfile::tempdir().unwrap();
    let feed = Arc::new(Broken(Feed::new(4)));
    let a = Arc::new(Archive::default());
    let broker = Arc::new(Broker::default());
    let with_archive =
        ExportWorkers::open(root.path(), feed.clone(), Some(a.clone()), None).unwrap();
    let standalone = ExportWorkers::open(root.path(), feed, None, Some(broker.clone())).unwrap();
    assert!(with_archive.archive_step().is_err());
    assert!(standalone.event_step().is_err());
    assert!(a.0.lock().unwrap().is_empty());
    assert!(broker.0.lock().unwrap().is_empty());
}

#[test]
fn matching_archive_head_still_requires_exact_saved_local_boundary() {
    struct Changed(Feed);
    impl LedgerFeed for Changed {
        fn identity(&self) -> Result<Identity> {
            self.0.identity()
        }
        fn applied_through(&self) -> Result<u64> {
            self.0.applied_through()
        }
        fn entries_after(&self, after: u64, through: u64) -> Result<Vec<RawEntry>> {
            let mut rows = self.0.entries_after(after, through)?;
            for r in &mut rows {
                if r.index == 2 {
                    r.bytes = vec![8];
                }
            }
            Ok(rows)
        }
        fn events_after(&self, a: Position, t: u64) -> Result<EventPage> {
            self.0.events_after(a, t)
        }
    }
    let root = tempfile::tempdir().unwrap();
    let archive = Arc::new(Archive::default());
    let initial = ExportWorkers::open(
        root.path(),
        Arc::new(Feed::new(2)),
        Some(archive.clone()),
        None,
    )
    .unwrap();
    initial.archive_step().unwrap();
    drop(initial);
    let reopened = ExportWorkers::open(
        root.path(),
        Arc::new(Changed(Feed::new(2))),
        Some(archive),
        None,
    )
    .unwrap();
    assert!(matches!(
        reopened.archive_step(),
        Err(ExportError::Invalid(_))
    ));
    assert_eq!(reopened.status().archived_through, None);
}
