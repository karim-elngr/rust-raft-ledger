//! A synchronous recovery journal on a POSIX filesystem. Local development
//! uses a shared directory. Recovery after loss of every voter disk requires
//! the journal to live on independently durable storage.

mod format;
mod journal;

pub use format::Segment as JournalBatch;
pub use format::{Head, MAX_ENTRIES, MAX_ENTRY_BYTES, MAX_SEGMENT_BYTES, Spec};
pub use journal::{EncodedCursor, Journal, JournalError, PreparedBase, RecoveryBase};

#[cfg(test)]
mod tests {
    use super::*;
    use raft::eraftpb::Entry;
    use std::{
        fs,
        path::Path,
        sync::{Arc, Barrier},
        thread,
    };
    use tempfile::tempdir;

    fn spec() -> Spec {
        Spec {
            ledger: 9,
            asset: "CAD".into(),
            scale: 2,
        }
    }

    fn entry(index: u64, term: u64, data: &str) -> Entry {
        Entry {
            index,
            term,
            data: data.as_bytes().to_vec().into(),
            ..Default::default()
        }
    }

    fn indexes(j: &Journal, cut: &Head) -> Result<Vec<u64>, JournalError> {
        let mut seen = vec![];
        j.visit(cut, |entries| {
            seen.extend(entries.iter().map(|e| e.index));
            Ok(())
        })?;
        Ok(seen)
    }

    fn race<T: Send + 'static>(
        j: &Journal,
        h: &Head,
        f: impl Fn(usize, Journal, Head) -> T + Sync + Send + 'static,
    ) -> Vec<T> {
        let start = Arc::new(Barrier::new(2));
        let action = Arc::new(f);
        let threads: Vec<_> = (0..2)
            .map(|i| {
                let (j, h, gate, action) = (j.clone(), h.clone(), start.clone(), action.clone());
                thread::spawn(move || {
                    gate.wait();
                    action(i, j, h)
                })
            })
            .collect();
        threads.into_iter().map(|t| t.join().unwrap()).collect()
    }

    #[test]
    fn contested_lock_times_out_without_publishing_or_reporting_a_cas_conflict() {
        use std::{fs::OpenOptions, io::ErrorKind, sync::mpsc, time::Duration};

        let dir = tempdir().unwrap();
        let j = Journal::init(dir.path(), &spec(), 1).unwrap();
        let head = j.head().unwrap();
        let holder = OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.path().join("LOCK"))
            .unwrap();
        holder.lock().unwrap();
        let (tx, rx) = mpsc::channel();
        let (writer, expected) = (j.clone(), head.clone());
        let attempt = thread::spawn(move || {
            let result = writer.append(&expected, &[entry(1, 1, "waiting")]);
            tx.send(result).unwrap();
        });
        // Release even if lock acquisition regresses to a blocking syscall.
        let result = rx.recv_timeout(Duration::from_secs(6));
        drop(holder);
        attempt.join().unwrap();
        assert!(matches!(result, Ok(Err(JournalError::Io(e))) if e.kind() == ErrorKind::TimedOut));
        assert_eq!(j.head().unwrap(), head);
        assert_eq!(j.append(&head, &[entry(1, 1, "retry")]).unwrap().index, 1);
    }

    #[test]
    fn one_publication_wins_and_ambiguous_retry_confirms_the_durable_head() {
        let dir = tempdir().unwrap();
        let j = Journal::init(dir.path(), &spec(), 1).unwrap();
        let genesis = j.head().unwrap();
        let results = race(&j, &genesis, |i, j, h| {
            j.append(&h, &[entry(1, 1, &i.to_string())])
        });
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, Err(JournalError::Conflict)))
                .count(),
            1
        );
        let winner = results.into_iter().find_map(Result::ok).unwrap();
        let reopened = Journal::open(dir.path(), &spec()).unwrap();
        assert_eq!(reopened.head().unwrap(), winner);
        assert!(matches!(
            reopened.append(&genesis, &[entry(1, 1, "0")]),
            Err(JournalError::Conflict)
        ));
        assert_eq!(indexes(&j, &winner).unwrap(), vec![1]);
        assert!(Journal::init(dir.path(), &spec(), 2).is_err());
        let mut foreign = spec();
        foreign.scale += 1;
        assert!(Journal::open(dir.path(), &foreign).is_err());
    }

    #[test]
    fn fence_serializes_with_publish_and_activation_retains_exact_history() {
        let dir = tempdir().unwrap();
        let j = Journal::init(dir.path(), &spec(), 8).unwrap();
        let base = j.append(&j.head().unwrap(), &[entry(1, 2, "a")]).unwrap();
        let won = race(&j, &base, |i, j, h| {
            if i == 0 {
                j.fence(&h)
            } else {
                j.append(&h, &[entry(2, 2, "b")])
            }
        });
        assert_eq!(won.iter().filter(|r| r.is_ok()).count(), 1);
        let head = j.head().unwrap();
        let fenced = if head.fenced {
            head
        } else {
            j.fence(&head).unwrap()
        };
        assert!(
            j.append(&fenced, &[entry(fenced.index + 1, 3, "denied")])
                .is_err()
        );
        assert!(j.activate(&fenced, 8).is_err());
        let active = j.activate(&fenced, 9).unwrap();
        assert_eq!((active.index, active.term), (fenced.index, fenced.term));
        assert_ne!(active.digest, fenced.digest);
        assert!(matches!(j.fence(&base), Err(JournalError::Conflict)));
        let tip = j
            .append(&active, &[entry(active.index + 1, 3, "new")])
            .unwrap();
        assert_eq!(
            indexes(&j, &fenced).unwrap(),
            (1..=fenced.index).collect::<Vec<_>>()
        );
        assert_eq!(
            indexes(&j, &tip).unwrap(),
            (1..=tip.index).collect::<Vec<_>>()
        );
        let mut delta = vec![];
        j.visit_since(&fenced, &tip, |es| {
            delta.extend(es.iter().map(|e| e.index));
            Ok(())
        })
        .unwrap();
        assert_eq!(delta, vec![tip.index]);
        let mut forged = fenced.clone();
        forged.fenced = false;
        assert!(indexes(&j, &forged).is_err());
    }

    #[test]
    fn damaged_middle_segment_is_detected_before_replay_begins() {
        for damage in ["missing", "short", "bitflip"] {
            let dir = tempdir().unwrap();
            let j = Journal::init(dir.path(), &spec(), 1).unwrap();
            let a = j.append(&j.head().unwrap(), &[entry(1, 2, "a")]).unwrap();
            let b = j.append(&a, &[entry(2, 3, "b")]).unwrap();
            let tip = j.append(&b, &[entry(3, 3, "c")]).unwrap();
            let path = crate::journal::object_path(dir.path(), &b.digest);
            match damage {
                "missing" => fs::remove_file(&path).unwrap(),
                "short" => fs::write(&path, b"xx").unwrap(),
                _ => {
                    let mut bytes = fs::read(&path).unwrap();
                    bytes[0] ^= 4;
                    fs::write(&path, bytes).unwrap();
                }
            }
            let mut calls = 0;
            assert!(
                j.visit(&tip, |_| {
                    calls += 1;
                    Ok(())
                })
                .is_err()
            );
            assert_eq!(calls, 0);
            assert!(
                j.visit_since(&a, &tip, |_| {
                    calls += 1;
                    Ok(())
                })
                .is_err()
            );
            assert_eq!(calls, 0);
        }
    }

    #[test]
    fn replacement_ids_are_opaque_and_can_never_repeat_an_old_generation() {
        let dir = tempdir().unwrap();
        let j = Journal::init(dir.path(), &spec(), u128::MAX).unwrap();
        let tip = j
            .append(&j.head().unwrap(), &[entry(1, 4, "first")])
            .unwrap();
        let old_fence = j.fence(&tip).unwrap();
        j.check_replacement(&old_fence, 17).unwrap();
        let replacement = j.activate(&old_fence, 17).unwrap();
        assert_eq!(
            (replacement.cluster, replacement.index, replacement.term),
            (17, 1, 4)
        );
        assert_ne!(replacement.digest, old_fence.digest);
        let next = j.append(&replacement, &[entry(2, 5, "second")]).unwrap();
        let current_fence = j.fence(&next).unwrap();

        assert_eq!(indexes(&j, &old_fence).unwrap(), [1]);
        let mut suffix = Vec::new();
        j.visit_since(&old_fence, &current_fence, |entries| {
            suffix.extend(entries.iter().map(|e| e.index));
            Ok(())
        })
        .unwrap();
        assert_eq!(suffix, [2]);
        for reused in [0, 17, u128::MAX] {
            assert!(matches!(
                j.check_replacement(&current_fence, reused),
                Err(JournalError::Invalid(_))
            ));
            assert!(j.activate(&current_fence, reused).is_err());
            assert_eq!(j.head().unwrap(), current_fence);
        }
        assert!(matches!(
            j.activate(&old_fence, 4),
            Err(JournalError::Conflict)
        ));
        assert_eq!(j.activate(&current_fence, 4).unwrap().cluster, 4);
    }

    #[test]
    fn two_fresh_replacements_cannot_activate_from_the_same_fence() {
        let dir = tempdir().unwrap();
        let j = Journal::init(dir.path(), &spec(), 98).unwrap();
        let cut = j.fence(&j.head().unwrap()).unwrap();
        for candidate in [1, 99] {
            j.check_replacement(&cut, candidate).unwrap();
        }
        let results = race(&j, &cut, |i, j, cut| j.activate(&cut, [1, 99][i]));
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, Err(JournalError::Conflict)))
                .count(),
            1
        );
        let published = results.into_iter().find_map(Result::ok).unwrap();
        assert_eq!(j.head().unwrap(), published);
        assert!(matches!(
            j.check_replacement(&cut, 6),
            Err(JournalError::Conflict)
        ));
        assert!(indexes(&j, &cut).unwrap().is_empty());
    }

    #[test]
    fn activation_requires_the_entire_fenced_history_to_be_intact() {
        let dir = tempdir().unwrap();
        let j = Journal::init(dir.path(), &spec(), 7).unwrap();
        let one = j.append(&j.head().unwrap(), &[entry(1, 1, "a")]).unwrap();
        let two = j.append(&one, &[entry(2, 2, "b")]).unwrap();
        let cut = j.fence(&two).unwrap();
        fs::remove_file(crate::journal::object_path(dir.path(), &one.digest)).unwrap();
        assert!(j.activate(&cut, 8).is_err());
        assert_eq!(j.head().unwrap(), cut);
    }

    #[test]
    fn replacement_replay_checks_historical_ids_before_import_and_delivers_entries_in_order() {
        let dir = tempdir().unwrap();
        let j = Journal::init(dir.path(), &spec(), 5).unwrap();
        let a = j.append(&j.head().unwrap(), &[entry(1, 1, "a")]).unwrap();
        let active = j.activate(&j.fence(&a).unwrap(), 6).unwrap();
        let b = j
            .append(&active, &[entry(2, 2, "b"), entry(3, 2, "c")])
            .unwrap();
        let cut = j.fence(&b).unwrap();
        assert!(
            j.visit_replacement(&cut, 5, |_| panic!("reused ID must fail before importing"))
                .is_err()
        );
        let mut seen = Vec::new();
        j.visit_replacement(&cut, 7, |entries| {
            seen.extend(entries.iter().map(|e| e.index));
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, [1, 2, 3]);
        assert_eq!(j.head().unwrap(), cut);

        let mut checked = Vec::new();
        let active = j
            .activate_with(&cut, 7, |entries| {
                checked.push((entries[0].index, entries.last().unwrap().index));
                Ok(())
            })
            .unwrap();
        assert_eq!(checked, [(2, 3), (1, 1)]);
        assert_eq!(active.cluster, 7);
    }

    #[test]
    fn failed_verification_or_a_concurrent_activation_cannot_publish_a_replacement() {
        let dir = tempdir().unwrap();
        let j = Journal::init(dir.path(), &spec(), 5).unwrap();
        let a = j.append(&j.head().unwrap(), &[entry(1, 1, "a")]).unwrap();
        let cut = j.fence(&a).unwrap();
        assert!(
            j.activate_with(&cut, 6, |_| Err(JournalError::Invalid(
                "different voter log".into()
            )))
            .is_err()
        );
        assert_eq!(j.head().unwrap(), cut);
        assert!(matches!(
            j.activate_with(&cut, 6, |_| {
                j.activate(&cut, 7)?;
                Ok(())
            }),
            Err(JournalError::Conflict)
        ));
        assert_eq!(j.head().unwrap().cluster, 7);

        let other = j.fence(&j.head().unwrap()).unwrap();
        assert!(matches!(
            j.visit_replacement(&other, 8, |_| {
                j.activate(&other, 9)?;
                Ok(())
            }),
            Err(JournalError::Conflict)
        ));
        assert_eq!(j.head().unwrap().cluster, 9);
    }

    #[test]
    fn replacement_evidence_is_bound_to_the_journal_checked_by_the_caller() {
        let make = |dir: &Path, data: &str| {
            let j = Journal::init(dir, &spec(), 1).unwrap();
            let a = j.append(&j.head().unwrap(), &[entry(1, 1, data)]).unwrap();
            let cut = j.fence(&a).unwrap();
            (j, a, cut)
        };
        let dir = tempdir().unwrap();
        let (good, _, cut) = make(&dir.path().join("good"), "original");
        let (foreign, _, _) = make(&dir.path().join("foreign"), "different");
        assert!(matches!(
            foreign.activate_with(&cut, 2, |_| panic!("foreign cut")),
            Err(JournalError::Conflict)
        ));
        let damaged_path = dir.path().join("damaged");
        let (damaged, a, same_cut) = make(&damaged_path, "original");
        assert_eq!(cut, same_cut);
        good.visit_replacement(&cut, 2, |_| Ok(())).unwrap();
        fs::write(
            crate::journal::object_path(&damaged_path, &a.digest),
            b"damaged",
        )
        .unwrap();
        assert!(
            damaged
                .visit_replacement(&cut, 2, |_| panic!("bad history must fail before import"))
                .is_err()
        );
        assert!(damaged.activate_with(&cut, 2, |_| Ok(())).is_err());
        assert_eq!(damaged.head().unwrap(), cut);
    }

    #[test]
    fn retry_recovers_from_interruption_on_either_side_of_head_rename() {
        use crate::journal::interruption::{self, Stage};
        let dir = tempdir().unwrap();
        let j = Journal::init(dir.path(), &spec(), 5).unwrap();
        let base = j.head().unwrap();
        let batch = [entry(1, 2, "recover")];
        interruption::inject(Stage::ObjectSynced);
        assert!(matches!(j.append(&base, &batch), Err(JournalError::Io(_))));
        assert_eq!(j.head().unwrap(), base);
        interruption::inject(Stage::HeadRenamed);
        assert!(matches!(j.append(&base, &batch), Err(JournalError::Io(_))));
        let j = Journal::open(dir.path(), &spec()).unwrap();
        let winner = j.head().unwrap();
        assert_eq!(winner.index, 1);
        assert!(matches!(
            j.append(&base, &batch),
            Err(JournalError::Conflict)
        ));
        j.visit(&winner, |es| {
            assert_eq!(es, &batch);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn count_entry_and_segment_limits_preserve_the_last_head() {
        let dir = tempdir().unwrap();
        let j = Journal::init(dir.path(), &spec(), 1).unwrap();
        let base = j.head().unwrap();
        let too_many: Vec<_> = (1..=MAX_ENTRIES as u64 + 1)
            .map(|i| entry(i, 1, "a"))
            .collect();
        assert!(j.append(&base, &too_many).is_err());
        let large = Entry {
            index: 1,
            term: 1,
            data: vec![0; MAX_ENTRY_BYTES].into(),
            ..Default::default()
        };
        assert!(j.append(&base, &[large]).is_err());
        let full: Vec<_> = (1..=8)
            .map(|i| Entry {
                index: i,
                term: 1,
                data: vec![0; 1 << 20].into(),
                ..Default::default()
            })
            .collect();
        assert!(j.append(&base, &full).is_err());
        assert_eq!(j.head().unwrap(), base);
        let h = j.append(&base, &full[..1]).unwrap();
        j.visit(&h, |es| {
            assert_eq!(es, &full[..1]);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn reverse_verification_visits_authenticated_groups_once_and_propagates_errors() {
        let dir = tempdir().unwrap();
        let j = Journal::init(dir.path(), &spec(), 1).unwrap();
        let first = j.append(&j.head().unwrap(), &[entry(1, 2, "a")]).unwrap();
        let next = j.activate(&j.fence(&first).unwrap(), 2).unwrap();
        let cut = j
            .append(&next, &[entry(2, 3, "b"), entry(3, 3, "c")])
            .unwrap();
        let tip = j.append(&cut, &[entry(4, 3, "d")]).unwrap();
        let groups = |base: Option<&Head>, cut: &Head| {
            let mut seen = Vec::new();
            j.verify_since(base, cut, |entries| {
                seen.push(entries.iter().map(|e| e.index).collect::<Vec<_>>());
                Ok(())
            })?;
            Ok::<_, JournalError>(seen)
        };
        assert_eq!(groups(None, &cut).unwrap(), [vec![2, 3], vec![1]]);
        assert_eq!(groups(Some(&first), &tip).unwrap(), [vec![4], vec![2, 3]]);
        assert!(groups(Some(&cut), &cut).unwrap().is_empty());
        assert!(groups(Some(&tip), &cut).is_err());
        let mut calls = 0;
        let result = j.verify_since(None, &cut, |_| {
            calls += 1;
            Err(JournalError::Invalid("entry mismatch".into()))
        });
        assert!(matches!(result, Err(JournalError::Invalid(msg)) if msg == "entry mismatch"));
        assert_eq!(calls, 1);
    }

    #[test]
    fn reverse_verification_rejects_foreign_anchors_and_missing_history() {
        let dir = tempdir().unwrap();
        let j = Journal::init(&dir.path().join("one"), &spec(), 1).unwrap();
        let other = Journal::init(&dir.path().join("two"), &spec(), 1).unwrap();
        let base = j.append(&j.head().unwrap(), &[entry(1, 1, "a")]).unwrap();
        let cut = j.append(&base, &[entry(2, 1, "b")]).unwrap();
        let foreign = other
            .append(&other.head().unwrap(), &[entry(1, 1, "c")])
            .unwrap();
        assert!(j.verify_since(Some(&foreign), &cut, |_| Ok(())).is_err());
        fs::remove_file(crate::journal::object_path(
            &dir.path().join("one"),
            &base.digest,
        ))
        .unwrap();
        assert!(j.verify_since(None, &cut, |_| Ok(())).is_err());
    }

    #[test]
    fn invisible_staging_and_declined_writes_leave_previous_head_usable() {
        let dir = tempdir().unwrap();
        let j = Journal::init(dir.path(), &spec(), 1).unwrap();
        let base = j.head().unwrap();
        fs::write(dir.path().join(".unfinished-head"), b"torn").unwrap();
        assert_eq!(
            Journal::open(dir.path(), &spec()).unwrap().head().unwrap(),
            base
        );
        for invalid in [
            vec![],
            vec![entry(2, 1, "gap")],
            vec![entry(1, 2, "a"), entry(2, 1, "term")],
        ] {
            assert!(j.append(&base, &invalid).is_err());
        }
        let head_file = dir.path().join("HEAD");
        let old_bytes = fs::read(&head_file).unwrap();
        let orphan = j
            .append(&base, &[entry(1, 1, "lost before rename")])
            .unwrap();
        fs::write(&head_file, old_bytes).unwrap();
        let good = j.append(&base, &[entry(1, 1, "published")]).unwrap();
        assert_ne!(good.digest, orphan.digest);
        assert!(indexes(&j, &orphan).is_err());
        assert!(
            j.verify_since(Some(&orphan), &orphan, |_| panic!("unpublished cut"))
                .is_err()
        );
        assert_eq!(indexes(&j, &good).unwrap(), [1]);
        fs::write(&head_file, b"torn").unwrap();
        assert!(j.head().is_err());
        assert!(Journal::open(dir.path(), &spec()).is_err());
        assert!(Journal::init(dir.path(), &spec(), 2).is_err());
        assert!(Journal::open(Path::new("/does-not-exist-ledger-journal"), &spec()).is_err());
    }
}
