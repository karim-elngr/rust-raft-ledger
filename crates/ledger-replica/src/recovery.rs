pub(crate) mod base_image;

use std::fs::{self, File};
use std::io::ErrorKind;
use std::path::Path;

use ledger_recovery::{Head, Journal, JournalError, RecoveryBase};
use raft::eraftpb::Entry;

use crate::raft_store::RaftStore;
use crate::replica::create_replica_dir;
use crate::{CacheBudget, Replica, ReplicaError, StaticReplicaConfig};

pub(crate) mod base;
mod staging;
pub use base::RecoveryStats;

/// Read-only handle for a background exporter. Uncommitted log suffixes are
/// deliberately inaccessible even when they are already on durable storage.
#[derive(Clone)]
pub struct CommittedSource(pub(crate) RaftStore, pub(crate) u128);

impl CommittedSource {
    /// The last verified independent journal head remembered by this voter.
    /// This is a local verification starting point. Serving still requires a
    /// fresh confirmation from the active journal in the same generation.
    pub fn journal_anchor(&self) -> Result<Option<Head>, ReplicaError> {
        self.0.journal_anchor(self.1)
    }

    /// Caller has authenticated the journal ancestry and compared its complete
    /// prefix with this source, or appended this source's exact committed bytes
    /// to such a verified head. Only the replica's verifier can supply this proof.
    pub(crate) fn save_journal_anchor(
        &self,
        expected: Option<&Head>,
        head: &Head,
    ) -> Result<(), ReplicaError> {
        self.0.save_journal_anchor(self.1, expected, head)
    }

    /// Read up to 64 entries in (after, through], bounded by 2 MiB. As with Raft
    /// Storage::entries, one larger entry can be returned to ensure progress.
    pub fn entries_after(&self, after: u64, through: u64) -> Result<Vec<Entry>, ReplicaError> {
        if after > through || through > self.0.committed_index()? {
            return Err(ReplicaError::Invalid(
                "range outside durable committed prefix".into(),
            ));
        }
        if after == through {
            return Ok(Vec::new());
        }
        let end = after.saturating_add(64).min(through);
        // The store reserves u64::MAX as the exclusive end of a log range.
        Ok(self
            .0
            .retained_entries(after + 1, end + 1, Some(2u64 << 20))?)
    }

    pub(crate) fn encoded_after(
        &self,
        after: u64,
        through: u64,
    ) -> Result<Vec<Vec<u8>>, ReplicaError> {
        self.0
            .committed_page(after, through, |_, bytes| bytes.into_vec())
    }

    pub(crate) fn verify_encoded(
        &self,
        group: &ledger_recovery::JournalBatch,
    ) -> Result<(), ReplicaError> {
        let entries = group.entries();
        let Some(first) = entries.first() else {
            return Ok(());
        };
        let end = entries.last().expect("nonempty segment").index;
        let mut from = first.index - 1;
        let mut remote = group.encoded();
        while from < end {
            let page = self.encoded_after(from, end)?;
            if page.is_empty() {
                return Err(ReplicaError::Invalid(
                    "missing local recovery history".into(),
                ));
            }
            for bytes in page {
                if remote.next() != Some(bytes.as_slice()) {
                    return Err(ReplicaError::Invalid(
                        "offsite bytes differ from local committed log".into(),
                    ));
                }
                from += 1;
            }
        }
        Ok(())
    }

    /// Compare a recovery segment against the locally durable, committed log.
    /// A single recovery segment may span several bounded storage reads.
    pub fn verify(&self, entries: &[Entry]) -> Result<(), ReplicaError> {
        let Some(first) = entries.first() else {
            return Ok(());
        };
        let last = entries.last().expect("nonempty segment").index;
        let mut from = first
            .index
            .checked_sub(1)
            .ok_or_else(|| ReplicaError::Invalid("recovery entries start at one".into()))?;
        let mut compared = 0;
        while from < last {
            let local = self.entries_after(from, last)?;
            if local.is_empty()
                || entries.get(compared..compared + local.len()) != Some(local.as_slice())
            {
                return Err(ReplicaError::Invalid(
                    "offsite history differs from the local committed log".into(),
                ));
            }
            from = local.last().expect("nonempty local range").index;
            compared += local.len();
        }
        if compared != entries.len() {
            return Err(ReplicaError::Invalid(
                "noncontiguous recovery segment".into(),
            ));
        }
        Ok(())
    }

    pub fn term(&self, index: u64) -> Result<u64, ReplicaError> {
        if index > self.0.committed_index()? {
            return Err(ReplicaError::Invalid(
                "term outside durable committed prefix".into(),
            ));
        }
        Ok(self.0.retained_term(index)?)
    }
}

fn recovery_error(error: JournalError) -> ReplicaError {
    ReplicaError::Invalid(format!("recovery journal: {error}"))
}

fn require_current_fence(journal: &Journal, cut: &Head) -> Result<(), ReplicaError> {
    if !cut.fenced || journal.head().map_err(recovery_error)? != *cut {
        return Err(ReplicaError::Invalid(
            "restore requires the current fenced journal cut".into(),
        ));
    }
    Ok(())
}

impl Replica {
    /// Rebuild an isolated voter from a fenced, independently recoverable cut.
    /// Ordinary restart must use reopen. An interrupted import can be repeated:
    /// partial work lives outside the configured voter directory.
    pub fn restore(
        path: &Path,
        cfg: impl Into<StaticReplicaConfig>,
        journal: &Journal,
        cut: &Head,
    ) -> Result<Self, ReplicaError> {
        Self::restore_with_cache(path, cfg, journal, cut, CacheBudget::default())
    }

    pub fn restore_with_cache(
        path: &Path,
        cfg: impl Into<StaticReplicaConfig>,
        journal: &Journal,
        cut: &Head,
        cache: CacheBudget,
    ) -> Result<Self, ReplicaError> {
        Self::restore_with_stats(path, cfg, journal, cut, cache).map(|(voter, _)| voter)
    }

    pub fn restore_with_stats(
        path: &Path,
        cfg: impl Into<StaticReplicaConfig>,
        journal: &Journal,
        cut: &Head,
        cache: CacheBudget,
    ) -> Result<(Self, RecoveryStats), ReplicaError> {
        let base = journal.recovery_base().map_err(recovery_error)?;
        Self::restore_at_base(path, cfg, journal, cut, cache, base.as_ref())
    }

    /// Verify every journal segment from genesis, even if an independent
    /// checkpoint was published. This is an explicit repair route for a damaged image.
    pub fn restore_journal_only(
        path: &Path,
        cfg: impl Into<StaticReplicaConfig>,
        journal: &Journal,
        cut: &Head,
        cache: CacheBudget,
    ) -> Result<(Self, RecoveryStats), ReplicaError> {
        Self::restore_at_base(path, cfg, journal, cut, cache, None)
    }

    fn restore_at_base(
        path: &Path,
        cfg: impl Into<StaticReplicaConfig>,
        journal: &Journal,
        cut: &Head,
        cache: CacheBudget,
        base: Option<&RecoveryBase>,
    ) -> Result<(Self, RecoveryStats), ReplicaError> {
        let cfg = cfg.into();
        require_current_fence(journal, cut)?;
        let spec = journal.spec();
        if cfg.ledger.0 != spec.ledger || cfg.asset != spec.asset || cfg.scale != spec.scale {
            return Err(ReplicaError::Invalid(
                "restore requires the same ledger definition".into(),
            ));
        }
        if path.file_name().is_none() {
            return Err(ReplicaError::Invalid(
                "restore requires a named voter directory".into(),
            ));
        }
        match fs::symlink_metadata(path) {
            Ok(meta) if meta.is_dir() && fs::read_dir(path)?.next().transpose()?.is_none() => {}
            Ok(_) => {
                return Err(ReplicaError::Invalid(
                    "restore destination must be absent or an empty directory".into(),
                ));
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        create_replica_dir(parent)?;
        let stage = staging::RestoreStage::open(parent, path, journal)?;
        let stats = Self::stage_replacement(&stage.work, cfg.clone(), journal, cut, cache, base)?;
        require_current_fence(journal, cut)?;
        // Directory rename cannot replace a nonempty directory. A concurrent
        // successful bootstrap or restore therefore wins without losing data.
        fs::rename(&stage.work, path)?;
        File::open(parent)?.sync_all()?;
        File::open(stage.work.parent().expect("stage parent"))?.sync_all()?;
        Ok((Self::reopen_with_cache(path, cfg, cache)?, stats))
    }

    fn stage_replacement(
        stage: &Path,
        cfg: impl Into<StaticReplicaConfig>,
        journal: &Journal,
        cut: &Head,
        cache: CacheBudget,
        base: Option<&RecoveryBase>,
    ) -> Result<RecoveryStats, ReplicaError> {
        let mut cfg = cfg.into();
        let identity = cfg.identity()?;
        let mut working = base::WorkingBase::open(
            stage,
            journal,
            base,
            Some((&identity, cfg.conf_state())),
            cache,
        )?;
        journal
            .visit_replacement_encoded(base, cut, cfg.cluster, |group| working.import(group))
            .map_err(recovery_error)?;
        working.validate(cut)?;
        if let Some(base) = base {
            working.store.save_import_proof(base, cut)?;
        }
        let staged = Self::from_projection(stage, cfg, working.store, working.state, cache)?;
        if cut.index != 0 {
            staged
                .begin_checkpoint()?
                .ok_or_else(|| {
                    ReplicaError::Invalid("missing initial replacement checkpoint".into())
                })?
                .finish()?;
        }
        drop(staged);
        File::open(stage)?.sync_all()?;
        Ok(working.stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Replica, ReplicaConfig, codec::encode_batch};
    use ledger_core::{AccountId, Bounds, Command, LedgerId, RequestId, Side};
    use ledger_recovery::{Journal, Spec};
    use raft::Storage;
    use raft::eraftpb::Entry;
    use raft::eraftpb::HardState;
    use tempfile::tempdir;

    fn config(cluster: u128) -> ReplicaConfig {
        ReplicaConfig {
            cluster,
            node: 1,
            voters: vec![1, 2, 3],
            ledger: LedgerId(42),
            asset: "USD".into(),
            scale: 2,
        }
    }

    #[test]
    fn restored_learner_uses_the_neutral_image_and_retains_its_native_role() {
        let dir = tempdir().unwrap();
        let spec = Spec {
            ledger: 42,
            asset: "USD".into(),
            scale: 2,
        };
        let journal = Journal::init(&dir.path().join("journal"), &spec, 7).unwrap();
        let entry = Entry {
            index: 1,
            term: 2,
            ..Default::default()
        };
        let head = journal.append(&journal.head().unwrap(), &[entry]).unwrap();
        Replica::create_recovery_base(&journal, &head, CacheBudget::default()).unwrap();
        let fence = journal.fence(&head).unwrap();
        let dest = dir.path().join("learner");
        let cfg = ReplicaConfig {
            node: 4,
            ..config(8)
        }
        .with_learner(4);
        let (member, stats) =
            Replica::restore_with_stats(&dest, &cfg, &journal, &fence, CacheBudget::default())
                .unwrap();
        assert_eq!((stats.base_index, stats.replayed_entries), (head.index, 0));
        assert_eq!(member.recovered_from(), head.index);
        drop(member);
        let mut member = Replica::reopen(&dest, cfg).unwrap();
        assert_eq!(member.snapshot_history().membership().voters, [1, 2, 3]);
        assert_eq!(member.snapshot_history().membership().learners, [4]);
        assert!(member.campaign().is_err());
        assert!(member.activate_replacement(&journal, &fence).is_err());
        assert_eq!(journal.head().unwrap(), fence);
    }

    #[test]
    fn export_is_bounded_by_committed_prefix_size_and_count() {
        let dir = tempdir().unwrap();
        let store = RaftStore::bootstrap(&dir.path().join("raft"), b"source", &[1, 2, 3]).unwrap();
        let entries: Vec<_> = (1..=100)
            .map(|index| Entry {
                index,
                term: 3,
                ..Default::default()
            })
            .collect();
        store
            .persist_ready(
                &entries,
                Some(&HardState {
                    term: 3,
                    commit: 99,
                    ..Default::default()
                }),
            )
            .unwrap();
        let source = CommittedSource(store.clone(), 7);
        assert_eq!(source.entries_after(0, 99).unwrap(), entries[..64]);
        assert_eq!(source.entries_after(64, 99).unwrap(), entries[64..99]);
        assert!(source.entries_after(90, 90).unwrap().is_empty());
        assert!(source.entries_after(99, 100).is_err());
        assert!(source.entries_after(100, 100).is_err());
        assert!(source.entries_after(5, 4).is_err());
        assert_eq!(source.term(0).unwrap(), 0);
        assert_eq!(source.term(99).unwrap(), 3);
        assert!(source.term(100).is_err());
        let large: Vec<_> = (100..=102)
            .map(|index| Entry {
                index,
                term: 3,
                data: vec![0; 1 << 20].into(),
                ..Default::default()
            })
            .collect();
        store
            .persist_ready(
                &large,
                Some(&HardState {
                    term: 3,
                    commit: 102,
                    ..Default::default()
                }),
            )
            .unwrap();
        assert_eq!(source.entries_after(99, 102).unwrap(), large[..1]);
    }

    #[test]
    fn only_publishes_a_verified_fenced_cut_into_fresh_voter_storage() {
        let dir = tempdir().unwrap();
        let spec = Spec {
            ledger: 42,
            asset: "USD".into(),
            scale: 2,
        };
        let journal = Journal::init(&dir.path().join("journal"), &spec, 7).unwrap();
        let wallet = AccountId::new(LedgerId(42), 9);
        let command = Command::CreateAccount {
            account: wallet,
            normal: Side::Debit,
            code: 8,
            bounds: Bounds::default(),
        };
        let records = [
            Entry {
                index: 1,
                term: 3,
                ..Default::default()
            },
            Entry {
                index: 2,
                term: 4,
                data: encode_batch(LedgerId(42), &[(RequestId(9), command)])
                    .unwrap()
                    .into(),
                ..Default::default()
            },
        ];
        let live = journal.append(&journal.head().unwrap(), &records).unwrap();
        let dest = dir.path().join("recovered/voter1");
        assert!(Replica::restore(&dest, config(8), &journal, &live).is_err());
        let cut = journal.fence(&live).unwrap();
        let mut forged = cut.clone();
        forged.digest[0] ^= 0xff;
        assert!(Replica::restore(&dest, config(8), &journal, &forged).is_err());
        assert!(Replica::restore(&dest, config(7), &journal, &cut).is_err());
        assert!(
            Replica::restore(
                &dest,
                ReplicaConfig {
                    scale: 3,
                    ..config(8)
                },
                &journal,
                &cut
            )
            .is_err()
        );
        let voter = Replica::restore(&dest, config(8), &journal, &cut).unwrap();
        assert_eq!(voter.recovered_from(), cut.index);
        let sealed = voter.scrub_checkpoint().unwrap();
        assert_eq!((sealed.index, sealed.term), (cut.index, cut.term));
        assert_eq!(voter.last_index(), 2);
        assert_eq!(voter.term(), 4);
        assert_eq!(voter.ledger().unwrap().account(wallet).unwrap().code(), 8);
        assert!(Replica::restore(&dest, config(9), &journal, &cut).is_err());
        drop(voter);
        let reopened = Replica::reopen(&dest, config(8)).unwrap();
        assert_eq!(reopened.recovered_from(), cut.index);
        assert_eq!(reopened.applied_index(), 2);
        assert!(
            reopened
                .ledger()
                .unwrap()
                .request_outcome(RequestId(9))
                .is_some()
        );
        drop(reopened);
        let identity = config(8).identity().unwrap();
        let store = RaftStore::reopen(&dest.join("raft"), &identity).unwrap();
        let hard = store.initial_state().unwrap().hard_state;
        assert_eq!((hard.term, hard.vote, hard.commit), (4, 0, 2));
        journal.activate(&cut, 8).unwrap();
        let stale = dir.path().join("stale");
        assert!(Replica::restore(&stale, config(9), &journal, &cut).is_err());
        assert!(!stale.exists());
    }

    fn fenced_noop(root: &Path) -> (Journal, Head, Entry) {
        let journal = Journal::init(
            &root.join("journal"),
            &Spec {
                ledger: 42,
                asset: "USD".into(),
                scale: 2,
            },
            7,
        )
        .unwrap();
        let entry = Entry {
            index: 1,
            term: 2,
            ..Default::default()
        };
        let live = journal
            .append(&journal.head().unwrap(), std::slice::from_ref(&entry))
            .unwrap();
        let cut = journal.fence(&live).unwrap();
        (journal, cut, entry)
    }

    #[test]
    fn restore_and_first_protected_read_use_the_independent_base_and_exact_tail() {
        let root = tempdir().unwrap();
        let journal = Journal::init(
            &root.path().join("journal"),
            &Spec {
                ledger: 42,
                asset: "USD".into(),
                scale: 2,
            },
            7,
        )
        .unwrap();
        let mut head = journal.head().unwrap();
        let mut originals = Vec::new();
        for i in 1..=4 {
            let row = Entry {
                index: i,
                term: 2,
                ..Default::default()
            };
            head = journal.append(&head, std::slice::from_ref(&row)).unwrap();
            originals.push(row);
            if i == 1 || i == 3 {
                let stats =
                    Replica::create_recovery_base(&journal, &head, CacheBudget::default()).unwrap();
                let previous = if i == 1 { 0 } else { 1 };
                assert_eq!(
                    (stats.base_index, stats.replayed_entries),
                    (previous, i - previous)
                );
            }
        }
        let fenced = journal.fence(&head).unwrap();
        let dest = root.path().join("voter");
        let (voter, stats) = Replica::restore_with_stats(
            &dest,
            config(8),
            &journal,
            &fenced,
            CacheBudget::default(),
        )
        .unwrap();
        assert_eq!((stats.base_index, stats.replayed_entries), (3, 1));
        voter.committed_source().verify(&originals).unwrap();
        let (active, stats) = voter.activate_replacement(&journal, &fenced).unwrap();
        assert_eq!(active.cluster, 8);
        assert_eq!((stats.base_index, stats.replayed_entries), (3, 1));
        drop(voter);
        let voter = Replica::reopen(&dest, config(8)).unwrap();
        let confirmed = crate::JournalVerifier::open(&voter, journal.clone())
            .unwrap()
            .observe()
            .unwrap();
        assert_eq!((confirmed.head, confirmed.compared_entries), (active, 0));
    }

    #[test]
    fn corrupt_replacement_checkpoint_fails_closed_and_its_full_log_can_rebuild() {
        let root = tempdir().unwrap();
        let (journal, cut, entry) = fenced_noop(root.path());
        let dest = root.path().join("voter");
        let voter = Replica::restore(&dest, config(8), &journal, &cut).unwrap();
        assert_eq!(voter.recovered_from(), 1);
        drop(voter);
        let generation = fs::read_dir(dest.join("checkpoints"))
            .unwrap()
            .map(Result::unwrap)
            .find(|d| d.file_type().unwrap().is_dir())
            .unwrap();
        fs::write(generation.path().join("manifest"), b"torn").unwrap();
        assert!(Replica::reopen(&dest, config(8)).is_err());
        let rebuilt = Replica::rebuild_from_log(&dest, config(8), CacheBudget::default()).unwrap();
        assert_eq!((rebuilt.recovered_from(), rebuilt.applied_index()), (0, 1));
        rebuilt.committed_source().verify(&[entry]).unwrap();
        assert_eq!(journal.head().unwrap(), cut);
    }

    #[test]
    fn interrupted_after_sealing_before_directory_publication_can_retry() {
        let root = tempdir().unwrap();
        let (journal, cut, _) = fenced_noop(root.path());
        let dest = root.path().join("voter");
        let abandoned = staging::RestoreStage::open(root.path(), &dest, &journal).unwrap();
        Replica::stage_replacement(
            &abandoned.work,
            config(8),
            &journal,
            &cut,
            CacheBudget::default(),
            None,
        )
        .unwrap();
        // Model a process loss after the stage and its Raft pointer are durable.
        let work = abandoned.work.clone();
        assert!(staging::RestoreStage::open(root.path(), &dest, &journal).is_err());
        drop(abandoned);
        assert!(!dest.exists());
        assert_eq!(
            Replica::scrub_stored_checkpoint(&work, config(8))
                .unwrap()
                .index,
            cut.index
        );
        let restored = Replica::restore(&dest, config(9), &journal, &cut).unwrap();
        assert!(!work.exists());
        assert_eq!(restored.cluster_id(), 9);
        assert_eq!(restored.recovered_from(), cut.index);
        assert_eq!(journal.head().unwrap(), cut);
    }

    #[test]
    fn interrupted_restore_preserves_unrecognized_files_and_cleans_private_partial_data_on_retry() {
        let root = tempdir().unwrap();
        let (journal, cut, _) = fenced_noop(root.path());
        let dest = root.path().join("voter");
        let abandoned = staging::RestoreStage::open(root.path(), &dest, &journal).unwrap();
        let raft = abandoned.work.join("raft");
        fs::create_dir(&raft).unwrap();
        fs::write(raft.join("000123.sst"), b"incomplete restore").unwrap();
        let extra = raft.join("operator-note");
        fs::write(&extra, b"preserve me").unwrap();
        drop(abandoned);
        assert!(Replica::restore(&dest, config(9), &journal, &cut).is_err());
        assert!(!dest.exists());
        assert_eq!(fs::read(&extra).unwrap(), b"preserve me");
        fs::remove_file(extra).unwrap();
        let restored = Replica::restore(&dest, config(9), &journal, &cut).unwrap();
        assert_eq!(restored.applied_index(), cut.index);
        assert!(!raft.exists());
    }

    #[test]
    fn invalid_committed_payload_never_publishes_a_partial_voter() {
        let root = tempdir().unwrap();
        let journal = Journal::init(
            &root.path().join("journal"),
            &Spec {
                ledger: 42,
                asset: "USD".into(),
                scale: 2,
            },
            7,
        )
        .unwrap();
        let entries = [
            Entry {
                index: 1,
                term: 1,
                ..Default::default()
            },
            Entry {
                index: 2,
                term: 1,
                data: b"unsupported".to_vec().into(),
                ..Default::default()
            },
        ];
        let head = journal.append(&journal.head().unwrap(), &entries).unwrap();
        let cut = journal.fence(&head).unwrap();
        let destination = root.path().join("new-voter");
        fs::create_dir(&destination).unwrap();
        assert!(Replica::restore(&destination, config(8), &journal, &cut).is_err());
        assert!(fs::read_dir(&destination).unwrap().next().is_none());
        assert!(Replica::reopen(&destination, config(8)).is_err());
    }

    #[test]
    fn concurrent_restores_publish_at_most_one_new_voter_identity() {
        let root = tempdir().unwrap();
        let spec = Spec {
            ledger: 42,
            asset: "USD".into(),
            scale: 2,
        };
        let journal_path = root.path().join("journal");
        let journal = Journal::init(&journal_path, &spec, 7).unwrap();
        let cut = journal.fence(&journal.head().unwrap()).unwrap();
        let dest = root.path().join("voter");
        let barrier = std::sync::Barrier::new(2);
        let results = std::thread::scope(|scope| {
            let workers = [8, 9].map(|cluster| {
                let (path, spec, dest, cut, barrier) =
                    (&journal_path, &spec, &dest, &cut, &barrier);
                scope.spawn(move || {
                    let journal = Journal::open(path, spec).unwrap();
                    barrier.wait();
                    Replica::restore(dest, config(cluster), &journal, cut).map(|v| v.cluster_id())
                })
            });
            workers.map(|worker| worker.join().unwrap())
        });
        let successful: Vec<_> = results.into_iter().filter_map(Result::ok).collect();
        assert_eq!(successful.len(), 1);
        assert!(Replica::reopen(&dest, config(successful[0])).is_ok());
    }
}

#[cfg(test)]
mod exact_export_tests {
    use super::*;
    use ledger_core::LedgerId;
    use ledger_recovery::Spec;
    #[test]
    fn noncanonical_wire_bytes_round_trip_through_genesis_and_checkpoint_restore() {
        let temp = tempfile::tempdir().unwrap();
        let journal = Journal::init(
            &temp.path().join("archive"),
            &Spec {
                ledger: 4,
                asset: "GBP".into(),
                scale: 2,
            },
            7,
        )
        .unwrap();
        let one = vec![0x18, 1, 0x10, 2];
        let two = vec![0x18, 2, 0x10, 3];
        let first = journal
            .append_encoded(&journal.head().unwrap(), &[&one])
            .unwrap();
        Replica::create_recovery_base(&journal, &first, CacheBudget::default()).unwrap();
        let tip = journal.append_encoded(&first, &[&two]).unwrap();
        let cut = journal.fence(&tip).unwrap();
        for (i, full) in [false, true].into_iter().enumerate() {
            let cfg = crate::ReplicaConfig {
                cluster: 11,
                node: i as u64 + 1,
                voters: vec![1, 2, 3],
                ledger: LedgerId(4),
                asset: "GBP".into(),
                scale: 2,
            };
            let path = temp.path().join(format!("voter-{i}"));
            let (r, _) = if full {
                Replica::restore_journal_only(&path, cfg, &journal, &cut, CacheBudget::default())
            } else {
                Replica::restore_with_stats(&path, cfg, &journal, &cut, CacheBudget::default())
            }
            .unwrap();
            assert_eq!(
                r.committed_source().encoded_after(0, 2).unwrap(),
                [one.clone(), two.clone()]
            );
            if full {
                let other =
                    Journal::init(&temp.path().join("different-encoding"), journal.spec(), 11)
                        .unwrap();
                let equivalent = [
                    Entry {
                        index: 1,
                        term: 2,
                        ..Default::default()
                    },
                    Entry {
                        index: 2,
                        term: 3,
                        ..Default::default()
                    },
                ];
                let other_tip = other.append(&other.head().unwrap(), &equivalent).unwrap();
                let mut verifier = crate::JournalVerifier::open(&r, other.clone()).unwrap();
                assert!(
                    verifier.observe().is_err(),
                    "same parsed entries with different bytes"
                );
                let other_cut = other.fence(&other_tip).unwrap();
                // A replacement for generation 11 is ineligible under its own fence.
                // Check byte mismatch directly on the independently authenticated groups too.
                assert!(
                    other
                        .verify_since_encoded(None, &other_cut, |g| r
                            .committed_source()
                            .verify_encoded(g)
                            .map_err(|e| JournalError::Invalid(e.to_string())))
                        .is_err()
                );
                r.activate_replacement(&journal, &cut).unwrap();
            }
        }
    }
}
