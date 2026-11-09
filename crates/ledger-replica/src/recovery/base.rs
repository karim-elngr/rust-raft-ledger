//! Optional journal-derived checkpoint. A private neutral history and ledger
//! image are published together; neither contains a serving vote or voter ID.
use super::{
    base_image::{self, PortableImage},
    recovery_error,
};
use crate::{CacheBudget, Replica, ReplicaError, projection::Projection, raft_store::RaftStore};
use ledger_core::{Ledger, LedgerId};
use ledger_recovery::{Head, Journal, JournalError, RecoveryBase};
use raft::{Storage, eraftpb::HardState};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RecoveryStats {
    /// Entries through this index were recovered from a verified image.
    pub base_index: u64,
    /// Commands are batched. This counts Raft entries actually visited after the image.
    pub replayed_entries: u64,
}

mod registry;
mod scratch;
pub(crate) use registry::{pin_archive, release_archive};

const IMAGES: &str = "recovery-images-v1";
fn invalid(reason: &str) -> ReplicaError {
    ReplicaError::Invalid(reason.into())
}
fn generation(root: &Path, hash: &[u8; 32]) -> PathBuf {
    use std::fmt::Write;
    let mut name = String::with_capacity(64);
    for byte in hash {
        write!(&mut name, "{byte:02x}").expect("writing to string");
    }
    root.join(name)
}
pub(crate) fn open_image(
    journal: &Journal,
    base: &RecoveryBase,
) -> Result<PortableImage, ReplicaError> {
    let root = journal.directory().join(IMAGES);
    if !fs::symlink_metadata(&root)?.is_dir() {
        return Err(invalid("invalid recovery image directory"));
    }
    PortableImage::open(&generation(&root, &base.manifest), base.manifest)
}

pub(super) struct WorkingBase {
    pub store: RaftStore,
    pub state: Projection,
    pub stats: RecoveryStats,
}
impl WorkingBase {
    pub(super) fn open(
        work: &Path,
        journal: &Journal,
        base: Option<&RecoveryBase>,
        voter: Option<(&[u8], raft::eraftpb::ConfState)>,
        cache: CacheBudget,
    ) -> Result<Self, ReplicaError> {
        let spec = journal.spec();
        let ledger = LedgerId(spec.ledger);
        let raft_path = work.join("raft");
        let state_path = work.join("state");
        let (store, state) = match base {
            Some(base) => {
                let image = open_image(journal, base)?;
                image.materialize_raft(&raft_path)?;
                let store = RaftStore::open_neutral(&raft_path, spec, &base.source, voter)?;
                let state = Projection::from_checkpoint(
                    &state_path,
                    ledger,
                    base.source.index,
                    cache,
                    |dest| image.materialize_state(dest),
                )?;
                (store, state)
            }
            None => {
                let store = match voter {
                    Some((identity, members)) => {
                        RaftStore::bootstrap_members(&raft_path, identity, members)?
                    }
                    None => RaftStore::new_neutral(&raft_path)?,
                };
                let state = Projection::reset_with_cache(
                    &state_path,
                    Ledger::new(ledger, &spec.asset, spec.scale),
                    cache.account_bytes,
                    cache.block_bytes,
                )?;
                (store, state)
            }
        };
        let stats = RecoveryStats {
            base_index: state.applied_index(),
            replayed_entries: 0,
        };
        Ok(Self {
            store,
            state,
            stats,
        })
    }
    pub(super) fn import(
        &mut self,
        group: &ledger_recovery::JournalBatch,
    ) -> Result<(), JournalError> {
        let entries = group.entries();
        let Some(last) = entries.last() else {
            return Ok(());
        };
        let hard = HardState {
            term: last.term,
            commit: last.index,
            ..Default::default()
        };
        self.store
            .persist_recovery(group, &hard)
            .map_err(journal_error)?;
        for row in entries {
            self.state.apply(row).map_err(journal_error)?;
        }
        self.stats.replayed_entries += entries.len() as u64;
        Ok(())
    }
    pub(super) fn validate(&self, head: &Head) -> Result<(), ReplicaError> {
        if self.store.committed_index()? != head.index
            || self.store.last_index()? != head.index
            || self.store.term(head.index)? != head.term
            || self.state.applied_index() != head.index
            || self
                .stats
                .base_index
                .checked_add(self.stats.replayed_entries)
                != Some(head.index)
        {
            return Err(invalid(
                "imported history and state differ from journal cut",
            ));
        }
        Ok(())
    }
}
fn journal_error(e: ReplicaError) -> JournalError {
    JournalError::Invalid(e.to_string())
}

impl Replica {
    /// Build once from authenticated journal commands. The builder never needs a voter disk.
    pub fn create_recovery_base(
        journal: &Journal,
        cut: &Head,
        cache: CacheBudget,
    ) -> Result<RecoveryStats, ReplicaError> {
        Self::build_recovery_base(journal, cut, cache, false)
    }

    /// Supersede an unreadable image by authenticating the complete live
    /// journal. Previously pinned images retain their exact names and contents.
    pub fn rebuild_recovery_base(
        journal: &Journal,
        cut: &Head,
        cache: CacheBudget,
    ) -> Result<RecoveryStats, ReplicaError> {
        Self::build_recovery_base(journal, cut, cache, true)
    }

    fn build_recovery_base(
        journal: &Journal,
        cut: &Head,
        cache: CacheBudget,
        repair: bool,
    ) -> Result<RecoveryStats, ReplicaError> {
        if cut.fenced || cut.index == 0 {
            return Err(invalid("a journal checkpoint needs a nonempty live head"));
        }
        let root = journal.directory().join(IMAGES);
        match fs::create_dir(&root) {
            Ok(()) => File::open(journal.directory())?.sync_all()?,
            Err(e)
                if e.kind() == std::io::ErrorKind::AlreadyExists
                    && fs::symlink_metadata(&root)?.is_dir() => {}
            Err(e) => return Err(e.into()),
        }
        // Directory lock serializes image builders and collection; journal writers
        // exclusively use the journal's independent, short-lived publication lock.
        let _lease = registry::lock(&root)?;
        scratch::recover(&root, journal)?;
        let previous = if repair {
            None
        } else {
            journal.recovery_base().map_err(recovery_error)?
        };
        if let Some(base) = previous.as_ref().filter(|b| &b.source == cut) {
            if journal.head().map_err(recovery_error)? != *cut {
                return Err(invalid("journal checkpoint head changed"));
            }
            open_image(journal, base)?.scrub()?;
            registry::collect(&root, &base.manifest)?;
            return Ok(RecoveryStats {
                base_index: cut.index,
                replayed_entries: 0,
            });
        }
        let work = scratch::create(&root, journal)?;
        let mut working = WorkingBase::open(work.path(), journal, previous.as_ref(), None, cache)?;
        let import = |group: &ledger_recovery::JournalBatch| working.import(group);
        let ticket = if repair {
            journal.prepare_base_repair(cut, import)
        } else {
            journal.prepare_base_encoded(previous.as_ref(), cut, import)
        }
        .map_err(recovery_error)?;
        working.validate(cut)?;
        let raw = work.path().join("raw-checkpoint");
        let state = work.path().join("state-checkpoint");
        working.store.capture_neutral(&raw, journal.spec(), cut)?;
        working.state.capture(&state)?;
        let image = work.path().join("image");
        fs::create_dir(&image)?;
        let hash = base_image::seal(&image, &raw, &state)?;
        let dest = generation(&root, &hash);
        if dest.try_exists()? {
            PortableImage::open(&dest, hash)?.scrub()?;
        } else {
            fs::rename(&image, &dest)?;
        }
        File::open(&root)?.sync_all()?;
        journal
            .publish_base(&ticket, hash)
            .map_err(recovery_error)?;
        registry::collect(&root, &hash)?;
        Ok(working.stats)
    }

    /// Activation verifies the current fence while excluding any previously used cluster ID.
    pub fn activate_replacement(
        &self,
        journal: &Journal,
        cut: &Head,
    ) -> Result<(Head, RecoveryStats), ReplicaError> {
        if self.is_learner() {
            return Err(invalid("activation requires a restored voting member"));
        }
        if self.applied_index() != cut.index
            || self.last_index() != cut.index
            || journal.spec().ledger != self.ledger_id().0
            || journal.spec().asset != self.asset()
            || journal.spec().scale != self.scale()
        {
            return Err(invalid(
                "replacement must match the ledger definition and exact fenced prefix",
            ));
        }
        let source = self.committed_source();
        let proof = source.0.import_proof()?;
        let mut stats = RecoveryStats::default();
        let base = proof
            .filter(|proof| proof.fence == *cut)
            .map(|proof| proof.base);
        stats.base_index = base.as_ref().map_or(0, |b| b.source.index);
        let active = journal
            .activate_encoded_with(base.as_ref(), cut, self.cluster_id(), |group| {
                source.verify_encoded(group).map_err(journal_error)?;
                stats.replayed_entries += group.entries().len() as u64;
                Ok(())
            })
            .map_err(recovery_error)?;
        Ok((active, stats))
    }
}
