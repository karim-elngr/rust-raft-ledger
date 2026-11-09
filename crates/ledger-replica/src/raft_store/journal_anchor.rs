//! Optional voter-local evidence for a previously compared independent prefix.
//! V1 is fixed width: magic, LE cluster/index/term, immutable head digest,
//! local entry digest, and SHA-256 binding the entire record to identity/v1.
//! A live head is the only representable variant. Earlier stores omit the key.
use super::{RaftStore, decode_entry, invalid, term_fits, write_sync};
use crate::ReplicaError;
use ledger_recovery::Head;
use rocksdb::WriteBatch;
use sha2::{Digest, Sha256};

const KEY: &[u8] = b"journal-verification/v1";
const MAGIC: &[u8; 8] = b"LGJANC01";
const BODY: usize = 104;
const RECORD: usize = BODY + 32;

impl RaftStore {
    fn anchor_checksum(&self, body: &[u8]) -> Result<[u8; 32], ReplicaError> {
        let identity = self
            .db
            .get_pinned(b"identity/v1")?
            .ok_or_else(|| invalid("missing Raft voter identity for verification anchor"))?;
        let mut hash = Sha256::new();
        hash.update(Sha256::digest(&*identity));
        hash.update(body);
        Ok(hash.finalize().into())
    }

    fn anchor_boundary(&self, cluster: u128, head: &Head) -> Result<[u8; 32], ReplicaError> {
        let meta = self.metadata()?;
        if cluster == 0
            || head.cluster != cluster
            || head.fenced
            || head.index > meta.hard.commit
            || (head.index == 0) != (head.term == 0)
            || (head.index != 0
                && !term_fits(head.index, head.term, meta.hard.term, meta.checkpoint))
        {
            return Err(invalid(
                "journal anchor outside this generation and committed Raft prefix",
            ));
        }
        if head.index == 0 {
            return Ok([0; 32]);
        }
        let archived = meta.archived;
        drop(meta); // A cold read may have to verify an independent image.
        let bytes = self.raw_bytes(head.index, archived)?;
        if decode_entry(head.index, &bytes)?.term != head.term {
            return Err(invalid("journal anchor disagrees with committed Raft term"));
        }
        Ok(Sha256::digest(&bytes).into())
    }

    pub(crate) fn journal_anchor(&self, cluster: u128) -> Result<Option<Head>, ReplicaError> {
        let Some(raw) = self.db.get_pinned(KEY)? else {
            return Ok(None);
        };
        let bytes: &[u8; RECORD] = (&*raw)
            .try_into()
            .map_err(|_| invalid("invalid journal verification anchor length"))?;
        if &bytes[..8] != MAGIC || self.anchor_checksum(&bytes[..BODY])? != bytes[BODY..] {
            return Err(invalid(
                "journal verification anchor version or identity checksum mismatch",
            ));
        }
        let head = Head {
            cluster: u128::from_le_bytes(bytes[8..24].try_into().expect("fixed anchor layout")),
            index: u64::from_le_bytes(bytes[24..32].try_into().expect("fixed anchor layout")),
            term: u64::from_le_bytes(bytes[32..40].try_into().expect("fixed anchor layout")),
            digest: bytes[40..72].try_into().expect("fixed anchor layout"),
            fenced: false,
        };
        if self.anchor_boundary(cluster, &head)? != bytes[72..BODY] {
            return Err(invalid(
                "journal verification anchor's committed entry changed",
            ));
        }
        Ok(Some(head))
    }

    pub(crate) fn save_journal_anchor(
        &self,
        cluster: u128,
        expected: Option<&Head>,
        head: &Head,
    ) -> Result<(), ReplicaError> {
        let _writer = self
            .anchor_writer
            .lock()
            .map_err(|_| invalid("journal anchor writer lock poisoned"))?;
        let local = self.anchor_boundary(cluster, head)?;
        let prior = self.journal_anchor(cluster)?;
        if prior.as_ref() == Some(head) {
            return Ok(());
        }
        if prior.as_ref() != expected {
            return Err(ReplicaError::JournalAnchorChanged);
        }
        if prior.is_some_and(|p| p.index >= head.index) {
            return Err(invalid(
                "journal verification anchor would regress or change digest at the same index",
            ));
        }
        let mut bytes = [0u8; RECORD];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..24].copy_from_slice(&head.cluster.to_le_bytes());
        bytes[24..32].copy_from_slice(&head.index.to_le_bytes());
        bytes[32..40].copy_from_slice(&head.term.to_le_bytes());
        bytes[40..72].copy_from_slice(&head.digest);
        bytes[72..BODY].copy_from_slice(&local);
        let checksum = self.anchor_checksum(&bytes[..BODY])?;
        bytes[BODY..].copy_from_slice(&checksum);
        let mut batch = WriteBatch::default();
        batch.put(KEY, bytes);
        write_sync(&self.db, batch)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        CommittedSource,
        raft_store::{RaftStore, entry_key},
    };
    use ledger_recovery::Head;
    use protobuf::Message;
    use raft::eraftpb::{Entry, HardState};
    use std::path::Path;
    use tempfile::tempdir;

    fn voter(path: &Path, identity: &[u8]) -> RaftStore {
        let store = RaftStore::bootstrap(path, identity, &[1, 2, 3]).unwrap();
        let entries = (1..=4)
            .map(|index| Entry {
                index,
                term: if index == 1 { 1 } else { 2 },
                data: vec![index as u8].into(),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        store
            .persist_ready(
                &entries,
                Some(&HardState {
                    term: 2,
                    commit: 3,
                    ..Default::default()
                }),
            )
            .unwrap();
        store
    }

    fn head(index: u64) -> Head {
        Head {
            cluster: 7,
            index,
            term: if index < 2 { index } else { 2 },
            digest: [index as u8; 32],
            fenced: false,
        }
    }

    #[test]
    fn an_optional_anchor_survives_restart_and_only_advances_through_committed_live_heads() {
        let disk = tempdir().unwrap();
        let path = disk.path().join("voter");
        let source = CommittedSource(voter(&path, b"original-voter"), 7);
        assert_eq!(source.journal_anchor().unwrap(), None);
        source.save_journal_anchor(None, &head(0)).unwrap();
        source
            .save_journal_anchor(Some(&head(0)), &head(2))
            .unwrap();
        source
            .save_journal_anchor(Some(&head(0)), &head(2))
            .unwrap();
        for invalid in [
            head(1),
            head(4),
            Head {
                cluster: 8,
                ..head(3)
            },
            Head {
                fenced: true,
                ..head(3)
            },
            Head { term: 1, ..head(3) },
            Head {
                digest: [99; 32],
                ..head(2)
            },
        ] {
            assert!(
                source
                    .save_journal_anchor(Some(&head(2)), &invalid)
                    .is_err()
            );
            assert_eq!(source.journal_anchor().unwrap(), Some(head(2)));
        }
        drop(source);
        let store = RaftStore::reopen(&path, b"original-voter").unwrap();
        assert!(CommittedSource(store.clone(), 9).journal_anchor().is_err());
        let source = CommittedSource(store, 7);
        assert_eq!(source.journal_anchor().unwrap(), Some(head(2)));
        source
            .save_journal_anchor(Some(&head(2)), &head(3))
            .unwrap();
        assert_eq!(source.journal_anchor().unwrap(), Some(head(3)));
    }

    #[test]
    fn concurrent_verifiers_cannot_replace_an_unobserved_durable_anchor() {
        let disk = tempdir().unwrap();
        let source = CommittedSource(voter(&disk.path().join("voter"), b"same-voter"), 7);
        let gate = std::sync::Barrier::new(2);
        let results = std::thread::scope(|scope| {
            let workers = [2, 3].map(|index| {
                let gate = &gate;
                let source = &source;
                scope.spawn(move || {
                    assert_eq!(source.journal_anchor().unwrap(), None);
                    gate.wait();
                    (
                        index,
                        source.save_journal_anchor(None, &head(index)).is_ok(),
                    )
                })
            });
            workers.map(|w| w.join().unwrap())
        });
        let winners: Vec<_> = results.iter().filter(|(_, ok)| *ok).collect();
        assert_eq!(winners.len(), 1);
        assert_eq!(source.journal_anchor().unwrap(), Some(head(winners[0].0)));
    }

    #[test]
    fn tampered_anchor_wrong_voter_and_changed_local_boundary_cannot_be_reused() {
        let disk = tempdir().unwrap();
        let store = voter(&disk.path().join("one"), b"voter-one");
        let source = CommittedSource(store.clone(), 7);
        source.save_journal_anchor(None, &head(2)).unwrap();
        let key = super::KEY;
        let original = store.db.get(key).unwrap().unwrap();
        for corrupt in [vec![], original[..original.len() - 1].to_vec(), {
            let mut bytes = original.clone();
            bytes[45] ^= 1;
            bytes
        }] {
            store.db.put(key, corrupt).unwrap();
            assert!(source.journal_anchor().is_err());
        }
        store.db.put(key, &original).unwrap();
        let older =
            RaftStore::bootstrap(&disk.path().join("older"), b"voter-one", &[1, 2, 3]).unwrap();
        older.db.put(key, &original).unwrap();
        assert!(CommittedSource(older, 7).journal_anchor().is_err());

        let other = voter(&disk.path().join("other"), b"voter-two");
        other.db.put(key, &original).unwrap();
        assert!(CommittedSource(other, 7).journal_anchor().is_err());

        // Same index and term, different committed entry payload.
        let substituted = Entry {
            index: 2,
            term: 2,
            data: b"changed".to_vec().into(),
            ..Default::default()
        };
        store
            .db
            .put(entry_key(2), substituted.write_to_bytes().unwrap())
            .unwrap();
        assert!(source.journal_anchor().is_err());
    }
}
