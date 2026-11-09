//! Raft history in a portable recovery checkpoint has never belonged to a
//! voting member. Bind only a verified private copy to a new voter identity.
use super::*;
use ledger_recovery::{Head, RecoveryBase, Spec};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const NEUTRAL: &[u8] = b"ledger-neutral-recovery/v1";
const CUT: &[u8] = b"portable-recovery-cut/v1";
const IMPORT: &[u8] = b"imported-recovery-base/v1";

#[derive(Serialize, Deserialize)]
pub(crate) struct ImportProof {
    pub(crate) base: RecoveryBase,
    pub(crate) fence: Head,
}

impl RaftStore {
    pub(crate) fn new_neutral(path: &Path) -> Result<Self, ReplicaError> {
        Self::bootstrap(path, NEUTRAL, &[1])
    }

    pub(crate) fn capture_neutral(
        &self,
        dest: &Path,
        spec: &Spec,
        head: &Head,
    ) -> Result<(), ReplicaError> {
        if self.committed_index()? != head.index
            || self.last_index()? != head.index
            || self.term(head.index)? != head.term
            || required(&self.db, b"identity/v1")? != NEUTRAL
        {
            return Err(invalid("neutral history differs from recovery checkpoint"));
        }
        let mut batch = WriteBatch::default();
        batch.put(CUT, bincode::serialize(&(spec, head))?);
        write_sync(&self.db, batch)?;
        self.db.flush()?;
        rocksdb::checkpoint::Checkpoint::new(&self.db)?.create_checkpoint(dest)?;
        Ok(())
    }

    /// The caller has authenticated every file in this private copy. Validate
    /// the neutral provenance and boundary before installing local metadata.
    pub(crate) fn open_neutral(
        path: &Path,
        spec: &Spec,
        head: &Head,
        voter: Option<(&[u8], ConfState)>,
    ) -> Result<Self, ReplicaError> {
        let db = DB::open(&db_options(false), path)?;
        let (hard, neutral_conf) = Self::validate_neutral(&db, spec, head)?;
        let conf = if let Some((identity, conf)) = voter {
            if identity.is_empty() || !valid_membership(&conf) {
                return Err(invalid("invalid replacement identity or membership"));
            }
            let mut batch = WriteBatch::default();
            batch.put(b"identity/v1", identity);
            batch.put(b"configuration", conf.write_to_bytes()?);
            batch.delete(CUT);
            write_sync(&db, batch)?;
            conf
        } else {
            neutral_conf
        };
        Ok(Self {
            db: Arc::new(db),
            conf,
            cold: Default::default(),
            anchor_writer: Arc::default(),
            history_pin: Arc::default(),
            meta: Arc::new(RwLock::new(Metadata {
                hard,
                last: head.index,
                last_term: head.term,
                checkpoint: None,
                compacted: 0,
                archived: 0,
            })),
        })
    }

    pub(super) fn validate_neutral(
        db: &DB,
        spec: &Spec,
        head: &Head,
    ) -> Result<(HardState, ConfState), ReplicaError> {
        let hard = HardState {
            term: head.term,
            commit: head.index,
            ..Default::default()
        };
        let neutral_conf = ConfState {
            voters: vec![1],
            ..Default::default()
        };
        if head.index == 0
            || head.index == u64::MAX
            || head.fenced
            || required(db, b"identity/v1")? != NEUTRAL
            || required(db, CUT)? != bincode::serialize(&(spec, head))?
            || required(db, b"last")? != head.index.to_be_bytes()
            || HardState::parse_from_bytes(&required(db, b"hard")?)? != hard
            || ConfState::parse_from_bytes(&required(db, b"configuration")?)? != neutral_conf
            || read_checkpoint(db)?.is_some()
            || snapshot::read_compacted(db)? != 0
            || db.get_pinned(reclaim::COLD)?.is_some()
            || archive::last_range(db)? != 0
            || decode_entry(head.index, &required(db, &entry_key(head.index))?)?.term != head.term
        {
            return Err(invalid("invalid portable Raft history or source identity"));
        }
        Ok((hard, neutral_conf))
    }

    fn proof_digest(
        &self,
        record: &[u8],
        fence: &Head,
        base: &RecoveryBase,
    ) -> Result<[u8; 32], ReplicaError> {
        if !fence.fenced
            || base.source.fenced
            || base.source.index == 0
            || base.source.index > fence.index
            || fence.index > self.committed_index()?
        {
            return Err(invalid("invalid recovery proof boundary"));
        }
        let mut hash = Sha256::new();
        hash.update(IMPORT);
        hash.update(Sha256::digest(required(&self.db, b"identity/v1")?));
        hash.update(record);
        let archived = self.metadata()?.archived;
        for head in [&base.source, fence] {
            let raw = self.raw_bytes(head.index, archived)?;
            if decode_entry(head.index, &raw)?.term != head.term {
                return Err(invalid("import proof differs from local Raft history"));
            }
            hash.update(Sha256::digest(raw));
        }
        Ok(hash.finalize().into())
    }
    pub(crate) fn save_import_proof(
        &self,
        base: &RecoveryBase,
        fence: &Head,
    ) -> Result<(), ReplicaError> {
        let mut bytes = bincode::serialize(&ImportProof {
            base: base.clone(),
            fence: fence.clone(),
        })?;
        bytes.extend(self.proof_digest(&bytes, fence, base)?);
        let mut batch = WriteBatch::default();
        batch.put(IMPORT, bytes);
        write_sync(&self.db, batch)
    }
    pub(crate) fn import_proof(&self) -> Result<Option<ImportProof>, ReplicaError> {
        let Some(bytes) = self.db.get(IMPORT)? else {
            return Ok(None);
        };
        let length = bytes.len();
        if !(33..=512).contains(&length) {
            return Err(invalid("invalid recovery proof length"));
        }
        let (body, checksum) = bytes.split_at(length - 32);
        let proof: ImportProof = bincode::deserialize(body)?;
        if checksum != self.proof_digest(body, &proof.fence, &proof.base)? {
            return Err(invalid("invalid local recovery proof checksum"));
        }
        Ok(Some(proof))
    }
}
