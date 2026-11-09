use super::{CheckpointBoundary, ReplicaError, invalid};
use std::collections::BTreeMap;
use std::fs::Metadata;
use std::io::Read;

// The format tag fixes the projection schema, bytewise comparator and the
// RocksDB 10.4.2 format used for these local checkpoints.
const MAGIC: &[u8] = b"ledger-checkpoint-v1\0projection-v2\0rocksdb-10.4.2-bytewise\0";
const WITH_LEARNER: &[u8] =
    b"ledger-checkpoint-v2-learner\0projection-v2\0rocksdb-10.4.2-bytewise\0";
pub(crate) const MAX_MANIFEST: usize = 64 << 20;
pub(crate) const MAX_FILES: usize = 262_144;
const MAX_ASSET: usize = 1 << 20;
pub(super) type Hash = [u8; 32];

pub(crate) fn immutable(name: &str) -> bool {
    name.split_once('.')
        .is_some_and(|(id, suffix)| digits(id) && matches!(suffix, "sst" | "blob"))
}
fn digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit()) && s.parse::<u64>().is_ok()
}
pub(crate) fn file_name(name: &str) -> bool {
    name.len() <= 255
        && (immutable(name)
            || name == "CURRENT"
            || name == "IDENTITY"
            || ["MANIFEST-", "OPTIONS-"]
                .iter()
                .any(|prefix| name.strip_prefix(prefix).is_some_and(digits))
            || name.strip_suffix(".log").is_some_and(digits))
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Stamp {
    device: u64,
    inode: u64,
    seconds: i64,
    nanos: i64,
}
impl Stamp {
    pub(super) fn of(meta: &Metadata) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Self {
                device: meta.dev(),
                inode: meta.ino(),
                seconds: meta.mtime(),
                nanos: meta.mtime_nsec(),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = meta;
            Self::default()
        }
    }
    pub(super) fn same_inode(self, other: Self) -> bool {
        self.inode != 0 && self.inode == other.inode && self.device == other.device
    }
    pub(super) fn matches(self, other: Self) -> bool {
        self.same_inode(other) && self == other
    }
}

#[derive(Clone)]
pub(super) struct Record {
    pub size: u64,
    pub hash: Hash,
    pub stamp: Stamp,
}
pub(super) struct Manifest {
    pub boundary: CheckpointBoundary,
    pub files: BTreeMap<String, Record>,
}

fn read<const N: usize>(r: &mut &[u8]) -> Result<[u8; N], ReplicaError> {
    let mut value = [0; N];
    r.read_exact(&mut value)?;
    Ok(value)
}
fn u64_field(r: &mut &[u8]) -> Result<u64, ReplicaError> {
    Ok(u64::from_le_bytes(read(r)?))
}
fn i64_field(r: &mut &[u8]) -> Result<i64, ReplicaError> {
    Ok(i64::from_le_bytes(read(r)?))
}
fn u32_field(r: &mut &[u8]) -> Result<usize, ReplicaError> {
    Ok(u32::from_le_bytes(read(r)?) as usize)
}
fn string(r: &mut &[u8], limit: usize) -> Result<String, ReplicaError> {
    let n = u32_field(r)?;
    if n > limit {
        return Err(invalid("oversized checkpoint string"));
    }
    let (head, tail) = r
        .split_at_checked(n)
        .ok_or_else(|| invalid("short checkpoint string"))?;
    *r = tail;
    Ok(std::str::from_utf8(head)
        .map_err(|_| invalid("invalid checkpoint text"))?
        .to_owned())
}
fn write_str(out: &mut Vec<u8>, s: &str) {
    out.extend((s.len() as u32).to_le_bytes());
    out.extend(s.as_bytes());
}

impl Manifest {
    pub fn bytes(&self) -> Result<Vec<u8>, ReplicaError> {
        let b = &self.boundary;
        if b.asset.len() > MAX_ASSET || b.voters.len() != 3 || self.files.len() > MAX_FILES {
            return Err(invalid("checkpoint exceeds format limits"));
        }
        let mut out = if b.learner.is_some() {
            WITH_LEARNER
        } else {
            MAGIC
        }
        .to_vec();
        out.extend(b.cluster.to_le_bytes());
        out.extend(b.ledger.to_le_bytes());
        write_str(&mut out, &b.asset);
        out.push(b.scale);
        for voter in &b.voters {
            out.extend(voter.to_le_bytes());
        }
        if let Some(id) = b.learner {
            out.extend(id.to_le_bytes());
        }
        out.extend(b.index.to_le_bytes());
        out.extend(b.term.to_le_bytes());
        out.extend((self.files.len() as u32).to_le_bytes());
        for (name, file) in &self.files {
            if !file_name(name) {
                return Err(invalid("unknown checkpoint filename"));
            }
            write_str(&mut out, name);
            out.extend(file.size.to_le_bytes());
            out.extend(file.hash);
            out.extend(file.stamp.device.to_le_bytes());
            out.extend(file.stamp.inode.to_le_bytes());
            out.extend(file.stamp.seconds.to_le_bytes());
            out.extend(file.stamp.nanos.to_le_bytes());
            if out.len() > MAX_MANIFEST {
                return Err(invalid("oversized checkpoint manifest"));
            }
        }
        Ok(out)
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, ReplicaError> {
        let (mut r, with_learner) = if let Some(r) = bytes.strip_prefix(MAGIC) {
            (r, false)
        } else if let Some(r) = bytes.strip_prefix(WITH_LEARNER) {
            (r, true)
        } else {
            return Err(invalid("incompatible checkpoint format"));
        };
        let cluster = u128::from_le_bytes(read(&mut r)?);
        let ledger = u128::from_le_bytes(read(&mut r)?);
        let asset = string(&mut r, MAX_ASSET)?;
        let [scale] = read(&mut r)?;
        let voters = vec![u64_field(&mut r)?, u64_field(&mut r)?, u64_field(&mut r)?];
        let learner = if with_learner {
            Some(u64_field(&mut r)?)
        } else {
            None
        };
        let index = u64_field(&mut r)?;
        let term = u64_field(&mut r)?;
        if cluster == 0
            || asset.trim().is_empty()
            || voters[0] == 0
            || voters.windows(2).any(|v| v[0] >= v[1])
            || learner.is_some_and(|id| id == 0 || voters.contains(&id))
            || (index == 0) != (term == 0)
        {
            return Err(invalid("invalid checkpoint boundary"));
        }
        let count = u32_field(&mut r)?;
        if count > MAX_FILES {
            return Err(invalid("too many checkpoint files"));
        }
        let mut files: BTreeMap<String, Record> = BTreeMap::new();
        for _ in 0..count {
            let name = string(&mut r, 255)?;
            if !file_name(&name)
                || files
                    .last_key_value()
                    .is_some_and(|(last, _)| last >= &name)
            {
                return Err(invalid("invalid or unsorted checkpoint file list"));
            }
            let size = u64_field(&mut r)?;
            let hash = read(&mut r)?;
            let stamp = Stamp {
                device: u64_field(&mut r)?,
                inode: u64_field(&mut r)?,
                seconds: i64_field(&mut r)?,
                nanos: i64_field(&mut r)?,
            };
            files.insert(name, Record { size, hash, stamp });
        }
        if !r.is_empty() {
            return Err(invalid("trailing checkpoint manifest data"));
        }
        Ok(Self {
            boundary: CheckpointBoundary {
                cluster,
                ledger,
                asset,
                scale,
                voters,
                learner,
                index,
                term,
            },
            files,
        })
    }
}
