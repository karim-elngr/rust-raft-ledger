use bincode::Options;
use protobuf::Message;
use raft::eraftpb::Entry;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::JournalError;

pub const MAX_ENTRIES: usize = 64;
pub const MAX_ENTRY_BYTES: usize = (1 << 20) + 4096;
pub const MAX_SEGMENT_BYTES: usize = 8 << 20;
pub(crate) const MAX_HEAD_BYTES: usize = 256;
const SPEC_LIMIT: usize = 16 << 10;
const SEGMENT_VERSION: [u8; 8] = *b"LDGRseg1";
const HEAD_VERSION: [u8; 8] = *b"LDGRhed1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Spec {
    pub ledger: u128,
    pub asset: String,
    pub scale: u8,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Head {
    pub cluster: u128,
    pub index: u64,
    pub term: u64,
    pub digest: [u8; 32],
    pub fenced: bool,
}

pub(crate) type Hash = [u8; 32];

pub(crate) fn hash(bytes: &[u8]) -> Hash {
    Sha256::digest(bytes).into()
}

pub(crate) fn invalid(reason: &str) -> JournalError {
    JournalError::Invalid(reason.into())
}

pub(crate) fn spec_hash(spec: &Spec) -> Result<Hash, JournalError> {
    if spec.asset.trim().is_empty() || spec.asset.len() > SPEC_LIMIT {
        return Err(invalid("invalid recovery asset"));
    }
    Ok(hash(&bincode::serialize(&(b"LDGRspc1", spec))?))
}

pub(crate) fn encode_head(spec: Hash, head: &Head) -> Result<Vec<u8>, JournalError> {
    let mut bytes = bincode::serialize(&(HEAD_VERSION, spec, head))?;
    bytes.extend_from_slice(&hash(&bytes));
    Ok(bytes)
}

pub(crate) fn decode_head(spec: Hash, bytes: &[u8]) -> Result<Head, JournalError> {
    let split = bytes
        .len()
        .checked_sub(32)
        .ok_or_else(|| invalid("truncated journal head"))?;
    let (body, digest) = bytes.split_at(split);
    if hash(body) != digest {
        return Err(invalid("journal head checksum mismatch"));
    }
    let (version, actual, head): ([u8; 8], Hash, Head) = bincode::options()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .deserialize(body)?;
    if version != HEAD_VERSION
        || actual != spec
        || head.cluster == 0
        || (head.index == 0) != (head.term == 0)
    {
        return Err(invalid("incompatible journal head"));
    }
    Ok(head)
}

#[derive(Serialize, Deserialize)]
pub(crate) enum Change {
    Genesis { spec: Spec, cluster: u128 },
    Append(u16),
    Fence,
    Activate(u128),
}

#[derive(Serialize, Deserialize)]
struct Header {
    version: [u8; 8],
    spec: Hash,
    prior: Option<Head>,
    change: Change,
}

impl Header {
    fn tip(&self, digest: Hash, entries: &[Entry]) -> Result<Head, JournalError> {
        match (&self.prior, &self.change) {
            (None, Change::Genesis { spec, cluster })
                if *cluster > 0 && entries.is_empty() && spec_hash(spec)? == self.spec =>
            {
                Ok(Head {
                    cluster: *cluster,
                    index: 0,
                    term: 0,
                    digest,
                    fenced: false,
                })
            }
            (Some(prior), Change::Append(count))
                if !prior.fenced
                    && !entries.is_empty()
                    && entries.len() == usize::from(*count)
                    && entries.len() <= MAX_ENTRIES =>
            {
                let mut next = Head {
                    digest,
                    ..prior.clone()
                };
                for entry in entries {
                    if next.index.checked_add(1) != Some(entry.index)
                        || entry.term == 0
                        || entry.term < next.term
                    {
                        return Err(invalid("journal entries are not a contiguous Raft prefix"));
                    }
                    next.index = entry.index;
                    next.term = entry.term;
                }
                Ok(next)
            }
            (Some(prior), Change::Fence) if !prior.fenced && entries.is_empty() => Ok(Head {
                digest,
                fenced: true,
                ..prior.clone()
            }),
            (Some(prior), Change::Activate(cluster))
                if prior.fenced
                    && *cluster != 0
                    && *cluster != prior.cluster
                    && entries.is_empty() =>
            {
                Ok(Head {
                    digest,
                    cluster: *cluster,
                    fenced: false,
                    ..prior.clone()
                })
            }
            _ => Err(invalid("invalid recovery journal transition")),
        }
    }
}

pub(crate) fn encode_segment(
    spec: Hash,
    prior: Option<&Head>,
    change: Change,
    entries: &[Entry],
) -> Result<(Head, Vec<u8>), JournalError> {
    let header = Header {
        version: SEGMENT_VERSION,
        spec,
        prior: prior.cloned(),
        change,
    };
    let mut head = header.tip([0; 32], entries)?;
    let mut bytes = bincode::serialize(&header)?;
    for entry in entries {
        let len = entry.compute_size() as usize;
        if len > MAX_ENTRY_BYTES || bytes.len() + 4 + len > MAX_SEGMENT_BYTES {
            return Err(invalid("journal segment exceeds byte limit"));
        }
        bytes.extend_from_slice(&(len as u32).to_le_bytes());
        entry.write_to_vec(&mut bytes)?;
    }
    head.digest = hash(&bytes);
    Ok((head, bytes))
}

pub(crate) fn encode_raw_append(
    spec: Hash,
    prior: &Head,
    rows: &[&[u8]],
) -> Result<(Head, Vec<u8>), JournalError> {
    if rows.is_empty() || rows.len() > MAX_ENTRIES {
        return Err(invalid("invalid journal entry count"));
    }
    let header = Header {
        version: SEGMENT_VERSION,
        spec,
        prior: Some(prior.clone()),
        change: Change::Append(rows.len() as u16),
    };
    let mut bytes = bincode::serialize(&header)?;
    let mut entries = Vec::with_capacity(rows.len());
    for row in rows {
        if row.len() > MAX_ENTRY_BYTES || bytes.len() + 4 + row.len() > MAX_SEGMENT_BYTES {
            return Err(invalid("journal segment exceeds byte limit"));
        }
        entries.push(Entry::parse_from_bytes(row)?);
        bytes.extend_from_slice(&(row.len() as u32).to_le_bytes());
        bytes.extend_from_slice(row);
    }
    Ok((header.tip(hash(&bytes), &entries)?, bytes))
}

/// An authenticated journal group with decoded entries and unchanged wire bytes.
pub struct Segment {
    pub(crate) prior: Option<Head>,
    pub(crate) entries: Vec<Entry>,
    bytes: Vec<u8>,
    frames: Vec<std::ops::Range<usize>>,
}

impl Segment {
    pub(crate) fn original(&self, slot: usize) -> &[u8] {
        &self.bytes[self.frames[slot].clone()]
    }
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }
    pub fn encoded(&self) -> impl ExactSizeIterator<Item = &[u8]> {
        self.frames.iter().map(|range| &self.bytes[range.clone()])
    }
}

pub(crate) fn decode_segment(
    spec: Hash,
    cut: &Head,
    bytes: Vec<u8>,
) -> Result<Segment, JournalError> {
    if hash(&bytes) != cut.digest {
        return Err(invalid("journal segment checksum mismatch"));
    }
    let mut body = bytes.as_slice();
    let header: Header = bincode::options()
        .with_fixint_encoding()
        .with_limit(MAX_SEGMENT_BYTES as u64)
        .deserialize_from(&mut body)?;
    if header.version != SEGMENT_VERSION || header.spec != spec {
        return Err(invalid("incompatible journal segment"));
    }
    let count = match header.change {
        Change::Append(n) => usize::from(n),
        _ => 0,
    };
    if count > MAX_ENTRIES {
        return Err(invalid("too many entries in journal segment"));
    }
    let mut entries = Vec::with_capacity(count);
    let mut frames = Vec::with_capacity(count);
    for _ in 0..count {
        let (size, rest) = body
            .split_at_checked(4)
            .ok_or_else(|| invalid("truncated journal entry length"))?;
        let len = u32::from_le_bytes(size.try_into().expect("fixed width")) as usize;
        if len > MAX_ENTRY_BYTES {
            return Err(invalid("oversized journal entry"));
        }
        let (message, rest) = rest
            .split_at_checked(len)
            .ok_or_else(|| invalid("truncated journal entry"))?;
        let end = bytes.len() - rest.len();
        frames.push(end - len..end);
        entries.push(Entry::parse_from_bytes(message)?);
        body = rest;
    }
    if !body.is_empty() || header.tip(cut.digest, &entries)? != *cut {
        return Err(invalid("journal segment does not match its head"));
    }
    Ok(Segment {
        prior: header.prior,
        entries,
        bytes,
        frames,
    })
}
