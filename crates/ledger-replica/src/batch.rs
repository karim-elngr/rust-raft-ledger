use ledger_core::{Command, LedgerId, RequestId};

use crate::ReplicaError;
use crate::codec::{self, HEADER_BYTES, batch_header, encode_request};

/// Immutable, size-checked bytes for one request in a v1 ledger proposal.
#[derive(Debug)]
pub struct PreparedCommand {
    ledger: LedgerId,
    bytes: Box<[u8]>,
}

impl PreparedCommand {
    pub fn new(ledger: LedgerId, id: RequestId, command: &Command) -> Result<Self, ReplicaError> {
        Ok(Self {
            ledger,
            bytes: encode_request(id, command)?.into_boxed_slice(),
        })
    }

    /// Request ID and command bytes. The enclosing batch owns the shared header.
    pub fn encoded_len(&self) -> usize {
        self.bytes.len()
    }
}

/// A single Raft proposal for one ledger, in insertion order.
#[derive(Debug)]
pub struct ProposalBatch {
    ledger: LedgerId,
    count: u16,
    bytes: Vec<u8>,
}

impl ProposalBatch {
    pub const MAX_COMMANDS: usize = codec::MAX_COMMANDS;
    pub const MAX_ENCODED_BYTES: usize = codec::MAX_BATCH_BYTES;

    pub fn new(ledger: LedgerId) -> Self {
        Self {
            ledger,
            count: 0,
            bytes: batch_header(ledger, 0).to_vec(),
        }
    }

    /// Returns false when the count or byte limit would be exceeded. The caller
    /// can then submit this batch and add the same prepared command to a new one.
    pub fn push(&mut self, prepared: &PreparedCommand) -> Result<bool, ReplicaError> {
        if prepared.ledger != self.ledger {
            return Err(ReplicaError::Invalid(
                "command prepared for another ledger".into(),
            ));
        }
        if self.len() == Self::MAX_COMMANDS
            || prepared.encoded_len() > Self::MAX_ENCODED_BYTES - self.encoded_len()
        {
            return Ok(false);
        }
        self.bytes.extend_from_slice(&prepared.bytes);
        self.count += 1;
        Ok(true)
    }

    pub fn len(&self) -> usize {
        usize::from(self.count)
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Complete encoded size, including the header shared by all commands.
    pub fn encoded_len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn finish(mut self, expected: LedgerId) -> Result<Vec<u8>, ReplicaError> {
        if self.ledger != expected || self.is_empty() {
            return Err(ReplicaError::Invalid(
                "expected a nonempty batch for this ledger".into(),
            ));
        }
        self.bytes[..HEADER_BYTES].copy_from_slice(&batch_header(self.ledger, self.count));
        Ok(self.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{MAX_BATCH_BYTES, MAX_COMMANDS, encode_batch};
    use ledger_core::{AccountId, Funding, Line, ReservationId};

    const BOOK: LedgerId = LedgerId(1);

    fn void(id: u128) -> Command {
        Command::Void {
            reservation: ReservationId(id),
        }
    }

    #[test]
    fn assembling_prepared_commands_matches_the_existing_v1_encoding() {
        let commands = [(RequestId(2), void(3)), (RequestId(4), void(5))];
        let mut batch = ProposalBatch::new(BOOK);
        let header = batch.encoded_len();
        assert!(batch.is_empty());
        let mut bytes = header;
        for (id, command) in &commands {
            let prepared = PreparedCommand::new(BOOK, *id, command).unwrap();
            assert!(batch.push(&prepared).unwrap());
            bytes += prepared.encoded_len();
            assert_eq!(batch.encoded_len(), bytes);
        }
        assert_eq!(batch.len(), 2);
        assert_eq!(
            batch.finish(BOOK).unwrap(),
            encode_batch(BOOK, &commands).unwrap()
        );
    }

    #[test]
    fn rejects_mixed_ledgers_and_empty_proposals_without_mutating_the_batch() {
        let other = LedgerId(2);
        let prepared = PreparedCommand::new(BOOK, RequestId(1), &void(2)).unwrap();
        let mut batch = ProposalBatch::new(other);
        let before = batch.encoded_len();
        assert!(batch.push(&prepared).is_err());
        assert_eq!(batch.encoded_len(), before);
        assert!(batch.is_empty());
        assert!(batch.finish(other).is_err());
        let mut batch = ProposalBatch::new(BOOK);
        assert!(batch.push(&prepared).unwrap());
        assert!(batch.finish(other).is_err());
    }

    #[test]
    fn count_and_byte_overflow_leave_the_command_for_the_next_batch() {
        let prepared = PreparedCommand::new(BOOK, RequestId(4), &void(5)).unwrap();
        let mut batch = ProposalBatch::new(BOOK);
        for _ in 0..MAX_COMMANDS {
            assert!(batch.push(&prepared).unwrap());
        }
        let before = batch.encoded_len();
        assert!(!batch.push(&prepared).unwrap());
        assert_eq!(batch.len(), MAX_COMMANDS);
        assert_eq!(batch.encoded_len(), before);

        let post = |line_count| {
            let lines = vec![Line::credit(AccountId::new(BOOK, 7), 5); line_count];
            PreparedCommand::new(
                BOOK,
                RequestId(9),
                &Command::Post {
                    funding: Funding::Explicit(lines),
                    conditions: vec![],
                    corrects: None,
                },
            )
            .unwrap()
        };
        // V1 has a 23-byte header, 23 bytes per explicit request and 49 per line.
        // With 64 requests and 21,369 lines the complete batch is exactly 1 MiB.
        let mut batch = ProposalBatch::new(BOOK);
        for (length, count) in [(333, 7), (334, 57)] {
            let request = post(length);
            for _ in 0..count {
                assert!(batch.push(&request).unwrap());
            }
        }
        assert_eq!(batch.encoded_len(), MAX_BATCH_BYTES);
        assert!(!batch.push(&prepared).unwrap());
        assert_eq!(batch.len(), 64);
        assert_eq!(batch.encoded_len(), MAX_BATCH_BYTES);
        assert!(ProposalBatch::new(BOOK).push(&prepared).unwrap());
    }

    #[test]
    fn voter_checks_leadership_and_ledger_before_appending() {
        use crate::{Replica, ReplicaConfig};
        use raft::eraftpb::{Message, MessageType};
        let root = tempfile::tempdir().unwrap();
        let mut replica = Replica::bootstrap(
            root.path(),
            ReplicaConfig {
                cluster: 4,
                node: 1,
                voters: vec![3, 1, 2],
                ledger: BOOK,
                asset: "USD".into(),
                scale: 2,
            },
        )
        .unwrap();
        let start = |book| {
            let mut batch = ProposalBatch::new(book);
            assert!(
                batch
                    .push(&PreparedCommand::new(book, RequestId(2), &void(6)).unwrap())
                    .unwrap()
            );
            batch
        };
        assert_eq!(replica.node_id(), 1);
        assert_eq!(replica.cluster_id(), 4);
        assert_eq!(replica.voters(), &[1, 2, 3]);
        assert_eq!(replica.leader_id(), None);
        assert!(matches!(
            replica.propose_batch(start(BOOK)),
            Err(ReplicaError::NotLeader)
        ));
        replica.campaign().unwrap();
        for (vote, granted) in [
            (
                MessageType::MsgRequestPreVote,
                MessageType::MsgRequestPreVoteResponse,
            ),
            (
                MessageType::MsgRequestVote,
                MessageType::MsgRequestVoteResponse,
            ),
        ] {
            let request = replica
                .drain_ready()
                .unwrap()
                .into_iter()
                .find(|m| m.to == 2 && m.get_msg_type() == vote)
                .unwrap();
            let mut response = Message {
                from: 2,
                to: 1,
                term: request.term,
                ..Message::default()
            };
            response.set_msg_type(granted);
            replica.step(response).unwrap();
        }
        assert_eq!(replica.leader_id(), Some(1));
        assert!(replica.term() > 0);
        let initial = replica.last_index();
        assert!(replica.propose_batch(start(LedgerId(2))).is_err());
        assert!(replica.propose_batch(ProposalBatch::new(BOOK)).is_err());
        assert_eq!(replica.last_index(), initial);
        replica.propose_batch(start(BOOK)).unwrap();
        assert_eq!(replica.last_index(), initial + 1);
    }
}
