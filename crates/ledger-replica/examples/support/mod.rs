//! A single real voter and a simulated acknowledging peer. Used only to build
//! local projection fixtures; every command is encoded, persisted and applied
//! by a real Replica before a point lookup or benchmark timer starts.
use ledger_core::{AccountId, Balance, Bounds, Command, LedgerId, RequestId, Side};
use ledger_replica::{Replica, ReplicaConfig};
use raft::eraftpb::{Message, MessageType};

pub const LEDGER: LedgerId = LedgerId(19);

pub fn config() -> ReplicaConfig {
    ReplicaConfig {
        cluster: 1,
        node: 1,
        voters: vec![1, 2, 3],
        ledger: LEDGER,
        asset: "USD".into(),
        scale: 2,
    }
}

pub fn create(number: u128, normal: Side) -> (RequestId, Command) {
    (
        RequestId(number),
        Command::CreateAccount {
            account: AccountId::new(LEDGER, number),
            code: 1,
            normal,
            bounds: Bounds {
                min: Some(Balance::positive(0)),
                max: None,
            },
        },
    )
}

pub fn elect(voter: &mut Replica) {
    let election_entry = voter.last_index() + 1;
    voter.campaign().unwrap();
    drive(voter, election_entry);
    assert!(voter.is_leader());
}

pub fn commit(voter: &mut Replica, commands: &[(RequestId, Command)]) {
    let entry = voter.last_index() + 1;
    voter.propose(commands).unwrap();
    drive(voter, entry);
}

fn drive(voter: &mut Replica, index: u64) {
    for _ in 0..32 {
        let messages = voter.drain_ready().unwrap();
        if voter.applied_index() == index {
            return;
        }
        for request in messages.into_iter().filter(|m| m.to == 2) {
            let kind = match request.get_msg_type() {
                MessageType::MsgRequestPreVote => MessageType::MsgRequestPreVoteResponse,
                MessageType::MsgRequestVote => MessageType::MsgRequestVoteResponse,
                MessageType::MsgAppend => MessageType::MsgAppendResponse,
                MessageType::MsgHeartbeat => MessageType::MsgHeartbeatResponse,
                _ => continue,
            };
            let mut response = Message {
                from: request.to,
                to: request.from,
                term: request.term,
                index: request
                    .entries
                    .last()
                    .map_or(request.index, |entry| entry.index),
                ..Message::default()
            };
            response.set_msg_type(kind);
            voter.step(response).unwrap();
        }
    }
    panic!("local voter did not apply index {index}");
}
