use super::*;
use crate::{CacheBudget, Replica, ReplicaConfig, codec::encode_batch, projection::Projection};
use ledger_core::{AccountId, Bounds, Command, LedgerId, RequestId, Side};
use raft::eraftpb::{Entry, Message, MessageType};

fn cfg(node: u64) -> ReplicaConfig {
    ReplicaConfig {
        cluster: 7,
        ledger: LedgerId(10),
        node,
        asset: "USD".into(),
        scale: 2,
        voters: vec![1, 2, 3],
    }
}
fn advance(voter: &mut Replica) {
    let index = voter.applied_index() + 1;
    let id = AccountId::new(voter.ledger_id(), u128::from(index));
    let command = Command::CreateAccount {
        account: id,
        code: 123,
        normal: Side::Credit,
        bounds: Bounds::default(),
    };
    let mut append = Message {
        from: 3,
        to: voter.node_id(),
        term: 2,
        index: index - 1,
        log_term: if index == 1 { 0 } else { 2 },
        commit: index,
        ..Default::default()
    };
    append.set_msg_type(MessageType::MsgAppend);
    append.entries.push(Entry {
        index,
        term: 2,
        data: encode_batch(voter.ledger_id(), &[(RequestId(id.number), command)])
            .unwrap()
            .into(),
        ..Default::default()
    });
    voter.step(append).unwrap();
    voter.drain_ready().unwrap();
    assert_eq!(voter.applied_index(), index);
}
fn seal(v: &mut Replica) -> CheckpointRef {
    v.begin_checkpoint().unwrap().unwrap().finish().unwrap()
}
#[test]
fn replay_authenticates_local_and_incoming_history_while_live_state_stays_private() {
    let root = tempfile::tempdir().unwrap();
    let mut donor = Replica::bootstrap(&root.path().join("sender"), cfg(1)).unwrap();
    let receiver_path = root.path().join("receiver");
    let mut voter = Replica::bootstrap(&receiver_path, cfg(2)).unwrap();
    advance(&mut donor);
    advance(&mut voter);
    let base = seal(&mut voter);
    advance(&mut voter); // Already committed locally beyond its sealed state.
    advance(&mut donor);
    advance(&mut donor);
    let remote = seal(&mut donor);
    let offer = donor.snapshot_files().unwrap().source(remote).unwrap();
    let sender = donor.snapshot_history();
    let proof = sender.replay_proof(remote, base).unwrap().unwrap();
    let target = voter.snapshot_files().unwrap();
    let mut receive = target
        .receive(remote, offer.manifest(), Some(base))
        .unwrap();
    assert_eq!(receive.replay_base(), Some(base));
    let foreign = sender.begin(remote).unwrap();
    assert!(receive.replay(&foreign, proof).is_err());
    let archive = voter.snapshot_history();
    let stage = archive.begin(remote).unwrap();
    assert_eq!(stage.resume_after().unwrap(), 2);
    stage
        .append(&sender.entries_after(remote, 2).unwrap())
        .unwrap();
    let image = receive.replay(&stage, proof).unwrap().unwrap();
    assert_eq!(image.source_reference(), remote);
    assert_eq!(voter.current_checkpoint().unwrap(), Some(base));
    assert_eq!(voter.applied_index(), 2);
    let recovered = Projection::from_checkpoint(
        &root.path().join("inspection"),
        cfg(2).ledger,
        3,
        CacheBudget::default(),
        |p| image.materialize(p),
    )
    .unwrap();
    for n in 1..=3 {
        let id = AccountId::new(cfg(2).ledger, n);
        assert_eq!(recovered.account(id).unwrap().unwrap().code(), 123);
    }
    assert!(
        !receiver_path
            .join("checkpoints/incoming-receipt-v1")
            .exists(),
        "a logical image is not an exact physical-file receipt"
    );
    drop(receive);
    let files = CheckpointFiles::new(&receiver_path).unwrap();
    let local = image.reference();
    let tomorrow = std::time::SystemTime::now() + std::time::Duration::from_secs(86_401);
    assert!(
        !target.expire_at(tomorrow).unwrap(),
        "owner still holds the staged history"
    );
    assert!(files.generation(&local).exists());
    drop((stage, image));
    assert!(
        target.expire_at(tomorrow).unwrap(),
        "an unadopted offer expires"
    );
    assert!(!files.generation(&local).exists());
    assert!(files.generation(&base).exists());
}

#[test]
fn a_bad_replay_base_falls_back_and_a_partial_physical_transfer_stays_physical() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("receiver");
    let mut voter = Replica::bootstrap(&path, cfg(2)).unwrap();
    let mut donor = Replica::bootstrap(&root.path().join("sender"), cfg(1)).unwrap();
    advance(&mut voter);
    let base = seal(&mut voter);
    advance(&mut donor);
    advance(&mut donor);
    let remote = seal(&mut donor);
    let sender = donor.snapshot_history();
    let proof = sender.replay_proof(remote, base).unwrap().unwrap();
    let offer = donor.snapshot_files().unwrap().source(remote).unwrap();
    let target = voter.snapshot_files().unwrap();
    let mut receive = target
        .receive(remote, offer.manifest(), Some(base))
        .unwrap();
    assert_eq!(receive.replay_base(), Some(base));
    let stage = voter.snapshot_history().begin(remote).unwrap();
    stage
        .append(&sender.entries_after(remote, 1).unwrap())
        .unwrap();
    let files = CheckpointFiles::new(&path).unwrap();
    let db = files.generation(&base).join("db");
    let name = super::db_names(&db)
        .unwrap()
        .into_iter()
        .find(|n| immutable(n))
        .unwrap();
    let sst = db.join(name);
    let saved = fs::read(&sst).unwrap();
    let mut corrupted = saved.clone();
    corrupted[0] ^= 1;
    let timestamp = fs::metadata(&sst).unwrap().modified().unwrap();
    fs::write(&sst, &corrupted).unwrap();
    File::open(&sst).unwrap().set_modified(timestamp).unwrap();
    assert!(
        receive.replay(&stage, [0; 32]).is_err(),
        "bad local SSTs cannot waive a disagreeing remote history proof"
    );
    assert!(
        stage.finish().is_err(),
        "divergent staged history was invalidated"
    );
    drop((stage, receive));
    let mut receive = target
        .receive(remote, offer.manifest(), Some(base))
        .unwrap();
    let stage = voter.snapshot_history().begin(remote).unwrap();
    stage
        .append(&sender.entries_after(remote, 1).unwrap())
        .unwrap();
    assert!(receive.replay(&stage, proof).unwrap().is_none());
    fs::write(&sst, saved).unwrap();
    let chunk = receive.next_range().unwrap().unwrap();
    receive
        .write_chunk(
            chunk,
            &offer
                .read_file(chunk.file, chunk.offset, chunk.length)
                .unwrap(),
        )
        .unwrap();
    drop(receive);
    let mut resumed = target
        .receive(remote, offer.manifest(), Some(base))
        .unwrap();
    assert_eq!(resumed.replay_base(), None);
    while let Some(r) = resumed.next_range().unwrap() {
        resumed
            .write_chunk(r, &offer.read_file(r.file, r.offset, r.length).unwrap())
            .unwrap();
    }
    assert_eq!(resumed.finish().unwrap().source_reference(), remote);
}
