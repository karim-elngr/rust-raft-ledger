use super::*;
use ledger_core::{Bounds, Funding, Line, Side};

const BOOK: LedgerId = LedgerId(82);
const DEBIT: AccountId = AccountId::new(BOOK, 1);
const CREDIT: AccountId = AccountId::new(BOOK, 2);
const PAYMENT: RequestId = RequestId(13);

fn cfg(node: u64) -> ReplicaConfig {
    ReplicaConfig {
        node,
        cluster: 7,
        voters: vec![1, 2, 3],
        ledger: BOOK,
        asset: "EUR".into(),
        scale: 2,
    }
}

fn append(voter: &mut Replica, index: u64, term: u64, commands: &[(RequestId, Command)]) {
    let previous = index - 1;
    let mut message = Message {
        from: 3,
        to: voter.node_id(),
        term,
        index: previous,
        log_term: voter.committed_source().term(previous).unwrap(),
        commit: index,
        ..Default::default()
    };
    message.set_msg_type(MessageType::MsgAppend);
    message.entries.push(Entry {
        index,
        term,
        data: encode_batch(BOOK, commands).unwrap().into(),
        ..Default::default()
    });
    voter.step(message).unwrap();
    voter.drain_ready().unwrap();
    assert_eq!(voter.applied_index(), index);
}

fn stage(donor: &Replica, recipient: &mut Replica, cut: CheckpointRef) {
    let source = donor.snapshot_files().unwrap().source(cut).unwrap();
    let mut receiver = recipient
        .snapshot_files()
        .unwrap()
        .receive(
            cut,
            source.manifest(),
            recipient.current_checkpoint().unwrap(),
        )
        .unwrap();
    while let Some(chunk) = receiver.next_range().unwrap() {
        let bytes = source
            .read_file(chunk.file, chunk.offset, chunk.length)
            .unwrap();
        receiver.write_chunk(chunk, &bytes).unwrap();
    }
    let image = receiver.finish().unwrap();
    let history = recipient.snapshot_history().begin(cut).unwrap();
    let source = donor.snapshot_history();
    while history.resume_after().unwrap() < cut.index {
        let page = source
            .entries_after(cut, history.resume_after().unwrap())
            .unwrap();
        history.append(&page).unwrap();
    }
    history.finish().unwrap();
    let mut message = Message {
        from: donor.node_id(),
        to: recipient.node_id(),
        term: 3,
        ..Default::default()
    };
    message.set_msg_type(MessageType::MsgSnapshot);
    // This is the actual offer obtained through the raft-rs Storage API.
    message.set_snapshot(
        donor
            .raw
            .store()
            .snapshot(cut.index, recipient.node_id())
            .unwrap(),
    );
    assert!(recipient.step_snapshot(message, image, history).unwrap());
}

#[test]
fn restart_selects_the_old_or_new_payment_view_at_the_durable_snapshot_ready_boundary() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("recipient");
    let mut recipient = Replica::bootstrap(&path, cfg(2)).unwrap();
    let mut donor = Replica::bootstrap(&root.path().join("donor"), cfg(1)).unwrap();
    let create = [(DEBIT, Side::Debit), (CREDIT, Side::Credit)].map(|(account, normal)| {
        (
            RequestId(account.number),
            Command::CreateAccount {
                account,
                normal,
                code: 1,
                bounds: Bounds::default(),
            },
        )
    });
    append(&mut recipient, 1, 1, &create);
    append(&mut donor, 1, 1, &create);
    let old = recipient
        .begin_checkpoint()
        .unwrap()
        .unwrap()
        .finish()
        .unwrap();
    let payment = Command::Post {
        funding: Funding::Explicit(vec![Line::debit(DEBIT, 315), Line::credit(CREDIT, 315)]),
        conditions: vec![],
        corrects: None,
    };
    let commands = [(PAYMENT, payment.clone())];
    append(&mut donor, 2, 2, &commands);
    let cut = donor.begin_checkpoint().unwrap().unwrap().finish().unwrap();
    let result = donor
        .outcome_for(PAYMENT, &payment)
        .unwrap()
        .unwrap()
        .unwrap();
    let canonical = donor
        .committed_source()
        .entries_after(0, cut.index)
        .unwrap();

    for commit_ready in [false, true] {
        stage(&donor, &mut recipient, cut);
        let ready = recipient.raw.ready();
        assert_eq!(CheckpointRef::from_snapshot(ready.snapshot()).unwrap(), cut);
        let incoming = recipient.incoming_snapshot.take().unwrap();
        let local = incoming.image.reference();
        let new_view = Projection::from_checkpoint(
            &path.join("peer-state"),
            BOOK,
            cut.index,
            recipient.cache_budget,
            |dest| incoming.image.materialize(dest),
        )
        .unwrap();
        assert_eq!(new_view.applied_index(), cut.index);
        assert!(recipient.outcome_for(PAYMENT, &payment).unwrap().is_none());
        if commit_ready {
            recipient
                .raw
                .store()
                .persist_snapshot_ready(
                    ready.snapshot(),
                    local,
                    &incoming.history,
                    ready.entries(),
                    ready.hs(),
                )
                .unwrap();
        }
        // Crash before the live projection swap and before sending Raft's reply.
        drop((new_view, incoming, ready, recipient));
        recipient = Replica::reopen(&path, cfg(2)).unwrap();
        let expected = if commit_ready { local } else { old };
        assert_eq!(recipient.current_checkpoint().unwrap(), Some(expected));
        assert_eq!(recipient.recovered_from(), expected.index);
        assert_eq!(recipient.applied_index(), expected.index);
        if commit_ready {
            assert_eq!(
                recipient.outcome_for(PAYMENT, &payment).unwrap(),
                Some(Ok(result.clone()))
            );
            assert_eq!(
                recipient.account(CREDIT).unwrap(),
                donor.account(CREDIT).unwrap()
            );
            recipient.committed_source().verify(&canonical).unwrap();
        } else {
            assert!(recipient.decision(PAYMENT).unwrap().is_none());
            assert_eq!(
                recipient.account(CREDIT).unwrap().unwrap().counters(),
                Counters::default()
            );
        }
    }
    // A new committed retry must return the original entry without posting twice.
    append(&mut recipient, 3, 4, &commands);
    assert_eq!(
        recipient.outcome_for(PAYMENT, &payment).unwrap(),
        Some(Ok(result))
    );
    assert_eq!(
        recipient.account(DEBIT).unwrap(),
        donor.account(DEBIT).unwrap()
    );
}
