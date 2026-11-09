# RFC 0018 — Add one permanent Raft learner

## Problem

Each voting node currently needs an independent recovery journal to serve protected payments. We want a deployment that can keep accepting payments with one voter down and with the backup and broker offline.

## Proposed solution

Configure three voting members and one permanent learner in a single Raft group. Every member names the same learner, the same three voters and a unique TLS identity for each peer. Learner-group peer envelopes bind the generation, ledger definition and fixed roster. Members reject messages with a different binding, including snapshot transfers. Raft replicates entries to the learner using the existing log and snapshot transport. It applies the same deterministic state machine and stores the same receipt history. A learner has no vote, cannot become leader and has no public client listener.

Voters acknowledge a financial decision after the durable voting majority commits the entry and the leader applies it. A read waits for a leader ReadIndex quorum barrier and local application. Neither path contacts a destination on the learner. A timeout can still mean a command committed. Clients retry with their original request ID. The existing three-voter protected deployment continues to require a fresh independent journal confirmation. Select the contract when creating the group.

Membership is fixed per generation. Provision every member from one authoritative roster and run one process per member identity. Stop or isolate a failed learner before bringing up a replacement under that ID. Initialize a voting disk only at genesis. A returning voter must recover its durable vote and log. Changing a TOML file cannot promote a learner or attach one to an existing three-voter disk. For migration, quiesce clients, positively stop or isolate the old voting majority, verify and fence the old journal, restore all four members from the same cut into fresh directories with a new cluster ID, activate the journal from a restored voter before starting any replacement member if using POSIX, then start the new members and switch client endpoints. For offline restore and activation a voter administration config may specify the archive as `recovery_dir`. Omit that field in its live configuration. Activation verifies the restored cut, and the first new leader election can already advance beyond it. Use new credentials or network isolation to keep retired voters from serving clients. A journal fence alone cannot stop a voter that never consults the journal.

## Tradeoffs

A learner costs a full replica of the data, replication bandwidth and leader send capacity. A learner plus one voter cannot elect a leader or commit a payment. A running voting majority can continue while the learner is disconnected. If all live copies are lost before the independent archive catches up, acknowledged decisions after the archived cut can be lost. The length of that gap depends on current lag. There is no fixed recovery-point guarantee in async mode.

[Raft replication](0006-raft-replication.md) · [Async archive](0019-async-recovery-archive.md) · [Membership implementation](../../crates/ledger-replica/src/replica/membership.rs)
