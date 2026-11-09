# RFC 0006 — Raft replication and current reads

The following describes the three-voter deployment. Groups configured with a permanent learner use quorum acknowledgements and can export asynchronously. [Learner topology](0018-static-raft-learner.md) · [Async archive](0019-async-recovery-archive.md)


**Status** — Implemented with raft-rs and a fixed group of three voters. Protected writes and current reads continue through a connected majority with the independent recovery journal available. Physical placement remains open. [Raft driver](../../crates/ledger-replica/src/replica.rs) · [Read barriers](../../crates/ledger-node/src/reads.rs)

## Problem

Two withdrawals can each pass a balance check while competing for the same funds. The replicas need one order for deciding which withdrawal succeeds. After a change of leader, a client must be able to retry a write or read its outcome without seeing an obsolete balance.

## Proposed solution

Start with one fixed group of three voting nodes. The Raft driver handles peer messages and election timers and routes writes to the leader. A versioned Raft entry names one ledger and contains an ordered sequence of request IDs and commands. Every account affected by a command belongs to the same group. [Microbatching and group commit](0011-microbatching-and-group-commit.md)

Bootstrap the group once, with a unique cluster ID, a distinct persistent node ID on each voter and the same three-voter membership. An ordinary restart must reopen that voter's durable term, vote, membership and log. Treat a missing database for a known voter as a recovery event. Bootstrapping an empty replacement with the same identity could let the voter forget an earlier vote. Replacement after permanent disk loss requires its own membership or recovery procedure. [raft-rs initial state](https://docs.rs/raft/0.7.0/raft/storage/trait.Storage.html#tymethod.initial_state)

Each replica applies committed entries in index order and commands in entry order. The Rust ledger core evaluates each command against the state produced by its predecessors. Replicas use the same checked arithmetic and validation order, including a deterministic choice of error when multiple rules fail. The log carries the IDs, amounts and instructions needed to reproduce each decision. Reply once Raft has committed the entry with durable replication, the leader has persisted its commit frontier, its locally applied results are published and the independently recoverable committed prefix covers the result. A timeout leaves the outcome unknown. [Request identity](0005-request-identity.md) · [Durable recovery](0007-durable-recovery.md)

Version the persisted ledger command independently of the public RPC envelope and Raft peer messages. Keep old command decoders for log replay. Enable a new command version only after the voters can all apply it. A replica encountering an unsupported committed version stops at the preceding applied index so it cannot silently change a financial rule. [Public client protocol](0009-client-protocol.md) · [Peer transport](0008-peer-transport.md)

Use a single owner for each `RawNode`. Start with the synchronous Raft-store persistence contract in raft-rs 0.7. Take the committed entries from `Ready`, persist its required Raft state and release persistence-dependent peer messages only after success. Use `advance_append` to report persistence. Process the resulting `LightReady` messages, commit update and additional committed entries. Apply both sets of committed entries in index order. During this process, call `advance_apply_to` only through the index fully published by the local state machine. On restart, initialize from the sealed recovery baseline in RFC 0007. [raft-rs RawNode advancement](https://docs.rs/raft/0.7.0/raft/raw_node/struct.RawNode.html#method.advance_append)

For a current balance, entry or request-outcome lookup, obtain a quorum-confirmed read index and match the response to that read. Wait until the serving replica has published through the index, then pin a coherent view of the ledger and account cache for the lookup. Confirm protection of that actual view's index in the active recovery generation before replying. [Offsite commit protection](0015-offsite-commit-protection.md) An isolated former leader times out or redirects. [Account cache](0010-account-cache.md) · [raft-rs read-index contract](https://docs.rs/raft/0.7.0/raft/raw_node/struct.RawNode.html#method.read_index)

## Tradeoffs

A single group gives a compound posting one commit boundary. Two connected voters can elect a leader and continue after one failure while offsite protection remains available. Losing the majority prevents new commits and quorum-confirmed reads. The leader's write capacity bounds throughput, and current reads incur quorum and local apply latency. Workload and latency targets remain open. Separate groups for independent books are a future option if measurements justify them and every entry fits within one book.

## Acceptance criteria

- Submit two withdrawals that each fit the initial balance but exceed it together. Verify that the first in committed order succeeds and that all replicas agree on both results and the ending balances.
- Replay the same committed commands independently, including a command that violates multiple rules. At an equal applied index, compare journal entries, all four counters, reservations, funding allocations and exact request outcomes.
- Commit a transfer and kill the leader before it replies. Retry through the newly elected leader, restart the old leader, and verify that the original entry ID is returned and every replica contains exactly one transfer.
- Isolate the former leader, complete a write on the majority, then issue a current read to the isolated node. Verify timeout or redirection. A successful current read through the majority includes the write.
- Stop each voter in turn, including the current leader. After any necessary election, the surviving pair must serve new writes and preserve acknowledged outcomes. Restart the stopped voter from its existing database and verify convergence. A blank disk launched as an existing voter must fail ordinary restart.
