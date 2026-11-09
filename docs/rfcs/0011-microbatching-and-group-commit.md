# RFC 0011 — Microbatching and group commit

**Status** — Leader-side microbatching and shared Raft WAL writes are implemented. Isolated and burst submissions are covered by integration tests. A local protected three-voter comparison measures queueing, Raft writes, independent journal publications and end-to-end latency. Production targets remain open.

## Problem

An $80 payment needs one durable outcome under its request ID. Concurrent payments can share a quorum round trip and disk syncs. A caller submitting a single payment during a quiet period should still receive a prompt reply.

## Proposed solution

The leader collects commands for the same Raft group in arrival order and proposes a versioned entry containing an ordered list. The batch header names the ledger once. Each command carries its own request ID and payload. Its individual result is computed during apply. The Raft index and position within the entry identify its order in the audit history. A caller can retry one payment without reconstructing its neighbors. [Request identity](0005-request-identity.md)

When idle, propose available work immediately. Under sustained load, drain queued commands up to hard limits on count and encoded bytes, including framing. Flush when another command would exceed either limit, the oldest command reaches its maximum wait, or an approaching request deadline leaves no time to gather more work. Reject an individually oversized command before proposal. Bound queued bytes and in-flight proposals, and signal overload at admission. A client deadline after proposal leaves its outcome unknown.

Apply committed entries in index order. Within an entry, evaluate each command against the last published state plus changes staged by earlier commands in that entry, including request outcomes. A business rejection records its deterministic result and contributes no balance changes. Continue with the next command. An identical retry returns the first outcome. Conflicting reuse reports an ID conflict and preserves the original record. Publish all effects, outcomes and the entry's applied index together in one WAL-disabled ledger `WriteBatch` with sync off. Reply after durable Raft commitment, local commit-frontier persistence, successful local publication and offsite protection through the entry. The recovery journal can confirm several committed batches together. [Offsite commit protection](0015-offsite-commit-protection.md) Recovery replays whole committed entries following the sealed snapshot. [Durable recovery](0007-durable-recovery.md)

Multiple entries can also share an AppendEntries message. Persist the entries and required Raft metadata from one `Ready` with a shared WAL sync. Release persistence-dependent acknowledgments only after that sync succeeds. Pipeline bounded proposals and replication while preserving persistence and apply order. Service ticks, heartbeats and snapshot traffic independently of the client batching timer. [raft-rs append configuration](https://docs.rs/raft/0.7.0/raft/struct.Config.html#structfield.batch_append) · [Ready persistence contract](https://docs.rs/raft/0.7.0/raft/raw_node/struct.Ready.html#method.persisted_messages)

## Tradeoffs

Larger batches amortize replication and storage costs. Every caller in a batch waits for its slowest command and for the result to become visible. Choose limits from measured queue delay, entry size and Raft sync cost.

Each voter reports completed synchronous Raft write batches since it opened. Ready batches include log, hard-state and snapshot installation writes. A separate count covers commit-only LightReady writes. The voter also reports how many Ready batches contained entries, their total entry count and how many append operations its verifier published to the independent journal. Use before-and-after differences on the same running voter. These counters measure successful storage operations at the application boundary. Physical device flushes and filesystem sync calls require separate instrumentation.

### Local protected comparison

A release-mode test submitted a fixed mix of two-, sixteen- and sixty-four-line postings through the embedded protected client. All three voters used loopback mTLS. The recovery journal occupied a separate directory on the same machine. The serial phase completed 45 payments and three expected rejections. The 24-client phase completed 360 payments and 24 rejections with each limit. Each caller retried admission refusals after one millisecond. [Reproducible comparison](../../crates/ledger-node/tests/network/group_commit.rs)

| Limit | Clients | Decisions/s | Decision p99, ms | Mean queue, µs | Commands/entry | Leader WAL batches/decision | Three-voter WAL batches/decision | Journal publications |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 42.0 | 44.47 | 15.26 | 1 | 2.000 | 6.000 | 48 |
| 256 | 1 | 62.2 | 25.35 | 11.57 | 1 | 2.000 | 6.000 | 48 |
| 1 | 24 | 204.2 | 864.22 | 1,625.38 | 1 | 1.833 | 4.844 | 107 |
| 256 | 24 | 1,360.4 | 29.66 | 69.56 | 24 | 0.083 | 0.250 | 16 |

With 24 callers waiting on the same leader, batching combined those 384 decisions into 16 Raft entries. The leader issued 32 synchronous Raft database writes, compared with 704 with one command per entry. Both configurations posted exactly 360 payments and rejected exactly 24. Throughput improved 6.7× in this trial and 5.1× in a repeat. With a single caller, each request still needed a separate entry and two leader writes. This benchmark measures the embedded protected submit path. [Repeated comparison and public API measurements](../measurements/2026-11-08-local-load.md#microbatching-and-group-commit)

The test ran each configuration once, with the single-command limit first. Each append batch contained one Raft entry in all four phases. The one-command concurrent phase retried 6,904 admission refusals. Its decision latency includes that wait. The optional `batch-diagnostics` feature measures queue time from submission enqueue to collection for a successful proposal. The test verified journal publication counts against the independent chain and reconciled balances. Reproduce with `cargo test --release -p ledger-node --features batch-diagnostics --test network group_commit::protected_group_commit_comparison -- --ignored --nocapture`.

## Acceptance criteria

- In one entry, include a successful debit, its identical retry, a conflicting reuse, an insufficient-funds rejection and another valid command. Compare individual results and balances with serial execution.
- Crash on either side of ledger publication and lose the replies. Retry individual IDs in different batches after failover. Verify original outcomes, one financial effect per accepted ID and no visible partial entry.
- Benchmark isolated requests, bursts and sustained mixed-size traffic against single-command entries with identical durability. Report throughput, end-to-end p99, queue delay, commands per entry, entries per append and WAL syncs per completed command for replication and apply. Repeat with a follower catching up from a snapshot and check heartbeat and election behavior.
