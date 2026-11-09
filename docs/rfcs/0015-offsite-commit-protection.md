# RFC 0015 — Recover every acknowledged payment after cluster loss

The following describes the three-voter deployment. Groups configured with a permanent learner use quorum acknowledgements and can export asynchronously. [Learner topology](0018-static-raft-learner.md) · [Async archive](0019-async-recovery-archive.md)


**Status** — Protected writes and quorum reads use a shared POSIX recovery journal. This backend is sufficient for local development. The goal of zero lost acknowledgments after permanent loss of every voter disk requires journal storage that survives the same failure. S3 or an equivalent backend is a future option. [Recovery journal](../../crates/ledger-recovery/src/journal.rs)

## Problem

A payment committed on all three voters can disappear if their disks fail together. After acknowledging that payment, the service must be able to recover its original journal entry ID, funding split and retry outcome from the surviving journal.

## Proposed solution

The journal retains a contiguous sequence of committed Raft entries from genesis, including ordered microbatches, terms, indexes and control entries. Its durable head identifies the source generation, covered index and content digest. On a shared POSIX volume, writers use cross-host file locks, atomic same-directory rename and filesystem sync to publish a new head. A separate directory on a developer's machine exercises this protocol locally. An operator meeting the disk-loss target must place the journal outside the voters' failure domain. Local checkpoints shorten individual voter restarts. [Local recovery](0007-durable-recovery.md)

After Raft commits a group of entries, append their immutable contents and conditionally advance the journal head from its previous index and digest. Return a terminal result, including a business rejection or an identical retry, only after the Raft commit frontier is durable, the result is applied and the recovery journal confirms coverage. If a journal write times out, check the head and content identity before retrying. [Group commit](0011-microbatching-and-group-commit.md)

For a current read, obtain a quorum-confirmed read index and pin a local view at or beyond it. Confirm that the journal head covers the index of that actual view in the active generation before replying. On leader failover, verify the head and resume from the contiguous committed suffix. Bound the queue of locally committed work awaiting protection. [Current reads](0006-raft-replication.md) · [Account cache](0010-account-cache.md)

For whole-cluster recovery, fence the old generation against its exact head and digest. If an append won the race, use the newer head and include its entries. Restore a verified independent checkpoint when one is published and replay its authenticated suffix through the fenced head. Without a checkpoint, verify and replay the journal from genesis into fresh voters. Use a new cluster identity. Activate that generation at the journal before serving protected requests. A missing or corrupt segment in the required replay range blocks this recovery path. Preserve the full journal and its decoding tools for recovery from genesis. [Replica restore](../../crates/ledger-replica/src/recovery.rs)

## Tradeoffs

A journal confirmation adds latency and makes protected operations depend on the journal even when a voter quorum is healthy. Batching spreads that cost across payments. Journal-only reconstruction costs grow with retained history. A recovery checkpoint trades an additional physical image for a shorter replay.

A deployment mounts one shared filesystem for the recovery journal and gives each voter its own Raft and ledger databases. A journal append syncs the new immutable segment, reads it back, syncs the directory, syncs a temporary head file and atomically renames it over `HEAD`, then syncs the directory before replying. One writer can advance a ledger's head at a time. The sustainable payment rate therefore depends on payments per published batch and the latency of that whole sequence. The number of small segment files also grows with the number of publications. A stalled journal delays protected responses even with a healthy Raft quorum. [POSIX publication](../../crates/ledger-recovery/src/journal.rs)

For example, Amazon EFS Regional documents cross-zone durability for synchronous writes and NFSv4 file locking. Using it for this protocol would require exercising the chosen mount's locking, directory-entry durability, failover and small-file p99 under competing client load. The local measurements below use the host filesystem. Deployment and storage products remain open. [EFS consistency and locking](https://docs.aws.amazon.com/efs/latest/ug/features.html) · [EFS performance considerations](https://docs.aws.amazon.com/efs/latest/ug/performance-tips.html)

## How much history is read today?

| Operation | History read |
| --- | --- |
| Confirm another request on a running leader | Check the live, durable journal head. If it advanced, compare new segments to the local Raft log back to the previously verified digest. Append locally committed entries beyond the head in bounded groups. |
| Start a voter with a surviving disk | Reopen the sealed RocksDB state and apply the committed tail when a checkpoint exists. For journal verification, load the saved journal head from the Raft store. Verify the suffix back to that exact digest. If no head has been saved, compare the journal from genesis. |
| Restore a fresh voter from a journal without an independent checkpoint | Read the full journal on each replacement. Verify the hash chain and the replacement cluster ID together, then import entries in forward order. Build the ledger state once and seal a checkpoint at the fenced index before publishing the voter directory. |
| Restore a fresh voter using a published journal checkpoint | Verify and copy both physical databases, then import and execute only the original entries after the checkpoint. Activation with a matching local import proof compares the same tail. |

Each voter saves the first nonempty journal head it verifies. Once the journal advances at least 1,024 Raft entries beyond that head, the next successful verification saves a new one. The small synced record binds the journal digest, index and term to the voter identity and the bytes of its local committed boundary entry. Updating it requires the exact previously saved head. The old record remains usable after an interrupted update. A leader still confirms live serving authority for every protected response. A follower can compare existing journal entries when it has applied another interval of entries, with at least five seconds between attempts. It cannot extend the journal by observing it. [Voter verification](../../crates/ledger-replica/src/journal_verifier.rs) · [Anchor storage](../../crates/ledger-replica/src/raft_store/journal_anchor.rs)

Journal verification assumes that independently stored segments already verified remain immutable and available. A full `journal-scrub` checks historical bytes even behind a saved digest, including on a fenced journal. Journal-only restore checks the complete fenced history. Checkpoint-based restore authenticates the image and every segment after its recorded source. A missing or corrupt required component blocks that recovery path. [Operator commands](../../README.md#protect-acknowledged-payments)

On the journal-only path, each voter restore reads each append segment twice. The backward pass authenticates the chain and checks the candidate cluster ID. A temporary file of fixed-size references supplies the order for the forward import. The working projection replays the committed Raft entries once. Subsequent opens clone the sealed state. Activation checks the restored voter's log in a single backward pass and rechecks the fence under the publication lock. Three sequential restores plus activation therefore traverse the append history seven times. These are logical reads. Physical disk traffic depends on filesystem caching. [Journal traversal](../../crates/ledger-recovery/src/journal.rs) · [Restore implementation](../../crates/ledger-replica/src/recovery.rs)

An operator can create an independent checkpoint on the same POSIX volume while the source generation is live. It includes the exact historical Raft bytes that replacements need for verification and repair. Each replacement still reads and verifies the entire physical image. [Independent checkpoint design](0016-journal-checkpoints.md)

### Local recovery measurements

| Unique outcomes | Fenced journal size | Three-voter recovery before | Three-voter recovery now | Current restore per voter | Subsequent voter reopen before → now |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 8,194 | 788,861 bytes | 2.486 s | 2.147 s | 0.633 s | 225 → 111 ms |
| 65,538 | 6,304,992 bytes | 14.306 s | 10.050 s | 3.191 s | 1,276 → 109 ms |

These are medians of three fresh-directory trials per dataset and version. The three-voter time includes CLI fencing, three sequential restores and activation. The per-voter column uses the middle restore in each trial by elapsed time. The earlier implementation read each append segment three times per replacement and rebuilt the projection twice. The workload had 4,096 or 32,768 accounts and the same number of posted entries, including a partially captured waterfall, a rejection and retried commands. The harness removed the original voter directory before restore. All three replacements matched every original account, posting and stored outcome, including the remaining reservation.

| Journal verification at restart | No saved head | Saved head equals current head | Two new Raft entries since saved head |
| --- | ---: | ---: | ---: |
| Smaller dataset | 39.23 ms for 32 entries | 5.85 ms for 0 entries | 6.44 ms for 2 entries |
| Larger dataset | 269.97 ms for 256 entries | 6.18 ms for 0 entries | 7.18 ms for 2 entries |

The verification measurement times opening the verifier and comparing the independent journal with a warm local Raft store. The last two Raft entries contained 261 submitted commands. The drills used the Rust development profile on an Apple M2 with 8 GB of memory, warmed CLI executables and a warm local filesystem. Server startup, election and time to the first protected RPC are outside these windows. Terabyte stores and remote volumes require separate measurement. [Reproduce the recovery drill](../../crates/ledger-replica/examples/journal_recovery.rs)

A subsequent run with peer checkpoint transfer enabled repeated the 65,538-outcome journal-only disaster drill. Fencing through activation took 10.93 s. The three replacements took 3.14, 3.17 and 3.18 s to restore. Median subsequent voter reopen was 106 ms. Journal verification took 293 ms without a saved head, 6.14 ms with the current head saved and 7.25 ms with two new Raft entries. The comparison of the restored ledger and all original outcomes passed for each voter.

## Acceptance criteria

- Acknowledge a partially captured waterfall, destroy every voter disk and rebuild using only the surviving journal. Verify the journal ID, funding split, remaining hold, balances and retry result.
- Interrupt a write before publication and again after publication but before the reply. Verify that covered commands recover once and that an uncovered decision stays unobservable.
- Delay journal confirmations during writes, current reads and a leader change. Each successful read must be reproducible from its covered view.
- Race a final publication against fencing. Verify only the activated replacement can serve and a gap blocks recovery. Measure protected p99 write latency, confirmations per command and time to restore from the journal.
