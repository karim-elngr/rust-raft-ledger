# RFC 0007 — Durable Raft storage and rebuildable ledger state

**Status** — Embedded RocksDB is chosen. The requested storage split gives Raft its own durable store and disables the ledger state's WAL and per-write sync. Local checkpoints, committed-tail replay and atomic installation of a verified peer snapshot are implemented. A logical replication boundary controls catch-up. The default configuration keeps the complete committed log on the voter for verification and repair. The optional offline `reclaim` command can remove voter-local duplicates covered by a pinned, independently verified recovery image. [Physical reclamation](0017-raft-log-reclamation.md)

## Problem

Syncing both a Raft append and a second ledger WAL adds disk work to every payment. Removing the second sync must still let a client recover the exact outcome of an acknowledged payment after power loss, including a funding split, a partially captured hold or a recorded rejection.

## Proposed solution

### Two independent stores per replica

| Store | Data and write contract |
| --- | --- |
| Raft store, a separate RocksDB database | Persist cluster and voter identity, term, vote, membership, Raft entries, durable commit progress, log boundaries and the active snapshot manifest. Keep its WAL enabled. Group related writes and complete the required device sync before releasing persistence-dependent messages or reporting persistence to raft-rs. |
| Ledger state, separate RocksDB database | Materialize accounts, journal, policies, reservations, request outcomes and the applied index. Publish the effects of a complete committed entry in one atomic `WriteBatch` using `disableWAL=true` and `sync=false`. This view is rebuildable. Its index describes what readers can see in this process. |

Use the Rust `rocksdb` binding and bounded storage work queues to keep blocking disk work off the network workers. WAL-disabled writes can be lost even on process failure. Different column families can flush at different times. A surviving applied-index key cannot certify that every corresponding account, journal and retry record reached disk. [Rust RocksDB binding](https://docs.rs/rocksdb/latest/rocksdb/) · [RocksDB write durability](https://github.com/meta/rocksdb/wiki/Basic-Operations#synchronous-writes) · [Multi-column-family flush behavior](https://github.com/meta/rocksdb/wiki/Atomic-flush)

### Commit and reply

1. Persist Raft entries and required hard state through the synchronous raft-rs `Ready` contract. Several commands can share a proposal and several entries can share a Raft store sync. A peer RPC receipt only admits a message. [Raft replication](0006-raft-replication.md) · [Group commit](0011-microbatching-and-group-commit.md)
2. Let raft-rs establish commitment. Persist the commit frontier in the Raft store, including advances from `LightReady`, before clients receive results through that index. This design can group those updates too. Maintain a contiguous log above the durable snapshot boundary. When a new leader overwrites an uncommitted suffix, atomically replace the suffix and its logical last index before acknowledging persistence. Retain the boundary term for log matching. [raft-rs Ready](https://docs.rs/raft/0.7.0/raft/raw_node/struct.Ready.html#method.persisted_messages) · [Raft storage contract](https://docs.rs/raft/0.7.0/raft/storage/trait.Storage.html)
3. Evaluate the committed commands in order. Publish ledger changes, outcomes and apply progress together, then return individual results after local publication, step 2 and independent confirmation that the contiguous committed prefix through this entry is restorable. [Offsite commit protection](0015-offsite-commit-protection.md) A business rejection advances progress and records an outcome. A storage error stops local apply and withholds a client result. Another replica can reproduce it from the same inputs. [Request identity](0005-request-identity.md)

### Snapshot, compaction and restart

Use a **sealed physical checkpoint** as the durable state-machine baseline. At a committed entry boundary S, capture all ledger column families, including account counters, open reservations, journal and policy history and exact retry identities and outcomes. The working state's WAL remains disabled. The periodic checkpoint requires a successful all-column-family flush at the cut and durable publication. Its complete manifest covers every dependency, including any immutable history segments. Verify S against the Raft group, term and membership. [Incremental snapshots](0012-incremental-snapshots.md) · [Immutable history](0014-immutable-ledger-history.md)

Write the snapshot files into a new immutable generation, verify the image and make its files and directory publication durable. Then sync the active manifest pointer and matching boundary metadata in the Raft store. Only after this can the replica compact the log through S. Keep the previous recoverable baseline and required log until publication succeeds. Install a peer snapshot through the same publication sequence after raft-rs accepts it, retaining the receiver's identity and its own term and vote. Serialize the switch with apply and invalidate the old account-cache generation. A crash between writing files and switching the pointer leaves an unused generation. [Peer snapshot transport](0008-peer-transport.md) · [Account cache](0010-account-cache.md)

On restart, open a fresh working copy of the verified manifest using shared immutable files and private writable metadata where the filesystem allows it. This avoids rereading every journal entry on an ordinary restart. An ordinary local restart with a sealed checkpoint and its retained suffix can reconstruct the working database without contacting the independent journal. Full-log repair and peer export of physically reclaimed commands require the voter's pinned independent raw base. With the default retention policy, the complete raw log remains local and a voter without a checkpoint can replay from genesis. Set `Config::applied` from that baseline and replay its contiguous committed suffix, including command order within each batch. Gate current reads on quorum confirmation, local catch-up and offsite protection of the served view. Missing or corrupt dependencies require recovery before serving. Local disk loss of a voter's Raft store requires the voter recovery procedure in RFC 0006. After whole-cluster loss, restore from the journal-bound recovery image when one is available and replay the remaining committed entries through the fenced head. With no base image, replay begins at genesis. [Raft restart index](https://docs.rs/raft/0.7.0/raft/struct.Config.html#structfield.applied) · [Recovery journal](0015-offsite-commit-protection.md)

## Tradeoffs

The Raft log pays the hot-path durability cost once per sync group. Removing per-command state sync shifts work to snapshots, extra disk space during publication and replay on restart. The Raft store may also need a commit-frontier sync. Measure syncs per successful command, snapshot size and time to resume reads before choosing retention and snapshot intervals. Background RocksDB flushing and durable snapshot publication still perform disk I/O.

A separate RocksDB Raft store reuses the existing Rust binding. TiKV's `raft-engine` is an alternative purpose-built log backend with a synced `LogBatch` write. Compare them under the same suffix-replacement, recovery and sync-group tests before choosing a different backend. raft-rs supplies the consensus algorithm and its `Storage` interface. The application owns this durability boundary with either backend. [TiKV Raft Engine](https://github.com/tikv/raft-engine) · [Raft Engine write contract](https://docs.rs/raft-engine/latest/raft_engine/struct.Engine.html#method.write)

## Acceptance criteria

- After client success, simulate power loss on all nodes before the state store flushes. Restore from the last sealed snapshot plus Raft logs. Retry a captured waterfall and a recorded rejection. Compare entry IDs, funding allocations and original outcomes. Exercise a commit update delivered in `LightReady`.
- Discard the working database, then repeat with a state directory where only some column families reached disk. Reconstruct the four counters at one applied index by summing posted journal lines and the outstanding amounts from reservation history. Include an open partial capture. A matching retry produces one effect.
- Interrupt snapshot creation, manifest publication, log trimming and peer installation on both sides of each durability boundary. Reopen a verified baseline with its required suffix or require peer catch-up. Check node identity and state convergence after compaction.
- Replace uncommitted entries 9–12 with 9–10. Crash before and after the Raft store sync. Acknowledged replacement must reopen with last index 10 and the old entries 11–12 inaccessible.
