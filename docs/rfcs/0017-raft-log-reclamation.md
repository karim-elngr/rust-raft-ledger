# RFC 0017 — Reclaim duplicate voter-local Raft log bytes

The following describes the three-voter deployment. Groups configured with a permanent learner use quorum acknowledgements and can export asynchronously. [Learner topology](0018-static-raft-learner.md) · [Async archive](0019-async-recovery-archive.md)


## Problem

Switching a lagging voter from ordinary replication to a checkpoint changes what Raft can request. Old command bytes still occupy voter-local disk. They also serve independent verification, full state repair and peer history transfer. Reclaiming them safely requires a durable and identifiable substitute for those reads.

## Proposed solution

Run `ledger-node reclaim CONFIG.toml` on a stopped voter. Its sealed local checkpoint must cover the source index of a published independent recovery base. The command scrubs the local checkpoint, verifies the independent image's Raft files, compares the original encoded committed entries and verifies that the base belongs to the active journal history. It persists a voter-specific claim on that exact image on the recovery volume. The claim survives process restarts and keeps the image available when a newer base is published. [Independent checkpoint](0016-journal-checkpoints.md) · [Durable image pins](../../crates/ledger-replica/src/recovery/base/registry.rs)

One synced RocksDB batch switches the voter to the verified image, records the ordinary replication floor and deletes the duplicate original rows below the boundary. The boundary entry, its term, hard state, vote and all subsequent entries remain local. Cleanup removes adopted snapshot-history rows below the same boundary. A forced RocksDB compaction releases obsolete SST space. A restart follows the durable handoff after a crash. Repeating maintenance finishes cleanup and releases an obsolete base claim after installing its successor. [Storage handoff](../../crates/ledger-replica/src/raft_store/reclaim.rs)

Ordinary restart opens the sealed local state and committed tail. A request for older raw commands opens the exact pinned image on demand, verifies the raw database and reads it with a shared pin. This supports comparison with journal entries, re-export to peers and an explicit full rebuild. Such a rebuild needs the original independent mount path to be available. After a successful rebuild the voter seals a local checkpoint before resuming. User-facing reads of historic postings and receipts continue to use the ledger's local history and indexes. [Entry lookup](../../crates/ledger-replica/src/raft_store/archive.rs) · [Ledger history](0014-immutable-ledger-history.md)

Before deleting the older rows, reclamation verifies the saved journal position and advances it to the checked boundary when it belongs to the same journal generation. It preserves a newer verified position and refuses a divergent one. On a protected restart, the voter can then compare only the journal suffix against locally retained entries. When the saved position belongs to an earlier generation and verification needs bytes below the local boundary, the voter must open and verify the pinned image. [Journal position validation](../../crates/ledger-replica/src/raft_store/reclaim.rs)

## Tradeoffs

The operation takes one voter offline, adds a full verification pass and consumes local compaction bandwidth. Run it on one voter at a time while the other two can form a quorum and the independent journal is available. If three voters reference the same independent base, their durable claims share the physical image. An offline voter can keep an older generation alive. Independently retained journal segments continue to support full-history disaster recovery and scrub. An unavailable or corrupt independent image fails a cold history read. Availability of ordinary checkpoint restart and local point lookups can be tested independently.

The command reports the number and encoded size of entries handed off on that invocation, and the live SST file sizes before and after compaction. The latter covers the Raft database. It excludes the independent image, projection, filesystem metadata and temporary space during compaction.

[Physical footprint and recovery measurements](../measurements/2026-11-08-local-load.md#recovery-and-physical-storage) cover actual SST compaction, a lost voter and interrupted handoff on the development machine.
