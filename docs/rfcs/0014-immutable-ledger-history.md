# RFC 0014 — Immutable ledger history

**Status** — The voter stores first decisions in a dedicated RocksDB column family with persistent request and journal-entry indexes. Sealed segments remain a conditional alternative if compaction measurements justify them. The 1 TB drill is deferred for this milestone.

## Problem

A wallet's current counters occupy little space beside years of postings and retry outcomes. Rewriting old records during compaction can increase the bytes a lagging voter must fetch. An old request ID must still resolve to its original decision after the funds, policy or reservation have changed.

## Proposed solution

Start with a separate append-ordered RocksDB column family for history. Order records by source history generation, committed entry index, command position within the entry and record position. Preserve journal IDs and ordered lines, policy changes, reservation admissions and resolutions, canonical requests and their exact results, including rejections and funding allocations. Keep counters, effective policies and remaining reservation lines in mutable state. Publish both parts and the applied index in one state-machine batch. [Durable recovery](0007-durable-recovery.md)

Keep a complete local index from `(ledger_id, request_id)` to the first request's stable history position. Before evaluating each mutation, consult the index and its staged additions from earlier commands in the batch. Compare canonical fields on a hit and return the original result or an ID conflict. Missing or unreadable referenced history is a storage error. Digests verify bytes. The records supply the full identity and result. [Request identity](0005-request-identity.md)

In the current single-ledger working database, each first decision occupies one row keyed by its big-endian Raft index and command position. A posting or capture includes its journal entry in that row. Request and journal-entry indexes both point to the same position. A matching retry preserves that row, including its exact funding allocation, even after the accounts or hold have changed. One RocksDB write batch publishes the history, indexes, affected accounts and reservations, last journal ID and applied Raft index. [Projection](../../crates/ledger-replica/src/projection.rs)

If measurements show historical compaction breaks the transfer or disk budget, seal completed, fixed ranges of committed entry indexes into immutable, versioned segment files. Encode records in the defined order without splitting an entry. Stage each range from a consistent view, verify coverage and checksums, and sync its files and directory. Atomically publish a catalog mapping the range to its verified segment. Reads and retries pin one catalog view and resolve each position from its segment or the recent RocksDB history. Keep both paths online on every serving replica.

If segments are introduced, include the catalog, lookup index, recent history and every referenced segment in each complete checkpoint manifest. Reclaim covered RocksDB history only after a durable checkpoint references the complete set and earlier readers finish. Retained checkpoints and active transfers keep their files pinned. A voter installing a checkpoint must obtain all missing segments. [Incremental snapshots](0012-incremental-snapshots.md)

## Tradeoffs

Separate column families keep the first version simple. Sealing limits historical rewrites but adds a catalog, online segment reads and space for staged ranges. The lifetime request-ID index still grows.

## Acceptance criteria

- Crash during sealing, catalog publication and reclamation. At the same applied index, reproduce every journal line, hold event and retry result exactly once.
- Retry a years-old success, rejection and conflicting reuse after compaction. Repeat after voter restart from a checkpoint and after cluster recovery from the journal.
- Deferred with the 1 TB drill. If the segment layout is introduced, compare the two layouts above 1 TB. Measure compaction bytes, incremental transfer bytes, peak disk usage and p99 retry latency.
