# RFC 0010 — Fast access to account state

**Status** — Implemented for the state machine, point reads and embedded multi-account reads. Local checkpoint restart and peer snapshot installation each start with an empty cache. [Snapshot installation](../../crates/ledger-replica/src/replica/snapshot.rs)

## Problem

A busy wallet can appear in thousands of postings, reservations and balance reads. Looking it up and decoding it every time costs work even when RocksDB already holds the underlying block in memory. Serving its posted balance with yesterday's pending debits or policy could authorize an invalid withdrawal.

## Proposed solution

Give each replica a byte-bounded cache of immutable, decoded account records keyed by ledger and account ID. Each record contains identity, normal side, policy, all four counters and the applied index at which the record was loaded or updated. Compute available balance from that record. Look up reservation line allocations separately in the same ledger view when resolving a hold. Budget the account cache alongside RocksDB's block cache, which caches storage blocks. [Balance constraints](0002-balance-constraints.md) · [RocksDB block cache](https://github.com/meta/rocksdb/wiki/Block-Cache)

The apply owner consults earlier changes in the same microbatch first, then the cache, then the active RocksDB generation. Stage each command's changes privately. Merge accepted changes and retry outcomes into the batch in order. A business rejection discards that command's financial changes and records its outcome. After the whole state batch succeeds, replace or evict every touched cache entry and publish the new applied index in one serialized step. On storage failure, stop apply. Eviction requires no writeback. [Microbatching](0011-microbatching-and-group-commit.md)

For a current read, first obtain a quorum-confirmed read index and wait for local publication through it. The serialized apply owner copies all requested accounts at one applied index, taking cache hits and reading misses from RocksDB before it can apply another entry. Before returning the result, confirm that the independent recovery head covers the pinned view's exact index in this generation. [Offsite commit protection](0015-offsite-commit-protection.md) This also permits hits for accounts last changed before the read index. Late fills may enter the shared cache only if the generation and published index still match. Leave missing accounts uncached so a later creation is immediately visible. [Current reads](0006-raft-replication.md)

Start with an empty cache after restart or publication of a restored generation. With the state-machine WAL disabled, an ordinary restart rebuilds the working state from the verified local checkpoint and its durable committed suffix. Full replay or peer export of older raw entries on an explicitly reclaimed voter reads its pinned independent base. [Durable recovery](0007-durable-recovery.md)

The voter builds a new working generation from its local checkpoint and committed Raft tail, then starts serving with an empty cache. Each fixed-size slot holds one decoded account and an applied index. A hash of the ledger and account IDs selects a slot. A collision replaces its previous occupant. Misses load through RocksDB, and a successful state batch refreshes each touched resident account. The read holds those copies while it waits for independent recovery coverage. [Cache implementation](../../crates/ledger-replica/src/account_cache.rs) · [Protected reads](../../crates/ledger-node/src/reads.rs)

## Tradeoffs

Decoded records reduce repeated lookups and decoding. They consume memory that could cache more RocksDB blocks, and consistent read capture adds work to the apply owner.

## Local measurements

The benchmark populated 32,768 accounts, flushed the working database to 9,498,299 bytes of SSTs, warmed each working set once and timed 131,072 local account reads per case. Each configuration allocated 1 MiB across the two cache budgets. The table gives the median of three runs on an Apple M2 with Rust 1.99.1 in the development profile. The OS file cache was enabled. The timed operation was `Replica::account`. [Reproducible benchmark](../../crates/ledger-replica/examples/account_cache.rs)

| Active accounts | Decoded / block budget | p50 | p99 | Reads/s | Decoded hits | RocksDB lookups |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | 0 / 1,024 KiB | 6.000 µs | 7.875 µs | 164,764 | 0% | 131,072 |
| 16 | 128 / 896 KiB | 0.292 µs | 0.334 µs | 3,040,020 | 100% | 0 |
| 32,768 | 0 / 1,024 KiB | 19.250 µs | 27.584 µs | 51,014 | 0% | 131,072 |
| 32,768 | 128 / 896 KiB | 19.584 µs | 27.917 µs | 50,061 | 0% | 131,072 |

With the 128 KiB budget, the decoded table held 682 slots and its fixed backing allocation peaked at 130,944 bytes. The wide traversal evicted an account on every read. RocksDB lookups can hit its block cache or the OS file cache. To reproduce the development-profile run, use `cargo run --locked -p ledger-replica --example account_cache`.

## Acceptance criteria

- Replay the same mixed workload with the cache disabled, warm and under eviction. Compare outcomes, policies, all counters and remaining holds at matching indexes.
- Race a multi-account read with a transfer, capture, policy change and snapshot switch. Every read matches one reference state. Exercise a lookup miss followed by account creation and a crash between batch success and cache publication.
- At equal total cache memory, benchmark repeated hot-wallet access and a working set larger than the budget. Report p50/p99 latency, throughput, hit rate, RocksDB account lookups and peak cache bytes.
