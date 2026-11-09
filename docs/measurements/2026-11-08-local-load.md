# Local ledger load and recovery measurements

## Public API workload

Three fresh three-voter clusters ran on one Apple M2 with eight cores and 8 GB RAM, macOS 27.0.1 arm64 and Rust 1.99.1 in the release profile. Each voter had its own RocksDB directories. All three used the same local POSIX journal. Raft peers and the eight warmed public gRPC connections used mutual TLS over loopback. Calls targeted the elected leader using the generated client. Protected writes waited for independent journal coverage and every measured account read used a quorum read. [Load driver](../../crates/ledger-node/tests/network/api/load.rs) · [Per-trial data including attempts, percentiles and frontiers](2026-11-08-public-load.json)

Each closed-loop phase submitted 2,048 new payments, 512 quorum reads and 256 duplicates with 32 callers. During the hot-wallet phase the balance permitted exactly 1,792 payments. The other 256 received the expected business rejection. The returning follower had physically reclaimed its old log before going offline. Its catch-up phase included ordinary Raft appends and the subsequent part of the same foreground workload. The follower caught up in 230–939 ms including reopen, with 248–1,124 public calls completing during that window. A separate drill measured actual bulk peer snapshot transfers. [Bulk transfer measurements](../rfcs/0012-incremental-snapshots.md#measured-peer-catch-up-under-protected-writes)

| Phase | New payment decisions/s | Payment p50 / p95 / p99, ms | Quorum read p99, ms | Duplicate p99, ms |
| --- | ---: | ---: | ---: | ---: |
| Healthy distributed wallets | 847.9 | 26.55 / 32.60 / 41.22 | 39.64 | 37.28 |
| Contended wallet and enforced floor | 850.2 | 27.19 / 34.03 / 38.29 | 38.36 | 34.56 |
| One follower offline | 819.3 | 27.06 / 37.06 / 47.39 | 38.86 | 38.34 |
| Follower reopening and catching up | 826.4 | 26.84 / 36.43 / 45.33 | 38.80 | 43.12 |

Figures are medians of the three per-run rates and percentiles. Payment decision rates count business rejections and exclude reads and duplicates. The hot-wallet phase posted a median of 744 payments/s. Every run reconciled all four counters for 64 customer accounts, the balancing account and every distinct payment entry ID. Each duplicate returned the same decision. Final protected and applied indexes agreed with the journal head at phase completion.

## Overload and sustained arrivals

An initial run sent 2,808 RPCs with first-attempt admission errors surfaced immediately. It encountered 79 `RESOURCE_EXHAUSTED` responses and stopped on its zero-error assertion. The benchmark then applied a five-millisecond backoff on that status and retried the same request ID within a bounded deadline. The three trials above include that elapsed time. Their raw data report first-attempt and total busy responses separately. The Rust application SDK surfaces initial overload to its caller by default. Transport failures and business rejections have different meanings for a retry. [Client contract](../rfcs/0009-client-protocol.md)

| Fixed offered rate | Scheduled / completed | Dropped by sender before transmission | First-attempt / total busy responses | Payment p50 / p95 / p99, ms |
| --- | ---: | ---: | ---: | ---: |
| 300 operations/s for 60 s | 18,000 / 18,000 | 0 | 2,875 / 3,449 | 28.46 / 43.09 / 59.60 |
| 3,000 operations/s for 4 s | 12,000 / 7,142 | 4,858 | 3,253 / 7,375 | 30.46 / 56.14 / 73.13 |

The fixed-rate generator measured latency from scheduled arrival and admitted at most 64 outstanding operations. Percentiles describe submitted operations. At 300/s the workload completed 13,500 payments, 2,250 quorum reads and 2,250 duplicates in 60.027 s. At 3,000/s it completed 5,379 payments, 884 reads and 879 duplicates in 4.014 s. All submitted operations completed successfully within the benchmark's deadline. At the higher offered rate 40.5% of scheduled operations were dropped before they reached the service. These results describe the named workload, local storage and enforced pipeline limits. Remote filesystem latency and deployment topology need their own measurements.

A subsequent one-minute release verification enabled batch diagnostics and used the same 300/s workload. It again completed all 18,000 operations, with zero sender drops. Scheduled payment p50/p95/p99 were 29.71/57.89/86.17 ms, and the three operation classes together observed 3,435 first-attempt and 4,613 total busy responses. Both one-minute trials met their deadlines. The latency difference illustrates the range observed on this machine.

## Microbatching and group commit

A separate release run compared one command per Raft entry with the default limit of 256. Each configuration submitted the same 384 decisions through the embedded protected client with 24 callers, three mTLS-connected voters and the recovery journal enabled. Both configurations posted 360 payments and recorded 24 expected rejections. The workload mixed postings with 2, 16 and 64 lines.

| Maximum commands/entry | Decisions/s | Decision p99 including retries | Raft entries proposed | Synced leader write batches | Journal publications |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 204.2 | 864.22 ms | 384 | 704 | 107 |
| 256 | 1,360.4 | 29.66 ms | 16 | 32 | 16 |

The default averaged 24 commands per entry. Each append batch contained one Raft entry in both cases. The counters count successful synchronous RocksDB writes at the Raft storage boundary. The single-command run incurred 6,904 admission refusals and the benchmark retried after one millisecond. With one caller, each of 48 decisions produced one entry, two synced leader batches and one journal publication at both limits. Each configuration was measured once in a fixed order. [Full comparison, queue timings and counter definitions](../rfcs/0011-microbatching-and-group-commit.md#local-protected-comparison)

A separate release verification repeated the 24-client workload. The one-command limit completed 239.2 decisions/s with 715 synced leader write batches. The default completed 1,225.8 decisions/s with 38 synced leader write batches. Both limits again produced 360 payments and 24 expected rejections.

After implementing verified snapshot replay, a third release run repeated the same comparison. With 24 callers, throughput increased from **242.9 to 1,398.2 decisions/s (5.76×)**. Sharing 16 Raft entries across the 384 decisions reduced synced leader writes from 698 to 32 and independent journal publications from 103 to 16. The one-command configuration retried 4,979 admission refusals and its p99 including retries was 990.49 ms. The default had zero refusals and a 20.94 ms p99. With a single caller the same run measured 62.3 and 62.4 decisions/s, with 96 leader writes in either case. All configurations produced the expected outcomes. [Load driver](../../crates/ledger-node/tests/network/group_commit.rs)

## Recovery and physical storage

The matched disaster test used two independent journals with identical original command bytes. One restored directly from genesis. The other used a verified checkpoint ending two Raft entries before the fence. For each path the test destroyed the original voter disks, fenced the journal, restored three voters sequentially, activated them, and checked all logical records and an independent digest of their original Raft entries. Results below are individual paired trials in the development profile. Checkpoint construction preceded the timed disaster. [Recovery drill](../../crates/ledger-replica/examples/journal_recovery.rs)

| Original outcomes | Journal-only fence to activation | With a journal checkpoint | Entries replayed per voter, full / checkpoint | Checkpoint build time / size |
| ---: | ---: | ---: | ---: | ---: |
| 8,194 | 2.136 s | 2.007 s | 36 / 2 | 1.559 s / 3.41 MB |
| 65,538 | 10.265 s | 7.776 s | 260 / 2 | 3.270 s / 26.92 MB |

In the public load trials, reclaiming the follower's already protected log reduced its live Raft SST files from 610,059–611,334 bytes to 1,903 bytes. This measures the Raft database immediately after compaction. The ledger projection, post-checkpoint writes and independent image consume additional space. Another fixture placed 64 KiB of context in each original Raft entry, removed 918,722 encoded bytes and reduced live Raft SSTs from 1,317,227 to 395,973 bytes. It exercised a crash on either side of the durable switch, a restart with the journal offline and a subsequent full repair from the pinned image. [Physical recovery tests](../../crates/ledger-replica/src/raft_store/reclaim/tests.rs)

The original release peer-snapshot drill fetched all 5.33 MB of files after the receiver's own RocksDB compacted its projection. A receiver with a shared checkpoint reused 2.76 MB in the preceding phase. In a follow-up three-run release comparison, verified replay from the independently compacted receiver's local checkpoint fetched zero state-file bytes. It saved a median 5,357,067 B (95.52%) per identical offer, including proof traffic, and recovered the original receipts and balances. [Original file transfer](../rfcs/0012-incremental-snapshots.md#independently-compacted-receiver) · [Verified replay comparison](../rfcs/0012-incremental-snapshots.md#catch-up-after-adding-verified-replay)

## Correctness review

- An independent accounting model ran 12,288 interleaved commands covering reservations, partial captures, releases, ordered funding, balance floors and delayed retries. The review also added a check that a stored reservation belongs to the active ledger before applying it. [Model workload](../../crates/ledger-core/tests/mixed_payments.rs) · [Stored reservation regression](../../crates/ledger-replica/src/projection/tests.rs)
- The client now enforces the encoded request limit before copying the request or connecting. A peer that stalls during the initial TLS handshake times out while the listener continues serving other peers. [Client regression](../../crates/ledger-api/src/client.rs) · [Peer regression](../../crates/ledger-node/tests/network.rs)
- Recovery tests exercised corrupt images and journal bytes, a divergent saved journal position, interrupted reclamation, an offline voter pinning its predecessor and a full restore after losing the voter disks. The explicit full-history recovery path checked the original receipt. [Administrative recovery tests](../../crates/ledger-node/tests/recovery_admin.rs) · [Reclamation regressions](../../crates/ledger-replica/src/raft_store/reclaim/tests.rs)

The local disaster drill deleted all voter directories and retained the journal directory on the same machine. To survive physical loss of that machine, a deployment needs a journal in a separate failure domain. Network filesystem latency, cross-host failover and the 1 TB working set remain unmeasured here. [POSIX journal deployment assumptions](../rfcs/0015-offsite-commit-protection.md#tradeoffs)

## Reproduce

Use the repo's documented C++ and protobuf prerequisites. Run each load drill alone with local TCP ports available. Increase `LEDGER_LOAD_SECONDS` to sustain the fixed-rate phase. Omit the environment variables for the four-second default.

```sh
LEDGER_LOAD_RATE=300 LEDGER_LOAD_SECONDS=60 cargo test --release --locked -p ledger-node --test network public_protected_load_suite -- --ignored --nocapture
LEDGER_LOAD_RATE=3000 LEDGER_LOAD_SECONDS=4 cargo test --release --locked -p ledger-node --test network public_protected_load_suite -- --ignored --nocapture
cargo test --release --locked -p ledger-node --features batch-diagnostics --test network group_commit::protected_group_commit_comparison -- --ignored --nocapture
cargo test --locked -p ledger-node --test network snapshot_catchup_under_payment_load -- --ignored --nocapture
cargo build --locked -p ledger-node --bin ledger-node
cargo run --locked -p ledger-replica --example journal_recovery -- 4096 32768
```
