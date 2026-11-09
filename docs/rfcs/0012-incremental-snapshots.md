# RFC 0012 — Incremental snapshots and follower catch-up

**Status** — Local restart and peer catch-up use sealed physical checkpoints. A returning voter can replay a verified interval from a local seal or transfer missing physical files with resume. Logical compaction controls which gaps use snapshots. Offline reclamation can release historical Raft bytes after a verified independent handoff. The 1 TB measurement is deferred.

## Problem

Exporting every journal row on each snapshot, or copying a terabyte before restarting a healthy disk, puts recovery time in proportion to the entire ledger. A lagging voter needs a complete state at one committed position, even when most of its files already exist locally.

## Proposed solution

**Seal a checkpoint.** Pause ledger apply at a complete committed entry S. Enable RocksDB atomic flush and create an all-column-family checkpoint with `log_size_for_flush=0`. Wait for the flush and checkpoint to succeed. Include the applied marker, journal, policies, counters, reservations and retry records. Resume apply once the checkpoint has captured the files. Read S from the checkpoint and match it to the Raft boundary. Hashing and transfer run after the pause. [RocksDB atomic flush](https://github.com/meta/rocksdb/wiki/Atomic-flush) · [Checkpoint API](https://github.com/meta/rocksdb/blob/main/include/rocksdb/utilities/checkpoint.h)

**Publish a complete manifest.** Bind the group, S, term and membership at S, ledger schema and RocksDB format requirements to every required file's relative name, length and content digest. Keep immutable SSTs and any blob files by digest. Preserve exact checkpoint metadata, including `CURRENT` and the RocksDB `MANIFEST`. Reuse verified digests for unchanged immutable files. Sync the files, manifest and directory entries before syncing the active manifest reference in the Raft store. Retain the previous recovery path until publication succeeds. Only then compact through S. Pin files needed by active manifests and transfers during garbage collection. [Durable recovery](0007-durable-recovery.md)

**Restart locally.** Clone the sealed generation into a fresh writable directory. Hard-link immutable files on the same filesystem, use copy-on-write clones where supported, or copy when necessary. Give the working database private writable metadata. Open it with compatible column families and comparators, verify its applied marker, and replay the contiguous committed suffix after S. A same-disk checkpoint shares its device's failure domain. [RocksDB checkpoints](https://github.com/meta/rocksdb/wiki/Checkpoints)

**Catch up a peer.** Use ordinary Raft append while the matching log prefix and suffix remain available. Bound extra log retention by time and bytes. After compaction, offer a compatible sealed manifest at or beyond Raft's requested index. The receiver inventories verified objects from retained generations and requests missing bytes over a throttled stream. Track verified chunks by manifest, file digest and offset to resume after disconnects. Pin the manifest and required suffix for a bounded transfer window. If that window expires, offer a newer generation. Reserve disk and network capacity for votes, heartbeats and foreground writes. [Raft storage contract](https://docs.rs/raft/0.7.0/raft/storage/trait.Storage.html#tymethod.snapshot)

For physical transfer, stage the sender's exact file set in a new directory. Independent compactions can give identical ledger contents different SSTs. Filename matches or files from the receiver's working database cannot establish equivalence. Validate paths, digests, lengths, format, group and index before installation. Publish only the snapshot accepted through raft-rs `Ready`, then replay the tail. [Peer transport](0008-peer-transport.md)

Size the retained suffix for writes that arrive during transfer and verification. For example, a 10,000-second transfer at 5 MB/s of new Raft entries adds 50 GB before replay starts, using decimal units. The follower must replay faster than new entries arrive to close that gap. Measure this under foreground load and use the result to set log and transfer budgets.

## Tradeoffs

For the expected history-heavy workload, isolate mutable state and index the immutable journal and retry outcomes. When history segments are enabled, the manifest also references their verified content and indexes. File inventories can reuse these segments across generations. [Immutable ledger history](0014-immutable-ledger-history.md)

Shared snapshot ancestry reduces transferred bytes. A cold follower needs the entire image unless preseeded with verified objects. Retained generations need disk headroom and a forced flush can stall local apply. Every acknowledged write must also survive loss of all voter disks. That requires a recoverable independent baseline and committed tail through the result's index. [Offsite commit protection](0015-offsite-commit-protection.md)

## Current local implementation

The serving owner reserves the live projection generation. A storage worker captures all six RocksDB column families with atomic flush enabled, checks the ledger identity, applied frontier and last journal entry in the captured database, then looks up the term for that exact committed index. It syncs the sealed generation before publishing the index, term and manifest digest in the Raft WAL. The owner may process later commits while the worker runs. The synchronous `Replica::begin_checkpoint()` API still fixes the capture at its call boundary. A pinned reader holds its generation through cleanup. [Checkpoint creation](../../crates/ledger-replica/src/checkpoint.rs) · [File format and retention](../../crates/ledger-replica/src/checkpoint_files.rs)

On a trusted local filesystem, the seal can reuse a predecessor SST or blob digest when the files share an inode and their length and modification time match. On restart, unchanged local immutable files use the recorded identity and metadata; remaining files are hashed. A full scrub hashes every byte, including files whose timestamps match. A writable clone uses hard links for immutable files where supported and private copies for mutable RocksDB metadata. This local fast path assumes the filesystem honors those identity and immutability properties. Physical peer transfers verify file contents against the offered manifest. A replay transfer checks the local base and the exact committed command interval before sealing a new image. [RocksDB Rust checkpoint binding](https://github.com/rust-rocksdb/rust-rocksdb/blob/v0.24.0/src/checkpoint.rs)

The durable Raft store checks the boundary entry and every following entry, including its uncommitted suffix. The working state applies only the committed suffix. Independent journal verification and an explicit rebuild use the retained log. A voter that has physically reclaimed a prefix retrieves those older commands from its pinned independent image. A stopped voter can scrub its local checkpoint. Full repair on a reclaimed voter also needs the pinned independent image. A protected service also confirms the independent POSIX journal before releasing a balance or payment outcome. Its initial comparison resumes from the voter's saved journal digest when available and starts at genesis otherwise. [Operator commands](../../README.md#local-voter-checkpoints) · [Journal verification](0015-offsite-commit-protection.md)

In three warm local runs with 34,560 accounts and 34,559 postings, median voter reopen from a checkpoint plus six committed Raft entries took 225 ms. Rebuild from genesis on the same log took 1,362 ms. The second capture paused apply for 28 ms and spent a further 70 ms sealing and publishing. It shared 20,557,309 bytes of immutable SSTs with the first capture and added 648,033 SST bytes. Each run used a fresh directory on an Apple M2 with 8 GB RAM, the development profile and simulated peer acknowledgments. The benchmark reopens from the checkpoint first, then runs the full rebuild with OS caching enabled. The timed operations exclude networking and independent journal recovery. [Reproduce the local checkpoint measurement](../../crates/ledger-replica/examples/checkpoint.rs)

## Peer installation and retention

The serving replica retains 8,192 ordinary Raft entries before its latest sealed cut by default. Operators can change this count through `checkpoint.retained_entries`. The sender uses the Raft storage contract to return a compacted range for older entries and offers the sealed snapshot when the follower needs it. A matching log prefix can fast-forward without installing the image. [Raft storage contract](https://docs.rs/raft/0.7.0/raft/storage/trait.Storage.html) · [Peer transport](0008-peer-transport.md)

The receiver keeps its current state visible while staging. It authenticates the remote manifest, obtains the files or reconstructs the offered state by verified replay, seals a local generation, and checks the RocksDB ledger identity, schema and applied frontier. Only a matching Raft `Ready` publishes the local manifest reference, newly adopted history range, receiver hard state and any new suffix in one synced write. Read caches and inspection views switch with the working projection. Ordinary restart follows the durable reference after an interruption. [Voter installation](../../crates/ledger-replica/src/replica/snapshot.rs) · [Atomic Raft publication](../../crates/ledger-replica/src/raft_store/snapshot.rs)

After sealing a physical transfer, the receiver releases the workspace’s file links immediately. A small receipt ties the sender’s manifest to the local seal, so a repeated offer can reuse every file while that generation remains valid. Interrupted downloads can resume for 24 hours after their last activity. A replacement offer can carry forward complete objects with the same size and SHA-256 digest, and partial ranges when the name, size and digest all match. The receiver hashes the completed object before accepting it. Voter startup, new offers and an hourly check reclaim idle transfer files. Cleanup skips active readers and preserves unrecognized contents for inspection. [Receive workspace lifecycle](../../crates/ledger-replica/src/checkpoint_files/transfer_lifetime.rs)

Each serving voter can access the original committed commands for independent journal comparison and repair from genesis. A returning voter therefore fetches the missing command range in addition to missing state files. Physical installation can use the transferred projection directly. The replay path executes only the entries after the local seal. A new voter with no local history needs both the full image and its command history. Logical compaction changes replication. A separate offline command reclaims the duplicate local command bytes and pins the exact independent copy. [Physical reclamation](0017-raft-log-reclamation.md) [Journal verification and repair](0015-offsite-commit-protection.md)

### Verified replay across different RocksDB layouts

On a new offer, use the active local checkpoint as a candidate if it precedes the offered cut by at most 4,096 Raft entries and matching immutable files cover less than half of the offered file bytes. A receiver with partial physical downloads continues from its staged files. A cold receiver requests files. [Receiver selection](../../crates/ledger-replica/src/checkpoint_files/transfer.rs)

The authenticated sender reads the exact committed Raft bytes from the base entry through the target. Its SHA-256 digest binds the protocol version domain, the offered manifest token, the base index and term, and the length and contents of each entry. Both voters use a 32 MiB allowance including the base entry. The optimization needs locally accessible history over the proof interval. An older peer, an interval the sender declines for cost, or a proof request that times out can use the physical protocol. A conflicting authenticated proof stops the offer. [Proof and limits](../../crates/ledger-replica/src/raft_store/snapshot/replay.rs)

The receiver scrubs its locally sealed files, materializes a private writable clone and verifies the interval against its own committed entries and the received, durably staged suffix. Deterministic ledger apply reconstructs the target at the same index and term. This uses Raft's common committed prefix and the trusted local checkpoint. The digest authenticates commands, including entries that leave ledger balances unchanged. The receiver seals its own physical layout. Only the matching Raft `Ready` can publish that seal. Disk work runs outside the Raft owner. The local scrub, clone, replay and seal consume disk bandwidth, including a read of the entire base. The replay limits bound command work. [Local reconstruction](../../crates/ledger-replica/src/checkpoint_files/transfer_replay.rs) · [Crash-safe installation](../../crates/ledger-replica/src/replica/snapshot.rs)

### Measured peer catch-up under protected writes

Three fresh three-voter clusters ran on one Apple M2 with 8 GB RAM, a warm local filesystem and a Rust development build. Each run posted 8,192 payments while one voter was offline. After that voter installed the first checkpoint, the drill took it offline again and added at least 2,048 more payments. Each returning voter had to recover the first posting, apply a payment committed after the checkpoint and reproduce its stored outcome. The final balance accounted for every acknowledged payment. [Reproduce the network drill](../../crates/ledger-node/tests/network/recovery/snapshot_load.rs)

| Median across three runs | Cold follower | Shared checkpoint |
| --- | ---: | ---: |
| File data received | 2.795 MB | 1.129 MB |
| Original Raft command history received | 0.997 MB | 0.322 MB |
| Manifest received | 923 B | 1,353 B |
| File bytes reused | 0 | 2.762 MB |
| Follower startup | 19.5 ms | 57.1 ms |
| End of startup to applying the target entry | 718 ms (703–726) | 814 ms (776–832) |
| Protected write p50 / p99 during catch-up | 39.03 / 53.35 ms | 34.95 / 50.13 ms |
| Protected write p50 / p99 in matched baseline | 29.96 / 35.97 ms | 31.74 / 37.53 ms |
| Receiver directory allocated bytes, start to sampled peak | 0.201 to 4.182 MB | 4.461 to 5.849 MB |

Peer traffic used mTLS over loopback. Timing covered Raft negotiation, checkpoint installation and the committed tail. The load used 32 concurrent callers, each with one protected write in flight through the embedded API. Baselines used the same caller count and per-caller number of writes. Across all three runs, 1,716 cold-case writes and 2,475 shared-case writes finished during catch-up. Percentiles are medians of per-run percentiles and include slow calls that finished after catch-up. Public gRPC client overhead is outside these timings. MB values use decimal units. Payload bytes exclude framing and TLS. Receiver disk samples counted each inode once every 100 ms, so the reported peak is a lower bound. A 1 TB transfer still needs its own measurement.

### Independently compacted receiver

The original physical-transfer baseline rebuilt the returning voter's projection from its own log and made that voter seal ten checkpoints across 1,280 new protected payments. RocksDB recorded four completed compactions. At the same committed index the sender's seal contained 15 SSTs and the receiver's seal contained 46. An independent comparison found zero byte-identical SSTs. The drill then took the receiver offline, wrote another 2,048 payments and compared the bytes it actually fetched and reused against the files in the two seals. It checked original receipts and the final balance. [Network fixture and file comparison](../../crates/ledger-node/tests/network/recovery/snapshot_load.rs)

| One release run | Cold receiver | Shared checkpoint | Independently compacted |
| --- | ---: | ---: | ---: |
| File bytes received | 2,795,201 | 1,068,725 | 5,330,916 |
| File bytes reused | 0 | 2,762,347 | 0 |
| Original Raft history bytes received | 996,826 | 311,021 | 249,327 |
| Reopen / catch-up time, ms | 13.3 / 614.7 | 43.4 / 661.6 | 63.7 / 1,264.2 |
| Protected writes completed during catch-up | 473 | 512 | 928 |
| Protected write p99, catch-up / baseline, ms | 58.91 / 31.11 | 72.17 / 18.09 | 58.76 / 37.92 |
| Receiver allocated bytes, initial / sampled peak | 196,608 / 4,165,632 | 4,423,680 / 5,722,112 | 10,878,976 / 16,605,184 |

The three phases occurred in sequence as history grew. The compacted receiver had matching logical state at the common cut and still needed every offered file. Reuse depends on identical immutable file contents, and the working history at the final phase was larger than at the first. [Public foreground and ordinary log catch-up measurements](../measurements/2026-11-08-local-load.md#public-api-workload)

### Catch-up after adding verified replay

The same M2 release drill ran three times using the original binary and three times using the new binary. Each trial used fresh three-voter directories, mTLS, the protected commit path and 32 concurrent foreground writers. All six trials observed four receiver compactions and zero matching SST bytes at the independently compacted cut. Each trial checked the original and new receipts, the entry after the offered cut and the final payment balance. [Workload](../../crates/ledger-node/tests/network/recovery/snapshot_load.rs) · [Per-run bytes, timings and measurement definitions](../measurements/2026-11-08-snapshot-replay.json)

| Median of three independent-compaction runs per binary | Original | Verified replay |
| --- | ---: | ---: |
| Snapshot transfer payload | 5,620,000 B | 251,482 B |
| RocksDB file bytes fetched | 5,368,584 B | 0 B |
| Catch-up time after reopen | 1,135.4 ms | 756.3 ms |
| Foreground protected write p99 during catch-up | 59.10 ms | 52.12 ms |
| Receiver disk allocation at sampled peak | 16,715,776 B | 12,201,984 B |

The source grows during foreground traffic, so two trials can produce slightly different offers. For each optimized run the fixture also independently counted the file bytes the original algorithm would need for that **exact offer**. Its median total was 5,608,549 B. Replay saved a median of **5,357,067 B (95.52%)**, with reductions of 95.50–95.53% across the three offers. The 251,482 B comprised 2,213 B of manifest, 249,172 B of original history and 97 B for the encoded proof request and response. Counting all reverse RPC message bodies in both directions gave 251,742 B. HTTP/2 and TLS framing, ordinary Raft traffic and the control offer are outside these totals.

Cold and shared-checkpoint controls kept physical transfer and matched the original algorithm's exact-offer byte count in every run. Cold catch-up medians were 606.7 ms before and 609.3 ms after. Shared-checkpoint medians were 653.6 ms and 684.0 ms. These local results do not establish latency for larger stores or different disks.

## Acceptance criteria

- Deferred for this milestone because of local disk capacity. With over 1 TB of live ledger files, measure checkpoint pause, bytes read and written, local clone time, replay entries per second, and time to the first current read.
- Compare a short log gap, shared-base transfer, independent compaction and cold bootstrap. Record bytes reused and transmitted, catch-up time, peak disk use and foreground p99 latency. A short gap transfers no snapshot. An uninterrupted shared-base transfer sends each missing object once.
- Interrupt transfer, publication and compaction. Resume verified data, reject corruption or incompatible images, and reproduce the journal, holds and original retry outcomes at the same applied index.
