# Replication and checkpoint qualification

## Findings and evidence

| Finding | Change and tested behavior | Qualification boundary |
| --- | --- | --- |
| Ordinary replication exceeded the 4 MiB wire limit | A three-voter regression built an eight-entry backlog with legal batches. raft-rs 0.7 combined bounded slices into a 5,421,716-byte `MsgAppend`. The replica now disables this second merge and keeps the original 2 MiB budget for entry bodies. The returning voter applies the full backlog, each serialized message fits the transport's limit, and its eight original decisions agree with the leader. [Regression](../crates/ledger-replica/tests/replication.rs) | This exercises real Raft drivers and serialized bytes with in-process delivery. |
| A checkpoint captured the projection on the voting thread | The serving owner hands a reserved projection generation to the blocking worker. RocksDB atomically flushes the six column families. The worker verifies the ledger and frontier in the captured database and uses the corresponding committed Raft term. A test occupies the sole blocking worker, submits a manual checkpoint, and processes another committed entry before releasing the worker. The checkpoint includes that entry. A restart test checks the seal and a later committed tail. A late completion also preserves a newer incoming snapshot index. [Serving test](../crates/ledger-node/src/checkpoints.rs) · [Recovery test](../crates/ledger-replica/tests/checkpoint.rs) | Disk contention, RocksDB write stalls and synchronous Raft WAL operations can still delay a voter. Measure heartbeat gaps and payment latency on the target storage under concurrent flush and compaction. |
| A healthy slow snapshot expired 24 hours after its offer began | Successful reverse reads renew a 24-hour idle lease. The local Raft owner cancels superseded outgoing offers. After receipt, each advance in the receiver's matched index renews the one-minute suffix retention period. A virtual-clock test spans 36 hours of activity and a further 24 hours of inactivity. Another test advances appends over 135 seconds and checks that a late failure from an older offer cannot remove the new pin. [Transfer tests](../crates/ledger-node/src/transport/snapshots/tests.rs) · [Retention test](../crates/ledger-node/src/checkpoints.rs) | Both ends need the new implementation for an offer to exceed a day. A reverse read still has a 30-second deadline. Sustained link speed and disk headroom must support the offered workload. |
| An interrupted or superseded offer repeated completed work | The TLS fault test stalls a file read, admits a heartbeat, cuts the TCP connection and resumes at the missing range. A new transfer test changes the manifest after one 256 KiB partial write. The receiver preserves a matching complete object under a different name and the partial range under the same name and digest. Changing that digest restarts the partial at offset zero. All completed objects are hashed before sealing. [TLS fault test](../crates/ledger-node/src/transport/snapshots/tests.rs) · [Supersession test](../crates/ledger-replica/src/checkpoint_files/tests.rs) | New file contents require transfer. Staged Raft history for a different token starts again from the receiver's committed prefix. A crash inside the directory switch may lose reusable staging progress. Raft adopts only a verified image and matching history. |
| A reclaimed voter depends on its pinned independent image for older raw commands | Existing tests interrupt reclamation on each side of the durable pointer switch, remove the journal mount and restart. A local seal and retained boundary still work. Older raw reads and full rebuild report failure while the mount is missing. Restoring it permits exact verification. Another test publishes a newer base and checks the prior pin still serves the original entries. [Reclamation tests](../crates/ledger-replica/src/raft_store/reclaim/tests.rs) · [Operator procedure](rfcs/0017-raft-log-reclamation.md) | Preserve the exact pinned image and original mount path until the voter releases its claim. A shared-host rename tests missing storage. Independent-volume loss and restoration need an infrastructure drill. |

## Matched release drills under protected writes

All six release drills passed. Each build ran the same three receiver phases in a fresh three-voter cluster, giving three observations per build and receiver, 18 phase rows in total. The runner alternated the two builds and reversed their order in the second round. Each phase used 32 concurrent embedded clients making protected payments on loopback. The harness checked recovered receipts, the first payment beyond the seal, canonical Raft history bytes and the final posted balance. [Six snapshot transcripts](measurements/rc-local/supplement/) · [Trial order](../scripts/measure-local.py) · [Workload and assertions](../crates/ledger-node/tests/network/recovery/snapshot_load.rs)

Values are the median and full range of the three trials, in milliseconds. Catch-up starts when the follower has reopened and ends when it has installed the seal and applied the payment beyond it. Foreground p99 measures calls started during that window. Each of the 18 windows acknowledged between 416 and 864 protected writes before the target was observed, with 32 more calls finishing afterward and included in p99.

| Receiver | Original catch-up | Candidate catch-up | Original foreground p99 | Candidate foreground p99 |
| --- | ---: | ---: | ---: | ---: |
| Cold | 594.3 (571.0–632.9) | 614.6 (588.9–728.9) | 59.05 (53.11–62.01) | 67.83 (55.90–81.74) |
| Shared checkpoint | 680.2 (637.9–697.9) | 728.8 (600.6–749.3) | 58.02 (55.47–65.94) | 60.28 (58.97–77.04) |
| Independently compacted checkpoint | 797.1 (694.4–800.1) | 791.1 (736.1–933.7) | 54.74 (51.09–79.67) | 51.97 (47.03–81.79) |

Cold foreground p99 rose in all three pairs. The candidate also took longer to catch up in two of three pairs for each receiver layout. The independent-layout candidate had a slightly lower median and the slowest single observation, 933.7 ms. These local measurements include concurrent writes and compaction on a shared physical disk. The number of Raft entries and the size of the checkpoint varied with scheduling. Both builds sent zero file bytes and a 97-byte replay proof in every independent-layout trial. The `legacy_same_offer_*` columns estimate the physical-transfer alternative for that particular offer. [Raw phase rows](measurements/rc-local/supplement/)

## Earlier single release drill

One release run served 32 concurrent writers while a voter reopened, installed a checkpoint and applied a payment beyond the checkpoint. Each phase checked original receipts, and the final balance accounted for all acknowledged postings. Times below begin after reopen. The allocation sampler measures the returning voter's directory and can miss peaks between its 100 ms samples. [Raw trial, command and binary fingerprint](measurements/2026-11-09-snapshot-qualification.log) · [Fixture and counter definitions](../crates/ledger-node/tests/network/recovery/snapshot_load.rs)

| Receiver | Catch-up | Protected writes completed during catch-up | Foreground p99 for calls started during catch-up | Transfer payload | Sampled peak allocated |
| --- | ---: | ---: | ---: | ---: | ---: |
| Cold | 675.2 ms | 444 | 66.63 ms | 3,793,287 B | 4,169,728 B |
| Shared checkpoint | 705.3 ms | 567 | 55.82 ms | 1,358,378 B | 5,824,512 B |
| Independently compacted checkpoint | 685.6 ms | 556 | 60.54 ms | 251,265 B | 12,132,352 B |

In the ordinary-append regression, the successful run delivered six data-bearing appends totalling 10,843,488 encoded bytes, including retransmissions. The largest was 1,807,248 bytes. The release snapshot fixture used the protected embedded client on loopback. It exercised local multi-megabyte recovery. Qualification for an operator's target link, three independent voting disks, sustained retained history, and 1 TB requires the corresponding deployment drill. [Environment](measurements/rc-local/environment.log)

The physical path sends one 256 KiB range at a time and syncs each received range. For sizing, a cold 1 TiB of file content would require at least 4,194,304 serial reads. At a round-trip time of 10 ms, those round trips alone consume about 11.7 hours. File verification, history transfer, storage waits and the post-install append tail add time. On the intended hosts, cap the bulk link at its expected bandwidth and latency, interrupt it during a large file, then resume with ongoing payments and a newer checkpoint. Record staged and retransmitted bytes, held generations and suffix disk usage, heartbeat gaps, first confirmed payment and time to three healthy disks. Compare these observations to the owner's recovery targets. [Transfer loop](../crates/ledger-node/src/transport/snapshots/receive.rs)

## Local reproduction

Run these commands from the workspace root with the locked compiler and dependencies. This run used Rust 1.99.1 on arm64 macOS 27.2 and protoc 3.21.12. The TLS and opt-in network tests need loopback sockets. The fixtures create temporary voter and journal directories on the same host.

```sh
export PROTOC="$PWD/target/tools/protobuf-3.21.12/protoc"  # or set the path to your protoc 3.21.12
cargo test --locked -p ledger-replica --test replication lagging_voter_receives_a_large_backlog -- --nocapture
cargo test --locked -p ledger-replica --test checkpoint
cargo test --locked -p ledger-replica --lib superseded_offer_reuses
cargo test --locked -p ledger-replica --lib raft_store::reclaim::tests -- --nocapture
cargo test --locked -p ledger-node --lib checkpoints::tests
cargo test --locked -p ledger-node --lib transport::snapshots::tests
cargo test --release --locked -p ledger-node --test network snapshot_catchup_under_payment_load -- --ignored --nocapture
```
