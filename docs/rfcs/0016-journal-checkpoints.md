# RFC 0016 — Shorten a whole-cluster restore

## Problem

After every voter disk is lost, rebuilding three copies of the same balances and request outcomes spends time executing commands whose effects were settled years ago. The independent journal must still cover each protected acknowledgment. Physical placement stays open, and local development uses the existing POSIX volume.

## Proposed solution

`ledger-node journal-checkpoint CONFIG.toml` captures a nonempty live journal position. A separate builder replays authenticated journal commands into two private RocksDB databases. One contains the original committed Raft entries. The other contains the ledger projection, including reservations, decision records and indexes. The builder records the exact ledger definition, index, term and journal digest in the raw history image. The first build reads from genesis. Later builds copy the previous verified image and apply the entries since its source position. Each build writes a complete independently recoverable image. A later build still reads and writes the base files even when its command tail is small. [Builder](../../crates/ledger-replica/src/recovery/base.rs)

A versioned SHA-256 manifest covers both file trees. The builder syncs the files and their directory entries, then publishes a separate `BASE` pointer. The binding includes the exact source head and the cluster IDs already used through that head. Publication compares the current binding and generation under the journal lock. Normal payments can extend the same live journal while the builder works. Fencing the source generation prevents an older builder from publishing afterward. The command runs on demand. [Journal binding](../../crates/ledger-recovery/src/journal/base.rs) · [Image format](../../crates/ledger-replica/src/recovery/base_image.rs)

A restore verifies the file hashes while copying the two databases to private voter storage. It validates the neutral history and ledger frontiers, assigns the fresh voter identity, verifies the journal suffix through the exact fence, and applies that suffix. A new local checkpoint and a voter-specific proof of the imported base and fence become durable before the voter directory is published. Activation uses that proof to compare the suffix and atomically change generation. The first protected request checks journal ancestry through activation. Old installations and journals without a base retain full-history recovery. [Voter restore](../../crates/ledger-replica/src/recovery.rs)

## Tradeoffs

This shortens command replay. Restoring an empty disk still requires reading and verifying the complete image, writing a private copy and sealing the restored state. The image contains both original Raft commands and projected state, so its byte size can exceed the command journal. A damaged published image or suffix prevents a restore through that base. The explicit `restore-from-journal` command verifies and replays from genesis when the full journal survives. A full journal scrub independently verifies all retained segments, including those older than the base. Checkpoint creation and restore can compete with live transactions for device bandwidth on the chosen volume.

The recovery journal still participates in every protected acknowledgment. The base has its own publication pointer and can lag by an arbitrary number of entries without changing the acknowledged recovery point. [Acknowledgment and fencing](0015-offsite-commit-protection.md)

The matched local disaster drill compares both paths at 8,194 and 65,538 outcomes. [Recovery measurements](../measurements/2026-11-08-local-load.md#recovery-and-physical-storage)
