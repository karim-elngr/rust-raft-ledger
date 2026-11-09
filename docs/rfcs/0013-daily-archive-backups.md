# RFC 0013 — Daily archive backups

The following describes the three-voter deployment. Groups configured with a permanent learner use quorum acknowledgements and can export asynchronously. [Learner topology](0018-static-raft-learner.md) · [Async archive](0019-async-recovery-archive.md)


**Status** — Deferred. Local development uses the synchronous POSIX recovery journal. Dated daily archives are outside the current scope.

## Problem

The ledger must recover every acknowledged payment after permanent loss of the voter disks. Does local development also require keeping a separate dated copy of the complete ledger every day?

## Proposed solution

Keep the committed entries through every protected acknowledgment on the shared POSIX volume. To survive permanent loss of all voter disks, deploy that volume in a failure domain that survives the same event. An independently stored recovery checkpoint can shorten reconstruction, and a peer checkpoint can help an individual voter catch up. Both work independently of any daily schedule. [Disaster recovery](0015-offsite-commit-protection.md) · [Recovery checkpoints](0016-journal-checkpoints.md) · [Peer checkpoints](0012-incremental-snapshots.md)

## Tradeoffs

This keeps local operations simple. Operators rely on the recovery volume's durability and the ability to verify its journal. A requirement to recover a historical business date or survive deletion of the independent volume would require a separate retention and protection decision. S3 or an equivalent object store remains a future backend option.
