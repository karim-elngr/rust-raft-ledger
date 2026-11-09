# RFC 0019 — Recover an asynchronously archived prefix

## Problem

A backup on every voter's write path adds a second storage dependency to every payment. Moving the backup to a learner requires an exact answer to “how far can we restore?” across crashes, lag, retries and storage outages.

## Proposed solution

The learner runs a background worker that reads bounded pages of original committed Raft entry bytes up to its applied index. It includes empty entries, retries and rejected commands so the existing restore can reproduce the ledger at a precise Raft index. The worker asks the archive adapter to verify a previously observed head and persist a contiguous page. The POSIX adapter uses the existing sync-and-rename journal with its conditional HEAD, authenticated segments and generation fence. It compares overlapping bytes on retries. Initialize the archive explicitly on independent storage and monitor its confirmed index. Live learner startup only compares configured paths. The POSIX worker checks their filesystem targets before opening the archive, so a missing or inaccessible mount can recover in place.

The application owns three small Rust traits. `LedgerFeed` supplies bounded reads, `RecoveryArchive` confirms a durable raw prefix, and `EventPublisher` confirms delivery of a page of decisions. The export crate owns cursor updates and retry policy. The node selects implementations at startup. Each enabled destination has a separate worker and a durable local cursor scoped to the ledger, generation and destination. Archive confirmation precedes cursor sync, so an interrupted operation may compare and repeat a page. A missing or unverifiable prefix, a rollback or a generation mismatch stops progress. The previous local cursor and the live verified archive frontier are reported separately. A fresh learner attaching to an existing archive compares its history in bounded pages. The POSIX journal is a backward-linked chain. An initial comparison walks the relevant segments to build an on-disk traversal index before reading them in ascending order. First attach can take time proportional to the archived history. Ordinary restart with a matching saved HEAD and local boundary hash resumes at its cursor.

`NoopArchive` disables archive work. Its frontier is absent, including after startup. Both ports disabled requires no cursor storage. A future object-store adapter must provide equivalent durable conditional updates, exact overlap checks and generation identity to qualify as a recovery archive. Its restore tooling must also understand its storage format. The POSIX archive supports the existing journal scrub, recovery checkpoint, fencing and restore commands.

## Tradeoffs

Voters retain their complete local raw history in this configuration. Logical Raft compaction and snapshot catch-up continue, while the offline command that moves old raw history onto an external mount is limited to the protected topology. This keeps learner recovery and peer catch-up independent of archive availability at the cost of disk growth. A slow disk or an outage grows the unarchived suffix. Graceful learner shutdown waits for in-flight destination operations, which can take as long as a stalled POSIX operation or the configured Kafka delivery timeout. There is no separate daily backup. For disaster recovery, isolate the former voting majority before restoring the last verified cut.

[Raft learner](0018-static-raft-learner.md) · [Recovery journal](0015-offsite-commit-protection.md) · [Export ports](../../crates/ledger-export/src/lib.rs) · [POSIX adapter](../../crates/ledger-export-adapters/src/posix.rs)
