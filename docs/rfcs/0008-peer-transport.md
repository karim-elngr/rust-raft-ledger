# RFC 0008 — Raft peer transport

The following describes the three-voter deployment. Groups configured with a permanent learner use quorum acknowledgements and can export asynchronously. [Learner topology](0018-static-raft-learner.md) · [Async archive](0019-async-recovery-archive.md)


**Status** — The fixed three-voter group uses mutual TLS, versioned envelopes and independent send workers. The peer listener gives incoming TLS handshakes two seconds to complete. Ordinary Raft messages and resumable checkpoint transfers use separate connections.

## Problem

A slow follower can hold up elections or replication to a healthy voter if all three share one send queue. A follower catching up after compaction also needs a ledger image that may be larger than RAM. The transport must deliver Raft messages and snapshots while the healthy majority continues serving writes.

## Proposed solution

Give each member a private tonic gRPC listener over HTTP/2 and reuse a channel to each peer. Require mutual TLS. Bind the authenticated certificate to its configured cluster and member ID. Validate the envelope's protocol version, cluster, sender and recipient against that identity and the decoded Raft message before delivery. [tonic transport](https://docs.rs/tonic/latest/tonic/transport/) · [Client certificate verification](https://docs.rs/tonic/latest/tonic/transport/server/struct.ServerTlsConfig.html#method.client_ca_root)

Use a unary `Step` RPC for ordinary peer messages. A receipt means the receiving node admitted the message to its bounded inbox. Its single Raft owner calls `RawNode::step`. Raft generates any vote or append response as a separate message. A receipt grants no vote and advances no commit index. Release persistence-dependent messages only after the required writes. [Raft replication](0006-raft-replication.md) · [raft-rs Ready](https://docs.rs/raft/0.7.0/raft/raw_node/struct.Ready.html)

Generate the versioned envelope with `tonic-prost-build`. Carry an encoded `eraftpb::Message` in a protobuf `bytes` field and decode it with raft-rs's default rust-protobuf 2 codec. The optional raft-rs 0.7 Prost codec uses Prost 0.11, while tonic-prost 0.14 uses Prost 0.14. A byte field avoids coupling the generated Rust types. [raft-rs manifest](https://raw.githubusercontent.com/tikv/raft-rs/v0.7.0/Cargo.toml) · [raft-proto manifest](https://raw.githubusercontent.com/tikv/raft-rs/v0.7.0/proto/Cargo.toml) · [tonic-prost dependencies](https://docs.rs/crate/tonic-prost/0.14.6)

Use bounded Tokio queues and independent send workers per peer. Bound message size, queued bytes and in-flight requests. Reserve capacity for heartbeats and votes. On saturation or deadline, report the failed delivery to the Raft owner and let it continue ticking and sending to other voters. [Tokio bounded queues](https://docs.rs/tokio/latest/tokio/sync/mpsc/) · [raft-rs transport feedback](https://docs.rs/raft/0.7.0/raft/raw_node/struct.RawNode.html#method.report_unreachable)

Put a small versioned descriptor with the sealed manifest's digest in `MsgSnapshot.snapshot.data`. Retain the Raft index, term and membership in snapshot metadata. Fetch the full manifest and transfer only the receiver's missing verified objects through a separate bounded, resumable stream. The receiver pins reusable objects, stages missing chunks durably and verifies all required content. It then hands the original Raft message with its descriptor to `RawNode::step`. Only a matching snapshot emitted by `Ready` can publish staged state. Expire abandoned staging after its resume window. The sender calls `report_snapshot` with the transport result after verified delivery to the receiving Raft owner or failure. [Snapshot format](https://raw.githubusercontent.com/tikv/raft-rs/v0.7.0/proto/proto/eraftpb.proto) · [Incremental snapshots](0012-incremental-snapshots.md)

## Tradeoffs

The implemented snapshot protocol uses an authenticated `OfferSnapshot` call and receiver-driven range requests. The offer names one source, recipient, cluster and sealed manifest. The sender pins its files until that offer finishes or expires. The receiver pulls the authenticated manifest on a separate connection. For a suitable local checkpoint it asks the same authenticated sender for a bounded history digest through the offered cut using `ReadSnapshotReplay`. A peer without this capability returns `UNIMPLEMENTED`. Physical transfer reads database file ranges up to 256 KiB. Both paths read missing original command bytes in bounded pages. It stages progress durably and validates the file digests or verifies the replay interval and seals the reconstructed state before passing it to Raft. A dropped connection can resume at the persisted offset for the same generation. The replicated fixed membership, authenticated peer and Raft term identify the sender that the voter trusts to supply the snapshot state. [Peer protocol](../../crates/ledger-node/proto/peer.proto) · [Snapshot transport](../../crates/ledger-node/src/transport/snapshots.rs)

Each voter admits one incoming transfer. Individual reverse reads have a 30-second timeout. Successful reads renew a 24-hour idle lease on both ends. The leader preserves the corresponding append suffix during the transfer. After admission, each advance in the follower's matched index renews a one-minute grace period until the follower reaches the regular retention window. Supersession and loss of leadership cancel the outgoing lease. An older peer can still enforce its original 24-hour total deadline. Transfer statistics count manifest, file and original history payloads, proof traffic and reverse RPC message bodies for the completed offer. Previously staged bytes and locally reused files appear together. Network framing and earlier attempts are excluded. [Current catch-up qualification](../replication-qualification.md)

Unary RPCs simplify reconnects and add framing per message. Chunking bounds memory but requires temporary disk space and cleanup.

## Acceptance criteria

- Stall one voter during snapshot transfer. Verify bounded queues and memory while the other two keep committing.
- Drop a `Step` receipt, duplicate a delivery and reject a mismatched peer identity. Verify that the journal converges with one entry per accepted request.
- Corrupt a snapshot chunk or supersede a transfer. Verify that the active ledger survives and that a later verified transfer restores the follower at the expected index.
- Offer a malformed or conflicting replay proof, then retry the same snapshot using a peer without the optional proof RPC. Only the verified physical image reaches Raft. [mTLS regressions](../../crates/ledger-node/src/transport/snapshots/tests.rs)
