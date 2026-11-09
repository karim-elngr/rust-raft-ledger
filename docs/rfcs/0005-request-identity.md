# RFC 0005 — Request identity and retries

**Status** — Implemented for all six mutation types. The ledger keeps the first canonical request and its decision. A public retry or decision lookup waits for independent recovery coverage. [Decision storage](../../crates/ledger-replica/src/projection/delta.rs) · [Protected reads](../../crates/ledger-node/src/reads.rs)

## Problem

A leader can apply a payment and lose the reply. A client sending the payment again needs to learn the original outcome without moving money twice. This also applies to reservations, captures, policy changes and rejected commands.

## Proposed solution

Give every mutation a caller-supplied request ID. Scope the ID to a ledger and share that namespace across command types. Keep the canonical client command and its outcome for the lifetime of the ledger. The identity includes the command kind, posting-line order, ordered preconditions, waterfall order and limits, and any capture allocations and remainder instruction. Exclude connection details and server-generated values.

Check for a recorded outcome before evaluating the request against current balances and policies. Apply the following rules in committed order.

| What the replica finds | What the client receives |
| --- | --- |
| New ID | Evaluate once. Record the payload identity and the posted entry ID, reservation ID, funding split or business rejection, as applicable. |
| Same ID and same canonical request | Return the original outcome, including the original funding split or rejection. |
| Same ID and different canonical request | Return an ID conflict and preserve the original outcome. |

Preserving acknowledged payments after loss of every voter disk requires a recovery journal outside those disks' failure domain. Expose a terminal outcome only once its committed decision has been applied and the recovery journal covers the contiguous committed prefix through that decision. This gate also applies to duplicate outcomes, business rejections and ID conflicts. [Commit protection](0015-offsite-commit-protection.md)

A timeout after local commit or after offsite sync but before the reply leaves the outcome unknown to the client. Retry the original ID and body, or call `GetRequestOutcome(ledger_id, request_id)` against a protected current view. An unknown ID means there was no outcome at that read point. An in-flight request can still commit later. A fresh attempt after a business rejection uses a fresh ID. [Current reads](0006-raft-replication.md) · [Client protocol](0009-client-protocol.md)

For example, Alice has $100 and Bob has $0. A transfer of $80 with request ID `17` succeeds, then the reply is lost. A retry returns the same entry ID, leaving Alice with $20 and Bob with $80. Changing the amount under `17` returns a conflict. A new $80 request fails Alice's zero-floor check and retains that rejection even if Alice is funded later.

## Tradeoffs

Durable outcomes let clients retry after failover or log compaction. Retaining payload identities and outcomes increases storage use and snapshot size. Including line and source order makes retries predictable and preserves the caller's funding instructions. A client that reorders a request must treat it as a different payload.

## Acceptance criteria

- Lose the reply after offsite protection, destroy all voter disks and restore from the independent copy. Retrying the original request returns its entry ID and allocation. The journal contains one payment.
- Retry an insufficient-funds rejection after adding funds. The same ID returns the saved rejection. A new ID evaluates against the new balance.
- Reuse an ID across command types, reorder waterfall sources or change a capture's remainder instruction. Each change returns a conflict and leaves the original outcome and balances intact.
- Repeat the lost-reply test after snapshot installation and log compaction.
