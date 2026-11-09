# RFC 0009 — Public ledger protocol

The following describes the three-voter deployment. Groups configured with a permanent learner use quorum acknowledgements and can export asynchronously. [Learner topology](0018-static-raft-learner.md) · [Async archive](0019-async-recovery-archive.md)


**Status** — The three-voter service implements `ledger.v1.Ledger` and a Rust client for backend applications. Browser access, an HTTP/JSON binding and account-specific authorization remain open.

## Problem

A client that loses a payment reply needs to recover the original outcome without posting twice. Callers in different languages must also agree on every minor unit, including amounts outside the exact integer range of common JSON number parsers.

## Proposed solution

Expose the ledger operations over HTTP/2 with mutual TLS. The six writes are `CreateAccount`, `SetAccountBounds`, `PostEntry`, `ReserveEntry`, `CaptureReservation` and `VoidReservation`. The four reads are `GetAccount`, `GetEntry`, `GetRequestOutcome` and `GetReservation`. The optional `[client]` listener requires independent commit protection. Its trusted client CA grants authenticated backends access to all methods by default. The optional administrator leaf allowlist restricts account creation, bounds changes and corrections. A separate monitor service exposes each member’s local health. Peer traffic uses its own listener and identity checks. [Public schema](../../crates/ledger-api/proto/ledger/v1/ledger.proto) · [Voter configuration](../../README.md#configure-a-voter)

Use canonical ASCII decimal strings for 128-bit IDs, amounts and counters. Signed balances and bounds span `−(2^128 − 1)` through `2^128 − 1`. The wire minus sign is ASCII `-`. Zero is `"0"`, and an absent bound leaves that side unrestricted. Journal entry IDs are Protobuf `uint64`. The v1 adapter normalizes each logical command for the versioned Raft codec. Retry identity includes ordered funding sources, posting lines and capture allocations. [Adapter](../../crates/ledger-api/src/adapter.rs) · [Protobuf serialization](https://protobuf.dev/programming-guides/serialization-not-canonical/)

For example, request `"17"` on ledger `"1"` can post `"8000"` cents using a `SHORTFALL_REQUIRE_FULL` waterfall. With `"3000"` available from account `"2"` and enough from `"3"`, the protected decision returns the entry ID, debits of `"3000"` and `"5000"` in source order, and an `"8000"` credit to destination `"4"`. An identical retry returns the same entry. Reversing the sources under `"17"` returns the typed rejection `ERROR_CODE_ID_CONFLICT` and preserves the original decision. [Funding waterfalls](0004-funding-waterfalls.md) · [Request identity](0005-request-identity.md)

| Result | Meaning for the caller |
| --- | --- |
| gRPC `OK`, `Decision.accepted` or `Decision.rejected` | The write reached a local committed, applied result covered by the independent journal. A rejection carries an error code and applicable context. |
| `INVALID_ARGUMENT` or `OUT_OF_RANGE` | Correct malformed or oversized input. Fixed limits include a 1 MiB request and 1,024 items per repeated input. |
| `RESOURCE_EXHAUSTED` | Temporary saturation. Back off before retrying. |
| `UNAVAILABLE`, a deadline or a lost reply | A write may have committed. Retry with the same ledger ID, request ID and body. |
| Successful lookup with an absent value | The object was absent at the protected read point. A pending request can still commit later. |

Current reads obtain a quorum-confirmed read index, sample one locally applied view and wait for independent coverage through that view. The write and read gates also apply during leader changes. [Current reads](0006-raft-replication.md) · [Offsite protection](0015-offsite-commit-protection.md)

The Rust client checks the encoded request size before copying it for retries or opening a connection. It starts from a configured seed and reuses the last successful seed. On `UNAVAILABLE` it follows a `ledger-leader-id` hint only to an untried configured voter. It rotates through the remaining seeds and backs off between rounds. Retries share an overall deadline and preserve the request. A first-attempt overload returns immediately. If a write may already have reached the ledger, a later overload returns `UNAVAILABLE` with an unknown outcome. [Rust client](../../crates/ledger-api/src/client.rs) · [gRPC status codes](https://grpc.io/docs/guides/status-codes/)

## Tradeoffs

Decimal strings cost more wire bytes than fixed-width integers while preserving the complete range across languages. The independent journal adds a durability round trip to payment acknowledgments. Deployments control which backend identities receive ledger-wide access through their client CA.

## Acceptance criteria

- Round-trip the maximum counter and both balance extremes through Rust and the C++ Protobuf tools. Preserve absent bounds and reject noncanonical forms such as `"-0"` and `"01"`.
- After a lost reply or a stale leader hint, retry through another seed. Verify one posting and the original entry ID. An identical rejected payment must return its original rejection.
- Pause offsite protection and commit a payment. Its duplicate and current lookups wait or time out until their sampled views are protected.
- Accept equivalent Protobuf field orderings as the same request. Changing an ordered posting input under the same ID produces an ID conflict.
- Reject over-limit submissions before proposal. Reject a backend credential outside the listener's client trust roots.
