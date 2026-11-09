# RFC 0003 — Reservations and capture

**Status** — Implemented. A reservation freezes its original funding lines. Callers can capture all or part, retain or release the remainder on a partial capture, and void outstanding funds. Automatic expiration remains open. [Reservation state machine](../../crates/ledger-core/src/reservation.rs)

## Problem

A payment can be authorized before its final amount is known. A $100.00 wallet purchase might settle for $70.00, with proceeds split between a merchant and a fee account. The ledger must protect the authorized amounts on those accounts and let the caller choose the final split.

## Proposed solution

`ReserveEntry` takes a request ID, a reservation ID, explicit balanced lines or ordered waterfall instructions, and any one-off conditions. Resolve the funding lines once when admitting the hold. Admission checks the conditions, reserves capacity for the posted counters, and adds each line's amount to the matching pending counter. On each account the ledger reserves gross debits and credits separately, even when both appear in the hold. Posted counters stay at their previous values. Every subsequent posting and bound change must respect the outstanding hold. [Balance constraints](0002-balance-constraints.md) · [Funding waterfalls](0004-funding-waterfalls.md)

`CaptureReservation` takes its own request ID and the reservation ID. For a two-line hold, one positive amount determines both sides. For a compound hold, the caller allocates a positive capture amount across original line IDs, the positions in the immutable reservation. Each referenced ID must be unique, each allocation must fit that line's remaining amount, and the allocated debits and credits must balance. The original lines determine the accounts, sides and asset. A successful capture moves the allocated amounts from pending to posted and records one journal entry.

A full capture closes the hold. Every partial capture requires an explicit disposition.

- `keep_reserved` leaves the unused amount on each line pending for a later capture or void.
- `release_remainder` releases all unused amounts and closes the hold in the same operation.

`VoidReservation` takes its own request ID and the reservation ID. It releases every remaining pending amount and closes the hold. A void after a partial capture preserves the journal entry already posted. A new request cannot consume a closed hold.

Concurrent resolutions apply in committed order. Each resolution writes its outcome, reservation state, affected counters, any journal entry and the applied index atomically. A retry returns its original outcome, including after failover. Capture allocations and remainder disposition are part of request identity. [Request identity](0005-request-identity.md) · [Durable recovery](0007-durable-recovery.md)

## Tradeoffs

Reserving both sides separately protects every permitted partial allocation. A credit-normal wallet with $50 posted and a zero floor cannot reserve a $60 debit and a $20 credit on that wallet within the same balanced hold. An immediate entry with those lines can pass because the wallet's posted balance ends at $10. Referencing original lines keeps fee and rounding decisions with the caller. Automatic hold expiration remains open.

## Acceptance criteria

The buyer starts with $100.00 posted, a zero floor and no prior holds on a USD ledger. The merchant and fee accounts have room to receive the amounts shown.

| Original line | Reserved | Capture | Remainder |
| --- | ---: | ---: | ---: |
| Buyer wallet debit | $100.00 | $70.00 | $30.00 |
| Merchant payable credit | $97.00 | $67.90 | $29.10 |
| Fee revenue credit | $3.00 | $2.10 | $0.90 |

- Admission increases pending counters by the reserved amounts. An unrelated posting cannot spend the buyer's reserved funds. A bound change that would invalidate the hold fails.
- With `keep_reserved`, post the three capture amounts once. The remainder column stays pending. Capturing those remaining amounts closes the hold. With `release_remainder` on the original $70.00 capture, post that same split and close the hold immediately.
- A $70.00 capture with $66.00 to the merchant and $4.00 to fees fails because the fee line reserved only $3.00. A missing partial disposition, repeated line ID or unbalanced allocation also fails. Each failure preserves the entire hold as it stood before the attempt.
- Race a full capture against a void using different request IDs. Only the first committed resolution succeeds. A retry of the winner returns the original outcome, and a retry with a changed allocation or disposition reports an ID conflict.
