# RFC 0004 — Ordered funding waterfalls

**Status** — Implemented for immediate postings and reservations. Each request supplies ordered sources and fixed destination amounts. The ledger records the resolved allocation when the sources cover the full payment. [Waterfall implementation](../../crates/ledger-core/src/funding.rs)

## Problem

A caller wants to spend rewards first and cash second on an $80 purchase. Between a client-side balance lookup and submission, another purchase could spend those rewards. The caller needs one command that applies its priorities to the balances available when the payment takes effect.

## Proposed solution

An immediate posting or reservation carries a request ID, target amount, ordered distinct sources, destination lines totaling the target, and a shortfall policy. Each source specifies a spend-down floor or explicitly selects its configured floor. Selecting an absent floor fails validation. A source may also have a per-payment cap. All accounts must exist in one ledger. Sources must share a posting side that decreases each source's normal balance. Reject duplicate sources and source/destination overlap. Fixed destination amounts use `require_full`.

When the committed command is applied, visit the sources in the supplied order. Calculate each source's available balance as its posted normal-side balance less pending decreases. Use the higher of the selected floor and any configured account floor. The source contributes the smallest of the following amounts.

1. Its available balance less that effective floor, clamped at zero.
2. Its per-payment cap, when supplied.
3. The amount still needed.

Evaluate capacity before narrowing it to a posting amount. For example, with M equal to the largest unsigned 128-bit value, a source with available balance +M and floor −M has mathematical headroom 2M. It can contribute one unit toward a one-unit payment when its resulting counters permit that debit. An intermediate signed or unsigned 128-bit subtraction would overflow on this valid request.

An exhausted source contributes zero. If the sources cover the target, validate the complete resulting entry against all account policies and command preconditions, including destination limits. Record the allocation and apply every leg atomically. A shortfall or failed check rejects the whole command, with zero postings or new reservations. [Accounts and postings](0001-accounts-and-postings.md) · [Balance constraints](0002-balance-constraints.md)

A successful reservation freezes the selected source amounts as original reservation lines. Later captures and voids address that reservation. For a partial capture across multiple sources, the caller supplies the line allocations and an explicit disposition for the remainder. The source order, floor selections, caps, destination instructions and shortfall policy are all part of the originating request's identity. Retries return the recorded allocation or rejection. [Reservations](0003-reservations.md) · [Request identity](0005-request-identity.md)

## Tradeoffs

Repeating the source list makes each payment's priority explicit and recoverable from its request. The final split becomes known at committed apply time. The v1 `require_full` policy preserves the submitted destination amounts. Other shortfall policies remain open because a partial payment to multiple destinations also needs a rule for dividing the shortfall.

## Acceptance criteria

- Start with credit-normal rewards and cash accounts, both with zero floors and no per-payment caps. Rewards has $40 posted and $10 pending for spending. Cash has $90 posted and no pending spending. An $80 payment with rewards first debits rewards $30, debits cash $50 and credits the merchant $80 in one entry.
- Submit two distinct $80 requests concurrently against those starting balances. The first in committed order gets the $30/$50 split. Only $40 of cash remains available, so the second request fails in full. Lose the winner's reply and retry it after failover. It returns the same journal entry and allocation, with no additional debit. [Raft replication](0006-raft-replication.md)
- With only $49 available in cash at the start, combined capacity is $79 and the $80 request fails. With enough source funds but a merchant ceiling that cannot accept $80, the request also fails. Both failures preserve every account's counters.
- Apply the successful example as a reservation. The $30/$50 source allocation and $80 destination leg become pending. A later change in source availability cannot change those reservation lines. Reversing the source order under the same request ID reports an ID conflict.
