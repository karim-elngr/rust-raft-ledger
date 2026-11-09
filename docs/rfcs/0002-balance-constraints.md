# RFC 0002 — Balance constraints

**Status** — Implemented. The ledger enforces inclusive balance bounds and ordered request conditions. An account policy can be replaced atomically while holds are outstanding. [Account rules](../../crates/ledger-core/src/account.rs) · [Posting validation](../../crates/ledger-core/src/posting.rs)

## Problem

Two withdrawals can each pass a client-side funds check and together overdraw the same wallet. A pending debit also commits funds that a later payment might try to spend. The goal is to enforce account limits and transaction-specific conditions at the same committed position as the posting or reservation.

## Proposed solution

Each replica evaluates the built-in rules while applying a committed command. Store inclusive `min_balance` and `max_balance` as optional account policy, in integer units on the account's normal side. Define the inputs to those checks from the four account counters. [Accounts and postings](0001-accounts-and-postings.md)

| Normal side | Posted balance | Pending decreases | Pending increases |
| --- | --- | --- | --- |
| Credit | Posted credits − posted debits | Pending debits | Pending credits |
| Debit | Posted debits − posted credits | Pending credits | Pending debits |

- **Floor.** Require `posted balance − pending decreases ≥ min_balance` when a floor exists. The left side is the available balance. With a floor of −5,000 and an available balance of zero, capacity to spend down to the floor is 5,000.
- **Ceiling.** Require `posted balance + pending increases ≤ max_balance` when a ceiling exists. Pending decreases cannot make room for an incoming reservation.
- **Per-command minimum.** `available_at_least(account, amount)` requires a participating account's available balance immediately **before** an immediate posting or reservation to meet the threshold. For example, a 500-cent fee may require at least 10,000 cents available before the charge. The account's floor still applies to the resulting balance. [TigerBeetle balance-conditional transfers](https://docs.tigerbeetle.com/coding/recipes/balance-conditional-transfers/)

A zero floor on a credit-normal account gives TigerBeetle's `debits_must_not_exceed_credits` behavior. On a debit-normal account it gives `credits_must_not_exceed_debits`. Native policy extends these checks to arbitrary bounds. TigerBeetle's bounds recipe uses control and operator accounts to express the upper limit. [TigerBeetle balance flags](https://docs.tigerbeetle.com/reference/account/#flagsdebits_must_not_exceed_credits) · [Balance bounds recipe](https://docs.tigerbeetle.com/coding/recipes/balance-bounds/)

For an immediate entry, aggregate all lines per account by side, then check bounds once against the resulting counters. For a reservation, reserve the full amount on each side. For each affected account and side, the sum of posted and pending amounts must fit the posted counter, including after an immediate posting. This preserves capacity to capture every outstanding hold. Use exact intermediate arithmetic for signed differences and sums. Capture consumes admitted capacity and void releases it. Neither repeats the one-off admission condition. [Reservations](0003-reservations.md)

For an otherwise valid, representable command, evaluate conditions in submitted order, then check affected accounts in ascending canonical account-ID order, checking the floor before the ceiling for each. Return the first failed rule and its account, independent of hash-table iteration. Reject the whole command. Record the rejection so a matching retry returns it even after later funding. Include the ordered conditions in the request identity. [Request identity](0005-request-identity.md)

`CreateAccount` checks the initial zero state. `SetAccountBounds(request_id, account_id, replacement_bounds)` checks that the supplied minimum is at most the maximum when both exist and that the current posted balance and all holds satisfy the replacement. A rejected change preserves the previous policy. Record each accepted policy and its committed position so a reviewer can recover the rule in effect for any entry. The public RPC requires a bounds message. An empty message clears both limits. [Public schema](../../crates/ledger-api/proto/ledger/v1/ledger.proto)

## Tradeoffs

Gross reservation checks preserve the capacity for either side of a hold to settle. They can reject a hold whose immediate posting would pass. Keeping conditions and policy updates inside the ledger avoids artificial accounting activity and requires every replica to implement identical checks and failure ordering.

## Acceptance criteria

| Scenario (USD cents) | Expected result |
| --- | --- |
| Two distinct withdrawals of 8,000 compete for a credit-normal wallet with 10,000 and a zero floor. | The first in committed order succeeds. The second returns `ERROR_CODE_BELOW_MINIMUM`. The wallet ends at 2,000. |
| A wallet with balance zero has a floor of −5,000. | A withdrawal of 5,000 succeeds at the boundary. A further cent fails. |
| A wallet with 10,000 posted and a zero floor has 7,000 in pending debits. | Available funds are 3,000. A debit of 4,000 fails even with a separate pending credit. |
| A balanced entry debits a wallet by 6,000 and credits it by 2,000. The wallet has 5,000 and a zero floor. | An immediate posting leaves 1,000. Reserving the same entry from the same starting state fails the gross debit check. |
| A 500-cent fee requires 10,000 available. | With otherwise valid lines and a zero floor, 10,000 available passes and 9,999 fails. |
| A policy update would violate a ceiling after counting a pending increase. | Reject the update. An update exactly at the projected ceiling succeeds and the hold can still be captured. Exercise both debit-normal and credit-normal accounts. |
| A command violates multiple bounds or conditions. | Replicas return the same first rule and account on replay. No posting or reservation is created. |
