# RFC 0001 — Accounts and postings

**Status** — Implemented in the Rust ledger core and public v1 API. One ledger has one asset and scale. Each accepted posting creates an immutable, balanced entry with the original line order. [Ledger core](../../crates/ledger-core/src/evaluation.rs) · [Public schema](../../crates/ledger-api/proto/ledger/v1/ledger.proto)

## Problem

A $100 wallet payment can settle $97 to a merchant and earn the platform a $3 fee. Recording those legs separately could leave the buyer charged while the merchant is still unpaid. The goal is to record the entire business transaction in one balanced, immutable journal entry.

## Proposed solution

Give each account an ID, ledger ID, category code and explicit debit-normal or credit-normal side. Give each ledger one asset and scale. The engine maintains four checked unsigned 128-bit counters. `debits_posted` and `credits_posted` accumulate gross postings. `debits_pending` and `credits_pending` track outstanding reservations. A debit-normal account's posted balance is posted debits minus posted credits. A credit-normal account uses the opposite subtraction. Bounds are optional for either side. [TigerBeetle account reference](https://docs.tigerbeetle.com/reference/account/)

| Command | v1 contract |
| --- | --- |
| `CreateAccount` | Supply a request ID, account ID, ledger ID, code, normal side and optional bounds. All four counters start at zero, which must satisfy the initial bounds. Fund the account with a balanced entry. |
| `PostEntry` | Supply a request ID, funding instructions and optional typed conditions. The explicit form supplies at least two ordered lines. The waterfall form supplies ordered sources and destination lines. Each resulting posting names an account, a side and a positive quantity in the ledger's smallest unit. Return a journal entry ID and the resolved allocation on success, or a stable business rejection. |
| `GetAccount` | Read all four counters and the derived posted and available balances from one consistent view. |

Validate account existence, common ledger, equal debit and credit totals, counter capacity and applicable rules. Sum debits and credits separately for each account, even when it appears on both sides. Publish the journal entry and all counter changes atomically. Correct a mistake with a new balanced entry referencing the original. Corrections pass the same checks as other postings. The balance rules and retry contract have separate RFCs. [Balance constraints](0002-balance-constraints.md) · [Request identity](0005-request-identity.md)

The 128-bit ceiling applies to individual line quantities and stored account counters. Compare the two whole-entry totals using exact arithmetic wide enough for the admitted number of lines. With M equal to the maximum counter value, debits of M and 1 on two accounts can balance credits of M and 1 on two other accounts. This entry is valid when each resulting account counter and policy permits it.

In the USD settlement, debit the buyer's wallet liability by 10,000 cents, credit the merchant payable by 9,700 and credit platform fee revenue by 300. Fee revenue is a real account because the platform earns the fee. These three lines are the entire settlement.

## Tradeoffs

The single-ledger restriction lets the engine compare quantities in one unit, but excludes a currency exchange from one entry. Four counters preserve gross activity and distinguish outstanding holds from settled amounts at the cost of more stored state. Immutable corrections retain the audit trail and add an entry for every correction.

## Acceptance criteria

- Post the three-line settlement using exactly its three accounts. A reader sees either all three updated balances and the entry, or the state before the settlement.
- Reject a one-cent imbalance, an unknown account, a cross-ledger line, a nonpositive quantity or counter overflow. Each rejection leaves journal entries and counters unchanged.
- Post a balanced entry that uses the same account on both sides. Both posted counters increase by their respective gross amounts. Replaying the journal reproduces posted totals, including after a correcting entry.
