# Integrate a backend and monitor the ledger

## Send a first payment

For a self-contained local run with temporary certificates and three voters, use `cargo test --locked -p ledger-node --test network first_payment_example_runs_twice_with_the_same_receipt`.

Use a fresh demonstration ledger. The example creates accounts 1–3 and reserves request IDs 1–5. It creates a cash account, customer wallet and merchant wallet, funds the customer with 100 minor units, then pays the merchant 25. Give the example an account administrator certificate when administrator pins are configured. From the repository root, with the three voters running, execute

```sh
cargo run --locked -p ledger-api --example first_payment -- \
  1 tls/api-server-ca.pem tls/app-admin.pem tls/app-admin-key.pem \
  1,https://127.0.0.1:7201,api1.ledger.example \
  2,https://127.0.0.1:7202,api2.ledger.example \
  3,https://127.0.0.1:7203,api3.ledger.example
```

On an empty ledger, the funding entry is 1 and payment request 5 produces entry 2. The example sends the same payment a second time and looks up the original receipt. It exits successfully only if all three decisions agree. Run the command again with the same arguments to exercise a process-level retry. The customer and merchant balances remain 75 and 25. The protected network test runs the example workflow twice. [Example](../crates/ledger-api/examples/first_payment/workflow.rs) · [Network tests](../crates/ledger-node/tests/network/api.rs)

In an application, persist each request ID and its complete ordered payload before first send. For a new ID, a successful payment RPC carries the recorded acceptance or business rejection. Reusing an ID with different content returns `ID_CONFLICT` and preserves the original decision for `GetRequestOutcome`. A lost reply or deadline leaves the outcome unknown. Retry with the same ID and payload or call `GetRequestOutcome`. An absent decision means the request had no decision at the sampled read point. It can still commit. [Public schema](../crates/ledger-api/proto/ledger/v1/ledger.proto)

| gRPC result | Backend action |
| --- | --- |
| `RESOURCE_EXHAUSTED` | This attempt encountered a temporary admission limit. Back off and reuse the same ID and payload. After a write attempt with an unknown outcome, a later seed’s refusal still leaves that write unresolved. The routing client returns `UNAVAILABLE` and includes the last status in its diagnostic. |
| `OUT_OF_RANGE` | Fixed size limit. The public v1 limit is 1 MiB per encoded message and 1,024 items per repeated input. Reduce the request before resubmitting. |
| `INVALID_ARGUMENT` | Correct the malformed request, ledger ID or decimal representation. |
| `PERMISSION_DENIED` | The authenticated certificate lacks permission for this method. |
| `UNAVAILABLE`, timeout or transport loss | Preserve request identity. If a voter dies mid-call, the Rust transport can also return `UNKNOWN` with a transport error. Resolve the write by looking up its original ID and retrying the identical payload. The Rust client follows leader hints only to configured voter IDs. |
| `OK` with `Decision.rejected` | Handle the business rejection. The same request returns the original rejection, including after balances change. |

Older clients that treated all size errors as `RESOURCE_EXHAUSTED` should handle `OUT_OF_RANGE` before rollout. Older voters report `RESOURCE_EXHAUSTED` for oversized input, so apply input limits client-side during mixed-version operation.

## Inspect a hold

Call `Ledger/GetReservation` with the ledger and reservation IDs. An absent reservation means the ID was missing at the current quorum read point. The response carries the original lines, their original order and a remaining amount at the same zero-based position for every line. Amounts include zeroes. After reserving 70 on two lines and capturing 30 with `keep_reserved`, the remaining amounts are `["40", "40"]`. A void then yields `VOIDED` and `["0", "0"]`. Full capture or capture with release yields `CAPTURED`. A protected voter waits for fresh independent journal confirmation through the sampled view. A quorum voter returns the applied quorum view.

## Set caller permissions

Each voter accepts only clients whose TLS certificate chains to its configured backend client CA. With the following optional setting under each voter's existing `[client]` section, only pinned backend leaves may create accounts, replace account bounds or post an entry with `corrects`. Each pin is the exact DER-encoded leaf certificate and its path is relative to the node configuration. Set the same policy on every voter.

```toml
[client]
listen = "127.0.0.1:7201"
ca_pem = "tls/backend-client-ca.pem"
cert_pem = "tls/node1-api.pem"
key_pem = "tls/node1-api-key.pem"
admin_cert_der = ["tls/app-admin.der"]
```

Export a pin with `openssl x509 -in tls/app-admin.pem -outform DER -out tls/app-admin.der`. Setting `admin_cert_der = []` disables all three administrative operations. Omitting the field preserves the original dedicated-CA policy, which grants all methods to every authenticated backend. For rotation, temporarily configure both old and new pins on every voter, restart one voter at a time, then retire the old pin after migration.

For read-only operational access, configure a `[monitor]` listener on each member with its own address and an operator CA separate from the backend client CA. This port registers only `Monitor/GetStatus`. On a voter, `[client]` can serve payments and status at the same time on a different port. Issuing an operator a certificate trusted by `[client]` grants the ordinary backend role, including money movement.

Ordinary trusted backend clients can post transfers, reserve, capture and void funds across the ledger, read financial state and read voter status. Account-specific spending authorization and end-user attribution belong to the calling service. The ledger stores the command and decision under its request ID. It does not record the TLS caller as part of that financial decision.

## Read member status

Build the member-status command once, then query each member's dedicated monitor address with the operator identity. This example expects ledger 1 and voter 1. The TLS name must match the monitor server certificate.

```sh
cargo build --release --locked -p ledger-api --example member_status
target/release/examples/member_status \
  1 1 https://127.0.0.1:7244 monitor1.ledger.example \
  tls/monitor-server-ca.pem tls/operator.pem tls/operator-key.pem
```

The command contacts only the named endpoint. Its five-second network deadline covers connection, TLS and the status call. A dash means the member supplied no value. A nonzero exit indicates argument, connection, authentication, identity, RPC or reported operational errors. A successful sample can come from an isolated voter. Check the applied indexes and quorum separately.

Compare generation, node and leader IDs, term, applied index, role and checkpoint across voters. The leader lists the confirmed match index and learner flag for each remote peer. A follower's peer list is empty. Match indexes describe Raft replication. On a protected voter, `verifiedJournalIndex` is the last observed independent coverage, and `protectionError` reports the last verification error. A once-observed index can persist during an outage. Status is a local observation and can be read without a quorum, including when payments are paused.

Add this block to a voter or learner. Use its monitor server name, address, operator identity and server trust root in the status command. Choose a different address from the member’s peer and payment listeners. A diagnostic voter can expose status without a payment listener. Export cursors are present only on a learner.

```toml
[monitor]
listen = "127.0.0.1:7244"
ca_pem = "tls/operator-client-ca.pem"
cert_pem = "tls/member-monitor.pem"
key_pem = "tls/member-monitor-key.pem"
```

The learner reports its saved archive cursor, independently verified archive index, and publication cursor including the command slot. Lag is measured in Raft entries behind the learner's local applied index. A partially scanned event entry counts as one outstanding entry. Slot 65535 means the whole entry has been scanned. The archive lag is absent while current verification is unavailable. A saved archive cursor can still be present. A disabled sink has no cursor. Compare the learner's applied index with the leader's applied index for additional replication backlog. A Kafka cursor reflects acknowledged publication and scan progress. Recovery uses the verified independent archive. [Export workers](../crates/ledger-export/src/workers.rs)
