# Rust Raft ledger

This workspace implements a deterministic double-entry ledger on three Raft voters. A group can add a permanent learner for asynchronous backup and Kafka publishing. Backend services use an mTLS gRPC API. Members exchange consensus messages on a separate private network. Amounts are integers in the ledger’s smallest unit.

| Crate | Responsibilities |
| --- | --- |
| [ledger-core](crates/ledger-core/src/lib.rs) | Balanced multi-leg postings, account bounds, caller-ordered funding waterfalls, reservations, explicit partial captures and idempotent decisions. |
| [ledger-replica](crates/ledger-replica/src/lib.rs) | A TiKV raft-rs voter, a versioned command-batch format, a durable RocksDB Raft log and a separate RocksDB projection of the ledger. |
| [ledger-recovery](crates/ledger-recovery/src/lib.rs) | An independent append-only journal, conditional head updates, generation fencing, and verified replay. |
| [ledger-api](crates/ledger-api/src/lib.rs) | The `ledger.v1` Protobuf schema, conversion to ledger commands and a Rust client with leader routing. Backend applications can use this crate independently of RocksDB. |
| [ledger-export](crates/ledger-export/src/lib.rs) | Bounded learner reads, independent archive and publisher workers, durable cursors and no-op adapters. |
| [ledger-export-adapters](crates/ledger-export-adapters/src/lib.rs) | POSIX journal archive and a Protobuf Kafka producer. |
| [ledger-node](crates/ledger-node/src/lib.rs) | A runnable voter or learner with private peer RPCs, an optional public mTLS listener, automatic command batching and an embedded Rust interface. |

A replica can propose several ordered commands in one entry. It syncs the Raft WAL before returning dependent peer messages. The working database publishes each committed entry’s changes and applied index together, with its WAL and per-write sync disabled. Restart clones the active local checkpoint and replays its committed Raft tail. Before the first checkpoint, replay starts at genesis. By default the voter retains the full Raft log, including any uncommitted suffix Raft may still replace. In a three-voter protected group, offline reclamation can move old raw-history reads to a pinned independent checkpoint.

Each voter has a single Raft owner and separate send workers for each peer. An individual command submitted through `NodeHandle::submit_local` can share a Raft entry with commands already queued. The owner gathers only work already waiting, ends collection early for approaching deadlines, and uses a 250 µs collection budget. The initial limits are 256 commands or 1 MiB per entry, eight proposals in flight, and an admission budget of 512 requests and 8 MiB of encoded request data. Different request IDs retain independent outcomes within the same entry.

In a three-voter group without a recovery journal the process-local interface returns diagnostic results from the local voter. A follower can lag the leader. With a recovery journal, `submit_protected` waits for Raft commitment, local publication, and independent confirmation through the batch. A retry or business rejection uses the same gate. Protected reads obtain a quorum read index, sample a consistent local view and wait for offsite coverage of that view. `protected_account` and `protected_entry` look up records. `protected_receipt` takes a request ID and returns its original decision. `protected_outcome` also takes the command payload and detects conflicting reuse of the ID. The local convenience methods on a protected node also use the protection gates.

The voter reads individual accounts, reservations, journal entries and original request decisions from RocksDB. It keeps a byte-bounded cache of decoded accounts. The embedded `protected_accounts` method reads several accounts at one protected point. The optional whole-ledger diagnostic `Replica::ledger()` reconstructs the complete history in memory on demand. Lagging voters can install a peer checkpoint and replay the committed tail. History lives in an indexed RocksDB column family. Separate immutable segments are a future option. The 1 TB drill is deferred. [Architecture RFCs](ARCHITECTURE.md)

Measured local throughput, protected p99 latency, admission backpressure, recovery time and disk reclamation are recorded in the [load and recovery report](docs/measurements/2026-11-08-local-load.md).

See the [local release qualification](docs/release-qualification.md) for the candidate, measured results and deployment decision.

The [project record and future direction](docs/project-record.md) brings together the crate boundaries, deployment guarantees, engineering assessment and proposed milestones.

## Run locally

The workspace was tested with Rust 1.99.1. Building the replica also requires a C++ compiler and libclang for embedded RocksDB. The default node builds librdkafka and vendored OpenSSL for the Kafka adapter. Build a voter or POSIX-only learner with `cargo build --release --locked -p ledger-node --no-default-features` to omit both native libraries. A Kafka publisher configuration requires the default build. Use `protoc` 3.15 or later in the 3.x series. The public schema uses proto3 optional fields, and the locked Raft generator requires 3.x. Tests use `protoc` 3.21.12. On Apple Silicon, put an executable native build on `PATH` or set `PROTOC` to its absolute path. [Protobuf 3.15 release notes](https://github.com/protocolbuffers/protobuf/releases/tag/v3.15.0) · [macOS arm64 protoc 3.21.12](https://repo.maven.apache.org/maven2/com/google/protobuf/protoc/3.21.12/protoc-3.21.12-osx-aarch_64.exe)

```sh
cargo test --locked --workspace
cargo test --locked -p ledger-api
cargo run --locked -p ledger-core --example waterfall
cargo test --locked -p ledger-node --test network
```

The API tests round-trip signed balance extremes and the maximum counter through Rust and the C++ Protobuf tools. The waterfall example spends rewards before cash and retries the payment under the same request ID. The replica tests cover replay after deleting the working database and restart with an uncommitted payment in the Raft log. The network tests use temporary certificates and local TCP ports to exercise elections, concurrent submissions, retry after leader loss, and catch-up.

## Run a group with a learner

Start with four fresh node directories. Use the TLS and peer settings in the voter example below. On **all four** configurations, set `learner_id = 4` at the top level, list the other three members under `[[peers]]`, and omit `recovery_dir`. Give the learner its own certificate and address. Only the three voters can configure a `[client]` listener.

Add the following tables to node 4. The archive path is relative to its configuration file. For independent recovery, use a volume that survives loss of the voter disks. Before `journal-init`, mount and verify that volume and provision the `recovery/` parent in the example. The command initializes the final `usd` directory. Create the Kafka topic and partition before publishing.

```toml
[export.archive]
kind = "posix"
path = "recovery/usd"

[export.publisher]
kind = "kafka"
brokers = "127.0.0.1:9092"
topic = "ledger.decisions.v1"
partition = 0
```

```sh
ledger-node journal-init node4.toml
for id in 1 2 3 4; do ledger-node init "node${id}.toml" || exit 1; done
# Run each command in its own terminal.
ledger-node run node1.toml
ledger-node run node2.toml
ledger-node run node3.toml
ledger-node run node4.toml
```

Either adapter can be disabled by setting its table to just `kind = "noop"` or omitting that table. With both disabled, the learner still follows Raft and creates no export cursors. With Kafka alone, events follow applied decisions. With both enabled, events wait for the confirmed archive prefix. `ledger-node journal-head node4.toml` reports the POSIX recovery cut. The running learner logs changes to verified archive progress, the broker cursor and delivery errors. Backups and publication can lag payments. A total loss of all replicas can lose acknowledged payments after the archived cut.

The adapter selection lives in [node configuration](crates/ledger-node/src/config/exports.rs). Implement the [export ports](crates/ledger-export/src/lib.rs) to supply another backend. Consumers can generate the [decision event schema](crates/ledger-export-adapters/proto/ledger/export/v1/events.proto) with the existing ledger API proto on their include path. [Learner topology and migration](docs/rfcs/0018-static-raft-learner.md) · [Archive guarantees](docs/rfcs/0019-async-recovery-archive.md) · [Kafka delivery](docs/rfcs/0020-ledger-decision-publishing.md)

The learner integration test starts real node processes, verifies a quorum-acknowledged transfer with the learner offline, consumes its Kafka record and restores both posting lines from POSIX. By default it uses librdkafka’s local test broker. To use a local Apache Kafka instance, supply its bootstrap address. The test creates a fresh single-partition topic.

```sh
LEDGER_TEST_KAFKA_BROKERS=127.0.0.1:9092 cargo test --locked -p ledger-node --test network learner_cli_archives
```

## Configure a three-voter protected group

Give each of the three voters a separate data directory, TLS identity, and configuration file. For example, the first voter's `node1.toml` can contain the following values.

```toml
cluster_id = "77"
ledger_id = "1"
node_id = 1
asset = "USD"
scale = 2
listen = "127.0.0.1:7101"
data_dir = "data/node1"
recovery_dir = "recovery/usd"

[checkpoint]
interval_entries = 65536
retained_entries = 8192

[cache]
account_bytes = 4194304
block_bytes = 12582912

[client]
listen = "127.0.0.1:7201"
ca_pem = "tls/backend-client-ca.pem"
cert_pem = "tls/node1-api.pem"
key_pem = "tls/node1-api-key.pem"

[tls]
ca_pem = "tls/ca.pem"
cert_pem = "tls/node1.pem"
key_pem = "tls/node1-key.pem"

[[peers]]
node_id = 2
endpoint = "https://127.0.0.1:7102"
domain = "node2.ledger.example"
cert_der = "tls/node2.der"

[[peers]]
node_id = 3
endpoint = "https://127.0.0.1:7103"
domain = "node3.ledger.example"
cert_der = "tls/node3.der"
```

Paths are relative to the configuration file. Provision a CA and a separate certificate and key for each voter. Each certificate needs its voter’s unique DNS name and permission for TLS server and client authentication. Outbound peers verify the configured DNS name under the CA. On inbound RPCs each voter verifies the sender's exact DER leaf certificate. Configure the other voters with the same cluster, ledger, asset, scale and membership, swapping the local node and its two peers.

The optional `[cache]` block sets the per-voter capacities for decoded accounts and the shared RocksDB state block cache. The example shows their defaults. Set `account_bytes = 0` to read each account from RocksDB. These values can change at restart. Other memory use includes RocksDB write buffers, the Raft store and the current batch of commands. The embedded `NodeHandle::status()` reports allocated account-cache bytes, occupancy, hits, misses, evictions and RocksDB account lookups.

The optional `[client]` block enables a separate backend listener on a protected voter or a voter in the four-member topology. A protected voter requires the top-level `recovery_dir`. The example uses public ports 7201–7203 and peer ports 7101–7103. Give each public listener a certificate covering its configured server name, such as `api1.ledger.example`. Its `ca_pem` identifies the CA trusted to issue *backend client* certificates. By default every authenticated backend can read and mutate the configured ledger. Set `admin_cert_der` to restrict account creation, bounds changes and corrections to specific backend certificates. [Backend permissions and monitoring](docs/backend-integration.md)

For the protected configuration above, initialize the shared journal once for the entire cluster. Then initialize each voter with its own configuration and run one process per voter. The following commands use node 1.

```sh
mkdir -p recovery
cargo run --locked -p ledger-node -- journal-init node1.toml
cargo run --locked -p ledger-node -- init node1.toml
cargo run --locked -p ledger-node -- run node1.toml
```

After stopping a process, use `run` with its existing data directory. A lost Raft store requires voter recovery. The private peer port carries consensus messages. The local Rust interface is available to applications embedding the node.

## Local voter checkpoints

Each voter automatically seals a RocksDB checkpoint. The default interval is 65,536 applied Raft entries, with initial captures staggered across voters. Configure it in `[checkpoint]`, or set `interval_entries = 0` to use manual captures. A running embedded voter accepts `NodeHandle::checkpoint()`. Its status exposes the latest sealed index, the base used at startup, and maintenance errors.

For a stopped voter, `ledger-node checkpoint node1.toml` seals its current committed state. `checkpoint-scrub` hashes every file in the active image. If the active image is missing or damaged, `rebuild` validates the retained Raft log, rebuilds the working state from genesis, and then clears the broken checkpoint reference. These commands use the local voter directory. After physical reclamation, a full rebuild also reads the pinned independent image. [Checkpoint design](docs/rfcs/0012-incremental-snapshots.md)

Within the `retained_entries` window, a follower catches up through ordinary Raft appends. For older gaps the leader can offer its sealed checkpoint. A returning follower can rebuild the offered state from its own checkpoint and a verified interval of original Raft entries when compaction has changed most files. Cold followers, large gaps and transfers with substantial reusable file data use resumable file downloads. Both paths retain the missing original entries for journal verification and repair. After Raft installs the snapshot it applies the committed tail. The default window is 8,192 entries before the sealed cut. This setting changes replication behavior while preserving the full local command history. Embedded node status reports the installed snapshot index, replay base when used, file and history bytes, proof bytes and reverse-RPC message bytes. [Peer protocol](docs/rfcs/0008-peer-transport.md)

After a physical file transfer, it releases its staging file links and records a small receipt so an exact retry can reopen the seal. Startup, new offers and hourly maintenance check for transfers idle for 24 hours. Cleanup preserves active transfers and unrecognized files. Embedded node status exposes the most recent transfer maintenance error. [Transfer lifecycle and measurements](docs/rfcs/0012-incremental-snapshots.md#peer-installation-and-retention)

## Call the backend API

Start the three voters and run the first-payment example on a fresh demonstration ledger. It creates three accounts, funds the customer, pays the merchant 25 minor units and checks the original receipt against a same-ID retry and a current lookup. [Runnable command, permissions, reservation reads and member health](docs/backend-integration.md)

All six mutation methods return a `Decision` on gRPC success. Match `decision.result` on `v1::decision::Result::Accepted(outcome)` or `Rejected(rejection)`. A rejection has a typed `ErrorCode` and any relevant context. Reusing a request ID with changed fields yields `ID_CONFLICT` and leaves the original decision available. An empty outcome lookup means the request was absent at the quorum-confirmed read point. After an uncertain reply, retry the original request with the same ID and fields. Backends in other languages can generate stubs from the same schema and implement the same seed-routing and retry rules. [Public schema](crates/ledger-api/proto/ledger/v1/ledger.proto) · [Status codes and routing](docs/rfcs/0009-client-protocol.md)

## Synchronous protection in a three-voter group

For this deployment, point `recovery_dir` on every voter at the same POSIX directory outside the voter data trees. The example assumes all three configuration files share a parent directory. The synchronous journal retains the committed history through every protected acknowledgment. Local development uses this journal as its recovery copy. Daily archives are outside the current scope. S3 or a similar object store is a possible future journal backend. [Recovery decision](docs/rfcs/0013-daily-archive-backups.md)

To recover after all voter disks are lost, the journal must survive that loss. A directory on the same physical disk shares the voters’ failure risk. A multi-host POSIX deployment needs shared advisory file locks, atomic same-directory rename, immutable object creation, durable file and directory sync, and protection against deletion or rollback of the head. Voter shutdown waits for outstanding journal operations to finish.

Use `journal-head` to inspect the generation, index, term, digest, and fence flag. Run `ledger-node journal-scrub CONFIG.toml` to verify the entire published journal through the reported head, including older segments. The scrub works with an active or fenced journal and only needs access to the recovery directory. A timeout on a payment leaves its outcome unknown. Retrying the original payload and request ID resolves it once protection is available.

To shorten a future disaster restore, run `ledger-node journal-checkpoint CONFIG.toml` while the journal is active. It needs only journal access, including space for two RocksDB images and private staging. A successful command publishes a complete verified base on the recovery volume. Later refreshes apply only newer commands. Restore output reports the base index and actual replayed Raft entries. [Independent checkpoint design](docs/rfcs/0016-journal-checkpoints.md)

## Reclaim voter disk space in protected mode

1. With the independent journal available, run `ledger-node journal-checkpoint CONFIG.toml` to publish a base.
2. Stop one voter and run `ledger-node checkpoint CONFIG.toml` if its last sealed local checkpoint predates the base. The voter must have applied through the source index.
3. Run `ledger-node reclaim CONFIG.toml`. It verifies both copies, keeps the boundary row and physically compacts older records. Its output includes live Raft SST bytes before and after.
4. Restart the voter and wait for it to catch up before taking another voter offline. Keep the independent recovery directory mounted at its recorded path for full repair and older raw-history requests. The persistent reference protects the exact base even while the voter is stopped. [Reclamation and crash recovery](docs/rfcs/0017-raft-log-reclamation.md)

## Recover a protected group after total voter loss

1. Recovery commands use the configuration and journal access without reading the old peer TLS keys. Use the old cluster configuration to run `ledger-node fence old-node1.toml`. It conditionally closes the current head to further publication. If a publisher won the race, inspect the new head and repeat. If a reply was lost, use `journal-head` to check whether the fence succeeded.
2. Configure a fresh three-voter group with a nonzero cluster ID that has never served this ledger, the same ledger ID, asset and scale, fresh voter identities and data directories, and the same recovery directory. Run `ledger-node restore NEW-CONFIG.toml` for **each** replacement voter while the old head is fenced. Each restore uses the published recovery checkpoint if present, validates its files, and replays the authenticated journal tail before publishing the new voter directory. With a journal alone, it verifies and imports the entire history.
   If the published recovery checkpoint is damaged and the complete journal is intact, run `ledger-node restore-from-journal NEW-CONFIG.toml` on an empty destination. This explicit path verifies and replays the entire journal.
3. Retire the old endpoints. Run `ledger-node activate NEW-CONFIG.toml` once using a restored voter. It checks the local committed history against the fence before conditionally authorizing the replacement cluster. Start the three restored voters using `run` and route clients to the replacement endpoints.

Each replacement voter seals the state at the fenced head as a local checkpoint. On a surviving disk, a voter can reopen its checkpoint and replay only the committed tail. The Raft store also saves the last journal head verified at each verification interval. A restarting leader checks the independent head and compares the suffix back to that saved digest. A replacement that imported an independent checkpoint can verify from its durable fence proof until it saves a live head. A voter with neither record checks from genesis. [Recovery costs by operation](docs/rfcs/0015-offsite-commit-protection.md#how-much-history-is-read-today)
