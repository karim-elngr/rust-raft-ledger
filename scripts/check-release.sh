#!/bin/sh
# Run from the workspace root with Rust, libclang, a C++ compiler and protoc 3.x.
set -eu
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo clippy --locked -p ledger-node --all-targets --no-default-features -- -D warnings
cargo test --locked --workspace --all-features
cargo test --locked -p ledger-api --example reconcile_receipts
cargo test --locked -p ledger-node --no-default-features --lib --bins
cargo build --locked --release -p ledger-node --no-default-features
tree=$(cargo tree --locked -p ledger-node --no-default-features -e normal,build,dev)
if printf '%s\n' "$tree" | grep -Eq 'rdkafka|openssl(-src|-sys)? v'; then
    echo "Kafka/OpenSSL was included in the voter-only build" >&2
    exit 1
fi
