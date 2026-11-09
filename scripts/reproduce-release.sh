#!/bin/sh
# Compare two clean release builds at the same absolute build paths. Keep both
# native dependency trees so differences can be inspected. Use a new work dir.
set -eu
if [ "$#" -ne 1 ]; then
    echo "usage: scripts/reproduce-release.sh NEW_BUILD_DIRECTORY" >&2
    exit 2
fi
mkdir "$1"
build_root=$(cd "$1" && pwd)
export CARGO_TARGET_DIR="$build_root/build"
export SOURCE_DATE_EPOCH=1794182400 ZERO_AR_DATE=1 CARGO_INCREMENTAL=0 LC_ALL=C TZ=UTC
for run in first second; do
    cargo build --release --locked -p ledger-node --bin ledger-node
    cp "$CARGO_TARGET_DIR/release/ledger-node" "$CARGO_TARGET_DIR/release/ledger-node-with-kafka"
    cargo build --release --locked -p ledger-node --bin ledger-node --no-default-features
    mv "$CARGO_TARGET_DIR/release/ledger-node" "$CARGO_TARGET_DIR/release/ledger-node-voter"
    mv "$CARGO_TARGET_DIR/release/ledger-node-with-kafka" "$CARGO_TARGET_DIR/release/ledger-node"
    mv "$CARGO_TARGET_DIR" "$build_root/$run"
done
cmp "$build_root/first/release/ledger-node" "$build_root/second/release/ledger-node"
cmp "$build_root/first/release/ledger-node-voter" "$build_root/second/release/ledger-node-voter"
echo "Both release variants are byte-identical across two clean builds."
