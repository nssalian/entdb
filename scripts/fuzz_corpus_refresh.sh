#!/usr/bin/env bash
set -euo pipefail

if ! command -v cargo-fuzz >/dev/null 2>&1; then
  echo "cargo-fuzz is required. Install with: cargo install cargo-fuzz"
  exit 1
fi

if ! rustup toolchain list | grep -q '^nightly'; then
  echo "nightly toolchain is required. Install with: rustup toolchain install nightly"
  exit 1
fi

pushd fuzz >/dev/null

cargo +nightly fuzz run tuple_roundtrip -- -runs=20000
cargo +nightly fuzz run wal_decode -- -runs=20000
cargo +nightly fuzz run query_parse_bind -- -runs=20000
cargo +nightly fuzz run catalog_load -- -runs=20000
cargo +nightly fuzz run polyglot_transpile -- -runs=20000

cargo +nightly fuzz cmin tuple_roundtrip corpus/tuple_roundtrip
cargo +nightly fuzz cmin wal_decode corpus/wal_decode
cargo +nightly fuzz cmin query_parse_bind corpus/query_parse_bind
cargo +nightly fuzz cmin catalog_load corpus/catalog_load
cargo +nightly fuzz cmin polyglot_transpile corpus/polyglot_transpile

popd >/dev/null

echo "Fuzz corpus refresh complete."
