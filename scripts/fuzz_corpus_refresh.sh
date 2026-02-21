#!/usr/bin/env bash
set -euo pipefail

if ! command -v cargo-fuzz >/dev/null 2>&1; then
  echo "cargo-fuzz is required. Install with: cargo install cargo-fuzz"
  exit 1
fi

pushd fuzz >/dev/null

cargo fuzz run tuple_roundtrip -- -runs=20000
cargo fuzz run wal_decode -- -runs=20000
cargo fuzz run query_parse_bind -- -runs=20000
cargo fuzz run catalog_load -- -runs=20000
cargo fuzz run polyglot_transpile -- -runs=20000

cargo fuzz cmin tuple_roundtrip corpus/tuple_roundtrip
cargo fuzz cmin wal_decode corpus/wal_decode
cargo fuzz cmin query_parse_bind corpus/query_parse_bind
cargo fuzz cmin catalog_load corpus/catalog_load
cargo fuzz cmin polyglot_transpile corpus/polyglot_transpile

popd >/dev/null

echo "Fuzz corpus refresh complete."
