#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

if ! cargo fuzz --help >/dev/null 2>&1; then
  echo "missing dependency: cargo-fuzz"
  echo "install with: cargo install cargo-fuzz"
  exit 1
fi
if ! rustup toolchain list | grep -q '^nightly'; then
  echo "missing dependency: nightly toolchain"
  echo "install with: rustup toolchain install nightly"
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "missing dependency: jq"
  echo "install with: brew install jq"
  exit 1
fi

if [[ "${ENTDB_SKIP_WORKSPACE_TESTS:-0}" == "1" ]]; then
  echo "[1/4] Workspace tests (skipped by ENTDB_SKIP_WORKSPACE_TESTS=1)"
else
  echo "[1/4] Workspace tests (single pass; no duplicated suites)"
  cargo test --workspace -- --nocapture
fi

echo "[2/4] Perf regression gate"
"$ROOT_DIR/scripts/perf_regression_gate.sh"

echo "[3/4] Criterion perf gate"
"$ROOT_DIR/scripts/criterion_gate.sh"

echo "[4/4] Fuzz smoke"
pushd fuzz >/dev/null
cargo +nightly fuzz run tuple_roundtrip -- -runs=5000
cargo +nightly fuzz run wal_decode -- -runs=5000
cargo +nightly fuzz run polyglot_transpile -- -runs=5000
cargo +nightly fuzz run query_parse_bind -- -runs=5000
popd >/dev/null

echo "Release readiness gate passed."
