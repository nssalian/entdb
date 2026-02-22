#!/usr/bin/env bash
set -euo pipefail

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
  echo "cargo-llvm-cov is required. Install with: cargo install cargo-llvm-cov"
  exit 1
fi

cargo llvm-cov \
  --workspace \
  --all-targets \
  --lcov \
  --output-path lcov.info \
  --fail-under-lines 80 \
  --fail-under-regions 70

echo "Coverage gate passed (lines >= 80%, regions >= 70%)."
