#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${1:-default}"

export PERF_BASELINE_PROFILE="$PROFILE"
"$ROOT_DIR/scripts/perf_regression_gate.sh"

mkdir -p "$ROOT_DIR/perf/history"
{
  printf '{"ts":"%s","profile":"%s","host":"%s","status":"pass"}\n' \
    "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
    "$PROFILE" \
    "$(uname -s)-$(uname -m)"
} >> "$ROOT_DIR/perf/history/profile_runs.jsonl"

echo "Perf profile '$PROFILE' captured."
