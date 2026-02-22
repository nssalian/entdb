#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PROFILE="${PERF_BASELINE_PROFILE:-default}"

"$ROOT_DIR/scripts/perf_regression_gate.sh"

mkdir -p "$ROOT_DIR/perf/history"
{
  printf '{"ts":"%s","sha":"%s","profile":"%s","status":"pass"}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" \
    "$PROFILE"
} >> "$ROOT_DIR/perf/history/perf_history.jsonl"

echo "Perf history recorded."
