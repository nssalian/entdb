#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

"$ROOT_DIR/scripts/coverage_gate.sh"

mkdir -p "$ROOT_DIR/coverage/history"
{
  printf '{"ts":"%s","sha":"%s","line_threshold":80,"region_threshold":70,"status":"pass"}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
} >> "$ROOT_DIR/coverage/history/coverage_history.jsonl"

echo "Coverage history recorded."
