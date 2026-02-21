#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MAX_WALL_SECS="${ENTDB_SOAK_MAX_WALL_SECS:-15000}"

start_epoch="$(date +%s)"
"$ROOT_DIR/scripts/soak_gate.sh"
end_epoch="$(date +%s)"
elapsed="$((end_epoch - start_epoch))"

echo "Soak gate wall-clock seconds: ${elapsed}"
if [[ "$elapsed" -gt "$MAX_WALL_SECS" ]]; then
  echo "Soak SLO failed: elapsed=${elapsed}s exceeds max=${MAX_WALL_SECS}s"
  exit 1
fi

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    echo "## Soak SLO"
    echo ""
    echo "- elapsed_seconds: ${elapsed}"
    echo "- max_allowed_seconds: ${MAX_WALL_SECS}"
    echo "- status: pass"
  } >> "$GITHUB_STEP_SUMMARY"
fi
