#!/usr/bin/env bash
set -euo pipefail

BASELINE_FILE="${1:-perf/baselines.json}"
PROFILE="${2:-default}"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to apply perf baseline budgets"
  exit 1
fi

if [[ ! -f "$BASELINE_FILE" ]]; then
  echo "baseline file not found: $BASELINE_FILE"
  exit 1
fi

had_values=0
while IFS= read -r line; do
  had_values=1
  export "$line"
  echo "export $line"
done < <(jq -r ".\"$PROFILE\" | to_entries[] | \"\(.key)=\(.value)\"" "$BASELINE_FILE")

if [[ $had_values -eq 0 ]]; then
  echo "no baseline values found for profile '$PROFILE'"
  exit 1
fi

echo "Perf baseline profile '$PROFILE' applied."
