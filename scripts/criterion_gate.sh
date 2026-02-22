#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
THRESHOLD_COUNT_NS="${ENTDB_CRITERION_QUERY_COUNT_MAX_NS:-25000000}"
THRESHOLD_ORDER_NS="${ENTDB_CRITERION_ORDER_LIMIT_MAX_NS:-30000000}"

cargo bench -p entdb --bench core_bench -- --noplot

COUNT_JSON="$ROOT_DIR/target/criterion/query_count_where/new/estimates.json"
ORDER_JSON="$ROOT_DIR/target/criterion/query_order_by_limit/new/estimates.json"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for criterion_gate"
  exit 1
fi
if [[ ! -f "$COUNT_JSON" || ! -f "$ORDER_JSON" ]]; then
  echo "criterion outputs missing"
  exit 1
fi

count_mean="$(jq '.mean.point_estimate' "$COUNT_JSON")"
order_mean="$(jq '.mean.point_estimate' "$ORDER_JSON")"

echo "criterion query_count_where mean(ns): $count_mean (max $THRESHOLD_COUNT_NS)"
echo "criterion query_order_by_limit mean(ns): $order_mean (max $THRESHOLD_ORDER_NS)"

if (( $(printf "%.0f\n" "$count_mean") > THRESHOLD_COUNT_NS )); then
  echo "criterion gate failed: query_count_where exceeded threshold"
  exit 1
fi
if (( $(printf "%.0f\n" "$order_mean") > THRESHOLD_ORDER_NS )); then
  echo "criterion gate failed: query_order_by_limit exceeded threshold"
  exit 1
fi

echo "criterion gate passed."
