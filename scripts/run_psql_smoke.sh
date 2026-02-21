#!/usr/bin/env bash
set -euo pipefail

CONN="${1:-host=127.0.0.1 port=5433 user=entdb password=entdb dbname=entdb}"
RUN_ID="$(date +%s)"
TMP_EXPECTED_OUT="$(mktemp)"
cleanup() {
  rm -f "$TMP_EXPECTED_OUT"
}
trap cleanup EXIT

echo "Running supported SQL smoke suite (run_id=${RUN_ID})..."
psql "${CONN}" -v ON_ERROR_STOP=1 -v run_id="${RUN_ID}" -f scripts/psql_supported_smoke.sql

echo
echo "Running expected-reject probes (run_id=${RUN_ID})..."
psql "${CONN}" -v ON_ERROR_STOP=0 -v run_id="${RUN_ID}" -f scripts/psql_expected_failures.sql >"$TMP_EXPECTED_OUT" 2>&1 || true

echo
echo "Expected-reject summary:"

if grep -q "CASE: varchar_overflow" "$TMP_EXPECTED_OUT"; then
  if grep -q "varchar length exceeded" "$TMP_EXPECTED_OUT"; then
    echo "  [expected reject] varchar_overflow"
  else
    echo "  [unexpected pass] varchar_overflow (expected overflow rejection was not observed)"
    echo
    cat "$TMP_EXPECTED_OUT"
    exit 1
  fi
else
  echo "  [missing case marker] varchar_overflow"
  echo
  cat "$TMP_EXPECTED_OUT"
  exit 1
fi

echo
echo "Done."
echo "Supported suite should pass cleanly."
echo "Expected-reject probes should reject invalid input by design."
