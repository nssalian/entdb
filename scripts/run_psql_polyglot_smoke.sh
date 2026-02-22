#!/usr/bin/env bash
set -euo pipefail

CONN="${1:-host=127.0.0.1 port=5433 user=entdb password=entdb dbname=entdb}"
RUN_ID="$(date +%s)"
TABLE_NAME="poly_users_${RUN_ID}"
TMP_OUT="$(mktemp)"
cleanup() {
  rm -f "$TMP_OUT"
}
trap cleanup EXIT

echo "Running polyglot dialect smoke suite..."
if ! psql "${CONN}" -v ON_ERROR_STOP=1 -v table_name="${TABLE_NAME}" -f scripts/psql_polyglot_smoke.sql >"$TMP_OUT" 2>&1; then
  if grep -q "Expected: an expression, found: \`" "$TMP_OUT"; then
    echo "Polyglot dialect smoke could not run: server-side polyglot transpilation is not enabled."
    echo "Restart server with: ENTDB_POLYGLOT=1 cargo run -p entdb-server -- --host 127.0.0.1 --port 5433 --data-path ./entdb.data --auth-user entdb --auth-password entdb"
    echo
    cat "$TMP_OUT"
    exit 2
  fi
  cat "$TMP_OUT"
  exit 1
fi
cat "$TMP_OUT"

echo
echo "Done."
echo "Polyglot dialect smoke passed."
