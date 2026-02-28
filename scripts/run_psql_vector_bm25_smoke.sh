#!/usr/bin/env bash
set -euo pipefail

CONN="${1:-host=127.0.0.1 port=5433 user=entdb password=entdb dbname=entdb}"
RUN_ID="$(date +%s)"
TABLE_PREFIX="smoke_vb_${RUN_ID}"
EMBEDDINGS_TABLE="${TABLE_PREFIX}_embeddings"
DOCS_TABLE="${TABLE_PREFIX}_docs"
INDEX_NAME="idx_docs_bm25_${RUN_ID}"
TMP_OUT="$(mktemp)"
cleanup() {
  rm -f "$TMP_OUT"
}
trap cleanup EXIT

echo "Running vector + BM25 smoke suite..."
if ! psql "${CONN}" \
  -v ON_ERROR_STOP=1 \
  -v embeddings_table="${EMBEDDINGS_TABLE}" \
  -v docs_table="${DOCS_TABLE}" \
  -v index_name="${INDEX_NAME}" \
  -f scripts/psql_vector_bm25_smoke.sql >"$TMP_OUT" 2>&1; then
  cat "$TMP_OUT"
  echo
  echo "[smoke fail] psql execution failed"
  exit 1
fi
cat "$TMP_OUT"

assert_contains() {
  local needle="$1"
  local label="$2"
  if ! grep -Fq -- "$needle" "$TMP_OUT"; then
    echo
    echo "[smoke fail] ${label}"
    exit 1
  fi
}

assert_contains $'---- VECTOR <-> order ----\n id \n----\n  1\n  2\n  3' "vector <-> ordering"
assert_contains $'---- VECTOR <=> order ----\n id \n----\n  1\n  2\n  3' "vector <=> ordering"
assert_contains $'---- BM25 score order ----\n id \n----\n  1\n  3\n  2' "bm25 score ordering"

echo
echo "Done."
echo "Vector + BM25 smoke passed."
