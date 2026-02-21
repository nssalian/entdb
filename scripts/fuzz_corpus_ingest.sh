#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
FUZZ_DIR="$ROOT_DIR/fuzz"

if [[ ! -d "$FUZZ_DIR" ]]; then
  echo "fuzz directory not found"
  exit 1
fi

copy_artifacts() {
  local target="$1"
  local src_dir="$FUZZ_DIR/artifacts/$target"
  local dst_dir="$FUZZ_DIR/corpus/$target"
  mkdir -p "$dst_dir"
  if [[ -d "$src_dir" ]]; then
    find "$src_dir" -type f | while read -r f; do
      local sha
      sha=$(shasum -a 256 "$f" | awk '{print $1}')
      cp "$f" "$dst_dir/$sha"
    done
  fi
}

copy_artifacts tuple_roundtrip
copy_artifacts wal_decode
copy_artifacts query_parse_bind
copy_artifacts catalog_load
copy_artifacts polyglot_transpile

"$ROOT_DIR/scripts/fuzz_corpus_refresh.sh"

echo "Fuzz corpus ingest + refresh complete."
