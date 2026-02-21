#!/usr/bin/env bash
set -euo pipefail

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
  echo "cargo-llvm-cov is required. Install with: cargo install cargo-llvm-cov"
  exit 1
fi

BASE_SHA="${GITHUB_BASE_SHA:-}"
HEAD_SHA="${GITHUB_SHA:-HEAD}"
if [[ -z "$BASE_SHA" ]]; then
  BASE_SHA="$(git rev-parse HEAD~1 2>/dev/null || git rev-parse HEAD)"
fi

mapfile -t changed_lines < <(
  git diff -U0 "$BASE_SHA" "$HEAD_SHA" -- '*.rs' | awk '
    /^\+\+\+ b\// { file=substr($0,7); next }
    /^@@ / {
      if (file == "") next;
      if (match($0, /\+[0-9]+(,[0-9]+)?/)) {
        h = substr($0, RSTART+1, RLENGTH-1);
        split(h, parts, ",");
        start = parts[1] + 0;
        count = (parts[2] == "" ? 1 : parts[2] + 0);
        if (count == 0) next;
        for (i = 0; i < count; i++) {
          printf "%s:%d\n", file, start + i;
        }
      }
    }
  '
)

if [[ ${#changed_lines[@]} -eq 0 ]]; then
  echo "No changed Rust lines; delta gate skipped."
  exit 0
fi

cargo llvm-cov \
  --workspace \
  --all-targets \
  --lcov \
  --output-path lcov.info \
  --fail-under-lines 0 \
  --fail-under-regions 0 >/dev/null

awk '
  /^SF:/ { sf=substr($0,4) }
  /^DA:/ {
    split(substr($0,4), arr, ",")
    line=arr[1]
    hits=arr[2]
    print sf "|" line "|" hits
  }
' lcov.info > /tmp/entdb-lcov-lines.txt

fail=0
for entry in "${changed_lines[@]}"; do
  file="${entry%%:*}"
  line="${entry##*:}"
  rec=$(grep "${file}|${line}|" /tmp/entdb-lcov-lines.txt | tail -n1 || true)

  if [[ -z "$rec" ]]; then
    # try matching against absolute paths by suffix
    rec=$(grep "/${file}|${line}|" /tmp/entdb-lcov-lines.txt | tail -n1 || true)
  fi

  if [[ -z "$rec" ]]; then
    echo "Delta line uncovered: ${file}:${line} (no coverage record)"
    fail=1
    continue
  fi

  hits=$(echo "$rec" | awk -F'|' '{print $3}')
  if [[ "$hits" -eq 0 ]]; then
    echo "Delta line uncovered: ${file}:${line}"
    fail=1
  fi
done

if [[ "$fail" -ne 0 ]]; then
  echo "Delta coverage gate failed (changed lines must be covered)."
  exit 1
fi

echo "Delta coverage gate passed (all changed Rust lines covered)."
