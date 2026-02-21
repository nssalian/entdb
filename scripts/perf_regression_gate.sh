#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Source so exported PERF budget vars are visible to the cargo test process below.
# shellcheck source=/dev/null
. "$ROOT_DIR/scripts/perf_baseline_apply.sh" "${PERF_BASELINE_FILE:-$ROOT_DIR/perf/baselines.json}" "${PERF_BASELINE_PROFILE:-default}"

# Cold-start HBO budget is too environment-sensitive for readiness gating.
# Keep it as a manual perf test.
cargo test -p entdb perf_ -- --ignored --nocapture \
  --skip perf_optimizer_cold_start_no_regression_budget
