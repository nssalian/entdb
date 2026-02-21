#!/usr/bin/env bash
set -euo pipefail

export ENTDB_STRESS_SOAK_SECS="${ENTDB_STRESS_SOAK_SECS:-7200}"
export ENTDB_MVCC_STRESS_SECS="${ENTDB_MVCC_STRESS_SECS:-7200}"
export ENTDB_WAL_STRESS_TXNS="${ENTDB_WAL_STRESS_TXNS:-20000}"

cargo test -p entdb --test stress_storage soak_table_randomized_mixed_operations -- --ignored --nocapture
cargo test -p entdb --test stress_wal stress_wal_many_transactions_recover -- --ignored --nocapture
cargo test -p entdb --test mvcc_stress_query mvcc_query_stress_abort_storm_with_predicates_and_join_reads -- --ignored --nocapture
