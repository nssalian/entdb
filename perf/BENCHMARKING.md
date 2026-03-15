# Benchmarking Guide

This project uses Criterion for microbenchmarks in `crates/entdb/benches`.

## Goals

- Compare equivalent SQL query shapes across engines.
- Separate API-level latency from prepared-statement lower bounds.
- Avoid over-interpreting one single query as an overall engine score.
- Keep durability policy explicit in write-path comparisons.
- Keep prepared-vs-prepared comparisons separate from plain API comparisons.

## Comparative Benchmark Scenarios

`cargo bench -p entdb --bench comparative_bench -- --noplot`

The benchmark includes:

- `api_entdb_count_where`: EntDB end-to-end `execute` path.
- `api_entdb_prepared_count_where`: EntDB prepared/parameterized path.
- `api_entdb_normal_count_where`: EntDB `DurabilityMode::Normal`.
- `api_sqlite_count_where`: SQLite end-to-end `query_row` path.
- `prepare_plus_query_sqlite_param_count_where`: SQLite prepare and execute in-loop with parameter binding.
- `prepared_sqlite_param_count_where`: SQLite prepared execute-only with parameter binding.
- `api_entdb_order_by_limit`: EntDB end-to-end for sort + limit.
- `api_entdb_prepared_order_by_limit`: EntDB prepared path for sort + limit.
- `api_entdb_normal_order_by_limit`: EntDB `DurabilityMode::Normal`.
- `api_sqlite_order_by_limit`: SQLite end-to-end for sort + limit with row materialization.
- `prepared_sqlite_order_by_limit`: SQLite execute-only with statement prepared once.

Interpretation:

- Compare `api_entdb_*` vs `api_sqlite_*` for high-level API parity.
- Compare `api_entdb_prepared_*` vs `prepared_sqlite_param_*` for prepared parity on parameterized queries.
- Compare `api_entdb_prepared_order_by_limit` vs `prepared_sqlite_order_by_limit` for prepared parity on non-parameterized queries.
- Treat `prepared_sqlite_*` as a lower bound on SQLite executor overhead for that query shape.
- If EntDB improves primarily via parser/planner optimization, prepared and unprepared EntDB deltas should diverge.

## Optional Baseline/Regression Gates

- `scripts/perf_regression_gate.sh`
- `scripts/perf_history_record.sh`

These are budget-based regressions checks, not replacements for Criterion output distributions.

## 100-Row Workload

`cargo bench -p entdb --bench workload_bench -- --noplot`

This benchmark mirrors the six-operation workload discussed in external "SQLite vs Rust rewrite" writeups:

- `INSERT 100 (no txn)`
- `SELECT ALL 100`
- `SELECT BY ID x100`
- `UPDATE 100`
- `DELETE 100`
- `TRANSACTION batch`

EntDB is benchmarked in two durability policies:

- `entdb`: `DurabilityMode::Full` (sync on each commit)
- `entdb_normal`: `DurabilityMode::Normal` (reduced per-commit sync pressure)

SQLite write workloads are benchmarked in:

- `sqlite`: `journal_mode=WAL`, `synchronous=FULL`
- `sqlite_normal`: `journal_mode=WAL`, `synchronous=NORMAL`

Notes:

- EntDB currently models the lookup path using `CREATE INDEX idx_test_id ON test(id)` because `INTEGER PRIMARY KEY` rowid alias semantics are SQLite-specific.
- Mutating workloads create a fresh temp database per sample to keep operation counts stable.
- Read-only workloads set up the database once outside the timed loop; setup time is not part of the measured result.
- Prepared read groups have explicit SQLite prepared counterparts.

Additional workload groups:

- `entdb_prepared`
- `entdb_normal_prepared`
- `sqlite_prepared`
- `workload_insert_100_bulk_api/*`
- `workload_update_100_bulk_api/*`
- `workload_delete_100_bulk_api/*`

Interpretation:

- Use `entdb_normal` vs `sqlite_normal` for practical write-throughput discussion.
- Use `entdb_prepared` vs `sqlite_prepared` for point-lookup/read-path discussion.
- Use bulk API groups to measure the best embedded write path without parser/binder/planner churn.

## Durability/Checkpoint Tuning

- Runtime mode: `DurabilityMode::{Full, Normal, Off}`.
- Per-call override: `ExecuteOptions { await_durable: Some(true) }`.
- Checkpoint cadence: `EntDb::set_checkpoint_interval(...)`.
- WAL buffer bytes via env: `ENTDB_WAL_BUFFER_BYTES`.
