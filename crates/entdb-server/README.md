# entdb-server

`entdb-server` exposes EntDB over the PostgreSQL wire protocol.

It provides:

- pgwire-compatible SQL endpoint (`psql` and Postgres drivers)
- auth + TLS configuration
- runtime guardrails and server metrics hooks

## Install

```bash
cargo install entdb-server --locked
```

## Run

```bash
entdb --host 127.0.0.1 --port 5433 --data-path ./entdb_data
```

Then connect with:

```bash
psql "host=127.0.0.1 port=5433 user=entdb password=entdb dbname=entdb"
```

Durability tuning:

```bash
entdb --host 127.0.0.1 --port 5433 --data-path ./entdb_data --durability-mode normal
entdb --host 127.0.0.1 --port 5433 --data-path ./entdb_data --durability-mode normal --await-durable
```

- `--durability-mode full`: fsync on commit path
- `--durability-mode normal`: relaxed sync policy for higher throughput
- `--durability-mode off`: no sync on commit path
- `--await-durable`: force commit calls to wait for durable flush

## Benchmark Snapshot

Core engine benchmarking commands:

```bash
cargo bench -p entdb --bench workload_bench -- --noplot
cargo bench -p entdb --bench comparative_bench -- --noplot
```

These are embedded core-engine numbers, not pgwire end-to-end server latency:

Write loops:

| Workload | EntDB Full | EntDB Normal | SQLite Full | SQLite Normal |
|---|---:|---:|---:|---:|
| INSERT 100 (no txn) | 462.20 ms | 11.201 ms | 7.0020 ms | 2.1716 ms |
| UPDATE 100 | 481.27 ms | 16.221 ms | 7.1612 ms | 2.1869 ms |
| DELETE 100 | 483.43 ms | 13.683 ms | 7.2658 ms | 2.1430 ms |
| TRANSACTION batch | 15.268 ms | 12.830 ms | 390.08 us | 403.73 us |

Prepared read paths:

| Workload | EntDB | EntDB Prepared | SQLite | SQLite Prepared |
|---|---:|---:|---:|---:|
| SELECT ALL 100 | 45.359 us | 38.762 us | 12.181 us | 10.886 us |
| SELECT BY ID x100 | 1.0403 ms | 196.61 us | 254.11 us | 122.53 us |

Comparative:

| Comparative | EntDB API | EntDB Prepared | SQLite API | SQLite Prepared |
|---|---:|---:|---:|---:|
| COUNT WHERE | 9.8194 ms | 8.5285 ms | 484.76 us | 485.46 us |
| ORDER BY LIMIT | 9.3891 ms | 9.3134 ms | 679.79 us | 658.22 us |

## Links

- Repository: <https://github.com/nssalian/entdb>
- Core engine crate: <https://crates.io/crates/entdb>
