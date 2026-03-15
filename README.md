# 🗄️ EntDB

[![CI](https://github.com/nssalian/entdb/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/nssalian/entdb/actions/workflows/ci.yml)
[![entdb crate](https://img.shields.io/crates/v/entdb.svg?label=entdb%20crate)](https://crates.io/crates/entdb)
[![entdb docs](https://img.shields.io/docsrs/entdb?label=entdb%20docs)](https://docs.rs/entdb)
[![entdb-server crate](https://img.shields.io/crates/v/entdb-server.svg?label=entdb-server%20crate)](https://crates.io/crates/entdb-server)
[![entdb-server docs](https://img.shields.io/docsrs/entdb-server?label=entdb-server%20docs)](https://docs.rs/entdb-server)

<p align="center">
  <img src="docs-site/src/assets/hero.png" alt="EntDB hero" width="360" />
</p>


EntDB is a Rust-based SQL database engine with a PostgreSQL wire-compatible server.

## What it provides

- Page-based storage engine with buffer pool and B+Tree index internals.
- WAL + restart recovery.
- MVCC transactions (`BEGIN`/`COMMIT`/`ROLLBACK`).
- Configurable commit durability: `Full`, `Normal`, `Off`.
- Prepared execution fast paths for common point reads and simple DML.
- Batched embedded APIs for `insert_many`, `update_many`, and `delete_many`.
- SQL subset for OLTP-style workloads.
- pgwire server for `psql` and PostgreSQL drivers.

## Polyglot SQL ingress (optional)

When enabled, EntDB rewrites selected non-PostgreSQL SQL forms before parsing.
It uses the external [`polyglot-sql`](https://crates.io/crates/polyglot-sql) crate.
This is available in both execution paths:

- server path: set `ENTDB_POLYGLOT=1`
- embedded path: `engine.set_polyglot_enabled(true)` (or `ConnectOptions`)

Current rewrites:

- MySQL-style backticks: `` `users` `` -> `"users"`
- MySQL numeric `LIMIT offset, count` -> `LIMIT count OFFSET offset`

## Vector + BM25 SQL support (server and embedded)

These features are available in both execution paths:

- server path (`entdb-server` via pgwire / `psql`)
- embedded path (`EntDb::connect` / `db.execute(...)`)

### Vector surface

- `VECTOR(n)` column type
- vector text literals such as `'[0.1,0.2,0.3]'`
- operators:
- `<->` L2 distance
- `<=>` cosine distance (distance form)

### BM25 text-search surface

- `CREATE INDEX ... USING bm25 (...) [WITH (text_config='english')]`
- `to_bm25query(query_text, index_name)`
- `<@` score operator, e.g. `content <@ to_bm25query(...)`
- BM25 sidecar index files are persisted and maintained on INSERT/UPDATE/DELETE
- BM25 sidecar files are schema-versioned with legacy unversioned compatibility

Current status:

- SQL surface above is supported in both modes.
- BM25 planner can route matching `<@ to_bm25query(...)` shapes to an index-backed scan path.

## Run Server

From crates.io (when `entdb-server` is published):

```bash
cargo install entdb-server --locked
entdb --host 127.0.0.1 --port 5433 --data-path ./entdb.data --auth-user entdb --auth-password entdb
```

From source (this repo):

```bash
cargo run -p entdb-server -- \
  --host 127.0.0.1 \
  --port 5433 \
  --data-path ./entdb.data \
  --auth-user entdb \
  --auth-password entdb
```

Optional polyglot ingress rewrites:

```bash
ENTDB_POLYGLOT=1 entdb --host 127.0.0.1 --port 5433 --data-path ./entdb.data --auth-user entdb --auth-password entdb
```

or from source:

```bash
ENTDB_POLYGLOT=1 cargo run -p entdb-server -- \
  --host 127.0.0.1 \
  --port 5433 \
  --data-path ./entdb.data \
  --auth-user entdb \
  --auth-password entdb
```

Connect:

```bash
psql "host=127.0.0.1 port=5433 user=entdb password=entdb dbname=entdb"
```

## Embedded Rust API

Add to `Cargo.toml`:

```toml
[dependencies]
entdb = "0.2.0"
```

```rust
use entdb::EntDb;

fn main() -> entdb::Result<()> {
    let db = EntDb::connect("./entdb_data")?;
    db.execute("CREATE TABLE users (id INT, name TEXT)")?;
    db.execute("INSERT INTO users VALUES (1, 'alice')")?;
    let rows = db.execute("SELECT * FROM users")?;
    println!("{rows:?}");
    db.close()?;
    Ok(())
}
```

## Core Execution Model

- `CREATE INDEX ... USING btree` builds a real B-tree over existing rows.
- Single-column equality filters can route through index lookup instead of a table scan.
- DML maintains those B-tree indexes on `INSERT`, `UPDATE`, `DELETE`, `UPSERT`, and `INSERT ... SELECT`.
- Durability is policy-driven:
  - `DurabilityMode::Full`: strict sync on commit
  - `DurabilityMode::Normal`: reduced sync pressure with crash-safe recovery semantics
  - `DurabilityMode::Off`: best-effort durability for ephemeral or scratch workloads
- Embedded callers can override per-call durability with:
  - `ExecuteOptions { await_durable: Some(true) }`
- Prepared statements now have a narrow fast path for:
  - `SELECT ... FROM t`
  - `SELECT ... FROM t WHERE col = $1`
  - `SELECT COUNT(*) ... WHERE col OP $1`
  - simple keyed `INSERT`, `UPDATE`, and `DELETE`
- Bulk APIs avoid parser/binder/planner churn on repeated writes:
  - `insert_many`
  - `update_many`
  - `delete_many`

## Smoke tests

```bash
./scripts/run_psql_smoke.sh "host=127.0.0.1 port=5433 user=entdb password=entdb dbname=entdb"
./scripts/run_psql_polyglot_smoke.sh "host=127.0.0.1 port=5433 user=entdb password=entdb dbname=entdb"
./scripts/run_psql_vector_bm25_smoke.sh "host=127.0.0.1 port=5433 user=entdb password=entdb dbname=entdb"
```

## Benchmark Snapshot

Environment:

- Criterion benchmark medians from local `cargo bench` runs.
- SQLite write-path runs are shown in both `synchronous=FULL` and `synchronous=NORMAL` where that comparison is meaningful.
- Prepared read rows are only compared against prepared SQLite rows.

Write workload (`cargo bench -p entdb --bench workload_bench -- --noplot`):

| Operation | EntDB Full | EntDB Normal | SQLite Full | SQLite Normal |
|---|---:|---:|---:|---:|
| INSERT 100 (no txn) | 462.20 ms | 11.201 ms | 7.0020 ms | 2.1716 ms |
| UPDATE 100 | 481.27 ms | 16.221 ms | 7.1612 ms | 2.1869 ms |
| DELETE 100 | 483.43 ms | 13.683 ms | 7.2658 ms | 2.1430 ms |
| TRANSACTION batch | 15.268 ms | 12.830 ms | 390.08 us | 403.73 us |

Read workload:

| Operation | EntDB | EntDB Prepared | SQLite | SQLite Prepared |
|---|---:|---:|---:|---:|
| SELECT ALL 100 | 45.359 us | 38.762 us | 12.181 us | 10.886 us |
| SELECT BY ID x100 | 1.0403 ms | 196.61 us | 254.11 us | 122.53 us |

Bulk embedded APIs:

| Operation | EntDB Full | EntDB Normal |
|---|---:|---:|
| insert_many (100 rows) | 5.7793 ms | 955.16 us |
| update_many (100 rows) | 11.465 ms | 7.1166 ms |
| delete_many (100 rows) | 9.6819 ms | 4.6883 ms |

Comparative microbench (`cargo bench -p entdb --bench comparative_bench -- --noplot`):

| Operation | EntDB API | EntDB Prepared | SQLite API | SQLite Prepared |
|---|---:|---:|---:|---:|
| COUNT WHERE | 9.8194 ms | 8.5285 ms | 484.76 us | 485.46 us |
| ORDER BY LIMIT | 9.3891 ms | 9.3134 ms | 679.79 us | 658.22 us |

Notes:

- `DurabilityMode::Normal` is the practical throughput mode for app writes.
- Prepared point lookups improved materially, but SQLite prepared is still faster.
- Bulk APIs are the honest best-case EntDB write path for repeated keyed mutations.
- The largest remaining gap is scan/sort/aggregate execution, not indexed equality lookup.

## Docs

- User docs: `docs-site/src/`

## License

[Apache-2.0](LICENSE)
