# entdb

`entdb` is the core Rust library crate for EntDB.

It provides:

- SQL parsing/planning/execution
- table/catalog management
- storage engine internals
- WAL + crash recovery
- MVCC transaction support
- durability policies (`Full`, `Normal`, `Off`)
- prepared execution fast paths
- batched write APIs for embedded use

## Install

```toml
[dependencies]
entdb = "0.2.0"
```

## Quick Start

```rust
use entdb::EntDb;

fn main() -> entdb::Result<()> {
    let db = EntDb::connect("./entdb_data")?;
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")?;
    db.execute("INSERT INTO users VALUES (1, 'alice')")?;
    let rows = db.execute("SELECT * FROM users")?;
    println!("{rows:?}");
    db.close()?;
    Ok(())
}
```

Useful embedded APIs:

- `prepare(...)` + `execute_prepared(...)`
- `execute_with_options(...)` with `ExecuteOptions { await_durable: Some(true) }`
- `insert_many(...)`
- `update_many(...)`
- `delete_many(...)`
- `set_durability_mode(DurabilityMode::Normal)`

## Benchmark Snapshot

From local Criterion runs (`cargo bench -p entdb --bench workload_bench -- --noplot`):

Statement-at-a-time writes:

| Operation | EntDB Full | EntDB Normal | SQLite Full | SQLite Normal |
|---|---:|---:|---:|---:|
| INSERT 100 (no txn) | 462.20 ms | 11.201 ms | 7.0020 ms | 2.1716 ms |
| UPDATE 100 | 481.27 ms | 16.221 ms | 7.1612 ms | 2.1869 ms |
| DELETE 100 | 483.43 ms | 13.683 ms | 7.2658 ms | 2.1430 ms |
| TRANSACTION batch | 15.268 ms | 12.830 ms | 390.08 us | 403.73 us |

Prepared read hot paths:

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

Notes:

- `DurabilityMode::Normal` is the practical write-throughput baseline.
- Prepared point lookups improved materially, but SQLite prepared remains faster.
- Bulk APIs are currently the best embedded write path for repeated keyed changes.

## Links

- Repository: <https://github.com/nssalian/entdb>
- Server crate: <https://crates.io/crates/entdb-server>
