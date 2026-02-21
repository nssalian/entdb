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
- SQL subset for OLTP-style workloads.
- pgwire server for `psql` and PostgreSQL drivers.

## Run

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
entdb = "0.1.0"
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

## Smoke tests

```bash
./scripts/run_psql_smoke.sh "host=127.0.0.1 port=5433 user=entdb password=entdb dbname=entdb"
./scripts/run_psql_polyglot_smoke.sh "host=127.0.0.1 port=5433 user=entdb password=entdb dbname=entdb"
```

## Docs

- User docs: `docs-site/src/`

## License

[Apache-2.0](LICENSE)
