# Quickstart

Run EntDB locally using one of two paths:

- Server path (`entdb-server` + `psql`)
- Embedded path (Rust API with `EntDb::connect`)

## Path A: Server (`entdb-server` + `psql`)

### 1. Start the server

```bash
cargo run -p entdb-server -- \
  --host 127.0.0.1 \
  --port 5433 \
  --data-path ./entdb.data \
  --auth-user entdb \
  --auth-password entdb
```

Optional: enable polyglot ingress rewrites (MySQL-style backticks and numeric `LIMIT offset, count`):

```bash
ENTDB_POLYGLOT=1 cargo run -p entdb-server -- \
  --host 127.0.0.1 \
  --port 5433 \
  --data-path ./entdb.data \
  --auth-user entdb \
  --auth-password entdb
```

### 2. Connect with `psql`

```bash
psql "host=127.0.0.1 port=5433 user=entdb password=entdb dbname=entdb"
```

### 3. Execute SQL

```sql
CREATE TABLE users (id INTEGER, name TEXT);
INSERT INTO users VALUES (1, 'alice'), (2, 'bob');
SELECT id, name FROM users ORDER BY id LIMIT 10;
```

Expected result:

```text
 id | name
----+-------
  1 | alice
  2 | bob
```

## Path B: Embedded Rust API

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
