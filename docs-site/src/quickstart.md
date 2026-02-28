# Quickstart

Run EntDB locally using one of two paths:

- Server path (`entdb-server` + `psql`)
- Embedded path (Rust API with `EntDb::connect`)

## Path A: Server (`entdb-server` + `psql`)

### 1. Start the server

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

Optional: enable polyglot ingress rewrites (MySQL-style backticks and numeric `LIMIT offset, count`):

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

### 4. Try vector and BM25 SQL (server mode)

Run the following SQL in `psql`:

```sql
CREATE TABLE embeddings (id INT, vec VECTOR(3));
INSERT INTO embeddings VALUES (1, '[0.1,0.2,0.3]'), (2, '[0.9,0.8,0.7]');
SELECT id, vec <-> '[0.2,0.2,0.2]' AS dist FROM embeddings ORDER BY id;

CREATE TABLE docs (id INT, content TEXT);
INSERT INTO docs VALUES (1, 'database systems'), (2, 'search indexing');
CREATE INDEX idx_docs_bm25 ON docs USING bm25 (content) WITH (text_config='english');
SELECT id, content <@ to_bm25query('database', 'idx_docs_bm25') AS score FROM docs ORDER BY id;
```

## Path B: Embedded Rust API

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

### 5. Try vector and BM25 SQL (embedded mode)

Use the same features directly from Rust:

```rust
use entdb::EntDb;

fn main() -> entdb::Result<()> {
    let db = EntDb::connect("./entdb_data")?;
    db.execute("CREATE TABLE embeddings (id INT, vec VECTOR(3))")?;
    db.execute("INSERT INTO embeddings VALUES (1, '[0.1,0.2,0.3]')")?;
    db.execute("SELECT id, vec <-> '[0.2,0.2,0.2]' AS dist FROM embeddings")?;

    db.execute("CREATE TABLE docs (id INT, content TEXT)")?;
    db.execute("INSERT INTO docs VALUES (1, 'database systems')")?;
    db.execute("CREATE INDEX idx_docs_bm25 ON docs USING bm25 (content)")?;
    db.execute(
        "SELECT id, content <@ to_bm25query('database', 'idx_docs_bm25') AS score FROM docs",
    )?;
    db.close()?;
    Ok(())
}
```
