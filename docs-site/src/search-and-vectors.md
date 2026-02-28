# Search and Vectors

EntDB supports vector similarity and BM25 text search in both runtime modes:

- server mode (`entdb-server` via pgwire clients like `psql`)
- embedded mode (`entdb` crate via `EntDb::execute`)

## Vector SQL (`pgvector`-style surface)

Supported:

- `VECTOR(n)` column type
- vector text literals: `'[0.1,0.2,0.3]'`
- `<->` L2 distance
- `<=>` cosine distance (distance form)

Example:

```sql
CREATE TABLE embeddings (id INT, vec VECTOR(3));
INSERT INTO embeddings VALUES
  (1, '[0.1,0.2,0.3]'),
  (2, '[0.9,0.8,0.7]');

SELECT id, vec <-> '[0.2,0.2,0.2]' AS l2_dist
FROM embeddings
ORDER BY id;
```

## BM25 SQL (`pg_textsearch`-style surface)

Supported:

- `CREATE INDEX ... USING bm25 (...)`
- optional `WITH (text_config='english'|'simple')`
- `to_bm25query(query_text, index_name)`
- `<@` score operator

Example:

```sql
CREATE TABLE docs (id INT, content TEXT);
INSERT INTO docs VALUES
  (1, 'database database systems'),
  (2, 'systems design'),
  (3, 'database retrieval');

CREATE INDEX idx_docs_bm25 ON docs USING bm25 (content) WITH (text_config='english');

SELECT id, content <@ to_bm25query('database', 'idx_docs_bm25') AS score
FROM docs
ORDER BY id;
```

## Embedded mode example

```rust
use entdb::EntDb;

fn main() -> entdb::Result<()> {
    let db = EntDb::connect("./entdb_data")?;
    db.execute("CREATE TABLE embeddings (id INT, vec VECTOR(3))")?;
    db.execute("INSERT INTO embeddings VALUES (1, '[0.1,0.2,0.3]')")?;
    db.execute("SELECT id, vec <-> '[0.2,0.2,0.2]' AS d FROM embeddings")?;

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

## Planner behavior and limitations

- BM25 sidecar files are persisted and maintained for DML operations.
- Planner can choose a BM25-backed scan for matching single-table query shapes that include
  `column <@ to_bm25query('...', 'index_name')` and a matching BM25 index.
- Non-matching shapes safely fall back to standard plan paths.

Current limitations:

- `text_config` supports only `english` and `simple`.
- BM25 shape specialization is not universal for all SQL forms.
- BM25/sidecar behavior is optimized for current SQL subset and continues to evolve.

## Reliability and sidecar format

- BM25 sidecar files are schema-versioned (`version` field).
- Legacy unversioned sidecars are read for compatibility.
- Writes are atomic via temp-file + rename.

## Troubleshooting

- `unsupported bm25 text_config ...`:
  use `english` or `simple`.
- `vector dimension mismatch ...`:
  ensure literal dimensions match `VECTOR(n)`.
- `to_bm25query first argument must be TEXT`:
  pass a string literal/query text.
