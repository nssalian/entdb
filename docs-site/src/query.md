# SQL and Query Engine

EntDB executes SQL through a fixed pipeline:

1. Parse SQL into AST.
2. Bind names/types/relations.
3. Build logical plan.
4. Optimize simple plan shape.
5. Execute with concrete operators.

## Supported SQL surface

Implemented subset includes:

- DDL:
- `CREATE TABLE`, `DROP TABLE`
- `CREATE INDEX`, `DROP INDEX`
- selected `ALTER TABLE` operations (add/drop/rename column, rename table)
- DML/query:
- `INSERT`, `INSERT ... SELECT`, `UPSERT/ON CONFLICT`, `SELECT`, `UPDATE`, `DELETE`, `TRUNCATE`
- extended DML forms:
- `UPDATE ... FROM`, `UPDATE ... RETURNING`
- `DELETE ... USING`, `DELETE ... RETURNING`, `DELETE ... ORDER BY/LIMIT`, multi-table `DELETE`
- predicates and ordering: `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`
- aggregation: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, multi-column `GROUP BY`
- relational operators: `INNER JOIN` (including join chains), `UNION`, `UNION ALL`
- query forms: CTE (`WITH`), derived subqueries, literal `SELECT` without `FROM`
- window/query features: `row_number() over (...)`, scalar function projections
- transaction SQL: `BEGIN`, `COMMIT`, `ROLLBACK`

It also supports:

- typed parameter binding for extended protocol (AST-level binding, no string substitution),
- optional SQL dialect transpilation ingress with guarded fallback/error contracts.

## Prepared execution

EntDB has two prepared execution layers:

- generic prepared execution, which reuses parsed SQL and bound parameters
- a narrow prepared fast path for common hot statements

The current fast path covers:

- `SELECT ... FROM t`
- `SELECT ... FROM t WHERE col = $1`
- `SELECT COUNT(*) ... WHERE col OP $1`
- simple single-row `INSERT`
- keyed `UPDATE`
- keyed `DELETE`

This fast path is intentionally narrow. Unsupported shapes fall back to the normal binder/planner/executor pipeline.

## Index-backed equality lookups

For single-column B-tree indexes, equality predicates can bypass full scans:

- `CREATE INDEX ... USING btree` builds the index over existing rows
- later DML keeps the index in sync
- simple equality filters can dispatch to an index lookup executor

This is the path used to speed up repeated keyed lookups such as `WHERE id = $1`.

## Bulk embedded APIs

The embedded Rust API also exposes batched write helpers:

- `insert_many(...)`
- `update_many(...)`
- `delete_many(...)`

These helpers are not new SQL syntax. They are embedded API shortcuts that execute repeated keyed changes in one transaction while avoiding repeated SQL parse/bind/plan work.

## SQL Dialect Transpiler Support

EntDB includes a guarded SQL transpiler ingress for selected non-PostgreSQL query shapes.

It uses the external [`polyglot-sql`](https://crates.io/crates/polyglot-sql) crate.

How it works:

- Transpiler is disabled by default.
- Enable it with `ENTDB_POLYGLOT=1` (server) or `engine.set_polyglot_enabled(true)` (embedded `QueryEngine`).
- SQL is transpiled before PostgreSQL parsing/binding.
- If transpilation changes SQL, EntDB records original and transpiled forms in error context for debugging.

Currently supported rewrites:

- MySQL-style identifier quoting: `` `users` `` -> `"users"`
- MySQL numeric `LIMIT offset, count` -> PostgreSQL `LIMIT count OFFSET offset`

Guardrails and behavior:

- Non-numeric `LIMIT offset, count` is left unchanged (no unsafe guessing).
- Unbalanced backticks are rejected.
- Unsupported delimiter syntax is rejected.
- Rewriter only triggers for candidate inputs; normal PostgreSQL SQL bypasses transpilation.

Example (with transpiler enabled):

```sql
SELECT `id`, `name` FROM `users` ORDER BY `id` LIMIT 1, 2;
```

is executed as:

```sql
SELECT "id", "name" FROM "users" ORDER BY "id" LIMIT 2 OFFSET 1;
```

## Vector and BM25 support (both modes)

The same SQL surface is supported through:

- server mode (`entdb-server`, pgwire clients such as `psql`)
- embedded mode (`EntDb` Rust API)

### Vector SQL

- type: `VECTOR(n)`
- literal form: `'[x,y,z]'`
- operators:
- `<->` (L2 distance)
- `<=>` (cosine distance)

Example:

```sql
CREATE TABLE embeddings (id INT, vec VECTOR(3));
INSERT INTO embeddings VALUES (1, '[0.1,0.2,0.3]');
SELECT id, vec <-> '[0.2,0.2,0.2]' AS dist FROM embeddings;
```

### BM25 SQL

- index DDL: `CREATE INDEX ... USING bm25 (...) [WITH (...)]`
- query constructor: `to_bm25query(query_text, index_name)`
- scoring operator: `<@`

Example:

```sql
CREATE TABLE docs (id INT, content TEXT);
CREATE INDEX idx_docs_bm25 ON docs USING bm25 (content) WITH (text_config='english');
SELECT id, content <@ to_bm25query('database', 'idx_docs_bm25') AS score
FROM docs;
```

Status note:

- BM25 sidecar index persistence and DML maintenance are implemented.
- Planner/executor can use a BM25-backed scan for matching `<@ to_bm25query(...)` query shapes.
- BM25 sidecar files are versioned (`version` field) with legacy unversioned read compatibility.

## Why this design

- Same core engine for embedded and pgwire paths.
- Predictable semantics for common transactional queries.
- Defensive error handling for malformed SQL and unsupported rewrites.

## Reference files

- `crates/entdb/src/query/binder.rs`
- `crates/entdb/src/query/planner.rs`
- `crates/entdb/src/query/optimizer.rs`
- `crates/entdb/src/query/executor/mod.rs`
- `crates/entdb/src/query/tests/engine_tests.rs`
