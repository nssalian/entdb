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
