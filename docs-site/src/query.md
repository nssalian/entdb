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
