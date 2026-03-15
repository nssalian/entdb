# Architecture

EntDB has two ingress paths:

- pgwire server path (`entdb-server`), and
- embedded Rust API path (`QueryEngine`).

Both use the same SQL core (binder/planner/optimizer/executor), MVCC model, and storage engine.

```text
+--------------------+
| External clients   |
| psql / pg drivers  |
+---------+----------+
          |
          v
+--------------------+                        +--------------------+
| entdb-server       |                        | Embedded app       |
| pgwire, auth, TLS  |                        | Rust API           |
+---------+----------+                        +---------+----------+
          |                                             |
          +----------------------+----------------------+
                                 |
                                 v
                      +------------------------------+
                      | SQL Core                     |
                      | Binder / Planner /           |
                      | Optimizer / Executors        |
                      +---------------+--------------+
                                      |
                                      v
                      +------------------------------+
                      | Catalog                       |
                      | table/index metadata          |
                      +---------------+--------------+
                                      |
                                      v
                      +------------------------------+
                      | TransactionManager            |
                      | MVCC + txn lifecycle          |
                      +-------+---------------+-------+
                              |               |
                              |               +--------------------------+
                              |                                          |
                              v                                          v
                    +----------------------+                  +----------------------+
                    | BufferPool + Table + |                  | *.txn.wal            |
                    | B+Tree               |                  | *.txn.json           |
                    +----------+-----------+                  +----------------------+
                               |
                               v
                    +----------------------+
                    | LogManager +         |
                    | RecoveryManager      |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | *.wal                |
                    +----------------------+
                               |
                               v
                    +----------------------+
                    | *.data               |
                    +----------------------+

+------------------------------+
| OptimizerHistoryRecorder     |
+--------------+---------------+
               ^
               |
+--------------+--------------+
| QueryEngine / entdb-server  |
| SQL path (read + write)     |
+--------------+--------------+
               |
               v
+------------------------------+
| *.optimizer_history.json     |
+------------------------------+

+------------------------------+
| *.bm25.<index>.json          |
| (versioned sidecar index)    |
+------------------------------+
```

## Startup (`Database::open`)

1. `DiskManager` + `LogManager`
2. `BufferPool::with_log_manager(...)`
3. `RecoveryManager::recover()`
4. `Catalog::load(...)`
5. `TransactionManager` persistence setup
6. `OptimizerHistoryRecorder` initialization

## Query flow

1. Parse SQL, or reuse a prepared statement.
2. Bind names/types/relations, unless a prepared fast path can bypass the generic binder.
3. Plan logical operators.
4. Optimize (CBO/HBO with history) or dispatch to a narrow prepared fast path.
5. Execute operators against MVCC-visible rows or index lookup paths.
6. Persist writes via WAL-first ordering and page flush according to durability policy.

## Durability policy

EntDB exposes three runtime durability policies:

- `Full`: strict sync on commit path
- `Normal`: reduced sync pressure for higher throughput
- `Off`: best-effort durability for ephemeral workloads

Embedded and server callers can also force durability on individual writes with an explicit barrier or per-call override.

## Index path

- `CREATE INDEX ... USING btree` builds a secondary B-tree over existing rows.
- DML maintains those indexes on later `INSERT`, `UPDATE`, `DELETE`, `UPSERT`, and `INSERT ... SELECT`.
- Equality filters on indexed columns can route directly to index lookup executors instead of scanning the table.

## Prepared and bulk execution

- Prepared statements can use a fast path for common point reads and simple keyed DML.
- Embedded bulk APIs (`insert_many`, `update_many`, `delete_many`) run repeated keyed writes in one transaction and avoid parser/binder/planner churn.

## Vector and BM25 path

- Vector operators (`<->`, `<=>`) are evaluated in expression execution.
- BM25 index metadata is stored in catalog (`IndexType::Bm25` with `text_config`).
- BM25 documents/postings are stored in per-index sidecar files:
  `*.bm25.<index>.json`.
- Sidecar writes are atomic (temp file + rename) and sidecar format is versioned.
- On matching query shapes (`column <@ to_bm25query(...)` with matching index),
  planner can route to a BM25-backed scan path; non-matching shapes fall back
  to regular scan/filter/project paths.
