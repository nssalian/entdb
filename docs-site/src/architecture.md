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

1. Parse/bind SQL.
2. Plan logical operators.
3. Optimize (CBO/HBO with history).
4. Execute operators against MVCC-visible rows.
5. Persist writes via WAL-first ordering and page flush.

## Vector and BM25 path

- Vector operators (`<->`, `<=>`) are evaluated in expression execution.
- BM25 index metadata is stored in catalog (`IndexType::Bm25` with `text_config`).
- BM25 documents/postings are stored in per-index sidecar files:
  `*.bm25.<index>.json`.
- Sidecar writes are atomic (temp file + rename) and sidecar format is versioned.
- On matching query shapes (`column <@ to_bm25query(...)` with matching index),
  planner can route to a BM25-backed scan path; non-matching shapes fall back
  to regular scan/filter/project paths.
