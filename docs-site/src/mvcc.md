# Transactions and MVCC

EntDB uses MVCC row versioning with transaction snapshots.

## MVCC Visibility Flow

```text
+-------------------------------------------+
| row version                               |
| (created_txn, deleted_txn?)               |
+-------------------------------------------+
                     |
                     v
+-------------------------------------------+
| lookup txn status for created/deleted txns|
+-------------------------------------------+
                     |
                     v
+-------------------------------------------+
| compare commit ts against reader snapshot |
+-------------------------------------------+
                     |
                     v
         +---------------------------+
         | created visible to reader?|
         +---------------------------+
            | no               | yes
            v                  v
   +----------------+   +---------------------------+
   | skip row       |   | deleted txn visible?      |
   +----------------+   +---------------------------+
                             | yes             | no
                             v                 v
                    +----------------+   +----------------+
                    | hide row       |   | return row     |
                    +----------------+   +----------------+
```

## How EntDB applies MVCC

Highlights:

- transaction API and SQL control (`BEGIN` / `COMMIT` / `ROLLBACK`),
- snapshot-based visibility rules,
- write-write conflict detection,
- persisted transaction metadata (`*.txn.wal`, `*.txn.json`),
- vacuum controls for version cleanup policy.

## Why this is useful

- readers get stable visibility while writers progress concurrently,
- write-write conflicts fail fast instead of silently corrupting visibility,
- restart behavior preserves committed state and hides incomplete work.

## Validation coverage

- concurrent transaction matrix tests,
- restart visibility tests,
- crash/recovery scenario tests.

## Reference files

- `crates/entdb/src/tx.rs`
- `crates/entdb/src/query/executor/mod.rs`
- `crates/entdb/src/query/executor/update.rs`
- `crates/entdb/src/query/executor/delete.rs`
- `crates/entdb/src/query/tests/mvcc_tests.rs`
