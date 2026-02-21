# Reliability and Recovery

EntDB durability and restart safety are driven by WAL-first write semantics and deterministic recovery.

## Reliability stack

- WAL record checksums and replay safety,
- analysis/redo/undo recovery paths,
- failpoint and crash-point matrices,
- idempotent recovery expectations.

## WAL Recovery Flow

```text
+------------------------------+
| startup                      |
+------------------------------+
               |
               v
+------------------------------+
| scan WAL records             |
+------------------------------+
               |
               v
+------------------------------+
| analysis                     |
| - collect txn states         |
| - collect touched pages      |
+------------------------------+
               |
               v
+------------------------------+
| redo                         |
| - replay committed updates   |
| - respect page LSN checks    |
+------------------------------+
               |
               v
+------------------------------+
| undo                         |
| - roll back incomplete txns  |
+------------------------------+
               |
               v
+------------------------------+
| consistent recovered state   |
+------------------------------+
```

## Validation coverage

Reliability behavior is validated with crash matrices, failpoint-driven recovery tests, and MVCC restart visibility tests.

## What this protects

- committed transactions remain visible across restart,
- incomplete/aborted transactions do not leak visibility,
- repeated recovery runs converge to the same state.

## Reference files

- `crates/entdb/src/wal/log_record.rs`
- `crates/entdb/src/wal/log_manager.rs`
- `crates/entdb/src/wal/recovery.rs`
- `crates/entdb/src/wal/tests/recovery_tests.rs`
- `crates/entdb/tests/crash_matrix.rs`
