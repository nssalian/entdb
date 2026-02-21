# Storage Engine

The storage layer is page-native and built for deterministic flush/recovery behavior under pressure.

## Buffer Pool Eviction/Flush Flow

```text
+------------------------+
| fetch/new page         |
+------------------------+
            |
            v
+------------------------+
| pin frame in pool      |
+------------------------+
            |
            v
+------------------------+
| mutate page (dirty=1)  |
+------------------------+
            |
            v
+------------------------+
| unpin frame            |
+------------------------+
            |
            v
   +-------------------+
   | pressure present? |
   +-------------------+
      | yes       | no
      v           v
+----------------------+   +----------------------+
| choose LRU-K victim  |   | reuse frame/continue |
+----------------------+   +----------------------+
            |
            v
   +-------------------+
   | victim is dirty?  |
   +-------------------+
      | yes       | no
      v           v
+----------------------+   +----------------------+
| WAL flush_up_to(LSN) |   | evict victim         |
+----------------------+   +----------------------+
            |
            v
+----------------------+
| write page to disk   |
+----------------------+
            |
            v
+----------------------+
| evict victim         |
+----------------------+
            |
            v
+----------------------+
| reuse frame/continue |
+----------------------+
```

## How EntDB uses storage components

- `Page`: fixed-size page with header/checksum, used as the stable on-disk unit.
- `DiskManager`: page allocation/deallocation and positioned I/O.
- `BufferPool`: in-memory frames with pin/unpin semantics, dirty tracking, and LRU-K victim selection.
- `SlottedPage`: tuple slot directory for variable-length row storage.
- `Table`: heap-page row layout plus tuple identity management.
- `B+Tree`: page-native index structure for point/range access.

## What this is good for

- deterministic correctness under pressure,
- stable persistence format with reopen/recovery invariants,
- predictable behavior during eviction-heavy and crash-recovery scenarios.

## Reference files

- `crates/entdb/src/storage/page.rs`
- `crates/entdb/src/storage/disk_manager.rs`
- `crates/entdb/src/storage/buffer_pool.rs`
- `crates/entdb/src/storage/slotted_page.rs`
- `crates/entdb/src/storage/table.rs`
- `crates/entdb/src/storage/btree/tree.rs`
