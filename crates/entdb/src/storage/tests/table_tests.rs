/*
 * Copyright 2026 EntDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::storage::buffer_pool::BufferPool;
use crate::storage::disk_manager::DiskManager;
use crate::storage::table::Table;
use crate::storage::tuple::Tuple;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn table_insert_scan_delete_subset() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("table.db");

    let dm = Arc::new(DiskManager::new(&path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(32, Arc::clone(&dm)));
    let table = Table::create(1, Arc::clone(&bp)).expect("create table");

    let mut tuple_ids = Vec::new();
    for i in 0..1000_u32 {
        let bytes = i.to_le_bytes().to_vec();
        let tid = table.insert(&Tuple::new(bytes)).expect("insert tuple");
        tuple_ids.push(tid);
    }

    let scanned = table.scan().count();
    assert_eq!(scanned, 1000);

    for (idx, tid) in tuple_ids.iter().enumerate() {
        if idx % 3 == 0 {
            table.delete(*tid).expect("delete tuple");
        }
    }

    let remaining = table.scan().count();
    assert_eq!(remaining, 1000 - (1000 / 3 + 1));
}

#[test]
fn table_get_and_update() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("table2.db");

    let dm = Arc::new(DiskManager::new(&path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(8, Arc::clone(&dm)));
    let table = Table::create(11, Arc::clone(&bp)).expect("create table");

    let tid = table
        .insert(&Tuple::new(b"old-value".to_vec()))
        .expect("insert");
    let before = table.get(tid).expect("get before");
    assert_eq!(before.data, b"old-value");

    table
        .update(tid, &Tuple::new(b"new-value".to_vec()))
        .expect("update");

    let after = table.get(tid).expect("get after");
    assert_eq!(after.data, b"new-value");
}

#[test]
fn table_scan_crosses_multiple_pages() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("table3.db");

    let dm = Arc::new(DiskManager::new(&path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(4, Arc::clone(&dm)));
    let table = Table::create(12, Arc::clone(&bp)).expect("create table");

    let payload = vec![0xCD; 512];
    for _ in 0..60 {
        table.insert(&Tuple::new(payload.clone())).expect("insert");
    }

    let count = table.scan().count();
    assert_eq!(count, 60);
}
