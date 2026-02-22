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

use crate::wal::log_manager::LogManager;
use crate::wal::log_record::LogRecord;
use tempfile::tempdir;

#[test]
fn log_manager_append_flush_and_iterate() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("ent.wal");
    let manager = LogManager::new(&path, 1024).expect("create log manager");

    let l1 = manager
        .append(LogRecord::Begin { txn_id: 1 })
        .expect("append begin");
    let l2 = manager
        .append(LogRecord::Update {
            txn_id: 1,
            page_id: 5,
            offset: 0,
            old_data: vec![0],
            new_data: vec![9],
        })
        .expect("append update");
    let l3 = manager
        .append(LogRecord::Commit { txn_id: 1 })
        .expect("append commit");

    assert_eq!(l1, 1);
    assert_eq!(l2, 2);
    assert_eq!(l3, 3);

    manager.flush().expect("flush");
    assert_eq!(manager.flushed_lsn(), 3);

    let entries = manager.iter_from(2).expect("iter from").collect::<Vec<_>>();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].lsn, 2);
    assert_eq!(entries[1].lsn, 3);
}

#[test]
fn log_manager_iter_from_after_end_is_empty() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("ent2.wal");
    let manager = LogManager::new(&path, 1024).expect("create");

    manager
        .append(LogRecord::Begin { txn_id: 1 })
        .expect("append");
    manager.flush().expect("flush");

    let entries = manager.iter_from(1000).expect("iter").collect::<Vec<_>>();
    assert!(entries.is_empty());
}

#[test]
fn log_manager_flush_up_to_advances_flushed_lsn() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("ent3.wal");
    let manager = LogManager::new(&path, 4096).expect("create");

    let l1 = manager
        .append(LogRecord::Begin { txn_id: 1 })
        .expect("append1");
    let l2 = manager
        .append(LogRecord::Commit { txn_id: 1 })
        .expect("append2");

    assert_eq!(manager.flushed_lsn(), 0);
    manager.flush_up_to(l1).expect("flush up to");
    assert!(manager.flushed_lsn() >= l1);
    assert!(manager.flushed_lsn() <= l2);
}
