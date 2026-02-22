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
use crate::storage::page::Page;
use crate::wal::log_manager::LogManager;
use crate::wal::log_record::LogRecord;
use crate::wal::recovery::RecoveryManager;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn recovery_redo_and_undo_behaviour() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("ent.db");
    let wal_path = dir.path().join("ent.wal");

    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(8, Arc::clone(&dm)));
    let log = Arc::new(LogManager::new(&wal_path, 4096).expect("log manager"));

    let page_id = {
        let mut p = bp.new_page().expect("new page");
        p.body_mut()[0] = 0;
        p.body_mut()[1] = 0;
        p.page_id()
    };
    bp.flush_page(page_id).expect("flush base page");
    bp.flush_all().expect("flush all");

    log.append(LogRecord::Begin { txn_id: 1 })
        .expect("txn1 begin");
    log.append(LogRecord::Update {
        txn_id: 1,
        page_id,
        offset: 0,
        old_data: vec![0],
        new_data: vec![7],
    })
    .expect("txn1 update");
    log.append(LogRecord::Commit { txn_id: 1 })
        .expect("txn1 commit");

    log.append(LogRecord::Begin { txn_id: 2 })
        .expect("txn2 begin");
    log.append(LogRecord::Update {
        txn_id: 2,
        page_id,
        offset: 1,
        old_data: vec![0],
        new_data: vec![9],
    })
    .expect("txn2 update");

    log.flush().expect("flush wal");

    drop(bp);
    drop(dm);
    drop(log);

    let dm2 = Arc::new(DiskManager::new(&db_path).expect("reopen disk manager"));
    let bp2 = Arc::new(BufferPool::new(8, Arc::clone(&dm2)));
    let log2 = Arc::new(LogManager::new(&wal_path, 4096).expect("reopen log manager"));
    let recovery = RecoveryManager::new(Arc::clone(&log2), Arc::clone(&bp2));
    recovery.recover().expect("recover");

    let mut page = Page::default();
    dm2.read_page(page_id, &mut page)
        .expect("read recovered page");
    assert_eq!(page.body()[0], 7, "committed update must be redone");
    assert_eq!(page.body()[1], 0, "uncommitted update must be undone");
}

#[test]
fn recovery_is_idempotent_on_repeated_runs() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("ent_idempotent.db");
    let wal_path = dir.path().join("ent_idempotent.wal");

    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(4, Arc::clone(&dm)));
    let log = Arc::new(LogManager::new(&wal_path, 4096).expect("log manager"));

    let page_id = {
        let p = bp.new_page().expect("new page");
        p.page_id()
    };

    log.append(LogRecord::Begin { txn_id: 1 }).expect("begin");
    log.append(LogRecord::Update {
        txn_id: 1,
        page_id,
        offset: 0,
        old_data: vec![0],
        new_data: vec![33],
    })
    .expect("update");
    log.append(LogRecord::Commit { txn_id: 1 }).expect("commit");
    log.flush().expect("flush wal");

    let recovery = RecoveryManager::new(Arc::clone(&log), Arc::clone(&bp));
    recovery.recover().expect("recover #1");
    recovery.recover().expect("recover #2");

    let mut page = Page::default();
    dm.read_page(page_id, &mut page).expect("read page");
    assert_eq!(page.body()[0], 33);
}
