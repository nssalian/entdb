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

use entdb::fault;
use entdb::storage::buffer_pool::BufferPool;
use entdb::storage::disk_manager::DiskManager;
use entdb::storage::page::Page;
use entdb::wal::log_manager::LogManager;
use entdb::wal::log_record::LogRecord;
use entdb::wal::recovery::RecoveryManager;
use std::sync::{Arc, Mutex, OnceLock};
use tempfile::tempdir;

fn failpoint_test_guard() -> std::sync::MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("failpoint test guard")
}

#[test]
fn recovery_failpoint_matrix_then_success() {
    let _guard = failpoint_test_guard();
    let failpoints = [
        "wal.flush",
        "wal.append",
        "wal.write",
        "wal.sync",
        "disk.read_page",
        "disk.write_page",
    ];

    for fp in failpoints {
        run_recovery_with_failpoint(fp);
    }
}

fn run_recovery_with_failpoint(failpoint_name: &str) {
    fault::clear_all_failpoints();

    let dir = tempdir().expect("tempdir");
    let db_path = dir
        .path()
        .join(format!("recover-{}.db", failpoint_name.replace('.', "_")));
    let wal_path = dir
        .path()
        .join(format!("recover-{}.wal", failpoint_name.replace('.', "_")));

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

    // committed txn update
    log.append(LogRecord::Begin { txn_id: 1 })
        .expect("begin txn1");
    log.append(LogRecord::Update {
        txn_id: 1,
        page_id,
        offset: 0,
        old_data: vec![0],
        new_data: vec![7],
    })
    .expect("update txn1");
    log.append(LogRecord::Commit { txn_id: 1 })
        .expect("commit txn1");

    // uncommitted txn update (forces undo + CLR append)
    log.append(LogRecord::Begin { txn_id: 2 })
        .expect("begin txn2");
    log.append(LogRecord::Update {
        txn_id: 2,
        page_id,
        offset: 1,
        old_data: vec![0],
        new_data: vec![9],
    })
    .expect("update txn2");

    log.flush().expect("flush wal before crash");

    drop(bp);
    drop(dm);
    drop(log);

    let dm2 = Arc::new(DiskManager::new(&db_path).expect("reopen disk"));
    let bp2 = Arc::new(BufferPool::new(8, Arc::clone(&dm2)));
    // small WAL buffer capacity to force write/sync paths during CLR append
    let log2 = Arc::new(LogManager::new(&wal_path, 1).expect("reopen wal"));

    fault::set_failpoint(failpoint_name, 0);
    let recovery = RecoveryManager::new(Arc::clone(&log2), Arc::clone(&bp2));
    let err = recovery
        .recover()
        .expect_err("recovery should fail under injected failpoint");
    assert!(
        err.to_string().contains("failpoint"),
        "expected failpoint error for {failpoint_name}, got: {err}"
    );

    fault::clear_all_failpoints();

    // rerun recovery without failpoints; must succeed and converge.
    let recovery2 = RecoveryManager::new(Arc::clone(&log2), Arc::clone(&bp2));
    recovery2
        .recover()
        .expect("recovery should succeed after clearing failpoint");

    let mut page = Page::default();
    dm2.read_page(page_id, &mut page)
        .expect("read recovered page");
    assert_eq!(page.body()[0], 7, "committed value must survive");
    assert_eq!(page.body()[1], 0, "uncommitted value must be undone");
}
