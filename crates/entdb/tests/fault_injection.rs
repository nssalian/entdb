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
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;

fn failpoint_test_guard() -> std::sync::MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("failpoint test guard")
}

#[test]
fn disk_failpoint_write_page_errors() {
    let _guard = failpoint_test_guard();
    fault::clear_all_failpoints();
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("fp_disk.db");

    let dm = DiskManager::new(&db_path).expect("disk manager");
    let pid = dm.allocate_page().expect("allocate page");

    fault::set_failpoint("disk.write_page", 0);
    let err = dm
        .write_page(pid, &Page::new(pid))
        .expect_err("expected write failpoint error");
    assert!(err.to_string().contains("failpoint"));
    fault::clear_all_failpoints();
}

#[test]
fn disk_failpoint_read_page_errors() {
    let _guard = failpoint_test_guard();
    fault::clear_all_failpoints();
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("fp_read.db");
    let dm = DiskManager::new(&db_path).expect("disk manager");
    let pid = dm.allocate_page().expect("allocate page");
    dm.write_page(pid, &Page::new(pid)).expect("write page");

    fault::set_failpoint("disk.read_page", 0);
    let mut out = Page::default();
    let err = dm
        .read_page(pid, &mut out)
        .expect_err("expected read failpoint error");
    assert!(err.to_string().contains("failpoint"));
    fault::clear_all_failpoints();
}

#[test]
fn disk_failpoint_allocate_page_errors() {
    let _guard = failpoint_test_guard();
    fault::clear_all_failpoints();
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("fp_alloc.db");
    let dm = DiskManager::new(&db_path).expect("disk manager");

    fault::set_failpoint("disk.allocate_page", 0);
    let err = dm
        .allocate_page()
        .expect_err("expected allocation failpoint error");
    assert!(err.to_string().contains("failpoint"));
    fault::clear_all_failpoints();
}

#[test]
fn disk_failpoint_sync_errors() {
    let _guard = failpoint_test_guard();
    fault::clear_all_failpoints();
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("fp_sync.db");

    let dm = DiskManager::new(&db_path).expect("disk manager");
    fault::set_failpoint("disk.sync", 0);
    let err = dm.sync().expect_err("expected sync failpoint error");
    assert!(err.to_string().contains("failpoint"));
    fault::clear_all_failpoints();
}

#[test]
fn wal_failpoint_flush_errors() {
    let _guard = failpoint_test_guard();
    fault::clear_all_failpoints();
    let dir = tempdir().expect("tempdir");
    let wal_path = dir.path().join("fp.wal");

    let lm = LogManager::new(&wal_path, 1024).expect("log manager");
    lm.append(LogRecord::Begin { txn_id: 1 }).expect("append");

    fault::set_failpoint("wal.flush", 0);
    let err = lm.flush().expect_err("expected wal flush failpoint error");
    assert!(err.to_string().contains("failpoint"));
    fault::clear_all_failpoints();
}

#[test]
fn disk_failpoint_partial_write_errors() {
    let _guard = failpoint_test_guard();
    fault::clear_all_failpoints();
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("fp_partial.db");

    let dm = DiskManager::new(&db_path).expect("disk manager");
    let pid = dm.allocate_page().expect("allocate page");

    fault::set_failpoint("disk.partial_write", 0);
    let err = dm
        .write_page(pid, &Page::new(pid))
        .expect_err("expected partial write failpoint");
    assert!(err.to_string().contains("failpoint"));
    fault::clear_all_failpoints();
}

#[test]
fn wal_failpoint_write_or_sync_errors() {
    let _guard = failpoint_test_guard();
    fault::clear_all_failpoints();
    let dir = tempdir().expect("tempdir");
    let wal_path = dir.path().join("fp_write_sync.wal");
    let lm = LogManager::new(&wal_path, 1024).expect("log manager");
    lm.append(LogRecord::Begin { txn_id: 1 }).expect("append");

    fault::set_failpoint("wal.write", 0);
    let err = lm.flush().expect_err("expected wal.write failpoint");
    assert!(err.to_string().contains("failpoint"));
    fault::clear_all_failpoints();

    lm.append(LogRecord::Begin { txn_id: 2 }).expect("append");
    fault::set_failpoint("wal.sync", 0);
    let err = lm.flush().expect_err("expected wal.sync failpoint");
    assert!(err.to_string().contains("failpoint"));
    fault::clear_all_failpoints();
}

#[test]
fn buffer_pool_flush_propagates_wal_flush_failpoint() {
    let _guard = failpoint_test_guard();
    fault::clear_all_failpoints();
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("fp_bp.db");
    let wal_path = dir.path().join("fp_bp.wal");

    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let lm = Arc::new(LogManager::new(&wal_path, 1024).expect("log manager"));
    let bp = BufferPool::with_log_manager(8, Arc::clone(&dm), Arc::clone(&lm));

    let lsn = lm
        .append(LogRecord::Begin { txn_id: 1 })
        .expect("append begin");
    let page_id = {
        let mut g = bp.new_page().expect("new page");
        g.set_lsn(lsn as u32);
        g.mark_dirty();
        g.page_id()
    };

    fault::set_failpoint("wal.flush", 0);
    let err = bp
        .flush_page(page_id)
        .expect_err("expected wal.flush failpoint to bubble through buffer pool flush");
    assert!(err.to_string().contains("failpoint"));
    fault::clear_all_failpoints();
}
