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
use entdb::storage::page::{Page, PageId};
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

#[derive(Clone, Copy)]
struct Workload {
    p1: PageId,
    p2: PageId,
    p3: PageId,
}

#[test]
fn recovery_failpoint_large_matrix_then_converges() {
    let _guard = failpoint_test_guard();

    let failpoint_cases = [
        ("wal.append", 0),
        ("wal.flush", 0),
        ("wal.write", 0),
        ("wal.sync", 0),
        ("disk.read_page", 0),
        ("disk.write_page", 0),
        ("disk.write_page", 2),
    ];

    for (fp, hits_before_fail) in failpoint_cases {
        run_large_failpoint_case(fp, hits_before_fail);
    }
}

fn run_large_failpoint_case(failpoint_name: &str, hits_before_fail: usize) {
    fault::clear_all_failpoints();

    let dir = tempdir().expect("tempdir");
    let suffix = format!("{}-{}", failpoint_name.replace('.', "_"), hits_before_fail);
    let db_path = dir.path().join(format!("recover_scale-{suffix}.db"));
    let wal_path = dir.path().join(format!("recover_scale-{suffix}.wal"));

    let workload = setup_workload(&db_path, &wal_path);

    let dm = Arc::new(DiskManager::new(&db_path).expect("reopen disk"));
    let bp = Arc::new(BufferPool::new(16, Arc::clone(&dm)));
    // tiny buffer forces write/sync paths when undo appends CLRs
    let lm = Arc::new(LogManager::new(&wal_path, 1).expect("reopen wal"));

    fault::set_failpoint(failpoint_name, hits_before_fail);
    let recovery = RecoveryManager::new(Arc::clone(&lm), Arc::clone(&bp));
    let err = recovery
        .recover()
        .expect_err("recovery should fail under failpoint");
    assert!(
        err.to_string().contains("failpoint"),
        "expected failpoint error for {failpoint_name}/{hits_before_fail}, got: {err}"
    );

    fault::clear_all_failpoints();

    let recovery_ok = RecoveryManager::new(Arc::clone(&lm), Arc::clone(&bp));
    recovery_ok
        .recover()
        .expect("recovery should succeed after failpoint clear");
    // idempotency under repeated replays
    recovery_ok.recover().expect("recovery second run");

    assert_expected(&dm, workload);
}

fn setup_workload(db_path: &std::path::Path, wal_path: &std::path::Path) -> Workload {
    let dm = Arc::new(DiskManager::new(db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(16, Arc::clone(&dm)));
    let lm = Arc::new(LogManager::new(wal_path, 4096).expect("log manager"));

    let p1 = allocate_zeroed_page(&bp);
    let p2 = allocate_zeroed_page(&bp);
    let p3 = allocate_zeroed_page(&bp);

    // txn1 committed
    lm.append(LogRecord::Begin { txn_id: 1 }).expect("t1 begin");
    append_update(&lm, &bp, 1, p1, 0, 11);
    append_update(&lm, &bp, 1, p2, 0, 12);
    lm.append(LogRecord::Commit { txn_id: 1 })
        .expect("t1 commit");

    // txn2 uncommitted
    lm.append(LogRecord::Begin { txn_id: 2 }).expect("t2 begin");
    append_update(&lm, &bp, 2, p2, 1, 21);
    append_update(&lm, &bp, 2, p3, 0, 22);

    // txn3 committed
    lm.append(LogRecord::Begin { txn_id: 3 }).expect("t3 begin");
    append_update(&lm, &bp, 3, p3, 1, 31);
    lm.append(LogRecord::Commit { txn_id: 3 })
        .expect("t3 commit");

    // txn4 uncommitted
    lm.append(LogRecord::Begin { txn_id: 4 }).expect("t4 begin");
    append_update(&lm, &bp, 4, p1, 2, 41);

    lm.flush().expect("flush wal before crash");

    // mixed persisted state pre-crash
    bp.flush_page(p2).expect("flush only one page before crash");

    drop(bp);
    drop(dm);
    drop(lm);

    Workload { p1, p2, p3 }
}

fn append_update(
    lm: &Arc<LogManager>,
    bp: &Arc<BufferPool>,
    txn_id: u64,
    page_id: PageId,
    offset: u16,
    new_value: u8,
) {
    lm.append(LogRecord::Update {
        txn_id,
        page_id,
        offset,
        old_data: vec![0],
        new_data: vec![new_value],
    })
    .expect("append update");

    let mut g = bp.fetch_page(page_id).expect("fetch page");
    g.body_mut()[offset as usize] = new_value;
    g.mark_dirty();
}

fn allocate_zeroed_page(bp: &Arc<BufferPool>) -> PageId {
    let mut g = bp.new_page().expect("new page");
    g.body_mut()[0] = 0;
    g.body_mut()[1] = 0;
    g.body_mut()[2] = 0;
    let pid = g.page_id();
    drop(g);
    bp.flush_page(pid).expect("flush zero page");
    pid
}

fn assert_expected(dm: &Arc<DiskManager>, workload: Workload) {
    let mut p1 = Page::default();
    let mut p2 = Page::default();
    let mut p3 = Page::default();
    dm.read_page(workload.p1, &mut p1).expect("read p1");
    dm.read_page(workload.p2, &mut p2).expect("read p2");
    dm.read_page(workload.p3, &mut p3).expect("read p3");

    // committed values remain
    assert_eq!(p1.body()[0], 11);
    assert_eq!(p2.body()[0], 12);
    assert_eq!(p3.body()[1], 31);

    // uncommitted values are undone
    assert_eq!(p2.body()[1], 0);
    assert_eq!(p3.body()[0], 0);
    assert_eq!(p1.body()[2], 0);
}
