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

use entdb::storage::buffer_pool::BufferPool;
use entdb::storage::disk_manager::DiskManager;
use entdb::storage::page::{Page, PageId};
use entdb::wal::log_manager::LogManager;
use entdb::wal::log_record::LogRecord;
use entdb::wal::recovery::RecoveryManager;
use std::sync::Arc;
use tempfile::tempdir;

#[derive(Clone, Copy)]
enum FlushMode {
    None,
    FirstPageOnly,
    All,
}

#[derive(Clone, Copy)]
struct Scenario {
    name: &'static str,
    commit_t1: bool,
    commit_t2: bool,
    flush_mode: FlushMode,
}

#[test]
fn recovery_crash_matrix_multi_page_multi_txn() {
    let scenarios = [
        Scenario {
            name: "none_committed_no_flush",
            commit_t1: false,
            commit_t2: false,
            flush_mode: FlushMode::None,
        },
        Scenario {
            name: "t1_only_commit_no_flush",
            commit_t1: true,
            commit_t2: false,
            flush_mode: FlushMode::None,
        },
        Scenario {
            name: "t2_only_commit_no_flush",
            commit_t1: false,
            commit_t2: true,
            flush_mode: FlushMode::None,
        },
        Scenario {
            name: "both_commit_no_flush",
            commit_t1: true,
            commit_t2: true,
            flush_mode: FlushMode::None,
        },
        Scenario {
            name: "none_committed_first_flush",
            commit_t1: false,
            commit_t2: false,
            flush_mode: FlushMode::FirstPageOnly,
        },
        Scenario {
            name: "t1_only_commit_first_flush",
            commit_t1: true,
            commit_t2: false,
            flush_mode: FlushMode::FirstPageOnly,
        },
        Scenario {
            name: "t2_only_commit_first_flush",
            commit_t1: false,
            commit_t2: true,
            flush_mode: FlushMode::FirstPageOnly,
        },
        Scenario {
            name: "both_commit_first_flush",
            commit_t1: true,
            commit_t2: true,
            flush_mode: FlushMode::FirstPageOnly,
        },
        Scenario {
            name: "none_committed_all_flush",
            commit_t1: false,
            commit_t2: false,
            flush_mode: FlushMode::All,
        },
        Scenario {
            name: "t1_only_commit_all_flush",
            commit_t1: true,
            commit_t2: false,
            flush_mode: FlushMode::All,
        },
        Scenario {
            name: "t2_only_commit_all_flush",
            commit_t1: false,
            commit_t2: true,
            flush_mode: FlushMode::All,
        },
        Scenario {
            name: "both_commit_all_flush",
            commit_t1: true,
            commit_t2: true,
            flush_mode: FlushMode::All,
        },
    ];

    for s in scenarios {
        run_multi_page_scenario(s);
    }
}

fn run_multi_page_scenario(s: Scenario) {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join(format!("{}.db", s.name));
    let wal_path = dir.path().join(format!("{}.wal", s.name));

    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(16, Arc::clone(&dm)));
    let lm = Arc::new(LogManager::new(&wal_path, 4096).expect("log manager"));

    let page1 = allocate_zeroed_page(&bp);
    let page2 = allocate_zeroed_page(&bp);

    lm.append(LogRecord::Begin { txn_id: 1 }).expect("begin t1");
    lm.append(LogRecord::Update {
        txn_id: 1,
        page_id: page1,
        offset: 0,
        old_data: vec![0],
        new_data: vec![7],
    })
    .expect("update t1");
    apply_update_to_buffer(&bp, page1, 0, 7);

    lm.append(LogRecord::Begin { txn_id: 2 }).expect("begin t2");
    lm.append(LogRecord::Update {
        txn_id: 2,
        page_id: page2,
        offset: 0,
        old_data: vec![0],
        new_data: vec![9],
    })
    .expect("update t2 page2");
    apply_update_to_buffer(&bp, page2, 0, 9);

    lm.append(LogRecord::Update {
        txn_id: 2,
        page_id: page1,
        offset: 1,
        old_data: vec![0],
        new_data: vec![5],
    })
    .expect("update t2 page1");
    apply_update_to_buffer(&bp, page1, 1, 5);

    if s.commit_t1 {
        lm.append(LogRecord::Commit { txn_id: 1 })
            .expect("commit t1");
    }
    if s.commit_t2 {
        lm.append(LogRecord::Commit { txn_id: 2 })
            .expect("commit t2");
    }

    lm.flush().expect("flush wal");

    match s.flush_mode {
        FlushMode::None => {}
        FlushMode::FirstPageOnly => {
            bp.flush_page(page1).expect("flush first page");
        }
        FlushMode::All => {
            bp.flush_page(page1).expect("flush page1");
            bp.flush_page(page2).expect("flush page2");
        }
    }

    drop(bp);
    drop(dm);
    drop(lm);

    recover_once(&db_path, &wal_path);
    assert_expected_post_recovery(&db_path, page1, page2, s.commit_t1, s.commit_t2);

    // Idempotency check under repeated recovery runs.
    recover_once(&db_path, &wal_path);
    assert_expected_post_recovery(&db_path, page1, page2, s.commit_t1, s.commit_t2);
}

fn recover_once(db_path: &std::path::Path, wal_path: &std::path::Path) {
    let dm = Arc::new(DiskManager::new(db_path).expect("reopen disk"));
    let bp = Arc::new(BufferPool::new(16, Arc::clone(&dm)));
    let lm = Arc::new(LogManager::new(wal_path, 4096).expect("reopen wal"));
    let recovery = RecoveryManager::new(Arc::clone(&lm), Arc::clone(&bp));
    recovery.recover().expect("recover");
}

fn assert_expected_post_recovery(
    db_path: &std::path::Path,
    page1: PageId,
    page2: PageId,
    commit_t1: bool,
    commit_t2: bool,
) {
    let dm = DiskManager::new(db_path).expect("open disk for assertion");

    let mut p1 = Page::default();
    dm.read_page(page1, &mut p1).expect("read p1");
    let mut p2 = Page::default();
    dm.read_page(page2, &mut p2).expect("read p2");

    assert_eq!(p1.body()[0], if commit_t1 { 7 } else { 0 });
    assert_eq!(p1.body()[1], if commit_t2 { 5 } else { 0 });
    assert_eq!(p2.body()[0], if commit_t2 { 9 } else { 0 });
}

fn allocate_zeroed_page(bp: &Arc<BufferPool>) -> PageId {
    let mut g = bp.new_page().expect("new page");
    g.body_mut()[0] = 0;
    g.body_mut()[1] = 0;
    let pid = g.page_id();
    drop(g);
    bp.flush_page(pid).expect("flush base page");
    pid
}

fn apply_update_to_buffer(bp: &Arc<BufferPool>, page_id: PageId, offset: usize, value: u8) {
    let mut g = bp.fetch_page(page_id).expect("fetch page");
    g.body_mut()[offset] = value;
    g.mark_dirty();
}
