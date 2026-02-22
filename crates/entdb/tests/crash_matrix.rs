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
use entdb::storage::page::Page;
use entdb::wal::log_manager::LogManager;
use entdb::wal::log_record::LogRecord;
use entdb::wal::recovery::RecoveryManager;
use std::sync::Arc;
use tempfile::tempdir;

#[derive(Clone, Copy)]
struct CrashScenario {
    name: &'static str,
    write_update: bool,
    write_commit: bool,
    flush_page_before_crash: bool,
    expect_value: u8,
}

#[test]
fn recovery_crash_point_matrix() {
    let scenarios = [
        CrashScenario {
            name: "crash_after_begin",
            write_update: false,
            write_commit: false,
            flush_page_before_crash: false,
            expect_value: 0,
        },
        CrashScenario {
            name: "crash_after_update_uncommitted",
            write_update: true,
            write_commit: false,
            flush_page_before_crash: false,
            expect_value: 0,
        },
        CrashScenario {
            name: "crash_after_commit_before_page_flush",
            write_update: true,
            write_commit: true,
            flush_page_before_crash: false,
            expect_value: 9,
        },
        CrashScenario {
            name: "crash_after_commit_after_page_flush",
            write_update: true,
            write_commit: true,
            flush_page_before_crash: true,
            expect_value: 9,
        },
    ];

    for scenario in scenarios {
        run_scenario(scenario);
    }
}

fn run_scenario(s: CrashScenario) {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join(format!("{}.db", s.name));
    let wal_path = dir.path().join(format!("{}.wal", s.name));

    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(8, Arc::clone(&dm)));
    let lm = Arc::new(LogManager::new(&wal_path, 4096).expect("log manager"));

    let page_id = {
        let mut p = bp.new_page().expect("new page");
        p.body_mut()[0] = 0;
        p.page_id()
    };
    bp.flush_page(page_id).expect("flush base page");

    lm.append(LogRecord::Begin { txn_id: 1 }).expect("begin");

    if s.write_update {
        lm.append(LogRecord::Update {
            txn_id: 1,
            page_id,
            offset: 0,
            old_data: vec![0],
            new_data: vec![9],
        })
        .expect("update");
    }

    if s.write_commit {
        lm.append(LogRecord::Commit { txn_id: 1 }).expect("commit");
    }

    lm.flush().expect("flush wal");

    if s.flush_page_before_crash {
        let mut p = bp.fetch_page(page_id).expect("fetch page");
        p.body_mut()[0] = 9;
        p.mark_dirty();
        drop(p);
        bp.flush_page(page_id).expect("flush updated page");
    }

    drop(bp);
    drop(dm);
    drop(lm);

    let dm2 = Arc::new(DiskManager::new(&db_path).expect("reopen disk"));
    let bp2 = Arc::new(BufferPool::new(8, Arc::clone(&dm2)));
    let lm2 = Arc::new(LogManager::new(&wal_path, 4096).expect("reopen wal"));

    let recovery = RecoveryManager::new(Arc::clone(&lm2), Arc::clone(&bp2));
    recovery
        .recover()
        .unwrap_or_else(|e| panic!("{}: recovery failed: {e}", s.name));

    let mut page = Page::default();
    dm2.read_page(page_id, &mut page)
        .unwrap_or_else(|e| panic!("{}: read failed: {e}", s.name));
    assert_eq!(
        page.body()[0],
        s.expect_value,
        "scenario {} produced wrong recovered value",
        s.name
    );
}
