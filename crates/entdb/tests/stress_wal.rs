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

#[test]
#[ignore = "stress test"]
fn stress_wal_many_transactions_recover() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("stress_wal.db");
    let wal_path = dir.path().join("stress_wal.wal");

    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(32, Arc::clone(&dm)));
    let lm = Arc::new(LogManager::new(&wal_path, 64 * 1024).expect("log manager"));

    let page_id = {
        let p = bp.new_page().expect("new page");
        p.page_id()
    };

    let txn_count = read_env_u64("ENTDB_WAL_STRESS_TXNS", 200);
    for txn in 1_u64..=txn_count {
        let off = txn as u16;
        lm.append(LogRecord::Begin { txn_id: txn }).expect("begin");
        lm.append(LogRecord::Update {
            txn_id: txn,
            page_id,
            offset: off,
            old_data: vec![0],
            new_data: vec![txn as u8],
        })
        .expect("update");
        if txn % 2 == 0 {
            lm.append(LogRecord::Commit { txn_id: txn })
                .expect("commit");
        }
    }
    lm.flush().expect("flush wal");

    let recovery = RecoveryManager::new(Arc::clone(&lm), Arc::clone(&bp));
    recovery.recover().expect("recover");

    let mut page = Page::default();
    dm.read_page(page_id, &mut page).expect("read page");
    for txn in 1_u64..=txn_count {
        let got = page.body()[txn as usize];
        if txn % 2 == 0 {
            assert_eq!(got, txn as u8);
        } else {
            assert_eq!(got, 0);
        }
    }
}

fn read_env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}
