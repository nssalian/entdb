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
use entdb::storage::table::Table;
use entdb::storage::tuple::Tuple;
use rand::Rng;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[test]
#[ignore = "stress test"]
fn stress_table_concurrent_inserts_and_scan() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("stress_table.db");

    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(64, Arc::clone(&dm)));
    let table = Arc::new(Table::create(100, Arc::clone(&bp)).expect("create table"));

    let threads = 8;
    let per_thread = 2_000;

    let mut handles = Vec::new();
    for t in 0..threads {
        let table_cloned = Arc::clone(&table);
        handles.push(thread::spawn(move || {
            for i in 0..per_thread {
                let mut row = Vec::with_capacity(8);
                row.extend_from_slice(&(t as u32).to_le_bytes());
                row.extend_from_slice(&(i as u32).to_le_bytes());
                table_cloned
                    .insert(&Tuple::new(row))
                    .expect("insert in stress thread");
            }
        }));
    }

    for h in handles {
        h.join().expect("join thread");
    }

    let count = table.scan().count();
    assert_eq!(count, threads * per_thread);
}

#[test]
#[ignore = "stress test"]
fn stress_buffer_pool_random_fetches() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("stress_bp.db");

    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(32, Arc::clone(&dm)));

    let mut page_ids = Vec::new();
    for i in 0..256_u32 {
        let mut g = bp.new_page().expect("new page");
        g.body_mut()[0..4].copy_from_slice(&i.to_le_bytes());
        page_ids.push(g.page_id());
    }

    let mut handles = Vec::new();
    for _ in 0..8 {
        let bp_cloned = Arc::clone(&bp);
        let ids = page_ids.clone();
        handles.push(thread::spawn(move || {
            for idx in 0..20_000usize {
                let page_id = ids[idx % ids.len()];
                let g = bp_cloned.fetch_page(page_id).expect("fetch page");
                assert_eq!(g.page_id(), page_id);
            }
        }));
    }

    for h in handles {
        h.join().expect("join thread");
    }
}

#[test]
#[ignore = "soak stress test"]
fn soak_table_randomized_mixed_operations() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("soak_table.db");

    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(128, Arc::clone(&dm)));
    let table = Arc::new(Table::create(101, Arc::clone(&bp)).expect("create table"));
    let tids = Arc::new(std::sync::Mutex::new(Vec::new()));

    let threads = 8;
    let duration = Duration::from_secs(read_env_u64("ENTDB_STRESS_SOAK_SECS", 20));
    let start = Instant::now();

    let mut handles = Vec::new();
    for t in 0..threads {
        let table = Arc::clone(&table);
        let tids = Arc::clone(&tids);
        handles.push(thread::spawn(move || {
            let mut rng = rand::thread_rng();
            while Instant::now().duration_since(start) < duration {
                let op = rng.gen_range(0..100);
                if op < 45 {
                    // insert
                    let mut row = Vec::with_capacity(8);
                    row.extend_from_slice(&(t as u32).to_le_bytes());
                    row.extend_from_slice(&(rng.r#gen::<u32>()).to_le_bytes());
                    if let Ok(tid) = table.insert(&Tuple::new(row)) {
                        tids.lock().expect("lock tids").push(tid);
                    }
                } else if op < 65 {
                    // get
                    if let Some(tid) = tids.lock().expect("lock tids").last().copied() {
                        let _ = table.get(tid);
                    }
                } else if op < 85 {
                    // update
                    if let Some(tid) = tids.lock().expect("lock tids").last().copied() {
                        let mut row = Vec::with_capacity(8);
                        row.extend_from_slice(&(t as u32).to_le_bytes());
                        row.extend_from_slice(&(rng.r#gen::<u32>()).to_le_bytes());
                        let _ = table.update(tid, &Tuple::new(row));
                    }
                } else {
                    // delete
                    if let Some(tid) = tids.lock().expect("lock tids").pop() {
                        let _ = table.delete(tid);
                    }
                }
            }
        }));
    }

    for h in handles {
        h.join().expect("join thread");
    }

    // Final scan must complete and produce a sensible count.
    let final_count = table.scan().count();
    let tracked = tids.lock().expect("lock tids").len();
    assert!(final_count <= tracked + 1024);
}

fn read_env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}
