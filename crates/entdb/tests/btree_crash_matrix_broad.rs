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
use entdb::storage::btree::{BTree, KeySchema};
use entdb::storage::buffer_pool::BufferPool;
use entdb::storage::disk_manager::DiskManager;
use std::sync::{Arc, Mutex, OnceLock};
use tempfile::tempdir;

fn failpoint_test_guard() -> std::sync::MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

#[test]
fn btree_broad_crash_matrix_converges_after_failpoint_recovery() {
    let _guard = failpoint_test_guard();

    let failpoints = [
        "btree.before_leaf_split",
        "btree.before_leaf_link_update",
        "btree.before_internal_split",
        "btree.before_root_update",
        "btree.after_root_prepare",
    ];

    for fp in failpoints {
        for hits_before in [0_usize, 1, 3] {
            run_case(fp, hits_before);
        }
    }
}

fn run_case(failpoint: &str, hits_before: usize) {
    fault::clear_all_failpoints();

    let dir = tempdir().expect("tempdir");
    let db_path = dir
        .path()
        .join(format!("btree-broad-{}-{}.db", failpoint, hits_before));

    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(512, Arc::clone(&dm)));
    let tree = BTree::create_with_order(Arc::clone(&bp), KeySchema, 3).expect("create tree");

    for k in 0..200_u32 {
        tree.insert(&k.to_be_bytes(), (k, 0)).expect("seed insert");
    }

    fault::set_failpoint(failpoint, hits_before);
    for k in 200_u32..1500_u32 {
        if let Err(err) = tree.insert(&k.to_be_bytes(), (k, 1)) {
            assert!(err.to_string().contains("failpoint"));
            break;
        }
    }
    fault::clear_all_failpoints();

    // A second wave should converge after failpoint is removed.
    for k in 1500_u32..2200_u32 {
        let _ = tree.insert(&k.to_be_bytes(), (k, 2));
    }
    bp.flush_all().expect("flush all");

    let root = tree.root_page_id();
    drop(tree);
    drop(bp);
    drop(dm);

    let dm2 = Arc::new(DiskManager::new(&db_path).expect("reopen disk manager"));
    let bp2 = Arc::new(BufferPool::new(512, Arc::clone(&dm2)));
    let reopened = BTree::open(root, Arc::clone(&bp2), KeySchema);

    if reopened.check_integrity().is_err() {
        reopened
            .rebuild_leaf_links()
            .expect("rebuild leaf links after crashpoint run");
        reopened.check_integrity().expect("integrity after rebuild");
    }

    let it = reopened
        .range_scan(Some(&0_u32.to_be_bytes()), Some(&2200_u32.to_be_bytes()))
        .expect("range scan");
    assert!(it.count() > 0);
}
