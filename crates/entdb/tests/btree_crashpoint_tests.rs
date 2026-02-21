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

fn make_tree(order: usize) -> BTree {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("btree_crashpoint.db");
    let dm = Arc::new(DiskManager::new(&path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(256, Arc::clone(&dm)));
    BTree::create_with_order(bp, KeySchema, order).expect("create btree")
}

#[test]
fn btree_failpoint_leaf_split_path() {
    let _guard = failpoint_test_guard();
    fault::clear_all_failpoints();

    let tree = make_tree(3);
    for k in 0..3_u32 {
        tree.insert(&k.to_be_bytes(), (k, 0)).expect("warm insert");
    }

    fault::set_failpoint("btree.before_leaf_split", 0);
    let err = tree
        .insert(&99_u32.to_be_bytes(), (99, 0))
        .expect_err("expected leaf split failpoint");
    assert!(err.to_string().contains("failpoint"));
    fault::clear_all_failpoints();
}

#[test]
fn btree_failpoint_internal_split_or_root_update_path() {
    let _guard = failpoint_test_guard();
    fault::clear_all_failpoints();

    // Root-update failpoint should be hit during early tree growth.
    let root_tree = make_tree(3);
    fault::set_failpoint("btree.before_root_update", 0);
    let mut root_hit = false;
    for k in 0..200_u32 {
        if let Err(err) = root_tree.insert(&k.to_be_bytes(), (k, 0)) {
            assert!(err.to_string().contains("failpoint"));
            root_hit = true;
            break;
        }
    }
    assert!(root_hit, "expected root update failpoint to be hit");
    fault::clear_all_failpoints();

    // Internal split failpoint: grow a deeper tree then continue inserts until split path hits.
    let tree = make_tree(3);
    for k in 0..300_u32 {
        tree.insert(&k.to_be_bytes(), (k, 0)).expect("seed insert");
    }

    fault::set_failpoint("btree.before_internal_split", 0);
    let mut internal_hit = false;
    for k in 1000_u32..3000_u32 {
        if let Err(err) = tree.insert(&k.to_be_bytes(), (k, 0)) {
            assert!(err.to_string().contains("failpoint"));
            internal_hit = true;
            break;
        }
    }
    fault::clear_all_failpoints();
    assert!(internal_hit, "expected internal split failpoint to be hit");
}

#[test]
fn btree_failpoint_leaf_link_update_path() {
    let _guard = failpoint_test_guard();
    fault::clear_all_failpoints();

    let tree = make_tree(3);
    // Build many leaves with sparse keys.
    for k in 0..40_u32 {
        let key = k * 10;
        tree.insert(&key.to_be_bytes(), (key, 0))
            .expect("seed insert");
    }

    fault::set_failpoint("btree.before_leaf_link_update", 0);
    // Force inserts into a non-rightmost key range so split needs sibling link update.
    let mut hit = false;
    for key in 151_u32..400_u32 {
        if let Err(err) = tree.insert(&key.to_be_bytes(), (key, 1)) {
            assert!(err.to_string().contains("failpoint"));
            hit = true;
            break;
        }
    }
    assert!(hit, "expected leaf link update failpoint to be hit");
    fault::clear_all_failpoints();
}
