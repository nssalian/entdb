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

use crate::fault;
use crate::storage::btree::{BTree, KeySchema};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::disk_manager::DiskManager;
use crate::wal::log_manager::LogManager;
use crate::wal::log_record::LogRecord;
use rand::seq::SliceRandom;
use std::sync::{Arc, Mutex, OnceLock};
use tempfile::tempdir;

fn make_tree(order: usize) -> BTree {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("btree.db");
    let dm = Arc::new(DiskManager::new(&path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(256, Arc::clone(&dm)));
    BTree::create_with_order(bp, KeySchema, order).expect("create btree")
}

fn failpoint_test_guard() -> std::sync::MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

#[test]
fn btree_persistence_reopen_round_trip() {
    let _guard = failpoint_test_guard();
    fault::clear_all_failpoints();
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("btree_persist.db");
    let dm = Arc::new(DiskManager::new(&path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(256, Arc::clone(&dm)));
    let tree = BTree::create_with_order(Arc::clone(&bp), KeySchema, 4).expect("create");

    for k in 0..150_u32 {
        tree.insert(&k.to_be_bytes(), (k + 1, (k % 13) as u16))
            .expect("insert");
    }
    bp.flush_all().expect("flush all");
    let root = tree.root_page_id();

    drop(tree);
    drop(bp);
    drop(dm);

    let dm2 = Arc::new(DiskManager::new(&path).expect("reopen disk manager"));
    let bp2 = Arc::new(BufferPool::new(256, Arc::clone(&dm2)));
    let reopened = BTree::open(root, Arc::clone(&bp2), KeySchema);

    for k in 0..150_u32 {
        assert_eq!(
            reopened.search(&k.to_be_bytes()).expect("search"),
            Some((k + 1, (k % 13) as u16))
        );
    }

    let keys = reopened
        .range_scan(None, None)
        .expect("range scan")
        .map(|(k, _)| u32::from_be_bytes(k.try_into().expect("u32 key")))
        .collect::<Vec<_>>();
    assert_eq!(keys, (0..150_u32).collect::<Vec<_>>());
}

#[test]
fn btree_structural_changes_emit_wal_records() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("btree_wal.db");
    let wal_path = dir.path().join("btree_wal.log");
    let dm = Arc::new(DiskManager::new(&path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(256, Arc::clone(&dm)));
    let lm = Arc::new(LogManager::new(&wal_path, 4096).expect("log manager"));
    let tree = BTree::create_with_wal(Arc::clone(&bp), KeySchema, Arc::clone(&lm)).expect("create");

    for k in 0..80_u32 {
        tree.insert(&k.to_be_bytes(), (k, 0)).expect("insert");
    }

    lm.flush().expect("flush wal");
    let entries = lm.iter_from(0).expect("iter wal").collect::<Vec<_>>();
    let structural = entries
        .iter()
        .filter(|e| matches!(e.record, LogRecord::BtreeStructure { .. }))
        .count();
    assert!(structural > 0, "expected btree structural wal records");
}

#[test]
fn btree_root_update_recovers_after_prepare_crash() {
    let _guard = failpoint_test_guard();
    fault::clear_all_failpoints();
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("btree_root_prepare.db");
    let dm = Arc::new(DiskManager::new(&path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(256, Arc::clone(&dm)));
    let tree = BTree::create_with_order(Arc::clone(&bp), KeySchema, 3).expect("create");

    // Trigger a root split while failing right after root-update prepare.
    fault::clear_all_failpoints();
    fault::set_failpoint("btree.after_root_prepare", 0);
    let mut failed = false;
    for k in 0..300_u32 {
        if tree.insert(&k.to_be_bytes(), (k, 0)).is_err() {
            failed = true;
            break;
        }
    }
    assert!(failed, "expected failpoint-triggered insert failure");
    fault::clear_all_failpoints();

    let meta = tree.root_page_id();
    bp.flush_all().expect("flush");
    drop(tree);
    drop(bp);
    drop(dm);

    // Reopen must finalize pending root-update metadata and keep tree readable.
    let dm2 = Arc::new(DiskManager::new(&path).expect("reopen dm"));
    let bp2 = Arc::new(BufferPool::new(256, Arc::clone(&dm2)));
    let reopened = BTree::open(meta, Arc::clone(&bp2), KeySchema);
    reopened.check_integrity().expect("integrity");

    // Ensure at least an inserted key remains searchable after reopen.
    let got = reopened.search(&0_u32.to_be_bytes()).expect("search");
    assert!(got.is_some());
    fault::clear_all_failpoints();
}

#[test]
fn btree_check_and_rebuild_leaf_links() {
    let tree = make_tree(4);
    for k in 0..120_u32 {
        tree.insert(&k.to_be_bytes(), (k, 0)).expect("insert");
    }
    tree.check_integrity().expect("integrity before rebuild");
    tree.rebuild_leaf_links().expect("rebuild leaf links");
    tree.check_integrity().expect("integrity after rebuild");
}

#[test]
fn btree_random_insert_sorted_retrieval() {
    let tree = make_tree(4);

    let mut keys: Vec<u32> = (0..200).collect();
    keys.shuffle(&mut rand::thread_rng());

    for k in &keys {
        tree.insert(&k.to_be_bytes(), (*k, (*k % 17) as u16))
            .expect("insert key");
    }

    for k in 0..200_u32 {
        let got = tree.search(&k.to_be_bytes()).expect("search");
        assert_eq!(got, Some((k, (k % 17) as u16)));
    }

    let collected = tree
        .range_scan(None, None)
        .expect("range scan")
        .map(|(k, _)| u32::from_be_bytes(k.try_into().expect("u32 key")))
        .collect::<Vec<_>>();

    let expected = (0..200_u32).collect::<Vec<_>>();
    assert_eq!(collected, expected);
}

#[test]
fn btree_range_scan_and_delete() {
    let tree = make_tree(5);
    for k in 0..100_u32 {
        tree.insert(&k.to_be_bytes(), (k + 10, 1)).expect("insert");
    }

    let ranged = tree
        .range_scan(Some(&20_u32.to_be_bytes()), Some(&30_u32.to_be_bytes()))
        .expect("range scan")
        .map(|(k, _)| u32::from_be_bytes(k.try_into().expect("u32 key")))
        .collect::<Vec<_>>();

    let expected = (20_u32..=30_u32).collect::<Vec<_>>();
    assert_eq!(ranged, expected);

    tree.delete(&25_u32.to_be_bytes()).expect("delete");
    assert_eq!(tree.search(&25_u32.to_be_bytes()).expect("search"), None);
}

#[test]
fn btree_insert_same_key_overwrites_value() {
    let tree = make_tree(4);
    let key = 77_u32.to_be_bytes();
    tree.insert(&key, (1, 1)).expect("insert1");
    tree.insert(&key, (2, 2)).expect("insert2");
    assert_eq!(tree.search(&key).expect("search"), Some((2, 2)));
}

#[test]
fn btree_empty_result_when_range_is_outside_keys() {
    let tree = make_tree(4);
    for k in 10..20_u32 {
        tree.insert(&k.to_be_bytes(), (k, 0)).expect("insert");
    }
    let out = tree
        .range_scan(Some(&100_u32.to_be_bytes()), Some(&110_u32.to_be_bytes()))
        .expect("range")
        .collect::<Vec<_>>();
    assert!(out.is_empty());
}
