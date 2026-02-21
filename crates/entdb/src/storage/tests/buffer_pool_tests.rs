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

use crate::storage::buffer_pool::BufferPool;
use crate::storage::disk_manager::DiskManager;
use crate::wal::log_manager::LogManager;
use crate::wal::log_record::LogRecord;
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

#[test]
fn fetch_unpin_and_flush_round_trip() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("entdb.data");

    let dm = Arc::new(DiskManager::new(&path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(2, Arc::clone(&dm)));

    let page_id;
    {
        let mut guard = bp.new_page().expect("new page");
        page_id = guard.page_id();
        guard.body_mut()[0..5].copy_from_slice(b"hello");
    }

    bp.flush_page(page_id).expect("flush page");

    let mut loaded = crate::storage::page::Page::default();
    dm.read_page(page_id, &mut loaded).expect("read back");
    assert_eq!(&loaded.body()[0..5], b"hello");
}

#[test]
fn eviction_under_pressure_persists_dirty_pages() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("entdb.data");

    let dm = Arc::new(DiskManager::new(&path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(1, Arc::clone(&dm)));

    let page_a;
    {
        let mut a = bp.new_page().expect("new page a");
        page_a = a.page_id();
        a.body_mut()[0..3].copy_from_slice(b"aaa");
    }

    let b = bp.new_page().expect("new page b triggers eviction");
    drop(b);
    bp.flush_all().expect("flush all");

    let mut loaded = crate::storage::page::Page::default();
    dm.read_page(page_a, &mut loaded).expect("read page a");
    assert_eq!(&loaded.body()[0..3], b"aaa");
}

#[test]
fn concurrent_access_fetches_same_page_safely() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("entdb.data");

    let dm = Arc::new(DiskManager::new(&path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(4, Arc::clone(&dm)));

    let page_id;
    {
        let mut page = bp.new_page().expect("new page");
        page_id = page.page_id();
        page.body_mut()[0] = 1;
    }
    bp.flush_page(page_id).expect("flush initialized page");

    let mut handles = Vec::new();
    for _ in 0..8 {
        let bp_clone = Arc::clone(&bp);
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                let mut guard = bp_clone.fetch_page(page_id).expect("fetch page");
                guard.body_mut()[0] = guard.body()[0].wrapping_add(1);
            }
        }));
    }

    for handle in handles {
        handle.join().expect("thread join");
    }

    bp.flush_page(page_id).expect("flush final value");
    let mut loaded = crate::storage::page::Page::default();
    dm.read_page(page_id, &mut loaded).expect("read final page");

    let increments = (8_usize * 100) as u8;
    assert_eq!(loaded.body()[0], 1_u8.wrapping_add(increments));
}

#[test]
fn buffer_pool_full_when_all_frames_pinned() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("entdb.data");
    let dm = Arc::new(DiskManager::new(&path).expect("disk manager"));
    let bp = BufferPool::new(1, Arc::clone(&dm));

    let _guard = bp.new_page().expect("new page");
    match bp.new_page() {
        Ok(_) => panic!("expected buffer pool full error"),
        Err(err) => assert!(err.to_string().contains("buffer pool full")),
    };
}

#[test]
fn delete_pinned_page_fails() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("entdb.data");
    let dm = Arc::new(DiskManager::new(&path).expect("disk manager"));
    let bp = BufferPool::new(2, Arc::clone(&dm));

    let guard = bp.new_page().expect("new page");
    let pid = guard.page_id();
    let err = bp.delete_page(pid).expect_err("delete pinned should fail");
    assert!(err.to_string().contains("pinned"));
    drop(guard);
}

#[test]
fn flush_page_obeys_wal_when_log_manager_attached() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("entdb.data");
    let wal_path = dir.path().join("entdb.wal");
    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let lm = Arc::new(LogManager::new(&wal_path, 1024).expect("log manager"));
    let bp = BufferPool::with_log_manager(2, Arc::clone(&dm), Arc::clone(&lm));

    let lsn = lm
        .append(LogRecord::Begin { txn_id: 10 })
        .expect("append begin");
    let pid = {
        let mut guard = bp.new_page().expect("new page");
        guard.set_lsn(lsn as u32);
        guard.body_mut()[0] = 1;
        guard.page_id()
    };

    assert_eq!(lm.flushed_lsn(), 0);
    bp.flush_page(pid).expect("flush page");
    assert!(lm.flushed_lsn() >= lsn);
}

#[test]
fn buffer_pool_stats_reflect_cache_pin_and_dirty_counts() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("entdb.data");
    let dm = Arc::new(DiskManager::new(&path).expect("disk manager"));
    let bp = BufferPool::new(2, Arc::clone(&dm));

    let first = bp.new_page().expect("new page");
    let stats_while_pinned = bp.stats();
    assert_eq!(stats_while_pinned.pool_size, 2);
    assert_eq!(stats_while_pinned.cached_pages, 1);
    assert_eq!(stats_while_pinned.pinned_frames, 1);
    assert_eq!(stats_while_pinned.dirty_frames, 1);
    assert_eq!(stats_while_pinned.free_frames, 1);
    drop(first);

    bp.flush_all().expect("flush all");
    let stats_after_flush = bp.stats();
    assert_eq!(stats_after_flush.cached_pages, 1);
    assert_eq!(stats_after_flush.pinned_frames, 0);
    assert_eq!(stats_after_flush.dirty_frames, 0);
}
