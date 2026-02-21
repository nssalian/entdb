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

use crate::storage::disk_manager::DiskManager;
use crate::storage::page::Page;
use tempfile::tempdir;

#[test]
fn disk_manager_write_read_reopen_persistence() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("entdb.data");

    let manager = DiskManager::new(&path).expect("create disk manager");
    let pid = manager.allocate_page().expect("allocate page");

    let mut page = Page::new(pid);
    page.body_mut()[0..11].copy_from_slice(b"hello world");
    page.refresh_checksum();

    manager.write_page(pid, &page).expect("write page");
    manager.sync().expect("sync");

    drop(manager);

    let manager = DiskManager::new(&path).expect("reopen disk manager");
    let mut loaded = Page::default();
    manager.read_page(pid, &mut loaded).expect("read page");

    assert_eq!(&loaded.body()[0..11], b"hello world");
    assert!(loaded.verify_checksum());
}

#[test]
fn disk_manager_reuses_deallocated_pages() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("entdb.data");

    let manager = DiskManager::new(&path).expect("create disk manager");
    let p1 = manager.allocate_page().expect("allocate p1");
    let p2 = manager.allocate_page().expect("allocate p2");

    manager.deallocate_page(p1).expect("deallocate p1");
    let p3 = manager.allocate_page().expect("allocate p3");

    assert_eq!(p2, 2);
    assert_eq!(p3, p1);
}

#[test]
fn disk_manager_out_of_bounds_returns_error() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("entdb.data");
    let manager = DiskManager::new(&path).expect("create disk manager");

    let mut page = Page::default();
    let read_err = manager
        .read_page(9999, &mut page)
        .expect_err("read should fail");
    assert!(read_err.to_string().contains("not found"));

    let write_err = manager
        .write_page(9999, &Page::new(9999))
        .expect_err("write should fail");
    assert!(write_err.to_string().contains("not found"));
}

#[test]
fn free_list_persists_across_reopen() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("entdb.data");
    let manager = DiskManager::new(&path).expect("create disk manager");

    let p1 = manager.allocate_page().expect("allocate p1");
    let _p2 = manager.allocate_page().expect("allocate p2");
    manager.deallocate_page(p1).expect("deallocate p1");
    manager.sync().expect("sync");
    drop(manager);

    let manager = DiskManager::new(&path).expect("reopen");
    let reused = manager.allocate_page().expect("allocate reused");
    assert_eq!(reused, p1);
}
