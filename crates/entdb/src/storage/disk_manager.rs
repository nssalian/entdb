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

use crate::error::{EntDbError, Result};
use crate::fault;
use crate::storage::page::{Page, PageId, PAGE_SIZE};
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

const META_MAGIC: u32 = 0x454E_5444; // ENTD
const META_HEADER_SIZE: usize = 8;

#[derive(Debug, Default)]
struct AllocState {
    free_list: Vec<PageId>,
}

pub struct DiskManager {
    db_file: File,
    db_path: PathBuf,
    num_pages: AtomicU32,
    alloc_state: Mutex<AllocState>,
}

impl DiskManager {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let db_path = path.as_ref().to_path_buf();
        let db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&db_path)?;

        let mut num_pages = page_count_for_len(db_file.metadata()?.len()) as u32;
        if num_pages == 0 {
            num_pages = 1;
            let mut meta_page = Page::new(0);
            write_free_list_to_page(&mut meta_page, &[])?;
            db_file.write_at(meta_page.data(), 0)?;
            db_file.sync_all()?;
        }

        let mgr = Self {
            db_file,
            db_path,
            num_pages: AtomicU32::new(num_pages),
            alloc_state: Mutex::new(AllocState::default()),
        };

        let free_list = mgr.load_free_list()?;
        mgr.alloc_state.lock().free_list = free_list;
        Ok(mgr)
    }

    pub fn read_page(&self, page_id: PageId, page: &mut Page) -> Result<()> {
        if fault::should_fail("disk.read_page") {
            return Err(EntDbError::Io(std::io::Error::other(
                "failpoint: disk.read_page",
            )));
        }
        if page_id >= self.num_pages() {
            return Err(EntDbError::PageNotFound(page_id));
        }

        let offset = page_offset(page_id);
        let buf = page.data_mut();
        let mut read = 0;
        while read < PAGE_SIZE {
            let bytes = self
                .db_file
                .read_at(&mut buf[read..], offset + read as u64)?;
            if bytes == 0 {
                break;
            }
            read += bytes;
        }

        if read < PAGE_SIZE {
            for b in &mut buf[read..] {
                *b = 0;
            }
        }

        Ok(())
    }

    pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<()> {
        if fault::should_fail("disk.write_page") {
            return Err(EntDbError::Io(std::io::Error::other(
                "failpoint: disk.write_page",
            )));
        }
        if page_id >= self.num_pages() {
            return Err(EntDbError::PageNotFound(page_id));
        }

        let offset = page_offset(page_id);
        let mut written = 0;
        while written < PAGE_SIZE {
            if written == 0 && fault::should_fail("disk.partial_write") {
                let partial = PAGE_SIZE / 2;
                let _ = self.db_file.write_at(&page.data()[..partial], offset)?;
                return Err(EntDbError::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failpoint: disk.partial_write",
                )));
            }
            let bytes = self
                .db_file
                .write_at(&page.data()[written..], offset + written as u64)?;
            if bytes == 0 {
                return Err(EntDbError::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write full page",
                )));
            }
            written += bytes;
        }

        Ok(())
    }

    pub fn allocate_page(&self) -> Result<PageId> {
        if fault::should_fail("disk.allocate_page") {
            return Err(EntDbError::Io(std::io::Error::other(
                "failpoint: disk.allocate_page",
            )));
        }
        let mut state = self.alloc_state.lock();
        let page_id = match state.free_list.pop() {
            Some(pid) => pid,
            None => self.num_pages.fetch_add(1, Ordering::SeqCst),
        };
        self.persist_free_list(&state.free_list)?;
        Ok(page_id)
    }

    pub fn deallocate_page(&self, page_id: PageId) -> Result<()> {
        if page_id == 0 {
            return Ok(());
        }
        if page_id >= self.num_pages() {
            return Err(EntDbError::PageNotFound(page_id));
        }

        let mut state = self.alloc_state.lock();
        if !state.free_list.contains(&page_id) {
            state.free_list.push(page_id);
            self.persist_free_list(&state.free_list)?;
        }
        Ok(())
    }

    pub fn num_pages(&self) -> u32 {
        self.num_pages.load(Ordering::Acquire)
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn sync(&self) -> Result<()> {
        if fault::should_fail("disk.sync") {
            return Err(EntDbError::Io(std::io::Error::other(
                "failpoint: disk.sync",
            )));
        }
        self.db_file.sync_all()?;
        Ok(())
    }

    fn load_free_list(&self) -> Result<Vec<PageId>> {
        let mut meta = Page::default();
        self.read_page(0, &mut meta)?;
        read_free_list_from_page(&meta)
    }

    fn persist_free_list(&self, free_list: &[PageId]) -> Result<()> {
        let mut meta = Page::new(0);
        write_free_list_to_page(&mut meta, free_list)?;
        self.write_page(0, &meta)?;
        Ok(())
    }
}

fn page_count_for_len(len: u64) -> usize {
    if len == 0 {
        0
    } else {
        (len as usize).div_ceil(PAGE_SIZE)
    }
}

fn page_offset(page_id: PageId) -> u64 {
    page_id as u64 * PAGE_SIZE as u64
}

fn read_free_list_from_page(page: &Page) -> Result<Vec<PageId>> {
    let body = page.body();
    let magic = u32::from_le_bytes(body[0..4].try_into().expect("meta magic"));
    if magic != META_MAGIC {
        // Backward-compatible fallback: treat unknown page 0 as empty free list.
        return Ok(Vec::new());
    }

    let free_count = u32::from_le_bytes(body[4..8].try_into().expect("meta free count")) as usize;
    let total_bytes = META_HEADER_SIZE + free_count * 4;
    if total_bytes > body.len() {
        return Err(EntDbError::Corruption(
            "metadata free list exceeds page body".to_string(),
        ));
    }

    let mut free = Vec::with_capacity(free_count);
    for i in 0..free_count {
        let off = META_HEADER_SIZE + i * 4;
        free.push(u32::from_le_bytes(
            body[off..off + 4].try_into().expect("free page id"),
        ));
    }
    Ok(free)
}

fn write_free_list_to_page(page: &mut Page, free_list: &[PageId]) -> Result<()> {
    let body = page.body_mut();
    for b in body.iter_mut() {
        *b = 0;
    }

    let required = META_HEADER_SIZE + free_list.len() * 4;
    if required > body.len() {
        return Err(EntDbError::InvalidPage(
            "free list metadata overflow on page 0".to_string(),
        ));
    }

    body[0..4].copy_from_slice(&META_MAGIC.to_le_bytes());
    body[4..8].copy_from_slice(&(free_list.len() as u32).to_le_bytes());
    for (idx, page_id) in free_list.iter().enumerate() {
        let off = META_HEADER_SIZE + idx * 4;
        body[off..off + 4].copy_from_slice(&page_id.to_le_bytes());
    }

    page.refresh_checksum();
    Ok(())
}
