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
use crate::storage::disk_manager::DiskManager;
use crate::storage::page::{Page, PageId};
use crate::storage::replacer::{FrameId, LruKReplacer};
use crate::wal::log_manager::LogManager;
use parking_lot::{Mutex, MutexGuard, RwLock};
use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

struct Frame {
    page: Page,
    page_id: Option<PageId>,
    pin_count: u32,
    is_dirty: bool,
}

impl Frame {
    fn empty() -> Self {
        Self {
            page: Page::default(),
            page_id: None,
            pin_count: 0,
            is_dirty: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferPoolStats {
    pub pool_size: usize,
    pub cached_pages: usize,
    pub pinned_frames: usize,
    pub dirty_frames: usize,
    pub free_frames: usize,
}

pub struct BufferPool {
    pool_size: usize,
    frames: Vec<Mutex<Frame>>,
    page_table: RwLock<HashMap<PageId, FrameId>>,
    free_list: Mutex<VecDeque<FrameId>>,
    replacer: Mutex<LruKReplacer>,
    disk_manager: Arc<DiskManager>,
    log_manager: Option<Arc<LogManager>>,
}

impl BufferPool {
    pub fn new(pool_size: usize, disk_manager: Arc<DiskManager>) -> Self {
        Self::new_inner(pool_size, disk_manager, None)
    }

    pub fn with_log_manager(
        pool_size: usize,
        disk_manager: Arc<DiskManager>,
        log_manager: Arc<LogManager>,
    ) -> Self {
        Self::new_inner(pool_size, disk_manager, Some(log_manager))
    }

    fn new_inner(
        pool_size: usize,
        disk_manager: Arc<DiskManager>,
        log_manager: Option<Arc<LogManager>>,
    ) -> Self {
        let mut free_list = VecDeque::with_capacity(pool_size);
        let mut frames = Vec::with_capacity(pool_size);
        for frame_id in 0..pool_size {
            free_list.push_back(frame_id);
            frames.push(Mutex::new(Frame::empty()));
        }

        Self {
            pool_size,
            frames,
            page_table: RwLock::new(HashMap::with_capacity(pool_size)),
            free_list: Mutex::new(free_list),
            replacer: Mutex::new(LruKReplacer::new(pool_size, 2)),
            disk_manager,
            log_manager,
        }
    }

    pub fn fetch_page(&self, page_id: PageId) -> Result<PageGuard<'_>> {
        if let Some(frame_id) = self.page_table.read().get(&page_id).copied() {
            let mut frame = self.frames[frame_id].lock();
            if frame.page_id != Some(page_id) {
                return Err(EntDbError::Corruption(format!(
                    "page table points to mismatched frame for page {page_id}"
                )));
            }

            frame.pin_count = frame.pin_count.saturating_add(1);
            {
                let mut replacer = self.replacer.lock();
                replacer.record_access(frame_id);
                replacer.set_evictable(frame_id, false);
            }

            return Ok(PageGuard {
                buffer_pool: self,
                frame_id,
                page_id,
                frame,
                is_dirty: false,
            });
        }

        let frame_id = self.acquire_frame_for_page(page_id)?;
        let mut frame = self.frames[frame_id].lock();
        frame.pin_count = 1;
        frame.is_dirty = false;
        frame.page_id = Some(page_id);
        self.disk_manager.read_page(page_id, &mut frame.page)?;

        {
            let mut page_table = self.page_table.write();
            if page_table.insert(page_id, frame_id).is_some() {
                return Err(EntDbError::PageAlreadyPresent(page_id));
            }
        }

        {
            let mut replacer = self.replacer.lock();
            replacer.record_access(frame_id);
            replacer.set_evictable(frame_id, false);
        }

        Ok(PageGuard {
            buffer_pool: self,
            frame_id,
            page_id,
            frame,
            is_dirty: false,
        })
    }

    pub fn new_page(&self) -> Result<PageGuard<'_>> {
        let page_id = self.disk_manager.allocate_page()?;
        let frame_id = self.acquire_frame_for_page(page_id)?;
        let mut frame = self.frames[frame_id].lock();

        frame.page = Page::new(page_id);
        frame.page_id = Some(page_id);
        frame.pin_count = 1;
        frame.is_dirty = true;

        {
            let mut page_table = self.page_table.write();
            if page_table.insert(page_id, frame_id).is_some() {
                return Err(EntDbError::PageAlreadyPresent(page_id));
            }
        }

        {
            let mut replacer = self.replacer.lock();
            replacer.record_access(frame_id);
            replacer.set_evictable(frame_id, false);
        }

        Ok(PageGuard {
            buffer_pool: self,
            frame_id,
            page_id,
            frame,
            is_dirty: true,
        })
    }

    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> Result<()> {
        let frame_id = self
            .page_table
            .read()
            .get(&page_id)
            .copied()
            .ok_or(EntDbError::PageNotFound(page_id))?;

        let mut frame = self.frames[frame_id].lock();
        if frame.pin_count == 0 {
            return Err(EntDbError::PagePinned(page_id));
        }

        if is_dirty {
            frame.is_dirty = true;
        }

        frame.pin_count -= 1;
        if frame.pin_count == 0 {
            self.replacer.lock().set_evictable(frame_id, true);
        }
        Ok(())
    }

    pub fn flush_page(&self, page_id: PageId) -> Result<()> {
        let frame_id = self
            .page_table
            .read()
            .get(&page_id)
            .copied()
            .ok_or(EntDbError::PageNotFound(page_id))?;

        let mut frame = self.frames[frame_id].lock();
        if frame.page_id != Some(page_id) {
            return Err(EntDbError::Corruption(format!(
                "frame-page mismatch on flush for page {page_id}"
            )));
        }

        if frame.is_dirty {
            if let Some(log_manager) = &self.log_manager {
                log_manager.flush_up_to(frame.page.lsn() as u64)?;
            }
            frame.page.refresh_checksum();
            self.disk_manager.write_page(page_id, &frame.page)?;
            frame.is_dirty = false;
        }

        Ok(())
    }

    pub fn flush_all(&self) -> Result<()> {
        let page_ids = self
            .page_table
            .read()
            .keys()
            .copied()
            .collect::<Vec<PageId>>();

        for page_id in page_ids {
            self.flush_page(page_id)?;
        }

        self.disk_manager.sync()?;
        Ok(())
    }

    pub fn delete_page(&self, page_id: PageId) -> Result<()> {
        let frame_id_opt = self.page_table.read().get(&page_id).copied();
        if let Some(frame_id) = frame_id_opt {
            let Some(frame) = self.frames[frame_id].try_lock() else {
                return Err(EntDbError::PagePinned(page_id));
            };
            if frame.pin_count > 0 {
                return Err(EntDbError::PagePinned(page_id));
            }
            drop(frame);

            self.page_table.write().remove(&page_id);
            let mut frame = self.frames[frame_id].lock();

            frame.page = Page::default();
            frame.page_id = None;
            frame.is_dirty = false;

            self.replacer.lock().remove(frame_id);
            self.free_list.lock().push_back(frame_id);
        }

        self.disk_manager.deallocate_page(page_id)
    }

    pub fn pool_size(&self) -> usize {
        self.pool_size
    }

    pub fn stats(&self) -> BufferPoolStats {
        let free_frames = self.free_list.lock().len();
        let cached_pages = self.page_table.read().len();
        let mut pinned_frames = 0usize;
        let mut dirty_frames = 0usize;

        for frame in &self.frames {
            if let Some(frame) = frame.try_lock() {
                if frame.pin_count > 0 {
                    pinned_frames += 1;
                }
                if frame.is_dirty {
                    dirty_frames += 1;
                }
            } else {
                // A locked frame is actively guarded/mutated; conservatively treat it as pinned+dirty
                // for pressure telemetry to avoid under-reporting while avoiding lock inversion.
                pinned_frames += 1;
                dirty_frames += 1;
            }
        }

        BufferPoolStats {
            pool_size: self.pool_size,
            cached_pages,
            pinned_frames,
            dirty_frames,
            free_frames,
        }
    }

    pub fn disk_path(&self) -> &std::path::Path {
        self.disk_manager.db_path()
    }

    fn acquire_frame_for_page(&self, incoming_page_id: PageId) -> Result<FrameId> {
        if let Some(frame_id) = self.free_list.lock().pop_front() {
            return Ok(frame_id);
        }

        let victim = self
            .replacer
            .lock()
            .evict()
            .ok_or(EntDbError::BufferPoolFull)?;

        let mut frame = self.frames[victim].lock();
        if frame.pin_count > 0 {
            return Err(EntDbError::BufferPoolFull);
        }

        if let Some(old_page_id) = frame.page_id {
            self.page_table.write().remove(&old_page_id);
            if frame.is_dirty {
                frame.page.refresh_checksum();
                self.disk_manager.write_page(old_page_id, &frame.page)?;
                frame.is_dirty = false;
            }
        }

        frame.page = Page::new(incoming_page_id);
        frame.page_id = None;
        frame.pin_count = 0;
        frame.is_dirty = false;
        drop(frame);

        Ok(victim)
    }
}

pub struct PageGuard<'a> {
    buffer_pool: &'a BufferPool,
    frame_id: FrameId,
    page_id: PageId,
    frame: MutexGuard<'a, Frame>,
    is_dirty: bool,
}

impl PageGuard<'_> {
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn mark_dirty(&mut self) {
        self.is_dirty = true;
    }
}

impl Deref for PageGuard<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        &self.frame.page
    }
}

impl DerefMut for PageGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.is_dirty = true;
        &mut self.frame.page
    }
}

impl Drop for PageGuard<'_> {
    fn drop(&mut self) {
        if self.is_dirty {
            self.frame.is_dirty = true;
        }

        if self.frame.pin_count > 0 {
            self.frame.pin_count -= 1;
            if self.frame.pin_count == 0 {
                self.buffer_pool
                    .replacer
                    .lock()
                    .set_evictable(self.frame_id, true);
            }
        }
    }
}
