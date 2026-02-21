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
use crate::storage::buffer_pool::BufferPool;
use crate::storage::page::{PageId, INVALID_PAGE_ID};
use crate::storage::slotted_page::SlottedPage;
use crate::storage::tuple::{SlotId, Tuple, TupleId};
use std::sync::Arc;

const NEXT_PAGE_OFFSET: usize = 0;
const TABLE_DATA_START: usize = 4;

pub struct Table {
    pub table_id: u32,
    pub first_page_id: PageId,
    buffer_pool: Arc<BufferPool>,
}

impl Table {
    pub fn create(table_id: u32, buffer_pool: Arc<BufferPool>) -> Result<Self> {
        let first_page_id = {
            let mut first = buffer_pool.new_page()?;
            let pid = first.page_id();
            set_next_page_id(&mut first, INVALID_PAGE_ID);
            SlottedPage::init_with_offset(&mut first, TABLE_DATA_START);
            pid
        };

        Ok(Self {
            table_id,
            first_page_id,
            buffer_pool,
        })
    }

    pub fn open(table_id: u32, first_page_id: PageId, buffer_pool: Arc<BufferPool>) -> Self {
        Self {
            table_id,
            first_page_id,
            buffer_pool,
        }
    }

    pub fn insert(&self, tuple: &Tuple) -> Result<TupleId> {
        let mut current_page_id = self.first_page_id;

        loop {
            let mut page = self.buffer_pool.fetch_page(current_page_id)?;
            let mut slotted = SlottedPage::from_page_with_offset(&mut page, TABLE_DATA_START);
            if let Some(slot_id) = slotted.insert(&tuple.data) {
                return Ok((current_page_id, slot_id));
            }

            let next = get_next_page_id(&page);
            if next != INVALID_PAGE_ID {
                current_page_id = next;
                continue;
            }

            let new_page_id = {
                let mut new_page = self.buffer_pool.new_page()?;
                let pid = new_page.page_id();
                set_next_page_id(&mut new_page, INVALID_PAGE_ID);
                SlottedPage::init_with_offset(&mut new_page, TABLE_DATA_START);
                pid
            };

            set_next_page_id(&mut page, new_page_id);
            current_page_id = new_page_id;
        }
    }

    pub fn delete(&self, tuple_id: TupleId) -> Result<()> {
        let (page_id, slot_id) = tuple_id;
        let mut page = self.buffer_pool.fetch_page(page_id)?;
        let mut slotted = SlottedPage::from_page_with_offset(&mut page, TABLE_DATA_START);
        if slotted.delete(slot_id) {
            Ok(())
        } else {
            Err(EntDbError::PageNotFound(page_id))
        }
    }

    pub fn update(&self, tuple_id: TupleId, tuple: &Tuple) -> Result<()> {
        let (page_id, slot_id) = tuple_id;
        let mut page = self.buffer_pool.fetch_page(page_id)?;
        let mut slotted = SlottedPage::from_page_with_offset(&mut page, TABLE_DATA_START);
        if slotted.update(slot_id, &tuple.data) {
            Ok(())
        } else {
            Err(EntDbError::InvalidPage(format!(
                "cannot update tuple at ({page_id}, {slot_id})"
            )))
        }
    }

    pub fn compare_and_update(
        &self,
        tuple_id: TupleId,
        expected: &[u8],
        tuple: &Tuple,
    ) -> Result<bool> {
        let (page_id, slot_id) = tuple_id;
        let mut page = self.buffer_pool.fetch_page(page_id)?;
        let mut slotted = SlottedPage::from_page_with_offset(&mut page, TABLE_DATA_START);
        let Some(current) = slotted.get(slot_id) else {
            return Ok(false);
        };
        if current != expected {
            return Ok(false);
        }
        Ok(slotted.update(slot_id, &tuple.data))
    }

    pub fn get(&self, tuple_id: TupleId) -> Result<Tuple> {
        let (page_id, slot_id) = tuple_id;
        let mut page = self.buffer_pool.fetch_page(page_id)?;
        let slotted = SlottedPage::from_page_with_offset(&mut page, TABLE_DATA_START);
        let data = slotted
            .get(slot_id)
            .ok_or_else(|| EntDbError::PageNotFound(page_id))?;
        Ok(Tuple::new(data.to_vec()))
    }

    pub fn scan(&self) -> TableIterator {
        TableIterator {
            buffer_pool: Arc::clone(&self.buffer_pool),
            current_page_id: Some(self.first_page_id),
            current_slot_id: 0,
        }
    }
}

pub struct TableIterator {
    buffer_pool: Arc<BufferPool>,
    current_page_id: Option<PageId>,
    current_slot_id: SlotId,
}

impl Iterator for TableIterator {
    type Item = (TupleId, Tuple);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let page_id = self.current_page_id?;
            let mut page = self.buffer_pool.fetch_page(page_id).ok()?;
            let slotted = SlottedPage::from_page_with_offset(&mut page, TABLE_DATA_START);
            let num_slots = slotted.num_slots();

            while self.current_slot_id < num_slots {
                let slot_id = self.current_slot_id;
                self.current_slot_id += 1;

                if let Some(data) = slotted.get(slot_id) {
                    return Some(((page_id, slot_id), Tuple::new(data.to_vec())));
                }
            }

            let next_page = get_next_page_id(&page);
            if next_page == INVALID_PAGE_ID {
                self.current_page_id = None;
                return None;
            }

            self.current_page_id = Some(next_page);
            self.current_slot_id = 0;
        }
    }
}

fn get_next_page_id(page: &crate::storage::page::Page) -> PageId {
    u32::from_le_bytes(
        page.body()[NEXT_PAGE_OFFSET..NEXT_PAGE_OFFSET + 4]
            .try_into()
            .expect("next page id bytes"),
    )
}

fn set_next_page_id(page: &mut crate::storage::page::Page, next: PageId) {
    page.body_mut()[NEXT_PAGE_OFFSET..NEXT_PAGE_OFFSET + 4].copy_from_slice(&next.to_le_bytes());
}
