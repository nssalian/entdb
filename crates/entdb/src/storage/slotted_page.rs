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

use crate::storage::page::Page;
use crate::storage::tuple::SlotId;

const SLOTTED_HEADER_SIZE: usize = 4;
const SLOT_ENTRY_SIZE: usize = 4;
const SLOT_DELETED_OFFSET: u16 = 0;
const SLOT_DELETED_LENGTH: u16 = 0;

pub struct SlottedPage<'a> {
    page: &'a mut Page,
    data_start: usize,
}

impl<'a> SlottedPage<'a> {
    pub fn init(page: &'a mut Page) -> Self {
        Self::init_with_offset(page, 0)
    }

    pub fn init_with_offset(page: &'a mut Page, data_start: usize) -> Self {
        let mut slotted = Self { page, data_start };
        let region_len = slotted.region_len();
        slotted.set_num_slots(0);
        slotted.set_free_space_offset(region_len as u16);
        slotted
    }

    pub fn from_page(page: &'a mut Page) -> Self {
        Self::from_page_with_offset(page, 0)
    }

    pub fn from_page_with_offset(page: &'a mut Page, data_start: usize) -> Self {
        Self { page, data_start }
    }

    pub fn insert(&mut self, data: &[u8]) -> Option<SlotId> {
        let required_data = data.len();
        let reuse_slot = self.first_deleted_slot();

        let required_meta = if reuse_slot.is_some() {
            0
        } else {
            SLOT_ENTRY_SIZE
        };
        if self.free_space() < required_data + required_meta {
            return None;
        }

        let free_off = self.free_space_offset() as usize;
        let new_off = free_off.checked_sub(required_data)?;
        {
            let body = self.region_mut();
            body[new_off..new_off + required_data].copy_from_slice(data);
        }
        self.set_free_space_offset(new_off as u16);

        let slot_id = if let Some(existing) = reuse_slot {
            existing
        } else {
            let slot = self.num_slots();
            self.set_num_slots(slot + 1);
            slot
        };

        self.set_slot(slot_id, new_off as u16, required_data as u16);
        Some(slot_id)
    }

    pub fn delete(&mut self, slot_id: SlotId) -> bool {
        if usize::from(slot_id) >= usize::from(self.num_slots()) {
            return false;
        }
        self.set_slot(slot_id, SLOT_DELETED_OFFSET, SLOT_DELETED_LENGTH);
        true
    }

    pub fn get(&self, slot_id: SlotId) -> Option<&[u8]> {
        if usize::from(slot_id) >= usize::from(self.num_slots()) {
            return None;
        }

        let (offset, len) = self.slot(slot_id);
        if len == SLOT_DELETED_LENGTH {
            return None;
        }

        let start = offset as usize;
        let end = start + len as usize;
        let body = self.region();
        if end > body.len() {
            return None;
        }

        Some(&body[start..end])
    }

    pub fn update(&mut self, slot_id: SlotId, data: &[u8]) -> bool {
        if usize::from(slot_id) >= usize::from(self.num_slots()) {
            return false;
        }

        let (offset, len) = self.slot(slot_id);
        if len == SLOT_DELETED_LENGTH {
            return false;
        }

        if data.len() <= len as usize {
            let start = offset as usize;
            let end = start + data.len();
            self.region_mut()[start..end].copy_from_slice(data);
            self.set_slot(slot_id, offset, data.len() as u16);
            return true;
        }

        if self.free_space() < data.len() - len as usize {
            return false;
        }

        let free_off = self.free_space_offset() as usize;
        let new_off = match free_off.checked_sub(data.len()) {
            Some(v) => v,
            None => return false,
        };

        self.region_mut()[new_off..new_off + data.len()].copy_from_slice(data);
        self.set_free_space_offset(new_off as u16);
        self.set_slot(slot_id, new_off as u16, data.len() as u16);
        true
    }

    pub fn free_space(&self) -> usize {
        let slot_dir_end = SLOTTED_HEADER_SIZE + usize::from(self.num_slots()) * SLOT_ENTRY_SIZE;
        let free_off = usize::from(self.free_space_offset());
        free_off.saturating_sub(slot_dir_end)
    }

    pub fn compact(&mut self) {
        let slots = self.num_slots();
        let region_len = self.region_len();
        let mut write_head = region_len;

        for slot_idx in 0..slots {
            let (offset, len) = self.slot(slot_idx);
            if len == SLOT_DELETED_LENGTH {
                continue;
            }

            let start = offset as usize;
            let end = start + len as usize;
            if end > region_len {
                self.set_slot(slot_idx, SLOT_DELETED_OFFSET, SLOT_DELETED_LENGTH);
                continue;
            }

            write_head -= len as usize;
            let bytes = {
                let body = self.region();
                body[start..end].to_vec()
            };
            self.region_mut()[write_head..write_head + len as usize].copy_from_slice(&bytes);
            self.set_slot(slot_idx, write_head as u16, len);
        }

        self.set_free_space_offset(write_head as u16);
    }

    pub fn num_slots(&self) -> u16 {
        let body = self.region();
        u16::from_le_bytes(body[0..2].try_into().expect("num slots bytes"))
    }

    fn free_space_offset(&self) -> u16 {
        let body = self.region();
        u16::from_le_bytes(body[2..4].try_into().expect("free offset bytes"))
    }

    fn set_num_slots(&mut self, num_slots: u16) {
        self.region_mut()[0..2].copy_from_slice(&num_slots.to_le_bytes());
    }

    fn set_free_space_offset(&mut self, free_off: u16) {
        self.region_mut()[2..4].copy_from_slice(&free_off.to_le_bytes());
    }

    fn slot(&self, slot_id: SlotId) -> (u16, u16) {
        let slot_base = SLOTTED_HEADER_SIZE + usize::from(slot_id) * SLOT_ENTRY_SIZE;
        let body = self.region();
        let offset = u16::from_le_bytes(
            body[slot_base..slot_base + 2]
                .try_into()
                .expect("slot offset bytes"),
        );
        let len = u16::from_le_bytes(
            body[slot_base + 2..slot_base + 4]
                .try_into()
                .expect("slot len bytes"),
        );
        (offset, len)
    }

    fn set_slot(&mut self, slot_id: SlotId, offset: u16, len: u16) {
        let slot_base = SLOTTED_HEADER_SIZE + usize::from(slot_id) * SLOT_ENTRY_SIZE;
        let body = self.region_mut();
        body[slot_base..slot_base + 2].copy_from_slice(&offset.to_le_bytes());
        body[slot_base + 2..slot_base + 4].copy_from_slice(&len.to_le_bytes());
    }

    fn first_deleted_slot(&self) -> Option<SlotId> {
        for slot_id in 0..self.num_slots() {
            let (_, len) = self.slot(slot_id);
            if len == SLOT_DELETED_LENGTH {
                return Some(slot_id);
            }
        }
        None
    }

    fn region(&self) -> &[u8] {
        &self.page.body()[self.data_start..]
    }

    fn region_mut(&mut self) -> &mut [u8] {
        &mut self.page.body_mut()[self.data_start..]
    }

    fn region_len(&self) -> usize {
        self.page.body().len() - self.data_start
    }
}
