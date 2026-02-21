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

use crc32fast::Hasher;

pub const PAGE_SIZE: usize = 4096;
pub const PAGE_HEADER_SIZE: usize = 16;
pub type PageId = u32;
pub const INVALID_PAGE_ID: PageId = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum PageType {
    Free = 0,
    BTreeInternal = 1,
    BTreeLeaf = 2,
    Overflow = 3,
}

impl TryFrom<u32> for PageType {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Free),
            1 => Ok(Self::BTreeInternal),
            2 => Ok(Self::BTreeLeaf),
            3 => Ok(Self::Overflow),
            _ => Err(()),
        }
    }
}

#[derive(Clone)]
pub struct Page {
    data: [u8; PAGE_SIZE],
}

impl Default for Page {
    fn default() -> Self {
        Self {
            data: [0_u8; PAGE_SIZE],
        }
    }
}

impl Page {
    pub fn new(page_id: PageId) -> Self {
        let mut page = Self::default();
        page.set_page_id(page_id);
        page.set_lsn(0);
        page.set_page_type(PageType::Free);
        page.refresh_checksum();
        page
    }

    pub fn from_bytes(bytes: [u8; PAGE_SIZE]) -> Self {
        Self { data: bytes }
    }

    pub fn page_id(&self) -> PageId {
        u32::from_le_bytes(self.data[0..4].try_into().expect("page_id bytes"))
    }

    pub fn set_page_id(&mut self, page_id: PageId) {
        self.data[0..4].copy_from_slice(&page_id.to_le_bytes());
    }

    pub fn lsn(&self) -> u32 {
        u32::from_le_bytes(self.data[4..8].try_into().expect("lsn bytes"))
    }

    pub fn set_lsn(&mut self, lsn: u32) {
        self.data[4..8].copy_from_slice(&lsn.to_le_bytes());
    }

    pub fn checksum(&self) -> u32 {
        u32::from_le_bytes(self.data[8..12].try_into().expect("checksum bytes"))
    }

    pub fn set_checksum(&mut self, checksum: u32) {
        self.data[8..12].copy_from_slice(&checksum.to_le_bytes());
    }

    pub fn page_type(&self) -> PageType {
        let raw = u32::from_le_bytes(self.data[12..16].try_into().expect("page_type bytes"));
        PageType::try_from(raw).unwrap_or(PageType::Free)
    }

    pub fn set_page_type(&mut self, page_type: PageType) {
        self.data[12..16].copy_from_slice(&(page_type as u32).to_le_bytes());
    }

    pub fn data(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.data
    }

    pub fn body(&self) -> &[u8] {
        &self.data[PAGE_HEADER_SIZE..]
    }

    pub fn body_mut(&mut self) -> &mut [u8] {
        &mut self.data[PAGE_HEADER_SIZE..]
    }

    pub fn compute_checksum(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&self.data[0..8]);
        hasher.update(&self.data[12..]);
        hasher.finalize()
    }

    pub fn refresh_checksum(&mut self) {
        let checksum = self.compute_checksum();
        self.set_checksum(checksum);
    }

    pub fn verify_checksum(&self) -> bool {
        self.checksum() == self.compute_checksum()
    }
}
