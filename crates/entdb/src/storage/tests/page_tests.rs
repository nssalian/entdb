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

use crate::storage::page::{Page, PageType, PAGE_HEADER_SIZE};

#[test]
fn page_header_round_trip_and_checksum() {
    let mut page = Page::new(42);
    page.set_lsn(1234);
    page.set_page_type(PageType::BTreeLeaf);
    page.body_mut()[0..4].copy_from_slice(&[1, 2, 3, 4]);
    page.refresh_checksum();

    assert_eq!(page.page_id(), 42);
    assert_eq!(page.lsn(), 1234);
    assert_eq!(page.page_type(), PageType::BTreeLeaf);
    assert!(page.verify_checksum());
    assert_eq!(&page.body()[0..4], &[1, 2, 3, 4]);
}

#[test]
fn checksum_detects_corruption() {
    let mut page = Page::new(7);
    page.body_mut()[100] = 0xAA;
    page.refresh_checksum();
    assert!(page.verify_checksum());

    page.data_mut()[PAGE_HEADER_SIZE + 100] ^= 0xFF;
    assert!(!page.verify_checksum());
}

#[test]
fn checksum_changes_when_header_fields_change() {
    let mut page = Page::new(9);
    page.refresh_checksum();
    let base = page.checksum();

    page.set_lsn(22);
    page.refresh_checksum();
    let with_new_lsn = page.checksum();
    assert_ne!(base, with_new_lsn);

    page.set_page_type(PageType::Overflow);
    page.refresh_checksum();
    let with_new_type = page.checksum();
    assert_ne!(with_new_lsn, with_new_type);
}
