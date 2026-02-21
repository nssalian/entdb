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
use crate::storage::slotted_page::SlottedPage;

#[test]
fn slotted_page_insert_delete_compact_reuse() {
    let mut page = Page::new(1);
    let mut slotted = SlottedPage::init(&mut page);

    let s1 = slotted.insert(b"alpha").expect("insert alpha");
    let s2 = slotted.insert(b"beta").expect("insert beta");
    let s3 = slotted.insert(b"gamma").expect("insert gamma");

    assert_eq!(slotted.get(s1), Some(&b"alpha"[..]));
    assert_eq!(slotted.get(s2), Some(&b"beta"[..]));
    assert_eq!(slotted.get(s3), Some(&b"gamma"[..]));

    assert!(slotted.delete(s2));
    assert_eq!(slotted.get(s2), None);

    slotted.compact();

    let s4 = slotted.insert(b"delta").expect("insert delta");
    assert_eq!(s4, s2, "deleted slot should be reused");
    assert_eq!(slotted.get(s4), Some(&b"delta"[..]));
}

#[test]
fn slotted_page_fills_until_full() {
    let mut page = Page::new(2);
    let mut slotted = SlottedPage::init(&mut page);

    let payload = vec![0xAB; 128];
    let mut inserted = 0;
    while slotted.insert(&payload).is_some() {
        inserted += 1;
    }

    assert!(inserted > 0);
    assert!(slotted.free_space() < payload.len() + 4);
}

#[test]
fn slotted_page_update_moves_record_when_growing() {
    let mut page = Page::new(3);
    let mut slotted = SlottedPage::init(&mut page);
    let slot = slotted.insert(b"tiny").expect("insert");
    assert!(slotted.update(slot, b"this-is-a-larger-record"));
    assert_eq!(slotted.get(slot), Some(&b"this-is-a-larger-record"[..]));
}

#[test]
fn slotted_page_invalid_slot_returns_none_or_false() {
    let mut page = Page::new(4);
    let mut slotted = SlottedPage::init(&mut page);
    assert_eq!(slotted.get(1000), None);
    assert!(!slotted.delete(1000));
    assert!(!slotted.update(1000, b"x"));
}
