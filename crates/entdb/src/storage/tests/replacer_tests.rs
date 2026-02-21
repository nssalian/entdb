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

use crate::storage::replacer::LruKReplacer;

#[test]
fn lruk_prefers_frames_with_less_than_k_history() {
    let mut replacer = LruKReplacer::new(10, 2);

    replacer.record_access(1);
    replacer.record_access(2);
    replacer.record_access(2);

    replacer.set_evictable(1, true);
    replacer.set_evictable(2, true);

    let evicted = replacer.evict().expect("expected one frame");
    assert_eq!(evicted, 1);
}

#[test]
fn lruk_uses_oldest_kth_access_for_eviction() {
    let mut replacer = LruKReplacer::new(10, 2);

    replacer.record_access(3); // t1
    replacer.record_access(4); // t2
    replacer.record_access(3); // t3 -> kth for 3 = t1
    replacer.record_access(4); // t4 -> kth for 4 = t2

    replacer.set_evictable(3, true);
    replacer.set_evictable(4, true);

    let evicted = replacer.evict().expect("expected one frame");
    assert_eq!(evicted, 3);
}

#[test]
fn set_evictable_and_remove_update_size() {
    let mut replacer = LruKReplacer::new(5, 2);
    replacer.record_access(0);
    replacer.record_access(1);

    replacer.set_evictable(0, true);
    replacer.set_evictable(1, true);
    assert_eq!(replacer.size(), 2);

    replacer.set_evictable(1, false);
    assert_eq!(replacer.size(), 1);

    replacer.remove(0);
    assert_eq!(replacer.size(), 0);
}

#[test]
fn replacer_never_evicts_non_evictable_frames() {
    let mut replacer = LruKReplacer::new(3, 2);
    replacer.record_access(0);
    replacer.record_access(1);
    replacer.record_access(2);
    replacer.set_evictable(0, true);
    replacer.set_evictable(1, false);
    replacer.set_evictable(2, false);

    assert_eq!(replacer.evict(), Some(0));
    assert_eq!(replacer.evict(), None);
}
