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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

pub type FrameId = usize;

pub struct LruKReplacer {
    k: usize,
    max_frames: usize,
    access_history: HashMap<FrameId, VecDeque<u64>>,
    evictable: HashSet<FrameId>,
    current_timestamp: AtomicU64,
}

impl LruKReplacer {
    pub fn new(max_frames: usize, k: usize) -> Self {
        Self {
            k: k.max(1),
            max_frames,
            access_history: HashMap::new(),
            evictable: HashSet::new(),
            current_timestamp: AtomicU64::new(1),
        }
    }

    pub fn record_access(&mut self, frame_id: FrameId) {
        if frame_id >= self.max_frames {
            return;
        }
        let ts = self.current_timestamp.fetch_add(1, Ordering::SeqCst);
        let history = self.access_history.entry(frame_id).or_default();
        history.push_back(ts);
        while history.len() > self.k {
            history.pop_front();
        }
    }

    pub fn evict(&mut self) -> Option<FrameId> {
        let mut best: Option<(FrameId, bool, u64)> = None;

        for frame_id in self.evictable.iter().copied() {
            let history = match self.access_history.get(&frame_id) {
                Some(h) if !h.is_empty() => h,
                _ => continue,
            };

            let has_k = history.len() >= self.k;
            let score_time = if has_k {
                history[0]
            } else {
                *history.back().expect("history not empty")
            };

            match best {
                None => best = Some((frame_id, has_k, score_time)),
                Some((_, best_has_k, best_time)) => {
                    let better =
                        (!has_k && best_has_k) || (has_k == best_has_k && score_time < best_time);
                    if better {
                        best = Some((frame_id, has_k, score_time));
                    }
                }
            }
        }

        best.map(|(frame_id, _, _)| {
            self.evictable.remove(&frame_id);
            frame_id
        })
    }

    pub fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        if frame_id >= self.max_frames {
            return;
        }
        if evictable {
            self.evictable.insert(frame_id);
        } else {
            self.evictable.remove(&frame_id);
        }
    }

    pub fn remove(&mut self, frame_id: FrameId) {
        self.evictable.remove(&frame_id);
        self.access_history.remove(&frame_id);
    }

    pub fn size(&self) -> usize {
        self.evictable.len()
    }
}
