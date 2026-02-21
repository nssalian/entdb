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
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, RwLock};

const HISTORY_VERSION: u32 = 1;
const RECENT_KEEP_PER_FINGERPRINT: usize = 4;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OptimizerHistoryRecord {
    pub fingerprint: String,
    pub plan_signature: String,
    pub schema_hash: String,
    pub captured_at_ms: u64,
    pub rowcount_observed_json: String,
    pub latency_ms: u64,
    pub memory_peak_bytes: u64,
    pub success: bool,
    pub error_class: Option<String>,
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OptimizerHistoryState {
    version: u32,
    schema_hash: String,
    entries: Vec<OptimizerHistoryRecord>,
}

pub struct OptimizerHistoryStore {
    path: PathBuf,
    schema_hash: String,
    max_entries_per_fingerprint: usize,
    entries: Vec<OptimizerHistoryRecord>,
}

pub struct OptimizerHistoryRecorder {
    tx: mpsc::SyncSender<OptimizerHistoryRecord>,
    dropped: Arc<AtomicU64>,
    worker_errors: Arc<AtomicU64>,
    cache: Arc<RwLock<Vec<OptimizerHistoryRecord>>>,
    path: PathBuf,
    schema_hash: String,
    max_entries_per_fingerprint: usize,
}

impl OptimizerHistoryStore {
    pub fn load_or_create(
        path: impl AsRef<Path>,
        schema_hash: &str,
        max_entries_per_fingerprint: usize,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut store = Self {
            path,
            schema_hash: schema_hash.to_string(),
            max_entries_per_fingerprint: max_entries_per_fingerprint.max(4),
            entries: Vec::new(),
        };
        if store.path.exists() {
            let bytes = fs::read(&store.path)?;
            let state: OptimizerHistoryState = serde_json::from_slice(&bytes).map_err(|e| {
                EntDbError::Corruption(format!("invalid optimizer history file: {e}"))
            })?;
            if state.version == HISTORY_VERSION && state.schema_hash == store.schema_hash {
                store.entries = state.entries;
            } else {
                // incompatible planner/history schema; reset cleanly.
                store.persist()?;
            }
        } else {
            store.persist()?;
        }
        Ok(store)
    }

    pub fn record(&mut self, entry: OptimizerHistoryRecord) -> Result<()> {
        self.entries.push(entry);
        self.compact();
        self.persist()
    }

    pub fn by_fingerprint(&self, fingerprint: &str) -> Vec<OptimizerHistoryRecord> {
        let mut out = self
            .entries
            .iter()
            .filter(|e| e.fingerprint == fingerprint)
            .cloned()
            .collect::<Vec<_>>();
        out.sort_by(|a, b| b.captured_at_ms.cmp(&a.captured_at_ms));
        out
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn clear(&mut self) -> Result<()> {
        self.entries.clear();
        self.persist()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn compact(&mut self) {
        let mut groups: HashMap<String, Vec<OptimizerHistoryRecord>> = HashMap::new();
        for e in self.entries.drain(..) {
            groups.entry(e.fingerprint.clone()).or_default().push(e);
        }

        let now = now_ms();
        let mut compacted = Vec::new();
        for (_, mut group) in groups {
            group.sort_by(|a, b| b.captured_at_ms.cmp(&a.captured_at_ms));
            let mut keep = Vec::new();
            let recent_take = RECENT_KEEP_PER_FINGERPRINT.min(group.len());
            keep.extend(group.drain(0..recent_take));

            if keep.len() < self.max_entries_per_fingerprint && !group.is_empty() {
                let remaining = self.max_entries_per_fingerprint - keep.len();
                group.sort_by(|a, b| {
                    let sa = weighted_score(a, now);
                    let sb = weighted_score(b, now);
                    sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
                });
                keep.extend(group.into_iter().take(remaining));
            }

            compacted.extend(keep);
        }
        compacted.sort_by(|a, b| b.captured_at_ms.cmp(&a.captured_at_ms));
        self.entries = compacted;
    }

    fn persist(&self) -> Result<()> {
        let state = OptimizerHistoryState {
            version: HISTORY_VERSION,
            schema_hash: self.schema_hash.clone(),
            entries: self.entries.clone(),
        };
        let encoded = serde_json::to_vec_pretty(&state)
            .map_err(|e| EntDbError::Wal(format!("optimizer history encode failed: {e}")))?;
        fs::write(&self.path, encoded)?;
        Ok(())
    }
}

impl OptimizerHistoryRecorder {
    pub fn new(
        path: impl AsRef<Path>,
        schema_hash: &str,
        max_entries_per_fingerprint: usize,
        queue_capacity: usize,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let schema_hash_owned = schema_hash.to_string();
        let max_entries_per_fingerprint = max_entries_per_fingerprint.max(4);
        let queue_capacity = queue_capacity.max(1);
        let (tx, rx) = mpsc::sync_channel::<OptimizerHistoryRecord>(queue_capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let worker_errors = Arc::new(AtomicU64::new(0));
        let worker_errors_bg = Arc::clone(&worker_errors);
        let path_bg = path.clone();
        let schema_bg = schema_hash_owned.clone();
        let initial_store = OptimizerHistoryStore::load_or_create(
            &path_bg,
            &schema_bg,
            max_entries_per_fingerprint,
        );
        let initial_entries = initial_store
            .as_ref()
            .map(|s| s.entries.clone())
            .unwrap_or_default();
        let cache = Arc::new(RwLock::new(initial_entries));
        let cache_bg = Arc::clone(&cache);

        std::thread::spawn(move || {
            let mut store = match initial_store {
                Ok(s) => s,
                Err(_) => return,
            };
            while let Ok(entry) = rx.recv() {
                match store.record(entry) {
                    Ok(_) => {
                        if let Ok(mut guard) = cache_bg.write() {
                            *guard = store.entries.clone();
                        }
                    }
                    Err(_) => {
                        worker_errors_bg.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });

        Ok(Self {
            tx,
            dropped,
            worker_errors,
            cache,
            path,
            schema_hash: schema_hash_owned,
            max_entries_per_fingerprint,
        })
    }

    pub fn new_with_store_path(
        path: impl AsRef<Path>,
        schema_hash: &str,
        max_entries_per_fingerprint: usize,
        queue_capacity: usize,
        store_root: impl AsRef<Path>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let schema_hash_owned = schema_hash.to_string();
        let max_entries_per_fingerprint = max_entries_per_fingerprint.max(4);
        let queue_capacity = queue_capacity.max(1);
        let (tx, rx) = mpsc::sync_channel::<OptimizerHistoryRecord>(queue_capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let worker_errors = Arc::new(AtomicU64::new(0));
        let worker_errors_bg = Arc::clone(&worker_errors);
        let path_bg = path.clone();
        let schema_bg = schema_hash_owned.clone();
        let store_root = store_root.as_ref().to_path_buf();
        let store_path = store_root.join(path_bg.file_name().unwrap_or_default());
        let initial_store = OptimizerHistoryStore::load_or_create(
            &store_path,
            &schema_bg,
            max_entries_per_fingerprint,
        );
        let initial_entries = initial_store
            .as_ref()
            .map(|s| s.entries.clone())
            .unwrap_or_default();
        let cache = Arc::new(RwLock::new(initial_entries));
        let cache_bg = Arc::clone(&cache);

        std::thread::spawn(move || {
            let mut store = match initial_store {
                Ok(s) => s,
                Err(_) => return,
            };
            while let Ok(entry) = rx.recv() {
                match store.record(entry) {
                    Ok(_) => {
                        if let Ok(mut guard) = cache_bg.write() {
                            *guard = store.entries.clone();
                        }
                    }
                    Err(_) => {
                        worker_errors_bg.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });

        Ok(Self {
            tx,
            dropped,
            worker_errors,
            cache,
            path,
            schema_hash: schema_hash_owned,
            max_entries_per_fingerprint,
        })
    }

    pub fn try_record(&self, entry: OptimizerHistoryRecord) {
        match self.tx.try_send(entry) {
            Ok(_) => {}
            Err(mpsc::TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                self.worker_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub fn worker_error_count(&self) -> u64 {
        self.worker_errors.load(Ordering::Relaxed)
    }

    pub fn read_for_fingerprint(&self, fingerprint: &str) -> Vec<OptimizerHistoryRecord> {
        if let Ok(guard) = self.cache.read() {
            let mut out = guard
                .iter()
                .filter(|e| e.fingerprint == fingerprint)
                .cloned()
                .collect::<Vec<_>>();
            out.sort_by(|a, b| b.captured_at_ms.cmp(&a.captured_at_ms));
            if !out.is_empty() {
                return out;
            }
        }

        // Fallback for externally-seeded history files (e.g. integration tests):
        // reload once from disk when cache has no matching fingerprint.
        match OptimizerHistoryStore::load_or_create(
            &self.path,
            &self.schema_hash,
            self.max_entries_per_fingerprint,
        ) {
            Ok(store) => {
                if let Ok(mut guard) = self.cache.write() {
                    *guard = store.entries.clone();
                }
                store.by_fingerprint(fingerprint)
            }
            Err(_) => Vec::new(),
        }
    }

    pub fn clear(&self) -> Result<()> {
        let mut store = OptimizerHistoryStore::load_or_create(
            &self.path,
            &self.schema_hash,
            self.max_entries_per_fingerprint,
        )?;
        store.clear()?;
        if let Ok(mut guard) = self.cache.write() {
            guard.clear();
        }
        Ok(())
    }
}

fn weighted_score(e: &OptimizerHistoryRecord, now_ms: u64) -> f64 {
    let age_ms = now_ms.saturating_sub(e.captured_at_ms);
    let age_days = age_ms as f64 / 86_400_000.0;
    let decay = 1.0 / (1.0 + age_days);
    let latency_term = if e.latency_ms == 0 {
        1.0
    } else {
        1.0 / e.latency_ms as f64
    };
    let success_bonus = if e.success { 1.0 } else { 0.2 };
    (latency_term * 0.7 + e.confidence.clamp(0.0, 1.0) * 0.3) * decay * success_bonus
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
