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
use crate::storage::page::PageId;
use crate::wal::log_manager::LogManager;
use crate::wal::log_record::LogRecord;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

pub struct RecoveryManager {
    log_manager: Arc<LogManager>,
    buffer_pool: Arc<BufferPool>,
}

impl RecoveryManager {
    pub fn new(log_manager: Arc<LogManager>, buffer_pool: Arc<BufferPool>) -> Self {
        Self {
            log_manager,
            buffer_pool,
        }
    }

    pub fn recover(&self) -> Result<()> {
        let (dirty_pages, active_txns, records) = self.analysis_pass()?;
        self.redo_pass(&dirty_pages, &records)?;
        self.undo_pass(&active_txns, &records)?;
        self.buffer_pool.flush_all()?;
        Ok(())
    }

    pub fn checkpoint(&self, active_txns: &[u64]) -> Result<()> {
        self.log_manager.append(LogRecord::Checkpoint {
            active_txns: active_txns.to_vec(),
        })?;
        self.log_manager.flush()
    }

    fn analysis_pass(&self) -> Result<(HashSet<PageId>, HashSet<u64>, Vec<(u64, LogRecord)>)> {
        let mut dirty_pages = HashSet::new();
        let mut active_txns = HashSet::new();
        let mut records = Vec::new();

        for entry in self.log_manager.iter_from(0)? {
            let lsn = entry.lsn;
            let record = entry.record;
            match &record {
                LogRecord::Begin { txn_id } => {
                    active_txns.insert(*txn_id);
                }
                LogRecord::Commit { txn_id } | LogRecord::Abort { txn_id } => {
                    active_txns.remove(txn_id);
                }
                LogRecord::Update { page_id, .. } => {
                    dirty_pages.insert(*page_id);
                }
                LogRecord::NewPage { page_id, .. } => {
                    dirty_pages.insert(*page_id);
                }
                LogRecord::Checkpoint { active_txns: txns } => {
                    for txn in txns {
                        active_txns.insert(*txn);
                    }
                }
                LogRecord::Clr { .. } | LogRecord::BtreeStructure { .. } => {}
            }
            records.push((lsn, record));
        }

        Ok((dirty_pages, active_txns, records))
    }

    fn redo_pass(&self, dirty_pages: &HashSet<PageId>, records: &[(u64, LogRecord)]) -> Result<()> {
        for (_, record) in records {
            match record {
                LogRecord::Update {
                    page_id,
                    offset,
                    new_data,
                    ..
                } if dirty_pages.contains(page_id) => {
                    self.apply_page_delta(*page_id, *offset as usize, new_data)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn undo_pass(&self, active_txns: &HashSet<u64>, records: &[(u64, LogRecord)]) -> Result<()> {
        if active_txns.is_empty() {
            return Ok(());
        }

        let mut stack = VecDeque::new();
        for (lsn, record) in records {
            stack.push_back((*lsn, record.clone()));
        }

        while let Some((lsn, record)) = stack.pop_back() {
            if let LogRecord::Update {
                txn_id,
                page_id,
                offset,
                old_data,
                ..
            } = record
            {
                if active_txns.contains(&txn_id) {
                    self.apply_page_delta(page_id, offset as usize, &old_data)?;
                    self.log_manager.append(LogRecord::Clr {
                        txn_id,
                        undo_next_lsn: lsn,
                    })?;
                }
            }
        }

        self.log_manager.flush()?;
        Ok(())
    }

    fn apply_page_delta(&self, page_id: PageId, offset: usize, bytes: &[u8]) -> Result<()> {
        let mut page = self.buffer_pool.fetch_page(page_id)?;
        let body = page.body_mut();
        let end = offset + bytes.len();
        if end > body.len() {
            return Err(EntDbError::Corruption(format!(
                "wal update out of bounds for page {page_id}: offset={offset}, len={}",
                bytes.len()
            )));
        }
        body[offset..end].copy_from_slice(bytes);
        page.mark_dirty();
        Ok(())
    }
}
