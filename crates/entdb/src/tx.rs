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
use crate::wal::log_manager::LogManager;
use crate::wal::log_record::LogRecord;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub type TxnId = u64;
const TXN_CHECKPOINT_INTERVAL: u64 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    Active { snapshot_ts: u64 },
    Committed(u64),
    Aborted,
}

#[derive(Debug, Clone, Copy)]
pub struct TransactionHandle {
    pub txn_id: TxnId,
    pub snapshot_ts: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct TxnStateFile {
    next_txn_id: u64,
    commit_ts: u64,
    committed: HashMap<TxnId, u64>,
    aborted: Vec<TxnId>,
    #[serde(default)]
    wal_checkpoint_lsn: u64,
}

pub struct TransactionManager {
    next_txn_id: AtomicU64,
    commit_ts: AtomicU64,
    txns: RwLock<HashMap<TxnId, TxnStatus>>,
    state_path: Option<PathBuf>,
    wal_manager: Option<Arc<LogManager>>,
    wal_checkpoint_lsn: AtomicU64,
    ops_since_checkpoint: AtomicU64,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            commit_ts: AtomicU64::new(0),
            txns: RwLock::new(HashMap::new()),
            state_path: None,
            wal_manager: None,
            wal_checkpoint_lsn: AtomicU64::new(0),
            ops_since_checkpoint: AtomicU64::new(0),
        }
    }

    pub fn with_persistence(path: impl AsRef<Path>) -> Result<Self> {
        let state_path = path.as_ref().to_path_buf();
        let state = load_or_default_state(&state_path)?;
        let mut txns = HashMap::new();
        for (txn_id, ts) in state.committed {
            txns.insert(txn_id, TxnStatus::Committed(ts));
        }
        for txn_id in state.aborted {
            txns.insert(txn_id, TxnStatus::Aborted);
        }

        let tm = Self {
            next_txn_id: AtomicU64::new(state.next_txn_id.max(1)),
            commit_ts: AtomicU64::new(state.commit_ts),
            txns: RwLock::new(txns),
            state_path: Some(state_path),
            wal_manager: None,
            wal_checkpoint_lsn: AtomicU64::new(state.wal_checkpoint_lsn),
            ops_since_checkpoint: AtomicU64::new(0),
        };
        tm.persist_state()?;
        Ok(tm)
    }

    pub fn with_wal_persistence(
        state_path: impl AsRef<Path>,
        wal_path: impl AsRef<Path>,
    ) -> Result<Self> {
        let state_path = state_path.as_ref().to_path_buf();
        let state = load_or_default_state(&state_path)?;
        let wal_manager = Arc::new(LogManager::new(wal_path, 4096)?);

        let mut txns = HashMap::new();
        for (txn_id, ts) in state.committed {
            txns.insert(txn_id, TxnStatus::Committed(ts));
        }
        for txn_id in state.aborted {
            txns.insert(txn_id, TxnStatus::Aborted);
        }

        let tm = Self {
            next_txn_id: AtomicU64::new(state.next_txn_id.max(1)),
            commit_ts: AtomicU64::new(state.commit_ts),
            txns: RwLock::new(txns),
            state_path: Some(state_path),
            wal_manager: Some(Arc::clone(&wal_manager)),
            wal_checkpoint_lsn: AtomicU64::new(state.wal_checkpoint_lsn),
            ops_since_checkpoint: AtomicU64::new(0),
        };

        tm.replay_wal_from_checkpoint()?;
        tm.align_next_txn_id_with_wal_history()?;
        tm.persist_state()?;
        Ok(tm)
    }

    pub fn begin(&self) -> TransactionHandle {
        let snapshot_ts = self.commit_ts.load(Ordering::Acquire);
        let txn_id = loop {
            let candidate = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
            let mut txns = self.txns.write();
            if txns.contains_key(&candidate) {
                continue;
            }
            txns.insert(candidate, TxnStatus::Active { snapshot_ts });
            break candidate;
        };

        if let Some(wal) = &self.wal_manager {
            let _ = wal.append(LogRecord::Begin { txn_id });
            self.record_mutation_noncritical();
        } else {
            let _ = self.persist_state();
        }

        TransactionHandle {
            txn_id,
            snapshot_ts,
        }
    }

    pub fn commit(&self, txn_id: TxnId) -> Result<u64> {
        let state = self.txns.read().get(&txn_id).copied();
        match state {
            Some(TxnStatus::Committed(ts)) => return Ok(ts),
            Some(TxnStatus::Active { .. }) => {}
            Some(TxnStatus::Aborted) => {
                return Err(EntDbError::Query(format!(
                    "cannot commit aborted transaction {txn_id}"
                )))
            }
            None => {
                return Err(EntDbError::Query(format!(
                    "cannot commit unknown transaction {txn_id}"
                )))
            }
        }

        if let Some(wal) = &self.wal_manager {
            wal.append(LogRecord::Commit { txn_id })?;
            wal.flush()?;
        }

        let ts = self.commit_ts.fetch_add(1, Ordering::SeqCst) + 1;
        let mut txns = self.txns.write();
        match txns.get(&txn_id).copied() {
            Some(TxnStatus::Active { .. }) => {
                txns.insert(txn_id, TxnStatus::Committed(ts));
            }
            Some(TxnStatus::Committed(existing)) => return Ok(existing),
            Some(TxnStatus::Aborted) => {
                return Err(EntDbError::Query(format!(
                    "cannot commit aborted transaction {txn_id}"
                )))
            }
            None => {
                return Err(EntDbError::Query(format!(
                    "cannot commit unknown transaction {txn_id}"
                )))
            }
        }
        drop(txns);

        if self.wal_manager.is_some() {
            self.record_mutation_checkpointed()?;
        }
        self.persist_state()?;

        Ok(ts)
    }

    pub fn abort(&self, txn_id: TxnId) {
        if let Some(wal) = &self.wal_manager {
            let _ = wal.append(LogRecord::Abort { txn_id });
            let _ = wal.flush();
        }

        self.txns.write().insert(txn_id, TxnStatus::Aborted);

        if self.wal_manager.is_some() {
            self.record_mutation_noncritical();
        }
        let _ = self.persist_state();
    }

    pub fn status(&self, txn_id: TxnId) -> TxnStatus {
        if txn_id == 0 {
            return TxnStatus::Committed(0);
        }
        self.txns
            .read()
            .get(&txn_id)
            .copied()
            .unwrap_or(TxnStatus::Aborted)
    }

    pub fn is_active(&self, txn_id: TxnId) -> bool {
        matches!(self.status(txn_id), TxnStatus::Active { .. })
    }

    pub fn oldest_active_snapshot(&self) -> Option<u64> {
        self.txns
            .read()
            .values()
            .filter_map(|s| match s {
                TxnStatus::Active { snapshot_ts } => Some(*snapshot_ts),
                _ => None,
            })
            .min()
    }

    pub fn latest_commit_ts(&self) -> u64 {
        self.commit_ts.load(Ordering::Acquire)
    }

    pub fn ensure_min_next_txn_id(&self, min_next_txn_id: u64) {
        let current = self.next_txn_id.load(Ordering::Acquire);
        if min_next_txn_id > current {
            self.next_txn_id.store(min_next_txn_id, Ordering::Release);
        }
    }

    pub fn persist_state(&self) -> Result<()> {
        let Some(path) = &self.state_path else {
            return Ok(());
        };

        let txns = self.txns.read();
        let mut committed = HashMap::new();
        let mut aborted = Vec::new();
        for (txn_id, status) in txns.iter() {
            match status {
                TxnStatus::Committed(ts) => {
                    committed.insert(*txn_id, *ts);
                }
                TxnStatus::Aborted => aborted.push(*txn_id),
                TxnStatus::Active { .. } => {}
            }
        }
        drop(txns);

        let state = TxnStateFile {
            next_txn_id: self.next_txn_id.load(Ordering::Acquire),
            commit_ts: self.commit_ts.load(Ordering::Acquire),
            committed,
            aborted,
            wal_checkpoint_lsn: self.wal_checkpoint_lsn.load(Ordering::Acquire),
        };

        let bytes = serde_json::to_vec_pretty(&state)
            .map_err(|e| EntDbError::Corruption(format!("txn state encode failed: {e}")))?;
        fs::write(path, bytes)?;
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<()> {
        let Some(wal) = &self.wal_manager else {
            return self.persist_state();
        };

        let active_txns = self
            .txns
            .read()
            .iter()
            .filter_map(|(txn_id, status)| {
                if matches!(status, TxnStatus::Active { .. }) {
                    Some(*txn_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let lsn = wal.append(LogRecord::Checkpoint { active_txns })?;
        wal.flush()?;
        self.wal_checkpoint_lsn.store(lsn, Ordering::Release);
        self.ops_since_checkpoint.store(0, Ordering::Release);
        self.persist_state()
    }

    fn replay_wal_from_checkpoint(&self) -> Result<()> {
        let Some(wal) = &self.wal_manager else {
            return Ok(());
        };

        let start_lsn = self
            .wal_checkpoint_lsn
            .load(Ordering::Acquire)
            .saturating_add(1);
        let mut max_txn_id = self.next_txn_id.load(Ordering::Acquire).saturating_sub(1);
        let mut commit_ts = self.commit_ts.load(Ordering::Acquire);
        let mut last_lsn = self.wal_checkpoint_lsn.load(Ordering::Acquire);

        let mut txns = self.txns.write();
        for entry in wal.iter_from(start_lsn)? {
            last_lsn = entry.lsn;
            match entry.record {
                LogRecord::Begin { txn_id } => {
                    max_txn_id = max_txn_id.max(txn_id);
                    txns.entry(txn_id).or_insert(TxnStatus::Active {
                        snapshot_ts: commit_ts,
                    });
                }
                LogRecord::Commit { txn_id } => {
                    max_txn_id = max_txn_id.max(txn_id);
                    commit_ts = commit_ts.saturating_add(1);
                    txns.insert(txn_id, TxnStatus::Committed(commit_ts));
                }
                LogRecord::Abort { txn_id } => {
                    max_txn_id = max_txn_id.max(txn_id);
                    txns.insert(txn_id, TxnStatus::Aborted);
                }
                _ => {}
            }
        }

        for status in txns.values_mut() {
            if matches!(status, TxnStatus::Active { .. }) {
                *status = TxnStatus::Aborted;
            }
        }
        drop(txns);

        self.commit_ts.store(commit_ts, Ordering::Release);
        self.next_txn_id
            .store(max_txn_id.saturating_add(1).max(1), Ordering::Release);
        self.wal_checkpoint_lsn.store(last_lsn, Ordering::Release);
        Ok(())
    }

    fn record_mutation_checkpointed(&self) -> Result<()> {
        let count = self.ops_since_checkpoint.fetch_add(1, Ordering::SeqCst) + 1;
        if count >= TXN_CHECKPOINT_INTERVAL {
            self.checkpoint()?;
        }
        Ok(())
    }

    fn record_mutation_noncritical(&self) {
        if self.wal_manager.is_none() {
            return;
        }
        let _ = self.record_mutation_checkpointed();
    }

    fn align_next_txn_id_with_wal_history(&self) -> Result<()> {
        let Some(wal) = &self.wal_manager else {
            return Ok(());
        };
        let mut wal_max_txn_id = 0_u64;
        for entry in wal.iter_from(0)? {
            let candidate = match entry.record {
                LogRecord::Begin { txn_id }
                | LogRecord::Commit { txn_id }
                | LogRecord::Abort { txn_id }
                | LogRecord::Update { txn_id, .. }
                | LogRecord::NewPage { txn_id, .. }
                | LogRecord::Clr { txn_id, .. }
                | LogRecord::BtreeStructure { txn_id, .. } => txn_id,
                LogRecord::Checkpoint { .. } => 0,
            };
            wal_max_txn_id = wal_max_txn_id.max(candidate);
        }

        let current_next = self.next_txn_id.load(Ordering::Acquire);
        let required_next = wal_max_txn_id.saturating_add(1).max(1);
        if required_next > current_next {
            self.next_txn_id.store(required_next, Ordering::Release);
        }
        Ok(())
    }
}

fn load_or_default_state(path: &Path) -> Result<TxnStateFile> {
    if !path.exists() {
        return Ok(TxnStateFile {
            next_txn_id: 1,
            commit_ts: 0,
            committed: HashMap::new(),
            aborted: Vec::new(),
            wal_checkpoint_lsn: 0,
        });
    }

    let bytes = fs::read(path)?;
    let state: TxnStateFile = serde_json::from_slice(&bytes)
        .map_err(|e| EntDbError::Corruption(format!("invalid txn state file: {e}")))?;
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::log_record::LogRecord;

    #[test]
    fn begin_does_not_reuse_existing_txn_id_when_counter_is_stale() {
        let unique = format!(
            "entdb-tx-stale-counter-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        );
        let state_path = std::env::temp_dir().join(unique);
        let seed = TxnStateFile {
            next_txn_id: 1,
            commit_ts: 1,
            committed: HashMap::from([(1_u64, 1_u64)]),
            aborted: vec![],
            wal_checkpoint_lsn: 0,
        };
        let bytes = serde_json::to_vec_pretty(&seed).expect("seed encode");
        fs::write(&state_path, bytes).expect("write seed state");

        let tm = TransactionManager::with_persistence(&state_path).expect("load tm");
        let tx = tm.begin();
        assert_ne!(tx.txn_id, 1);
    }

    #[test]
    fn wal_history_prevents_reusing_old_txn_ids_even_if_state_is_stale() {
        let unique = format!(
            "entdb-tx-wal-align-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        );
        let state_path = std::env::temp_dir().join(format!("{unique}.json"));
        let wal_path = std::env::temp_dir().join(format!("{unique}.wal"));

        let seed = TxnStateFile {
            next_txn_id: 1,
            commit_ts: 0,
            committed: HashMap::new(),
            aborted: vec![],
            wal_checkpoint_lsn: 0,
        };
        let bytes = serde_json::to_vec_pretty(&seed).expect("seed encode");
        fs::write(&state_path, bytes).expect("write seed state");

        let wal = LogManager::new(&wal_path, 1024).expect("open wal");
        wal.append(LogRecord::Begin { txn_id: 42 })
            .expect("append begin");
        wal.append(LogRecord::Abort { txn_id: 42 })
            .expect("append abort");
        wal.flush().expect("flush wal");

        let tm = TransactionManager::with_wal_persistence(&state_path, &wal_path).expect("load tm");
        let tx = tm.begin();
        assert!(tx.txn_id > 42);
    }
}
