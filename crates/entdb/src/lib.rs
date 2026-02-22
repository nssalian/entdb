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

pub mod catalog;
pub mod error;
pub mod fault;
pub mod query;
pub mod storage;
pub mod tx;
pub mod types;
pub mod wal;

pub use error::{EntDbError, Result};
pub use query::{QueryEngine, QueryOutput};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::catalog::Catalog;
use crate::storage::buffer_pool::BufferPool;
use crate::storage::disk_manager::DiskManager;

#[derive(Debug, Clone, Copy)]
pub struct ConnectOptions {
    pub buffer_pool_pages: usize,
    pub durable_txn_metadata: bool,
    pub polyglot_enabled: bool,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            buffer_pool_pages: 256,
            durable_txn_metadata: true,
            polyglot_enabled: false,
        }
    }
}

pub struct EntDb {
    engine: QueryEngine,
}

impl EntDb {
    pub fn connect(path: impl AsRef<Path>) -> Result<Self> {
        Self::connect_with(path, ConnectOptions::default())
    }

    pub fn connect_with(path: impl AsRef<Path>, opts: ConnectOptions) -> Result<Self> {
        let db_file = resolve_db_file(path.as_ref());
        if let Some(parent) = db_file.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let dm = Arc::new(DiskManager::new(&db_file)?);
        let bp = Arc::new(BufferPool::new(opts.buffer_pool_pages, Arc::clone(&dm)));
        let catalog = Arc::new(Catalog::load(Arc::clone(&bp))?);
        let engine = QueryEngine::with_txn_persistence(catalog, opts.durable_txn_metadata);
        engine.set_polyglot_enabled(opts.polyglot_enabled);
        Ok(Self { engine })
    }

    pub fn execute(&self, sql: &str) -> Result<Vec<QueryOutput>> {
        self.engine.execute(sql)
    }

    pub fn close(&self) -> Result<()> {
        self.engine.flush_all()
    }

    pub fn engine(&self) -> &QueryEngine {
        &self.engine
    }
}

impl Drop for EntDb {
    fn drop(&mut self) {
        let _ = self.engine.flush_all();
    }
}

fn resolve_db_file(path: &Path) -> PathBuf {
    if path.extension().is_some() {
        path.to_path_buf()
    } else {
        path.join("entdb.data")
    }
}
