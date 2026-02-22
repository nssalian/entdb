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

use crate::catalog::schema::Schema;
use crate::error::{EntDbError, Result};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::page::PageId;
use crate::types::Value;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableInfo {
    pub table_id: u32,
    pub name: String,
    pub schema: Schema,
    pub first_page_id: PageId,
    pub indexes: Vec<IndexInfo>,
    #[serde(default)]
    pub stats: TableStats,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexInfo {
    pub index_id: u32,
    pub name: String,
    pub root_page_id: PageId,
    pub column_indices: Vec<usize>,
    pub unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableStats {
    pub row_count: u64,
    pub last_analyzed_at_ms: Option<u64>,
    pub mutation_count_since_analyze: u64,
    pub stale: bool,
    pub columns: Vec<ColumnStats>,
}

impl Default for TableStats {
    fn default() -> Self {
        Self {
            row_count: 0,
            last_analyzed_at_ms: None,
            mutation_count_since_analyze: 0,
            stale: true,
            columns: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnStats {
    pub name: String,
    pub ndv: Option<u64>,
    pub null_fraction: f64,
    pub min: Option<Value>,
    pub max: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CatalogState {
    next_table_id: u32,
    next_index_id: u32,
    tables: HashMap<String, TableInfo>,
}

pub struct Catalog {
    tables: RwLock<HashMap<String, TableInfo>>,
    next_table_id: RwLock<u32>,
    next_index_id: RwLock<u32>,
    buffer_pool: Arc<BufferPool>,
    catalog_path: PathBuf,
}

impl Catalog {
    pub fn init(buffer_pool: Arc<BufferPool>) -> Result<Self> {
        let catalog = Self::new_empty(buffer_pool);
        catalog.persist()?;
        Ok(catalog)
    }

    pub fn load(buffer_pool: Arc<BufferPool>) -> Result<Self> {
        let catalog = Self::new_empty(buffer_pool);
        if catalog.catalog_path.exists() {
            let bytes = fs::read(&catalog.catalog_path)?;
            let state: CatalogState = serde_json::from_slice(&bytes)
                .map_err(|e| EntDbError::Corruption(format!("invalid catalog file: {e}")))?;
            *catalog.tables.write() = state.tables;
            *catalog.next_table_id.write() = state.next_table_id;
            *catalog.next_index_id.write() = state.next_index_id;
        }
        Ok(catalog)
    }

    pub fn create_table(&self, name: &str, schema: Schema) -> Result<TableInfo> {
        if self.tables.read().contains_key(name) {
            return Err(EntDbError::InvalidPage(format!(
                "table '{name}' already exists"
            )));
        }

        let table_id = {
            let mut id = self.next_table_id.write();
            let out = *id;
            *id = id.saturating_add(1);
            out
        };

        let first_page_id = {
            let page = self.buffer_pool.new_page()?;
            page.page_id()
        };

        let info = TableInfo {
            table_id,
            name: name.to_string(),
            schema,
            first_page_id,
            indexes: Vec::new(),
            stats: TableStats::default(),
        };

        self.tables.write().insert(name.to_string(), info.clone());
        self.persist()?;
        Ok(info)
    }

    pub fn drop_table(&self, name: &str) -> Result<()> {
        self.tables.write().remove(name);
        self.persist()
    }

    pub fn drop_index(&self, index_name: &str) -> Result<bool> {
        let mut tables = self.tables.write();
        for table in tables.values_mut() {
            if let Some(pos) = table.indexes.iter().position(|i| i.name == index_name) {
                table.indexes.remove(pos);
                drop(tables);
                self.persist()?;
                return Ok(true);
            }
        }
        Err(EntDbError::InvalidPage(format!(
            "index '{index_name}' does not exist"
        )))
    }

    pub fn rename_table(&self, old_name: &str, new_name: &str) -> Result<()> {
        let mut tables = self.tables.write();
        if tables.contains_key(new_name) {
            return Err(EntDbError::InvalidPage(format!(
                "table '{new_name}' already exists"
            )));
        }
        let Some(mut table) = tables.remove(old_name) else {
            return Err(EntDbError::InvalidPage(format!(
                "table '{old_name}' does not exist"
            )));
        };
        table.name = new_name.to_string();
        tables.insert(new_name.to_string(), table);
        drop(tables);
        self.persist()
    }

    pub fn add_column(
        &self,
        table_name: &str,
        column: crate::catalog::Column,
        if_not_exists: bool,
    ) -> Result<bool> {
        let mut tables = self.tables.write();
        let table = tables.get_mut(table_name).ok_or_else(|| {
            EntDbError::InvalidPage(format!("table '{table_name}' does not exist"))
        })?;
        if table.schema.column_index(&column.name).is_some() {
            if if_not_exists {
                return Ok(false);
            }
            return Err(EntDbError::InvalidPage(format!(
                "column '{}' already exists",
                column.name
            )));
        }
        table.schema.columns.push(column);
        drop(tables);
        self.persist()?;
        Ok(true)
    }

    pub fn drop_column(
        &self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> Result<Option<usize>> {
        let mut tables = self.tables.write();
        let table = tables.get_mut(table_name).ok_or_else(|| {
            EntDbError::InvalidPage(format!("table '{table_name}' does not exist"))
        })?;
        let Some(idx) = table.schema.column_index(column_name) else {
            if if_exists {
                return Ok(None);
            }
            return Err(EntDbError::InvalidPage(format!(
                "column '{column_name}' does not exist"
            )));
        };
        table.schema.columns.remove(idx);
        drop(tables);
        self.persist()?;
        Ok(Some(idx))
    }

    pub fn rename_column(&self, table_name: &str, old_name: &str, new_name: &str) -> Result<()> {
        let mut tables = self.tables.write();
        let table = tables.get_mut(table_name).ok_or_else(|| {
            EntDbError::InvalidPage(format!("table '{table_name}' does not exist"))
        })?;
        if table.schema.column_index(new_name).is_some() {
            return Err(EntDbError::InvalidPage(format!(
                "column '{new_name}' already exists"
            )));
        }
        let idx = table.schema.column_index(old_name).ok_or_else(|| {
            EntDbError::InvalidPage(format!("column '{old_name}' does not exist"))
        })?;
        table.schema.columns[idx].name = new_name.to_string();
        drop(tables);
        self.persist()
    }

    pub fn get_table(&self, name: &str) -> Option<TableInfo> {
        self.tables.read().get(name).cloned()
    }

    pub fn create_index(
        &self,
        table_name: &str,
        index_name: &str,
        columns: &[String],
        unique: bool,
    ) -> Result<IndexInfo> {
        let mut tables = self.tables.write();
        let table = tables.get_mut(table_name).ok_or_else(|| {
            EntDbError::InvalidPage(format!("table '{table_name}' does not exist"))
        })?;

        if table.indexes.iter().any(|i| i.name == index_name) {
            return Err(EntDbError::InvalidPage(format!(
                "index '{index_name}' already exists"
            )));
        }

        let mut column_indices = Vec::with_capacity(columns.len());
        for c in columns {
            let idx = table
                .schema
                .column_index(c)
                .ok_or_else(|| EntDbError::InvalidPage(format!("column '{c}' does not exist")))?;
            column_indices.push(idx);
        }

        let index_id = {
            let mut id = self.next_index_id.write();
            let out = *id;
            *id = id.saturating_add(1);
            out
        };

        let root_page_id = {
            let page = self.buffer_pool.new_page()?;
            page.page_id()
        };

        let idx = IndexInfo {
            index_id,
            name: index_name.to_string(),
            root_page_id,
            column_indices,
            unique,
        };
        table.indexes.push(idx.clone());
        drop(tables);

        self.persist()?;
        Ok(idx)
    }

    pub fn list_tables(&self) -> Vec<TableInfo> {
        let mut out = self.tables.read().values().cloned().collect::<Vec<_>>();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    pub fn update_table_stats(
        &self,
        table_name: &str,
        row_count: u64,
        columns: Vec<ColumnStats>,
        analyzed_at_ms: u64,
    ) -> Result<()> {
        let mut tables = self.tables.write();
        let table = tables.get_mut(table_name).ok_or_else(|| {
            EntDbError::InvalidPage(format!("table '{table_name}' does not exist"))
        })?;
        table.stats = TableStats {
            row_count,
            last_analyzed_at_ms: Some(analyzed_at_ms),
            mutation_count_since_analyze: 0,
            stale: false,
            columns,
        };
        drop(tables);
        self.persist()
    }

    pub fn mark_table_stats_stale(&self, table_name: &str, mutations: u64) -> Result<()> {
        let mut tables = self.tables.write();
        let Some(table) = tables.get_mut(table_name) else {
            return Ok(());
        };
        table.stats.stale = true;
        table.stats.mutation_count_since_analyze = table
            .stats
            .mutation_count_since_analyze
            .saturating_add(mutations.max(1));
        drop(tables);
        self.persist()
    }

    pub fn buffer_pool(&self) -> Arc<BufferPool> {
        Arc::clone(&self.buffer_pool)
    }

    fn new_empty(buffer_pool: Arc<BufferPool>) -> Self {
        let db_path = buffer_pool.disk_path().to_path_buf();
        let catalog_path = catalog_path_for_db(&db_path);
        Self {
            tables: RwLock::new(HashMap::new()),
            next_table_id: RwLock::new(1),
            next_index_id: RwLock::new(1),
            buffer_pool,
            catalog_path,
        }
    }

    fn persist(&self) -> Result<()> {
        let state = CatalogState {
            next_table_id: *self.next_table_id.read(),
            next_index_id: *self.next_index_id.read(),
            tables: self.tables.read().clone(),
        };

        let encoded = serde_json::to_vec_pretty(&state)
            .map_err(|e| EntDbError::Wal(format!("catalog encode failed: {e}")))?;
        fs::write(&self.catalog_path, encoded)?;
        Ok(())
    }
}

fn catalog_path_for_db(db_path: &std::path::Path) -> PathBuf {
    let mut p = db_path.to_path_buf();
    p.set_extension("catalog.json");
    p
}
