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

use crate::catalog::{Catalog, Schema, TableInfo};
use crate::error::Result;
use crate::query::executor::{btree_maintenance, Executor, TxExecutionContext};
use crate::types::Value;
use std::sync::Arc;

pub struct IndexLookupExecutor {
    table_info: TableInfo,
    catalog: Arc<Catalog>,
    tx: Option<TxExecutionContext>,
    lookup_col_idx: usize,
    lookup_value: Value,
    rows: Vec<Vec<Value>>,
    next_row: usize,
}

impl IndexLookupExecutor {
    pub fn new(
        table_info: TableInfo,
        catalog: Arc<Catalog>,
        tx: Option<TxExecutionContext>,
        lookup_col_idx: usize,
        lookup_value: Value,
    ) -> Self {
        Self {
            table_info,
            catalog,
            tx,
            lookup_col_idx,
            lookup_value,
            rows: Vec::new(),
            next_row: 0,
        }
    }
}

impl Executor for IndexLookupExecutor {
    fn open(&mut self) -> Result<()> {
        self.rows = btree_maintenance::lookup_visible_rows(
            &self.catalog,
            &self.table_info,
            self.lookup_col_idx,
            &self.lookup_value,
            self.tx.as_ref(),
        )?
        .unwrap_or_default()
        .into_iter()
        .map(|(_, version, _)| version.values)
        .collect();
        self.next_row = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        if self.next_row >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.next_row].clone();
        self.next_row += 1;
        Ok(Some(row))
    }

    fn close(&mut self) -> Result<()> {
        self.rows.clear();
        self.next_row = 0;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.table_info.schema
    }
}
