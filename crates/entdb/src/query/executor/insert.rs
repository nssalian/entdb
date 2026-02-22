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
use crate::query::executor::{encode_mvcc_row, Executor, MvccRow, TxExecutionContext};
use crate::storage::table::Table;
use crate::storage::tuple::Tuple;
use crate::types::Value;
use std::sync::Arc;

pub struct InsertExecutor {
    table_info: TableInfo,
    rows: Vec<Vec<Value>>,
    catalog: Arc<Catalog>,
    tx: Option<TxExecutionContext>,
    done: bool,
    affected_rows: u64,
    out_schema: Schema,
}

impl InsertExecutor {
    pub fn new(
        table_info: TableInfo,
        rows: Vec<Vec<Value>>,
        catalog: Arc<Catalog>,
        tx: Option<TxExecutionContext>,
    ) -> Self {
        Self {
            table_info,
            rows,
            catalog,
            tx,
            done: false,
            affected_rows: 0,
            out_schema: Schema { columns: vec![] },
        }
    }
}

impl Executor for InsertExecutor {
    fn open(&mut self) -> Result<()> {
        self.done = false;
        self.affected_rows = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        if self.done {
            return Ok(None);
        }

        let table = Table::open(
            self.table_info.table_id,
            self.table_info.first_page_id,
            self.catalog.buffer_pool(),
        );

        for row in &self.rows {
            let created_txn = self.tx.as_ref().map(|t| t.txn_id).unwrap_or(0);
            let bytes = encode_mvcc_row(&MvccRow {
                values: row.clone(),
                created_txn,
                deleted_txn: None,
            })?;
            table.insert(&Tuple::new(bytes))?;
            self.affected_rows = self.affected_rows.saturating_add(1);
        }

        self.done = true;
        Ok(None)
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.out_schema
    }

    fn affected_rows(&self) -> Option<u64> {
        Some(self.affected_rows)
    }
}
