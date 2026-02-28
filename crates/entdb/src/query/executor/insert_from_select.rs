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
use crate::error::{EntDbError, Result};
use crate::query::executor::bm25_maintenance;
use crate::query::executor::{encode_mvcc_row, encode_row, Executor, TxExecutionContext};
use crate::storage::table::Table;
use crate::storage::tuple::Tuple;
use crate::types::Value;
use std::sync::Arc;

pub struct InsertFromSelectExecutor {
    table: TableInfo,
    input: Box<dyn Executor>,
    catalog: Arc<Catalog>,
    tx: Option<TxExecutionContext>,
    done: bool,
    out_schema: Schema,
    affected_rows: u64,
}

impl InsertFromSelectExecutor {
    pub fn new(
        table: TableInfo,
        input: Box<dyn Executor>,
        catalog: Arc<Catalog>,
        tx: Option<TxExecutionContext>,
    ) -> Self {
        Self {
            table,
            input,
            catalog,
            tx,
            done: false,
            out_schema: Schema { columns: vec![] },
            affected_rows: 0,
        }
    }
}

impl Executor for InsertFromSelectExecutor {
    fn open(&mut self) -> Result<()> {
        self.done = false;
        self.affected_rows = 0;
        self.input.open()
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        if self.done {
            return Ok(None);
        }

        let table = Table::open(
            self.table.table_id,
            self.table.first_page_id,
            self.catalog.buffer_pool(),
        );

        while let Some(row) = self.input.next()? {
            if row.len() != self.table.schema.columns.len() {
                return Err(EntDbError::Query(format!(
                    "INSERT SELECT row has {} values, expected {}",
                    row.len(),
                    self.table.schema.columns.len()
                )));
            }
            let mut casted = Vec::with_capacity(row.len());
            for (v, col) in row.iter().zip(&self.table.schema.columns) {
                casted.push(v.cast_to(&col.data_type).map_err(|e| {
                    EntDbError::Query(format!("cannot cast value for column '{}': {e}", col.name))
                })?);
            }
            let bytes = if let Some(tx) = &self.tx {
                encode_mvcc_row(&crate::query::executor::MvccRow {
                    values: casted,
                    created_txn: tx.txn_id,
                    deleted_txn: None,
                })?
            } else {
                encode_row(&casted)?
            };
            let tid = table.insert(&Tuple::new(bytes))?;
            let inserted = table.get(tid)?;
            let inserted_row = match crate::query::executor::decode_stored_row(&inserted.data)? {
                crate::query::executor::DecodedRow::Legacy(v) => v,
                crate::query::executor::DecodedRow::Versioned(v) => v.values,
            };
            bm25_maintenance::on_insert(&self.catalog, &self.table, &inserted_row, tid)?;
            self.affected_rows = self.affected_rows.saturating_add(1);
        }

        self.done = true;
        Ok(None)
    }

    fn close(&mut self) -> Result<()> {
        self.input.close()
    }

    fn schema(&self) -> &Schema {
        &self.out_schema
    }

    fn affected_rows(&self) -> Option<u64> {
        Some(self.affected_rows)
    }
}
