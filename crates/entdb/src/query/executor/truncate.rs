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
use crate::query::executor::{
    decode_stored_row, encode_mvcc_row, row_visible, DecodedRow, Executor, MvccRow,
    TxExecutionContext,
};
use crate::storage::table::Table;
use crate::storage::tuple::Tuple;
use crate::types::Value;
use std::sync::Arc;

pub struct TruncateExecutor {
    tables: Vec<TableInfo>,
    catalog: Arc<Catalog>,
    tx: Option<TxExecutionContext>,
    done: bool,
    affected_rows: u64,
    out_schema: Schema,
}

impl TruncateExecutor {
    pub fn new(
        tables: Vec<TableInfo>,
        catalog: Arc<Catalog>,
        tx: Option<TxExecutionContext>,
    ) -> Self {
        Self {
            tables,
            catalog,
            tx,
            done: false,
            affected_rows: 0,
            out_schema: Schema { columns: vec![] },
        }
    }
}

impl Executor for TruncateExecutor {
    fn open(&mut self) -> Result<()> {
        self.done = false;
        self.affected_rows = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        if self.done {
            return Ok(None);
        }

        let this_txn = self.tx.as_ref().map(|t| t.txn_id).unwrap_or(0);
        let tx_ref = self.tx.as_ref();
        for table_info in &self.tables {
            let table = Table::open(
                table_info.table_id,
                table_info.first_page_id,
                self.catalog.buffer_pool(),
            );
            let mut targets = Vec::new();
            for (tid, tuple) in table.scan() {
                let version = match decode_stored_row(&tuple.data)? {
                    DecodedRow::Legacy(row) => MvccRow {
                        values: row,
                        created_txn: 0,
                        deleted_txn: None,
                    },
                    DecodedRow::Versioned(v) => v,
                };
                if !row_visible(&version, tx_ref) || version.deleted_txn.is_some() {
                    continue;
                }
                targets.push((tid, version, tuple.data));
            }

            for (tid, mut version, expected_bytes) in targets {
                version.deleted_txn = Some(this_txn);
                let deleted = Tuple::new(encode_mvcc_row(&version)?);
                if table.compare_and_update(tid, &expected_bytes, &deleted)? {
                    self.affected_rows = self.affected_rows.saturating_add(1);
                }
            }
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
