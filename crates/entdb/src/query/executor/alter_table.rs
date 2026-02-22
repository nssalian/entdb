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

use crate::catalog::{Catalog, Schema};
use crate::error::{EntDbError, Result};
use crate::query::executor::{
    decode_stored_row, encode_mvcc_row, encode_row, DecodedRow, Executor,
};
use crate::query::plan::AlterTableOp;
use crate::storage::table::Table;
use crate::storage::tuple::Tuple;
use crate::types::Value;
use std::sync::Arc;

pub struct AlterTableExecutor {
    table_name: String,
    operations: Vec<AlterTableOp>,
    catalog: Arc<Catalog>,
    done: bool,
    out_schema: Schema,
    affected_rows: u64,
}

impl AlterTableExecutor {
    pub fn new(table_name: String, operations: Vec<AlterTableOp>, catalog: Arc<Catalog>) -> Self {
        Self {
            table_name,
            operations,
            catalog,
            done: false,
            out_schema: Schema { columns: vec![] },
            affected_rows: 0,
        }
    }

    fn rewrite_table_rows_for_add_column(&self, table_name: &str) -> Result<u64> {
        let table_info = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| EntDbError::Query(format!("table '{table_name}' does not exist")))?;
        let table = Table::open(
            table_info.table_id,
            table_info.first_page_id,
            self.catalog.buffer_pool(),
        );
        let mut rewritten = 0_u64;
        for (tid, tuple) in table.scan() {
            let bytes = match decode_stored_row(&tuple.data)? {
                DecodedRow::Legacy(mut row) => {
                    row.push(Value::Null);
                    encode_row(&row)?
                }
                DecodedRow::Versioned(mut v) => {
                    v.values.push(Value::Null);
                    encode_mvcc_row(&v)?
                }
            };
            table.update(tid, &Tuple::new(bytes))?;
            rewritten = rewritten.saturating_add(1);
        }
        Ok(rewritten)
    }

    fn rewrite_table_rows_for_drop_column(&self, table_name: &str, drop_idx: usize) -> Result<u64> {
        let table_info = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| EntDbError::Query(format!("table '{table_name}' does not exist")))?;
        let table = Table::open(
            table_info.table_id,
            table_info.first_page_id,
            self.catalog.buffer_pool(),
        );
        let mut rewritten = 0_u64;
        for (tid, tuple) in table.scan() {
            let bytes = match decode_stored_row(&tuple.data)? {
                DecodedRow::Legacy(mut row) => {
                    if drop_idx < row.len() {
                        row.remove(drop_idx);
                    }
                    encode_row(&row)?
                }
                DecodedRow::Versioned(mut v) => {
                    if drop_idx < v.values.len() {
                        v.values.remove(drop_idx);
                    }
                    encode_mvcc_row(&v)?
                }
            };
            table.update(tid, &Tuple::new(bytes))?;
            rewritten = rewritten.saturating_add(1);
        }
        Ok(rewritten)
    }
}

impl Executor for AlterTableExecutor {
    fn open(&mut self) -> Result<()> {
        self.done = false;
        self.affected_rows = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        if self.done {
            return Ok(None);
        }

        let mut current_name = self.table_name.clone();
        for op in &self.operations {
            match op {
                AlterTableOp::AddColumn {
                    column,
                    if_not_exists,
                } => {
                    let changed =
                        self.catalog
                            .add_column(&current_name, column.clone(), *if_not_exists)?;
                    if changed {
                        self.affected_rows = self
                            .affected_rows
                            .saturating_add(self.rewrite_table_rows_for_add_column(&current_name)?);
                    }
                }
                AlterTableOp::DropColumn { name, if_exists } => {
                    let idx = self.catalog.drop_column(&current_name, name, *if_exists)?;
                    if let Some(drop_idx) = idx {
                        self.affected_rows = self.affected_rows.saturating_add(
                            self.rewrite_table_rows_for_drop_column(&current_name, drop_idx)?,
                        );
                    }
                }
                AlterTableOp::RenameColumn { old_name, new_name } => {
                    self.catalog
                        .rename_column(&current_name, old_name, new_name)?;
                }
                AlterTableOp::RenameTable { new_name } => {
                    self.catalog.rename_table(&current_name, new_name)?;
                    current_name = new_name.clone();
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
