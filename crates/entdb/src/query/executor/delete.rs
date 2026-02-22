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
use crate::query::executor::{
    decode_stored_row, encode_mvcc_row, row_visible, DecodedRow, Executor, MvccRow,
    TxExecutionContext,
};
use crate::query::expression::{eval_predicate, BoundExpr};
use crate::storage::table::Table;
use crate::types::Value;
use std::sync::Arc;

pub struct DeleteExecutor {
    table_info: TableInfo,
    filter: Option<BoundExpr>,
    catalog: Arc<Catalog>,
    tx: Option<TxExecutionContext>,
    done: bool,
    affected_rows: u64,
    out_schema: Schema,
}

impl DeleteExecutor {
    pub fn new(
        table_info: TableInfo,
        filter: Option<BoundExpr>,
        catalog: Arc<Catalog>,
        tx: Option<TxExecutionContext>,
    ) -> Self {
        Self {
            table_info,
            filter,
            catalog,
            tx,
            done: false,
            affected_rows: 0,
            out_schema: Schema { columns: vec![] },
        }
    }
}

impl Executor for DeleteExecutor {
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
        let this_txn = self.tx.as_ref().map(|t| t.txn_id).unwrap_or(0);
        let tx_ref = self.tx.as_ref();
        let mut targets = Vec::new();

        for (tid, tuple) in table.scan() {
            let stored = decode_stored_row(&tuple.data)?;
            let (row, version) = match stored {
                DecodedRow::Legacy(row) => (
                    row.clone(),
                    MvccRow {
                        values: row,
                        created_txn: 0,
                        deleted_txn: None,
                    },
                ),
                DecodedRow::Versioned(v) => (v.values.clone(), v),
            };

            if !row_visible(&version, tx_ref) {
                continue;
            }

            if let Some(filter) = &self.filter {
                if !eval_predicate(filter, &row)? {
                    continue;
                }
            }

            if has_delete_conflict(&version, tx_ref) {
                return Err(EntDbError::Query(format!(
                    "write-write conflict on table '{}' tuple {:?}",
                    self.table_info.name, tid
                )));
            }

            targets.push((tid, version, tuple.data.clone()));
        }

        for (tid, mut version, expected_bytes) in targets {
            let latest = table.get(tid)?;
            let latest_version = match decode_stored_row(&latest.data)? {
                DecodedRow::Legacy(values) => MvccRow {
                    values,
                    created_txn: 0,
                    deleted_txn: None,
                },
                DecodedRow::Versioned(v) => v,
            };
            if latest_version != version || has_delete_conflict(&latest_version, tx_ref) {
                return Err(EntDbError::Query(format!(
                    "write-write conflict on table '{}' tuple {:?}",
                    self.table_info.name, tid
                )));
            }

            version.deleted_txn = Some(this_txn);
            let marked_deleted = crate::storage::tuple::Tuple::new(encode_mvcc_row(&version)?);
            let cas_applied = table.compare_and_update(tid, &expected_bytes, &marked_deleted)?;
            if !cas_applied {
                return Err(EntDbError::Query(format!(
                    "write-write conflict on table '{}' tuple {:?}",
                    self.table_info.name, tid
                )));
            }
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

fn has_delete_conflict(version: &MvccRow, tx: Option<&TxExecutionContext>) -> bool {
    let Some(tx) = tx else {
        return false;
    };
    let Some(del_txn) = version.deleted_txn else {
        return false;
    };
    if del_txn == tx.txn_id {
        return false;
    }

    match tx.txn_manager.status(del_txn) {
        crate::tx::TxnStatus::Active { .. } => true,
        crate::tx::TxnStatus::Committed(ts) => ts > tx.snapshot_ts,
        crate::tx::TxnStatus::Aborted => false,
    }
}
