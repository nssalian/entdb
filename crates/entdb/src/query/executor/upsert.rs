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
use crate::query::expression::{eval_expr, eval_predicate};
use crate::query::plan::{UpdateAssignment, UpsertAction};
use crate::storage::table::Table;
use crate::storage::tuple::Tuple;
use crate::types::Value;
use std::sync::Arc;

pub struct UpsertExecutor {
    table_info: TableInfo,
    rows: Vec<Vec<Value>>,
    conflict_cols: Vec<usize>,
    action: UpsertAction,
    catalog: Arc<Catalog>,
    tx: Option<TxExecutionContext>,
    done: bool,
    affected_rows: u64,
    out_schema: Schema,
}

impl UpsertExecutor {
    pub fn new(
        table_info: TableInfo,
        rows: Vec<Vec<Value>>,
        conflict_cols: Vec<usize>,
        action: UpsertAction,
        catalog: Arc<Catalog>,
        tx: Option<TxExecutionContext>,
    ) -> Self {
        Self {
            table_info,
            rows,
            conflict_cols,
            action,
            catalog,
            tx,
            done: false,
            affected_rows: 0,
            out_schema: Schema { columns: vec![] },
        }
    }
}

impl Executor for UpsertExecutor {
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

        for input_row in &self.rows {
            let existing = find_conflict_row(&table, input_row, &self.conflict_cols, tx_ref)?;
            match existing {
                None => {
                    table.insert(&Tuple::new(encode_mvcc_row(&MvccRow {
                        values: input_row.clone(),
                        created_txn: this_txn,
                        deleted_txn: None,
                    })?))?;
                    self.affected_rows = self.affected_rows.saturating_add(1);
                }
                Some((tid, mut existing_version, expected_bytes)) => match &self.action {
                    UpsertAction::DoNothing => {}
                    UpsertAction::DoUpdate {
                        assignments,
                        selection,
                        excluded_offset,
                    } => {
                        apply_upsert_update(
                            &table,
                            &self.table_info,
                            tid,
                            &mut existing_version,
                            &expected_bytes,
                            input_row,
                            *excluded_offset,
                            assignments,
                            selection.as_ref(),
                            this_txn,
                            tx_ref,
                        )?;
                        self.affected_rows = self.affected_rows.saturating_add(1);
                    }
                },
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

fn apply_upsert_update(
    table: &Table,
    table_info: &TableInfo,
    tid: crate::storage::tuple::TupleId,
    existing_version: &mut MvccRow,
    expected_bytes: &[u8],
    excluded_row: &[Value],
    excluded_offset: usize,
    assignments: &[UpdateAssignment],
    selection: Option<&crate::query::expression::BoundExpr>,
    this_txn: u64,
    tx_ref: Option<&TxExecutionContext>,
) -> Result<()> {
    if has_delete_conflict(existing_version, tx_ref) {
        return Err(EntDbError::Query(format!(
            "write-write conflict on table '{}' tuple {:?}",
            table_info.name, tid
        )));
    }
    let mut eval_row = existing_version.values.clone();
    if excluded_offset != eval_row.len() {
        return Err(EntDbError::Query(
            "invalid excluded scope for UPSERT".to_string(),
        ));
    }
    eval_row.extend_from_slice(excluded_row);

    if let Some(predicate) = selection {
        if !eval_predicate(predicate, &eval_row)? {
            return Ok(());
        }
    }

    let mut updated_row = existing_version.values.clone();
    for assignment in assignments {
        let value = eval_expr(&assignment.expr, &eval_row)?;
        let target_col = &table_info.schema.columns[assignment.col_idx];
        let casted = value.cast_to(&target_col.data_type)?;
        updated_row[assignment.col_idx] = casted;
    }

    existing_version.deleted_txn = Some(this_txn);
    let marked_deleted = Tuple::new(encode_mvcc_row(existing_version)?);
    let cas_applied = table.compare_and_update(tid, expected_bytes, &marked_deleted)?;
    if !cas_applied {
        return Err(EntDbError::Query(format!(
            "write-write conflict on table '{}' tuple {:?}",
            table_info.name, tid
        )));
    }
    table.insert(&Tuple::new(encode_mvcc_row(&MvccRow {
        values: updated_row,
        created_txn: this_txn,
        deleted_txn: None,
    })?))?;
    Ok(())
}

fn find_conflict_row(
    table: &Table,
    input_row: &[Value],
    conflict_cols: &[usize],
    tx_ref: Option<&TxExecutionContext>,
) -> Result<Option<(crate::storage::tuple::TupleId, MvccRow, Vec<u8>)>> {
    for (tid, tuple) in table.scan() {
        let version = match decode_stored_row(&tuple.data)? {
            DecodedRow::Legacy(row) => MvccRow {
                values: row,
                created_txn: 0,
                deleted_txn: None,
            },
            DecodedRow::Versioned(v) => v,
        };
        if !row_visible(&version, tx_ref) {
            continue;
        }
        if conflict_match(&version.values, input_row, conflict_cols)? {
            return Ok(Some((tid, version, tuple.data)));
        }
    }
    Ok(None)
}

fn conflict_match(existing: &[Value], input: &[Value], cols: &[usize]) -> Result<bool> {
    for idx in cols {
        let a = existing
            .get(*idx)
            .ok_or_else(|| EntDbError::Query("conflict column index out of bounds".to_string()))?;
        let b = input
            .get(*idx)
            .ok_or_else(|| EntDbError::Query("conflict column index out of bounds".to_string()))?;
        if !a.eq(b)? {
            return Ok(false);
        }
    }
    Ok(true)
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
