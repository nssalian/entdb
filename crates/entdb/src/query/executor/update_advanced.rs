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
    decode_stored_row, encode_mvcc_row, row_visible, DecodedRow, Executor, MvccRow,
    TxExecutionContext,
};
use crate::query::expression::{eval_expr, eval_predicate, BoundExpr};
use crate::query::plan::{BoundTableRef, ReturningBinding, UpdateAssignment};
use crate::storage::table::Table;
use crate::storage::tuple::{Tuple, TupleId};
use crate::types::Value;
use std::sync::Arc;

#[derive(Clone)]
struct TargetRow {
    tid: TupleId,
    version: MvccRow,
    expected_bytes: Vec<u8>,
}

#[derive(Clone)]
struct SourceRow {
    values: Vec<Value>,
}

pub struct UpdateAdvancedExecutor {
    target: BoundTableRef,
    from: Vec<BoundTableRef>,
    assignments: Vec<UpdateAssignment>,
    filter: Option<BoundExpr>,
    returning: Option<ReturningBinding>,
    catalog: Arc<Catalog>,
    tx: Option<TxExecutionContext>,
    done: bool,
    affected_rows: u64,
    output_rows: Vec<Vec<Value>>,
    output_pos: usize,
    out_schema: Schema,
}

impl UpdateAdvancedExecutor {
    pub fn new(
        target: BoundTableRef,
        from: Vec<BoundTableRef>,
        assignments: Vec<UpdateAssignment>,
        filter: Option<BoundExpr>,
        returning: Option<ReturningBinding>,
        catalog: Arc<Catalog>,
        tx: Option<TxExecutionContext>,
    ) -> Self {
        let out_schema = match &returning {
            Some(r) => Schema {
                columns: r
                    .projection_names
                    .iter()
                    .map(|n| crate::catalog::Column {
                        name: n.clone(),
                        data_type: crate::types::DataType::Null,
                        nullable: true,
                        default: None,
                        primary_key: false,
                    })
                    .collect(),
            },
            None => Schema { columns: vec![] },
        };
        Self {
            target,
            from,
            assignments,
            filter,
            returning,
            catalog,
            tx,
            done: false,
            affected_rows: 0,
            output_rows: Vec::new(),
            output_pos: 0,
            out_schema,
        }
    }
}

impl Executor for UpdateAdvancedExecutor {
    fn open(&mut self) -> Result<()> {
        self.done = false;
        self.affected_rows = 0;
        self.output_rows.clear();
        self.output_pos = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        if !self.done {
            self.execute_all()?;
            self.done = true;
        }

        if self.output_pos >= self.output_rows.len() {
            return Ok(None);
        }
        let row = self.output_rows[self.output_pos].clone();
        self.output_pos += 1;
        Ok(Some(row))
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

impl UpdateAdvancedExecutor {
    fn execute_all(&mut self) -> Result<()> {
        let target_table = Table::open(
            self.target.table.table_id,
            self.target.table.first_page_id,
            self.catalog.buffer_pool(),
        );
        let tx_ref = self.tx.as_ref();
        let this_txn = tx_ref.map(|t| t.txn_id).unwrap_or(0);
        let target_rows = load_target_rows(&target_table, tx_ref)?;

        let mut from_rows = Vec::with_capacity(self.from.len());
        for from in &self.from {
            let t = Table::open(
                from.table.table_id,
                from.table.first_page_id,
                self.catalog.buffer_pool(),
            );
            from_rows.push(load_source_rows(&t, tx_ref)?);
        }

        for target in target_rows {
            let mut best_combo = None;
            if self.from.is_empty() {
                let combined = target.version.values.clone();
                let pass = match self.filter.as_ref() {
                    None => true,
                    Some(f) => eval_predicate(f, &combined)?,
                };
                if pass {
                    best_combo = Some(Vec::new());
                }
            } else {
                let mut current = Vec::new();
                find_first_combo(
                    &target.version.values,
                    &from_rows,
                    0,
                    &mut current,
                    self.filter.as_ref(),
                    &mut best_combo,
                )?;
            }

            let Some(combo) = best_combo else {
                continue;
            };

            if has_delete_conflict(&target.version, tx_ref) {
                return Err(EntDbError::Query(format!(
                    "write-write conflict on table '{}' tuple {:?}",
                    self.target.table.name, target.tid
                )));
            }

            let mut eval_row = target.version.values.clone();
            for row_idx in &combo {
                eval_row.extend(from_rows[row_idx.0][row_idx.1].values.clone());
            }
            let mut updated_values = target.version.values.clone();
            for assignment in &self.assignments {
                let value = eval_expr(&assignment.expr, &eval_row)?;
                let target_col = &self.target.table.schema.columns[assignment.col_idx];
                updated_values[assignment.col_idx] = value.cast_to(&target_col.data_type)?;
            }

            let mut deleted = target.version.clone();
            deleted.deleted_txn = Some(this_txn);
            let deleted_tuple = Tuple::new(encode_mvcc_row(&deleted)?);
            let cas = target_table.compare_and_update(
                target.tid,
                &target.expected_bytes,
                &deleted_tuple,
            )?;
            if !cas {
                return Err(EntDbError::Query(format!(
                    "write-write conflict on table '{}' tuple {:?}",
                    self.target.table.name, target.tid
                )));
            }
            target_table.insert(&Tuple::new(encode_mvcc_row(&MvccRow {
                values: updated_values.clone(),
                created_txn: this_txn,
                deleted_txn: None,
            })?))?;
            self.affected_rows = self.affected_rows.saturating_add(1);

            if let Some(ret) = &self.returning {
                let mut combined = updated_values;
                for row_idx in combo {
                    combined.extend(from_rows[row_idx.0][row_idx.1].values.clone());
                }
                self.output_rows.push(project_returning(ret, &combined));
            }
        }

        Ok(())
    }
}

fn project_returning(ret: &ReturningBinding, row: &[Value]) -> Vec<Value> {
    match &ret.projection {
        None => row.to_vec(),
        Some(idxs) => idxs
            .iter()
            .map(|idx| row.get(*idx).cloned().unwrap_or(Value::Null))
            .collect(),
    }
}

fn find_first_combo(
    target_values: &[Value],
    from_rows: &[Vec<SourceRow>],
    table_idx: usize,
    current: &mut Vec<(usize, usize)>,
    filter: Option<&BoundExpr>,
    out: &mut Option<Vec<(usize, usize)>>,
) -> Result<()> {
    if out.is_some() {
        return Ok(());
    }
    if table_idx == from_rows.len() {
        let mut combined = target_values.to_vec();
        for (t, r) in current.iter() {
            combined.extend(from_rows[*t][*r].values.clone());
        }
        let pass = match filter {
            None => true,
            Some(expr) => eval_predicate(expr, &combined)?,
        };
        if pass {
            *out = Some(current.clone());
        }
        return Ok(());
    }

    for idx in 0..from_rows[table_idx].len() {
        current.push((table_idx, idx));
        find_first_combo(
            target_values,
            from_rows,
            table_idx + 1,
            current,
            filter,
            out,
        )?;
        current.pop();
        if out.is_some() {
            break;
        }
    }
    Ok(())
}

fn load_target_rows(table: &Table, tx_ref: Option<&TxExecutionContext>) -> Result<Vec<TargetRow>> {
    let mut out = Vec::new();
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
        out.push(TargetRow {
            tid,
            version,
            expected_bytes: tuple.data,
        });
    }
    Ok(out)
}

fn load_source_rows(table: &Table, tx_ref: Option<&TxExecutionContext>) -> Result<Vec<SourceRow>> {
    let mut out = Vec::new();
    for (_, tuple) in table.scan() {
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
        out.push(SourceRow {
            values: version.values,
        });
    }
    Ok(out)
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
