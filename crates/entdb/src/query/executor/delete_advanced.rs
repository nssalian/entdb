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
use crate::query::expression::{eval_predicate, BoundExpr};
use crate::query::plan::{BoundTableRef, ReturningBinding, SortKey};
use crate::storage::table::Table;
use crate::storage::tuple::{Tuple, TupleId};
use crate::types::Value;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
struct TableRow {
    tid: TupleId,
    version: MvccRow,
    expected: Vec<u8>,
}

#[derive(Clone)]
struct MatchRow {
    from_idx: Vec<usize>,
    combined: Vec<Value>,
}

pub struct DeleteAdvancedExecutor {
    targets: Vec<String>,
    from: Vec<BoundTableRef>,
    using: Vec<BoundTableRef>,
    filter: Option<BoundExpr>,
    order_by: Vec<SortKey>,
    limit: Option<usize>,
    returning: Option<ReturningBinding>,
    catalog: Arc<Catalog>,
    tx: Option<TxExecutionContext>,
    done: bool,
    affected_rows: u64,
    output_rows: Vec<Vec<Value>>,
    output_pos: usize,
    out_schema: Schema,
}

impl DeleteAdvancedExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        targets: Vec<String>,
        from: Vec<BoundTableRef>,
        using: Vec<BoundTableRef>,
        filter: Option<BoundExpr>,
        order_by: Vec<SortKey>,
        limit: Option<usize>,
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
            targets,
            from,
            using,
            filter,
            order_by,
            limit,
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

impl Executor for DeleteAdvancedExecutor {
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
        let out = self.output_rows[self.output_pos].clone();
        self.output_pos += 1;
        Ok(Some(out))
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

impl DeleteAdvancedExecutor {
    fn execute_all(&mut self) -> Result<()> {
        if self.from.is_empty() {
            return Err(EntDbError::Query("DELETE requires FROM tables".to_string()));
        }
        if self.returning.is_some() && self.targets.len() > 1 {
            return Err(EntDbError::Query(
                "DELETE ... RETURNING with multi-table DELETE is not supported".to_string(),
            ));
        }

        let tx_ref = self.tx.as_ref();
        let this_txn = tx_ref.map(|t| t.txn_id).unwrap_or(0);
        let mut from_tables = Vec::new();
        let mut from_rows = Vec::new();
        for from in &self.from {
            let t = Table::open(
                from.table.table_id,
                from.table.first_page_id,
                self.catalog.buffer_pool(),
            );
            from_rows.push(load_rows(&t, tx_ref)?);
            from_tables.push(t);
        }
        let mut using_rows = Vec::new();
        for using in &self.using {
            let t = Table::open(
                using.table.table_id,
                using.table.first_page_id,
                self.catalog.buffer_pool(),
            );
            using_rows.push(load_rows(&t, tx_ref)?);
        }

        let mut matches = Vec::new();
        let mut from_stack = Vec::new();
        let mut using_stack = Vec::new();
        enumerate_matches(
            &from_rows,
            &using_rows,
            0,
            0,
            &mut from_stack,
            &mut using_stack,
            self.filter.as_ref(),
            &mut matches,
        )?;

        if !self.order_by.is_empty() {
            sort_matches(&self.order_by, &mut matches)?;
        }
        if let Some(limit) = self.limit {
            matches.truncate(limit);
        }

        let mut delete_targets: HashMap<(u32, TupleId), (usize, TableRow, Vec<Value>)> =
            HashMap::new();
        for m in &matches {
            for target_alias in &self.targets {
                let Some(table_pos) = self
                    .from
                    .iter()
                    .position(|t| t.alias == *target_alias || t.table.name == *target_alias)
                else {
                    return Err(EntDbError::Query(format!(
                        "DELETE target '{}' not found in FROM clause",
                        target_alias
                    )));
                };
                let row_pos = m.from_idx[table_pos];
                let row = from_rows[table_pos][row_pos].clone();
                delete_targets.insert(
                    (self.from[table_pos].table.table_id, row.tid),
                    (table_pos, row, m.combined.clone()),
                );
            }
        }

        for ((table_id, tid), (table_pos, mut row, combined)) in delete_targets {
            if has_delete_conflict(&row.version, tx_ref) {
                return Err(EntDbError::Query(format!(
                    "write-write conflict on table '{}' tuple {:?}",
                    self.from[table_pos].table.name, tid
                )));
            }
            row.version.deleted_txn = Some(this_txn);
            let deleted = Tuple::new(encode_mvcc_row(&row.version)?);
            if let Some(table_exec_pos) =
                self.from.iter().position(|t| t.table.table_id == table_id)
            {
                let table = &from_tables[table_exec_pos];
                let cas = table.compare_and_update(tid, &row.expected, &deleted)?;
                if cas {
                    self.affected_rows = self.affected_rows.saturating_add(1);
                    if let Some(ret) = &self.returning {
                        self.output_rows.push(project_returning(ret, &combined));
                    }
                }
            }
        }

        Ok(())
    }
}

fn enumerate_matches(
    from_rows: &[Vec<TableRow>],
    using_rows: &[Vec<TableRow>],
    from_idx: usize,
    using_idx: usize,
    from_stack: &mut Vec<usize>,
    using_stack: &mut Vec<usize>,
    filter: Option<&BoundExpr>,
    out: &mut Vec<MatchRow>,
) -> Result<()> {
    if from_idx < from_rows.len() {
        for i in 0..from_rows[from_idx].len() {
            from_stack.push(i);
            enumerate_matches(
                from_rows,
                using_rows,
                from_idx + 1,
                using_idx,
                from_stack,
                using_stack,
                filter,
                out,
            )?;
            from_stack.pop();
        }
        return Ok(());
    }
    if using_idx < using_rows.len() {
        for i in 0..using_rows[using_idx].len() {
            using_stack.push(i);
            enumerate_matches(
                from_rows,
                using_rows,
                from_idx,
                using_idx + 1,
                from_stack,
                using_stack,
                filter,
                out,
            )?;
            using_stack.pop();
        }
        return Ok(());
    }

    let mut combined = Vec::new();
    for (table_pos, row_pos) in from_stack.iter().enumerate() {
        combined.extend(from_rows[table_pos][*row_pos].version.values.clone());
    }
    for (table_pos, row_pos) in using_stack.iter().enumerate() {
        combined.extend(using_rows[table_pos][*row_pos].version.values.clone());
    }
    let pass = match filter {
        None => true,
        Some(f) => eval_predicate(f, &combined)?,
    };
    if pass {
        out.push(MatchRow {
            from_idx: from_stack.clone(),
            combined,
        });
    }
    Ok(())
}

fn sort_matches(keys: &[SortKey], rows: &mut [MatchRow]) -> Result<()> {
    for i in 1..rows.len() {
        let mut j = i;
        while j > 0 {
            let ord = compare_rows(keys, &rows[j - 1].combined, &rows[j].combined)?;
            if ord == Ordering::Greater {
                rows.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
    }
    Ok(())
}

fn compare_rows(keys: &[SortKey], left: &[Value], right: &[Value]) -> Result<Ordering> {
    for key in keys {
        let l = left
            .get(key.col_idx)
            .ok_or_else(|| EntDbError::Query("ORDER BY column index out of bounds".to_string()))?;
        let r = right
            .get(key.col_idx)
            .ok_or_else(|| EntDbError::Query("ORDER BY column index out of bounds".to_string()))?;

        let ord = compare_values(l, r)?;
        if ord != Ordering::Equal {
            return Ok(if key.asc { ord } else { ord.reverse() });
        }
    }
    Ok(Ordering::Equal)
}

fn compare_values(left: &Value, right: &Value) -> Result<Ordering> {
    if left.eq(right)? {
        return Ok(Ordering::Equal);
    }
    if left.lt(right)? {
        return Ok(Ordering::Less);
    }
    Ok(Ordering::Greater)
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

fn load_rows(table: &Table, tx_ref: Option<&TxExecutionContext>) -> Result<Vec<TableRow>> {
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
        out.push(TableRow {
            tid,
            version,
            expected: tuple.data,
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
