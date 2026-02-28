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

pub mod alter_table;
pub mod analyze;
pub mod bm25_maintenance;
pub mod bm25_scan;
pub mod count;
pub mod create_index;
pub mod create_table;
pub mod delete;
pub mod delete_advanced;
pub mod drop_index;
pub mod drop_table;
pub mod filter;
pub mod group_aggregate;
pub mod group_count;
pub mod insert;
pub mod insert_from_select;
pub mod limit;
pub mod nested_loop_join;
pub mod project;
pub mod project_expr;
pub mod seq_scan;
pub mod sort;
pub mod truncate;
pub mod union;
pub mod update;
pub mod update_advanced;
pub mod upsert;
pub mod values;

use crate::catalog::{Catalog, Schema};
use crate::error::{EntDbError, Result};
use crate::query::plan::LogicalPlan;
use crate::tx::{TransactionManager, TxnId, TxnStatus};
use crate::types::Value;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub trait Executor {
    fn open(&mut self) -> Result<()>;
    fn next(&mut self) -> Result<Option<Vec<Value>>>;
    fn close(&mut self) -> Result<()>;
    fn schema(&self) -> &Schema;
    fn affected_rows(&self) -> Option<u64> {
        None
    }
}

pub struct ExecutionContext {
    pub catalog: Arc<Catalog>,
    pub tx: Option<TxExecutionContext>,
}

#[derive(Clone)]
pub struct TxExecutionContext {
    pub txn_id: TxnId,
    pub snapshot_ts: u64,
    pub txn_manager: Arc<TransactionManager>,
}

pub fn build_executor(plan: &LogicalPlan, ctx: &ExecutionContext) -> Result<Box<dyn Executor>> {
    match plan {
        LogicalPlan::Values {
            rows,
            output_schema,
        } => Ok(Box::new(values::ValuesExecutor::new(
            rows.clone(),
            output_schema.clone(),
        ))),
        LogicalPlan::SeqScan { table } => Ok(Box::new(seq_scan::SeqScanExecutor::new(
            table.clone(),
            Arc::clone(&ctx.catalog),
            ctx.tx.clone(),
        ))),
        LogicalPlan::Bm25Scan {
            table,
            index_name,
            terms,
            output_schema,
        } => Ok(Box::new(bm25_scan::Bm25ScanExecutor::new(
            table.clone(),
            index_name.clone(),
            terms.clone(),
            output_schema.clone(),
            Arc::clone(&ctx.catalog),
            ctx.tx.clone(),
        ))),
        LogicalPlan::NestedLoopJoin {
            left,
            right,
            left_on,
            right_on,
            output_schema,
        } => {
            let left_exec = build_executor(left, ctx)?;
            let right_exec = build_executor(right, ctx)?;
            Ok(Box::new(nested_loop_join::NestedLoopJoinExecutor::new(
                left_exec,
                right_exec,
                *left_on,
                *right_on,
                output_schema.clone(),
            )))
        }
        LogicalPlan::Filter { predicate, child } => {
            let child_exec = build_executor(child, ctx)?;
            Ok(Box::new(filter::FilterExecutor::new(
                predicate.clone(),
                child_exec,
            )))
        }
        LogicalPlan::Sort { keys, child } => {
            let child_exec = build_executor(child, ctx)?;
            Ok(Box::new(sort::SortExecutor::new(keys.clone(), child_exec)))
        }
        LogicalPlan::Union {
            left,
            right,
            all,
            output_schema,
        } => {
            let left_exec = build_executor(left, ctx)?;
            let right_exec = build_executor(right, ctx)?;
            Ok(Box::new(union::UnionExecutor::new(
                left_exec,
                right_exec,
                *all,
                output_schema.clone(),
            )))
        }
        LogicalPlan::Count { alias, child } => {
            let child_exec = build_executor(child, ctx)?;
            Ok(Box::new(count::CountExecutor::new(
                alias.clone(),
                child_exec,
            )))
        }
        LogicalPlan::GroupCount {
            group_col_idx,
            group_col_name,
            group_col_type,
            count_alias,
            child,
        } => {
            let child_exec = build_executor(child, ctx)?;
            Ok(Box::new(group_count::GroupCountExecutor::new(
                *group_col_idx,
                group_col_name.clone(),
                group_col_type.clone(),
                count_alias.clone(),
                child_exec,
            )))
        }
        LogicalPlan::GroupAggregate {
            group_keys,
            aggregates,
            output_schema: _,
            child,
        } => {
            let child_exec = build_executor(child, ctx)?;
            Ok(Box::new(group_aggregate::GroupAggregateExecutor::new(
                group_keys.clone(),
                aggregates.clone(),
                child_exec,
            )))
        }
        LogicalPlan::Project {
            projection,
            projection_names,
            output_schema,
            child,
        } => {
            let child_exec = build_executor(child, ctx)?;
            Ok(Box::new(project::ProjectExecutor::new(
                projection.clone(),
                projection_names.clone(),
                output_schema.clone(),
                child_exec,
            )))
        }
        LogicalPlan::ProjectExpr {
            exprs,
            projection_names,
            output_schema,
            child,
        } => {
            let child_exec = build_executor(child, ctx)?;
            Ok(Box::new(project_expr::ProjectExprExecutor::new(
                exprs.clone(),
                projection_names.clone(),
                output_schema.clone(),
                child_exec,
            )))
        }
        LogicalPlan::Limit {
            count,
            offset,
            child,
        } => {
            let child_exec = build_executor(child, ctx)?;
            Ok(Box::new(limit::LimitExecutor::new(
                *count, *offset, child_exec,
            )))
        }
        LogicalPlan::Insert { table, rows } => Ok(Box::new(insert::InsertExecutor::new(
            table.clone(),
            rows.clone(),
            Arc::clone(&ctx.catalog),
            ctx.tx.clone(),
        ))),
        LogicalPlan::InsertFromSelect { table, input } => {
            let input_exec = build_executor(input, ctx)?;
            Ok(Box::new(insert_from_select::InsertFromSelectExecutor::new(
                table.clone(),
                input_exec,
                Arc::clone(&ctx.catalog),
                ctx.tx.clone(),
            )))
        }
        LogicalPlan::Update {
            table,
            assignments,
            filter,
        } => Ok(Box::new(update::UpdateExecutor::new(
            table.clone(),
            assignments.clone(),
            filter.clone(),
            Arc::clone(&ctx.catalog),
            ctx.tx.clone(),
        ))),
        LogicalPlan::Delete { table, filter } => Ok(Box::new(delete::DeleteExecutor::new(
            table.clone(),
            filter.clone(),
            Arc::clone(&ctx.catalog),
            ctx.tx.clone(),
        ))),
        LogicalPlan::UpdateAdvanced {
            target,
            from,
            assignments,
            filter,
            returning,
        } => Ok(Box::new(update_advanced::UpdateAdvancedExecutor::new(
            target.clone(),
            from.clone(),
            assignments.clone(),
            filter.clone(),
            returning.clone(),
            Arc::clone(&ctx.catalog),
            ctx.tx.clone(),
        ))),
        LogicalPlan::DeleteAdvanced {
            targets,
            from,
            using,
            filter,
            order_by,
            limit,
            returning,
        } => Ok(Box::new(delete_advanced::DeleteAdvancedExecutor::new(
            targets.clone(),
            from.clone(),
            using.clone(),
            filter.clone(),
            order_by.clone(),
            *limit,
            returning.clone(),
            Arc::clone(&ctx.catalog),
            ctx.tx.clone(),
        ))),
        LogicalPlan::Upsert {
            table,
            rows,
            conflict_cols,
            action,
        } => Ok(Box::new(upsert::UpsertExecutor::new(
            table.clone(),
            rows.clone(),
            conflict_cols.clone(),
            action.clone(),
            Arc::clone(&ctx.catalog),
            ctx.tx.clone(),
        ))),
        LogicalPlan::Analyze { table } => {
            Ok(Box::new(analyze::AnalyzeExecutor::new(table.clone(), ctx)))
        }
        LogicalPlan::Truncate { tables } => Ok(Box::new(truncate::TruncateExecutor::new(
            tables.clone(),
            Arc::clone(&ctx.catalog),
            ctx.tx.clone(),
        ))),
        LogicalPlan::CreateTable { name, schema } => {
            Ok(Box::new(create_table::CreateTableExecutor::new(
                name.clone(),
                schema.clone(),
                Arc::clone(&ctx.catalog),
            )))
        }
        LogicalPlan::DropTable { name, if_exists } => Ok(Box::new(
            drop_table::DropTableExecutor::new(name.clone(), *if_exists, Arc::clone(&ctx.catalog)),
        )),
        LogicalPlan::CreateIndex {
            table_name,
            index_name,
            columns,
            unique,
            if_not_exists,
            index_type,
        } => Ok(Box::new(create_index::CreateIndexExecutor::new(
            table_name.clone(),
            index_name.clone(),
            columns.clone(),
            *unique,
            *if_not_exists,
            index_type.clone(),
            Arc::clone(&ctx.catalog),
        ))),
        LogicalPlan::DropIndex {
            index_name,
            if_exists,
        } => Ok(Box::new(drop_index::DropIndexExecutor::new(
            index_name.clone(),
            *if_exists,
            Arc::clone(&ctx.catalog),
        ))),
        LogicalPlan::AlterTable {
            table_name,
            operations,
        } => Ok(Box::new(alter_table::AlterTableExecutor::new(
            table_name.clone(),
            operations.clone(),
            Arc::clone(&ctx.catalog),
        ))),
    }
}

pub fn encode_row(row: &[Value]) -> Result<Vec<u8>> {
    serde_json::to_vec(row).map_err(|e| EntDbError::Query(format!("row encode failed: {e}")))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MvccRow {
    pub values: Vec<Value>,
    pub created_txn: TxnId,
    pub deleted_txn: Option<TxnId>,
}

#[derive(Debug, Clone)]
pub enum DecodedRow {
    Legacy(Vec<Value>),
    Versioned(MvccRow),
}

pub fn encode_mvcc_row(row: &MvccRow) -> Result<Vec<u8>> {
    serde_json::to_vec(row).map_err(|e| EntDbError::Query(format!("mvcc row encode failed: {e}")))
}

pub fn decode_row(bytes: &[u8]) -> Result<Vec<Value>> {
    serde_json::from_slice(bytes).map_err(|e| EntDbError::Query(format!("row decode failed: {e}")))
}

pub fn decode_stored_row(bytes: &[u8]) -> Result<DecodedRow> {
    if let Ok(versioned) = serde_json::from_slice::<MvccRow>(bytes) {
        return Ok(DecodedRow::Versioned(versioned));
    }
    Ok(DecodedRow::Legacy(decode_row(bytes)?))
}

pub fn row_visible(row: &MvccRow, tx: Option<&TxExecutionContext>) -> bool {
    let Some(tx) = tx else {
        // autocommit read path sees committed versions only
        return matches!(
            tx_status_created(row.created_txn, None),
            TxnStatus::Committed(_)
        ) && match row.deleted_txn {
            None => true,
            Some(del_txn) => !matches!(tx_status_created(del_txn, None), TxnStatus::Committed(_)),
        };
    };

    let created_status = tx_status_created(row.created_txn, Some(tx));
    let created_visible = if row.created_txn == tx.txn_id {
        true
    } else {
        match created_status {
            TxnStatus::Committed(ts) => ts <= tx.snapshot_ts,
            TxnStatus::Active { .. } => false,
            _ => false,
        }
    };
    if !created_visible {
        return false;
    }

    match row.deleted_txn {
        None => true,
        Some(del_txn) if del_txn == tx.txn_id => false,
        Some(del_txn) => match tx_status_created(del_txn, Some(tx)) {
            TxnStatus::Committed(ts) => ts > tx.snapshot_ts,
            TxnStatus::Active { .. } => true,
            TxnStatus::Aborted => true,
        },
    }
}

fn tx_status_created(txn_id: TxnId, tx: Option<&TxExecutionContext>) -> TxnStatus {
    if txn_id == 0 {
        return TxnStatus::Committed(0);
    }
    if let Some(tx) = tx {
        return tx.txn_manager.status(txn_id);
    }
    TxnStatus::Aborted
}
