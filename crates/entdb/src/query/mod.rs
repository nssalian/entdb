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

pub mod binder;
pub mod cardinality;
pub mod cost;
pub mod executor;
pub mod expression;
pub mod history;
pub mod optimizer;
pub mod plan;
pub mod planner;
pub mod polyglot;
pub mod prepared_fast;

#[cfg(test)]
mod tests;

use crate::catalog::Catalog;
use crate::error::{EntDbError, Result};
use crate::query::binder::Binder;
use crate::query::executor::delete::DeleteExecutor;
use crate::query::executor::insert::InsertExecutor;
use crate::query::executor::update::UpdateExecutor;
use crate::query::executor::{
    build_executor, decode_stored_row, row_visible, DecodedRow, ExecutionContext,
    TxExecutionContext,
};
use crate::query::expression::{BinaryOp, BoundExpr};
use crate::query::history::{OptimizerHistoryRecord, OptimizerHistoryRecorder};
use crate::query::optimizer::{Optimizer, OptimizerConfig, OptimizerTrace};
use crate::query::plan::UpdateAssignment;
use crate::query::planner::Planner;
use crate::query::polyglot::{transpile_with_meta, PolyglotOptions};
use crate::storage::table::Table;
use crate::tx::{DurabilityMode, TransactionHandle, TransactionManager, TxnStatus};
use crate::types::Value;
use parking_lot::Mutex;
use sqlparser::ast::{FromTable, Statement, TableObject};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone, PartialEq)]
pub enum QueryOutput {
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    AffectedRows(u64),
}

#[derive(Debug, Clone, Copy)]
pub struct VacuumPolicy {
    pub auto_trigger_deleted_versions: Option<u64>,
    pub force_trigger_deleted_versions: Option<u64>,
    pub min_interval_ms: u64,
    pub max_reclaims_per_run: Option<u64>,
}

impl Default for VacuumPolicy {
    fn default() -> Self {
        Self {
            auto_trigger_deleted_versions: None,
            force_trigger_deleted_versions: None,
            min_interval_ms: 0,
            max_reclaims_per_run: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ExecuteOptions {
    pub await_durable: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct PreparedStatement {
    statements: Option<Arc<Vec<Statement>>>,
    parse_error: Option<String>,
    fast_path: Option<prepared_fast::PreparedFastPath>,
}

#[derive(Debug, Clone)]
pub struct BulkUpdate {
    pub key: Value,
    pub assignments: Vec<(String, Value)>,
}

pub struct QueryEngine {
    catalog: Arc<Catalog>,
    txn_manager: Arc<TransactionManager>,
    current_txn: Mutex<Option<TransactionHandle>>,
    vacuum_policy: Mutex<VacuumPolicy>,
    deleted_versions_since_vacuum: AtomicU64,
    last_vacuum_reclaimed: AtomicU64,
    last_vacuum_epoch_ms: AtomicU64,
    polyglot: Mutex<PolyglotOptions>,
    optimizer_config: Mutex<OptimizerConfig>,
    last_optimizer_trace: Mutex<Option<OptimizerTrace>>,
    optimizer_history: OptimizerHistoryRecorder,
    read_plan_cache: Mutex<HashMap<String, CachedReadPlan>>,
    read_plan_cache_epoch: AtomicU64,
    read_plan_cache_max_entries: usize,
}

#[derive(Debug, Clone)]
struct CachedReadPlan {
    epoch: u64,
    fingerprint: String,
    chosen_plan_signature: String,
    trace: OptimizerTrace,
    plan: crate::query::plan::LogicalPlan,
}

impl QueryEngine {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self::with_txn_options(catalog, true, DurabilityMode::Full)
    }

    pub fn with_txn_persistence(catalog: Arc<Catalog>, durable: bool) -> Self {
        Self::with_txn_options(catalog, durable, DurabilityMode::Full)
    }

    pub fn with_txn_options(
        catalog: Arc<Catalog>,
        durable: bool,
        durability_mode: DurabilityMode,
    ) -> Self {
        let txn_manager = if durable {
            let state_path = txn_state_path_for_catalog(&catalog);
            let wal_path = txn_wal_path_for_catalog(&catalog);
            TransactionManager::with_wal_persistence(&state_path, &wal_path)
                .or_else(|_| TransactionManager::with_persistence(&state_path))
                .unwrap_or_else(|_| TransactionManager::new())
        } else {
            TransactionManager::new()
        };
        txn_manager.set_durability_mode(durability_mode);
        let optimizer_history_path = optimizer_history_path_for_catalog(&catalog);
        Self {
            catalog,
            txn_manager: Arc::new(txn_manager),
            current_txn: Mutex::new(None),
            vacuum_policy: Mutex::new(VacuumPolicy::default()),
            deleted_versions_since_vacuum: AtomicU64::new(0),
            last_vacuum_reclaimed: AtomicU64::new(0),
            last_vacuum_epoch_ms: AtomicU64::new(0),
            polyglot: Mutex::new(PolyglotOptions {
                enabled: std::env::var("ENTDB_POLYGLOT")
                    .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false),
            }),
            optimizer_config: Mutex::new(OptimizerConfig::default()),
            last_optimizer_trace: Mutex::new(None),
            optimizer_history: OptimizerHistoryRecorder::new(
                optimizer_history_path,
                optimizer_history_schema_hash(),
                16,
                1024,
            )
            .unwrap_or_else(|_| {
                OptimizerHistoryRecorder::new(
                    std::env::temp_dir().join("entdb.optimizer_history.fallback.json"),
                    optimizer_history_schema_hash(),
                    16,
                    1024,
                )
                .expect("fallback optimizer history recorder")
            }),
            read_plan_cache: Mutex::new(HashMap::new()),
            read_plan_cache_epoch: AtomicU64::new(0),
            read_plan_cache_max_entries: 512,
        }
        .with_txn_id_floor_from_storage()
    }

    pub fn execute(&self, sql: &str) -> Result<Vec<QueryOutput>> {
        self.execute_with_options(sql, ExecuteOptions::default())
    }

    pub fn execute_with_options(
        &self,
        sql: &str,
        options: ExecuteOptions,
    ) -> Result<Vec<QueryOutput>> {
        let statements = self.parse_sql_statements(sql)?;
        self.execute_statement_list_with_options(&statements, &[], options)
    }

    pub fn begin_txn(&self) -> TransactionHandle {
        self.txn_manager.begin()
    }

    pub fn commit_txn(&self, tx: TransactionHandle) -> Result<()> {
        self.commit_txn_with_options(tx, None)
    }

    pub fn commit_txn_with_options(
        &self,
        tx: TransactionHandle,
        await_durable: Option<bool>,
    ) -> Result<()> {
        self.txn_manager
            .commit_with_options(tx.txn_id, await_durable)?;
        Ok(())
    }

    pub fn abort_txn(&self, tx: TransactionHandle) {
        self.txn_manager.abort(tx.txn_id);
    }

    pub fn execute_in_txn(&self, tx: &TransactionHandle, sql: &str) -> Result<Vec<QueryOutput>> {
        let statements = self.parse_sql_statements(sql)?;

        let mut out = Vec::new();
        for stmt in &statements {
            match stmt {
                Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. } => {
                    return Err(EntDbError::Query(
                        "transaction control statements are managed via QueryEngine API"
                            .to_string(),
                    ));
                }
                _ => match self.execute_statement_in_txn_with_params(tx, stmt, &[]) {
                    Ok(o) => out.push(o),
                    Err(e) => {
                        self.record_optimizer_history(OptimizerHistoryRecord {
                            fingerprint: format!("stmt:{}", statement_kind(stmt)),
                            plan_signature: "error".to_string(),
                            schema_hash: optimizer_history_schema_hash().to_string(),
                            captured_at_ms: now_epoch_millis(),
                            rowcount_observed_json: "{\"root\":0}".to_string(),
                            latency_ms: 0,
                            memory_peak_bytes: 0,
                            success: false,
                            error_class: Some(error_class_for_error(&e)),
                            confidence: 0.0,
                        });
                        return Err(e);
                    }
                },
            }
        }
        Ok(out)
    }

    pub fn vacuum(&self) -> Result<u64> {
        let horizon = self
            .txn_manager
            .oldest_active_snapshot()
            .unwrap_or_else(|| self.txn_manager.latest_commit_ts());
        let mut removed = 0_u64;
        let max_reclaims = self.vacuum_policy.lock().max_reclaims_per_run;

        for table in self.catalog.list_tables() {
            let t = Table::open(
                table.table_id,
                table.first_page_id,
                self.catalog.buffer_pool(),
            );
            let mut purge = Vec::new();
            for (tid, tuple) in t.scan() {
                let DecodedRow::Versioned(v) = decode_stored_row(&tuple.data)? else {
                    continue;
                };

                if let Some(del_txn) = v.deleted_txn {
                    if let TxnStatus::Committed(ts) = self.txn_manager.status(del_txn) {
                        if ts <= horizon {
                            purge.push(tid);
                        }
                    }
                }
            }

            for tid in purge {
                t.delete(tid)?;
                removed = removed.saturating_add(1);
                if max_reclaims.is_some_and(|m| removed >= m) {
                    break;
                }
            }
            if max_reclaims.is_some_and(|m| removed >= m) {
                break;
            }
        }

        self.last_vacuum_reclaimed.store(removed, Ordering::Release);
        self.deleted_versions_since_vacuum
            .store(0, Ordering::Release);
        self.last_vacuum_epoch_ms
            .store(now_epoch_millis(), Ordering::Release);
        Ok(removed)
    }

    pub fn flush_all(&self) -> Result<()> {
        self.catalog.buffer_pool().flush_all()
    }

    pub fn flush_durable(&self) -> Result<()> {
        self.flush_all()?;
        self.txn_manager.flush_durable()
    }

    pub fn set_vacuum_policy(&self, policy: VacuumPolicy) {
        *self.vacuum_policy.lock() = policy;
    }

    pub fn vacuum_stats(&self) -> (u64, u64) {
        (
            self.deleted_versions_since_vacuum.load(Ordering::Acquire),
            self.last_vacuum_reclaimed.load(Ordering::Acquire),
        )
    }

    pub fn set_polyglot_enabled(&self, enabled: bool) {
        self.polyglot.lock().enabled = enabled;
        self.invalidate_read_plan_cache();
    }

    pub fn set_optimizer_config(&self, config: OptimizerConfig) {
        *self.optimizer_config.lock() = config.sanitize();
        self.invalidate_read_plan_cache();
    }

    pub fn disable_hbo(&self) {
        let mut cfg = self.optimizer_config();
        cfg.hbo_enabled = false;
        *self.optimizer_config.lock() = cfg;
        self.invalidate_read_plan_cache();
    }

    pub fn force_baseline_planner(&self) {
        let mut cfg = self.optimizer_config();
        cfg.cbo_enabled = false;
        *self.optimizer_config.lock() = cfg;
        self.invalidate_read_plan_cache();
    }

    pub fn optimizer_config(&self) -> OptimizerConfig {
        *self.optimizer_config.lock()
    }

    pub fn set_durability_mode(&self, mode: DurabilityMode) {
        self.txn_manager.set_durability_mode(mode);
    }

    pub fn durability_mode(&self) -> DurabilityMode {
        self.txn_manager.durability_mode()
    }

    pub fn set_checkpoint_interval(&self, interval: u64) {
        self.txn_manager.set_checkpoint_interval(interval);
    }

    pub fn last_optimizer_trace(&self) -> Option<OptimizerTrace> {
        self.last_optimizer_trace.lock().clone()
    }

    pub fn optimizer_history_for_fingerprint(
        &self,
        fingerprint: &str,
    ) -> Vec<OptimizerHistoryRecord> {
        self.optimizer_history.read_for_fingerprint(fingerprint)
    }

    pub fn optimizer_history_drop_stats(&self) -> (u64, u64) {
        (
            self.optimizer_history.dropped_count(),
            self.optimizer_history.worker_error_count(),
        )
    }

    pub fn clear_optimizer_history(&self) -> Result<()> {
        self.invalidate_read_plan_cache();
        self.optimizer_history.clear()
    }

    pub fn explain_optimizer_trace(&self, sql: &str) -> Result<Vec<OptimizerTrace>> {
        let transpiled = transpile_with_meta(sql, *self.polyglot.lock())?;
        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, &transpiled.transpiled_sql).map_err(|e| {
            if transpiled.changed {
                EntDbError::Query(format!(
                    "parse error: {e}; original_sql={:?}; transpiled_sql={:?}",
                    transpiled.original_sql, transpiled.transpiled_sql
                ))
            } else {
                EntDbError::Query(format!("parse error: {e}"))
            }
        })?;

        let mut traces = Vec::with_capacity(statements.len());
        let binder = Binder::new(Arc::clone(&self.catalog));
        let planner = Planner;
        let config = self.optimizer_config();
        for stmt in &statements {
            if matches!(
                stmt,
                Statement::StartTransaction { .. }
                    | Statement::Commit { .. }
                    | Statement::Rollback { .. }
            ) {
                return Err(EntDbError::Query(
                    "transaction control statements are managed via QueryEngine API".to_string(),
                ));
            }
            let bound = binder.bind(stmt)?;
            let fingerprint = Optimizer::fingerprint_bound_statement(&bound);
            let history = self.optimizer_history_for_fingerprint(&fingerprint);
            let plan = planner.plan(bound)?;
            let outcome =
                Optimizer::optimize_with_trace_and_history(plan, &fingerprint, config, &history);
            traces.push(outcome.trace);
        }
        Ok(traces)
    }

    pub fn explain_optimizer(&self, sql: &str) -> Result<Vec<QueryOutput>> {
        let transpiled = transpile_with_meta(sql, *self.polyglot.lock())?;
        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, &transpiled.transpiled_sql).map_err(|e| {
            if transpiled.changed {
                EntDbError::Query(format!(
                    "parse error: {e}; original_sql={:?}; transpiled_sql={:?}",
                    transpiled.original_sql, transpiled.transpiled_sql
                ))
            } else {
                EntDbError::Query(format!("parse error: {e}"))
            }
        })?;
        let mut outputs = Vec::new();
        let binder = Binder::new(Arc::clone(&self.catalog));
        let planner = Planner;
        let config = self.optimizer_config();
        for stmt in &statements {
            if matches!(
                stmt,
                Statement::StartTransaction { .. }
                    | Statement::Commit { .. }
                    | Statement::Rollback { .. }
            ) {
                return Err(EntDbError::Query(
                    "transaction control statements are managed via QueryEngine API".to_string(),
                ));
            }
            let bound = binder.bind(stmt)?;
            let fingerprint = Optimizer::fingerprint_bound_statement(&bound);
            let history = self.optimizer_history_for_fingerprint(&fingerprint);
            let plan = planner.plan(bound)?;
            let outcome = Optimizer::optimize_with_trace_and_history(
                plan.clone(),
                &fingerprint,
                config,
                &history,
            );
            let cost =
                Optimizer::explain_cost(&outcome.plan, crate::query::cost::CostWeights::default());
            let trace_json = serde_json::to_string(&outcome.trace)
                .map_err(|e| EntDbError::Query(format!("trace encode failed: {e}")))?;
            outputs.push(QueryOutput::Rows {
                columns: vec![
                    "fingerprint".to_string(),
                    "plan_signature".to_string(),
                    "estimated_rows".to_string(),
                    "estimated_cost".to_string(),
                    "trace_json".to_string(),
                ],
                rows: vec![vec![
                    Value::Text(fingerprint),
                    Value::Text(
                        outcome
                            .trace
                            .chosen_plan_signature
                            .unwrap_or_else(|| "baseline".to_string()),
                    ),
                    Value::Int64(cost.estimated_rows as i64),
                    Value::Float64(cost.estimated_cost),
                    Value::Text(trace_json),
                ]],
            });
        }
        Ok(outputs)
    }

    pub fn explain_optimizer_analyze(&self, sql: &str) -> Result<Vec<QueryOutput>> {
        let transpiled = transpile_with_meta(sql, *self.polyglot.lock())?;
        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, &transpiled.transpiled_sql).map_err(|e| {
            if transpiled.changed {
                EntDbError::Query(format!(
                    "parse error: {e}; original_sql={:?}; transpiled_sql={:?}",
                    transpiled.original_sql, transpiled.transpiled_sql
                ))
            } else {
                EntDbError::Query(format!("parse error: {e}"))
            }
        })?;
        let mut outputs = Vec::new();
        for stmt in &statements {
            if matches!(
                stmt,
                Statement::StartTransaction { .. }
                    | Statement::Commit { .. }
                    | Statement::Rollback { .. }
            ) {
                return Err(EntDbError::Query(
                    "transaction control statements are managed via QueryEngine API".to_string(),
                ));
            }
            let before = now_epoch_millis();
            let out = self.execute(&stmt.to_string())?;
            let elapsed = now_epoch_millis().saturating_sub(before);
            let observed_rows = out
                .iter()
                .find_map(|o| match o {
                    QueryOutput::Rows { rows, .. } => Some(rows.len() as u64),
                    QueryOutput::AffectedRows(n) => Some(*n),
                })
                .unwrap_or(0);

            let traces = self.explain_optimizer_trace(&stmt.to_string())?;
            let trace = traces
                .first()
                .cloned()
                .ok_or_else(|| EntDbError::Query("optimizer trace missing".to_string()))?;
            outputs.push(QueryOutput::Rows {
                columns: vec![
                    "fingerprint".to_string(),
                    "observed_rows".to_string(),
                    "elapsed_ms".to_string(),
                    "estimated_base_cost".to_string(),
                    "estimated_adjusted_cost".to_string(),
                    "history_matches".to_string(),
                ],
                rows: vec![vec![
                    Value::Text(trace.fingerprint),
                    Value::Int64(observed_rows as i64),
                    Value::Int64(elapsed as i64),
                    Value::Float64(trace.chosen_base_cost.unwrap_or(0.0)),
                    Value::Float64(trace.chosen_adjusted_cost.unwrap_or(0.0)),
                    Value::Int64(trace.history_matches as i64),
                ]],
            });
        }
        Ok(outputs)
    }

    pub fn optimizer_history_metrics(&self) -> QueryOutput {
        let (dropped, worker_errors) = self.optimizer_history_drop_stats();
        QueryOutput::Rows {
            columns: vec![
                "history_dropped".to_string(),
                "history_worker_errors".to_string(),
            ],
            rows: vec![vec![
                Value::Int64(dropped as i64),
                Value::Int64(worker_errors as i64),
            ]],
        }
    }

    fn parse_sql_statements(&self, sql: &str) -> Result<Vec<Statement>> {
        let transpiled = transpile_with_meta(sql, *self.polyglot.lock())?;
        let dialect = PostgreSqlDialect {};
        Parser::parse_sql(&dialect, &transpiled.transpiled_sql).map_err(|e| {
            if transpiled.changed {
                EntDbError::Query(format!(
                    "parse error: {e}; original_sql={:?}; transpiled_sql={:?}",
                    transpiled.original_sql, transpiled.transpiled_sql
                ))
            } else {
                EntDbError::Query(format!("parse error: {e}"))
            }
        })
    }

    fn execute_statement_in_txn_with_params(
        &self,
        tx: &TransactionHandle,
        stmt: &Statement,
        params: &[Value],
    ) -> Result<QueryOutput> {
        if tx.txn_id == 0 && statement_is_read_only(stmt) {
            if let Some(out) = self.execute_cached_read_statement_in_txn(tx, stmt)? {
                return Ok(out);
            }
        }

        let binder = Binder::new(Arc::clone(&self.catalog));
        let planner = Planner;

        let started = Instant::now();
        let bound = binder.bind_with_params(stmt, params)?;
        let fingerprint = Optimizer::fingerprint_bound_statement(&bound);
        let history = self.optimizer_history_for_fingerprint(&fingerprint);
        let plan = planner.plan(bound)?;
        let optimizer_config = self.optimizer_config();
        let optimized_outcome = Optimizer::optimize_with_trace_and_history(
            plan,
            &fingerprint,
            optimizer_config,
            &history,
        );
        let chosen_plan_signature = optimized_outcome
            .trace
            .chosen_plan_signature
            .clone()
            .unwrap_or_else(|| "baseline".to_string());
        *self.last_optimizer_trace.lock() = Some(optimized_outcome.trace.clone());
        let optimized = optimized_outcome.plan;
        if tx.txn_id == 0 && statement_is_read_only(stmt) {
            let cache_key = stmt.to_string();
            let epoch = self.read_plan_cache_epoch.load(Ordering::Acquire);
            let mut cache = self.read_plan_cache.lock();
            if cache.len() >= self.read_plan_cache_max_entries {
                cache.clear();
            }
            cache.insert(
                cache_key,
                CachedReadPlan {
                    epoch,
                    fingerprint: fingerprint.clone(),
                    chosen_plan_signature: chosen_plan_signature.clone(),
                    trace: optimized_outcome.trace.clone(),
                    plan: optimized.clone(),
                },
            );
        }

        let ctx = ExecutionContext {
            catalog: Arc::clone(&self.catalog),
            tx: Some(TxExecutionContext {
                txn_id: tx.txn_id,
                snapshot_ts: tx.snapshot_ts,
                txn_manager: Arc::clone(&self.txn_manager),
            }),
        };

        let mut exec = build_executor(&optimized, &ctx)?;
        exec.open()?;

        let mut rows = Vec::new();
        while let Some(row) = exec.next()? {
            rows.push(row);
        }
        let columns: Vec<String> = exec
            .schema()
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();
        let affected_rows = exec.affected_rows();
        exec.close()?;
        let latency_ms = started.elapsed().as_millis() as u64;
        let observed_rows = if !rows.is_empty() {
            rows.len() as u64
        } else {
            affected_rows.unwrap_or(0)
        };
        self.record_optimizer_history(OptimizerHistoryRecord {
            fingerprint,
            plan_signature: chosen_plan_signature,
            schema_hash: optimizer_history_schema_hash().to_string(),
            captured_at_ms: now_epoch_millis(),
            rowcount_observed_json: format!("{{\"root\":{observed_rows}}}"),
            latency_ms,
            memory_peak_bytes: 0,
            success: true,
            error_class: None,
            confidence: 1.0,
        });

        if statement_creates_deleted_versions(stmt) {
            self.deleted_versions_since_vacuum
                .fetch_add(affected_rows.unwrap_or(0), Ordering::SeqCst);
        }
        self.mark_stats_stale_for_statement(stmt, affected_rows.unwrap_or(1))?;
        if !statement_is_read_only(stmt) {
            self.invalidate_read_plan_cache();
        }

        if rows.is_empty() && columns.is_empty() {
            Ok(QueryOutput::AffectedRows(affected_rows.unwrap_or(0)))
        } else {
            Ok(QueryOutput::Rows { columns, rows })
        }
    }

    fn execute_cached_read_statement_in_txn(
        &self,
        tx: &TransactionHandle,
        stmt: &Statement,
    ) -> Result<Option<QueryOutput>> {
        let cache_key = stmt.to_string();
        let epoch = self.read_plan_cache_epoch.load(Ordering::Acquire);
        let Some(entry) = self.read_plan_cache.lock().get(&cache_key).cloned() else {
            return Ok(None);
        };
        if entry.epoch != epoch {
            return Ok(None);
        }

        *self.last_optimizer_trace.lock() = Some(entry.trace.clone());
        let started = Instant::now();
        let ctx = ExecutionContext {
            catalog: Arc::clone(&self.catalog),
            tx: Some(TxExecutionContext {
                txn_id: tx.txn_id,
                snapshot_ts: tx.snapshot_ts,
                txn_manager: Arc::clone(&self.txn_manager),
            }),
        };
        let mut exec = build_executor(&entry.plan, &ctx)?;
        exec.open()?;
        let mut rows = Vec::new();
        while let Some(row) = exec.next()? {
            rows.push(row);
        }
        let columns: Vec<String> = exec
            .schema()
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();
        let affected_rows = exec.affected_rows();
        exec.close()?;

        let latency_ms = started.elapsed().as_millis() as u64;
        let observed_rows = if !rows.is_empty() {
            rows.len() as u64
        } else {
            affected_rows.unwrap_or(0)
        };
        self.record_optimizer_history(OptimizerHistoryRecord {
            fingerprint: entry.fingerprint,
            plan_signature: entry.chosen_plan_signature,
            schema_hash: optimizer_history_schema_hash().to_string(),
            captured_at_ms: now_epoch_millis(),
            rowcount_observed_json: format!("{{\"root\":{observed_rows}}}"),
            latency_ms,
            memory_peak_bytes: 0,
            success: true,
            error_class: None,
            confidence: 1.0,
        });

        if rows.is_empty() && columns.is_empty() {
            Ok(Some(QueryOutput::AffectedRows(affected_rows.unwrap_or(0))))
        } else {
            Ok(Some(QueryOutput::Rows { columns, rows }))
        }
    }

    fn maybe_auto_vacuum(&self) -> Result<()> {
        let policy = *self.vacuum_policy.lock();
        let Some(threshold) = policy.auto_trigger_deleted_versions else {
            return Ok(());
        };
        if threshold == 0 {
            return Ok(());
        }

        let pending = self.deleted_versions_since_vacuum.load(Ordering::Acquire);
        let force_hit = policy
            .force_trigger_deleted_versions
            .is_some_and(|v| v > 0 && pending >= v);
        if pending < threshold && !force_hit {
            return Ok(());
        }

        let last = self.last_vacuum_epoch_ms.load(Ordering::Acquire);
        let now = now_epoch_millis();
        let too_soon = policy.min_interval_ms > 0
            && now.saturating_sub(last) < policy.min_interval_ms
            && !force_hit;
        if too_soon {
            return Ok(());
        }

        if pending >= threshold || force_hit {
            let _ = self.vacuum()?;
        }
        Ok(())
    }

    fn with_txn_id_floor_from_storage(self) -> Self {
        let mut max_seen = 0_u64;
        for table in self.catalog.list_tables() {
            let t = Table::open(
                table.table_id,
                table.first_page_id,
                self.catalog.buffer_pool(),
            );
            for (_, tuple) in t.scan() {
                if let Ok(DecodedRow::Versioned(v)) = decode_stored_row(&tuple.data) {
                    max_seen = max_seen.max(v.created_txn);
                    if let Some(del) = v.deleted_txn {
                        max_seen = max_seen.max(del);
                    }
                }
            }
        }
        self.txn_manager
            .ensure_min_next_txn_id(max_seen.saturating_add(1).max(1));
        self
    }

    fn mark_stats_stale_for_statement(&self, stmt: &Statement, mutations: u64) -> Result<()> {
        for name in mutated_tables(stmt) {
            if name.is_empty() {
                continue;
            }
            self.catalog.mark_table_stats_stale(&name, mutations)?;
        }
        Ok(())
    }

    fn record_optimizer_history(&self, entry: OptimizerHistoryRecord) {
        self.optimizer_history.try_record(entry);
    }

    fn invalidate_read_plan_cache(&self) {
        self.read_plan_cache_epoch.fetch_add(1, Ordering::AcqRel);
        self.read_plan_cache.lock().clear();
    }

    pub fn prepare(&self, sql_template: &str) -> PreparedStatement {
        match self.parse_sql_statements(sql_template) {
            Ok(statements) => PreparedStatement {
                fast_path: prepared_fast::PreparedFastPath::compile(&self.catalog, &statements),
                statements: Some(Arc::new(statements)),
                parse_error: None,
            },
            Err(err) => PreparedStatement {
                fast_path: None,
                statements: None,
                parse_error: Some(err.to_string()),
            },
        }
    }

    pub fn execute_prepared(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<Vec<QueryOutput>> {
        self.execute_prepared_with_options(prepared, params, ExecuteOptions::default())
    }

    pub fn execute_prepared_with_options(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<Vec<QueryOutput>> {
        if let Some(err) = prepared.parse_error.as_ref() {
            return Err(EntDbError::Query(format!(
                "prepared statement parse error: {err}"
            )));
        }
        let statements = prepared.statements.as_ref().ok_or_else(|| {
            EntDbError::Query("prepared statement has no parsed statements".to_string())
        })?;

        if let Some(fast_path) = prepared.fast_path.as_ref() {
            return self
                .execute_fast_prepared_with_options(fast_path, params, options)
                .map(|output| vec![output]);
        }

        self.execute_statement_list_with_options(&statements, params, options)
    }

    fn execute_fast_prepared_with_options(
        &self,
        fast_path: &prepared_fast::PreparedFastPath,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<QueryOutput> {
        let active_tx = *self.current_txn.lock();
        if let Some(tx) = active_tx {
            return self.execute_fast_prepared_in_txn(&tx, fast_path, params);
        }

        if fast_path.is_read_only() {
            let snapshot_tx = TransactionHandle {
                txn_id: 0,
                snapshot_ts: self.txn_manager.latest_commit_ts(),
            };
            return self.execute_fast_prepared_in_txn(&snapshot_tx, fast_path, params);
        }

        let tx = self.begin_txn();
        match self.execute_fast_prepared_in_txn(&tx, fast_path, params) {
            Ok(output) => {
                if let Err(err) = self.commit_txn_with_options(tx, options.await_durable) {
                    self.abort_txn(tx);
                    return Err(err);
                }
                self.maybe_auto_vacuum()?;
                Ok(output)
            }
            Err(err) => {
                self.abort_txn(tx);
                Err(err)
            }
        }
    }

    fn execute_fast_prepared_in_txn(
        &self,
        tx: &TransactionHandle,
        fast_path: &prepared_fast::PreparedFastPath,
        params: &[Value],
    ) -> Result<QueryOutput> {
        let output = fast_path.execute(self, tx, params)?;
        if fast_path.is_read_only() {
            return Ok(output);
        }

        let affected_rows = match &output {
            QueryOutput::AffectedRows(rows) => *rows,
            QueryOutput::Rows { rows, .. } => rows.len() as u64,
        };

        if fast_path.creates_deleted_versions() {
            self.deleted_versions_since_vacuum
                .fetch_add(affected_rows, Ordering::SeqCst);
        }
        if let Some(table_name) = fast_path.mutated_table_name() {
            self.catalog
                .mark_table_stats_stale(table_name, affected_rows.max(1))?;
        }
        self.invalidate_read_plan_cache();
        Ok(output)
    }

    pub fn insert_many(&self, table_name: &str, rows: &[Vec<Value>]) -> Result<u64> {
        self.insert_many_with_options(table_name, rows, ExecuteOptions::default())
    }

    pub fn insert_many_with_options(
        &self,
        table_name: &str,
        rows: &[Vec<Value>],
        options: ExecuteOptions,
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        let table = self.require_table(table_name)?;
        let normalized_rows = rows
            .iter()
            .map(|row| normalize_row_values(&table, row))
            .collect::<Result<Vec<_>>>()?;
        let affected = self.run_write_in_txn(options, |engine, tx| {
            let ctx = TxExecutionContext {
                txn_id: tx.txn_id,
                snapshot_ts: tx.snapshot_ts,
                txn_manager: Arc::clone(&engine.txn_manager),
            };
            let mut exec = InsertExecutor::new(
                table.clone(),
                normalized_rows.clone(),
                Arc::clone(&engine.catalog),
                Some(ctx),
            );
            run_affected_rows_executor(&mut exec)
        })?;
        self.catalog
            .mark_table_stats_stale(table_name, affected.max(1))?;
        self.invalidate_read_plan_cache();
        Ok(affected)
    }

    pub fn update_many(
        &self,
        table_name: &str,
        key_column: &str,
        updates: &[BulkUpdate],
    ) -> Result<u64> {
        self.update_many_with_options(table_name, key_column, updates, ExecuteOptions::default())
    }

    pub fn update_many_with_options(
        &self,
        table_name: &str,
        key_column: &str,
        updates: &[BulkUpdate],
        options: ExecuteOptions,
    ) -> Result<u64> {
        if updates.is_empty() {
            return Ok(0);
        }
        let table = self.require_table(table_name)?;
        let key_col_idx = table.schema.column_index(key_column).ok_or_else(|| {
            EntDbError::Query(format!(
                "column '{key_column}' does not exist on table '{table_name}'"
            ))
        })?;
        let key_type = table.schema.columns[key_col_idx].data_type.clone();
        let compiled_updates = updates
            .iter()
            .map(|update| {
                let key = update.key.cast_to(&key_type)?;
                let assignments = update
                    .assignments
                    .iter()
                    .map(|(column, value)| {
                        let col_idx = table.schema.column_index(column).ok_or_else(|| {
                            EntDbError::Query(format!(
                                "column '{column}' does not exist on table '{table_name}'"
                            ))
                        })?;
                        let casted = value.cast_to(&table.schema.columns[col_idx].data_type)?;
                        Ok(UpdateAssignment {
                            col_idx,
                            expr: BoundExpr::Literal(casted),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok((key, assignments))
            })
            .collect::<Result<Vec<_>>>()?;

        let affected = self.run_write_in_txn(options, |engine, tx| {
            let mut total = 0_u64;
            for (key, assignments) in &compiled_updates {
                let ctx = TxExecutionContext {
                    txn_id: tx.txn_id,
                    snapshot_ts: tx.snapshot_ts,
                    txn_manager: Arc::clone(&engine.txn_manager),
                };
                let filter = BoundExpr::BinaryOp {
                    op: BinaryOp::Eq,
                    left: Box::new(BoundExpr::ColumnRef {
                        col_idx: key_col_idx,
                    }),
                    right: Box::new(BoundExpr::Literal(key.clone())),
                };
                let mut exec = UpdateExecutor::new(
                    table.clone(),
                    assignments.clone(),
                    Some(filter),
                    Arc::clone(&engine.catalog),
                    Some(ctx),
                );
                total = total.saturating_add(run_affected_rows_executor(&mut exec)?);
            }
            Ok(total)
        })?;
        self.deleted_versions_since_vacuum
            .fetch_add(affected, Ordering::SeqCst);
        self.catalog
            .mark_table_stats_stale(table_name, affected.max(1))?;
        self.invalidate_read_plan_cache();
        Ok(affected)
    }

    pub fn delete_many(&self, table_name: &str, key_column: &str, keys: &[Value]) -> Result<u64> {
        self.delete_many_with_options(table_name, key_column, keys, ExecuteOptions::default())
    }

    pub fn delete_many_with_options(
        &self,
        table_name: &str,
        key_column: &str,
        keys: &[Value],
        options: ExecuteOptions,
    ) -> Result<u64> {
        if keys.is_empty() {
            return Ok(0);
        }
        let table = self.require_table(table_name)?;
        let key_col_idx = table.schema.column_index(key_column).ok_or_else(|| {
            EntDbError::Query(format!(
                "column '{key_column}' does not exist on table '{table_name}'"
            ))
        })?;
        let key_type = table.schema.columns[key_col_idx].data_type.clone();
        let normalized_keys = keys
            .iter()
            .map(|key| key.cast_to(&key_type))
            .collect::<Result<Vec<_>>>()?;

        let affected = self.run_write_in_txn(options, |engine, tx| {
            let mut total = 0_u64;
            for key in &normalized_keys {
                let ctx = TxExecutionContext {
                    txn_id: tx.txn_id,
                    snapshot_ts: tx.snapshot_ts,
                    txn_manager: Arc::clone(&engine.txn_manager),
                };
                let filter = BoundExpr::BinaryOp {
                    op: BinaryOp::Eq,
                    left: Box::new(BoundExpr::ColumnRef {
                        col_idx: key_col_idx,
                    }),
                    right: Box::new(BoundExpr::Literal(key.clone())),
                };
                let mut exec = DeleteExecutor::new(
                    table.clone(),
                    Some(filter),
                    Arc::clone(&engine.catalog),
                    Some(ctx),
                );
                total = total.saturating_add(run_affected_rows_executor(&mut exec)?);
            }
            Ok(total)
        })?;
        self.deleted_versions_since_vacuum
            .fetch_add(affected, Ordering::SeqCst);
        self.catalog
            .mark_table_stats_stale(table_name, affected.max(1))?;
        self.invalidate_read_plan_cache();
        Ok(affected)
    }

    fn require_table(&self, table_name: &str) -> Result<crate::catalog::TableInfo> {
        self.catalog
            .get_table(table_name)
            .ok_or_else(|| EntDbError::Query(format!("table '{table_name}' does not exist")))
    }

    fn run_write_in_txn<F>(&self, options: ExecuteOptions, f: F) -> Result<u64>
    where
        F: FnOnce(&Self, &TransactionHandle) -> Result<u64>,
    {
        let active_tx = *self.current_txn.lock();
        if let Some(tx) = active_tx {
            return f(self, &tx);
        }

        let tx = self.begin_txn();
        match f(self, &tx) {
            Ok(affected) => {
                if let Err(err) = self.commit_txn_with_options(tx, options.await_durable) {
                    self.abort_txn(tx);
                    return Err(err);
                }
                self.maybe_auto_vacuum()?;
                Ok(affected)
            }
            Err(err) => {
                self.abort_txn(tx);
                Err(err)
            }
        }
    }

    fn execute_statement_list_with_options(
        &self,
        statements: &[Statement],
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<Vec<QueryOutput>> {
        let mut outputs = Vec::new();
        for stmt in statements {
            match stmt {
                Statement::StartTransaction { .. } => {
                    let mut guard = self.current_txn.lock();
                    if guard.is_some() {
                        return Err(EntDbError::Query(
                            "transaction already active for this session".to_string(),
                        ));
                    }
                    *guard = Some(self.begin_txn());
                    outputs.push(QueryOutput::AffectedRows(0));
                }
                Statement::Commit { .. } => {
                    let tx = {
                        let mut guard = self.current_txn.lock();
                        guard.take().ok_or_else(|| {
                            EntDbError::Query("no active transaction to COMMIT".to_string())
                        })?
                    };
                    if let Err(e) = self.commit_txn_with_options(tx, options.await_durable) {
                        self.abort_txn(tx);
                        return Err(e);
                    }
                    self.maybe_auto_vacuum()?;
                    outputs.push(QueryOutput::AffectedRows(0));
                }
                Statement::Rollback { .. } => {
                    let tx = {
                        let mut guard = self.current_txn.lock();
                        guard.take().ok_or_else(|| {
                            EntDbError::Query("no active transaction to ROLLBACK".to_string())
                        })?
                    };
                    self.abort_txn(tx);
                    outputs.push(QueryOutput::AffectedRows(0));
                }
                _ => {
                    let active_tx = *self.current_txn.lock();
                    if let Some(tx) = active_tx {
                        match self.execute_statement_in_txn_with_params(&tx, stmt, params) {
                            Ok(out) => outputs.push(out),
                            Err(e) => {
                                self.record_optimizer_history(OptimizerHistoryRecord {
                                    fingerprint: format!("stmt:{}", statement_kind(stmt)),
                                    plan_signature: "error".to_string(),
                                    schema_hash: optimizer_history_schema_hash().to_string(),
                                    captured_at_ms: now_epoch_millis(),
                                    rowcount_observed_json: "{\"root\":0}".to_string(),
                                    latency_ms: 0,
                                    memory_peak_bytes: 0,
                                    success: false,
                                    error_class: Some(error_class_for_error(&e)),
                                    confidence: 0.0,
                                });
                                return Err(e);
                            }
                        }
                    } else {
                        if statement_is_read_only(stmt) {
                            let snapshot_tx = TransactionHandle {
                                txn_id: 0,
                                snapshot_ts: self.txn_manager.latest_commit_ts(),
                            };
                            match self.execute_statement_in_txn_with_params(
                                &snapshot_tx,
                                stmt,
                                params,
                            ) {
                                Ok(output) => outputs.push(output),
                                Err(e) => {
                                    self.record_optimizer_history(OptimizerHistoryRecord {
                                        fingerprint: format!("stmt:{}", statement_kind(stmt)),
                                        plan_signature: "error".to_string(),
                                        schema_hash: optimizer_history_schema_hash().to_string(),
                                        captured_at_ms: now_epoch_millis(),
                                        rowcount_observed_json: "{\"root\":0}".to_string(),
                                        latency_ms: 0,
                                        memory_peak_bytes: 0,
                                        success: false,
                                        error_class: Some(error_class_for_error(&e)),
                                        confidence: 0.0,
                                    });
                                    return Err(e);
                                }
                            }
                        } else {
                            let tx = self.begin_txn();
                            match self.execute_statement_in_txn_with_params(&tx, stmt, params) {
                                Ok(output) => {
                                    if let Err(e) =
                                        self.commit_txn_with_options(tx, options.await_durable)
                                    {
                                        self.abort_txn(tx);
                                        return Err(e);
                                    }
                                    self.maybe_auto_vacuum()?;
                                    outputs.push(output);
                                }
                                Err(e) => {
                                    self.abort_txn(tx);
                                    self.record_optimizer_history(OptimizerHistoryRecord {
                                        fingerprint: format!("stmt:{}", statement_kind(stmt)),
                                        plan_signature: "error".to_string(),
                                        schema_hash: optimizer_history_schema_hash().to_string(),
                                        captured_at_ms: now_epoch_millis(),
                                        rowcount_observed_json: "{\"root\":0}".to_string(),
                                        latency_ms: 0,
                                        memory_peak_bytes: 0,
                                        success: false,
                                        error_class: Some(error_class_for_error(&e)),
                                        confidence: 0.0,
                                    });
                                    return Err(e);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(outputs)
    }
}

fn normalize_row_values(table: &crate::catalog::TableInfo, row: &[Value]) -> Result<Vec<Value>> {
    if row.len() != table.schema.columns.len() {
        return Err(EntDbError::Query(format!(
            "row for table '{}' has {} values but schema expects {}",
            table.name,
            row.len(),
            table.schema.columns.len()
        )));
    }

    row.iter()
        .zip(&table.schema.columns)
        .map(|(value, column)| value.cast_to(&column.data_type))
        .collect()
}

fn run_affected_rows_executor(exec: &mut dyn executor::Executor) -> Result<u64> {
    exec.open()?;
    while exec.next()?.is_some() {}
    let affected = exec.affected_rows().unwrap_or(0);
    exec.close()?;
    Ok(affected)
}

fn txn_state_path_for_catalog(catalog: &Catalog) -> PathBuf {
    let mut p = catalog.buffer_pool().disk_path().to_path_buf();
    p.set_extension("txn.json");
    p
}

fn txn_wal_path_for_catalog(catalog: &Catalog) -> PathBuf {
    let mut p = catalog.buffer_pool().disk_path().to_path_buf();
    p.set_extension("txn.wal");
    p
}

fn optimizer_history_path_for_catalog(catalog: &Catalog) -> PathBuf {
    let mut p = catalog.buffer_pool().disk_path().to_path_buf();
    p.set_extension("optimizer_history.json");
    p
}

fn optimizer_history_schema_hash() -> &'static str {
    "optimizer_history_schema_v1_planner_v1"
}

fn statement_creates_deleted_versions(stmt: &Statement) -> bool {
    match stmt {
        Statement::Update { .. } | Statement::Delete { .. } | Statement::Truncate { .. } => true,
        Statement::Insert(insert) => matches!(
            &insert.on,
            Some(sqlparser::ast::OnInsert::OnConflict(
                sqlparser::ast::OnConflict {
                    action: sqlparser::ast::OnConflictAction::DoUpdate(_),
                    ..
                }
            ))
        ),
        _ => false,
    }
}

fn mutated_tables(stmt: &Statement) -> Vec<String> {
    match stmt {
        Statement::Insert(insert) => match &insert.table {
            TableObject::TableName(name) => vec![object_name_to_string(name)],
            TableObject::TableFunction(_) => Vec::new(),
        },
        Statement::Update { table, .. } => vec![table_name_from_table_with_joins(table)],
        Statement::Delete(delete) => match &delete.from {
            FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => {
                t.iter().map(table_name_from_table_with_joins).collect()
            }
        },
        Statement::Truncate { table_names, .. } => table_names
            .iter()
            .map(|t| object_name_to_string(&t.name))
            .collect(),
        Statement::AlterTable { name, .. } => vec![object_name_to_string(name)],
        _ => Vec::new(),
    }
}

fn table_name_from_table_with_joins(table: &sqlparser::ast::TableWithJoins) -> String {
    match &table.relation {
        sqlparser::ast::TableFactor::Table { name, .. } => object_name_to_string(name),
        _ => String::new(),
    }
}

fn object_name_to_string(name: &sqlparser::ast::ObjectName) -> String {
    name.0
        .iter()
        .map(|p| p.as_ident().map(|i| i.value.clone()).unwrap_or_default())
        .collect::<Vec<_>>()
        .join(".")
}

fn statement_kind(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Query(_) => "query",
        Statement::Insert(_) => "insert",
        Statement::Update { .. } => "update",
        Statement::Delete(_) => "delete",
        Statement::Truncate { .. } => "truncate",
        Statement::CreateTable(_) => "create_table",
        Statement::CreateIndex(_) => "create_index",
        Statement::Drop { .. } => "drop",
        Statement::AlterTable { .. } => "alter_table",
        Statement::Analyze { .. } => "analyze",
        Statement::StartTransaction { .. } => "begin",
        Statement::Commit { .. } => "commit",
        Statement::Rollback { .. } => "rollback",
        _ => "other",
    }
}

fn statement_is_read_only(stmt: &Statement) -> bool {
    matches!(stmt, Statement::Query(_))
}

fn error_class_for_error(err: &EntDbError) -> String {
    match err {
        EntDbError::Io(_) => "io",
        EntDbError::PageNotFound(_) => "page_not_found",
        EntDbError::BufferPoolFull => "buffer_pool_full",
        EntDbError::InvalidPage(_) => "invalid_page",
        EntDbError::PagePinned(_) => "page_pinned",
        EntDbError::PageAlreadyPresent(_) => "page_already_present",
        EntDbError::Corruption(_) => "corruption",
        EntDbError::Wal(_) => "wal",
        EntDbError::Query(_) => "query",
    }
    .to_string()
}

fn now_epoch_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[allow(dead_code)]
fn _debug_row_visible_example(
    engine: &QueryEngine,
    row: &executor::MvccRow,
    tx: &TransactionHandle,
) -> bool {
    row_visible(
        row,
        Some(&TxExecutionContext {
            txn_id: tx.txn_id,
            snapshot_ts: tx.snapshot_ts,
            txn_manager: Arc::clone(&engine.txn_manager),
        }),
    )
}
