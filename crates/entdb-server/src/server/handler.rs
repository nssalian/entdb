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

use crate::server::type_map::{encode_value, ent_to_pg_type};
use async_trait::async_trait;
use entdb::catalog::Catalog;
use entdb::error::EntDbError;
use entdb::query::binder::Binder;
use entdb::query::executor::{build_executor, ExecutionContext, TxExecutionContext};
use entdb::query::history::OptimizerHistoryRecord;
use entdb::query::optimizer::Optimizer;
use entdb::query::planner::Planner;
use entdb::query::polyglot::{transpile_with_meta, PolyglotOptions};
use entdb::storage::table::Table;
use entdb::tx::TransactionHandle;
use futures::{stream, Sink};
use parking_lot::Mutex;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use sqlparser::ast::{visit_expressions_mut, Expr, Statement, Value as SqlValue};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::fmt::Debug;
use std::ops::ControlFlow;
use std::sync::Arc;
use tracing::{debug, info_span};

use super::metrics::ServerMetrics;
use super::{optimizer_history_schema_hash, Database};

pub struct EntHandler {
    db: Arc<Database>,
    current_txn: Mutex<Option<TransactionHandle>>,
    query_parser: Arc<NoopQueryParser>,
    max_statement_bytes: usize,
    query_timeout_ms: u64,
    metrics: Arc<ServerMetrics>,
    polyglot: PolyglotOptions,
}

impl EntHandler {
    pub fn new(
        db: Arc<Database>,
        max_statement_bytes: usize,
        query_timeout_ms: u64,
        metrics: Arc<ServerMetrics>,
    ) -> Self {
        Self {
            db,
            current_txn: Mutex::new(None),
            query_parser: Arc::new(NoopQueryParser::new()),
            max_statement_bytes,
            query_timeout_ms,
            metrics,
            polyglot: PolyglotOptions {
                enabled: std::env::var("ENTDB_POLYGLOT")
                    .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false),
            },
        }
    }

    fn execute_sql(
        &self,
        sql: &str,
        max_rows: Option<usize>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        if sql.len() > self.max_statement_bytes {
            return Err(user_error(
                "54000",
                format!(
                    "statement exceeds configured max_statement_bytes ({} > {})",
                    sql.len(),
                    self.max_statement_bytes
                ),
            ));
        }
        let _span = info_span!(
            "pgwire.execute_sql",
            sql_len = sql.len(),
            max_rows = max_rows.unwrap_or(0)
        )
        .entered();
        let statements = parse_sql_with_polyglot(sql, self.polyglot)?;
        let mut responses = Vec::new();
        let started = std::time::Instant::now();

        for stmt in &statements {
            if started.elapsed().as_millis() as u64 > self.query_timeout_ms {
                return Err(user_error("57014", "query timeout exceeded"));
            }
            debug!(statement = %tag_for_statement_name(stmt), "processing statement");
            match stmt {
                Statement::StartTransaction { .. } => {
                    let mut guard = self.current_txn.lock();
                    if guard.is_some() {
                        return Err(user_error(
                            "25001",
                            "transaction already active for this session",
                        ));
                    }
                    *guard = Some(self.db.txn_manager.begin());
                    responses.push(Response::TransactionStart(Tag::new("BEGIN")));
                }
                Statement::Commit { .. } => {
                    let tx = {
                        let mut guard = self.current_txn.lock();
                        guard
                            .take()
                            .ok_or_else(|| user_error("25000", "no active transaction to COMMIT"))?
                    };
                    self.db
                        .txn_manager
                        .commit(tx.txn_id)
                        .map_err(map_entdb_error)?;
                    responses.push(Response::TransactionEnd(Tag::new("COMMIT")));
                }
                Statement::Rollback { .. } => {
                    let tx = {
                        let mut guard = self.current_txn.lock();
                        guard.take().ok_or_else(|| {
                            user_error("25000", "no active transaction to ROLLBACK")
                        })?
                    };
                    self.db.txn_manager.abort(tx.txn_id);
                    responses.push(Response::TransactionEnd(Tag::new("ROLLBACK")));
                }
                _ => {
                    let active_tx = *self.current_txn.lock();
                    let exec_resp = if let Some(tx) = active_tx {
                        self.execute_statement_in_txn(&tx, stmt, max_rows, None)?
                    } else {
                        let tx = self.db.txn_manager.begin();
                        match self.execute_statement_in_txn(&tx, stmt, max_rows, None) {
                            Ok(resp) => {
                                if let Err(e) = self.db.txn_manager.commit(tx.txn_id) {
                                    self.db.txn_manager.abort(tx.txn_id);
                                    return Err(map_entdb_error(e));
                                }
                                resp
                            }
                            Err(e) => {
                                self.db.txn_manager.abort(tx.txn_id);
                                return Err(e);
                            }
                        }
                    };
                    responses.push(exec_resp);
                }
            }
        }

        Ok(responses)
    }

    fn execute_statement_in_txn(
        &self,
        tx: &TransactionHandle,
        stmt: &Statement,
        max_rows: Option<usize>,
        row_format: Option<&Format>,
    ) -> PgWireResult<Response<'static>> {
        let stmt_started = std::time::Instant::now();
        let _span = info_span!(
            "pgwire.execute_statement",
            statement = %tag_for_statement_name(stmt),
            txn_id = tx.txn_id
        )
        .entered();
        let binder = Binder::new(Arc::clone(&self.db.catalog));
        let planner = Planner;

        let bound = match binder.bind(stmt) {
            Ok(b) => b,
            Err(e) => {
                self.record_optimizer_history_error(stmt, "bind_error", stmt_started.elapsed());
                return Err(map_entdb_error(e));
            }
        };
        let fingerprint = Optimizer::fingerprint_bound_statement(&bound);
        let history = self.db.optimizer_history.read_for_fingerprint(&fingerprint);
        self.ensure_statement_timeout(stmt_started)?;
        let plan = match planner.plan(bound) {
            Ok(p) => p,
            Err(e) => {
                self.record_optimizer_history(OptimizerHistoryRecord {
                    fingerprint,
                    plan_signature: "error".to_string(),
                    schema_hash: optimizer_history_schema_hash().to_string(),
                    captured_at_ms: now_epoch_millis(),
                    rowcount_observed_json: "{\"root\":0}".to_string(),
                    latency_ms: stmt_started.elapsed().as_millis() as u64,
                    memory_peak_bytes: 0,
                    success: false,
                    error_class: Some("plan_error".to_string()),
                    confidence: 0.0,
                });
                return Err(map_entdb_error(e));
            }
        };
        self.ensure_statement_timeout(stmt_started)?;
        let optimized_outcome = Optimizer::optimize_with_trace_and_history(
            plan,
            &fingerprint,
            self.db.optimizer_config,
            &history,
        );
        let chosen_plan_signature = optimized_outcome
            .trace
            .chosen_plan_signature
            .clone()
            .unwrap_or_else(|| "baseline".to_string());
        let optimized = optimized_outcome.plan;
        self.ensure_statement_timeout(stmt_started)?;

        let ctx = ExecutionContext {
            catalog: Arc::clone(&self.db.catalog),
            tx: Some(TxExecutionContext {
                txn_id: tx.txn_id,
                snapshot_ts: tx.snapshot_ts,
                txn_manager: Arc::clone(&self.db.txn_manager),
            }),
        };

        let mut exec = match build_executor(&optimized, &ctx) {
            Ok(exec) => exec,
            Err(e) => {
                self.record_optimizer_history(OptimizerHistoryRecord {
                    fingerprint,
                    plan_signature: chosen_plan_signature,
                    schema_hash: optimizer_history_schema_hash().to_string(),
                    captured_at_ms: now_epoch_millis(),
                    rowcount_observed_json: "{\"root\":0}".to_string(),
                    latency_ms: stmt_started.elapsed().as_millis() as u64,
                    memory_peak_bytes: 0,
                    success: false,
                    error_class: Some("executor_build_error".to_string()),
                    confidence: 0.0,
                });
                return Err(map_entdb_error(e));
            }
        };
        self.ensure_statement_timeout(stmt_started)?;
        if let Err(e) = exec.open() {
            self.record_optimizer_history(OptimizerHistoryRecord {
                fingerprint,
                plan_signature: chosen_plan_signature,
                schema_hash: optimizer_history_schema_hash().to_string(),
                captured_at_ms: now_epoch_millis(),
                rowcount_observed_json: "{\"root\":0}".to_string(),
                latency_ms: stmt_started.elapsed().as_millis() as u64,
                memory_peak_bytes: 0,
                success: false,
                error_class: Some("executor_open_error".to_string()),
                confidence: 0.0,
            });
            return Err(map_entdb_error(e));
        }
        self.ensure_statement_timeout(stmt_started)?;

        let schema = exec.schema().clone();
        let mut rows = Vec::new();
        while let Some(row) = match exec.next() {
            Ok(r) => r,
            Err(e) => {
                self.record_optimizer_history(OptimizerHistoryRecord {
                    fingerprint: fingerprint.clone(),
                    plan_signature: chosen_plan_signature.clone(),
                    schema_hash: optimizer_history_schema_hash().to_string(),
                    captured_at_ms: now_epoch_millis(),
                    rowcount_observed_json: format!("{{\"root\":{}}}", rows.len()),
                    latency_ms: stmt_started.elapsed().as_millis() as u64,
                    memory_peak_bytes: 0,
                    success: false,
                    error_class: Some("executor_next_error".to_string()),
                    confidence: 0.0,
                });
                return Err(map_entdb_error(e));
            }
        } {
            self.ensure_statement_timeout(stmt_started)?;
            rows.push(row);
            if let Some(limit) = max_rows {
                if limit > 0 && rows.len() >= limit {
                    break;
                }
            }
        }
        if let Err(e) = exec.close() {
            self.record_optimizer_history(OptimizerHistoryRecord {
                fingerprint,
                plan_signature: chosen_plan_signature,
                schema_hash: optimizer_history_schema_hash().to_string(),
                captured_at_ms: now_epoch_millis(),
                rowcount_observed_json: format!("{{\"root\":{}}}", rows.len()),
                latency_ms: stmt_started.elapsed().as_millis() as u64,
                memory_peak_bytes: 0,
                success: false,
                error_class: Some("executor_close_error".to_string()),
                confidence: 0.0,
            });
            return Err(map_entdb_error(e));
        }

        let observed_rows = if schema.columns.is_empty() {
            exec.affected_rows().unwrap_or(0)
        } else {
            rows.len() as u64
        };
        self.record_optimizer_history(OptimizerHistoryRecord {
            fingerprint,
            plan_signature: chosen_plan_signature,
            schema_hash: optimizer_history_schema_hash().to_string(),
            captured_at_ms: now_epoch_millis(),
            rowcount_observed_json: format!("{{\"root\":{observed_rows}}}"),
            latency_ms: stmt_started.elapsed().as_millis() as u64,
            memory_peak_bytes: 0,
            success: true,
            error_class: None,
            confidence: 1.0,
        });

        if schema.columns.is_empty() {
            let affected = exec.affected_rows().unwrap_or(0) as usize;
            let tag = execution_tag_for_statement(stmt, affected);
            return Ok(Response::Execution(tag));
        }

        let fields = schema
            .columns
            .iter()
            .enumerate()
            .map(|(idx, c)| {
                FieldInfo::new(
                    c.name.clone(),
                    None,
                    None,
                    ent_to_pg_type(&c.data_type),
                    row_format
                        .map(|f| f.format_for(idx))
                        .unwrap_or(FieldFormat::Text),
                )
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(fields);

        let mut encoded = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut encoder = DataRowEncoder::new(Arc::clone(&schema));
            for (idx, value) in row.iter().enumerate() {
                let field = &schema[idx];
                encode_value(&mut encoder, value, field.datatype(), field.format())?;
            }
            encoded.push(encoder.finish()?);
        }

        let data_row_stream = stream::iter(encoded.into_iter().map(Ok));
        let mut query = QueryResponse::new(schema, data_row_stream);
        query.set_command_tag(tag_for_statement_name(stmt));
        Ok(Response::Query(query))
    }

    fn describe_statement_from_stmt(
        &self,
        stmt: &Statement,
        format: &Format,
    ) -> PgWireResult<Vec<FieldInfo>> {
        if !statement_returns_rows(stmt) {
            return Ok(Vec::new());
        }

        let tx = self.db.txn_manager.begin();
        let binder = Binder::new(Arc::clone(&self.db.catalog));
        let planner = Planner;

        let bound = binder.bind(stmt).map_err(map_entdb_error)?;
        let fingerprint = Optimizer::fingerprint_bound_statement(&bound);
        let history = self.db.optimizer_history.read_for_fingerprint(&fingerprint);
        let plan = planner.plan(bound).map_err(map_entdb_error)?;
        let optimized = Optimizer::optimize_with_trace_and_history(
            plan,
            &fingerprint,
            self.db.optimizer_config,
            &history,
        )
        .plan;

        let ctx = ExecutionContext {
            catalog: Arc::clone(&self.db.catalog),
            tx: Some(TxExecutionContext {
                txn_id: tx.txn_id,
                snapshot_ts: tx.snapshot_ts,
                txn_manager: Arc::clone(&self.db.txn_manager),
            }),
        };

        let exec = build_executor(&optimized, &ctx).map_err(map_entdb_error)?;
        let fields = exec
            .schema()
            .columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                FieldInfo::new(
                    col.name.clone(),
                    None,
                    None,
                    ent_to_pg_type(&col.data_type),
                    format.format_for(idx),
                )
            })
            .collect::<Vec<_>>();

        self.db.txn_manager.abort(tx.txn_id);
        Ok(fields)
    }

    fn describe_statement_inner(&self, sql: &str, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
        let stmt = parse_single_statement_with_polyglot(
            sql,
            "describe supports a single statement",
            self.polyglot,
        )?;
        self.describe_statement_from_stmt(&stmt, format)
    }
}

impl EntHandler {
    fn record_optimizer_history(&self, entry: OptimizerHistoryRecord) {
        self.db.optimizer_history.try_record(entry);
    }

    fn record_optimizer_history_error(
        &self,
        stmt: &Statement,
        class: &str,
        elapsed: std::time::Duration,
    ) {
        self.record_optimizer_history(OptimizerHistoryRecord {
            fingerprint: format!("stmt:{}", tag_for_statement_name(stmt)),
            plan_signature: "error".to_string(),
            schema_hash: optimizer_history_schema_hash().to_string(),
            captured_at_ms: now_epoch_millis(),
            rowcount_observed_json: "{\"root\":0}".to_string(),
            latency_ms: elapsed.as_millis() as u64,
            memory_peak_bytes: 0,
            success: false,
            error_class: Some(class.to_string()),
            confidence: 0.0,
        });
    }
}

fn now_epoch_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[async_trait]
impl SimpleQueryHandler for EntHandler {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let started = std::time::Instant::now();
        let result = self.execute_sql(query, None);
        self.record_query_metric(started, result.as_ref().err());
        result
    }
}

#[async_trait]
impl ExtendedQueryHandler for EntHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::clone(&self.query_parser)
    }

    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let started = std::time::Instant::now();
        let result = (|| -> PgWireResult<Response<'a>> {
            let stmt = statement_from_portal(portal, self.polyglot)?;
            if matches!(
                stmt,
                Statement::StartTransaction { .. }
                    | Statement::Commit { .. }
                    | Statement::Rollback { .. }
            ) {
                let mut responses = self.execute_sql(&stmt.to_string(), Some(max_rows))?;
                if responses.is_empty() {
                    return Ok(Response::EmptyQuery);
                }
                return Ok(responses.remove(0));
            }

            let active_tx = *self.current_txn.lock();
            if let Some(tx) = active_tx {
                return self.execute_statement_in_txn(
                    &tx,
                    &stmt,
                    Some(max_rows),
                    Some(&portal.result_column_format),
                );
            }

            let tx = self.db.txn_manager.begin();
            match self.execute_statement_in_txn(
                &tx,
                &stmt,
                Some(max_rows),
                Some(&portal.result_column_format),
            ) {
                Ok(resp) => {
                    if let Err(e) = self.db.txn_manager.commit(tx.txn_id) {
                        self.db.txn_manager.abort(tx.txn_id);
                        Err(map_entdb_error(e))
                    } else {
                        Ok(resp)
                    }
                }
                Err(e) => {
                    self.db.txn_manager.abort(tx.txn_id);
                    Err(e)
                }
            }
        })();
        self.record_query_metric(started, result.as_ref().err());
        result
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let mut parameter_types = stmt.parameter_types.clone();
        let inferred_count = max_placeholder_index(&stmt.statement);
        if parameter_types.len() < inferred_count {
            parameter_types.resize(inferred_count, Type::INT4);
        }
        for ty in &mut parameter_types {
            if *ty == Type::UNKNOWN {
                *ty = Type::INT4;
            }
        }

        let fields = self
            .describe_statement_inner(&stmt.statement, &Format::UnifiedBinary)
            .or_else(|_| {
                let normalized = normalize_placeholders_for_describe(&stmt.statement);
                self.describe_statement_inner(&normalized, &Format::UnifiedBinary)
            })
            .unwrap_or_default();
        Ok(DescribeStatementResponse::new(parameter_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let stmt = statement_from_portal(portal, self.polyglot)?;
        let fields = self.describe_statement_from_stmt(&stmt, &portal.result_column_format)?;
        Ok(DescribePortalResponse::new(fields))
    }
}

#[cfg(test)]
fn parse_sql(sql: &str) -> PgWireResult<Vec<Statement>> {
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, sql).map_err(|e| user_error("42601", format!("parse error: {e}")))
}

fn parse_sql_with_polyglot(sql: &str, opts: PolyglotOptions) -> PgWireResult<Vec<Statement>> {
    let transpiled = transpile_with_meta(sql, opts).map_err(map_entdb_error)?;
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, &transpiled.transpiled_sql).map_err(|e| {
        if transpiled.changed {
            user_error(
                "42601",
                format!(
                    "parse error: {e}; original_sql={:?}; transpiled_sql={:?}",
                    transpiled.original_sql, transpiled.transpiled_sql
                ),
            )
        } else {
            user_error("42601", format!("parse error: {e}"))
        }
    })
}

impl EntHandler {
    fn ensure_statement_timeout(&self, started: std::time::Instant) -> PgWireResult<()> {
        if started.elapsed().as_millis() as u64 > self.query_timeout_ms {
            return Err(user_error("57014", "query timeout exceeded"));
        }
        Ok(())
    }

    fn record_query_metric(&self, started: std::time::Instant, err: Option<&PgWireError>) {
        let elapsed = started.elapsed().as_nanos() as u64;
        let sqlstate = err.and_then(sqlstate_from_error);
        self.metrics.on_query_finished(elapsed, sqlstate);
    }
}

fn sqlstate_from_error(err: &PgWireError) -> Option<&str> {
    match err {
        PgWireError::UserError(info) => Some(info.code.as_str()),
        _ => Some("XX000"),
    }
}

#[cfg(test)]
fn parse_single_statement(sql: &str, message: &'static str) -> PgWireResult<Statement> {
    let mut statements = parse_sql(sql)?;
    if statements.len() != 1 {
        return Err(user_error("0A000", message));
    }
    Ok(statements.remove(0))
}

fn parse_single_statement_with_polyglot(
    sql: &str,
    message: &'static str,
    opts: PolyglotOptions,
) -> PgWireResult<Statement> {
    let mut statements = parse_sql_with_polyglot(sql, opts)?;
    if statements.len() != 1 {
        return Err(user_error("0A000", message));
    }
    Ok(statements.remove(0))
}

fn statement_from_portal(
    portal: &Portal<String>,
    opts: PolyglotOptions,
) -> PgWireResult<Statement> {
    let mut stmt = parse_single_statement_with_polyglot(
        &portal.statement.statement,
        "extended protocol supports a single statement per execute",
        opts,
    )?;
    if portal.parameter_len() == 0 {
        return Ok(stmt);
    }

    let values = collect_portal_values(portal)?;
    bind_statement_placeholders(&mut stmt, &values)?;
    Ok(stmt)
}

fn collect_portal_values(portal: &Portal<String>) -> PgWireResult<Vec<SqlValue>> {
    let mut values = Vec::with_capacity(portal.parameter_len());
    for i in 0..portal.parameter_len() {
        let pg_type = portal
            .statement
            .parameter_types
            .get(i)
            .unwrap_or(&Type::UNKNOWN);
        values.push(parameter_sql_value(portal, i, pg_type)?);
    }
    Ok(values)
}

fn bind_statement_placeholders(stmt: &mut Statement, values: &[SqlValue]) -> PgWireResult<()> {
    let mut question_mark_idx = 0usize;
    let outcome = visit_expressions_mut(stmt, |expr| {
        if let Expr::Value(v) = expr {
            if let SqlValue::Placeholder(placeholder) = &v.value {
                let idx = match placeholder_index(placeholder, values.len(), &mut question_mark_idx)
                {
                    Ok(idx) => idx,
                    Err(err) => return ControlFlow::Break(err),
                };
                v.value = values[idx - 1].clone();
            }
        }
        ControlFlow::Continue(())
    });

    match outcome {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(err) => Err(err),
    }
}

fn placeholder_index(
    placeholder: &str,
    total: usize,
    question_mark_idx: &mut usize,
) -> PgWireResult<usize> {
    let idx = if placeholder == "?" {
        *question_mark_idx += 1;
        *question_mark_idx
    } else if let Some(raw) = placeholder.strip_prefix('$') {
        raw.parse::<usize>()
            .map_err(|_| user_error("22023", format!("invalid placeholder '{placeholder}'")))?
    } else {
        return Err(user_error(
            "22023",
            format!("unsupported placeholder syntax '{placeholder}'"),
        ));
    };

    if idx == 0 || idx > total {
        return Err(user_error(
            "22023",
            format!("placeholder '{placeholder}' is out of range"),
        ));
    }
    Ok(idx)
}

fn parameter_sql_value(
    portal: &Portal<String>,
    idx: usize,
    pg_type: &Type,
) -> PgWireResult<SqlValue> {
    if *pg_type == Type::UNKNOWN {
        return parameter_sql_value_unknown(portal, idx);
    }
    if *pg_type == Type::BOOL {
        return Ok(match portal.parameter::<bool>(idx, pg_type)? {
            Some(v) => SqlValue::Boolean(v),
            None => SqlValue::Null,
        });
    }
    if *pg_type == Type::INT2 {
        return Ok(match portal.parameter::<i16>(idx, pg_type)? {
            Some(v) => SqlValue::Number(v.to_string(), false),
            None => SqlValue::Null,
        });
    }
    if *pg_type == Type::INT4 {
        return Ok(match portal.parameter::<i32>(idx, pg_type)? {
            Some(v) => SqlValue::Number(v.to_string(), false),
            None => SqlValue::Null,
        });
    }
    if *pg_type == Type::INT8 {
        return Ok(match portal.parameter::<i64>(idx, pg_type)? {
            Some(v) => SqlValue::Number(v.to_string(), false),
            None => SqlValue::Null,
        });
    }
    if *pg_type == Type::FLOAT4 {
        return Ok(match portal.parameter::<f32>(idx, pg_type)? {
            Some(v) => SqlValue::Number((v as f64).to_string(), false),
            None => SqlValue::Null,
        });
    }
    if *pg_type == Type::FLOAT8 {
        return Ok(match portal.parameter::<f64>(idx, pg_type)? {
            Some(v) => SqlValue::Number(v.to_string(), false),
            None => SqlValue::Null,
        });
    }

    Ok(match portal.parameter::<String>(idx, pg_type)? {
        Some(v) => SqlValue::SingleQuotedString(v),
        None => SqlValue::Null,
    })
}

fn parameter_sql_value_unknown(portal: &Portal<String>, idx: usize) -> PgWireResult<SqlValue> {
    let Some(raw) = portal.parameters.get(idx) else {
        return Ok(SqlValue::Null);
    };
    let Some(bytes) = raw else {
        return Ok(SqlValue::Null);
    };

    if portal.parameter_format.is_binary(idx) {
        match bytes.len() {
            1 => {
                let b = bytes[0];
                if b == 0 || b == 1 {
                    return Ok(SqlValue::Boolean(b == 1));
                }
            }
            4 => {
                let v = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                return Ok(SqlValue::Number(v.to_string(), false));
            }
            8 => {
                let v = i64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                return Ok(SqlValue::Number(v.to_string(), false));
            }
            _ => {}
        }
    }

    match std::str::from_utf8(bytes) {
        Ok(s) => {
            if let Ok(i) = s.parse::<i64>() {
                return Ok(SqlValue::Number(i.to_string(), false));
            }
            if let Ok(f) = s.parse::<f64>() {
                return Ok(SqlValue::Number(f.to_string(), false));
            }
            if s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("false") {
                return Ok(SqlValue::Boolean(s.eq_ignore_ascii_case("true")));
            }
            Ok(SqlValue::SingleQuotedString(s.to_string()))
        }
        Err(_) => Ok(SqlValue::SingleQuotedString(
            String::from_utf8_lossy(bytes).into_owned(),
        )),
    }
}

fn tag_for_statement(stmt: &Statement) -> Tag {
    Tag::new(tag_for_statement_name(stmt))
}

fn statement_returns_rows(stmt: &Statement) -> bool {
    match stmt {
        Statement::Query(_) => true,
        Statement::Insert(insert) => insert.returning.is_some(),
        Statement::Update { returning, .. } => returning.is_some(),
        Statement::Delete(delete) => delete.returning.is_some(),
        _ => false,
    }
}

fn execution_tag_for_statement(stmt: &Statement, affected: usize) -> Tag {
    match stmt {
        // PostgreSQL command-complete format for INSERT is "INSERT 0 <rows>".
        Statement::Insert(_) => Tag::new("INSERT 0").with_rows(affected),
        _ => tag_for_statement(stmt).with_rows(affected),
    }
}

fn tag_for_statement_name(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Query(_) => "SELECT",
        Statement::Insert(_) => "INSERT",
        Statement::Update { .. } => "UPDATE",
        Statement::Delete { .. } => "DELETE",
        Statement::Truncate { .. } => "TRUNCATE",
        Statement::CreateTable { .. } => "CREATE TABLE",
        Statement::CreateIndex(_) => "CREATE INDEX",
        Statement::AlterTable { .. } => "ALTER TABLE",
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Table,
            ..
        } => "DROP TABLE",
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Index,
            ..
        } => "DROP INDEX",
        Statement::StartTransaction { .. } => "BEGIN",
        Statement::Commit { .. } => "COMMIT",
        Statement::Rollback { .. } => "ROLLBACK",
        _ => "OK",
    }
}

fn map_entdb_error(err: EntDbError) -> PgWireError {
    let (code, message) = match err {
        EntDbError::Query(m) => (query_sqlstate(&m), m),
        EntDbError::BufferPoolFull => ("53200", "buffer pool full".to_string()),
        EntDbError::PagePinned(pid) => ("55006", format!("page {pid} is pinned")),
        EntDbError::PageNotFound(pid) => ("XX000", format!("page {pid} not found")),
        EntDbError::Io(e) => ("58000", format!("io error: {e}")),
        EntDbError::Wal(m) => ("XX000", format!("wal error: {m}")),
        EntDbError::Corruption(m) => ("XX001", format!("corruption: {m}")),
        EntDbError::InvalidPage(m) => ("XX000", format!("invalid page: {m}")),
        EntDbError::PageAlreadyPresent(pid) => ("XX000", format!("page {pid} already present")),
    };
    user_error(code, message)
}

fn query_sqlstate(message: &str) -> &'static str {
    let lower = message.to_ascii_lowercase();
    if lower.contains("parse error") {
        return "42601";
    }
    if lower.contains("already active") {
        return "25001";
    }
    if lower.contains("no active transaction") {
        return "25000";
    }
    if lower.contains("write-write conflict") {
        return "40001";
    }
    if lower.contains("does not exist") {
        if lower.contains("table") {
            return "42P01";
        }
        if lower.contains("column") {
            return "42703";
        }
    }
    if lower.contains("cannot cast value")
        || lower.contains("invalid")
        || lower.contains("unsupported")
        || lower.contains("must be numeric")
    {
        return "22000";
    }
    "XX000"
}

fn user_error(code: impl Into<String>, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        code.into(),
        message.into(),
    )))
}

fn max_placeholder_index(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut max_idx = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            let start = i + 1;
            let mut j = start;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > start {
                if let Ok(n) = sql[start..j].parse::<usize>() {
                    max_idx = max_idx.max(n);
                }
                i = j;
                continue;
            }
        }
        i += 1;
    }
    max_idx
}

fn normalize_placeholders_for_describe(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            let start = i + 1;
            let mut j = start;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > start {
                // Use a numeric placeholder value so planning/type checks succeed
                // for common predicates and LIMIT/OFFSET contexts.
                out.push('0');
                i = j;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

pub fn scan_max_txn_id_from_storage(catalog: &Catalog) -> Result<u64, EntDbError> {
    let mut max_txn = 0_u64;
    for table in catalog.list_tables() {
        let t = Table::open(table.table_id, table.first_page_id, catalog.buffer_pool());
        for (_, tuple) in t.scan() {
            let decoded = entdb::query::executor::decode_stored_row(&tuple.data)?;
            if let entdb::query::executor::DecodedRow::Versioned(v) = decoded {
                max_txn = max_txn.max(v.created_txn);
                if let Some(d) = v.deleted_txn {
                    max_txn = max_txn.max(d);
                }
            }
        }
    }
    Ok(max_txn)
}

#[cfg(test)]
mod tests {
    use super::{
        bind_statement_placeholders, execution_tag_for_statement, max_placeholder_index,
        normalize_placeholders_for_describe, parse_single_statement, parse_sql, EntHandler,
    };
    use crate::server::Database;
    use entdb::catalog::Catalog;
    use entdb::query::history::OptimizerHistoryRecorder;
    use entdb::query::optimizer::OptimizerConfig;
    use entdb::storage::buffer_pool::BufferPool;
    use entdb::storage::disk_manager::DiskManager;
    use entdb::tx::TransactionManager;
    use entdb::wal::log_manager::LogManager;
    use pgwire::api::results::Tag;
    use pgwire::error::PgWireError;
    use proptest::prelude::*;
    use sqlparser::ast::Value as SqlValue;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn test_handler() -> EntHandler {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("handler.db");
        let wal_path = dir.path().join("handler.wal");

        let dm = Arc::new(DiskManager::new(&db_path).expect("disk"));
        let lm = Arc::new(LogManager::new(&wal_path, 4096).expect("wal"));
        let bp = Arc::new(BufferPool::with_log_manager(
            32,
            Arc::clone(&dm),
            Arc::clone(&lm),
        ));
        let catalog = Arc::new(Catalog::load(Arc::clone(&bp)).expect("catalog"));
        let txn = Arc::new(TransactionManager::new());
        let history_path = dir.path().join("handler.optimizer_history.json");
        let optimizer_history = Arc::new(
            OptimizerHistoryRecorder::new(
                &history_path,
                super::optimizer_history_schema_hash(),
                16,
                128,
            )
            .expect("optimizer history"),
        );
        let db = Arc::new(Database {
            disk_manager: dm,
            log_manager: lm,
            buffer_pool: bp,
            catalog,
            txn_manager: txn,
            optimizer_history,
            optimizer_config: OptimizerConfig::default(),
        });
        EntHandler::new(
            db,
            1024 * 1024,
            30_000,
            Arc::new(super::ServerMetrics::default()),
        )
    }

    #[test]
    fn bind_statement_placeholders_replaces_parameter_nodes() {
        let mut stmt = parse_single_statement(
            "SELECT id FROM t WHERE v > $1 ORDER BY id LIMIT $2",
            "single",
        )
        .expect("parse");
        bind_statement_placeholders(
            &mut stmt,
            &[
                SqlValue::Number("7".to_string(), false),
                SqlValue::Number("3".to_string(), false),
            ],
        )
        .expect("bind placeholders");

        let rendered = stmt.to_string();
        assert!(rendered.contains("v > 7"), "rendered SQL: {rendered}");
        assert!(rendered.contains("LIMIT 3"), "rendered SQL: {rendered}");
    }

    #[test]
    fn bind_statement_placeholders_rejects_out_of_range_index() {
        let mut stmt = parse_single_statement("SELECT $3", "single").expect("parse");
        let err = match bind_statement_placeholders(
            &mut stmt,
            &[SqlValue::Number("1".to_string(), false)],
        ) {
            Ok(_) => panic!("expected out-of-range placeholder failure"),
            Err(e) => e,
        };
        match err {
            PgWireError::UserError(info) => {
                assert_eq!(info.code, "22023");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parse_single_statement_rejects_multi_statement_sql() {
        let err = match parse_single_statement("SELECT 1; SELECT 2", "single") {
            Ok(_) => panic!("expected single-statement rejection"),
            Err(e) => e,
        };
        match err {
            PgWireError::UserError(info) => {
                assert_eq!(info.code, "0A000");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn max_placeholder_index_finds_highest_parameter() {
        assert_eq!(max_placeholder_index("SELECT $1, $2, $10"), 10);
        assert_eq!(max_placeholder_index("SELECT 1"), 0);
    }

    #[test]
    fn normalize_placeholders_for_describe_rewrites_all_numbered_params() {
        let sql = "SELECT id FROM t WHERE v > $1 ORDER BY id LIMIT $2";
        let normalized = normalize_placeholders_for_describe(sql);
        assert_eq!(
            normalized,
            "SELECT id FROM t WHERE v > 0 ORDER BY id LIMIT 0"
        );
    }

    #[test]
    fn execution_tag_for_insert_matches_postgres_format() {
        let stmt = parse_single_statement("INSERT INTO t VALUES (1)", "single").expect("parse");
        let tag = execution_tag_for_statement(&stmt, 2);
        assert_eq!(tag, Tag::new("INSERT 0").with_rows(2));
    }

    #[test]
    fn execution_tag_for_update_and_delete_include_row_count() {
        let update = parse_single_statement("UPDATE t SET v = 1", "single").expect("parse");
        let delete = parse_single_statement("DELETE FROM t", "single").expect("parse");
        assert_eq!(
            execution_tag_for_statement(&update, 3),
            Tag::new("UPDATE").with_rows(3)
        );
        assert_eq!(
            execution_tag_for_statement(&delete, 4),
            Tag::new("DELETE").with_rows(4)
        );
    }

    #[test]
    fn handler_begin_commit_round_trip() {
        let handler = test_handler();
        let out = handler
            .execute_sql("BEGIN; COMMIT;", None)
            .expect("execute");
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn handler_enforces_max_statement_size() {
        let handler = test_handler();
        let huge = "X".repeat(2 * 1024 * 1024);
        let err = match handler.execute_sql(&huge, None) {
            Ok(_) => panic!("max statement size should be enforced"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("max_statement_bytes"));
    }

    proptest! {
        #[test]
        fn parse_sql_never_panics_on_random_input(input in ".*") {
            let _ = parse_sql(&input);
        }
    }
}
