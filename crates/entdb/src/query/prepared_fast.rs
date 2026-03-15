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

use crate::catalog::{Catalog, TableInfo};
use crate::error::{EntDbError, Result};
use crate::query::executor::btree_maintenance;
use crate::query::executor::delete::DeleteExecutor;
use crate::query::executor::insert::InsertExecutor;
use crate::query::executor::update::UpdateExecutor;
use crate::query::executor::{
    decode_stored_row, row_visible, DecodedRow, Executor, MvccRow, TxExecutionContext,
};
use crate::query::expression::{BinaryOp, BoundExpr};
use crate::query::plan::UpdateAssignment;
use crate::query::{QueryEngine, QueryOutput};
use crate::storage::table::Table;
use crate::tx::TransactionHandle;
use crate::types::{DataType, Value};
use sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    GroupByExpr, OrderByKind, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableObject, UnaryOperator, Value as SqlValue,
};

#[derive(Debug, Clone)]
pub enum PreparedFastPath {
    SelectAll(PreparedSelectAll),
    SelectByEq(PreparedSelectByEq),
    CountCompare(PreparedCountCompare),
    InsertSingle(PreparedInsertSingle),
    UpdateByEq(PreparedUpdateByEq),
    DeleteByEq(PreparedDeleteByEq),
}

#[derive(Debug, Clone)]
pub struct PreparedSelectAll {
    table: TableInfo,
    projection: Vec<usize>,
    columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PreparedSelectByEq {
    table: TableInfo,
    projection: Vec<usize>,
    columns: Vec<String>,
    filter_col_idx: usize,
    filter_value: ValueTemplate,
}

#[derive(Debug, Clone)]
pub struct PreparedCountCompare {
    table: TableInfo,
    filter_col_idx: usize,
    filter_op: BinaryOp,
    filter_value: ValueTemplate,
    alias: String,
}

#[derive(Debug, Clone)]
pub struct PreparedInsertSingle {
    table: TableInfo,
    values: Vec<ValueTemplate>,
}

#[derive(Debug, Clone)]
pub struct PreparedUpdateByEq {
    table: TableInfo,
    filter_col_idx: usize,
    filter_value: ValueTemplate,
    assignments: Vec<AssignmentTemplate>,
}

#[derive(Debug, Clone)]
pub struct PreparedDeleteByEq {
    table: TableInfo,
    filter_col_idx: usize,
    filter_value: ValueTemplate,
}

#[derive(Debug, Clone)]
pub struct AssignmentTemplate {
    pub col_idx: usize,
    pub value: ValueTemplate,
}

#[derive(Debug, Clone)]
pub enum ValueTemplate {
    Literal(Value),
    Param(usize),
}

impl ValueTemplate {
    fn resolve(&self, params: &[Value], target: &DataType) -> Result<Value> {
        let value = match self {
            Self::Literal(value) => value.clone(),
            Self::Param(index) => params.get(*index).cloned().ok_or_else(|| {
                EntDbError::Query(format!(
                    "parameter ${} out of bounds (have {} values)",
                    index + 1,
                    params.len()
                ))
            })?,
        };
        value.cast_to(target)
    }
}

impl PreparedFastPath {
    pub fn compile(catalog: &Catalog, statements: &[Statement]) -> Option<Self> {
        if statements.len() != 1 {
            return None;
        }
        compile_statement(catalog, &statements[0]).ok().flatten()
    }

    pub fn is_read_only(&self) -> bool {
        matches!(
            self,
            Self::SelectAll(_) | Self::SelectByEq(_) | Self::CountCompare(_)
        )
    }

    pub fn mutated_table_name(&self) -> Option<&str> {
        match self {
            Self::InsertSingle(stmt) => Some(&stmt.table.name),
            Self::UpdateByEq(stmt) => Some(&stmt.table.name),
            Self::DeleteByEq(stmt) => Some(&stmt.table.name),
            _ => None,
        }
    }

    pub fn creates_deleted_versions(&self) -> bool {
        matches!(self, Self::UpdateByEq(_) | Self::DeleteByEq(_))
    }

    pub fn execute(
        &self,
        engine: &QueryEngine,
        tx: &TransactionHandle,
        params: &[Value],
    ) -> Result<QueryOutput> {
        match self {
            Self::SelectAll(stmt) => execute_select_all(engine, tx, stmt),
            Self::SelectByEq(stmt) => execute_select_by_eq(engine, tx, stmt, params),
            Self::CountCompare(stmt) => execute_count_compare(engine, tx, stmt, params),
            Self::InsertSingle(stmt) => execute_insert_single(engine, tx, stmt, params),
            Self::UpdateByEq(stmt) => execute_update_by_eq(engine, tx, stmt, params),
            Self::DeleteByEq(stmt) => execute_delete_by_eq(engine, tx, stmt, params),
        }
    }
}

fn compile_statement(catalog: &Catalog, stmt: &Statement) -> Result<Option<PreparedFastPath>> {
    match stmt {
        Statement::Query(query) => compile_query(catalog, query),
        Statement::Insert(insert) => {
            let table_name = match &insert.table {
                TableObject::TableName(name) => object_name_to_string(name),
                _ => return Ok(None),
            };
            let Some(table) = catalog.get_table(&table_name) else {
                return Ok(None);
            };
            let Some(source) = insert.source.as_ref() else {
                return Ok(None);
            };
            let SetExpr::Values(values) = &*source.body else {
                return Ok(None);
            };
            if values.rows.len() != 1 || values.rows[0].len() != table.schema.columns.len() {
                return Ok(None);
            }
            if insert.on.is_some() {
                return Ok(None);
            }
            let value_templates = values.rows[0]
                .iter()
                .map(expr_to_value_template)
                .collect::<Result<Vec<_>>>()?;
            Ok(Some(PreparedFastPath::InsertSingle(PreparedInsertSingle {
                table,
                values: value_templates,
            })))
        }
        Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
            or,
        } => {
            if from.is_some() || returning.is_some() || or.is_some() {
                return Ok(None);
            }
            let TableFactor::Table { name, .. } = &table.relation else {
                return Ok(None);
            };
            let table_name = object_name_to_string(name);
            let Some(table_info) = catalog.get_table(&table_name) else {
                return Ok(None);
            };
            let Some(selection) = selection.as_ref() else {
                return Ok(None);
            };
            let (filter_col_idx, filter_value) = compile_simple_eq_filter(&table_info, selection)?;
            let assignments = compile_assignments(&table_info, assignments)?;
            Ok(Some(PreparedFastPath::UpdateByEq(PreparedUpdateByEq {
                table: table_info,
                filter_col_idx,
                filter_value,
                assignments,
            })))
        }
        Statement::Delete(delete) => {
            if !delete.using.as_ref().is_none_or(|u| u.is_empty())
                || !delete.order_by.is_empty()
                || delete.limit.is_some()
                || delete.returning.is_some()
            {
                return Ok(None);
            }
            let from_tables = match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables)
                | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
            };
            if from_tables.len() != 1 || !delete.tables.is_empty() {
                return Ok(None);
            }
            let TableFactor::Table { name, .. } = &from_tables[0].relation else {
                return Ok(None);
            };
            let table_name = object_name_to_string(name);
            let Some(table_info) = catalog.get_table(&table_name) else {
                return Ok(None);
            };
            let Some(selection) = delete.selection.as_ref() else {
                return Ok(None);
            };
            let (filter_col_idx, filter_value) = compile_simple_eq_filter(&table_info, selection)?;
            Ok(Some(PreparedFastPath::DeleteByEq(PreparedDeleteByEq {
                table: table_info,
                filter_col_idx,
                filter_value,
            })))
        }
        _ => Ok(None),
    }
}

fn compile_query(catalog: &Catalog, query: &Query) -> Result<Option<PreparedFastPath>> {
    if query.limit.is_some() || query.offset.is_some() || query.with.is_some() {
        return Ok(None);
    }
    let SetExpr::Select(select) = &*query.body else {
        return Ok(None);
    };
    if select.distinct.is_some() {
        return Ok(None);
    }
    let GroupByExpr::Expressions(group_by_exprs, _) = &select.group_by else {
        return Ok(None);
    };
    if !group_by_exprs.is_empty() {
        return Ok(None);
    }
    let Some(table_info) = compile_single_table_source(catalog, select)? else {
        return Ok(None);
    };

    if let Some(count_fast) = compile_count_compare(select, query, &table_info)? {
        return Ok(Some(PreparedFastPath::CountCompare(count_fast)));
    }

    if has_order_by(query) {
        return Ok(None);
    }

    let (projection, columns) = compile_projection(&table_info, &select.projection)?;
    if let Some(selection) = select.selection.as_ref() {
        let (filter_col_idx, filter_value) = compile_simple_eq_filter(&table_info, selection)?;
        return Ok(Some(PreparedFastPath::SelectByEq(PreparedSelectByEq {
            table: table_info,
            projection,
            columns,
            filter_col_idx,
            filter_value,
        })));
    }

    Ok(Some(PreparedFastPath::SelectAll(PreparedSelectAll {
        table: table_info,
        projection,
        columns,
    })))
}

fn compile_single_table_source(catalog: &Catalog, select: &Select) -> Result<Option<TableInfo>> {
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Ok(None);
    }
    let TableFactor::Table { name, .. } = &select.from[0].relation else {
        return Ok(None);
    };
    let table_name = object_name_to_string(name);
    Ok(catalog.get_table(&table_name))
}

fn compile_count_compare(
    select: &Select,
    query: &Query,
    table: &TableInfo,
) -> Result<Option<PreparedCountCompare>> {
    if has_order_by(query) || select.projection.len() != 1 {
        return Ok(None);
    }
    let alias = match &select.projection[0] {
        SelectItem::UnnamedExpr(Expr::Function(func)) if is_count_star(func) => "count".to_string(),
        SelectItem::ExprWithAlias {
            expr: Expr::Function(func),
            alias,
        } if is_count_star(func) => alias.value.clone(),
        _ => return Ok(None),
    };
    let Some(selection) = select.selection.as_ref() else {
        return Ok(None);
    };
    let Some((filter_col_idx, filter_op, filter_value)) =
        compile_simple_compare_filter(table, selection)?
    else {
        return Ok(None);
    };
    Ok(Some(PreparedCountCompare {
        table: table.clone(),
        filter_col_idx,
        filter_op,
        filter_value,
        alias,
    }))
}

fn compile_projection(
    table: &TableInfo,
    items: &[SelectItem],
) -> Result<(Vec<usize>, Vec<String>)> {
    if items.len() == 1 && matches!(items[0], SelectItem::Wildcard(_)) {
        let projection = (0..table.schema.columns.len()).collect::<Vec<_>>();
        let columns = table
            .schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();
        return Ok((projection, columns));
    }

    let mut projection = Vec::with_capacity(items.len());
    let mut columns = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let col_idx = table.schema.column_index(&ident.value).ok_or_else(|| {
                    EntDbError::Query(format!("column '{}' does not exist", ident.value))
                })?;
                projection.push(col_idx);
                columns.push(ident.value.clone());
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(ident),
                alias,
            } => {
                let col_idx = table.schema.column_index(&ident.value).ok_or_else(|| {
                    EntDbError::Query(format!("column '{}' does not exist", ident.value))
                })?;
                projection.push(col_idx);
                columns.push(alias.value.clone());
            }
            _ => {
                return Err(EntDbError::Query(
                    "prepared fast path only supports column projections".to_string(),
                ))
            }
        }
    }
    Ok((projection, columns))
}

fn compile_simple_eq_filter(table: &TableInfo, expr: &Expr) -> Result<(usize, ValueTemplate)> {
    let Some((col_idx, op, value)) = compile_simple_compare_filter(table, expr)? else {
        return Err(EntDbError::Query(
            "prepared fast path requires a simple comparison filter".to_string(),
        ));
    };
    if !matches!(op, BinaryOp::Eq) {
        return Err(EntDbError::Query(
            "prepared fast path requires '=' filter".to_string(),
        ));
    }
    Ok((col_idx, value))
}

fn compile_simple_compare_filter(
    table: &TableInfo,
    expr: &Expr,
) -> Result<Option<(usize, BinaryOp, ValueTemplate)>> {
    let Expr::BinaryOp { left, op, right } = expr else {
        return Ok(None);
    };
    let binary_op = match op {
        sqlparser::ast::BinaryOperator::Eq => BinaryOp::Eq,
        sqlparser::ast::BinaryOperator::Gt => BinaryOp::Gt,
        sqlparser::ast::BinaryOperator::GtEq => BinaryOp::Gte,
        sqlparser::ast::BinaryOperator::Lt => BinaryOp::Lt,
        sqlparser::ast::BinaryOperator::LtEq => BinaryOp::Lte,
        _ => return Ok(None),
    };

    if let Some((col_idx, value)) = try_column_value_filter(table, left, right)? {
        return Ok(Some((col_idx, binary_op, value)));
    }
    if let Some((col_idx, value)) = try_column_value_filter(table, right, left)? {
        let normalized_op = match binary_op {
            BinaryOp::Gt => BinaryOp::Lt,
            BinaryOp::Gte => BinaryOp::Lte,
            BinaryOp::Lt => BinaryOp::Gt,
            BinaryOp::Lte => BinaryOp::Gte,
            other => other,
        };
        return Ok(Some((col_idx, normalized_op, value)));
    }

    Ok(None)
}

fn try_column_value_filter(
    table: &TableInfo,
    column_expr: &Expr,
    value_expr: &Expr,
) -> Result<Option<(usize, ValueTemplate)>> {
    let Expr::Identifier(ident) = column_expr else {
        return Ok(None);
    };
    let Some(col_idx) = table.schema.column_index(&ident.value) else {
        return Ok(None);
    };
    let value = expr_to_value_template(value_expr)?;
    Ok(Some((col_idx, value)))
}

fn compile_assignments(
    table: &TableInfo,
    assignments: &[Assignment],
) -> Result<Vec<AssignmentTemplate>> {
    let mut out = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let col_name = match &assignment.target {
            AssignmentTarget::ColumnName(obj) => {
                if obj.0.len() != 1 {
                    return Err(EntDbError::Query(
                        "qualified UPDATE assignment targets are not supported".to_string(),
                    ));
                }
                obj.0[0]
                    .as_ident()
                    .map(|ident| ident.value.clone())
                    .ok_or_else(|| {
                        EntDbError::Query("invalid UPDATE assignment target".to_string())
                    })?
            }
            AssignmentTarget::Tuple(_) => {
                return Err(EntDbError::Query(
                    "prepared fast path does not support tuple assignments".to_string(),
                ))
            }
        };
        let expr = match &assignment.value {
            Expr::Value(_) | Expr::UnaryOp { .. } => &assignment.value,
            _ => {
                return Err(EntDbError::Query(
                    "prepared fast path only supports literal/parameter assignments".to_string(),
                ))
            }
        };
        let col_idx = table
            .schema
            .column_index(&col_name)
            .ok_or_else(|| EntDbError::Query(format!("column '{col_name}' does not exist")))?;
        out.push(AssignmentTemplate {
            col_idx,
            value: expr_to_value_template(expr)?,
        });
    }
    Ok(out)
}

fn expr_to_value_template(expr: &Expr) -> Result<ValueTemplate> {
    match expr {
        Expr::Value(v) => match &v.value {
            SqlValue::Placeholder(placeholder) => {
                Ok(ValueTemplate::Param(parse_param_index(placeholder)?))
            }
            _ => Ok(ValueTemplate::Literal(bind_literal(expr)?)),
        },
        Expr::UnaryOp { .. } => Ok(ValueTemplate::Literal(bind_literal(expr)?)),
        _ => Err(EntDbError::Query(
            "prepared fast path only supports literal/parameter values".to_string(),
        )),
    }
}

fn bind_literal(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Value(v) => match &v.value {
            SqlValue::Boolean(b) => Ok(Value::Boolean(*b)),
            SqlValue::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::Int64(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Value::Float64(f))
                } else {
                    Err(EntDbError::Query(format!("invalid numeric literal '{n}'")))
                }
            }
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                Ok(Value::Text(s.clone()))
            }
            SqlValue::Null => Ok(Value::Null),
            _ => Err(EntDbError::Query("literal type unsupported".to_string())),
        },
        Expr::UnaryOp { op, expr } => {
            let inner = bind_literal(expr)?;
            match op {
                UnaryOperator::Plus => Ok(inner),
                UnaryOperator::Minus => match inner {
                    Value::Int16(v) => Ok(Value::Int16(v.saturating_neg())),
                    Value::Int32(v) => Ok(Value::Int32(v.saturating_neg())),
                    Value::Int64(v) => Ok(Value::Int64(v.saturating_neg())),
                    Value::Float64(v) => Ok(Value::Float64(-v)),
                    _ => Err(EntDbError::Query(
                        "unary '-' requires numeric literal".to_string(),
                    )),
                },
                _ => Err(EntDbError::Query(
                    "unsupported unary operator in literal".to_string(),
                )),
            }
        }
        _ => Err(EntDbError::Query("expected literal expression".to_string())),
    }
}

fn parse_param_index(placeholder: &str) -> Result<usize> {
    let trimmed = placeholder.trim();
    let index_str = if let Some(rest) = trimmed.strip_prefix('$') {
        rest
    } else if let Some(rest) = trimmed.strip_prefix('?') {
        rest
    } else {
        return Err(EntDbError::Query(
            "only numeric placeholders are supported".to_string(),
        ));
    };
    let index = index_str
        .parse::<usize>()
        .map_err(|_| EntDbError::Query(format!("invalid parameter marker '{placeholder}'")))?;
    if index == 0 {
        return Err(EntDbError::Query(format!(
            "parameter marker '{placeholder}' is 1-based"
        )));
    }
    Ok(index - 1)
}

fn is_count_star(func: &sqlparser::ast::Function) -> bool {
    if !func.name.to_string().eq_ignore_ascii_case("count") {
        return false;
    }
    let FunctionArguments::List(args) = &func.args else {
        return false;
    };
    args.args.len() == 1
        && matches!(
            &args.args[0],
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
        )
}

fn has_order_by(query: &Query) -> bool {
    query
        .order_by
        .as_ref()
        .is_some_and(|order_by| match &order_by.kind {
            OrderByKind::Expressions(exprs) => !exprs.is_empty(),
            OrderByKind::All(_) => true,
        })
}

fn object_name_to_string(name: &sqlparser::ast::ObjectName) -> String {
    name.0
        .iter()
        .map(|p| {
            p.as_ident()
                .map(|i| i.value.clone())
                .unwrap_or_else(|| p.to_string())
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn execute_select_all(
    engine: &QueryEngine,
    tx: &TransactionHandle,
    stmt: &PreparedSelectAll,
) -> Result<QueryOutput> {
    let ctx = TxExecutionContext {
        txn_id: tx.txn_id,
        snapshot_ts: tx.snapshot_ts,
        txn_manager: engine.txn_manager.clone(),
    };
    let rows = scan_projected_rows(&stmt.table, &stmt.projection, engine.catalog.as_ref(), &ctx)?;
    Ok(QueryOutput::Rows {
        columns: stmt.columns.clone(),
        rows,
    })
}

fn execute_select_by_eq(
    engine: &QueryEngine,
    tx: &TransactionHandle,
    stmt: &PreparedSelectByEq,
    params: &[Value],
) -> Result<QueryOutput> {
    let ctx = TxExecutionContext {
        txn_id: tx.txn_id,
        snapshot_ts: tx.snapshot_ts,
        txn_manager: engine.txn_manager.clone(),
    };
    let filter_type = &stmt.table.schema.columns[stmt.filter_col_idx].data_type;
    let filter_value = stmt.filter_value.resolve(params, filter_type)?;
    let rows = if let Some(matches) = btree_maintenance::lookup_visible_rows(
        engine.catalog.as_ref(),
        &stmt.table,
        stmt.filter_col_idx,
        &filter_value,
        Some(&ctx),
    )? {
        matches
            .into_iter()
            .map(|(_, version, _)| project_values(&version.values, &stmt.projection))
            .collect()
    } else {
        scan_eq_projected_rows(
            &stmt.table,
            stmt.filter_col_idx,
            &filter_value,
            &stmt.projection,
            engine.catalog.as_ref(),
            &ctx,
        )?
    };
    Ok(QueryOutput::Rows {
        columns: stmt.columns.clone(),
        rows,
    })
}

fn execute_count_compare(
    engine: &QueryEngine,
    tx: &TransactionHandle,
    stmt: &PreparedCountCompare,
    params: &[Value],
) -> Result<QueryOutput> {
    let ctx = TxExecutionContext {
        txn_id: tx.txn_id,
        snapshot_ts: tx.snapshot_ts,
        txn_manager: engine.txn_manager.clone(),
    };
    let filter_type = &stmt.table.schema.columns[stmt.filter_col_idx].data_type;
    let filter_value = stmt.filter_value.resolve(params, filter_type)?;
    let table = Table::open(
        stmt.table.table_id,
        stmt.table.first_page_id,
        engine.catalog.buffer_pool(),
    );
    let mut count = 0_i64;
    for (_tid, tuple) in table.scan() {
        let version = match decode_stored_row(&tuple.data)? {
            DecodedRow::Legacy(values) => MvccRow {
                values,
                created_txn: 0,
                deleted_txn: None,
            },
            DecodedRow::Versioned(v) => v,
        };
        if !row_visible(&version, Some(&ctx)) {
            continue;
        }
        let Some(actual) = version.values.get(stmt.filter_col_idx) else {
            continue;
        };
        let matched = match stmt.filter_op {
            BinaryOp::Eq => actual.eq(&filter_value)?,
            BinaryOp::Gt => actual.gt(&filter_value)?,
            BinaryOp::Gte => actual.gt(&filter_value)? || actual.eq(&filter_value)?,
            BinaryOp::Lt => actual.lt(&filter_value)?,
            BinaryOp::Lte => actual.lt(&filter_value)? || actual.eq(&filter_value)?,
            _ => false,
        };
        if matched {
            count += 1;
        }
    }
    Ok(QueryOutput::Rows {
        columns: vec![stmt.alias.clone()],
        rows: vec![vec![Value::Int64(count)]],
    })
}

fn execute_insert_single(
    engine: &QueryEngine,
    tx: &TransactionHandle,
    stmt: &PreparedInsertSingle,
    params: &[Value],
) -> Result<QueryOutput> {
    let row = stmt
        .values
        .iter()
        .zip(&stmt.table.schema.columns)
        .map(|(template, col)| template.resolve(params, &col.data_type))
        .collect::<Result<Vec<_>>>()?;
    let ctx = TxExecutionContext {
        txn_id: tx.txn_id,
        snapshot_ts: tx.snapshot_ts,
        txn_manager: engine.txn_manager.clone(),
    };
    let mut exec = InsertExecutor::new(
        stmt.table.clone(),
        vec![row],
        engine.catalog.clone(),
        Some(ctx),
    );
    execute_executor(&mut exec)
}

fn execute_update_by_eq(
    engine: &QueryEngine,
    tx: &TransactionHandle,
    stmt: &PreparedUpdateByEq,
    params: &[Value],
) -> Result<QueryOutput> {
    let filter_type = &stmt.table.schema.columns[stmt.filter_col_idx].data_type;
    let filter_value = stmt.filter_value.resolve(params, filter_type)?;
    let assignments = stmt
        .assignments
        .iter()
        .map(|assignment| {
            let target_type = &stmt.table.schema.columns[assignment.col_idx].data_type;
            Ok(UpdateAssignment {
                col_idx: assignment.col_idx,
                expr: BoundExpr::Literal(assignment.value.resolve(params, target_type)?),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let filter = BoundExpr::BinaryOp {
        op: BinaryOp::Eq,
        left: Box::new(BoundExpr::ColumnRef {
            col_idx: stmt.filter_col_idx,
        }),
        right: Box::new(BoundExpr::Literal(filter_value)),
    };
    let ctx = TxExecutionContext {
        txn_id: tx.txn_id,
        snapshot_ts: tx.snapshot_ts,
        txn_manager: engine.txn_manager.clone(),
    };
    let mut exec = UpdateExecutor::new(
        stmt.table.clone(),
        assignments,
        Some(filter),
        engine.catalog.clone(),
        Some(ctx),
    );
    execute_executor(&mut exec)
}

fn execute_delete_by_eq(
    engine: &QueryEngine,
    tx: &TransactionHandle,
    stmt: &PreparedDeleteByEq,
    params: &[Value],
) -> Result<QueryOutput> {
    let filter_type = &stmt.table.schema.columns[stmt.filter_col_idx].data_type;
    let filter_value = stmt.filter_value.resolve(params, filter_type)?;
    let filter = BoundExpr::BinaryOp {
        op: BinaryOp::Eq,
        left: Box::new(BoundExpr::ColumnRef {
            col_idx: stmt.filter_col_idx,
        }),
        right: Box::new(BoundExpr::Literal(filter_value)),
    };
    let ctx = TxExecutionContext {
        txn_id: tx.txn_id,
        snapshot_ts: tx.snapshot_ts,
        txn_manager: engine.txn_manager.clone(),
    };
    let mut exec = DeleteExecutor::new(
        stmt.table.clone(),
        Some(filter),
        engine.catalog.clone(),
        Some(ctx),
    );
    execute_executor(&mut exec)
}

fn execute_executor(exec: &mut dyn Executor) -> Result<QueryOutput> {
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
    let affected_rows = exec.affected_rows().unwrap_or(0);
    exec.close()?;
    if rows.is_empty() && columns.is_empty() {
        Ok(QueryOutput::AffectedRows(affected_rows))
    } else {
        Ok(QueryOutput::Rows { columns, rows })
    }
}

fn scan_projected_rows(
    table_info: &TableInfo,
    projection: &[usize],
    catalog: &Catalog,
    tx: &TxExecutionContext,
) -> Result<Vec<Vec<Value>>> {
    let table = Table::open(
        table_info.table_id,
        table_info.first_page_id,
        catalog.buffer_pool(),
    );
    let mut rows = Vec::new();
    for (_tid, tuple) in table.scan() {
        let version = match decode_stored_row(&tuple.data)? {
            DecodedRow::Legacy(values) => MvccRow {
                values,
                created_txn: 0,
                deleted_txn: None,
            },
            DecodedRow::Versioned(v) => v,
        };
        if !row_visible(&version, Some(tx)) {
            continue;
        }
        rows.push(project_values(&version.values, projection));
    }
    Ok(rows)
}

fn scan_eq_projected_rows(
    table_info: &TableInfo,
    filter_col_idx: usize,
    filter_value: &Value,
    projection: &[usize],
    catalog: &Catalog,
    tx: &TxExecutionContext,
) -> Result<Vec<Vec<Value>>> {
    let table = Table::open(
        table_info.table_id,
        table_info.first_page_id,
        catalog.buffer_pool(),
    );
    let mut rows = Vec::new();
    for (_tid, tuple) in table.scan() {
        let version = match decode_stored_row(&tuple.data)? {
            DecodedRow::Legacy(values) => MvccRow {
                values,
                created_txn: 0,
                deleted_txn: None,
            },
            DecodedRow::Versioned(v) => v,
        };
        if !row_visible(&version, Some(tx)) {
            continue;
        }
        let Some(actual) = version.values.get(filter_col_idx) else {
            continue;
        };
        if !actual.eq(filter_value)? {
            continue;
        }
        rows.push(project_values(&version.values, projection));
    }
    Ok(rows)
}

fn project_values(values: &[Value], projection: &[usize]) -> Vec<Value> {
    let mut out = Vec::with_capacity(projection.len());
    for idx in projection {
        if let Some(value) = values.get(*idx) {
            out.push(value.clone());
        }
    }
    out
}
