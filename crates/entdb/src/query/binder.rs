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

use crate::catalog::{Catalog, Column, Schema, TableInfo};
use crate::error::{EntDbError, Result};
use crate::query::expression::{BinaryOp, BoundExpr};
use crate::query::plan::{
    AggregateFn, AlterTableOp, BoundTableRef, GroupKey, ReturningBinding, SortKey,
    UpdateAssignment, UpsertAction,
};
use crate::types::{DataType, Value};
use sqlparser::ast::{
    AlterTableOperation, Assignment, AssignmentTarget, BinaryOperator, CharacterLength,
    ConflictTarget, Expr, FromTable, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    Ident, JoinConstraint, JoinOperator, ObjectName, ObjectType, OnConflictAction, OnInsert,
    OrderBy, OrderByExpr, OrderByKind, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableObject, TableWithJoins, TruncateTableTarget, UnaryOperator, UpdateTableFromKind,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum BoundSource {
    Values {
        rows: Vec<Vec<Value>>,
        schema: Schema,
    },
    Single {
        table: TableInfo,
    },
    Derived {
        source: Box<BoundStatement>,
        schema: Schema,
    },
    InnerJoin {
        left: Box<BoundSource>,
        right: Box<BoundSource>,
        left_on: usize,
        right_on: usize,
    },
}

#[derive(Debug, Clone)]
pub enum BoundStatement {
    Select {
        source: BoundSource,
        projection: Option<Vec<usize>>, // None => *
        projection_exprs: Option<Vec<BoundExpr>>,
        projection_names: Vec<String>,
        filter: Option<BoundExpr>,
        order_by: Vec<SortKey>,
        limit: Option<usize>,
        offset: usize,
        aggregate: Option<AggregateBinding>,
    },
    Insert {
        table: TableInfo,
        rows: Vec<Vec<Value>>,
    },
    InsertSelect {
        table: TableInfo,
        source: Box<BoundStatement>,
    },
    Update {
        table: TableInfo,
        assignments: Vec<UpdateAssignment>,
        filter: Option<BoundExpr>,
        from: Vec<BoundTableRef>,
        returning: Option<ReturningBinding>,
    },
    Delete {
        table: TableInfo,
        filter: Option<BoundExpr>,
        targets: Vec<String>,
        from: Vec<BoundTableRef>,
        using: Vec<BoundTableRef>,
        order_by: Vec<SortKey>,
        limit: Option<usize>,
        returning: Option<ReturningBinding>,
    },
    Upsert {
        table: TableInfo,
        rows: Vec<Vec<Value>>,
        conflict_cols: Vec<usize>,
        action: UpsertAction,
    },
    Analyze {
        table: TableInfo,
    },
    Truncate {
        tables: Vec<TableInfo>,
    },
    CreateTable {
        name: String,
        schema: Schema,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    CreateIndex {
        table_name: String,
        index_name: String,
        columns: Vec<String>,
        unique: bool,
        if_not_exists: bool,
    },
    DropIndex {
        index_name: String,
        if_exists: bool,
    },
    AlterTable {
        table_name: String,
        operations: Vec<AlterTableOp>,
    },
    Union {
        left: Box<BoundStatement>,
        right: Box<BoundStatement>,
        all: bool,
    },
}

#[derive(Debug, Clone)]
pub struct AggregateBinding {
    pub group_keys: Vec<GroupKey>,
    pub aggregates: Vec<AggregateExprBinding>,
}

#[derive(Debug, Clone)]
pub struct AggregateExprBinding {
    pub agg_fn: AggregateFn,
    pub agg_col_idx: Option<usize>,
    pub agg_col_name: Option<String>,
    pub agg_alias: String,
}

pub struct Binder {
    catalog: Arc<Catalog>,
}

#[derive(Clone)]
struct ScopeColumn {
    table_name: String,
    column_name: String,
    global_idx: usize,
}

impl Binder {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }

    pub fn bind(&self, stmt: &Statement) -> Result<BoundStatement> {
        match stmt {
            Statement::Query(query) => self.bind_query(query),
            Statement::Insert(insert) => {
                let table_name = match &insert.table {
                    TableObject::TableName(name) => object_name_to_string(name),
                    TableObject::TableFunction(_) => {
                        return Err(EntDbError::Query(
                            "INSERT into table function is not supported".to_string(),
                        ))
                    }
                };
                let table = self.catalog.get_table(&table_name).ok_or_else(|| {
                    EntDbError::Query(format!("table '{table_name}' does not exist"))
                })?;

                let source_query = insert
                    .source
                    .as_ref()
                    .ok_or_else(|| EntDbError::Query("INSERT missing source".to_string()))?;

                let rows = match &*source_query.body {
                    SetExpr::Values(values) => {
                        let mut out = Vec::new();
                        for row in &values.rows {
                            if row.len() != table.schema.columns.len() {
                                return Err(EntDbError::Query(format!(
                                    "INSERT row has {} values, expected {}",
                                    row.len(),
                                    table.schema.columns.len()
                                )));
                            }

                            let mut bound_row = Vec::new();
                            for (expr, col) in row.iter().zip(&table.schema.columns) {
                                let v = bind_literal(expr)?;
                                let casted = v.cast_to(&col.data_type).map_err(|e| {
                                    EntDbError::Query(format!(
                                        "cannot cast value for column '{}': {e}",
                                        col.name
                                    ))
                                })?;
                                bound_row.push(casted);
                            }
                            out.push(bound_row);
                        }
                        out
                    }
                    _ => {
                        if insert.on.is_some() {
                            return Err(EntDbError::Query(
                                "ON CONFLICT is currently only supported for VALUES inserts"
                                    .to_string(),
                            ));
                        }
                        let source_stmt = self.bind_query(source_query)?;
                        return Ok(BoundStatement::InsertSelect {
                            table,
                            source: Box::new(source_stmt),
                        });
                    }
                };

                if let Some(on_insert) = &insert.on {
                    let (conflict_cols, action) = bind_on_conflict(on_insert, &table)?;
                    return Ok(BoundStatement::Upsert {
                        table,
                        rows,
                        conflict_cols,
                        action,
                    });
                }

                Ok(BoundStatement::Insert { table, rows })
            }
            Statement::Update {
                table,
                assignments,
                from,
                selection,
                returning,
                or,
            } => {
                if or.is_some() {
                    return Err(EntDbError::Query(
                        "UPDATE conflict clauses are not supported yet".to_string(),
                    ));
                }

                let (target_table, target_alias) = table_with_joins_to_ref(table, &self.catalog)?;
                let mut scope = build_scope_from_schema(&target_table.schema, &target_alias, 0);
                let (from_tables, from_join_predicates) = match from {
                    Some(UpdateTableFromKind::BeforeSet(tables))
                    | Some(UpdateTableFromKind::AfterSet(tables)) => {
                        collect_table_refs_and_join_preds(tables, &self.catalog)?
                    }
                    None => (Vec::new(), Vec::new()),
                };
                let mut global_idx = scope.len();
                for entry in &from_tables {
                    let table_scope =
                        build_scope_from_schema(&entry.table.schema, &entry.alias, global_idx);
                    global_idx += table_scope.len();
                    scope.extend(table_scope);
                }

                let mut bound_assignments = Vec::with_capacity(assignments.len());
                for a in assignments {
                    bound_assignments.push(bind_update_assignment(a, &target_table, &scope)?);
                }

                let mut filter = match selection {
                    Some(expr) => Some(bind_expr(expr, &scope)?),
                    None => None,
                };
                for join_expr in from_join_predicates {
                    let bound = bind_expr(&join_expr, &scope)?;
                    filter = Some(match filter {
                        None => bound,
                        Some(existing) => BoundExpr::BinaryOp {
                            op: BinaryOp::And,
                            left: Box::new(existing),
                            right: Box::new(bound),
                        },
                    });
                }
                let returning = bind_returning(returning, &target_table, &scope)?;

                Ok(BoundStatement::Update {
                    table: target_table,
                    assignments: bound_assignments,
                    filter,
                    from: from_tables,
                    returning,
                })
            }
            Statement::Delete(delete) => {
                let from_tables_ast = match &delete.from {
                    FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
                        if tables.is_empty() {
                            return Err(EntDbError::Query(
                                "DELETE requires at least one FROM table".to_string(),
                            ));
                        }
                        tables
                    }
                };
                let (from_tables, from_join_predicates) =
                    collect_table_refs_and_join_preds(from_tables_ast, &self.catalog)?;
                let (using_tables, using_join_predicates) = match &delete.using {
                    Some(using) => collect_table_refs_and_join_preds(using, &self.catalog)?,
                    None => (Vec::new(), Vec::new()),
                };
                let mut scope = Vec::new();
                let mut global_idx = 0;
                for entry in &from_tables {
                    let table_scope =
                        build_scope_from_schema(&entry.table.schema, &entry.alias, global_idx);
                    global_idx += table_scope.len();
                    scope.extend(table_scope);
                }
                for entry in &using_tables {
                    let table_scope =
                        build_scope_from_schema(&entry.table.schema, &entry.alias, global_idx);
                    global_idx += table_scope.len();
                    scope.extend(table_scope);
                }

                let mut filter = match &delete.selection {
                    Some(expr) => Some(bind_expr(expr, &scope)?),
                    None => None,
                };
                for join_expr in from_join_predicates
                    .into_iter()
                    .chain(using_join_predicates.into_iter())
                {
                    let bound = bind_expr(&join_expr, &scope)?;
                    filter = Some(match filter {
                        None => bound,
                        Some(existing) => BoundExpr::BinaryOp {
                            op: BinaryOp::And,
                            left: Box::new(existing),
                            right: Box::new(bound),
                        },
                    });
                }
                let order_by = bind_order_by_exprs(&delete.order_by, &scope)?;
                let limit = bind_limit_expr(delete.limit.as_ref())?;
                let primary = from_tables.first().ok_or_else(|| {
                    EntDbError::Query("DELETE requires primary FROM table".to_string())
                })?;
                let returning = bind_returning(&delete.returning, &primary.table, &scope)?;
                let targets = if delete.tables.is_empty() {
                    vec![primary.alias.clone()]
                } else {
                    delete
                        .tables
                        .iter()
                        .map(object_name_to_string)
                        .collect::<Vec<_>>()
                };

                Ok(BoundStatement::Delete {
                    table: primary.table.clone(),
                    filter,
                    targets,
                    from: from_tables,
                    using: using_tables,
                    order_by,
                    limit,
                    returning,
                })
            }
            Statement::Analyze { table_name, .. } => {
                let name = object_name_to_string(table_name);
                let table = self
                    .catalog
                    .get_table(&name)
                    .ok_or_else(|| EntDbError::Query(format!("table '{name}' does not exist")))?;
                Ok(BoundStatement::Analyze { table })
            }
            Statement::Truncate { table_names, .. } => {
                let mut tables = Vec::with_capacity(table_names.len());
                for target in table_names {
                    let TruncateTableTarget { name, .. } = target;
                    let table_name = object_name_to_string(name);
                    let table = self.catalog.get_table(&table_name).ok_or_else(|| {
                        EntDbError::Query(format!("table '{table_name}' does not exist"))
                    })?;
                    tables.push(table);
                }
                Ok(BoundStatement::Truncate { tables })
            }
            Statement::CreateTable(create) => {
                let mut columns = Vec::new();
                for c in &create.columns {
                    columns.push(Column {
                        name: c.name.value.clone(),
                        data_type: map_sql_type(&c.data_type)?,
                        nullable: true,
                        default: None,
                        primary_key: false,
                    });
                }
                Ok(BoundStatement::CreateTable {
                    name: object_name_to_string(&create.name),
                    schema: Schema::new(columns),
                })
            }
            Statement::CreateIndex(create_idx) => {
                let index_name = create_idx
                    .name
                    .as_ref()
                    .map(object_name_to_string)
                    .ok_or_else(|| EntDbError::Query("CREATE INDEX requires a name".to_string()))?;
                let table_name = object_name_to_string(&create_idx.table_name);
                let mut columns = Vec::with_capacity(create_idx.columns.len());
                for c in &create_idx.columns {
                    columns.push(column_name_for_expr(&c.expr)?);
                }
                Ok(BoundStatement::CreateIndex {
                    table_name,
                    index_name,
                    columns,
                    unique: create_idx.unique,
                    if_not_exists: create_idx.if_not_exists,
                })
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => {
                if names.len() != 1 {
                    return Err(EntDbError::Query(
                        "only single-object DROP is supported".to_string(),
                    ));
                }
                match object_type {
                    ObjectType::Table => Ok(BoundStatement::DropTable {
                        name: object_name_to_string(&names[0]),
                        if_exists: *if_exists,
                    }),
                    ObjectType::Index => Ok(BoundStatement::DropIndex {
                        index_name: object_name_to_string(&names[0]),
                        if_exists: *if_exists,
                    }),
                    _ => Err(EntDbError::Query(
                        "only DROP TABLE and DROP INDEX are currently supported".to_string(),
                    )),
                }
            }
            Statement::AlterTable {
                name, operations, ..
            } => {
                let table_name = object_name_to_string(name);
                let mut bound_ops = Vec::with_capacity(operations.len());
                for op in operations {
                    match op {
                        AlterTableOperation::AddColumn {
                            if_not_exists,
                            column_def,
                            ..
                        } => {
                            bound_ops.push(AlterTableOp::AddColumn {
                                column: Column {
                                    name: column_def.name.value.clone(),
                                    data_type: map_sql_type(&column_def.data_type)?,
                                    nullable: true,
                                    default: None,
                                    primary_key: false,
                                },
                                if_not_exists: *if_not_exists,
                            });
                        }
                        AlterTableOperation::DropColumn {
                            column_name,
                            if_exists,
                            ..
                        } => {
                            bound_ops.push(AlterTableOp::DropColumn {
                                name: column_name.value.clone(),
                                if_exists: *if_exists,
                            });
                        }
                        AlterTableOperation::RenameColumn {
                            old_column_name,
                            new_column_name,
                        } => {
                            bound_ops.push(AlterTableOp::RenameColumn {
                                old_name: old_column_name.value.clone(),
                                new_name: new_column_name.value.clone(),
                            });
                        }
                        AlterTableOperation::RenameTable {
                            table_name: new_name,
                        } => {
                            bound_ops.push(AlterTableOp::RenameTable {
                                new_name: object_name_to_string(new_name),
                            });
                        }
                        _ => {
                            return Err(EntDbError::Query(
                                "ALTER TABLE operation not supported yet".to_string(),
                            ))
                        }
                    }
                }
                Ok(BoundStatement::AlterTable {
                    table_name,
                    operations: bound_ops,
                })
            }
            _ => Err(EntDbError::Query(
                "statement type not supported yet".to_string(),
            )),
        }
    }

    fn bind_query(&self, query: &Query) -> Result<BoundStatement> {
        let mut ctes = std::collections::HashMap::new();
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                let bound = self.bind_query(&cte.query)?;
                let schema = bound_statement_schema(&bound)?;
                ctes.insert(
                    cte.alias.name.value.clone(),
                    BoundSource::Derived {
                        source: Box::new(bound),
                        schema,
                    },
                );
            }
        }

        match &*query.body {
            SetExpr::Select(select) => self.bind_select(select, query, &ctes),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                if op != &sqlparser::ast::SetOperator::Union {
                    return Err(EntDbError::Query(
                        "only UNION/UNION ALL set operations are supported".to_string(),
                    ));
                }
                let left_stmt = self.bind_query(&Query {
                    with: None,
                    body: left.clone(),
                    order_by: None,
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                })?;
                let right_stmt = self.bind_query(&Query {
                    with: None,
                    body: right.clone(),
                    order_by: None,
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                })?;
                let left_schema = bound_statement_schema(&left_stmt)?;
                let right_schema = bound_statement_schema(&right_stmt)?;
                if left_schema.columns.len() != right_schema.columns.len() {
                    return Err(EntDbError::Query(
                        "UNION operands must return the same number of columns".to_string(),
                    ));
                }
                Ok(BoundStatement::Union {
                    left: Box::new(left_stmt),
                    right: Box::new(right_stmt),
                    all: matches!(*set_quantifier, sqlparser::ast::SetQuantifier::All),
                })
            }
            _ => Err(EntDbError::Query("only SELECT supported".to_string())),
        }
    }

    fn bind_select(
        &self,
        select: &Select,
        query: &Query,
        ctes: &std::collections::HashMap<String, BoundSource>,
    ) -> Result<BoundStatement> {
        if select.from.is_empty() {
            return self.bind_select_without_from(select, query);
        }
        if select.from.len() != 1 {
            return Err(EntDbError::Query(
                "only one FROM relation is currently supported".to_string(),
            ));
        }

        let (source, scope) = self.bind_source(&select.from[0], ctes)?;

        let projection_result = bind_projection(&select.projection, &scope)?;

        let aggregate = bind_aggregate(&select.group_by, &projection_result)?;

        let filter = match &select.selection {
            Some(expr) => Some(bind_expr(expr, &scope)?),
            None => None,
        };

        let order_by = bind_order_by(query.order_by.as_ref(), &scope)?;

        let limit = match &query.limit {
            Some(Expr::Value(v)) => Some(match &v.value {
                sqlparser::ast::Value::Number(n, _) => n
                    .parse::<usize>()
                    .map_err(|_| EntDbError::Query("invalid LIMIT value".to_string()))?,
                _ => return Err(EntDbError::Query("LIMIT must be numeric".to_string())),
            }),
            Some(_) => {
                return Err(EntDbError::Query(
                    "LIMIT expression unsupported".to_string(),
                ))
            }
            None => None,
        };
        let offset = match &query.offset {
            Some(o) => match &o.value {
                Expr::Value(v) => match &v.value {
                    sqlparser::ast::Value::Number(n, _) => n
                        .parse::<usize>()
                        .map_err(|_| EntDbError::Query("invalid OFFSET value".to_string()))?,
                    _ => return Err(EntDbError::Query("OFFSET must be numeric".to_string())),
                },
                _ => {
                    return Err(EntDbError::Query(
                        "OFFSET expression unsupported".to_string(),
                    ))
                }
            },
            None => 0,
        };

        Ok(BoundStatement::Select {
            source,
            projection: projection_result.projection,
            projection_exprs: projection_result.projection_exprs,
            projection_names: projection_result.projection_names,
            filter,
            order_by,
            limit,
            offset,
            aggregate,
        })
    }

    fn bind_select_without_from(&self, select: &Select, query: &Query) -> Result<BoundStatement> {
        if select.distinct.is_some() {
            return Err(EntDbError::Query(
                "SELECT DISTINCT without FROM is not supported".to_string(),
            ));
        }
        let group_by_present = match &select.group_by {
            GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
            GroupByExpr::All(_) => true,
        };
        if group_by_present {
            return Err(EntDbError::Query(
                "GROUP BY without FROM is not supported".to_string(),
            ));
        }
        if select.selection.is_some() {
            return Err(EntDbError::Query(
                "WHERE without FROM is not supported for literal projection".to_string(),
            ));
        }
        let has_order_by = match query.order_by.as_ref() {
            Some(order_by) => match &order_by.kind {
                OrderByKind::Expressions(exprs) => !exprs.is_empty(),
                OrderByKind::All(_) => true,
            },
            None => false,
        };
        if has_order_by {
            return Err(EntDbError::Query(
                "ORDER BY without FROM is not supported for literal projection".to_string(),
            ));
        }

        let mut values = Vec::new();
        let mut columns = Vec::new();
        let mut names = Vec::new();
        for (i, item) in select.projection.iter().enumerate() {
            let (expr, out_name) = match item {
                SelectItem::UnnamedExpr(expr) => {
                    let name = match expr {
                        Expr::Identifier(id) => id.value.clone(),
                        _ => format!("col{}", i + 1),
                    };
                    (expr, name)
                }
                SelectItem::ExprWithAlias { expr, alias } => (expr, alias.value.clone()),
                _ => {
                    return Err(EntDbError::Query(
                        "only literal expressions are supported in SELECT without FROM".to_string(),
                    ))
                }
            };
            let value = bind_literal(expr)?;
            let data_type = value.data_type();
            values.push(value);
            names.push(out_name.clone());
            columns.push(Column {
                name: out_name,
                data_type,
                nullable: false,
                default: None,
                primary_key: false,
            });
        }

        let source = BoundSource::Values {
            rows: vec![values],
            schema: Schema { columns },
        };
        let projection = Some((0..names.len()).collect());

        let limit = match &query.limit {
            Some(Expr::Value(v)) => Some(match &v.value {
                sqlparser::ast::Value::Number(n, _) => n
                    .parse::<usize>()
                    .map_err(|_| EntDbError::Query("invalid LIMIT value".to_string()))?,
                _ => return Err(EntDbError::Query("LIMIT must be numeric".to_string())),
            }),
            Some(_) => {
                return Err(EntDbError::Query(
                    "LIMIT expression unsupported".to_string(),
                ))
            }
            None => None,
        };
        let offset = match &query.offset {
            Some(o) => match &o.value {
                Expr::Value(v) => match &v.value {
                    sqlparser::ast::Value::Number(n, _) => n
                        .parse::<usize>()
                        .map_err(|_| EntDbError::Query("invalid OFFSET value".to_string()))?,
                    _ => return Err(EntDbError::Query("OFFSET must be numeric".to_string())),
                },
                _ => {
                    return Err(EntDbError::Query(
                        "OFFSET expression unsupported".to_string(),
                    ))
                }
            },
            None => 0,
        };

        Ok(BoundStatement::Select {
            source,
            projection,
            projection_exprs: None,
            projection_names: names,
            filter: None,
            order_by: Vec::new(),
            limit,
            offset,
            aggregate: None,
        })
    }

    fn bind_source(
        &self,
        from: &TableWithJoins,
        ctes: &std::collections::HashMap<String, BoundSource>,
    ) -> Result<(BoundSource, Vec<ScopeColumn>)> {
        let (mut source, mut scope) = self.bind_relation_with_alias(&from.relation, ctes)?;

        for join in &from.joins {
            let (right, right_scope) = self.bind_relation_with_alias(&join.relation, ctes)?;
            let on_expr = match &join.join_operator {
                JoinOperator::Inner(JoinConstraint::On(expr))
                | JoinOperator::Join(JoinConstraint::On(expr)) => expr,
                _ => {
                    return Err(EntDbError::Query(
                        "only INNER JOIN ... ON is currently supported".to_string(),
                    ))
                }
            };
            let (left_on, right_on) = bind_join_on_columns(on_expr, &scope, &right_scope)?;

            source = BoundSource::InnerJoin {
                left: Box::new(source),
                right: Box::new(right),
                left_on,
                right_on,
            };

            let mut combined = scope.clone();
            let left_width = combined.len();
            for (idx, col) in right_scope.iter().enumerate() {
                combined.push(ScopeColumn {
                    table_name: col.table_name.clone(),
                    column_name: col.column_name.clone(),
                    global_idx: left_width + idx,
                });
            }
            scope = combined;
        }

        Ok((source, scope))
    }

    fn bind_relation_with_alias(
        &self,
        relation: &TableFactor,
        ctes: &std::collections::HashMap<String, BoundSource>,
    ) -> Result<(BoundSource, Vec<ScopeColumn>)> {
        match relation {
            TableFactor::Table { name, alias, .. } => {
                let table_name = object_name_to_string(name);
                if let Some(cte_source) = ctes.get(&table_name) {
                    let schema = bound_source_schema(cte_source)?;
                    let alias_name = alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or(table_name.clone());
                    let scope = build_scope_from_schema(&schema, &alias_name, 0);
                    return Ok((cte_source.clone(), scope));
                }
                let table = self.catalog.get_table(&table_name).ok_or_else(|| {
                    EntDbError::Query(format!("table '{table_name}' does not exist"))
                })?;
                let alias_name = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or(table.name.clone());
                let scope = build_scope_from_schema(&table.schema, &alias_name, 0);
                Ok((BoundSource::Single { table }, scope))
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let bound = self.bind_query(subquery)?;
                let schema = bound_statement_schema(&bound)?;
                let alias_name = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "__subquery".to_string());
                let scope = build_scope_from_schema(&schema, &alias_name, 0);
                Ok((
                    BoundSource::Derived {
                        source: Box::new(bound),
                        schema,
                    },
                    scope,
                ))
            }
            _ => Err(EntDbError::Query(
                "only base-table and derived subquery relations are supported".to_string(),
            )),
        }
    }
}

struct ProjectionBinding {
    projection: Option<Vec<usize>>,
    projection_exprs: Option<Vec<BoundExpr>>,
    projection_names: Vec<String>,
    aggregate: Option<AggregateProjection>,
}

struct AggregateProjection {
    group_keys: Vec<GroupKey>,
    aggregates: Vec<AggregateExprBinding>,
}

fn bind_projection(items: &[SelectItem], scope: &[ScopeColumn]) -> Result<ProjectionBinding> {
    if items.len() == 1 && matches!(items[0], SelectItem::Wildcard(_)) {
        let cols = scope
            .iter()
            .map(|c| c.column_name.clone())
            .collect::<Vec<_>>();
        return Ok(ProjectionBinding {
            projection: None,
            projection_exprs: None,
            projection_names: cols,
            aggregate: None,
        });
    }

    if !items.is_empty() {
        // Non-grouped aggregate projection: every item is an aggregate.
        let mut all_aggregates = Vec::with_capacity(items.len());
        let mut agg_names = Vec::with_capacity(items.len());
        let mut all_items_are_aggregates = true;
        for item in items {
            if let Some(agg) = parse_aggregate_select_item(item, scope)? {
                agg_names.push(agg.agg_alias.clone());
                all_aggregates.push(agg);
            } else {
                all_items_are_aggregates = false;
                break;
            }
        }
        if all_items_are_aggregates {
            return Ok(ProjectionBinding {
                projection: None,
                projection_exprs: None,
                projection_names: agg_names,
                aggregate: Some(AggregateProjection {
                    group_keys: Vec::new(),
                    aggregates: all_aggregates,
                }),
            });
        }
    }

    // Grouped aggregate projection: one or more grouping columns + one or more aggregates.
    let mut group_keys = Vec::new();
    let mut aggregates = Vec::new();
    let mut projection_names = Vec::new();
    let mut saw_aggregate = false;
    for item in items {
        if let Some(agg) = parse_aggregate_select_item(item, scope)? {
            saw_aggregate = true;
            projection_names.push(agg.agg_alias.clone());
            aggregates.push(agg);
            continue;
        }
        if saw_aggregate {
            return Ok(non_aggregate_projection(items, scope)?);
        }
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _ => return Ok(non_aggregate_projection(items, scope)?),
        };
        let col_idx = match resolve_column_ref(expr, scope) {
            Ok(v) => v,
            Err(_) => return Ok(non_aggregate_projection(items, scope)?),
        };
        let col_name = alias.unwrap_or(column_name_for_expr(expr)?);
        group_keys.push(GroupKey {
            col_idx,
            col_name: col_name.clone(),
            col_type: DataType::Null,
        });
        projection_names.push(col_name);
    }

    if !aggregates.is_empty() {
        return Ok(ProjectionBinding {
            projection: None,
            projection_exprs: None,
            projection_names,
            aggregate: Some(AggregateProjection {
                group_keys,
                aggregates,
            }),
        });
    }

    non_aggregate_projection(items, scope)
}

fn non_aggregate_projection(
    items: &[SelectItem],
    scope: &[ScopeColumn],
) -> Result<ProjectionBinding> {
    let mut idxs = Vec::new();
    let mut exprs = Vec::new();
    let mut names = Vec::new();
    let mut all_refs = true;
    for item in items {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                match resolve_column_ref(expr, scope) {
                    Ok(idx) => {
                        idxs.push(idx);
                        exprs.push(BoundExpr::ColumnRef { col_idx: idx });
                    }
                    Err(_) => {
                        all_refs = false;
                        exprs.push(bind_expr(expr, scope)?);
                    }
                }
                names.push(column_name_for_select_expr(expr, None)?);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                match resolve_column_ref(expr, scope) {
                    Ok(idx) => {
                        idxs.push(idx);
                        exprs.push(BoundExpr::ColumnRef { col_idx: idx });
                    }
                    Err(_) => {
                        all_refs = false;
                        exprs.push(bind_expr(expr, scope)?);
                    }
                }
                names.push(alias.value.clone());
            }
            _ => {
                return Err(EntDbError::Query(
                    "projection item not supported".to_string(),
                ))
            }
        }
    }

    Ok(ProjectionBinding {
        projection: if all_refs { Some(idxs) } else { None },
        projection_exprs: if all_refs { None } else { Some(exprs) },
        projection_names: names,
        aggregate: None,
    })
}

fn bind_aggregate(
    group_by: &GroupByExpr,
    projection: &ProjectionBinding,
) -> Result<Option<AggregateBinding>> {
    let Some(agg) = &projection.aggregate else {
        match group_by {
            GroupByExpr::Expressions(exprs, _) if !exprs.is_empty() => {
                return Err(EntDbError::Query(
                    "GROUP BY currently requires aggregate projection".to_string(),
                ));
            }
            GroupByExpr::All(_) => {
                return Err(EntDbError::Query(
                    "GROUP BY ALL is not supported".to_string(),
                ));
            }
            _ => return Ok(None),
        }
    };

    let group_exprs = match group_by {
        GroupByExpr::Expressions(exprs, _) => exprs.as_slice(),
        GroupByExpr::All(_) => {
            return Err(EntDbError::Query(
                "GROUP BY ALL is not supported".to_string(),
            ))
        }
    };

    if agg.group_keys.is_empty() {
        if !group_exprs.is_empty() {
            return Err(EntDbError::Query(
                "GROUP BY requires projected grouping columns".to_string(),
            ));
        }
    } else if group_exprs.len() != agg.group_keys.len() {
        return Err(EntDbError::Query(
            "GROUP BY column must match projected grouping column".to_string(),
        ));
    } else {
        for (expr, projected) in group_exprs.iter().zip(&agg.group_keys) {
            let group_col_name = column_name_for_expr(expr)?;
            if group_col_name != projected.col_name {
                return Err(EntDbError::Query(
                    "GROUP BY column must match projected grouping column".to_string(),
                ));
            }
        }
    }

    if agg.aggregates.is_empty() {
        return Err(EntDbError::Query(
            "at least one aggregate expression is required".to_string(),
        ));
    }
    for entry in &agg.aggregates {
        if matches!(entry.agg_fn, AggregateFn::CountStar) && entry.agg_col_idx.is_some() {
            return Err(EntDbError::Query(
                "COUNT(*) cannot have aggregate argument column".to_string(),
            ));
        }
        if !matches!(entry.agg_fn, AggregateFn::CountStar) && entry.agg_col_idx.is_none() {
            return Err(EntDbError::Query(
                "aggregate function requires column argument".to_string(),
            ));
        }
    }

    Ok(Some(AggregateBinding {
        group_keys: agg.group_keys.clone(),
        aggregates: agg.aggregates.clone(),
    }))
}

fn parse_aggregate_select_item(
    item: &SelectItem,
    scope: &[ScopeColumn],
) -> Result<Option<AggregateExprBinding>> {
    let (func, alias_override) = match item {
        SelectItem::UnnamedExpr(Expr::Function(f)) => (f, None),
        SelectItem::ExprWithAlias {
            expr: Expr::Function(f),
            alias,
        } => (f, Some(alias.value.clone())),
        _ => return Ok(None),
    };

    if is_count_star_function(func) {
        return Ok(Some(AggregateExprBinding {
            agg_fn: AggregateFn::CountStar,
            agg_col_idx: None,
            agg_col_name: None,
            agg_alias: alias_override.unwrap_or_else(|| "count".to_string()),
        }));
    }

    let Some((agg_fn, arg_expr, default_alias)) = parse_single_arg_aggregate(func) else {
        return Ok(None);
    };
    let col_idx = resolve_column_ref(arg_expr, scope)?;
    let col_name = column_name_for_expr(arg_expr)?;
    let alias = alias_override.unwrap_or(default_alias);
    Ok(Some(AggregateExprBinding {
        agg_fn,
        agg_col_idx: Some(col_idx),
        agg_col_name: Some(col_name),
        agg_alias: alias,
    }))
}

fn bind_order_by(order_by: Option<&OrderBy>, scope: &[ScopeColumn]) -> Result<Vec<SortKey>> {
    let Some(order_by) = order_by else {
        return Ok(Vec::new());
    };

    let exprs = match &order_by.kind {
        OrderByKind::Expressions(exprs) => exprs,
        OrderByKind::All(_) => {
            return Err(EntDbError::Query(
                "ORDER BY ALL is not supported".to_string(),
            ))
        }
    };

    let mut out = Vec::with_capacity(exprs.len());
    for ob in exprs {
        let col_idx = resolve_column_ref(&ob.expr, scope)?;
        out.push(SortKey {
            col_idx,
            asc: ob.options.asc.unwrap_or(true),
        });
    }
    Ok(out)
}

fn bind_order_by_exprs(order_by: &[OrderByExpr], scope: &[ScopeColumn]) -> Result<Vec<SortKey>> {
    let mut out = Vec::with_capacity(order_by.len());
    for ob in order_by {
        let col_idx = resolve_column_ref(&ob.expr, scope)?;
        out.push(SortKey {
            col_idx,
            asc: ob.options.asc.unwrap_or(true),
        });
    }
    Ok(out)
}

fn bind_limit_expr(limit: Option<&Expr>) -> Result<Option<usize>> {
    match limit {
        None => Ok(None),
        Some(Expr::Value(v)) => match &v.value {
            sqlparser::ast::Value::Number(n, _) => n
                .parse::<usize>()
                .map(Some)
                .map_err(|_| EntDbError::Query("invalid LIMIT value".to_string())),
            _ => Err(EntDbError::Query("LIMIT must be numeric".to_string())),
        },
        Some(_) => Err(EntDbError::Query(
            "LIMIT expression unsupported".to_string(),
        )),
    }
}

fn bind_returning(
    returning: &Option<Vec<SelectItem>>,
    target_table: &TableInfo,
    scope: &[ScopeColumn],
) -> Result<Option<ReturningBinding>> {
    let Some(items) = returning else {
        return Ok(None);
    };
    if items.len() == 1 && matches!(items[0], SelectItem::Wildcard(_)) {
        return Ok(Some(ReturningBinding {
            projection: Some((0..target_table.schema.columns.len()).collect()),
            projection_names: target_table
                .schema
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect(),
        }));
    }

    let mut projection = Vec::with_capacity(items.len());
    let mut names = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let idx = resolve_column_ref(expr, scope)?;
                projection.push(idx);
                names.push(column_name_for_select_expr(expr, None)?);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let idx = resolve_column_ref(expr, scope)?;
                projection.push(idx);
                names.push(alias.value.clone());
            }
            _ => {
                return Err(EntDbError::Query(
                    "RETURNING supports column references only".to_string(),
                ))
            }
        }
    }

    Ok(Some(ReturningBinding {
        projection: Some(projection),
        projection_names: names,
    }))
}

fn bind_update_assignment(
    assignment: &Assignment,
    table: &TableInfo,
    scope: &[ScopeColumn],
) -> Result<UpdateAssignment> {
    let col_name = match &assignment.target {
        AssignmentTarget::ColumnName(obj) => {
            if obj.0.len() != 1 {
                return Err(EntDbError::Query(
                    "qualified UPDATE assignment targets are not supported".to_string(),
                ));
            }
            obj.0[0]
                .as_ident()
                .map(|i| i.value.clone())
                .ok_or_else(|| EntDbError::Query("invalid UPDATE assignment target".to_string()))?
        }
        AssignmentTarget::Tuple(_) => {
            return Err(EntDbError::Query(
                "tuple assignments are not supported".to_string(),
            ))
        }
    };

    let col_idx = table
        .schema
        .column_index(&col_name)
        .ok_or_else(|| EntDbError::Query(format!("column '{col_name}' does not exist")))?;

    let expr = bind_expr(&assignment.value, scope)?;
    Ok(UpdateAssignment { col_idx, expr })
}

fn bind_on_conflict(on_insert: &OnInsert, table: &TableInfo) -> Result<(Vec<usize>, UpsertAction)> {
    let OnInsert::OnConflict(on_conflict) = on_insert else {
        return Err(EntDbError::Query(
            "only ON CONFLICT is supported for UPSERT".to_string(),
        ));
    };
    let target = on_conflict.conflict_target.as_ref().ok_or_else(|| {
        EntDbError::Query("ON CONFLICT requires conflict target columns".to_string())
    })?;

    let conflict_cols = match target {
        ConflictTarget::Columns(cols) => {
            let mut out = Vec::with_capacity(cols.len());
            for c in cols {
                let idx = table.schema.column_index(&c.value).ok_or_else(|| {
                    EntDbError::Query(format!("column '{}' does not exist", c.value))
                })?;
                out.push(idx);
            }
            out
        }
        ConflictTarget::OnConstraint(_) => {
            return Err(EntDbError::Query(
                "ON CONFLICT ON CONSTRAINT is not supported".to_string(),
            ))
        }
    };

    let action = match &on_conflict.action {
        OnConflictAction::DoNothing => UpsertAction::DoNothing,
        OnConflictAction::DoUpdate(update) => {
            let existing_alias = table.name.clone();
            let excluded_alias = "excluded".to_string();
            let mut scope = build_scope_from_schema(&table.schema, &existing_alias, 0);
            let excluded_offset = scope.len();
            scope.extend(build_scope_from_schema(
                &table.schema,
                &excluded_alias,
                excluded_offset,
            ));
            let mut assignments = Vec::with_capacity(update.assignments.len());
            for a in &update.assignments {
                assignments.push(bind_update_assignment(a, table, &scope)?);
            }
            let selection = match &update.selection {
                Some(expr) => Some(bind_expr(expr, &scope)?),
                None => None,
            };
            UpsertAction::DoUpdate {
                assignments,
                selection,
                excluded_offset,
            }
        }
    };

    Ok((conflict_cols, action))
}

fn bind_join_on_columns(
    on_expr: &Expr,
    left_scope: &[ScopeColumn],
    right_scope: &[ScopeColumn],
) -> Result<(usize, usize)> {
    match on_expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if let (Ok(left_idx), Ok(right_idx)) = (
                resolve_column_ref(left, left_scope),
                resolve_column_ref(right, right_scope),
            ) {
                return Ok((left_idx, right_idx));
            }
            if let (Ok(left_idx), Ok(right_idx)) = (
                resolve_column_ref(right, left_scope),
                resolve_column_ref(left, right_scope),
            ) {
                return Ok((left_idx, right_idx));
            }
            Err(EntDbError::Query(
                "JOIN ON must compare one column from each table".to_string(),
            ))
        }
        _ => Err(EntDbError::Query(
            "JOIN ON must be an equality between left and right columns".to_string(),
        )),
    }
}

fn build_scope_from_schema(schema: &Schema, table_name: &str, base_idx: usize) -> Vec<ScopeColumn> {
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(i, col)| ScopeColumn {
            table_name: table_name.to_string(),
            column_name: col.name.clone(),
            global_idx: base_idx + i,
        })
        .collect()
}

fn table_factor_to_ref(relation: &TableFactor, catalog: &Catalog) -> Result<(TableInfo, String)> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let table_name = object_name_to_string(name);
            let table = catalog
                .get_table(&table_name)
                .ok_or_else(|| EntDbError::Query(format!("table '{table_name}' does not exist")))?;
            let alias_name = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or(table_name);
            Ok((table, alias_name))
        }
        _ => Err(EntDbError::Query(
            "only base tables are supported".to_string(),
        )),
    }
}

fn table_with_joins_to_ref(t: &TableWithJoins, catalog: &Catalog) -> Result<(TableInfo, String)> {
    if !t.joins.is_empty() {
        return Err(EntDbError::Query(
            "target relation cannot contain joins".to_string(),
        ));
    }
    table_factor_to_ref(&t.relation, catalog)
}

fn collect_table_refs_and_join_preds(
    tables: &[TableWithJoins],
    catalog: &Catalog,
) -> Result<(Vec<BoundTableRef>, Vec<Expr>)> {
    let mut refs = Vec::new();
    let mut preds = Vec::new();
    for t in tables {
        let (table, alias) = table_factor_to_ref(&t.relation, catalog)?;
        refs.push(BoundTableRef { table, alias });
        for join in &t.joins {
            let (table, alias) = table_factor_to_ref(&join.relation, catalog)?;
            refs.push(BoundTableRef { table, alias });
            match &join.join_operator {
                JoinOperator::Inner(JoinConstraint::On(expr))
                | JoinOperator::Join(JoinConstraint::On(expr)) => preds.push(expr.clone()),
                _ => {
                    return Err(EntDbError::Query(
                        "only INNER JOIN ... ON is supported in this statement".to_string(),
                    ))
                }
            }
        }
    }
    Ok((refs, preds))
}

fn bound_source_schema(source: &BoundSource) -> Result<Schema> {
    match source {
        BoundSource::Values { schema, .. } => Ok(schema.clone()),
        BoundSource::Single { table } => Ok(table.schema.clone()),
        BoundSource::Derived { schema, .. } => Ok(schema.clone()),
        BoundSource::InnerJoin { left, right, .. } => {
            let mut left_schema = bound_source_schema(left)?;
            let mut right_schema = bound_source_schema(right)?;
            left_schema.columns.append(&mut right_schema.columns);
            Ok(left_schema)
        }
    }
}

fn bound_statement_schema(stmt: &BoundStatement) -> Result<Schema> {
    match stmt {
        BoundStatement::Select {
            source,
            projection,
            projection_exprs,
            projection_names,
            aggregate,
            ..
        } => {
            let input = bound_source_schema(source)?;
            if let Some(agg) = aggregate {
                let mut cols = Vec::new();
                for key in &agg.group_keys {
                    let dt = input
                        .columns
                        .get(key.col_idx)
                        .map(|c| c.data_type.clone())
                        .unwrap_or(DataType::Null);
                    cols.push(Column {
                        name: key.col_name.clone(),
                        data_type: dt,
                        nullable: true,
                        default: None,
                        primary_key: false,
                    });
                }
                for a in &agg.aggregates {
                    cols.push(Column {
                        name: a.agg_alias.clone(),
                        data_type: DataType::Null,
                        nullable: true,
                        default: None,
                        primary_key: false,
                    });
                }
                Ok(Schema { columns: cols })
            } else {
                match (projection, projection_exprs) {
                    (_, Some(exprs)) => {
                        let mut cols = Vec::with_capacity(exprs.len());
                        for (idx, expr) in exprs.iter().enumerate() {
                            cols.push(Column {
                                name: projection_names[idx].clone(),
                                data_type: infer_expr_type(expr, &input),
                                nullable: true,
                                default: None,
                                primary_key: false,
                            });
                        }
                        Ok(Schema { columns: cols })
                    }
                    (None, None) => Ok(input),
                    (Some(idxs), None) => {
                        let mut cols = Vec::with_capacity(idxs.len());
                        for (out_idx, idx) in idxs.iter().enumerate() {
                            let src = input.columns.get(*idx).ok_or_else(|| {
                                EntDbError::Query(
                                    "projection column index out of bounds".to_string(),
                                )
                            })?;
                            cols.push(Column {
                                name: projection_names[out_idx].clone(),
                                data_type: src.data_type.clone(),
                                nullable: src.nullable,
                                default: src.default.clone(),
                                primary_key: src.primary_key,
                            });
                        }
                        Ok(Schema { columns: cols })
                    }
                }
            }
        }
        BoundStatement::Union { left, .. } => bound_statement_schema(left),
        _ => Err(EntDbError::Query(
            "statement does not produce row schema".to_string(),
        )),
    }
}

fn resolve_column_ref(expr: &Expr, scope: &[ScopeColumn]) -> Result<usize> {
    match expr {
        Expr::Identifier(Ident { value, .. }) => {
            let mut matches = scope.iter().filter(|c| c.column_name == *value);
            let first = matches
                .next()
                .ok_or_else(|| EntDbError::Query(format!("column '{value}' does not exist")))?;
            if matches.next().is_some() {
                return Err(EntDbError::Query(format!(
                    "column '{value}' is ambiguous; qualify with table name"
                )));
            }
            Ok(first.global_idx)
        }
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let table = &parts[0].value;
            let col = &parts[1].value;
            let entry = scope
                .iter()
                .find(|c| c.table_name == *table && c.column_name == *col)
                .ok_or_else(|| {
                    EntDbError::Query(format!("unknown column reference '{table}.{col}'"))
                })?;
            Ok(entry.global_idx)
        }
        _ => Err(EntDbError::Query(
            "only column references are supported in this position".to_string(),
        )),
    }
}

fn column_name_for_expr(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(i) => Ok(i.value.clone()),
        Expr::CompoundIdentifier(parts) if !parts.is_empty() => {
            Ok(parts.last().expect("non-empty").value.clone())
        }
        _ => Err(EntDbError::Query(
            "projection expression name unsupported".to_string(),
        )),
    }
}

fn column_name_for_select_expr(expr: &Expr, fallback: Option<String>) -> Result<String> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => column_name_for_expr(expr),
        Expr::Function(func) => Ok(func.name.to_string().to_lowercase()),
        Expr::Value(_) => Ok(fallback.unwrap_or_else(|| "expr".to_string())),
        _ => Ok(fallback.unwrap_or_else(|| "expr".to_string())),
    }
}

fn bind_expr(expr: &Expr, scope: &[ScopeColumn]) -> Result<BoundExpr> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let bop = match op {
                BinaryOperator::Eq => BinaryOp::Eq,
                BinaryOperator::Gt => BinaryOp::Gt,
                BinaryOperator::Lt => BinaryOp::Lt,
                BinaryOperator::GtEq => BinaryOp::Gte,
                BinaryOperator::LtEq => BinaryOp::Lte,
                BinaryOperator::NotEq => BinaryOp::Neq,
                BinaryOperator::Plus => BinaryOp::Add,
                BinaryOperator::Minus => BinaryOp::Sub,
                BinaryOperator::Multiply => BinaryOp::Mul,
                BinaryOperator::Divide => BinaryOp::Div,
                BinaryOperator::And => BinaryOp::And,
                BinaryOperator::Or => BinaryOp::Or,
                _ => return Err(EntDbError::Query("unsupported binary operator".to_string())),
            };

            let l = bind_expr(left, scope)?;
            let r = bind_expr(right, scope)?;
            Ok(BoundExpr::BinaryOp {
                op: bop,
                left: Box::new(l),
                right: Box::new(r),
            })
        }
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let idx = resolve_column_ref(expr, scope)?;
            Ok(BoundExpr::ColumnRef { col_idx: idx })
        }
        Expr::Function(func) => bind_function_expr(func, scope),
        Expr::Trim {
            expr,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            if trim_where.is_some() || trim_what.is_some() || trim_characters.is_some() {
                return Err(EntDbError::Query(
                    "TRIM variant is not supported in this form".to_string(),
                ));
            }
            Ok(BoundExpr::Function {
                name: "trim".to_string(),
                args: vec![bind_expr(expr, scope)?],
            })
        }
        Expr::Nested(inner) => bind_expr(inner, scope),
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Plus => bind_expr(expr, scope),
            UnaryOperator::Minus => Ok(BoundExpr::BinaryOp {
                op: BinaryOp::Sub,
                left: Box::new(BoundExpr::Literal(Value::Int64(0))),
                right: Box::new(bind_expr(expr, scope)?),
            }),
            _ => Err(EntDbError::Query("unsupported unary operator".to_string())),
        },
        Expr::Value(_) => Ok(BoundExpr::Literal(bind_literal(expr)?)),
        _ => Err(EntDbError::Query("unsupported expression".to_string())),
    }
}

fn bind_function_expr(func: &sqlparser::ast::Function, scope: &[ScopeColumn]) -> Result<BoundExpr> {
    let name = func.name.to_string().to_lowercase();

    if name == "row_number" {
        let Some(over) = &func.over else {
            return Err(EntDbError::Query(
                "ROW_NUMBER requires OVER(...)".to_string(),
            ));
        };
        let sqlparser::ast::WindowType::WindowSpec(spec) = over else {
            return Err(EntDbError::Query(
                "named windows are not supported".to_string(),
            ));
        };
        let keys = bind_order_by_exprs(&spec.order_by, scope)?;
        if keys.is_empty() {
            return Err(EntDbError::Query(
                "ROW_NUMBER requires ORDER BY in OVER(...)".to_string(),
            ));
        }
        return Ok(BoundExpr::WindowRowNumber { order_by: keys });
    }
    if func.over.is_some() {
        return Err(EntDbError::Query(
            "only ROW_NUMBER window function is currently supported".to_string(),
        ));
    }

    let args = match &func.args {
        FunctionArguments::List(list) => &list.args,
        _ => {
            return Err(EntDbError::Query(
                "unsupported function argument list".to_string(),
            ))
        }
    };

    let mut bound_args = Vec::new();
    for arg in args {
        let FunctionArg::Unnamed(arg_expr) = arg else {
            return Err(EntDbError::Query(
                "named function arguments are not supported".to_string(),
            ));
        };
        let FunctionArgExpr::Expr(expr) = arg_expr else {
            return Err(EntDbError::Query(
                "wildcard function arguments are not supported here".to_string(),
            ));
        };
        bound_args.push(bind_expr(expr, scope)?);
    }

    Ok(BoundExpr::Function {
        name,
        args: bound_args,
    })
}

fn infer_expr_type(expr: &BoundExpr, input_schema: &Schema) -> DataType {
    match expr {
        BoundExpr::ColumnRef { col_idx } => input_schema
            .columns
            .get(*col_idx)
            .map(|c| c.data_type.clone())
            .unwrap_or(DataType::Null),
        BoundExpr::Literal(v) => v.data_type(),
        BoundExpr::BinaryOp { .. } => DataType::Null,
        BoundExpr::Function { name, .. } => match name.as_str() {
            "length" => DataType::Int64,
            "upper" | "lower" | "trim" | "concat" => DataType::Text,
            "abs" => DataType::Float64,
            "coalesce" => DataType::Null,
            _ => DataType::Null,
        },
        BoundExpr::WindowRowNumber { .. } => DataType::Int64,
    }
}

fn is_count_star_function(func: &sqlparser::ast::Function) -> bool {
    if !func.name.to_string().eq_ignore_ascii_case("count") {
        return false;
    }

    let FunctionArguments::List(args) = &func.args else {
        return false;
    };

    if args.args.len() != 1 {
        return false;
    }

    matches!(
        &args.args[0],
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
    )
}

fn parse_single_arg_aggregate(
    func: &sqlparser::ast::Function,
) -> Option<(AggregateFn, &Expr, String)> {
    let agg_fn = if func.name.to_string().eq_ignore_ascii_case("count") {
        AggregateFn::Count
    } else if func.name.to_string().eq_ignore_ascii_case("sum") {
        AggregateFn::Sum
    } else if func.name.to_string().eq_ignore_ascii_case("avg") {
        AggregateFn::Avg
    } else if func.name.to_string().eq_ignore_ascii_case("min") {
        AggregateFn::Min
    } else if func.name.to_string().eq_ignore_ascii_case("max") {
        AggregateFn::Max
    } else {
        return None;
    };

    let FunctionArguments::List(args) = &func.args else {
        return None;
    };
    if args.args.len() != 1 {
        return None;
    }
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &args.args[0] else {
        return None;
    };
    let default_alias = match agg_fn {
        AggregateFn::Count => "count",
        AggregateFn::Sum => "sum",
        AggregateFn::Avg => "avg",
        AggregateFn::Min => "min",
        AggregateFn::Max => "max",
        AggregateFn::CountStar => "count",
    }
    .to_string();
    Some((agg_fn, expr, default_alias))
}

fn bind_literal(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Value(v) => match &v.value {
            sqlparser::ast::Value::Boolean(b) => Ok(Value::Boolean(*b)),
            sqlparser::ast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::Int64(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Value::Float64(f))
                } else {
                    Err(EntDbError::Query(format!("invalid numeric literal '{n}'")))
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => Ok(Value::Text(s.clone())),
            sqlparser::ast::Value::Null => Ok(Value::Null),
            _ => Err(EntDbError::Query("literal type unsupported".to_string())),
        },
        Expr::UnaryOp { op, expr } => {
            let inner = bind_literal(expr)?;
            match op {
                UnaryOperator::Plus => match inner {
                    Value::Int64(_) | Value::Float64(_) | Value::Int32(_) | Value::Int16(_) => {
                        Ok(inner)
                    }
                    _ => Err(EntDbError::Query(
                        "unary '+' requires numeric literal".to_string(),
                    )),
                },
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

fn map_sql_type(dt: &sqlparser::ast::DataType) -> Result<DataType> {
    use sqlparser::ast::DataType as D;
    match dt {
        D::Boolean => Ok(DataType::Boolean),
        D::SmallInt(_) => Ok(DataType::Int16),
        D::Int(_) | D::Integer(_) => Ok(DataType::Int32),
        D::BigInt(_) => Ok(DataType::Int64),
        D::Float(_) | D::Double(_) | D::DoublePrecision => Ok(DataType::Float64),
        D::Real => Ok(DataType::Float32),
        D::Text => Ok(DataType::Text),
        D::Varchar(size) => Ok(DataType::Varchar(
            size.map(|s| match s {
                CharacterLength::IntegerLength { length, .. } => length as u32,
                CharacterLength::Max => 65_535,
            })
            .unwrap_or(255),
        )),
        D::Timestamp(_, _) => Ok(DataType::Timestamp),
        _ => Err(EntDbError::Query(format!("unsupported SQL type: {dt:?}"))),
    }
}

fn object_name_to_string(name: &ObjectName) -> String {
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
