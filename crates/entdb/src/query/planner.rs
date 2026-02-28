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

use crate::catalog::{Column, Schema};
use crate::error::Result;
use crate::query::binder::{BoundSource, BoundStatement};
use crate::query::expression::{BinaryOp, BoundExpr};
use crate::query::plan::{AggregateExpr, AggregateFn, GroupKey, LogicalPlan};

pub struct Planner;

impl Planner {
    pub fn plan(&self, stmt: BoundStatement) -> Result<LogicalPlan> {
        match stmt {
            BoundStatement::Select {
                source,
                projection,
                mut projection_exprs,
                projection_names,
                mut filter,
                order_by,
                limit,
                offset,
                aggregate,
            } => {
                let mut plan = self.source_to_plan(source)?;
                if let LogicalPlan::SeqScan { table } = &plan {
                    if let Some(spec) =
                        detect_bm25_spec(projection_exprs.as_deref(), filter.as_ref())
                    {
                        if bm25_index_matches_table(table, &spec) {
                            let score_col_idx = table.schema.columns.len();
                            if let Some(exprs) = projection_exprs.as_mut() {
                                rewrite_exprs_to_bm25_score(exprs, &spec, score_col_idx);
                            }
                            if let Some(pred) = filter.as_mut() {
                                rewrite_expr_to_bm25_score(pred, &spec, score_col_idx);
                            }
                            let mut out_cols = table.schema.columns.clone();
                            out_cols.push(Column {
                                name: "__bm25_score".to_string(),
                                data_type: crate::types::DataType::Float64,
                                nullable: false,
                                default: None,
                                primary_key: false,
                            });
                            plan = LogicalPlan::Bm25Scan {
                                table: table.clone(),
                                index_name: spec.index_name,
                                terms: spec.terms,
                                output_schema: Schema { columns: out_cols },
                            };
                        }
                    }
                }

                if let Some(predicate) = filter {
                    plan = LogicalPlan::Filter {
                        predicate,
                        child: Box::new(plan),
                    };
                }

                if !order_by.is_empty() {
                    plan = LogicalPlan::Sort {
                        keys: order_by,
                        child: Box::new(plan),
                    };
                }

                if let Some(aggregate) = aggregate {
                    let input_schema = plan_output_schema(&plan);
                    let mut group_keys = Vec::with_capacity(aggregate.group_keys.len());
                    for key in &aggregate.group_keys {
                        let dt = input_schema
                            .columns
                            .get(key.col_idx)
                            .map(|c| c.data_type.clone())
                            .ok_or_else(|| {
                                crate::error::EntDbError::Query(
                                    "GROUP BY column index out of bounds".to_string(),
                                )
                            })?;
                        group_keys.push(GroupKey {
                            col_idx: key.col_idx,
                            col_name: key.col_name.clone(),
                            col_type: dt,
                        });
                    }
                    let mut aggregates = Vec::with_capacity(aggregate.aggregates.len());
                    for agg in &aggregate.aggregates {
                        let agg_col_type = agg
                            .agg_col_idx
                            .map(|idx| {
                                input_schema
                                    .columns
                                    .get(idx)
                                    .map(|c| c.data_type.clone())
                                    .ok_or_else(|| {
                                        crate::error::EntDbError::Query(
                                            "aggregate column index out of bounds".to_string(),
                                        )
                                    })
                            })
                            .transpose()?;
                        if matches!(agg.agg_fn, AggregateFn::Sum | AggregateFn::Avg)
                            && !matches!(
                                agg_col_type,
                                Some(crate::types::DataType::Int16)
                                    | Some(crate::types::DataType::Int32)
                                    | Some(crate::types::DataType::Int64)
                                    | Some(crate::types::DataType::Float32)
                                    | Some(crate::types::DataType::Float64)
                            )
                        {
                            return Err(crate::error::EntDbError::Query(
                                "SUM/AVG require numeric column".to_string(),
                            ));
                        }
                        aggregates.push(AggregateExpr {
                            agg_fn: agg.agg_fn,
                            agg_col_idx: agg.agg_col_idx,
                            agg_col_name: agg.agg_col_name.clone(),
                            agg_col_type,
                            agg_alias: agg.agg_alias.clone(),
                        });
                    }
                    let mut out_cols = Vec::new();
                    for key in &group_keys {
                        out_cols.push(Column {
                            name: key.col_name.clone(),
                            data_type: key.col_type.clone(),
                            nullable: true,
                            default: None,
                            primary_key: false,
                        });
                    }
                    for agg in &aggregates {
                        let agg_type = match agg.agg_fn {
                            AggregateFn::CountStar | AggregateFn::Count => {
                                crate::types::DataType::Int64
                            }
                            AggregateFn::Sum => match agg.agg_col_type {
                                Some(crate::types::DataType::Float32)
                                | Some(crate::types::DataType::Float64) => {
                                    crate::types::DataType::Float64
                                }
                                _ => crate::types::DataType::Int64,
                            },
                            AggregateFn::Avg => crate::types::DataType::Float64,
                            AggregateFn::Min | AggregateFn::Max => agg
                                .agg_col_type
                                .clone()
                                .unwrap_or(crate::types::DataType::Null),
                        };
                        out_cols.push(Column {
                            name: agg.agg_alias.clone(),
                            data_type: agg_type,
                            nullable: true,
                            default: None,
                            primary_key: false,
                        });
                    }
                    plan = LogicalPlan::GroupAggregate {
                        group_keys,
                        aggregates,
                        output_schema: Schema { columns: out_cols },
                        child: Box::new(plan),
                    };
                } else {
                    if let Some(exprs) = projection_exprs {
                        let output_schema = projection_expr_schema_from_child(
                            plan_output_schema(&plan),
                            &exprs,
                            &projection_names,
                        );
                        plan = LogicalPlan::ProjectExpr {
                            exprs,
                            projection_names,
                            output_schema,
                            child: Box::new(plan),
                        };
                    } else {
                        let output_schema = projection_schema_from_child(
                            plan_output_schema(&plan),
                            &projection,
                            &projection_names,
                        );
                        plan = LogicalPlan::Project {
                            projection,
                            projection_names,
                            output_schema,
                            child: Box::new(plan),
                        };
                    }
                }

                if let Some(count) = limit {
                    plan = LogicalPlan::Limit {
                        count,
                        offset,
                        child: Box::new(plan),
                    };
                }

                Ok(plan)
            }
            BoundStatement::Insert { table, rows } => Ok(LogicalPlan::Insert { table, rows }),
            BoundStatement::InsertSelect { table, source } => {
                let bound_query = self.plan(*source)?;
                Ok(LogicalPlan::InsertFromSelect {
                    table,
                    input: Box::new(bound_query),
                })
            }
            BoundStatement::Update {
                table,
                assignments,
                filter,
                from,
                returning,
            } => {
                if from.is_empty() && returning.is_none() {
                    Ok(LogicalPlan::Update {
                        table,
                        assignments,
                        filter,
                    })
                } else {
                    let target = crate::query::plan::BoundTableRef {
                        alias: table.name.clone(),
                        table,
                    };
                    Ok(LogicalPlan::UpdateAdvanced {
                        target,
                        from,
                        assignments,
                        filter,
                        returning,
                    })
                }
            }
            BoundStatement::Delete {
                table,
                filter,
                targets,
                from,
                using,
                order_by,
                limit,
                returning,
            } => {
                if targets.len() == 1
                    && targets[0] == table.name
                    && from.len() == 1
                    && from[0].table.name == table.name
                    && using.is_empty()
                    && order_by.is_empty()
                    && limit.is_none()
                    && returning.is_none()
                {
                    Ok(LogicalPlan::Delete { table, filter })
                } else {
                    Ok(LogicalPlan::DeleteAdvanced {
                        targets,
                        from,
                        using,
                        filter,
                        order_by,
                        limit,
                        returning,
                    })
                }
            }
            BoundStatement::Upsert {
                table,
                rows,
                conflict_cols,
                action,
            } => Ok(LogicalPlan::Upsert {
                table,
                rows,
                conflict_cols,
                action,
            }),
            BoundStatement::Analyze { table } => Ok(LogicalPlan::Analyze { table }),
            BoundStatement::Truncate { tables } => Ok(LogicalPlan::Truncate { tables }),
            BoundStatement::CreateTable { name, schema } => {
                Ok(LogicalPlan::CreateTable { name, schema })
            }
            BoundStatement::DropTable { name, if_exists } => {
                Ok(LogicalPlan::DropTable { name, if_exists })
            }
            BoundStatement::CreateIndex {
                table_name,
                index_name,
                columns,
                unique,
                if_not_exists,
                index_type,
            } => Ok(LogicalPlan::CreateIndex {
                table_name,
                index_name,
                columns,
                unique,
                if_not_exists,
                index_type,
            }),
            BoundStatement::DropIndex {
                index_name,
                if_exists,
            } => Ok(LogicalPlan::DropIndex {
                index_name,
                if_exists,
            }),
            BoundStatement::AlterTable {
                table_name,
                operations,
            } => Ok(LogicalPlan::AlterTable {
                table_name,
                operations,
            }),
            BoundStatement::Union { left, right, all } => {
                let left_plan = self.plan(*left)?;
                let right_plan = self.plan(*right)?;
                let left_schema = plan_output_schema(&left_plan).clone();
                let right_schema = plan_output_schema(&right_plan).clone();
                if left_schema.columns.len() != right_schema.columns.len() {
                    return Err(crate::error::EntDbError::Query(
                        "UNION operands must return same number of columns".to_string(),
                    ));
                }
                Ok(LogicalPlan::Union {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    all,
                    output_schema: left_schema,
                })
            }
        }
    }
}

impl Planner {
    fn source_to_plan(&self, source: BoundSource) -> Result<LogicalPlan> {
        Ok(match source {
            BoundSource::Values { rows, schema } => LogicalPlan::Values {
                rows,
                output_schema: schema,
            },
            BoundSource::Single { table } => LogicalPlan::SeqScan { table },
            BoundSource::Derived { source, .. } => self.plan(*source)?,
            BoundSource::InnerJoin {
                left,
                right,
                left_on,
                right_on,
            } => {
                let left_plan = self.source_to_plan(*left)?;
                let right_plan = self.source_to_plan(*right)?;
                let left_schema = plan_output_schema(&left_plan).clone();
                let right_schema = plan_output_schema(&right_plan).clone();
                let mut columns = left_schema.columns.clone();
                columns.extend(right_schema.columns.clone());
                LogicalPlan::NestedLoopJoin {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    left_on,
                    right_on,
                    output_schema: Schema { columns },
                }
            }
        })
    }
}

fn plan_output_schema(plan: &LogicalPlan) -> &Schema {
    match plan {
        LogicalPlan::Values { output_schema, .. } => output_schema,
        LogicalPlan::SeqScan { table } => &table.schema,
        LogicalPlan::Bm25Scan { output_schema, .. } => output_schema,
        LogicalPlan::NestedLoopJoin { output_schema, .. } => output_schema,
        LogicalPlan::Union { output_schema, .. } => output_schema,
        LogicalPlan::Filter { child, .. } => plan_output_schema(child),
        LogicalPlan::Sort { child, .. } => plan_output_schema(child),
        LogicalPlan::Project { output_schema, .. } => output_schema,
        LogicalPlan::ProjectExpr { output_schema, .. } => output_schema,
        LogicalPlan::Count { .. } => panic!("count schema should not be used here"),
        LogicalPlan::GroupCount { .. } => panic!("group-count schema should not be used here"),
        LogicalPlan::GroupAggregate { output_schema, .. } => output_schema,
        LogicalPlan::Limit { child, .. } => plan_output_schema(child),
        LogicalPlan::Insert { .. }
        | LogicalPlan::InsertFromSelect { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::UpdateAdvanced { .. }
        | LogicalPlan::Delete { .. }
        | LogicalPlan::DeleteAdvanced { .. }
        | LogicalPlan::Upsert { .. }
        | LogicalPlan::Analyze { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::AlterTable { .. } => panic!("non-row plan has no schema"),
    }
}

#[derive(Clone)]
struct Bm25PlanSpec {
    col_idx: usize,
    index_name: String,
    terms: Vec<String>,
}

fn detect_bm25_spec(
    projection_exprs: Option<&[crate::query::expression::BoundExpr]>,
    filter: Option<&crate::query::expression::BoundExpr>,
) -> Option<Bm25PlanSpec> {
    if let Some(exprs) = projection_exprs {
        for expr in exprs {
            if let Some(spec) = extract_bm25_spec(expr) {
                return Some(spec);
            }
        }
    }
    filter.and_then(extract_bm25_spec)
}

fn extract_bm25_spec(expr: &crate::query::expression::BoundExpr) -> Option<Bm25PlanSpec> {
    match expr {
        crate::query::expression::BoundExpr::BinaryOp { op, left, right } => {
            if matches!(op, BinaryOp::Bm25Score) {
                let BoundExpr::ColumnRef { col_idx } = left.as_ref() else {
                    return None;
                };
                if let Some((terms, index_name)) = extract_bm25_query_args(right) {
                    return Some(Bm25PlanSpec {
                        col_idx: *col_idx,
                        index_name,
                        terms,
                    });
                }
            }
            extract_bm25_spec(left).or_else(|| extract_bm25_spec(right))
        }
        crate::query::expression::BoundExpr::Function { args, .. } => {
            for arg in args {
                if let Some(spec) = extract_bm25_spec(arg) {
                    return Some(spec);
                }
            }
            None
        }
        _ => None,
    }
}

fn extract_bm25_query_args(
    expr: &crate::query::expression::BoundExpr,
) -> Option<(Vec<String>, String)> {
    let crate::query::expression::BoundExpr::Function { name, args } = expr else {
        return None;
    };
    if name != "to_bm25query" || args.len() != 2 {
        return None;
    }
    let crate::query::expression::BoundExpr::Literal(crate::types::Value::Text(query)) = &args[0]
    else {
        return None;
    };
    let crate::query::expression::BoundExpr::Literal(crate::types::Value::Text(index_name)) =
        &args[1]
    else {
        return None;
    };
    let terms = query
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_ascii_lowercase())
        .collect::<Vec<_>>();
    Some((terms, index_name.clone()))
}

fn rewrite_exprs_to_bm25_score(
    exprs: &mut [crate::query::expression::BoundExpr],
    spec: &Bm25PlanSpec,
    score_col_idx: usize,
) {
    for expr in exprs {
        rewrite_expr_to_bm25_score(expr, spec, score_col_idx);
    }
}

fn rewrite_expr_to_bm25_score(
    expr: &mut crate::query::expression::BoundExpr,
    spec: &Bm25PlanSpec,
    score_col_idx: usize,
) {
    match expr {
        crate::query::expression::BoundExpr::BinaryOp { op, left, right } => {
            if matches!(op, BinaryOp::Bm25Score)
                && matches!(left.as_ref(), BoundExpr::ColumnRef { col_idx } if *col_idx == spec.col_idx)
                && matches_bm25_query(right, spec)
            {
                *expr = BoundExpr::ColumnRef {
                    col_idx: score_col_idx,
                };
                return;
            }
            rewrite_expr_to_bm25_score(left, spec, score_col_idx);
            rewrite_expr_to_bm25_score(right, spec, score_col_idx);
        }
        crate::query::expression::BoundExpr::Function { args, .. } => {
            for arg in args {
                rewrite_expr_to_bm25_score(arg, spec, score_col_idx);
            }
        }
        _ => {}
    }
}

fn matches_bm25_query(expr: &BoundExpr, spec: &Bm25PlanSpec) -> bool {
    let Some((terms, index_name)) = extract_bm25_query_args(expr) else {
        return false;
    };
    index_name == spec.index_name && terms == spec.terms
}

fn bm25_index_matches_table(table: &crate::catalog::TableInfo, spec: &Bm25PlanSpec) -> bool {
    table.indexes.iter().any(|idx| {
        idx.name == spec.index_name
            && idx.column_indices == vec![spec.col_idx]
            && matches!(idx.index_type, crate::catalog::IndexType::Bm25 { .. })
    })
}

fn projection_schema_from_child(
    input_schema: &Schema,
    projection: &Option<Vec<usize>>,
    names: &[String],
) -> Schema {
    match projection {
        None => input_schema.clone(),
        Some(idxs) => {
            let columns = idxs
                .iter()
                .enumerate()
                .map(|(i, idx)| {
                    let input = &input_schema.columns[*idx];
                    Column {
                        name: names[i].clone(),
                        data_type: input.data_type.clone(),
                        nullable: input.nullable,
                        default: input.default.clone(),
                        primary_key: input.primary_key,
                    }
                })
                .collect();
            Schema { columns }
        }
    }
}

fn projection_expr_schema_from_child(
    input_schema: &Schema,
    exprs: &[crate::query::expression::BoundExpr],
    names: &[String],
) -> Schema {
    let columns = exprs
        .iter()
        .enumerate()
        .map(|(i, expr)| Column {
            name: names[i].clone(),
            data_type: infer_expr_type(expr, input_schema),
            nullable: true,
            default: None,
            primary_key: false,
        })
        .collect();
    Schema { columns }
}

fn infer_expr_type(
    expr: &crate::query::expression::BoundExpr,
    input_schema: &Schema,
) -> crate::types::DataType {
    match expr {
        crate::query::expression::BoundExpr::ColumnRef { col_idx } => input_schema
            .columns
            .get(*col_idx)
            .map(|c| c.data_type.clone())
            .unwrap_or(crate::types::DataType::Null),
        crate::query::expression::BoundExpr::Literal(v) => v.data_type(),
        crate::query::expression::BoundExpr::Function { name, .. } => match name.as_str() {
            "length" => crate::types::DataType::Int64,
            "upper" | "lower" | "trim" | "concat" => crate::types::DataType::Text,
            "abs" => crate::types::DataType::Float64,
            "coalesce" => crate::types::DataType::Null,
            "to_bm25query" => crate::types::DataType::Bm25Query,
            _ => crate::types::DataType::Null,
        },
        crate::query::expression::BoundExpr::WindowRowNumber { .. } => {
            crate::types::DataType::Int64
        }
        crate::query::expression::BoundExpr::BinaryOp { op, .. } => match op {
            crate::query::expression::BinaryOp::L2Distance
            | crate::query::expression::BinaryOp::CosineDistance
            | crate::query::expression::BinaryOp::Bm25Score => crate::types::DataType::Float64,
            _ => crate::types::DataType::Null,
        },
    }
}
