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

use crate::catalog::IndexType;
use crate::catalog::{Column, Schema, TableInfo};
use crate::query::expression::BoundExpr;
use crate::types::DataType;
use crate::types::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey {
    pub col_idx: usize,
    pub asc: bool,
}

#[derive(Debug, Clone)]
pub struct UpdateAssignment {
    pub col_idx: usize,
    pub expr: BoundExpr,
}

#[derive(Debug, Clone)]
pub struct BoundTableRef {
    pub table: TableInfo,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub struct ReturningBinding {
    pub projection: Option<Vec<usize>>, // None => *
    pub projection_names: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum UpsertAction {
    DoNothing,
    DoUpdate {
        assignments: Vec<UpdateAssignment>,
        selection: Option<BoundExpr>,
        excluded_offset: usize,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum AggregateFn {
    CountStar,
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub agg_fn: AggregateFn,
    pub agg_col_idx: Option<usize>,
    pub agg_col_name: Option<String>,
    pub agg_col_type: Option<DataType>,
    pub agg_alias: String,
}

#[derive(Debug, Clone)]
pub struct GroupKey {
    pub col_idx: usize,
    pub col_name: String,
    pub col_type: DataType,
}

#[derive(Debug, Clone)]
pub enum AlterTableOp {
    AddColumn { column: Column, if_not_exists: bool },
    DropColumn { name: String, if_exists: bool },
    RenameColumn { old_name: String, new_name: String },
    RenameTable { new_name: String },
}

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Values {
        rows: Vec<Vec<Value>>,
        output_schema: Schema,
    },
    SeqScan {
        table: TableInfo,
    },
    Bm25Scan {
        table: TableInfo,
        index_name: String,
        terms: Vec<String>,
        output_schema: Schema,
    },
    NestedLoopJoin {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        left_on: usize,
        right_on: usize,
        output_schema: Schema,
    },
    Union {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        all: bool,
        output_schema: Schema,
    },
    Filter {
        predicate: BoundExpr,
        child: Box<LogicalPlan>,
    },
    Sort {
        keys: Vec<SortKey>,
        child: Box<LogicalPlan>,
    },
    Count {
        alias: String,
        child: Box<LogicalPlan>,
    },
    GroupCount {
        group_col_idx: usize,
        group_col_name: String,
        group_col_type: DataType,
        count_alias: String,
        child: Box<LogicalPlan>,
    },
    GroupAggregate {
        group_keys: Vec<GroupKey>,
        aggregates: Vec<AggregateExpr>,
        output_schema: Schema,
        child: Box<LogicalPlan>,
    },
    Project {
        projection: Option<Vec<usize>>, // None => all
        projection_names: Vec<String>,
        output_schema: Schema,
        child: Box<LogicalPlan>,
    },
    ProjectExpr {
        exprs: Vec<BoundExpr>,
        projection_names: Vec<String>,
        output_schema: Schema,
        child: Box<LogicalPlan>,
    },
    Limit {
        count: usize,
        offset: usize,
        child: Box<LogicalPlan>,
    },
    Insert {
        table: TableInfo,
        rows: Vec<Vec<Value>>,
    },
    InsertFromSelect {
        table: TableInfo,
        input: Box<LogicalPlan>,
    },
    Update {
        table: TableInfo,
        assignments: Vec<UpdateAssignment>,
        filter: Option<BoundExpr>,
    },
    UpdateAdvanced {
        target: BoundTableRef,
        from: Vec<BoundTableRef>,
        assignments: Vec<UpdateAssignment>,
        filter: Option<BoundExpr>,
        returning: Option<ReturningBinding>,
    },
    Delete {
        table: TableInfo,
        filter: Option<BoundExpr>,
    },
    DeleteAdvanced {
        targets: Vec<String>,
        from: Vec<BoundTableRef>,
        using: Vec<BoundTableRef>,
        filter: Option<BoundExpr>,
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
        index_type: IndexType,
    },
    DropIndex {
        index_name: String,
        if_exists: bool,
    },
    AlterTable {
        table_name: String,
        operations: Vec<AlterTableOp>,
    },
}
