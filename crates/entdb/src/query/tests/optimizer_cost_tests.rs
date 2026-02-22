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

use crate::catalog::{Column, Schema, TableInfo};
use crate::query::cost::{CostModel, CostWeights, MemoryRisk, OperatorEstimate, OperatorKind};
use crate::query::plan::LogicalPlan;
use crate::types::DataType;

fn test_table(name: &str) -> TableInfo {
    TableInfo {
        table_id: 1,
        name: name.to_string(),
        schema: Schema::new(vec![Column {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            default: None,
            primary_key: true,
        }]),
        first_page_id: 1,
        indexes: Vec::new(),
        stats: Default::default(),
    }
}

#[test]
fn operator_cost_is_monotonic_with_rows_for_scan() {
    let weights = CostWeights::default();
    let small = CostModel::estimate_operator_cost(
        OperatorEstimate {
            kind: OperatorKind::Scan,
            input_rows: 100,
            output_rows: 100,
            expr_complexity: 1,
            memory_risk: MemoryRisk::Low,
        },
        weights,
    );
    let large = CostModel::estimate_operator_cost(
        OperatorEstimate {
            kind: OperatorKind::Scan,
            input_rows: 1000,
            output_rows: 1000,
            expr_complexity: 1,
            memory_risk: MemoryRisk::Low,
        },
        weights,
    );
    assert!(large.total_cost > small.total_cost);
}

#[test]
fn sort_cost_is_higher_than_scan_for_same_rows() {
    let weights = CostWeights::default();
    let scan = CostModel::estimate_operator_cost(
        OperatorEstimate {
            kind: OperatorKind::Scan,
            input_rows: 10_000,
            output_rows: 10_000,
            expr_complexity: 1,
            memory_risk: MemoryRisk::Low,
        },
        weights,
    );
    let sort = CostModel::estimate_operator_cost(
        OperatorEstimate {
            kind: OperatorKind::Sort,
            input_rows: 10_000,
            output_rows: 10_000,
            expr_complexity: 1,
            memory_risk: MemoryRisk::Medium,
        },
        weights,
    );
    assert!(sort.total_cost > scan.total_cost);
}

#[test]
fn memory_risk_penalty_increases_total_cost() {
    let weights = CostWeights::default();
    let low = CostModel::estimate_operator_cost(
        OperatorEstimate {
            kind: OperatorKind::Join,
            input_rows: 5000,
            output_rows: 2500,
            expr_complexity: 2,
            memory_risk: MemoryRisk::Low,
        },
        weights,
    );
    let high = CostModel::estimate_operator_cost(
        OperatorEstimate {
            kind: OperatorKind::Join,
            input_rows: 5000,
            output_rows: 2500,
            expr_complexity: 2,
            memory_risk: MemoryRisk::High,
        },
        weights,
    );
    assert!(high.total_cost > low.total_cost);
    assert!(high.memory_penalty > low.memory_penalty);
}

#[test]
fn configurable_weights_change_cost_output() {
    let base = CostModel::estimate_operator_cost(
        OperatorEstimate {
            kind: OperatorKind::Filter,
            input_rows: 1000,
            output_rows: 300,
            expr_complexity: 3,
            memory_risk: MemoryRisk::Low,
        },
        CostWeights::default(),
    );
    let tuned = CostModel::estimate_operator_cost(
        OperatorEstimate {
            kind: OperatorKind::Filter,
            input_rows: 1000,
            output_rows: 300,
            expr_complexity: 3,
            memory_risk: MemoryRisk::Low,
        },
        CostWeights {
            cpu_expr_cost: 2.0,
            ..CostWeights::default()
        },
    );
    assert!(tuned.total_cost > base.total_cost);
}

#[test]
fn plan_cost_summary_includes_rows_cost_and_rationale() {
    let plan = LogicalPlan::Limit {
        count: 10,
        offset: 0,
        child: Box::new(LogicalPlan::Sort {
            keys: Vec::new(),
            child: Box::new(LogicalPlan::Filter {
                predicate: crate::query::expression::BoundExpr::Literal(
                    crate::types::Value::Boolean(true),
                ),
                child: Box::new(LogicalPlan::SeqScan {
                    table: test_table("t"),
                }),
            }),
        }),
    };
    let summary = CostModel::estimate_plan_cost(&plan, CostWeights::default());
    assert!(summary.estimated_rows <= 10);
    assert!(summary.estimated_cost > 0.0);
    assert!(!summary.rationale.is_empty());
}
