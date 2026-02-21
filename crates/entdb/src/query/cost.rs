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

use crate::query::plan::LogicalPlan;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorKind {
    Scan,
    Filter,
    Project,
    Join,
    Sort,
    Aggregate,
    Limit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryRisk {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy)]
pub struct CostWeights {
    pub row_read_cost: f64,
    pub cpu_expr_cost: f64,
    pub join_factor: f64,
    pub sort_factor: f64,
    pub aggregate_factor: f64,
    pub memory_risk_penalty: f64,
}

impl Default for CostWeights {
    fn default() -> Self {
        Self {
            row_read_cost: 1.0,
            cpu_expr_cost: 0.15,
            join_factor: 1.4,
            sort_factor: 1.8,
            aggregate_factor: 1.2,
            memory_risk_penalty: 250.0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct OperatorEstimate {
    pub kind: OperatorKind,
    pub input_rows: u64,
    pub output_rows: u64,
    pub expr_complexity: u32,
    pub memory_risk: MemoryRisk,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OperatorCostBreakdown {
    pub io_cost: f64,
    pub cpu_cost: f64,
    pub memory_penalty: f64,
    pub total_cost: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanCostSummary {
    pub estimated_rows: u64,
    pub estimated_cost: f64,
    pub rationale: String,
}

pub struct CostModel;

impl CostModel {
    pub fn estimate_operator_cost(
        estimate: OperatorEstimate,
        weights: CostWeights,
    ) -> OperatorCostBreakdown {
        let input_rows = estimate.input_rows as f64;
        let output_rows = estimate.output_rows as f64;
        let complexity = estimate.expr_complexity.max(1) as f64;

        let io_cost = (input_rows.max(output_rows)) * weights.row_read_cost;
        let base_cpu = output_rows * complexity * weights.cpu_expr_cost;
        let kind_factor = match estimate.kind {
            OperatorKind::Scan => 1.0,
            OperatorKind::Filter => 1.1,
            OperatorKind::Project => 1.0,
            OperatorKind::Join => weights.join_factor,
            OperatorKind::Sort => weights.sort_factor,
            OperatorKind::Aggregate => weights.aggregate_factor,
            OperatorKind::Limit => 0.6,
        };
        let cpu_cost = base_cpu * kind_factor;
        let risk_mult = match estimate.memory_risk {
            MemoryRisk::Low => 0.0,
            MemoryRisk::Medium => 0.5,
            MemoryRisk::High => 1.0,
        };
        let memory_penalty = risk_mult * weights.memory_risk_penalty;
        let total_cost = (io_cost + cpu_cost + memory_penalty).max(0.0);

        OperatorCostBreakdown {
            io_cost,
            cpu_cost,
            memory_penalty,
            total_cost,
        }
    }

    pub fn estimate_plan_cost(plan: &LogicalPlan, weights: CostWeights) -> PlanCostSummary {
        let (rows, cost, reason) = estimate_plan_inner(plan, weights);
        PlanCostSummary {
            estimated_rows: rows,
            estimated_cost: cost,
            rationale: reason,
        }
    }
}

fn estimate_plan_inner(plan: &LogicalPlan, weights: CostWeights) -> (u64, f64, String) {
    match plan {
        LogicalPlan::SeqScan { table } => {
            let rows = 1000_u64.max(table.schema.columns.len() as u64 * 50);
            let breakdown = CostModel::estimate_operator_cost(
                OperatorEstimate {
                    kind: OperatorKind::Scan,
                    input_rows: rows,
                    output_rows: rows,
                    expr_complexity: 1,
                    memory_risk: MemoryRisk::Low,
                },
                weights,
            );
            (
                rows,
                breakdown.total_cost,
                format!("scan rows={rows} on {}", table.name),
            )
        }
        LogicalPlan::Filter { child, .. } => {
            let (in_rows, child_cost, _) = estimate_plan_inner(child, weights);
            let out_rows = ((in_rows as f64) * 0.30).round() as u64;
            let breakdown = CostModel::estimate_operator_cost(
                OperatorEstimate {
                    kind: OperatorKind::Filter,
                    input_rows: in_rows,
                    output_rows: out_rows.max(1),
                    expr_complexity: 2,
                    memory_risk: MemoryRisk::Low,
                },
                weights,
            );
            (
                out_rows.max(1),
                child_cost + breakdown.total_cost,
                format!(
                    "filter selectivity~0.30 rows={} -> {}",
                    in_rows,
                    out_rows.max(1)
                ),
            )
        }
        LogicalPlan::Project { child, .. } | LogicalPlan::ProjectExpr { child, .. } => {
            let (rows, child_cost, _) = estimate_plan_inner(child, weights);
            let breakdown = CostModel::estimate_operator_cost(
                OperatorEstimate {
                    kind: OperatorKind::Project,
                    input_rows: rows,
                    output_rows: rows,
                    expr_complexity: 2,
                    memory_risk: MemoryRisk::Low,
                },
                weights,
            );
            (
                rows,
                child_cost + breakdown.total_cost,
                format!("project rows={rows}"),
            )
        }
        LogicalPlan::Sort { child, .. } => {
            let (rows, child_cost, _) = estimate_plan_inner(child, weights);
            let breakdown = CostModel::estimate_operator_cost(
                OperatorEstimate {
                    kind: OperatorKind::Sort,
                    input_rows: rows,
                    output_rows: rows,
                    expr_complexity: 1,
                    memory_risk: if rows > 10_000 {
                        MemoryRisk::High
                    } else {
                        MemoryRisk::Medium
                    },
                },
                weights,
            );
            (
                rows,
                child_cost + breakdown.total_cost,
                format!("sort rows={rows}"),
            )
        }
        LogicalPlan::NestedLoopJoin { left, right, .. } => {
            let (l_rows, l_cost, _) = estimate_plan_inner(left, weights);
            let (r_rows, r_cost, _) = estimate_plan_inner(right, weights);
            let out_rows = l_rows.min(r_rows).max(1);
            let breakdown = CostModel::estimate_operator_cost(
                OperatorEstimate {
                    kind: OperatorKind::Join,
                    input_rows: l_rows.saturating_add(r_rows),
                    output_rows: out_rows,
                    expr_complexity: 2,
                    memory_risk: MemoryRisk::Medium,
                },
                weights,
            );
            (
                out_rows,
                l_cost + r_cost + breakdown.total_cost,
                format!("join rows={}x{} -> {}", l_rows, r_rows, out_rows),
            )
        }
        LogicalPlan::GroupAggregate { child, .. } | LogicalPlan::GroupCount { child, .. } => {
            let (in_rows, child_cost, _) = estimate_plan_inner(child, weights);
            let out_rows = ((in_rows as f64).sqrt().round() as u64).max(1);
            let breakdown = CostModel::estimate_operator_cost(
                OperatorEstimate {
                    kind: OperatorKind::Aggregate,
                    input_rows: in_rows,
                    output_rows: out_rows,
                    expr_complexity: 2,
                    memory_risk: if in_rows > 10_000 {
                        MemoryRisk::High
                    } else {
                        MemoryRisk::Medium
                    },
                },
                weights,
            );
            (
                out_rows,
                child_cost + breakdown.total_cost,
                format!("aggregate rows={} -> {}", in_rows, out_rows),
            )
        }
        LogicalPlan::Limit { child, count, .. } => {
            let (in_rows, child_cost, _) = estimate_plan_inner(child, weights);
            let out_rows = in_rows.min(*count as u64);
            let breakdown = CostModel::estimate_operator_cost(
                OperatorEstimate {
                    kind: OperatorKind::Limit,
                    input_rows: in_rows,
                    output_rows: out_rows,
                    expr_complexity: 1,
                    memory_risk: MemoryRisk::Low,
                },
                weights,
            );
            (
                out_rows,
                child_cost + breakdown.total_cost,
                format!("limit rows={} -> {}", in_rows, out_rows),
            )
        }
        LogicalPlan::Union { left, right, .. } => {
            let (l_rows, l_cost, _) = estimate_plan_inner(left, weights);
            let (r_rows, r_cost, _) = estimate_plan_inner(right, weights);
            let rows = l_rows.saturating_add(r_rows);
            (
                rows,
                l_cost + r_cost,
                format!("union rows={}+{}", l_rows, r_rows),
            )
        }
        LogicalPlan::Count { child, .. } => {
            let (in_rows, child_cost, _) = estimate_plan_inner(child, weights);
            let breakdown = CostModel::estimate_operator_cost(
                OperatorEstimate {
                    kind: OperatorKind::Aggregate,
                    input_rows: in_rows,
                    output_rows: 1,
                    expr_complexity: 1,
                    memory_risk: MemoryRisk::Low,
                },
                weights,
            );
            (
                1,
                child_cost + breakdown.total_cost,
                "count aggregate".to_string(),
            )
        }
        LogicalPlan::Values { rows, .. } => {
            let n = rows.len() as u64;
            (n, n as f64, format!("values rows={n}"))
        }
        LogicalPlan::InsertFromSelect { input, .. } => {
            let (rows, cost, reason) = estimate_plan_inner(input, weights);
            (
                0,
                cost,
                format!("insert-from-select source: {reason}; rows={rows}"),
            )
        }
        _ => (0, 0.0, "non-row operator baseline".to_string()),
    }
}
