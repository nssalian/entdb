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

use crate::query::binder::{BoundSource, BoundStatement};
use crate::query::cost::{CostModel, CostWeights, PlanCostSummary};
use crate::query::expression::BoundExpr;
use crate::query::history::OptimizerHistoryRecord;
use crate::query::plan::LogicalPlan;
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

pub struct Optimizer;

#[derive(Debug, Clone, Copy)]
pub struct OptimizerConfig {
    pub cbo_enabled: bool,
    pub hbo_enabled: bool,
    pub max_search_ms: u64,
    pub max_join_relations: usize,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            cbo_enabled: true,
            hbo_enabled: false,
            max_search_ms: 50,
            max_join_relations: 8,
        }
    }
}

impl OptimizerConfig {
    pub fn sanitize(self) -> Self {
        Self {
            cbo_enabled: self.cbo_enabled,
            hbo_enabled: self.hbo_enabled,
            max_search_ms: self.max_search_ms.clamp(1, 5_000),
            max_join_relations: self.max_join_relations.clamp(1, 32),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OptimizerStageTrace {
    pub stage: String,
    pub elapsed_ms: u64,
    pub plan_root: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OptimizerTrace {
    pub fingerprint: String,
    pub cbo_enabled: bool,
    pub hbo_enabled: bool,
    pub max_search_ms: u64,
    pub stages: Vec<OptimizerStageTrace>,
    pub fallback_reason: Option<String>,
    pub enumerated_candidates: usize,
    pub chosen_plan_signature: Option<String>,
    pub history_matches: usize,
    pub hbo_applied: bool,
    pub hbo_contradictory: bool,
    pub chosen_base_cost: Option<f64>,
    pub chosen_adjusted_cost: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct OptimizeOutcome {
    pub plan: LogicalPlan,
    pub trace: OptimizerTrace,
}

impl Optimizer {
    pub fn optimize(plan: LogicalPlan) -> LogicalPlan {
        Self::optimize_with_trace(plan, "", OptimizerConfig::default()).plan
    }

    pub fn optimize_with_trace(
        plan: LogicalPlan,
        fingerprint: &str,
        config: OptimizerConfig,
    ) -> OptimizeOutcome {
        Self::optimize_with_trace_and_history(plan, fingerprint, config, &[])
    }

    pub fn optimize_with_trace_and_history(
        plan: LogicalPlan,
        fingerprint: &str,
        config: OptimizerConfig,
        history: &[OptimizerHistoryRecord],
    ) -> OptimizeOutcome {
        let config = config.sanitize();
        let mut current = plan.clone();
        let mut trace = OptimizerTrace {
            fingerprint: fingerprint.to_string(),
            cbo_enabled: config.cbo_enabled,
            hbo_enabled: config.hbo_enabled,
            max_search_ms: config.max_search_ms,
            stages: Vec::new(),
            fallback_reason: None,
            enumerated_candidates: 0,
            chosen_plan_signature: None,
            history_matches: 0,
            hbo_applied: false,
            hbo_contradictory: false,
            chosen_base_cost: None,
            chosen_adjusted_cost: None,
        };

        if !config.cbo_enabled {
            trace.fallback_reason = Some("optimizer disabled by config".to_string());
            return OptimizeOutcome { plan, trace };
        }

        if config.max_search_ms == 0 {
            trace.fallback_reason = Some("optimizer max_search_ms is 0".to_string());
            return OptimizeOutcome { plan, trace };
        }

        let deadline = Instant::now() + Duration::from_millis(config.max_search_ms);

        let stage_start = Instant::now();
        current = match run_stage(|| Self::stage_logical_rewrite(current, config, deadline)) {
            Ok(next) => next,
            Err(reason) => {
                trace.fallback_reason = Some(reason);
                return OptimizeOutcome { plan, trace };
            }
        };
        trace.stages.push(OptimizerStageTrace {
            stage: "logical_rewrite".to_string(),
            elapsed_ms: stage_start.elapsed().as_millis() as u64,
            plan_root: plan_root_name(&current).to_string(),
        });

        if Instant::now() > deadline {
            trace.fallback_reason = Some("optimizer timeout before stage 'estimate'".to_string());
            return OptimizeOutcome { plan, trace };
        }

        let stage_start = Instant::now();
        current = match run_stage(|| Self::stage_estimate(current, config, deadline)) {
            Ok(next) => next,
            Err(reason) => {
                trace.fallback_reason = Some(reason);
                return OptimizeOutcome { plan, trace };
            }
        };
        trace.stages.push(OptimizerStageTrace {
            stage: "estimate".to_string(),
            elapsed_ms: stage_start.elapsed().as_millis() as u64,
            plan_root: plan_root_name(&current).to_string(),
        });

        if Instant::now() > deadline {
            trace.fallback_reason = Some("optimizer timeout before stage 'enumerate'".to_string());
            return OptimizeOutcome { plan, trace };
        }

        let stage_start = Instant::now();
        let candidates =
            match run_stage(|| Self::stage_enumerate_candidates(&current, config, deadline)) {
                Ok(cands) => cands,
                Err(reason) => {
                    trace.fallback_reason = Some(reason);
                    return OptimizeOutcome { plan, trace };
                }
            };
        trace.enumerated_candidates = candidates.len();
        trace.stages.push(OptimizerStageTrace {
            stage: "enumerate".to_string(),
            elapsed_ms: stage_start.elapsed().as_millis() as u64,
            plan_root: plan_root_name(&current).to_string(),
        });

        if Instant::now() > deadline {
            trace.fallback_reason = Some("optimizer timeout before stage 'choose'".to_string());
            return OptimizeOutcome { plan, trace };
        }

        let stage_start = Instant::now();
        current = match run_stage(|| {
            Self::stage_choose(candidates, config, deadline, history, &mut trace)
        }) {
            Ok(next) => next,
            Err(reason) => {
                trace.fallback_reason = Some(reason);
                return OptimizeOutcome { plan, trace };
            }
        };
        trace.chosen_plan_signature = Some(plan_signature(&current));
        trace.stages.push(OptimizerStageTrace {
            stage: "choose".to_string(),
            elapsed_ms: stage_start.elapsed().as_millis() as u64,
            plan_root: plan_root_name(&current).to_string(),
        });

        if Instant::now() > deadline {
            trace.fallback_reason = Some("optimizer timeout before stage 'finalize'".to_string());
            return OptimizeOutcome { plan, trace };
        }

        let stage_start = Instant::now();
        current = match run_stage(|| Self::stage_finalize(current, config, deadline)) {
            Ok(next) => next,
            Err(reason) => {
                trace.fallback_reason = Some(reason);
                return OptimizeOutcome { plan, trace };
            }
        };
        trace.stages.push(OptimizerStageTrace {
            stage: "finalize".to_string(),
            elapsed_ms: stage_start.elapsed().as_millis() as u64,
            plan_root: plan_root_name(&current).to_string(),
        });

        OptimizeOutcome {
            plan: current,
            trace,
        }
    }

    pub fn fingerprint_bound_statement(stmt: &BoundStatement) -> String {
        let mut out = String::new();
        fingerprint_stmt(stmt, &mut out);
        out
    }

    pub fn explain_cost(plan: &LogicalPlan, weights: CostWeights) -> PlanCostSummary {
        CostModel::estimate_plan_cost(plan, weights)
    }

    pub fn enumerate_candidate_signatures(
        plan: &LogicalPlan,
        config: OptimizerConfig,
    ) -> Vec<String> {
        let config = config.sanitize();
        let deadline = Instant::now() + Duration::from_millis(config.max_search_ms.max(1));
        match Self::stage_enumerate_candidates(plan, config, deadline) {
            Ok(candidates) => candidates.into_iter().map(|p| plan_signature(&p)).collect(),
            Err(_) => vec![plan_signature(plan)],
        }
    }
}

fn run_stage<T, F>(f: F) -> std::result::Result<T, String>
where
    F: FnOnce() -> std::result::Result<T, String>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(_) => Err("optimizer stage panicked; fallback to baseline".to_string()),
    }
}

impl Optimizer {
    fn stage_logical_rewrite(
        plan: LogicalPlan,
        _config: OptimizerConfig,
        _deadline: Instant,
    ) -> std::result::Result<LogicalPlan, String> {
        Ok(Self::predicate_pushdown(plan))
    }

    fn stage_estimate(
        plan: LogicalPlan,
        _config: OptimizerConfig,
        _deadline: Instant,
    ) -> std::result::Result<LogicalPlan, String> {
        Ok(plan)
    }

    fn stage_enumerate_candidates(
        plan: &LogicalPlan,
        config: OptimizerConfig,
        deadline: Instant,
    ) -> std::result::Result<Vec<LogicalPlan>, String> {
        let joins = count_joins(plan);
        if joins > config.max_join_relations {
            return Err(format!(
                "join graph size {joins} exceeds max_join_relations {}",
                config.max_join_relations
            ));
        }

        const MAX_ENUM_CANDIDATES: usize = 32;
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(plan.clone());

        while let Some(candidate) = queue.pop_front() {
            if Instant::now() > deadline {
                return Err("optimizer timeout during enumeration".to_string());
            }
            let sig = plan_signature(&candidate);
            if !seen.insert(sig) {
                continue;
            }
            out.push(candidate.clone());
            if out.len() >= MAX_ENUM_CANDIDATES {
                break;
            }

            for next in one_swap_variants(&candidate) {
                if out.len() + queue.len() >= MAX_ENUM_CANDIDATES {
                    break;
                }
                queue.push_back(next);
            }
        }

        if out.is_empty() {
            out.push(plan.clone());
        }
        Ok(out)
    }

    fn stage_choose(
        candidates: Vec<LogicalPlan>,
        _config: OptimizerConfig,
        deadline: Instant,
        history: &[OptimizerHistoryRecord],
        trace: &mut OptimizerTrace,
    ) -> std::result::Result<LogicalPlan, String> {
        let weights = CostWeights::default();
        let mut best: Option<(LogicalPlan, f64, f64, String)> = None;
        let mut hbo_map = HashMap::<String, HistoryAdjustedScore>::new();
        if trace.hbo_enabled {
            hbo_map = build_history_score_map(history, now_millis());
            trace.history_matches = hbo_map.len();
            trace.hbo_contradictory = hbo_map.values().any(|s| s.contradictory);
        }
        for candidate in candidates {
            if Instant::now() > deadline {
                return Err("optimizer timeout during choose".to_string());
            }
            let summary = CostModel::estimate_plan_cost(&candidate, weights);
            let sig = plan_signature(&candidate);
            let adjusted = if trace.hbo_enabled {
                if let Some(h) = hbo_map.get(&sig) {
                    if h.contradictory || h.confidence < HBO_MIN_CONFIDENCE {
                        summary.estimated_cost
                    } else {
                        trace.hbo_applied = true;
                        summary.estimated_cost * h.correction_factor
                    }
                } else {
                    summary.estimated_cost
                }
            } else {
                summary.estimated_cost
            };
            match &best {
                None => best = Some((candidate, summary.estimated_cost, adjusted, sig)),
                Some((_, _, best_adj, best_sig)) => {
                    if adjusted < *best_adj || (adjusted == *best_adj && sig < *best_sig) {
                        best = Some((candidate, summary.estimated_cost, adjusted, sig));
                    }
                }
            }
        }
        if let Some((_, base, adjusted, _)) = &best {
            trace.chosen_base_cost = Some(*base);
            trace.chosen_adjusted_cost = Some(*adjusted);
        }
        best.map(|(plan, _, _, _)| plan)
            .ok_or_else(|| "no candidate plan generated".to_string())
    }

    fn stage_finalize(
        plan: LogicalPlan,
        _config: OptimizerConfig,
        _deadline: Instant,
    ) -> std::result::Result<LogicalPlan, String> {
        Ok(plan)
    }

    fn predicate_pushdown(plan: LogicalPlan) -> LogicalPlan {
        // Current plans are already produced in scan->filter->project order where applicable.
        plan
    }
}

const HBO_STALENESS_TTL_MS: u64 = 24 * 60 * 60 * 1000;
const HBO_MIN_CONFIDENCE: f64 = 0.30;

#[derive(Debug, Clone, Copy)]
struct HistoryAdjustedScore {
    correction_factor: f64,
    confidence: f64,
    contradictory: bool,
}

fn build_history_score_map(
    history: &[OptimizerHistoryRecord],
    now_ms: u64,
) -> HashMap<String, HistoryAdjustedScore> {
    let mut grouped: HashMap<String, Vec<&OptimizerHistoryRecord>> = HashMap::new();
    for rec in history {
        if !rec.success {
            continue;
        }
        if now_ms.saturating_sub(rec.captured_at_ms) > HBO_STALENESS_TTL_MS {
            continue;
        }
        grouped
            .entry(rec.plan_signature.clone())
            .or_default()
            .push(rec);
    }

    let mut out = HashMap::new();
    for (sig, recs) in grouped {
        let mut weighted_sum = 0.0;
        let mut weight_sum = 0.0;
        let mut latencies = Vec::with_capacity(recs.len());
        let mut conf_sum = 0.0;

        for r in &recs {
            let age = now_ms.saturating_sub(r.captured_at_ms) as f64;
            let age_decay = 1.0 / (1.0 + age / (60_000.0 * 60.0));
            let weight = age_decay * r.confidence.clamp(0.0, 1.0).max(0.05);
            weighted_sum += weight * r.latency_ms as f64;
            weight_sum += weight;
            latencies.push(r.latency_ms as f64);
            conf_sum += r.confidence.clamp(0.0, 1.0);
        }
        let mean_latency = if weight_sum > 0.0 {
            weighted_sum / weight_sum
        } else {
            1.0
        };
        let confidence = (conf_sum / recs.len() as f64).clamp(0.0, 1.0)
            * (recs.len() as f64 / 5.0).clamp(0.0, 1.0);
        let contradictory = is_contradictory(&latencies);
        let correction_factor = (mean_latency / 100.0).clamp(0.5, 2.0);
        out.insert(
            sig,
            HistoryAdjustedScore {
                correction_factor,
                confidence,
                contradictory,
            },
        );
    }
    out
}

fn is_contradictory(samples: &[f64]) -> bool {
    if samples.len() < 3 {
        return false;
    }
    let mean = samples.iter().copied().sum::<f64>() / samples.len() as f64;
    if mean <= 0.0 {
        return false;
    }
    let var = samples
        .iter()
        .map(|x| {
            let d = *x - mean;
            d * d
        })
        .sum::<f64>()
        / samples.len() as f64;
    let stddev = var.sqrt();
    (stddev / mean) > 1.0
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn one_swap_variants(plan: &LogicalPlan) -> Vec<LogicalPlan> {
    // Keep variants semantics-preserving.
    // Structural join rewrites are intentionally disabled until column-index rewrites are safe.
    if !is_read_plan(plan) {
        return Vec::new();
    }
    if matches!(
        plan,
        LogicalPlan::Limit {
            count,
            offset,
            ..
        } if *count == usize::MAX && *offset == 0
    ) {
        return Vec::new();
    }
    vec![LogicalPlan::Limit {
        count: usize::MAX,
        offset: 0,
        child: Box::new(plan.clone()),
    }]
}

fn is_read_plan(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Values { .. }
            | LogicalPlan::SeqScan { .. }
            | LogicalPlan::Bm25Scan { .. }
            | LogicalPlan::NestedLoopJoin { .. }
            | LogicalPlan::Union { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Count { .. }
            | LogicalPlan::GroupCount { .. }
            | LogicalPlan::GroupAggregate { .. }
            | LogicalPlan::Project { .. }
            | LogicalPlan::ProjectExpr { .. }
            | LogicalPlan::Limit { .. }
    )
}

fn plan_signature(plan: &LogicalPlan) -> String {
    match plan {
        LogicalPlan::Values { rows, .. } => format!("Values({})", rows.len()),
        LogicalPlan::SeqScan { table } => format!("SeqScan({})", table.name),
        LogicalPlan::Bm25Scan {
            table, index_name, ..
        } => {
            format!("Bm25Scan({},{})", table.name, index_name)
        }
        LogicalPlan::NestedLoopJoin {
            left,
            right,
            left_on,
            right_on,
            ..
        } => format!(
            "Join({},{};{}={})",
            plan_signature(left),
            plan_signature(right),
            left_on,
            right_on
        ),
        LogicalPlan::Union {
            left, right, all, ..
        } => format!(
            "Union({},{};all={})",
            plan_signature(left),
            plan_signature(right),
            all
        ),
        LogicalPlan::Filter { child, .. } => format!("Filter({})", plan_signature(child)),
        LogicalPlan::Sort { child, .. } => format!("Sort({})", plan_signature(child)),
        LogicalPlan::Count { child, .. } => format!("Count({})", plan_signature(child)),
        LogicalPlan::GroupCount { child, .. } => format!("GroupCount({})", plan_signature(child)),
        LogicalPlan::GroupAggregate { child, .. } => {
            format!("GroupAgg({})", plan_signature(child))
        }
        LogicalPlan::Project { child, .. } => format!("Project({})", plan_signature(child)),
        LogicalPlan::ProjectExpr { child, .. } => format!("ProjectExpr({})", plan_signature(child)),
        LogicalPlan::Limit {
            child,
            count,
            offset,
        } => {
            format!("Limit({},{},{})", plan_signature(child), count, offset)
        }
        LogicalPlan::Insert { table, .. } => format!("Insert({})", table.name),
        LogicalPlan::InsertFromSelect { table, input } => {
            format!("InsertFromSelect({},{})", table.name, plan_signature(input))
        }
        LogicalPlan::Update { table, .. } => format!("Update({})", table.name),
        LogicalPlan::UpdateAdvanced { target, .. } => format!("UpdateAdv({})", target.alias),
        LogicalPlan::Delete { table, .. } => format!("Delete({})", table.name),
        LogicalPlan::DeleteAdvanced { targets, .. } => format!("DeleteAdv({})", targets.join("|")),
        LogicalPlan::Upsert { table, .. } => format!("Upsert({})", table.name),
        LogicalPlan::Analyze { table } => format!("Analyze({})", table.name),
        LogicalPlan::Truncate { tables } => format!("Truncate({})", tables.len()),
        LogicalPlan::CreateTable { name, .. } => format!("CreateTable({})", name),
        LogicalPlan::DropTable { name, .. } => format!("DropTable({})", name),
        LogicalPlan::CreateIndex { index_name, .. } => format!("CreateIndex({})", index_name),
        LogicalPlan::DropIndex { index_name, .. } => format!("DropIndex({})", index_name),
        LogicalPlan::AlterTable { table_name, .. } => format!("AlterTable({})", table_name),
    }
}

fn plan_root_name(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Values { .. } => "Values",
        LogicalPlan::SeqScan { .. } => "SeqScan",
        LogicalPlan::Bm25Scan { .. } => "Bm25Scan",
        LogicalPlan::NestedLoopJoin { .. } => "NestedLoopJoin",
        LogicalPlan::Union { .. } => "Union",
        LogicalPlan::Filter { .. } => "Filter",
        LogicalPlan::Sort { .. } => "Sort",
        LogicalPlan::Count { .. } => "Count",
        LogicalPlan::GroupCount { .. } => "GroupCount",
        LogicalPlan::GroupAggregate { .. } => "GroupAggregate",
        LogicalPlan::Project { .. } => "Project",
        LogicalPlan::ProjectExpr { .. } => "ProjectExpr",
        LogicalPlan::Limit { .. } => "Limit",
        LogicalPlan::Insert { .. } => "Insert",
        LogicalPlan::InsertFromSelect { .. } => "InsertFromSelect",
        LogicalPlan::Update { .. } => "Update",
        LogicalPlan::UpdateAdvanced { .. } => "UpdateAdvanced",
        LogicalPlan::Delete { .. } => "Delete",
        LogicalPlan::DeleteAdvanced { .. } => "DeleteAdvanced",
        LogicalPlan::Upsert { .. } => "Upsert",
        LogicalPlan::Analyze { .. } => "Analyze",
        LogicalPlan::Truncate { .. } => "Truncate",
        LogicalPlan::CreateTable { .. } => "CreateTable",
        LogicalPlan::DropTable { .. } => "DropTable",
        LogicalPlan::CreateIndex { .. } => "CreateIndex",
        LogicalPlan::DropIndex { .. } => "DropIndex",
        LogicalPlan::AlterTable { .. } => "AlterTable",
    }
}

fn count_joins(plan: &LogicalPlan) -> usize {
    match plan {
        LogicalPlan::NestedLoopJoin { left, right, .. } => {
            1 + count_joins(left) + count_joins(right)
        }
        LogicalPlan::Union { left, right, .. } => count_joins(left) + count_joins(right),
        LogicalPlan::Filter { child, .. }
        | LogicalPlan::Sort { child, .. }
        | LogicalPlan::Count { child, .. }
        | LogicalPlan::GroupCount { child, .. }
        | LogicalPlan::GroupAggregate { child, .. }
        | LogicalPlan::Project { child, .. }
        | LogicalPlan::ProjectExpr { child, .. }
        | LogicalPlan::Limit { child, .. } => count_joins(child),
        LogicalPlan::InsertFromSelect { input, .. } => count_joins(input),
        _ => 0,
    }
}

fn fingerprint_stmt(stmt: &BoundStatement, out: &mut String) {
    match stmt {
        BoundStatement::Select {
            source,
            projection,
            projection_exprs,
            projection_names,
            filter,
            order_by,
            limit,
            offset,
            aggregate,
        } => {
            out.push_str("select(");
            fingerprint_source(source, out);
            out.push_str(",proj=");
            match projection {
                Some(idxs) => out.push_str(&format!("idx{}", idxs.len())),
                None => out.push('*'),
            }
            if let Some(exprs) = projection_exprs {
                out.push_str(",expr=");
                out.push_str(&exprs.len().to_string());
            }
            out.push_str(",names=");
            out.push_str(&projection_names.join("|"));
            out.push_str(",filter=");
            out.push_str(if filter.is_some() { "y" } else { "n" });
            out.push_str(",ord=");
            out.push_str(&order_by.len().to_string());
            out.push_str(",lim=");
            out.push_str(&limit.unwrap_or(0).to_string());
            out.push_str(",off=");
            out.push_str(&offset.to_string());
            out.push_str(",agg=");
            out.push_str(if aggregate.is_some() { "y" } else { "n" });
            out.push(')');
        }
        BoundStatement::Insert { table, .. } => {
            out.push_str("insert(");
            out.push_str(&table.name);
            out.push(')');
        }
        BoundStatement::InsertSelect { table, source } => {
            out.push_str("insert_select(");
            out.push_str(&table.name);
            out.push(',');
            fingerprint_stmt(source, out);
            out.push(')');
        }
        BoundStatement::Update { table, from, .. } => {
            out.push_str("update(");
            out.push_str(&table.name);
            out.push_str(",from=");
            out.push_str(&from.len().to_string());
            out.push(')');
        }
        BoundStatement::Delete {
            table,
            targets,
            from,
            using,
            ..
        } => {
            out.push_str("delete(");
            out.push_str(&table.name);
            out.push_str(",targets=");
            out.push_str(&targets.len().to_string());
            out.push_str(",from=");
            out.push_str(&from.len().to_string());
            out.push_str(",using=");
            out.push_str(&using.len().to_string());
            out.push(')');
        }
        BoundStatement::Upsert {
            table,
            conflict_cols,
            ..
        } => {
            out.push_str("upsert(");
            out.push_str(&table.name);
            out.push_str(",conf=");
            out.push_str(&conflict_cols.len().to_string());
            out.push(')');
        }
        BoundStatement::Analyze { table } => {
            out.push_str("analyze(");
            out.push_str(&table.name);
            out.push(')');
        }
        BoundStatement::Truncate { tables } => {
            out.push_str("truncate(");
            out.push_str(&tables.len().to_string());
            out.push(')');
        }
        BoundStatement::CreateTable { name, .. } => {
            out.push_str("create_table(");
            out.push_str(name);
            out.push(')');
        }
        BoundStatement::DropTable { name, .. } => {
            out.push_str("drop_table(");
            out.push_str(name);
            out.push(')');
        }
        BoundStatement::CreateIndex {
            table_name,
            index_name,
            ..
        } => {
            out.push_str("create_index(");
            out.push_str(table_name);
            out.push(',');
            out.push_str(index_name);
            out.push(')');
        }
        BoundStatement::DropIndex { index_name, .. } => {
            out.push_str("drop_index(");
            out.push_str(index_name);
            out.push(')');
        }
        BoundStatement::AlterTable {
            table_name,
            operations,
        } => {
            out.push_str("alter_table(");
            out.push_str(table_name);
            out.push_str(",ops=");
            out.push_str(&operations.len().to_string());
            out.push(')');
        }
        BoundStatement::Union { left, right, all } => {
            out.push_str("union(");
            out.push_str(if *all { "all," } else { "distinct," });
            fingerprint_stmt(left, out);
            out.push(',');
            fingerprint_stmt(right, out);
            out.push(')');
        }
    }
}

fn fingerprint_source(source: &BoundSource, out: &mut String) {
    match source {
        BoundSource::Values { rows, .. } => {
            out.push_str("values(");
            out.push_str(&rows.len().to_string());
            out.push(')');
        }
        BoundSource::Single { table } => {
            out.push_str("table(");
            out.push_str(&table.name);
            out.push(')');
        }
        BoundSource::Derived { source, .. } => {
            out.push_str("derived(");
            fingerprint_stmt(source, out);
            out.push(')');
        }
        BoundSource::InnerJoin { left, right, .. } => {
            out.push_str("join(");
            fingerprint_source(left, out);
            out.push(',');
            fingerprint_source(right, out);
            out.push(')');
        }
    }
}

#[allow(dead_code)]
fn _expr_shape(expr: &BoundExpr) -> &'static str {
    match expr {
        BoundExpr::ColumnRef { .. } => "col",
        BoundExpr::Literal(_) => "lit",
        BoundExpr::BinaryOp { .. } => "binary",
        BoundExpr::Function { .. } => "fn",
        BoundExpr::WindowRowNumber { .. } => "row_number",
    }
}
