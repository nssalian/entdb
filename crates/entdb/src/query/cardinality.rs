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

use crate::query::expression::{BinaryOp, BoundExpr};
use crate::types::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EstimateConfidence {
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CardinalityEstimate {
    pub rows: u64,
    pub selectivity: f64,
    pub confidence: EstimateConfidence,
    pub used_fallback: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ColumnCardinalityStats {
    pub ndv: Option<u64>,
    pub null_fraction: f64,
}

pub struct CardinalityEstimator;

impl CardinalityEstimator {
    pub fn estimate_filter_selectivity(
        input_rows: u64,
        predicate: &BoundExpr,
        column_stats: &[ColumnCardinalityStats],
    ) -> CardinalityEstimate {
        if input_rows == 0 {
            return CardinalityEstimate {
                rows: 0,
                selectivity: 0.0,
                confidence: EstimateConfidence::High,
                used_fallback: false,
            };
        }

        let (selectivity, confidence, used_fallback) =
            estimate_predicate_selectivity(predicate, column_stats);
        let rows = ((input_rows as f64) * selectivity).round() as u64;
        CardinalityEstimate {
            rows: rows.min(input_rows),
            selectivity,
            confidence,
            used_fallback,
        }
    }

    pub fn estimate_join_cardinality(
        left_rows: u64,
        right_rows: u64,
        left_key_stats: Option<ColumnCardinalityStats>,
        right_key_stats: Option<ColumnCardinalityStats>,
    ) -> CardinalityEstimate {
        if left_rows == 0 || right_rows == 0 {
            return CardinalityEstimate {
                rows: 0,
                selectivity: 0.0,
                confidence: EstimateConfidence::High,
                used_fallback: false,
            };
        }

        let cross = (left_rows as f64) * (right_rows as f64);
        let (rows_f64, confidence, used_fallback) = match (left_key_stats, right_key_stats) {
            (
                Some(ColumnCardinalityStats { ndv: Some(l), .. }),
                Some(ColumnCardinalityStats { ndv: Some(r), .. }),
            ) if l > 0 && r > 0 => {
                let max_ndv = l.max(r) as f64;
                ((cross / max_ndv).max(1.0), EstimateConfidence::High, false)
            }
            (Some(ColumnCardinalityStats { ndv: Some(l), .. }), _) if l > 0 => (
                (cross / (l as f64)).max(1.0),
                EstimateConfidence::Medium,
                false,
            ),
            (_, Some(ColumnCardinalityStats { ndv: Some(r), .. })) if r > 0 => (
                (cross / (r as f64)).max(1.0),
                EstimateConfidence::Medium,
                false,
            ),
            _ => (
                (left_rows.min(right_rows) as f64).max(1.0),
                EstimateConfidence::Low,
                true,
            ),
        };

        let rows = rows_f64.round() as u64;
        CardinalityEstimate {
            rows: rows.min((left_rows as u128 * right_rows as u128) as u64),
            selectivity: (rows_f64 / cross).clamp(0.0, 1.0),
            confidence,
            used_fallback,
        }
    }

    pub fn estimate_group_cardinality(
        input_rows: u64,
        group_cols: &[Option<ColumnCardinalityStats>],
    ) -> CardinalityEstimate {
        if input_rows == 0 {
            return CardinalityEstimate {
                rows: 0,
                selectivity: 0.0,
                confidence: EstimateConfidence::High,
                used_fallback: false,
            };
        }

        if group_cols.is_empty() {
            return CardinalityEstimate {
                rows: 1,
                selectivity: 1.0 / (input_rows as f64),
                confidence: EstimateConfidence::High,
                used_fallback: false,
            };
        }

        let mut ndv_product = 1_u128;
        let mut known = 0usize;
        for col in group_cols.iter().flatten() {
            if let Some(ndv) = col.ndv {
                known = known.saturating_add(1);
                ndv_product = ndv_product.saturating_mul((ndv.max(1)) as u128);
            }
        }

        let (rows, confidence, used_fallback) = if known == group_cols.len() {
            (
                (ndv_product.min(input_rows as u128) as u64).max(1),
                EstimateConfidence::High,
                false,
            )
        } else if known > 0 {
            (
                (ndv_product.min(input_rows as u128) as u64).max(1),
                EstimateConfidence::Medium,
                false,
            )
        } else {
            (
                ((input_rows as f64).sqrt().round() as u64).max(1),
                EstimateConfidence::Low,
                true,
            )
        };

        CardinalityEstimate {
            rows: rows.min(input_rows),
            selectivity: (rows as f64 / input_rows as f64).clamp(0.0, 1.0),
            confidence,
            used_fallback,
        }
    }
}

fn estimate_predicate_selectivity(
    expr: &BoundExpr,
    column_stats: &[ColumnCardinalityStats],
) -> (f64, EstimateConfidence, bool) {
    match expr {
        BoundExpr::BinaryOp {
            op: BinaryOp::And,
            left,
            right,
        } => {
            let (l_sel, l_conf, l_fallback) = estimate_predicate_selectivity(left, column_stats);
            let (r_sel, r_conf, r_fallback) = estimate_predicate_selectivity(right, column_stats);
            (
                (l_sel * r_sel).clamp(0.0, 1.0),
                min_confidence(l_conf, r_conf),
                l_fallback || r_fallback,
            )
        }
        BoundExpr::BinaryOp {
            op: BinaryOp::Or,
            left,
            right,
        } => {
            let (l_sel, l_conf, l_fallback) = estimate_predicate_selectivity(left, column_stats);
            let (r_sel, r_conf, r_fallback) = estimate_predicate_selectivity(right, column_stats);
            let or_sel = l_sel + r_sel - (l_sel * r_sel);
            (
                or_sel.clamp(0.0, 1.0),
                min_confidence(l_conf, r_conf),
                l_fallback || r_fallback,
            )
        }
        BoundExpr::BinaryOp {
            op: BinaryOp::Eq,
            left,
            right,
        } => {
            if let Some((col_idx, lit)) = col_lit_pair(left, right) {
                if *lit == Value::Null {
                    return estimate_is_null(col_idx, column_stats);
                }
                return estimate_eq(col_idx, column_stats);
            }
            (0.25, EstimateConfidence::Low, true)
        }
        BoundExpr::BinaryOp {
            op: BinaryOp::Gt | BinaryOp::Lt | BinaryOp::Gte | BinaryOp::Lte,
            left,
            right,
        } => {
            if let Some((col_idx, _)) = col_lit_pair(left, right) {
                return estimate_range(col_idx, column_stats);
            }
            (0.30, EstimateConfidence::Low, true)
        }
        _ => (0.25, EstimateConfidence::Low, true),
    }
}

fn col_lit_pair<'a>(left: &'a BoundExpr, right: &'a BoundExpr) -> Option<(usize, &'a Value)> {
    match (left, right) {
        (BoundExpr::ColumnRef { col_idx }, BoundExpr::Literal(v))
        | (BoundExpr::Literal(v), BoundExpr::ColumnRef { col_idx }) => Some((*col_idx, v)),
        _ => None,
    }
}

fn estimate_eq(
    col_idx: usize,
    stats: &[ColumnCardinalityStats],
) -> (f64, EstimateConfidence, bool) {
    let Some(col) = stats.get(col_idx).copied() else {
        return (0.10, EstimateConfidence::Low, true);
    };
    let ndv = col.ndv.unwrap_or(0);
    if ndv > 0 {
        let non_null = (1.0 - col.null_fraction).clamp(0.0, 1.0);
        (
            (non_null / ndv as f64).max(1e-9),
            EstimateConfidence::High,
            false,
        )
    } else {
        (0.10, EstimateConfidence::Medium, true)
    }
}

fn estimate_range(
    col_idx: usize,
    stats: &[ColumnCardinalityStats],
) -> (f64, EstimateConfidence, bool) {
    let Some(col) = stats.get(col_idx).copied() else {
        return (0.30, EstimateConfidence::Low, true);
    };
    let non_null = (1.0 - col.null_fraction).clamp(0.0, 1.0);
    (0.30 * non_null, EstimateConfidence::Medium, false)
}

fn estimate_is_null(
    col_idx: usize,
    stats: &[ColumnCardinalityStats],
) -> (f64, EstimateConfidence, bool) {
    let Some(col) = stats.get(col_idx).copied() else {
        return (0.05, EstimateConfidence::Low, true);
    };
    (
        col.null_fraction.clamp(0.0, 1.0),
        EstimateConfidence::High,
        false,
    )
}

fn min_confidence(a: EstimateConfidence, b: EstimateConfidence) -> EstimateConfidence {
    use EstimateConfidence::*;
    match (a, b) {
        (Low, _) | (_, Low) => Low,
        (Medium, _) | (_, Medium) => Medium,
        (High, High) => High,
    }
}
