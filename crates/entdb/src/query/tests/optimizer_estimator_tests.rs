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

use crate::query::cardinality::{CardinalityEstimator, ColumnCardinalityStats, EstimateConfidence};
use crate::query::expression::{BinaryOp, BoundExpr};
use crate::types::Value;

#[test]
fn filter_eq_estimate_handles_null_heavy_column() {
    let pred = BoundExpr::BinaryOp {
        op: BinaryOp::Eq,
        left: Box::new(BoundExpr::ColumnRef { col_idx: 0 }),
        right: Box::new(BoundExpr::Literal(Value::Int32(7))),
    };
    let stats = vec![ColumnCardinalityStats {
        ndv: Some(5),
        null_fraction: 0.8,
    }];

    let est = CardinalityEstimator::estimate_filter_selectivity(1000, &pred, &stats);
    assert_eq!(est.confidence, EstimateConfidence::High);
    assert!(!est.used_fallback);
    assert_eq!(est.rows, 40);
}

#[test]
fn filter_cold_start_fallback_is_conservative_and_low_confidence() {
    let pred = BoundExpr::BinaryOp {
        op: BinaryOp::Eq,
        left: Box::new(BoundExpr::ColumnRef { col_idx: 0 }),
        right: Box::new(BoundExpr::Literal(Value::Int32(1))),
    };
    let est = CardinalityEstimator::estimate_filter_selectivity(1000, &pred, &[]);
    assert!(est.used_fallback);
    assert_eq!(est.confidence, EstimateConfidence::Low);
    assert!(est.rows > 0);
    assert!(est.rows < 1000);
}

#[test]
fn join_estimate_skewed_ndv_prefers_high_row_result() {
    let est = CardinalityEstimator::estimate_join_cardinality(
        1_000,
        1_000,
        Some(ColumnCardinalityStats {
            ndv: Some(1),
            null_fraction: 0.0,
        }),
        Some(ColumnCardinalityStats {
            ndv: Some(1),
            null_fraction: 0.0,
        }),
    );
    assert_eq!(est.confidence, EstimateConfidence::High);
    assert!(!est.used_fallback);
    assert_eq!(est.rows, 1_000_000);
}

#[test]
fn join_empty_input_estimates_zero() {
    let est = CardinalityEstimator::estimate_join_cardinality(
        0,
        500,
        Some(ColumnCardinalityStats {
            ndv: Some(10),
            null_fraction: 0.0,
        }),
        Some(ColumnCardinalityStats {
            ndv: Some(10),
            null_fraction: 0.0,
        }),
    );
    assert_eq!(est.rows, 0);
    assert_eq!(est.selectivity, 0.0);
}

#[test]
fn group_estimate_uses_known_ndv_product() {
    let est = CardinalityEstimator::estimate_group_cardinality(
        1_000,
        &[
            Some(ColumnCardinalityStats {
                ndv: Some(10),
                null_fraction: 0.0,
            }),
            Some(ColumnCardinalityStats {
                ndv: Some(20),
                null_fraction: 0.0,
            }),
        ],
    );
    assert_eq!(est.confidence, EstimateConfidence::High);
    assert_eq!(est.rows, 200);
}

#[test]
fn group_estimate_cold_start_uses_sqrt_fallback() {
    let est = CardinalityEstimator::estimate_group_cardinality(10_000, &[None, None]);
    assert_eq!(est.confidence, EstimateConfidence::Low);
    assert!(est.used_fallback);
    assert_eq!(est.rows, 100);
}

#[test]
fn filter_and_or_combination_produces_bounded_selectivity() {
    let p1 = BoundExpr::BinaryOp {
        op: BinaryOp::Eq,
        left: Box::new(BoundExpr::ColumnRef { col_idx: 0 }),
        right: Box::new(BoundExpr::Literal(Value::Int32(1))),
    };
    let p2 = BoundExpr::BinaryOp {
        op: BinaryOp::Gt,
        left: Box::new(BoundExpr::ColumnRef { col_idx: 1 }),
        right: Box::new(BoundExpr::Literal(Value::Int32(5))),
    };
    let pred = BoundExpr::BinaryOp {
        op: BinaryOp::Or,
        left: Box::new(BoundExpr::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(p1),
            right: Box::new(p2),
        }),
        right: Box::new(BoundExpr::BinaryOp {
            op: BinaryOp::Eq,
            left: Box::new(BoundExpr::ColumnRef { col_idx: 0 }),
            right: Box::new(BoundExpr::Literal(Value::Null)),
        }),
    };
    let stats = vec![
        ColumnCardinalityStats {
            ndv: Some(10),
            null_fraction: 0.1,
        },
        ColumnCardinalityStats {
            ndv: Some(100),
            null_fraction: 0.0,
        },
    ];

    let est = CardinalityEstimator::estimate_filter_selectivity(5_000, &pred, &stats);
    assert!(est.selectivity > 0.0);
    assert!(est.selectivity <= 1.0);
    assert!(est.rows <= 5_000);
}
