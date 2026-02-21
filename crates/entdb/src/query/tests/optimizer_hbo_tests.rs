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
use crate::query::history::OptimizerHistoryRecord;
use crate::query::optimizer::{Optimizer, OptimizerConfig};
use crate::query::plan::LogicalPlan;
use crate::query::{QueryEngine, QueryOutput};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::disk_manager::DiskManager;
use crate::types::{DataType, Value};
use std::sync::Arc;

fn setup_engine() -> QueryEngine {
    let unique = format!(
        "entdb-query-hbo-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    );
    let db_path = std::env::temp_dir().join(unique);
    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(64, Arc::clone(&dm)));
    let catalog = Arc::new(Catalog::init(Arc::clone(&bp)).expect("catalog init"));
    QueryEngine::new(catalog)
}

fn sample_plan() -> LogicalPlan {
    let table = TableInfo {
        table_id: 1,
        name: "t".to_string(),
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
    };
    LogicalPlan::SeqScan { table }
}

fn hist(
    sig: &str,
    latency_ms: u64,
    captured_at_ms: u64,
    confidence: f64,
) -> OptimizerHistoryRecord {
    OptimizerHistoryRecord {
        fingerprint: "q".to_string(),
        plan_signature: sig.to_string(),
        schema_hash: "optimizer_history_schema_v1_planner_v1".to_string(),
        captured_at_ms,
        rowcount_observed_json: "{\"root\":1}".to_string(),
        latency_ms,
        memory_peak_bytes: 0,
        success: true,
        error_class: None,
        confidence,
    }
}

#[test]
fn hbo_contradictory_history_is_rejected() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_millis() as u64;
    let history = vec![
        hist("SeqScan(t)", 1, now - 1_000, 1.0),
        hist("SeqScan(t)", 10_000, now - 2_000, 1.0),
        hist("SeqScan(t)", 2, now - 3_000, 1.0),
    ];
    let out = Optimizer::optimize_with_trace_and_history(
        sample_plan(),
        "q",
        OptimizerConfig {
            cbo_enabled: true,
            hbo_enabled: true,
            max_search_ms: 100,
            max_join_relations: 8,
        },
        &history,
    );
    assert!(out.trace.hbo_contradictory);
    assert!(!out.trace.hbo_applied);
}

#[test]
fn hbo_replanning_preserves_query_semantics() {
    let engine = setup_engine();
    engine.set_optimizer_config(OptimizerConfig {
        cbo_enabled: true,
        hbo_enabled: true,
        max_search_ms: 100,
        max_join_relations: 8,
    });
    engine
        .execute("CREATE TABLE t (id INT, v INT)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .expect("insert");

    let a = engine
        .execute("SELECT id FROM t WHERE v >= 20 ORDER BY id")
        .expect("first");
    std::thread::sleep(std::time::Duration::from_millis(50));
    let b = engine
        .execute("SELECT id FROM t WHERE v >= 20 ORDER BY id")
        .expect("second");
    assert_eq!(a, b);
    match &a[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows, &vec![vec![Value::Int32(2)], vec![Value::Int32(3)]]);
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn repeated_query_history_increases_hbo_signal() {
    let engine = setup_engine();
    engine.set_optimizer_config(OptimizerConfig {
        cbo_enabled: true,
        hbo_enabled: true,
        max_search_ms: 100,
        max_join_relations: 8,
    });
    engine
        .execute("CREATE TABLE t (id INT, v INT)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .expect("insert");

    engine
        .execute("SELECT id FROM t WHERE v > 10 ORDER BY id LIMIT 2")
        .expect("run1");
    let t1 = engine.last_optimizer_trace().expect("trace 1");
    std::thread::sleep(std::time::Duration::from_millis(100));

    engine
        .execute("SELECT id FROM t WHERE v > 10 ORDER BY id LIMIT 2")
        .expect("run2");
    let t2 = engine.last_optimizer_trace().expect("trace 2");

    assert!(t2.history_matches >= t1.history_matches);
    assert!(t2.chosen_adjusted_cost.is_some());
    assert!(t2.chosen_base_cost.is_some());
}
