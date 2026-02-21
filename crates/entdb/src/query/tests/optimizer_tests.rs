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

use crate::catalog::Catalog;
use crate::query::optimizer::OptimizerConfig;
use crate::query::{QueryEngine, QueryOutput};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::disk_manager::DiskManager;
use crate::types::Value;
use std::sync::Arc;

fn setup_engine() -> QueryEngine {
    let unique = format!(
        "entdb-query-optimizer-{}-{}.db",
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

#[test]
fn explain_optimizer_trace_returns_stage_pipeline() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (id INT, v INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .expect("insert");

    let traces = engine
        .explain_optimizer_trace("SELECT id FROM t WHERE v > 10 ORDER BY id LIMIT 1")
        .expect("explain optimizer trace");
    assert_eq!(traces.len(), 1);
    let trace = &traces[0];
    assert!(!trace.fingerprint.is_empty());
    assert!(trace.fallback_reason.is_none());
    let stages: Vec<&str> = trace.stages.iter().map(|s| s.stage.as_str()).collect();
    assert_eq!(
        stages,
        vec![
            "logical_rewrite",
            "estimate",
            "enumerate",
            "choose",
            "finalize"
        ]
    );
}

#[test]
fn optimizer_timeout_budget_zero_falls_back_without_result_regression() {
    let engine = setup_engine();
    engine.set_optimizer_config(OptimizerConfig {
        cbo_enabled: true,
        hbo_enabled: false,
        max_search_ms: 0,
        max_join_relations: 8,
    });
    engine
        .execute("CREATE TABLE t (id INT, v INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .expect("insert rows");

    let out = engine
        .execute("SELECT id FROM t WHERE v >= 20 ORDER BY id")
        .expect("select rows");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows, &vec![vec![Value::Int32(2)], vec![Value::Int32(3)]]);
        }
        _ => panic!("expected rows"),
    }

    let cfg = engine.optimizer_config();
    assert_eq!(cfg.max_search_ms, 1);
    let trace = engine
        .last_optimizer_trace()
        .expect("trace should be captured");
    assert!(trace.fallback_reason.is_none());
}

#[test]
fn optimizer_join_relation_cap_falls_back_deterministically() {
    let engine = setup_engine();
    engine.set_optimizer_config(OptimizerConfig {
        cbo_enabled: true,
        hbo_enabled: false,
        max_search_ms: 100,
        max_join_relations: 1,
    });
    engine.execute("CREATE TABLE a (id INT)").expect("create a");
    engine.execute("CREATE TABLE b (id INT)").expect("create b");
    engine.execute("CREATE TABLE c (id INT)").expect("create c");
    engine
        .execute("INSERT INTO a VALUES (1), (2), (3)")
        .expect("insert a");
    engine
        .execute("INSERT INTO b VALUES (2), (3), (4)")
        .expect("insert b");
    engine
        .execute("INSERT INTO c VALUES (3), (4), (5)")
        .expect("insert c");

    let out = engine
        .execute(
            "SELECT a.id FROM a \
             INNER JOIN b ON a.id = b.id \
             INNER JOIN c ON b.id = c.id",
        )
        .expect("join should execute via fallback");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows, &vec![vec![Value::Int32(3)]]);
        }
        _ => panic!("expected rows"),
    }

    let trace = engine
        .last_optimizer_trace()
        .expect("trace should be captured");
    let reason = trace.fallback_reason.expect("expected fallback reason");
    assert!(reason.contains("join graph size"));
    assert!(reason.contains("max_join_relations"));
}

#[test]
fn optimizer_choose_is_deterministic_for_same_query() {
    let engine = setup_engine();
    engine.set_optimizer_config(OptimizerConfig {
        cbo_enabled: true,
        hbo_enabled: false,
        max_search_ms: 100,
        max_join_relations: 8,
    });
    engine.execute("CREATE TABLE a (id INT)").expect("create a");
    engine.execute("CREATE TABLE b (id INT)").expect("create b");
    engine.execute("CREATE TABLE c (id INT)").expect("create c");
    engine
        .execute("INSERT INTO a VALUES (1), (2), (3)")
        .expect("insert a");
    engine
        .execute("INSERT INTO b VALUES (2), (3), (4)")
        .expect("insert b");
    engine
        .execute("INSERT INTO c VALUES (3), (4), (5)")
        .expect("insert c");

    for _ in 0..2 {
        let out = engine
            .execute(
                "SELECT a.id FROM a \
                 INNER JOIN b ON a.id = b.id \
                 INNER JOIN c ON b.id = c.id \
                 ORDER BY a.id",
            )
            .expect("join chain");
        match &out[0] {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows, &vec![vec![Value::Int32(3)]]);
            }
            _ => panic!("expected rows"),
        }
    }

    let t1 = engine
        .explain_optimizer_trace(
            "SELECT a.id FROM a INNER JOIN b ON a.id = b.id INNER JOIN c ON b.id = c.id",
        )
        .expect("trace 1");
    let t2 = engine
        .explain_optimizer_trace(
            "SELECT a.id FROM a INNER JOIN b ON a.id = b.id INNER JOIN c ON b.id = c.id",
        )
        .expect("trace 2");
    assert_eq!(t1.len(), 1);
    assert_eq!(t2.len(), 1);
    assert_eq!(t1[0].chosen_plan_signature, t2[0].chosen_plan_signature);
}

#[test]
fn optimizer_enumeration_is_bounded_and_reports_candidate_count() {
    let engine = setup_engine();
    engine.set_optimizer_config(OptimizerConfig {
        cbo_enabled: true,
        hbo_enabled: false,
        max_search_ms: 100,
        max_join_relations: 8,
    });
    engine.execute("CREATE TABLE a (id INT)").expect("create a");
    engine.execute("CREATE TABLE b (id INT)").expect("create b");
    engine.execute("CREATE TABLE c (id INT)").expect("create c");
    engine.execute("CREATE TABLE d (id INT)").expect("create d");
    engine
        .execute("INSERT INTO a VALUES (1)")
        .expect("insert a");
    engine
        .execute("INSERT INTO b VALUES (1)")
        .expect("insert b");
    engine
        .execute("INSERT INTO c VALUES (1)")
        .expect("insert c");
    engine
        .execute("INSERT INTO d VALUES (1)")
        .expect("insert d");

    let traces = engine
        .explain_optimizer_trace(
            "SELECT a.id FROM a \
             INNER JOIN b ON a.id = b.id \
             INNER JOIN c ON b.id = c.id \
             INNER JOIN d ON c.id = d.id",
        )
        .expect("trace");
    let trace = &traces[0];
    assert!(trace.fallback_reason.is_none());
    assert!(trace.enumerated_candidates >= 1);
    assert!(trace.enumerated_candidates <= 32);
    assert!(trace.chosen_plan_signature.is_some());
}

#[test]
fn optimizer_config_is_sanitized_to_safe_bounds() {
    let engine = setup_engine();
    engine.set_optimizer_config(OptimizerConfig {
        cbo_enabled: true,
        hbo_enabled: true,
        max_search_ms: 0,
        max_join_relations: 0,
    });
    let cfg = engine.optimizer_config();
    assert_eq!(cfg.max_search_ms, 1);
    assert_eq!(cfg.max_join_relations, 1);
}
