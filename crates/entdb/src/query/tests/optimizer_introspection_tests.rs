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
use crate::query::{QueryEngine, QueryOutput};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::disk_manager::DiskManager;
use std::sync::Arc;

fn setup_engine() -> QueryEngine {
    let unique = format!(
        "entdb-query-introspection-{}-{}.db",
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
fn explain_optimizer_output_shape_and_trace_json_parseable() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (id INT, v INT)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .expect("insert");

    let out = engine
        .explain_optimizer("SELECT id FROM t WHERE v > 10 ORDER BY id LIMIT 2")
        .expect("explain optimizer");
    match &out[0] {
        QueryOutput::Rows { columns, rows } => {
            assert_eq!(
                columns,
                &vec![
                    "fingerprint".to_string(),
                    "plan_signature".to_string(),
                    "estimated_rows".to_string(),
                    "estimated_cost".to_string(),
                    "trace_json".to_string()
                ]
            );
            assert_eq!(rows.len(), 1);
            let trace_json = match &rows[0][4] {
                crate::types::Value::Text(s) => s.clone(),
                _ => panic!("trace json should be text"),
            };
            let v: serde_json::Value =
                serde_json::from_str(&trace_json).expect("trace json parseable");
            assert!(v.get("stages").is_some());
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn explain_optimizer_analyze_returns_observed_and_estimated_fields() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (id INT, v INT)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .expect("insert");

    let out = engine
        .explain_optimizer_analyze("SELECT id FROM t WHERE v >= 20 ORDER BY id")
        .expect("explain analyze");
    match &out[0] {
        QueryOutput::Rows { columns, rows } => {
            assert_eq!(
                columns,
                &vec![
                    "fingerprint".to_string(),
                    "observed_rows".to_string(),
                    "elapsed_ms".to_string(),
                    "estimated_base_cost".to_string(),
                    "estimated_adjusted_cost".to_string(),
                    "history_matches".to_string(),
                ]
            );
            assert_eq!(rows.len(), 1);
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn optimizer_history_metrics_output_is_stable() {
    let engine = setup_engine();
    let out = engine.optimizer_history_metrics();
    match out {
        QueryOutput::Rows { columns, rows } => {
            assert_eq!(
                columns,
                vec![
                    "history_dropped".to_string(),
                    "history_worker_errors".to_string()
                ]
            );
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 2);
        }
        _ => panic!("expected rows"),
    }
}
