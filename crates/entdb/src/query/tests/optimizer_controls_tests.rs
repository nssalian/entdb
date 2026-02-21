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
use crate::query::QueryEngine;
use crate::storage::buffer_pool::BufferPool;
use crate::storage::disk_manager::DiskManager;
use std::sync::Arc;

fn setup_engine() -> QueryEngine {
    let unique = format!(
        "entdb-query-controls-{}-{}.db",
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
fn optimizer_admin_controls_update_runtime_config() {
    let engine = setup_engine();
    engine.set_optimizer_config(OptimizerConfig {
        cbo_enabled: true,
        hbo_enabled: true,
        max_search_ms: 100,
        max_join_relations: 8,
    });
    engine.disable_hbo();
    let cfg = engine.optimizer_config();
    assert!(!cfg.hbo_enabled);
    assert!(cfg.cbo_enabled);

    engine.force_baseline_planner();
    let cfg = engine.optimizer_config();
    assert!(!cfg.cbo_enabled);
}

#[test]
fn clear_optimizer_history_resets_persisted_entries() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (id INT, v INT)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .expect("insert");
    let query = "SELECT id FROM t WHERE v > 10 ORDER BY id LIMIT 2";
    engine.execute(query).expect("run query");
    std::thread::sleep(std::time::Duration::from_millis(100));
    let trace = engine.last_optimizer_trace().expect("trace");
    let rows = engine.optimizer_history_for_fingerprint(&trace.fingerprint);
    assert!(!rows.is_empty());

    engine.clear_optimizer_history().expect("clear history");
    let rows = engine.optimizer_history_for_fingerprint(&trace.fingerprint);
    assert!(rows.is_empty());
}
