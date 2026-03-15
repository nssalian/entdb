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
use crate::query::{ExecuteOptions, QueryEngine, QueryOutput};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::disk_manager::DiskManager;
use crate::tx::DurabilityMode;
use std::sync::Arc;
fn setup_engine() -> QueryEngine {
    let db_path = std::env::temp_dir().join(format!(
        "entdb-durability-tests-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(256, Arc::clone(&dm)));
    let catalog = Arc::new(Catalog::init(Arc::clone(&bp)).expect("catalog init"));
    QueryEngine::new(catalog)
}

#[test]
fn durability_mode_roundtrip_updates_runtime_config() {
    let engine = setup_engine();
    assert_eq!(engine.durability_mode(), DurabilityMode::Full);

    engine.set_durability_mode(DurabilityMode::Normal);
    assert_eq!(engine.durability_mode(), DurabilityMode::Normal);

    engine.set_durability_mode(DurabilityMode::Off);
    assert_eq!(engine.durability_mode(), DurabilityMode::Off);
}

#[test]
fn execute_with_options_accepts_await_durable_override() {
    let engine = setup_engine();
    engine.set_durability_mode(DurabilityMode::Normal);

    engine
        .execute("CREATE TABLE t (id INT, v INT)")
        .expect("create");
    engine
        .execute_with_options(
            "INSERT INTO t VALUES (1, 10)",
            ExecuteOptions {
                await_durable: Some(true),
            },
        )
        .expect("insert with await_durable");

    let out = engine.execute("SELECT COUNT(*) FROM t").expect("count");
    assert_eq!(out.len(), 1);
    match &out[0] {
        QueryOutput::Rows { rows, .. } => assert_eq!(rows.len(), 1),
        QueryOutput::AffectedRows(_) => panic!("expected rows output"),
    }

    engine.flush_durable().expect("flush durable");
}

#[test]
fn prepared_statement_executes_with_bound_params() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (id INT, name TEXT)")
        .expect("create");

    let ins = engine.prepare("INSERT INTO t VALUES ($1, $2)");
    engine
        .execute_prepared(
            &ins,
            &[
                crate::types::Value::Int64(1),
                crate::types::Value::Text("alice".to_string()),
            ],
        )
        .expect("insert prepared");

    let sel = engine.prepare("SELECT name FROM t WHERE id = $1");
    let out = engine
        .execute_prepared(&sel, &[crate::types::Value::Int64(1)])
        .expect("select prepared");
    assert_eq!(out.len(), 1);
}
