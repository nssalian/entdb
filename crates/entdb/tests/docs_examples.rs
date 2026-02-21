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

use entdb::catalog::Catalog;
use entdb::query::QueryEngine;
use entdb::query::QueryOutput;
use entdb::storage::buffer_pool::BufferPool;
use entdb::storage::disk_manager::DiskManager;
use entdb::types::Value;
use std::sync::Arc;
use tempfile::tempdir;

fn setup_engine(db_path: &std::path::Path) -> QueryEngine {
    let dm = Arc::new(DiskManager::new(db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(64, Arc::clone(&dm)));
    let catalog = Arc::new(Catalog::init(Arc::clone(&bp)).expect("catalog init"));
    QueryEngine::new(catalog)
}

#[test]
fn docs_quickstart_example_runs() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("docs-quickstart.db");
    let engine = setup_engine(&db_path);

    engine
        .execute("CREATE TABLE users (id INTEGER, name TEXT)")
        .expect("create table");
    engine
        .execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .expect("insert");

    let result = engine
        .execute("SELECT id, name FROM users ORDER BY id LIMIT 10")
        .expect("select");
    match &result[0] {
        QueryOutput::Rows { columns, rows } => {
            assert_eq!(columns, &vec!["id".to_string(), "name".to_string()]);
            assert_eq!(rows.len(), 2);
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn docs_tx_visibility_example_runs() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("docs-mvcc.db");
    let engine = setup_engine(&db_path);
    engine
        .execute("CREATE TABLE t (id INTEGER, v INTEGER)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 10)")
        .expect("seed");

    let tx1 = engine.begin_txn();
    engine
        .execute_in_txn(&tx1, "UPDATE t SET v = 20 WHERE id = 1")
        .expect("tx1 update");

    let outside_before = engine
        .execute("SELECT v FROM t WHERE id = 1")
        .expect("outside read before commit");
    match &outside_before[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows[0][0], Value::Int32(10));
        }
        _ => panic!("expected rows"),
    }

    engine.commit_txn(tx1).expect("commit");

    let outside_after = engine
        .execute("SELECT v FROM t WHERE id = 1")
        .expect("outside read after commit");
    match &outside_after[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows[0][0], Value::Int32(20));
        }
        _ => panic!("expected rows"),
    }
}
