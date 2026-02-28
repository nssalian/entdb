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
use crate::types::Value;
use std::sync::Arc;

fn setup_engine() -> QueryEngine {
    let unique = format!(
        "entdb-query-vector-{}-{}.db",
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
fn vector_l2_distance_query_projects_distance() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE embeddings (id INT, vec VECTOR(3))")
        .expect("create table");
    engine
        .execute(
            "INSERT INTO embeddings VALUES \
             (1, '[0.0,0.0,0.0]'), \
             (2, '[0.5,0.5,0.5]'), \
             (3, '[2.0,2.0,2.0]')",
        )
        .expect("insert");

    let out = engine
        .execute(
            "SELECT id, vec <-> '[0.5,0.5,0.5]' AS dist \
             FROM embeddings WHERE id = 2",
        )
        .expect("select");

    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows[0][0], Value::Int32(2));
            assert_eq!(rows[0][1], Value::Float64(0.0));
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn vector_cosine_distance_query_filters_rows() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE embeddings (id INT, vec VECTOR(3))")
        .expect("create table");
    engine
        .execute(
            "INSERT INTO embeddings VALUES \
             (1, '[1,0,0]'), \
             (2, '[0,1,0]'), \
             (3, '[1,1,0]')",
        )
        .expect("insert");

    let out = engine
        .execute(
            "SELECT id FROM embeddings \
             WHERE vec <=> '[1,0,0]' < 0.3 ORDER BY id ASC",
        )
        .expect("select");

    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows[0][0], Value::Int32(1));
            assert_eq!(rows[1][0], Value::Int32(3));
        }
        _ => panic!("expected rows"),
    }
}
