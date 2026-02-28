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

use entdb::types::Value;
use entdb::{EntDb, QueryOutput};
use tempfile::tempdir;

#[test]
fn embedded_vector_distance_round_trip() {
    let dir = tempdir().expect("tempdir");
    let db_root = dir.path().join("embedded_vector");
    let db = EntDb::connect(&db_root).expect("connect");
    db.execute("CREATE TABLE embeddings (id INT, vec VECTOR(3))")
        .expect("create");
    db.execute("INSERT INTO embeddings VALUES (1, '[0,0,0]'), (2, '[1,1,1]')")
        .expect("insert");

    let out = db
        .execute("SELECT id, vec <-> '[0,0,0]' AS dist FROM embeddings ORDER BY id")
        .expect("select");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0], Value::Int32(1));
            assert_eq!(rows[0][1], Value::Float64(0.0));
            assert_eq!(rows[1][0], Value::Int32(2));
        }
        _ => panic!("expected row output"),
    }
}
