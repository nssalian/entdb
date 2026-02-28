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
fn embedded_bm25_query_scores_documents() {
    let dir = tempdir().expect("tempdir");
    let db_root = dir.path().join("embedded_bm25");
    let db = EntDb::connect(&db_root).expect("connect");
    db.execute("CREATE TABLE docs (id INT, content TEXT)")
        .expect("create");
    db.execute(
        "INSERT INTO docs VALUES \
         (1, 'database database systems'), \
         (2, 'systems design'), \
         (3, 'database retrieval')",
    )
    .expect("insert");
    db.execute("CREATE INDEX idx_docs_bm25 ON docs USING bm25 (content)")
        .expect("create index");

    let out = db
        .execute(
            "SELECT id, content <@ to_bm25query('database', 'idx_docs_bm25') AS score \
             FROM docs ORDER BY id",
        )
        .expect("query");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            let s1 = match rows[0][1] {
                Value::Float64(v) => v,
                _ => panic!("expected float score"),
            };
            let s2 = match rows[1][1] {
                Value::Float64(v) => v,
                _ => panic!("expected float score"),
            };
            let s3 = match rows[2][1] {
                Value::Float64(v) => v,
                _ => panic!("expected float score"),
            };
            assert!(s1 > s3);
            assert_eq!(s2, 0.0);
            assert!(s3 > 0.0);
        }
        _ => panic!("expected row output"),
    }
}

#[test]
fn embedded_bm25_text_config_changes_tokenization() {
    let dir = tempdir().expect("tempdir");
    let db_root = dir.path().join("embedded_bm25_cfg");
    let db = EntDb::connect(&db_root).expect("connect");
    db.execute("CREATE TABLE docs_en (id INT, content TEXT)")
        .expect("create en");
    db.execute("CREATE TABLE docs_simple (id INT, content TEXT)")
        .expect("create simple");
    db.execute("INSERT INTO docs_en VALUES (1, 'the database')")
        .expect("insert en");
    db.execute("INSERT INTO docs_simple VALUES (1, 'the database')")
        .expect("insert simple");
    db.execute("CREATE INDEX idx_en ON docs_en USING bm25 (content) WITH (text_config='english')")
        .expect("idx en");
    db.execute(
        "CREATE INDEX idx_simple ON docs_simple USING bm25 (content) WITH (text_config='simple')",
    )
    .expect("idx simple");

    let en = db
        .execute(
            "SELECT content <@ to_bm25query('the', 'idx_en') AS score FROM docs_en WHERE id = 1",
        )
        .expect("query en");
    let simple = db
        .execute(
            "SELECT content <@ to_bm25query('the', 'idx_simple') AS score FROM docs_simple WHERE id = 1",
        )
        .expect("query simple");

    let en_score = match &en[0] {
        QueryOutput::Rows { rows, .. } => match rows[0][0] {
            Value::Float64(v) => v,
            _ => panic!("expected float score"),
        },
        _ => panic!("expected rows"),
    };
    let simple_score = match &simple[0] {
        QueryOutput::Rows { rows, .. } => match rows[0][0] {
            Value::Float64(v) => v,
            _ => panic!("expected float score"),
        },
        _ => panic!("expected rows"),
    };

    assert_eq!(en_score, 0.0, "english config should stopword-filter 'the'");
    assert!(simple_score > 0.0, "simple config should keep 'the'");
}
