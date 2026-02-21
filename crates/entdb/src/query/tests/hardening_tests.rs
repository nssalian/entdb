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
use crate::query::QueryEngine;
use crate::storage::buffer_pool::BufferPool;
use crate::storage::disk_manager::DiskManager;
use std::sync::Arc;

fn setup_engine() -> QueryEngine {
    let unique = format!(
        "entdb-query-hardening-{}-{}.db",
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
fn malformed_sql_returns_parse_error() {
    let engine = setup_engine();
    let err = engine
        .execute("SELECT FROM")
        .expect_err("malformed SQL should fail");
    assert!(err.to_string().contains("parse error"));
}

#[test]
fn select_multiple_from_relations_not_supported() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE a (id INT)")
        .expect("create table a");
    engine
        .execute("CREATE TABLE b (id INT)")
        .expect("create table b");

    let err = engine
        .execute("SELECT * FROM a, b")
        .expect_err("multiple FROM relations should fail");
    assert!(err.to_string().contains("one FROM relation"));
}

#[test]
fn update_from_requires_existing_join_columns() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE users (id INT, age INT)")
        .expect("create table");
    engine
        .execute("CREATE TABLE src (id INT, age INT)")
        .expect("create src");

    let err = engine
        .execute("UPDATE users SET age = src.missing FROM src WHERE users.id = src.id")
        .expect_err("UPDATE FROM should fail");
    assert!(err.to_string().contains("unknown column reference"));
}

#[test]
fn delete_using_requires_valid_predicate_columns() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE users (id INT)")
        .expect("create users");
    engine
        .execute("CREATE TABLE shadow (id INT)")
        .expect("create shadow");

    let err = engine
        .execute("DELETE FROM users USING shadow WHERE users.id = shadow.missing")
        .expect_err("DELETE USING should fail");
    assert!(err.to_string().contains("unknown column reference"));
}

#[test]
fn join_on_must_compare_across_tables() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE users (id INT)")
        .expect("create users");
    engine
        .execute("CREATE TABLE orders (user_id INT)")
        .expect("create orders");

    let err = engine
        .execute("SELECT * FROM users INNER JOIN orders ON users.id = users.id")
        .expect_err("invalid ON clause should fail");
    assert!(err
        .to_string()
        .contains("must compare one column from each table"));
}

#[test]
fn count_non_star_supported() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (id INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO t VALUES (1), (2), (3)")
        .expect("insert rows");

    let out = engine
        .execute("SELECT COUNT(id) AS c FROM t")
        .expect("count");
    match &out[0] {
        crate::query::QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows, &vec![vec![crate::types::Value::Int64(3)]]);
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn group_by_must_match_projected_group_column() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (a INT, b INT)")
        .expect("create table");

    let err = engine
        .execute("SELECT a, SUM(b) FROM t GROUP BY b")
        .expect_err("group by mismatch should fail");
    assert!(err
        .to_string()
        .contains("GROUP BY column must match projected grouping column"));
}

#[test]
fn sum_and_avg_require_numeric_column() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (name TEXT)")
        .expect("create table");

    let sum_err = engine
        .execute("SELECT SUM(name) FROM t")
        .expect_err("sum text should fail");
    assert!(sum_err.to_string().contains("SUM/AVG require numeric"));

    let avg_err = engine
        .execute("SELECT AVG(name) FROM t")
        .expect_err("avg text should fail");
    assert!(avg_err.to_string().contains("SUM/AVG require numeric"));
}
