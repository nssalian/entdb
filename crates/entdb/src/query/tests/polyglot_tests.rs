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
use crate::query::polyglot::{transpile, transpile_with_meta, PolyglotOptions};
use crate::query::{QueryEngine, QueryOutput};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::disk_manager::DiskManager;
use crate::types::Value;
use std::sync::Arc;

fn setup_engine() -> QueryEngine {
    let unique = format!(
        "entdb-query-polyglot-{}-{}.db",
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
fn polyglot_transpile_backticks_and_limit_comma() {
    let sql = "SELECT `id` FROM `users` ORDER BY `id` LIMIT 2, 5";
    let out = transpile(sql, PolyglotOptions { enabled: true }).expect("transpile");
    assert_eq!(
        out,
        "SELECT \"id\" FROM \"users\" ORDER BY \"id\" LIMIT 5 OFFSET 2"
    );
}

#[test]
fn polyglot_transpile_backticks_and_limit_comma_with_semicolon() {
    let sql = "SELECT `id` FROM `users` ORDER BY `id` LIMIT 2, 5;";
    let out = transpile(sql, PolyglotOptions { enabled: true }).expect("transpile");
    assert_eq!(
        out,
        "SELECT \"id\" FROM \"users\" ORDER BY \"id\" LIMIT 5 OFFSET 2;"
    );
}

#[test]
fn polyglot_transpile_keeps_non_numeric_limit_expression_unchanged() {
    let sql = "SELECT `id` FROM `users` LIMIT x, 3";
    let out = transpile(sql, PolyglotOptions { enabled: true }).expect("transpile");
    assert_eq!(out, "SELECT \"id\" FROM \"users\" LIMIT x, 3");
}

#[test]
fn polyglot_rejects_unbalanced_backticks_with_stable_code() {
    let err = transpile("SELECT `id FROM users", PolyglotOptions { enabled: true })
        .expect_err("must reject unbalanced backticks");
    assert!(err.to_string().contains("POLYGLOT_E_UNBALANCED_BACKTICKS"));
}

#[test]
fn polyglot_meta_reports_original_and_transpiled_sql() {
    let out = transpile_with_meta(
        "SELECT `id` FROM `users` LIMIT 1, 2",
        PolyglotOptions { enabled: true },
    )
    .expect("transpile meta");
    assert!(out.changed);
    assert_eq!(out.original_sql, "SELECT `id` FROM `users` LIMIT 1, 2");
    assert_eq!(
        out.transpiled_sql,
        "SELECT \"id\" FROM \"users\" LIMIT 2 OFFSET 1"
    );
}

#[test]
fn polyglot_meta_skips_plain_postgres_sql() {
    let sql = "SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 1";
    let out = transpile_with_meta(sql, PolyglotOptions { enabled: true }).expect("transpile");
    assert_eq!(out.original_sql, sql);
    assert_eq!(out.transpiled_sql, sql);
    assert!(!out.changed);
}

#[test]
fn polyglot_engine_executes_mysql_style_limit_and_backticks() {
    let engine = setup_engine();
    engine.set_polyglot_enabled(true);
    engine
        .execute("CREATE TABLE users (id INT, v INT)")
        .expect("create");
    engine
        .execute("INSERT INTO users VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .expect("insert");

    let out = engine
        .execute("SELECT `id` FROM `users` ORDER BY `id` LIMIT 1, 2")
        .expect("select");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows, &vec![vec![Value::Int32(2)], vec![Value::Int32(3)]]);
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn polyglot_parse_error_includes_original_and_transpiled_sql() {
    let engine = setup_engine();
    engine.set_polyglot_enabled(true);
    let err = engine
        .execute("SELECT `id` FROM `users` LIMIT 1, ")
        .expect_err("expected parse/transpile failure");
    let msg = err.to_string();
    assert!(msg.contains("original_sql=") || msg.contains("POLYGLOT_E_"));
}

#[test]
fn polyglot_dialect_smoke_executes_through_parser() {
    let engine = setup_engine();
    engine.set_polyglot_enabled(true);
    engine
        .execute("CREATE TABLE users (id INT, v INT)")
        .expect("create");
    engine
        .execute("INSERT INTO users VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .expect("insert");

    let mysql = engine
        .execute("SELECT `id` FROM `users` ORDER BY `id` LIMIT 1, 2")
        .expect("mysql-style");
    let postgres = engine
        .execute("SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 1")
        .expect("postgres-style");
    let sqlite_like = engine
        .execute("SELECT id FROM users ORDER BY id LIMIT 2")
        .expect("sqlite-like");

    match &mysql[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows, &vec![vec![Value::Int32(2)], vec![Value::Int32(3)]]);
        }
        _ => panic!("expected rows"),
    }
    match &postgres[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows, &vec![vec![Value::Int32(2)], vec![Value::Int32(3)]]);
        }
        _ => panic!("expected rows"),
    }
    match &sqlite_like[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows, &vec![vec![Value::Int32(1)], vec![Value::Int32(2)]]);
        }
        _ => panic!("expected rows"),
    }
}
