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

use crate::catalog::{Catalog, IndexType};
use crate::query::{QueryEngine, QueryOutput};
use crate::storage::bm25::Bm25Index;
use crate::storage::buffer_pool::BufferPool;
use crate::storage::disk_manager::DiskManager;
use crate::types::Value;
use std::sync::Arc;

fn setup_engine() -> QueryEngine {
    let unique = format!(
        "entdb-query-{}-{}.db",
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

fn setup_engine_with_catalog() -> (QueryEngine, Arc<Catalog>) {
    let unique = format!(
        "entdb-query-with-catalog-{}-{}.db",
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
    (QueryEngine::new(Arc::clone(&catalog)), catalog)
}

#[test]
fn create_insert_select_where_limit() {
    let engine = setup_engine();

    engine
        .execute("CREATE TABLE users (id INT, name TEXT, age INT)")
        .expect("create table");

    engine
        .execute("INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35)")
        .expect("insert rows");

    let out = engine
        .execute("SELECT id, name FROM users WHERE age > 27 LIMIT 2")
        .expect("select rows");

    assert_eq!(out.len(), 1);
    match &out[0] {
        QueryOutput::Rows { columns, rows } => {
            assert_eq!(columns, &vec!["id".to_string(), "name".to_string()]);
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[0],
                vec![Value::Int32(1), Value::Text("alice".to_string())]
            );
            assert_eq!(
                rows[1],
                vec![Value::Int32(3), Value::Text("carol".to_string())]
            );
        }
        _ => panic!("expected row output"),
    }
}

#[test]
fn select_star_and_unknown_column_error() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (a INT, b INT)")
        .expect("create t");
    engine
        .execute("INSERT INTO t VALUES (1, 2)")
        .expect("insert");

    let out = engine.execute("SELECT * FROM t").expect("select *");
    match &out[0] {
        QueryOutput::Rows { columns, rows } => {
            assert_eq!(columns, &vec!["a".to_string(), "b".to_string()]);
            assert_eq!(rows.len(), 1);
        }
        _ => panic!("expected rows"),
    }

    let err = engine
        .execute("SELECT z FROM t")
        .expect_err("expected unknown column error");
    assert!(err.to_string().contains("column 'z'"));
}

#[test]
fn select_literal_without_from_returns_single_row() {
    let engine = setup_engine();
    let out = engine
        .execute("SELECT 1 AS one, 'ent' AS name")
        .expect("select literal");
    match &out[0] {
        QueryOutput::Rows { columns, rows } => {
            assert_eq!(columns, &vec!["one".to_string(), "name".to_string()]);
            assert_eq!(
                rows,
                &vec![vec![Value::Int64(1), Value::Text("ent".to_string())]]
            );
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn unknown_table_error() {
    let engine = setup_engine();
    let err = engine
        .execute("SELECT * FROM nope")
        .expect_err("expected unknown table error");
    assert!(err.to_string().contains("table 'nope'"));
}

#[test]
fn order_by_and_count_star() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE users (id INT, age INT)")
        .expect("create users");
    engine
        .execute("INSERT INTO users VALUES (1, 30), (2, 10), (3, 20)")
        .expect("insert users");

    let sorted = engine
        .execute("SELECT id FROM users ORDER BY age ASC")
        .expect("select ordered");
    match &sorted[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0], vec![Value::Int32(2)]);
            assert_eq!(rows[1], vec![Value::Int32(3)]);
            assert_eq!(rows[2], vec![Value::Int32(1)]);
        }
        _ => panic!("expected rows"),
    }

    let sorted_desc = engine
        .execute("SELECT id FROM users ORDER BY age DESC")
        .expect("select ordered desc");
    match &sorted_desc[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows[0], vec![Value::Int32(1)]);
            assert_eq!(rows[1], vec![Value::Int32(3)]);
            assert_eq!(rows[2], vec![Value::Int32(2)]);
        }
        _ => panic!("expected rows"),
    }

    let count = engine
        .execute("SELECT COUNT(*) FROM users WHERE age >= 20")
        .expect("count");
    match &count[0] {
        QueryOutput::Rows { columns, rows } => {
            assert_eq!(columns, &vec!["count".to_string()]);
            assert_eq!(rows, &vec![vec![Value::Int64(2)]]);
        }
        _ => panic!("expected rows"),
    }

    engine
        .execute("CREATE TABLE names (n TEXT)")
        .expect("create names");
    engine
        .execute("INSERT INTO names VALUES ('carol'), ('alice'), ('bob')")
        .expect("insert names");
    let text_sorted = engine
        .execute("SELECT n FROM names ORDER BY n ASC")
        .expect("text order");
    match &text_sorted[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows[0], vec![Value::Text("alice".to_string())]);
            assert_eq!(rows[1], vec![Value::Text("bob".to_string())]);
            assert_eq!(rows[2], vec![Value::Text("carol".to_string())]);
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn analyze_populates_stats_and_dml_marks_stale() {
    let (engine, catalog) = setup_engine_with_catalog();
    engine
        .execute("CREATE TABLE stats_t (id INT, v INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO stats_t VALUES (1, 10), (2, 20)")
        .expect("insert rows");

    let analyze = engine.execute("ANALYZE stats_t").expect("analyze table");
    assert_eq!(analyze, vec![QueryOutput::AffectedRows(1)]);

    let after_analyze = catalog.get_table("stats_t").expect("stats_t exists");
    assert_eq!(after_analyze.stats.row_count, 2);
    assert!(!after_analyze.stats.stale);
    assert_eq!(after_analyze.stats.mutation_count_since_analyze, 0);
    assert!(after_analyze.stats.last_analyzed_at_ms.is_some());
    assert_eq!(after_analyze.stats.columns.len(), 2);
    assert_eq!(after_analyze.stats.columns[0].name, "id");
    assert_eq!(after_analyze.stats.columns[0].ndv, Some(2));

    engine
        .execute("INSERT INTO stats_t VALUES (3, 30)")
        .expect("insert one");
    let after_insert = catalog.get_table("stats_t").expect("stats_t exists");
    assert!(after_insert.stats.stale);
    assert!(after_insert.stats.mutation_count_since_analyze >= 1);
}

#[test]
fn update_and_delete_affect_rows_and_data() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE users (id INT, age INT)")
        .expect("create users");
    engine
        .execute("INSERT INTO users VALUES (1, 10), (2, 20), (3, 30)")
        .expect("insert users");

    let update = engine
        .execute("UPDATE users SET age = age + 5 WHERE id = 2")
        .expect("update");
    assert_eq!(update, vec![QueryOutput::AffectedRows(1)]);

    let delete = engine
        .execute("DELETE FROM users WHERE id = 1")
        .expect("delete");
    assert_eq!(delete, vec![QueryOutput::AffectedRows(1)]);

    let out = engine
        .execute("SELECT id, age FROM users ORDER BY id")
        .expect("verify rows");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0], vec![Value::Int32(2), Value::Int32(25)]);
            assert_eq!(rows[1], vec![Value::Int32(3), Value::Int32(30)]);
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn inner_join_select_basic_path() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE users (id INT, name TEXT)")
        .expect("create users");
    engine
        .execute("CREATE TABLE orders (id INT, user_id INT)")
        .expect("create orders");
    engine
        .execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .expect("insert users");
    engine
        .execute("INSERT INTO orders VALUES (10, 1), (11, 1), (12, 2)")
        .expect("insert orders");

    let out = engine
        .execute(
            "SELECT users.name, orders.id FROM users INNER JOIN orders ON users.id = orders.user_id ORDER BY orders.id",
        )
        .expect("join");

    match &out[0] {
        QueryOutput::Rows { columns, rows } => {
            assert_eq!(columns, &vec!["name".to_string(), "id".to_string()]);
            assert_eq!(rows.len(), 3);
            assert_eq!(
                rows[0],
                vec![Value::Text("alice".to_string()), Value::Int32(10)]
            );
            assert_eq!(
                rows[2],
                vec![Value::Text("bob".to_string()), Value::Int32(12)]
            );
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn where_logical_and_or_and_arithmetic_filter() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE users (id INT, age INT)")
        .expect("create users");
    engine
        .execute("INSERT INTO users VALUES (1, 18), (2, 25), (3, 40), (4, 41)")
        .expect("insert users");

    let out = engine
        .execute(
            "SELECT id FROM users WHERE (age - 1) >= 39 OR (id = 2 AND age > 20) ORDER BY id ASC",
        )
        .expect("where with logical and arithmetic");

    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0], vec![Value::Int32(2)]);
            assert_eq!(rows[1], vec![Value::Int32(3)]);
            assert_eq!(rows[2], vec![Value::Int32(4)]);
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn join_ambiguous_column_reference_errors() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE users (id INT, age INT)")
        .expect("create users");
    engine
        .execute("CREATE TABLE orders (id INT, user_id INT)")
        .expect("create orders");

    let err = engine
        .execute("SELECT id FROM users INNER JOIN orders ON users.id = orders.user_id")
        .expect_err("ambiguous column should fail");
    assert!(err.to_string().contains("ambiguous"));
}

#[test]
fn insert_unary_minus_literal_support() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE metrics (id INT, delta INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO metrics VALUES (1, -7), (2, +3)")
        .expect("insert signed literals");

    let out = engine
        .execute("SELECT delta FROM metrics ORDER BY id")
        .expect("select rows");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0], vec![Value::Int32(-7)]);
            assert_eq!(rows[1], vec![Value::Int32(3)]);
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn where_multiplication_and_division_filter() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (id INT, a INT, b INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO t VALUES (1, 10, 2), (2, 8, 4), (3, 6, 3)")
        .expect("insert rows");

    let out = engine
        .execute("SELECT id FROM t WHERE (a / b) = 2 OR (a * b) = 20 ORDER BY id")
        .expect("select arithmetic");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(
                rows,
                &vec![
                    vec![Value::Int32(1)],
                    vec![Value::Int32(2)],
                    vec![Value::Int32(3)]
                ]
            );
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn group_by_count_star_single_column() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE events (kind TEXT, v INT)")
        .expect("create events");
    engine
        .execute(
            "INSERT INTO events VALUES ('click', 1), ('click', 2), ('view', 10), ('view', 11), ('view', 12)",
        )
        .expect("insert events");

    let out = engine
        .execute("SELECT kind, COUNT(*) AS cnt FROM events GROUP BY kind ORDER BY kind")
        .expect("group count");

    match &out[0] {
        QueryOutput::Rows { columns, rows } => {
            assert_eq!(columns, &vec!["kind".to_string(), "cnt".to_string()]);
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[0],
                vec![Value::Text("click".to_string()), Value::Int64(2)]
            );
            assert_eq!(
                rows[1],
                vec![Value::Text("view".to_string()), Value::Int64(3)]
            );
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn group_by_requires_supported_projection_shape() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE events (kind TEXT, v INT)")
        .expect("create events");
    engine
        .execute("INSERT INTO events VALUES ('click', 1), ('view', 2)")
        .expect("insert");

    let err = engine
        .execute("SELECT kind FROM events GROUP BY kind")
        .expect_err("group by without count should error");
    assert!(err
        .to_string()
        .contains("GROUP BY currently requires aggregate projection"));
}

#[test]
fn aggregates_sum_avg_min_max_without_group_by() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE m (v INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO m VALUES (1), (2), (7)")
        .expect("insert rows");

    let sum = engine.execute("SELECT SUM(v) AS s FROM m").expect("sum");
    let avg = engine.execute("SELECT AVG(v) AS a FROM m").expect("avg");
    let min = engine.execute("SELECT MIN(v) AS mn FROM m").expect("min");
    let max = engine.execute("SELECT MAX(v) AS mx FROM m").expect("max");

    match &sum[0] {
        QueryOutput::Rows { rows, .. } => assert_eq!(rows, &vec![vec![Value::Int64(10)]]),
        _ => panic!("expected rows"),
    }
    match &avg[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 1);
            let Value::Float64(v) = rows[0][0] else {
                panic!("expected float avg");
            };
            assert!((v - (10.0 / 3.0)).abs() < 1e-9);
        }
        _ => panic!("expected rows"),
    }
    match &min[0] {
        QueryOutput::Rows { rows, .. } => assert_eq!(rows, &vec![vec![Value::Int32(1)]]),
        _ => panic!("expected rows"),
    }
    match &max[0] {
        QueryOutput::Rows { rows, .. } => assert_eq!(rows, &vec![vec![Value::Int32(7)]]),
        _ => panic!("expected rows"),
    }
}

#[test]
fn grouped_sum_and_max_work() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE events (kind TEXT, v INT)")
        .expect("create table");
    engine
        .execute(
            "INSERT INTO events VALUES ('click', 1), ('click', 2), ('view', 10), ('view', 11), ('view', 12)",
        )
        .expect("insert rows");

    let out = engine
        .execute("SELECT kind, SUM(v) AS total FROM events GROUP BY kind ORDER BY kind")
        .expect("group sum");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(
                rows,
                &vec![
                    vec![Value::Text("click".to_string()), Value::Int64(3)],
                    vec![Value::Text("view".to_string()), Value::Int64(33)],
                ]
            );
        }
        _ => panic!("expected rows"),
    }

    let out = engine
        .execute("SELECT kind, MAX(v) AS mx FROM events GROUP BY kind ORDER BY kind")
        .expect("group max");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(
                rows,
                &vec![
                    vec![Value::Text("click".to_string()), Value::Int32(2)],
                    vec![Value::Text("view".to_string()), Value::Int32(12)],
                ]
            );
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn grouped_multi_aggregate_projection_works() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE events (kind TEXT, v INT)")
        .expect("create table");
    engine
        .execute(
            "INSERT INTO events VALUES ('click', 1), ('click', 2), ('view', 10), ('view', 11), ('view', 12)",
        )
        .expect("insert rows");

    let out = engine
        .execute(
            "SELECT kind, COUNT(*) AS cnt, SUM(v) AS total, AVG(v) AS avg_v, MIN(v) AS min_v, MAX(v) AS max_v FROM events GROUP BY kind ORDER BY kind",
        )
        .expect("grouped multi-aggregate");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[0],
                vec![
                    Value::Text("click".to_string()),
                    Value::Int64(2),
                    Value::Int64(3),
                    Value::Float64(1.5),
                    Value::Int32(1),
                    Value::Int32(2),
                ]
            );
            assert_eq!(
                rows[1],
                vec![
                    Value::Text("view".to_string()),
                    Value::Int64(3),
                    Value::Int64(33),
                    Value::Float64(11.0),
                    Value::Int32(10),
                    Value::Int32(12),
                ]
            );
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn non_grouped_multi_aggregate_projection_works() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE m (v INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO m VALUES (1), (2), (7)")
        .expect("insert");
    let out = engine
        .execute("SELECT AVG(v) AS avg_v, MIN(v) AS min_v FROM m")
        .expect("non-grouped multi-aggregate");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 2);
            let Value::Float64(avg) = rows[0][0] else {
                panic!("expected float avg");
            };
            assert!((avg - (10.0 / 3.0)).abs() < 1e-9);
            assert_eq!(rows[0][1], Value::Int32(1));
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn insert_timestamp_text_literal_is_coerced() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE ts_t (id INT, ts TIMESTAMP)")
        .expect("create table");
    engine
        .execute("INSERT INTO ts_t VALUES (1, '2024-01-01 00:00:00')")
        .expect("insert timestamp");
    let out = engine
        .execute("SELECT id, ts FROM ts_t")
        .expect("select timestamp");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Int32(1));
            let Value::Timestamp(ts) = rows[0][1] else {
                panic!("expected timestamp");
            };
            assert_eq!(ts, 1_704_067_200);
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn drop_table_removes_table_and_if_exists_is_safe() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE doomed (id INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO doomed VALUES (1)")
        .expect("insert row");

    let out = engine.execute("DROP TABLE doomed").expect("drop table");
    assert_eq!(out, vec![QueryOutput::AffectedRows(1)]);

    let err = engine
        .execute("SELECT * FROM doomed")
        .expect_err("dropped table should not resolve");
    assert!(err.to_string().contains("table 'doomed'"));

    let out = engine
        .execute("DROP TABLE IF EXISTS doomed")
        .expect("drop if exists");
    assert_eq!(out, vec![QueryOutput::AffectedRows(0)]);
}

#[test]
fn drop_table_without_if_exists_errors_for_missing_table() {
    let engine = setup_engine();
    let err = engine
        .execute("DROP TABLE missing_table")
        .expect_err("drop without IF EXISTS should fail");
    assert!(err
        .to_string()
        .contains("table 'missing_table' does not exist"));
}

#[test]
fn insert_select_copies_rows() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE src (id INT, v INT)")
        .expect("create src");
    engine
        .execute("CREATE TABLE dst (id INT, v INT)")
        .expect("create dst");
    engine
        .execute("INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)")
        .expect("insert src");

    let out = engine
        .execute("INSERT INTO dst SELECT id, v FROM src WHERE v >= 20")
        .expect("insert select");
    assert_eq!(out, vec![QueryOutput::AffectedRows(2)]);

    let out = engine
        .execute("SELECT id, v FROM dst ORDER BY id")
        .expect("select dst");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0], vec![Value::Int32(2), Value::Int32(20)]);
            assert_eq!(rows[1], vec![Value::Int32(3), Value::Int32(30)]);
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn create_and_drop_index_sql_path() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE users (id INT, email TEXT)")
        .expect("create table");

    let out = engine
        .execute("CREATE INDEX idx_users_id ON users (id)")
        .expect("create index");
    assert_eq!(out, vec![QueryOutput::AffectedRows(1)]);

    let out = engine
        .execute("DROP INDEX idx_users_id")
        .expect("drop index");
    assert_eq!(out, vec![QueryOutput::AffectedRows(1)]);

    let out = engine
        .execute("DROP INDEX IF EXISTS idx_users_id")
        .expect("drop index if exists");
    assert_eq!(out, vec![QueryOutput::AffectedRows(0)]);
}

#[test]
fn create_bm25_index_sql_path_persists_metadata() {
    let (engine, catalog) = setup_engine_with_catalog();
    engine
        .execute("CREATE TABLE docs (id INT, content TEXT)")
        .expect("create table");

    let out = engine
        .execute(
            "CREATE INDEX idx_docs_bm25 ON docs USING bm25 (content) WITH (text_config='english')",
        )
        .expect("create bm25 index");
    assert_eq!(out, vec![QueryOutput::AffectedRows(1)]);

    let table = catalog.get_table("docs").expect("table exists");
    assert_eq!(table.indexes.len(), 1);
    assert_eq!(table.indexes[0].name, "idx_docs_bm25");
    assert_eq!(
        table.indexes[0].index_type,
        IndexType::Bm25 {
            text_config: "english".to_string()
        }
    );
}

#[test]
fn bm25_function_and_operator_score_rows() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE docs (id INT, content TEXT)")
        .expect("create table");
    engine
        .execute(
            "INSERT INTO docs VALUES \
             (1, 'database database systems'), \
             (2, 'systems design'), \
             (3, 'database indexing retrieval')",
        )
        .expect("insert");
    engine
        .execute("CREATE INDEX idx_docs_bm25 ON docs USING bm25 (content)")
        .expect("create bm25 index");

    let out = engine
        .execute(
            "SELECT id, content <@ to_bm25query('database', 'idx_docs_bm25') \
             FROM docs ORDER BY id ASC",
        )
        .expect("score query");

    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 3);
            let s1 = match rows[0][1] {
                Value::Float64(v) => v,
                _ => panic!("expected float score for row 1"),
            };
            let s2 = match rows[1][1] {
                Value::Float64(v) => v,
                _ => panic!("expected float score for row 2"),
            };
            let s3 = match rows[2][1] {
                Value::Float64(v) => v,
                _ => panic!("expected float score for row 3"),
            };
            assert!(s1 > s3);
            assert_eq!(s2, 0.0);
            assert!(s3 > 0.0);
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn bm25_sidecar_builds_on_create_index_and_tracks_dml() {
    let (engine, catalog) = setup_engine_with_catalog();
    engine
        .execute("CREATE TABLE docs (id INT, content TEXT)")
        .expect("create table");
    engine
        .execute("INSERT INTO docs VALUES (1, 'database systems'), (2, 'search indexing')")
        .expect("seed rows");

    engine
        .execute("CREATE INDEX idx_docs_bm25_track ON docs USING bm25 (content)")
        .expect("create bm25 index");

    let db_path = catalog.buffer_pool().disk_path().to_path_buf();
    let mut bm = Bm25Index::load(&db_path, "idx_docs_bm25_track").expect("load bm25 sidecar");
    assert_eq!(bm.document_count(), 2);
    assert_eq!(bm.document_frequency("database"), 1);

    engine
        .execute("INSERT INTO docs VALUES (3, 'database retrieval')")
        .expect("insert doc");
    bm = Bm25Index::load(&db_path, "idx_docs_bm25_track").expect("reload sidecar");
    assert_eq!(bm.document_count(), 3);
    assert_eq!(bm.document_frequency("database"), 2);

    engine
        .execute("UPDATE docs SET content = 'updated corpus' WHERE id = 1")
        .expect("update doc");
    bm = Bm25Index::load(&db_path, "idx_docs_bm25_track").expect("reload sidecar");
    assert_eq!(bm.document_count(), 3);
    assert_eq!(bm.document_frequency("database"), 1);

    engine
        .execute("DELETE FROM docs WHERE id = 3")
        .expect("delete doc");
    bm = Bm25Index::load(&db_path, "idx_docs_bm25_track").expect("reload sidecar");
    assert_eq!(bm.document_count(), 2);
    assert_eq!(bm.document_frequency("database"), 0);
}

#[test]
fn bm25_query_uses_bm25_scan_plan_signature_when_index_matches() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE docs (id INT, content TEXT)")
        .expect("create table");
    engine
        .execute("INSERT INTO docs VALUES (1, 'database systems')")
        .expect("insert");
    engine
        .execute("CREATE INDEX idx_docs_bm25 ON docs USING bm25 (content)")
        .expect("create index");

    let traces = engine
        .explain_optimizer_trace(
            "SELECT id, content <@ to_bm25query('database', 'idx_docs_bm25') FROM docs",
        )
        .expect("trace");
    let chosen = traces[0]
        .chosen_plan_signature
        .clone()
        .expect("chosen signature");
    assert!(chosen.contains("Bm25Scan("), "signature was: {chosen}");
}

#[test]
fn bm25_query_falls_back_when_index_name_is_unknown() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE docs (id INT, content TEXT)")
        .expect("create table");
    engine
        .execute("INSERT INTO docs VALUES (1, 'database systems')")
        .expect("insert");
    engine
        .execute("CREATE INDEX idx_docs_bm25 ON docs USING bm25 (content)")
        .expect("create index");

    let traces = engine
        .explain_optimizer_trace(
            "SELECT id, content <@ to_bm25query('database', 'missing_idx') FROM docs",
        )
        .expect("trace");
    let chosen = traces[0]
        .chosen_plan_signature
        .clone()
        .expect("chosen signature");
    assert!(chosen.contains("SeqScan("), "signature was: {chosen}");
}

#[test]
fn alter_table_add_drop_rename_column_and_rename_table() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (id INT, name TEXT)")
        .expect("create table");
    engine
        .execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
        .expect("insert");

    engine
        .execute("ALTER TABLE t ADD COLUMN age INT")
        .expect("add column");
    let out = engine
        .execute("SELECT id, name, age FROM t ORDER BY id")
        .expect("select with added column");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(
                rows,
                &vec![
                    vec![
                        Value::Int32(1),
                        Value::Text("alice".to_string()),
                        Value::Null
                    ],
                    vec![Value::Int32(2), Value::Text("bob".to_string()), Value::Null],
                ]
            );
        }
        _ => panic!("expected rows"),
    }

    engine
        .execute("ALTER TABLE t RENAME COLUMN name TO full_name")
        .expect("rename column");
    let out = engine
        .execute("SELECT id, full_name FROM t ORDER BY id")
        .expect("select renamed column");
    match &out[0] {
        QueryOutput::Rows { columns, .. } => {
            assert_eq!(columns, &vec!["id".to_string(), "full_name".to_string()]);
        }
        _ => panic!("expected rows"),
    }

    engine
        .execute("ALTER TABLE t DROP COLUMN age")
        .expect("drop column");
    engine
        .execute("ALTER TABLE t RENAME TO t2")
        .expect("rename table");

    let out = engine
        .execute("SELECT id, full_name FROM t2 ORDER BY id")
        .expect("select renamed table");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2);
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn multi_inner_join_chain_works() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE users (id INT, name TEXT)")
        .expect("create users");
    engine
        .execute("CREATE TABLE orders (id INT, user_id INT)")
        .expect("create orders");
    engine
        .execute("CREATE TABLE payments (id INT, order_id INT)")
        .expect("create payments");
    engine
        .execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .expect("insert users");
    engine
        .execute("INSERT INTO orders VALUES (10, 1), (11, 1), (12, 2)")
        .expect("insert orders");
    engine
        .execute("INSERT INTO payments VALUES (100, 10), (101, 12)")
        .expect("insert payments");

    let out = engine
        .execute(
            "SELECT users.name, orders.id, payments.id \
             FROM users \
             INNER JOIN orders ON users.id = orders.user_id \
             INNER JOIN payments ON orders.id = payments.order_id \
             ORDER BY payments.id",
        )
        .expect("multi join");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[0],
                vec![
                    Value::Text("alice".to_string()),
                    Value::Int32(10),
                    Value::Int32(100)
                ]
            );
            assert_eq!(
                rows[1],
                vec![
                    Value::Text("bob".to_string()),
                    Value::Int32(12),
                    Value::Int32(101)
                ]
            );
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn group_by_multiple_columns_works() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE events (kind TEXT, region TEXT, v INT)")
        .expect("create events");
    engine
        .execute(
            "INSERT INTO events VALUES \
             ('click', 'us', 1), ('click', 'us', 2), ('click', 'eu', 3), \
             ('view', 'us', 10), ('view', 'us', 11)",
        )
        .expect("insert events");

    let out = engine
        .execute(
            "SELECT kind, region, COUNT(*) AS cnt, SUM(v) AS total \
             FROM events \
             GROUP BY kind, region \
             ORDER BY kind, region",
        )
        .expect("group by multiple cols");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 3);
            assert_eq!(
                rows[0],
                vec![
                    Value::Text("click".to_string()),
                    Value::Text("eu".to_string()),
                    Value::Int64(1),
                    Value::Int64(3),
                ]
            );
            assert_eq!(
                rows[1],
                vec![
                    Value::Text("click".to_string()),
                    Value::Text("us".to_string()),
                    Value::Int64(2),
                    Value::Int64(3),
                ]
            );
            assert_eq!(
                rows[2],
                vec![
                    Value::Text("view".to_string()),
                    Value::Text("us".to_string()),
                    Value::Int64(2),
                    Value::Int64(21),
                ]
            );
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn upsert_on_conflict_do_nothing_and_do_update() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE users (id INT, name TEXT, age INT)")
        .expect("create users");
    engine
        .execute("INSERT INTO users VALUES (1, 'alice', 10)")
        .expect("insert seed");

    let out = engine
        .execute("INSERT INTO users VALUES (1, 'dup', 11) ON CONFLICT (id) DO NOTHING")
        .expect("upsert do nothing");
    assert_eq!(out, vec![QueryOutput::AffectedRows(0)]);

    let out = engine
        .execute(
            "INSERT INTO users VALUES (1, 'alice2', 20) \
             ON CONFLICT (id) DO UPDATE SET age = excluded.age, name = excluded.name",
        )
        .expect("upsert do update");
    assert_eq!(out, vec![QueryOutput::AffectedRows(1)]);

    let out = engine
        .execute("SELECT id, name, age FROM users ORDER BY id")
        .expect("verify");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(
                rows,
                &vec![vec![
                    Value::Int32(1),
                    Value::Text("alice2".to_string()),
                    Value::Int32(20)
                ]]
            );
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn update_from_and_returning_work() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE users (id INT, age INT)")
        .expect("create users");
    engine
        .execute("CREATE TABLE ages (id INT, next_age INT)")
        .expect("create ages");
    engine
        .execute("INSERT INTO users VALUES (1, 10), (2, 20), (3, 30)")
        .expect("insert users");
    engine
        .execute("INSERT INTO ages VALUES (2, 99), (3, 77)")
        .expect("insert ages");

    let out = engine
        .execute(
            "UPDATE users SET age = ages.next_age \
             FROM ages \
             WHERE users.id = ages.id \
             RETURNING users.id, users.age",
        )
        .expect("update from returning");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert!(rows.contains(&vec![Value::Int32(2), Value::Int32(99)]));
            assert!(rows.contains(&vec![Value::Int32(3), Value::Int32(77)]));
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn delete_using_returning_and_limit_work() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE users (id INT, age INT)")
        .expect("create users");
    engine
        .execute("CREATE TABLE shadow (id INT)")
        .expect("create shadow");
    engine
        .execute("INSERT INTO users VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .expect("insert users");
    engine
        .execute("INSERT INTO shadow VALUES (2), (4)")
        .expect("insert shadow");

    let out = engine
        .execute(
            "DELETE FROM users USING shadow \
             WHERE users.id = shadow.id \
             ORDER BY users.id DESC \
             LIMIT 1",
        )
        .expect("delete using");
    assert_eq!(out, vec![QueryOutput::AffectedRows(1)]);

    let out = engine
        .execute(
            "DELETE FROM users USING shadow \
             WHERE users.id = shadow.id \
             RETURNING users.id",
        )
        .expect("delete using returning");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => assert_eq!(rows, &vec![vec![Value::Int32(2)]]),
        _ => panic!("expected rows"),
    }
}

#[test]
fn multi_table_delete_works() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE users (id INT)")
        .expect("create users");
    engine
        .execute("CREATE TABLE orders (id INT, user_id INT)")
        .expect("create orders");
    engine
        .execute("INSERT INTO users VALUES (1), (2)")
        .expect("insert users");
    engine
        .execute("INSERT INTO orders VALUES (10, 1), (11, 2)")
        .expect("insert orders");

    let out = engine
        .execute(
            "DELETE users, orders \
             FROM users \
             INNER JOIN orders ON users.id = orders.user_id \
             WHERE users.id = 1",
        )
        .expect("multi delete");
    assert_eq!(out, vec![QueryOutput::AffectedRows(2)]);
}

#[test]
fn truncate_marks_rows_deleted() {
    let engine = setup_engine();
    engine.execute("CREATE TABLE t (id INT)").expect("create t");
    engine
        .execute("INSERT INTO t VALUES (1), (2), (3)")
        .expect("insert");
    let out = engine.execute("TRUNCATE TABLE t").expect("truncate");
    assert_eq!(out, vec![QueryOutput::AffectedRows(3)]);

    let out = engine.execute("SELECT id FROM t").expect("select");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => assert!(rows.is_empty()),
        _ => panic!("expected rows"),
    }
}

#[test]
fn scalar_functions_and_row_number_window_work() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE f (id INT, name TEXT)")
        .expect("create f");
    engine
        .execute("INSERT INTO f VALUES (2, ' Alice '), (1, 'bob')")
        .expect("insert");

    let out = engine
        .execute(
            "SELECT row_number() OVER (ORDER BY id) AS rn, upper(trim(name)) AS nm, length(name) AS len \
             FROM f",
        )
        .expect("function projection");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0], Value::Int64(1));
            assert_eq!(rows[1][0], Value::Int64(2));
        }
        _ => panic!("expected rows"),
    }

    let out = engine
        .execute(
            "SELECT abs(-7) AS a, concat('en', 't') AS c, coalesce(NULL, 'tree') AS z \
             FROM f ORDER BY id LIMIT 1",
        )
        .expect("extra scalar functions");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Int64(7));
            assert_eq!(rows[0][1], Value::Text("ent".to_string()));
            assert_eq!(rows[0][2], Value::Text("tree".to_string()));
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn union_and_union_all_work() {
    let engine = setup_engine();
    engine.execute("CREATE TABLE a (id INT)").expect("create a");
    engine.execute("CREATE TABLE b (id INT)").expect("create b");
    engine
        .execute("INSERT INTO a VALUES (1), (2)")
        .expect("insert a");
    engine
        .execute("INSERT INTO b VALUES (2), (3)")
        .expect("insert b");

    let out = engine
        .execute("SELECT id FROM a UNION SELECT id FROM b")
        .expect("union");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 3);
        }
        _ => panic!("expected rows"),
    }

    let out = engine
        .execute("SELECT id FROM a UNION ALL SELECT id FROM b")
        .expect("union all");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 4);
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn with_cte_select_path_works() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE users (id INT, age INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO users VALUES (1, 20), (2, 30), (3, 40)")
        .expect("insert");

    let out = engine
        .execute(
            "WITH older AS (SELECT id, age FROM users WHERE age >= 30) \
             SELECT id FROM older ORDER BY id",
        )
        .expect("with cte");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows, &vec![vec![Value::Int32(2)], vec![Value::Int32(3)]]);
        }
        _ => panic!("expected rows"),
    }
}

#[test]
fn from_subquery_select_path_works() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE users (id INT, age INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO users VALUES (1, 20), (2, 30), (3, 40)")
        .expect("insert");

    let out = engine
        .execute("SELECT s.id FROM (SELECT id, age FROM users WHERE age >= 30) s ORDER BY s.id")
        .expect("from subquery");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows, &vec![vec![Value::Int32(2)], vec![Value::Int32(3)]]);
        }
        _ => panic!("expected rows"),
    }
}
