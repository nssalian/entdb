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

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use entdb::catalog::Catalog;
use entdb::query::QueryEngine;
use entdb::storage::buffer_pool::BufferPool;
use entdb::storage::disk_manager::DiskManager;
use entdb::types::Value;
use entdb::DurabilityMode;
use rusqlite::Connection;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};

const ROWS: i32 = 20_000;
const COUNT_WHERE_PREPARED_SQL: &str = "SELECT COUNT(*) FROM t WHERE v > $1";
const COUNT_WHERE_SQLITE_PREPARED_SQL: &str = "SELECT COUNT(*) FROM t WHERE v > ?1";
const COUNT_WHERE_SQL: &str = "SELECT COUNT(*) FROM t WHERE v > 10000";
const ORDER_LIMIT_SQL: &str = "SELECT id FROM t ORDER BY v DESC LIMIT 50";

fn seed_entdb(engine: &QueryEngine, rows: i32) {
    let mut start = 0_i32;
    let chunk_size = 1_000_i32;
    while start < rows {
        let end = (start + chunk_size).min(rows);
        let mut values = String::new();
        for i in start..end {
            if i > start {
                values.push_str(", ");
            }
            values.push_str(&format!("({}, {})", i, rows - i));
        }
        engine
            .execute(&format!("INSERT INTO t VALUES {values}"))
            .expect("insert chunk");
        start = end;
    }
}

fn setup_entdb(mode: DurabilityMode) -> (TempDir, QueryEngine) {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("bench_entdb.data");
    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(256, Arc::clone(&dm)));
    let catalog = Arc::new(Catalog::init(Arc::clone(&bp)).expect("catalog init"));
    let engine = QueryEngine::new(catalog);
    engine.set_durability_mode(mode);
    engine
        .execute("CREATE TABLE t (id INT, v INT)")
        .expect("create");
    seed_entdb(&engine, ROWS);
    (dir, engine)
}

fn setup_sqlite() -> (TempDir, Connection) {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("bench_sqlite.db");
    let conn = Connection::open(db_path).expect("open sqlite");
    conn.execute("CREATE TABLE t (id INTEGER, v INTEGER)", [])
        .expect("create");
    let tx = conn.unchecked_transaction().expect("tx");
    {
        let mut stmt = tx
            .prepare("INSERT INTO t (id, v) VALUES (?1, ?2)")
            .expect("prepare");
        for i in 0..i64::from(ROWS) {
            stmt.execute((i, i64::from(ROWS) - i)).expect("insert");
        }
    }
    tx.commit().expect("commit");
    (dir, conn)
}

fn benchmark_count_where(c: &mut Criterion) {
    let (_dir, engine) = setup_entdb(DurabilityMode::Full);
    let (_dir_normal, engine_normal) = setup_entdb(DurabilityMode::Normal);
    let (_sqlite_dir, sqlite) = setup_sqlite();

    c.bench_function("api_entdb_count_where", |b| {
        b.iter(|| {
            let out = engine
                .execute(black_box(COUNT_WHERE_SQL))
                .expect("entdb count");
            black_box(out);
        });
    });

    c.bench_function("api_entdb_prepared_count_where", |b| {
        let prepared = engine.prepare(COUNT_WHERE_PREPARED_SQL);
        let params = [Value::Int64(10_000)];
        b.iter(|| {
            let out = engine
                .execute_prepared(&prepared, black_box(&params))
                .expect("entdb prepared count");
            black_box(out);
        });
    });

    c.bench_function("api_entdb_normal_count_where", |b| {
        b.iter(|| {
            let out = engine_normal
                .execute(black_box(COUNT_WHERE_SQL))
                .expect("entdb normal count");
            black_box(out);
        });
    });

    c.bench_function("api_sqlite_count_where", |b| {
        b.iter(|| {
            let count: i64 = sqlite
                .query_row(black_box(COUNT_WHERE_SQL), [], |r| r.get(0))
                .expect("query");
            black_box(count);
        });
    });

    c.bench_function("prepare_plus_query_sqlite_param_count_where", |b| {
        b.iter(|| {
            let mut stmt = sqlite
                .prepare(black_box(COUNT_WHERE_SQLITE_PREPARED_SQL))
                .expect("prepare");
            let count: i64 = stmt.query_row([10_000_i64], |r| r.get(0)).expect("query");
            black_box(count);
        });
    });

    let mut prepared_param_stmt = sqlite
        .prepare(COUNT_WHERE_SQLITE_PREPARED_SQL)
        .expect("prepare once");
    c.bench_function("prepared_sqlite_param_count_where", |b| {
        b.iter(|| {
            let count: i64 = prepared_param_stmt
                .query_row([10_000_i64], |r| r.get(0))
                .expect("query");
            black_box(count);
        });
    });
}

fn benchmark_order_limit(c: &mut Criterion) {
    let (_dir, engine) = setup_entdb(DurabilityMode::Full);
    let (_dir_normal, engine_normal) = setup_entdb(DurabilityMode::Normal);
    let (_sqlite_dir, sqlite) = setup_sqlite();

    c.bench_function("api_entdb_order_by_limit", |b| {
        b.iter(|| {
            let out = engine
                .execute(black_box(ORDER_LIMIT_SQL))
                .expect("entdb order");
            black_box(out);
        });
    });

    c.bench_function("api_entdb_prepared_order_by_limit", |b| {
        let prepared = engine.prepare(ORDER_LIMIT_SQL);
        b.iter(|| {
            let out = engine
                .execute_prepared(&prepared, &[])
                .expect("entdb prepared order");
            black_box(out);
        });
    });

    c.bench_function("api_entdb_normal_order_by_limit", |b| {
        b.iter(|| {
            let out = engine_normal
                .execute(black_box(ORDER_LIMIT_SQL))
                .expect("entdb normal order");
            black_box(out);
        });
    });

    c.bench_function("api_sqlite_order_by_limit", |b| {
        b.iter(|| {
            let mut stmt = sqlite.prepare(black_box(ORDER_LIMIT_SQL)).expect("prepare");
            let out: Vec<i64> = stmt
                .query_map([], |r| r.get::<_, i64>(0))
                .expect("query map")
                .collect::<Result<Vec<_>, _>>()
                .expect("collect");
            black_box(out);
        });
    });

    let mut prepared_stmt = sqlite.prepare(ORDER_LIMIT_SQL).expect("prepare once");
    c.bench_function("prepared_sqlite_order_by_limit", |b| {
        b.iter(|| {
            let out: Vec<i64> = prepared_stmt
                .query_map([], |r| r.get::<_, i64>(0))
                .expect("query map")
                .collect::<Result<Vec<_>, _>>()
                .expect("collect");
            black_box(out);
        });
    });
}

criterion_group!(benches, benchmark_count_where, benchmark_order_limit);
criterion_main!(benches);
