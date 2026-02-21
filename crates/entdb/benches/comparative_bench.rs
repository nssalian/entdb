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
use rusqlite::Connection;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};

fn setup_entdb() -> (TempDir, QueryEngine) {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("bench_entdb.data");
    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(256, Arc::clone(&dm)));
    let catalog = Arc::new(Catalog::init(Arc::clone(&bp)).expect("catalog init"));
    let engine = QueryEngine::new(catalog);
    engine
        .execute("CREATE TABLE t (id INT, v INT)")
        .expect("create");
    for i in 0..2_000_i32 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({}, {})", i, 2_000 - i))
            .expect("insert");
    }
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
        for i in 0..2_000_i64 {
            stmt.execute((i, 2_000 - i)).expect("insert");
        }
    }
    tx.commit().expect("commit");
    (dir, conn)
}

fn benchmark_count_where(c: &mut Criterion) {
    let (_dir, engine) = setup_entdb();
    let (_sqlite_dir, sqlite) = setup_sqlite();

    c.bench_function("compare_entdb_count_where", |b| {
        b.iter(|| {
            let out = engine
                .execute(black_box("SELECT COUNT(*) FROM t WHERE v > 1000"))
                .expect("entdb count");
            black_box(out);
        });
    });

    c.bench_function("compare_sqlite_count_where", |b| {
        b.iter(|| {
            let mut stmt = sqlite
                .prepare("SELECT COUNT(*) FROM t WHERE v > 1000")
                .expect("prepare");
            let count: i64 = stmt.query_row([], |r| r.get(0)).expect("query");
            black_box(count);
        });
    });
}

criterion_group!(benches, benchmark_count_where);
criterion_main!(benches);
