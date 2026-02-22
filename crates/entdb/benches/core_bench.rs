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
use std::sync::Arc;
use tempfile::{tempdir, TempDir};

fn setup_engine() -> (TempDir, QueryEngine) {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("criterion.db");
    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(256, Arc::clone(&dm)));
    let catalog = Arc::new(Catalog::init(Arc::clone(&bp)).expect("catalog init"));
    let engine = QueryEngine::new(catalog);

    engine
        .execute("CREATE TABLE bench_users (id INT, v INT)")
        .expect("create table");
    let mut values = String::new();
    for i in 0..2000_i32 {
        if i > 0 {
            values.push_str(", ");
        }
        values.push_str(&format!("({}, {})", i, 2000 - i));
    }
    engine
        .execute(&format!("INSERT INTO bench_users VALUES {values}"))
        .expect("seed rows");
    (dir, engine)
}

fn criterion_query_count(c: &mut Criterion) {
    let (_dir, engine) = setup_engine();
    c.bench_function("query_count_where", |b| {
        b.iter(|| {
            let out = engine
                .execute(black_box("SELECT COUNT(*) FROM bench_users WHERE v > 1000"))
                .expect("count");
            black_box(out);
        })
    });
}

fn criterion_query_order_limit(c: &mut Criterion) {
    let (_dir, engine) = setup_engine();
    c.bench_function("query_order_by_limit", |b| {
        b.iter(|| {
            let out = engine
                .execute(black_box(
                    "SELECT id FROM bench_users ORDER BY v DESC LIMIT 50",
                ))
                .expect("order query");
            black_box(out);
        })
    });
}

criterion_group!(benches, criterion_query_count, criterion_query_order_limit);
criterion_main!(benches);
