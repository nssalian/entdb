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

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use entdb::catalog::Catalog;
use entdb::query::QueryEngine;
use entdb::storage::buffer_pool::BufferPool;
use entdb::storage::disk_manager::DiskManager;
use entdb::types::Value;
use entdb::BulkUpdate;
use entdb::DurabilityMode;
use rusqlite::Connection;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};

const ROWS: i32 = 100;

struct EntDbState {
    _dir: TempDir,
    engine: QueryEngine,
}

struct SqliteState {
    _dir: TempDir,
    conn: Connection,
}

#[derive(Clone, Copy)]
enum SqliteSyncMode {
    Full,
    Normal,
}

fn setup_entdb(seed_rows: bool, durability_mode: DurabilityMode) -> EntDbState {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("workload_entdb.data");
    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(256, Arc::clone(&dm)));
    let catalog = Arc::new(Catalog::init(Arc::clone(&bp)).expect("catalog init"));
    let engine = QueryEngine::new(catalog);
    engine.set_durability_mode(durability_mode);

    engine
        .execute("CREATE TABLE test (id INT PRIMARY KEY, name TEXT, value FLOAT)")
        .expect("create table");
    engine
        .execute("CREATE INDEX idx_test_id ON test (id)")
        .expect("create index");

    if seed_rows {
        seed_entdb(&engine);
    }

    EntDbState { _dir: dir, engine }
}

fn setup_sqlite(seed_rows: bool, sync_mode: SqliteSyncMode) -> SqliteState {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("workload_sqlite.db");
    let conn = Connection::open(db_path).expect("open sqlite");
    conn.pragma_update(None, "journal_mode", "WAL")
        .expect("set wal mode");
    let synchronous = match sync_mode {
        SqliteSyncMode::Full => "FULL",
        SqliteSyncMode::Normal => "NORMAL",
    };
    conn.pragma_update(None, "synchronous", synchronous)
        .expect("set sync mode");
    conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value REAL)",
        [],
    )
    .expect("create table");

    if seed_rows {
        seed_sqlite(&conn);
    }

    SqliteState { _dir: dir, conn }
}

fn seed_entdb(engine: &QueryEngine) {
    let mut values = String::new();
    for i in 0..ROWS {
        if i > 0 {
            values.push_str(", ");
        }
        values.push_str(&format!("({}, 'name{}', {})", i, i, i));
    }
    engine
        .execute(&format!("INSERT INTO test VALUES {values}"))
        .expect("seed");
}

fn seed_sqlite(conn: &Connection) {
    let tx = conn.unchecked_transaction().expect("tx");
    {
        let mut stmt = tx
            .prepare("INSERT INTO test (id, name, value) VALUES (?1, ?2, ?3)")
            .expect("prepare");
        for i in 0..i64::from(ROWS) {
            let name = format!("name{i}");
            stmt.execute((i, name, i as f64)).expect("insert");
        }
    }
    tx.commit().expect("commit");
}

fn bench_insert_100_no_txn(c: &mut Criterion) {
    let mut group = c.benchmark_group("workload_insert_100_no_txn");
    group.sample_size(30);
    let insert_sql: Vec<String> = (0..ROWS)
        .map(|i| format!("INSERT INTO test VALUES ({i}, 'name{i}', {i})"))
        .collect();

    group.bench_function("entdb", |b| {
        b.iter_batched(
            || setup_entdb(false, DurabilityMode::Full),
            |state| {
                for sql in &insert_sql {
                    let out = state.engine.execute(sql).expect("insert");
                    black_box(out);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("entdb_normal", |b| {
        b.iter_batched(
            || setup_entdb(false, DurabilityMode::Normal),
            |state| {
                for sql in &insert_sql {
                    let out = state.engine.execute(sql).expect("insert");
                    black_box(out);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("sqlite", |b| {
        b.iter_batched(
            || setup_sqlite(false, SqliteSyncMode::Full),
            |state| {
                for i in 0..i64::from(ROWS) {
                    let name = format!("name{i}");
                    let rows = state
                        .conn
                        .execute(
                            "INSERT INTO test (id, name, value) VALUES (?1, ?2, ?3)",
                            (i, name, i as f64),
                        )
                        .expect("insert");
                    black_box(rows);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("sqlite_normal", |b| {
        b.iter_batched(
            || setup_sqlite(false, SqliteSyncMode::Normal),
            |state| {
                for i in 0..i64::from(ROWS) {
                    let name = format!("name{i}");
                    let rows = state
                        .conn
                        .execute(
                            "INSERT INTO test (id, name, value) VALUES (?1, ?2, ?3)",
                            (i, name, i as f64),
                        )
                        .expect("insert");
                    black_box(rows);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_select_all_100(c: &mut Criterion) {
    let mut group = c.benchmark_group("workload_select_all_100");
    group.sample_size(30);
    let entdb = setup_entdb(true, DurabilityMode::Full);
    let entdb_normal = setup_entdb(true, DurabilityMode::Normal);
    let sqlite = setup_sqlite(true, SqliteSyncMode::Full);
    let prepared = entdb.engine.prepare("SELECT id, name, value FROM test");
    let prepared_normal = entdb_normal
        .engine
        .prepare("SELECT id, name, value FROM test");

    group.bench_function("entdb", |b| {
        b.iter(|| {
            let out = entdb
                .engine
                .execute("SELECT id, name, value FROM test")
                .expect("select all");
            black_box(out);
        });
    });

    group.bench_function("entdb_normal", |b| {
        b.iter(|| {
            let out = entdb_normal
                .engine
                .execute("SELECT id, name, value FROM test")
                .expect("select all");
            black_box(out);
        });
    });

    group.bench_function("entdb_prepared", |b| {
        b.iter(|| {
            let out = entdb
                .engine
                .execute_prepared(&prepared, &[])
                .expect("select all prepared");
            black_box(out);
        });
    });

    group.bench_function("entdb_normal_prepared", |b| {
        b.iter(|| {
            let out = entdb_normal
                .engine
                .execute_prepared(&prepared_normal, &[])
                .expect("select all prepared");
            black_box(out);
        });
    });

    group.bench_function("sqlite", |b| {
        b.iter(|| {
            let mut stmt = sqlite
                .conn
                .prepare("SELECT id, name, value FROM test")
                .expect("prepare");
            let rows: Vec<(i64, String, f64)> = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .expect("query")
                .collect::<Result<Vec<_>, _>>()
                .expect("collect");
            black_box(rows);
        });
    });

    group.bench_function("sqlite_prepared", |b| {
        let mut stmt = sqlite
            .conn
            .prepare("SELECT id, name, value FROM test")
            .expect("prepare");
        b.iter(|| {
            let rows: Vec<(i64, String, f64)> = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .expect("query")
                .collect::<Result<Vec<_>, _>>()
                .expect("collect");
            black_box(rows);
        });
    });

    group.finish();
}

fn bench_select_by_id_x100(c: &mut Criterion) {
    let mut group = c.benchmark_group("workload_select_by_id_x100");
    group.sample_size(30);
    let select_sql: Vec<String> = (0..ROWS)
        .map(|i| format!("SELECT id, name, value FROM test WHERE id = {i}"))
        .collect();
    let entdb = setup_entdb(true, DurabilityMode::Full);
    let entdb_normal = setup_entdb(true, DurabilityMode::Normal);
    let sqlite = setup_sqlite(true, SqliteSyncMode::Full);
    let prepared = entdb
        .engine
        .prepare("SELECT id, name, value FROM test WHERE id = $1");
    let prepared_normal = entdb_normal
        .engine
        .prepare("SELECT id, name, value FROM test WHERE id = $1");

    group.bench_function("entdb", |b| {
        b.iter(|| {
            for sql in &select_sql {
                let out = entdb.engine.execute(sql).expect("select by id");
                black_box(out);
            }
        });
    });

    group.bench_function("entdb_normal", |b| {
        b.iter(|| {
            for sql in &select_sql {
                let out = entdb_normal.engine.execute(sql).expect("select by id");
                black_box(out);
            }
        });
    });

    group.bench_function("entdb_prepared", |b| {
        b.iter(|| {
            for i in 0..i64::from(ROWS) {
                let params = [Value::Int64(i)];
                let out = entdb
                    .engine
                    .execute_prepared(&prepared, &params)
                    .expect("select by id prepared");
                black_box(out);
            }
        });
    });

    group.bench_function("entdb_normal_prepared", |b| {
        b.iter(|| {
            for i in 0..i64::from(ROWS) {
                let params = [Value::Int64(i)];
                let out = entdb_normal
                    .engine
                    .execute_prepared(&prepared_normal, &params)
                    .expect("select by id prepared");
                black_box(out);
            }
        });
    });

    group.bench_function("sqlite", |b| {
        b.iter(|| {
            for i in 0..i64::from(ROWS) {
                let row: (i64, String, f64) = sqlite
                    .conn
                    .query_row("SELECT id, name, value FROM test WHERE id = ?1", [i], |r| {
                        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                    })
                    .expect("select by id");
                black_box(row);
            }
        });
    });

    group.bench_function("sqlite_prepared", |b| {
        let mut stmt = sqlite
            .conn
            .prepare("SELECT id, name, value FROM test WHERE id = ?1")
            .expect("prepare");
        b.iter(|| {
            for i in 0..i64::from(ROWS) {
                let row: (i64, String, f64) = stmt
                    .query_row([i], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                    .expect("select by id");
                black_box(row);
            }
        });
    });

    group.finish();
}

fn bench_update_100(c: &mut Criterion) {
    let mut group = c.benchmark_group("workload_update_100");
    group.sample_size(30);
    let update_sql: Vec<String> = (0..ROWS)
        .map(|i| format!("UPDATE test SET value = value + 1 WHERE id = {i}"))
        .collect();

    group.bench_function("entdb", |b| {
        b.iter_batched(
            || setup_entdb(true, DurabilityMode::Full),
            |state| {
                for sql in &update_sql {
                    let out = state.engine.execute(sql).expect("update");
                    black_box(out);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("entdb_normal", |b| {
        b.iter_batched(
            || setup_entdb(true, DurabilityMode::Normal),
            |state| {
                for sql in &update_sql {
                    let out = state.engine.execute(sql).expect("update");
                    black_box(out);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("sqlite", |b| {
        b.iter_batched(
            || setup_sqlite(true, SqliteSyncMode::Full),
            |state| {
                for i in 0..i64::from(ROWS) {
                    let rows = state
                        .conn
                        .execute("UPDATE test SET value = value + 1 WHERE id = ?1", [i])
                        .expect("update");
                    black_box(rows);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("sqlite_normal", |b| {
        b.iter_batched(
            || setup_sqlite(true, SqliteSyncMode::Normal),
            |state| {
                for i in 0..i64::from(ROWS) {
                    let rows = state
                        .conn
                        .execute("UPDATE test SET value = value + 1 WHERE id = ?1", [i])
                        .expect("update");
                    black_box(rows);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_delete_100(c: &mut Criterion) {
    let mut group = c.benchmark_group("workload_delete_100");
    group.sample_size(30);
    let delete_sql: Vec<String> = (0..ROWS)
        .map(|i| format!("DELETE FROM test WHERE id = {i}"))
        .collect();

    group.bench_function("entdb", |b| {
        b.iter_batched(
            || setup_entdb(true, DurabilityMode::Full),
            |state| {
                for sql in &delete_sql {
                    let out = state.engine.execute(sql).expect("delete");
                    black_box(out);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("entdb_normal", |b| {
        b.iter_batched(
            || setup_entdb(true, DurabilityMode::Normal),
            |state| {
                for sql in &delete_sql {
                    let out = state.engine.execute(sql).expect("delete");
                    black_box(out);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("sqlite", |b| {
        b.iter_batched(
            || setup_sqlite(true, SqliteSyncMode::Full),
            |state| {
                for i in 0..i64::from(ROWS) {
                    let rows = state
                        .conn
                        .execute("DELETE FROM test WHERE id = ?1", [i])
                        .expect("delete");
                    black_box(rows);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("sqlite_normal", |b| {
        b.iter_batched(
            || setup_sqlite(true, SqliteSyncMode::Normal),
            |state| {
                for i in 0..i64::from(ROWS) {
                    let rows = state
                        .conn
                        .execute("DELETE FROM test WHERE id = ?1", [i])
                        .expect("delete");
                    black_box(rows);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_transaction_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("workload_transaction_batch");
    group.sample_size(30);
    let insert_sql: Vec<String> = (0..ROWS)
        .map(|i| format!("INSERT INTO test VALUES ({i}, 'name{i}', {i})"))
        .collect();

    group.bench_function("entdb", |b| {
        b.iter_batched(
            || setup_entdb(false, DurabilityMode::Full),
            |state| {
                black_box(state.engine.execute("BEGIN").expect("begin"));
                for sql in &insert_sql {
                    black_box(state.engine.execute(sql).expect("insert"));
                }
                black_box(state.engine.execute("COMMIT").expect("commit"));
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("entdb_normal", |b| {
        b.iter_batched(
            || setup_entdb(false, DurabilityMode::Normal),
            |state| {
                black_box(state.engine.execute("BEGIN").expect("begin"));
                for sql in &insert_sql {
                    black_box(state.engine.execute(sql).expect("insert"));
                }
                black_box(state.engine.execute("COMMIT").expect("commit"));
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("sqlite", |b| {
        b.iter_batched(
            || setup_sqlite(false, SqliteSyncMode::Full),
            |mut state| {
                let tx = state.conn.transaction().expect("begin");
                {
                    let mut stmt = tx
                        .prepare("INSERT INTO test (id, name, value) VALUES (?1, ?2, ?3)")
                        .expect("prepare");
                    for i in 0..i64::from(ROWS) {
                        let name = format!("name{i}");
                        black_box(stmt.execute((i, name, i as f64)).expect("insert"));
                    }
                }
                tx.commit().expect("commit");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("sqlite_normal", |b| {
        b.iter_batched(
            || setup_sqlite(false, SqliteSyncMode::Normal),
            |mut state| {
                let tx = state.conn.transaction().expect("begin");
                {
                    let mut stmt = tx
                        .prepare("INSERT INTO test (id, name, value) VALUES (?1, ?2, ?3)")
                        .expect("prepare");
                    for i in 0..i64::from(ROWS) {
                        let name = format!("name{i}");
                        black_box(stmt.execute((i, name, i as f64)).expect("insert"));
                    }
                }
                tx.commit().expect("commit");
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_bulk_api(c: &mut Criterion) {
    let mut insert_group = c.benchmark_group("workload_insert_100_bulk_api");
    insert_group.sample_size(30);
    let rows: Vec<Vec<Value>> = (0..i64::from(ROWS))
        .map(|i| {
            vec![
                Value::Int64(i),
                Value::Text(format!("name{i}")),
                Value::Float64(i as f64),
            ]
        })
        .collect();

    insert_group.bench_function("entdb", |b| {
        b.iter_batched(
            || setup_entdb(false, DurabilityMode::Full),
            |state| {
                let rows = state
                    .engine
                    .insert_many("test", &rows)
                    .expect("bulk insert");
                black_box(rows);
            },
            BatchSize::SmallInput,
        );
    });

    insert_group.bench_function("entdb_normal", |b| {
        b.iter_batched(
            || setup_entdb(false, DurabilityMode::Normal),
            |state| {
                let rows = state
                    .engine
                    .insert_many("test", &rows)
                    .expect("bulk insert");
                black_box(rows);
            },
            BatchSize::SmallInput,
        );
    });
    insert_group.finish();

    let mut update_group = c.benchmark_group("workload_update_100_bulk_api");
    update_group.sample_size(30);
    let updates: Vec<BulkUpdate> = (0..i64::from(ROWS))
        .map(|i| BulkUpdate {
            key: Value::Int64(i),
            assignments: vec![("value".to_string(), Value::Float64((i + 1) as f64))],
        })
        .collect();

    update_group.bench_function("entdb", |b| {
        b.iter_batched(
            || setup_entdb(true, DurabilityMode::Full),
            |state| {
                let rows = state
                    .engine
                    .update_many("test", "id", &updates)
                    .expect("bulk update");
                black_box(rows);
            },
            BatchSize::SmallInput,
        );
    });

    update_group.bench_function("entdb_normal", |b| {
        b.iter_batched(
            || setup_entdb(true, DurabilityMode::Normal),
            |state| {
                let rows = state
                    .engine
                    .update_many("test", "id", &updates)
                    .expect("bulk update");
                black_box(rows);
            },
            BatchSize::SmallInput,
        );
    });
    update_group.finish();

    let mut delete_group = c.benchmark_group("workload_delete_100_bulk_api");
    delete_group.sample_size(30);
    let delete_keys: Vec<Value> = (0..i64::from(ROWS)).map(Value::Int64).collect();

    delete_group.bench_function("entdb", |b| {
        b.iter_batched(
            || setup_entdb(true, DurabilityMode::Full),
            |state| {
                let rows = state
                    .engine
                    .delete_many("test", "id", &delete_keys)
                    .expect("bulk delete");
                black_box(rows);
            },
            BatchSize::SmallInput,
        );
    });

    delete_group.bench_function("entdb_normal", |b| {
        b.iter_batched(
            || setup_entdb(true, DurabilityMode::Normal),
            |state| {
                let rows = state
                    .engine
                    .delete_many("test", "id", &delete_keys)
                    .expect("bulk delete");
                black_box(rows);
            },
            BatchSize::SmallInput,
        );
    });
    delete_group.finish();
}

criterion_group!(
    benches,
    bench_insert_100_no_txn,
    bench_select_all_100,
    bench_select_by_id_x100,
    bench_update_100,
    bench_delete_100,
    bench_transaction_batch,
    bench_bulk_api
);
criterion_main!(benches);
