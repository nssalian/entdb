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

use entdb::catalog::Catalog;
use entdb::fault;
use entdb::query::{QueryEngine, QueryOutput};
use entdb::storage::buffer_pool::BufferPool;
use entdb::storage::disk_manager::DiskManager;
use entdb::types::Value;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::sync::Arc;

fn open_engine(db_path: &std::path::Path, load_existing: bool) -> QueryEngine {
    let dm = Arc::new(DiskManager::new(db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(128, Arc::clone(&dm)));
    let catalog = if load_existing {
        Arc::new(Catalog::load(Arc::clone(&bp)).expect("catalog load"))
    } else {
        Arc::new(Catalog::init(Arc::clone(&bp)).expect("catalog init"))
    };
    QueryEngine::new(catalog)
}

fn fetch_rows(engine: &QueryEngine) -> Vec<(i32, i32)> {
    let out = engine
        .execute("SELECT id, v FROM t ORDER BY id")
        .expect("select rows");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => rows
            .iter()
            .map(|r| match (&r[0], &r[1]) {
                (Value::Int32(id), Value::Int32(v)) => (*id, *v),
                other => panic!("unexpected row shape: {other:?}"),
            })
            .collect(),
        _ => panic!("expected row output"),
    }
}

#[test]
fn mvcc_recovery_matrix_committed_aborted_and_dangling_transactions() {
    let unique = format!(
        "entdb-mvcc-recovery-matrix-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    );
    let db_path = std::env::temp_dir().join(unique);

    let mut engine = open_engine(&db_path, false);
    engine
        .execute("CREATE TABLE t (id INT, v INT)")
        .expect("create table");

    let mut model: HashMap<i32, i32> = HashMap::new();
    let mut rng = StdRng::seed_from_u64(0xE11DBAAD);
    let mut history = Vec::new();

    for round in 0..40 {
        let id = rng.gen_range(0..12_i32);
        let op = rng.gen_range(0..5_u8);
        let mut op_desc = format!("round={round} op={op} id={id}");

        match op {
            // committed insert
            0 => {
                if !model.contains_key(&id) {
                    let value = rng.gen_range(0..10_000_i32);
                    op_desc = format!("round={round} committed_insert id={id} value={value}");
                    engine
                        .execute(&format!("INSERT INTO t VALUES ({id}, {value})"))
                        .expect("insert");
                    model.insert(id, value);
                }
            }
            // committed update
            1 => {
                if model.contains_key(&id) {
                    let value = rng.gen_range(0..10_000_i32);
                    op_desc = format!("round={round} committed_update id={id} value={value}");
                    engine
                        .execute(&format!("UPDATE t SET v = {value} WHERE id = {id}"))
                        .expect("update");
                    model.insert(id, value);
                }
            }
            // committed delete
            2 => {
                if model.contains_key(&id) {
                    op_desc = format!("round={round} committed_delete id={id}");
                    engine
                        .execute(&format!("DELETE FROM t WHERE id = {id}"))
                        .expect("delete");
                    model.remove(&id);
                }
            }
            // aborted update
            3 => {
                if model.contains_key(&id) {
                    let value = rng.gen_range(0..10_000_i32);
                    op_desc = format!("round={round} aborted_update id={id} value={value}");
                    engine.execute("BEGIN").expect("begin");
                    engine
                        .execute(&format!("UPDATE t SET v = {value} WHERE id = {id}"))
                        .expect("update in tx");
                    engine.execute("ROLLBACK").expect("rollback");
                }
            }
            // dangling insert (crash with active txn)
            _ => {
                if !model.contains_key(&id) {
                    let value = rng.gen_range(0..10_000_i32);
                    op_desc = format!("round={round} dangling_insert id={id} value={value}");
                    engine.execute("BEGIN").expect("begin");
                    engine
                        .execute(&format!("INSERT INTO t VALUES ({id}, {value})"))
                        .expect("insert in tx");
                }
            }
        }
        history.push(op_desc);

        engine.flush_all().expect("flush");
        drop(engine);
        engine = open_engine(&db_path, true);

        let mut expected = model.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>();
        expected.sort_by_key(|(id, _)| *id);
        let actual = fetch_rows(&engine);
        assert_eq!(actual, expected, "history: {:?}", history);
    }
}

#[test]
fn mvcc_recovery_commit_failpoint_does_not_leak_visible_rows_after_restart() {
    let unique = format!(
        "entdb-mvcc-recovery-failpoint-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    );
    let db_path = std::env::temp_dir().join(unique);

    let engine = open_engine(&db_path, false);
    engine
        .execute("CREATE TABLE t (id INT, v INT)")
        .expect("create table");

    fault::set_failpoint("wal.sync", 0);
    let err = engine
        .execute("INSERT INTO t VALUES (1, 10)")
        .expect_err("commit path should fail under wal.sync failpoint");
    assert!(err.to_string().contains("failpoint"));
    fault::clear_all_failpoints();

    engine.flush_all().expect("flush");
    drop(engine);

    let reopened = open_engine(&db_path, true);
    let rows = fetch_rows(&reopened);
    assert!(rows.is_empty());
}
