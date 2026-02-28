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

use entdb::fault;
use entdb::storage::bm25::Bm25Index;
use entdb::EntDb;
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;

fn failpoint_test_guard() -> std::sync::MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

#[test]
fn bm25_sidecar_recovers_after_failpoint_and_rebuild() {
    let _guard = failpoint_test_guard();
    fault::clear_all_failpoints();
    let dir = tempdir().expect("tempdir");
    let db_root = dir.path().join("bm25_fail_rebuild");
    let db = EntDb::connect(&db_root).expect("connect");
    db.execute("CREATE TABLE docs (id INT, content TEXT)")
        .expect("create");
    db.execute("INSERT INTO docs VALUES (1, 'database systems')")
        .expect("insert");
    db.execute("CREATE INDEX idx_docs_bm25 ON docs USING bm25 (content)")
        .expect("index");

    fault::set_failpoint("bm25.persist.rename", 0);
    let err = db
        .execute("INSERT INTO docs VALUES (2, 'database retrieval')")
        .expect_err("persist failpoint should fail write");
    assert!(err.to_string().contains("failpoint"));
    fault::clear_all_failpoints();

    // Rebuild index from table data should converge sidecar state.
    db.execute("DROP INDEX idx_docs_bm25").expect("drop index");
    db.execute("CREATE INDEX idx_docs_bm25 ON docs USING bm25 (content)")
        .expect("recreate index");
    let db_file = db_root.join("entdb.data");
    let sidecar = Bm25Index::load(&db_file, "idx_docs_bm25").expect("load sidecar");
    assert_eq!(sidecar.document_count(), 2);
    fault::clear_all_failpoints();
}

#[test]
fn bm25_concurrent_insert_maintains_sidecar_readability() {
    let _guard = failpoint_test_guard();
    fault::clear_all_failpoints();
    let dir = tempdir().expect("tempdir");
    let db_root = dir.path().join("bm25_concurrent");
    let db = EntDb::connect(&db_root).expect("connect");
    db.execute("CREATE TABLE docs (id INT, content TEXT)")
        .expect("create");
    db.execute("CREATE INDEX idx_docs_bm25 ON docs USING bm25 (content)")
        .expect("index");
    db.close().expect("close bootstrap handle");

    let mut threads = Vec::new();
    for t in 0..4_i32 {
        let db_root_cloned = db_root.clone();
        threads.push(std::thread::spawn(move || {
            let dbc = EntDb::connect(&db_root_cloned).expect("connect thread db");
            for i in 0..40_i32 {
                let id = t * 1000 + i;
                let sql = format!("INSERT INTO docs VALUES ({id}, 'database systems {id}')");
                dbc.execute(&sql).expect("concurrent insert");
            }
            dbc.close().expect("close thread db");
        }));
    }
    for th in threads {
        th.join().expect("join");
    }

    let db_file = db_root.join("entdb.data");
    let sidecar = Bm25Index::load(&db_file, "idx_docs_bm25").expect("load sidecar");
    assert!(
        sidecar.document_count() >= 40,
        "expected at least one writer batch to persist without sidecar corruption"
    );
    assert!(sidecar.document_frequency("database") >= 1);
    fault::clear_all_failpoints();
}
