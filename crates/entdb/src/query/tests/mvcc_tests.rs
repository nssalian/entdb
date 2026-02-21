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
use crate::query::{QueryEngine, QueryOutput, VacuumPolicy};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::disk_manager::DiskManager;
use crate::types::Value;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::time::Duration;

fn setup_engine() -> QueryEngine {
    let db_path = unique_db_path("entdb-query-mvcc");
    setup_engine_at_path(&db_path, false, false)
}

fn unique_db_path(prefix: &str) -> PathBuf {
    let unique = format!(
        "{prefix}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    );
    std::env::temp_dir().join(unique)
}

fn setup_engine_at_path(
    db_path: &Path,
    load_existing: bool,
    durable_txn_state: bool,
) -> QueryEngine {
    let dm = Arc::new(DiskManager::new(db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(64, Arc::clone(&dm)));
    let catalog = if load_existing {
        Arc::new(Catalog::load(Arc::clone(&bp)).expect("catalog load"))
    } else {
        Arc::new(Catalog::init(Arc::clone(&bp)).expect("catalog init"))
    };
    QueryEngine::with_txn_persistence(catalog, durable_txn_state)
}

fn extract_single_i64(out: &[QueryOutput]) -> i64 {
    match &out[0] {
        QueryOutput::Rows { rows, .. } => match &rows[0][0] {
            Value::Int64(v) => *v,
            other => panic!("expected Int64, got {other:?}"),
        },
        _ => panic!("expected rows"),
    }
}

fn run_with_timeout<F>(name: &str, timeout: Duration, f: F)
where
    F: FnOnce() + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
            .map_err(|_| "panic in worker".to_string())
            .map(|_| ());
        let _ = tx.send(result);
    });
    match rx.recv_timeout(timeout) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("{name} failed: {e}"),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("{name} timed out after {:?}", timeout)
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("{name} disconnected before completion")
        }
    }
}

#[test]
fn snapshot_read_does_not_see_future_commits() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (id INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO t VALUES (1)")
        .expect("insert first row");

    let tx = engine.begin_txn();
    let before = engine
        .execute_in_txn(&tx, "SELECT COUNT(*) FROM t")
        .expect("count before");
    assert_eq!(extract_single_i64(&before), 1);

    engine
        .execute("INSERT INTO t VALUES (2)")
        .expect("insert second row");

    let still_before = engine
        .execute_in_txn(&tx, "SELECT COUNT(*) FROM t")
        .expect("count in same snapshot");
    assert_eq!(extract_single_i64(&still_before), 1);
    engine.commit_txn(tx).expect("commit read tx");

    let now = engine.execute("SELECT COUNT(*) FROM t").expect("count now");
    assert_eq!(extract_single_i64(&now), 2);
}

#[test]
fn aborted_transaction_writes_are_invisible() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (id INT)")
        .expect("create table");

    let tx = engine.begin_txn();
    engine
        .execute_in_txn(&tx, "INSERT INTO t VALUES (10)")
        .expect("insert in tx");
    engine.abort_txn(tx);

    let out = engine
        .execute("SELECT COUNT(*) FROM t")
        .expect("count rows");
    assert_eq!(extract_single_i64(&out), 0);
}

#[test]
fn write_write_conflict_detected_on_same_row() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (id INT, v INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO t VALUES (1, 10)")
        .expect("seed row");

    let tx1 = engine.begin_txn();
    engine
        .execute_in_txn(&tx1, "UPDATE t SET v = 11 WHERE id = 1")
        .expect("tx1 update");

    let tx2 = engine.begin_txn();
    let err = engine
        .execute_in_txn(&tx2, "UPDATE t SET v = 12 WHERE id = 1")
        .expect_err("tx2 should conflict");
    assert!(err.to_string().contains("write-write conflict"));

    engine.abort_txn(tx1);
    engine.abort_txn(tx2);
}

#[test]
fn sql_begin_commit_and_rollback_control_visibility() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (id INT)")
        .expect("create table");

    engine.execute("BEGIN").expect("begin");
    engine
        .execute("INSERT INTO t VALUES (1)")
        .expect("insert in txn");
    engine.execute("COMMIT").expect("commit");
    let committed = engine.execute("SELECT COUNT(*) FROM t").expect("count");
    assert_eq!(extract_single_i64(&committed), 1);

    engine.execute("BEGIN").expect("begin second");
    engine
        .execute("INSERT INTO t VALUES (2)")
        .expect("insert second");
    engine.execute("ROLLBACK").expect("rollback");
    let rolled_back = engine.execute("SELECT COUNT(*) FROM t").expect("count");
    assert_eq!(extract_single_i64(&rolled_back), 1);
}

#[test]
fn sql_txn_control_errors_are_explicit() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (id INT)")
        .expect("create table");

    let commit_err = engine
        .execute("COMMIT")
        .expect_err("commit without begin should fail");
    assert!(commit_err.to_string().contains("no active transaction"));

    engine.execute("BEGIN").expect("begin");
    let nested_begin = engine
        .execute("BEGIN")
        .expect_err("nested begin should fail");
    assert!(nested_begin.to_string().contains("already active"));
    engine.execute("ROLLBACK").expect("rollback");
}

#[test]
fn committed_txn_state_persists_across_engine_restart() {
    let db_path = unique_db_path("entdb-query-mvcc-restart");
    {
        let engine = setup_engine_at_path(&db_path, false, true);
        engine
            .execute("CREATE TABLE t (id INT)")
            .expect("create table");
        engine
            .execute("INSERT INTO t VALUES (7)")
            .expect("insert committed row");
        engine.flush_all().expect("flush before restart");
    }

    let reopened = setup_engine_at_path(&db_path, true, true);
    let out = reopened
        .execute("SELECT COUNT(*) FROM t")
        .expect("count after reopen");
    assert_eq!(extract_single_i64(&out), 1);
}

#[test]
fn txn_wal_replay_recovers_when_state_file_is_missing() {
    let db_path = unique_db_path("entdb-query-mvcc-wal-replay");
    {
        let engine = setup_engine_at_path(&db_path, false, true);
        engine
            .execute("CREATE TABLE t (id INT)")
            .expect("create table");
        engine
            .execute("INSERT INTO t VALUES (1), (2)")
            .expect("insert rows");
        engine.flush_all().expect("flush");
    }

    let mut state_path = db_path.clone();
    state_path.set_extension("txn.json");
    std::fs::remove_file(&state_path).expect("remove txn state sidecar");

    let reopened = setup_engine_at_path(&db_path, true, true);
    let out = reopened.execute("SELECT COUNT(*) FROM t").expect("count");
    assert_eq!(extract_single_i64(&out), 2);
}

#[test]
fn vacuum_respects_oldest_snapshot_and_then_reclaims() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (id INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO t VALUES (1), (2)")
        .expect("seed rows");

    let reader = engine.begin_txn();
    let before_delete = engine
        .execute_in_txn(&reader, "SELECT COUNT(*) FROM t")
        .expect("snapshot count before delete");
    assert_eq!(extract_single_i64(&before_delete), 2);

    engine
        .execute("DELETE FROM t WHERE id = 2")
        .expect("delete row");

    let blocked = engine.vacuum().expect("vacuum with active snapshot");
    assert_eq!(blocked, 0);
    let still_visible = engine
        .execute_in_txn(&reader, "SELECT COUNT(*) FROM t")
        .expect("snapshot count still sees row");
    assert_eq!(extract_single_i64(&still_visible), 2);
    engine.commit_txn(reader).expect("commit reader");

    let removed = engine.vacuum().expect("vacuum after reader commit");
    assert_eq!(removed, 1);
    let no_more = engine.vacuum().expect("second vacuum");
    assert_eq!(no_more, 0);
}

#[test]
fn auto_vacuum_triggers_after_deleted_version_threshold() {
    let engine = setup_engine();
    engine.set_vacuum_policy(VacuumPolicy {
        auto_trigger_deleted_versions: Some(2),
        force_trigger_deleted_versions: None,
        min_interval_ms: 0,
        max_reclaims_per_run: None,
    });
    engine
        .execute("CREATE TABLE t (id INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO t VALUES (1), (2), (3)")
        .expect("seed rows");

    engine
        .execute("DELETE FROM t WHERE id = 1")
        .expect("delete 1");
    let (pending, reclaimed) = engine.vacuum_stats();
    assert_eq!(pending, 1);
    assert_eq!(reclaimed, 0);

    engine
        .execute("DELETE FROM t WHERE id = 2")
        .expect("delete 2");
    let (pending_after, reclaimed_after) = engine.vacuum_stats();
    assert_eq!(pending_after, 0);
    assert_eq!(reclaimed_after, 2);
}

#[test]
fn vacuum_force_trigger_overrides_min_interval() {
    let engine = setup_engine();
    engine.set_vacuum_policy(VacuumPolicy {
        auto_trigger_deleted_versions: Some(10),
        force_trigger_deleted_versions: Some(2),
        min_interval_ms: 60_000,
        max_reclaims_per_run: None,
    });
    engine
        .execute("CREATE TABLE t (id INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO t VALUES (1), (2), (3)")
        .expect("seed rows");
    engine
        .execute("DELETE FROM t WHERE id = 1")
        .expect("delete 1");
    let (pending, reclaimed) = engine.vacuum_stats();
    assert_eq!(pending, 1);
    assert_eq!(reclaimed, 0);
    engine
        .execute("DELETE FROM t WHERE id = 2")
        .expect("delete 2");
    let (pending_after, reclaimed_after) = engine.vacuum_stats();
    assert_eq!(pending_after, 0);
    assert_eq!(reclaimed_after, 2);
}

#[test]
fn vacuum_max_reclaims_per_run_is_respected() {
    let engine = setup_engine();
    engine.set_vacuum_policy(VacuumPolicy {
        auto_trigger_deleted_versions: None,
        force_trigger_deleted_versions: None,
        min_interval_ms: 0,
        max_reclaims_per_run: Some(1),
    });
    engine
        .execute("CREATE TABLE t (id INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO t VALUES (1), (2), (3)")
        .expect("seed rows");
    engine
        .execute("DELETE FROM t WHERE id = 1")
        .expect("delete 1");
    engine
        .execute("DELETE FROM t WHERE id = 2")
        .expect("delete 2");
    let removed = engine.vacuum().expect("vacuum");
    assert_eq!(removed, 1);
}

#[test]
fn vacuum_min_interval_throttles_auto_runs() {
    let engine = setup_engine();
    engine.set_vacuum_policy(VacuumPolicy {
        auto_trigger_deleted_versions: Some(1),
        force_trigger_deleted_versions: None,
        min_interval_ms: 60_000,
        max_reclaims_per_run: None,
    });
    engine
        .execute("CREATE TABLE t (id INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO t VALUES (1), (2), (3)")
        .expect("seed rows");
    engine
        .execute("DELETE FROM t WHERE id = 1")
        .expect("delete 1");
    let (_, first_reclaimed) = engine.vacuum_stats();
    assert_eq!(first_reclaimed, 1);

    engine
        .execute("DELETE FROM t WHERE id = 2")
        .expect("delete 2");
    let (pending, second_reclaimed) = engine.vacuum_stats();
    assert_eq!(pending, 1);
    assert_eq!(second_reclaimed, 1);
}

#[test]
fn execute_in_txn_rejects_sql_transaction_control_statements() {
    let engine = setup_engine();
    let tx = engine.begin_txn();
    let err = engine
        .execute_in_txn(&tx, "COMMIT")
        .expect_err("sql txn control inside execute_in_txn should fail");
    assert!(err
        .to_string()
        .contains("transaction control statements are managed via QueryEngine API"));
    engine.abort_txn(tx);
}

#[test]
fn committed_delete_visibility_persists_across_engine_restart() {
    let db_path = unique_db_path("entdb-query-mvcc-restart-delete");
    {
        let engine = setup_engine_at_path(&db_path, false, true);
        engine
            .execute("CREATE TABLE t (id INT)")
            .expect("create table");
        engine
            .execute("INSERT INTO t VALUES (1), (2)")
            .expect("insert rows");
        engine
            .execute("DELETE FROM t WHERE id = 2")
            .expect("delete row");
        engine.flush_all().expect("flush before restart");
    }

    let reopened = setup_engine_at_path(&db_path, true, true);
    let out = reopened
        .execute("SELECT COUNT(*) FROM t")
        .expect("count after reopen");
    assert_eq!(extract_single_i64(&out), 1);
}

#[test]
fn concurrent_disjoint_updates_commit_without_losing_rows() {
    run_with_timeout(
        "concurrent_disjoint_updates_commit_without_losing_rows",
        Duration::from_secs(8),
        || {
            let engine = Arc::new(setup_engine());
            engine
                .execute("CREATE TABLE t (id INT, v INT)")
                .expect("create table");
            for id in 0..4_i32 {
                engine
                    .execute(&format!("INSERT INTO t VALUES ({id}, 0)"))
                    .expect("seed row");
            }

            let mut threads = Vec::new();
            for id in 0..4_i32 {
                let engine = Arc::clone(&engine);
                threads.push(thread::spawn(move || {
                    for _ in 0..4 {
                        let tx = engine.begin_txn();
                        engine
                            .execute_in_txn(&tx, &format!("UPDATE t SET v = v + 1 WHERE id = {id}"))
                            .expect("update in tx");
                        engine.commit_txn(tx).expect("commit tx");
                    }
                }));
            }
            for t in threads {
                t.join().expect("join thread");
            }

            let count = engine
                .execute("SELECT COUNT(*) FROM t")
                .expect("final count");
            assert_eq!(extract_single_i64(&count), 4);

            let rows = engine
                .execute("SELECT v FROM t ORDER BY id")
                .expect("final values");
            match &rows[0] {
                QueryOutput::Rows { rows, .. } => {
                    assert_eq!(rows.len(), 4);
                    for row in rows {
                        assert_eq!(row, &vec![Value::Int32(4)]);
                    }
                }
                _ => panic!("expected rows"),
            }
        },
    );
}

#[test]
fn concurrent_mixed_txn_matrix_preserves_row_count_and_visibility() {
    run_with_timeout(
        "concurrent_mixed_txn_matrix_preserves_row_count_and_visibility",
        Duration::from_secs(12),
        || {
            let engine = Arc::new(setup_engine());
            engine
                .execute("CREATE TABLE t (id INT, v INT)")
                .expect("create table");
            for id in 0..16_i32 {
                engine
                    .execute(&format!("INSERT INTO t VALUES ({id}, 0)"))
                    .expect("seed row");
            }

            let mut threads = Vec::new();
            for worker in 0..3_u64 {
                let engine = Arc::clone(&engine);
                threads.push(thread::spawn(move || {
                    let mut rng = StdRng::seed_from_u64(1000 + worker);
                    for _ in 0..10 {
                        let tx = engine.begin_txn();
                        let id = rng.gen_range(0..16_i32);
                        let _ = engine.execute_in_txn(
                            &tx,
                            &format!("UPDATE t SET v = v + 1 WHERE id = {id}"),
                        );
                        if rng.gen_bool(0.35) {
                            engine.abort_txn(tx);
                        } else {
                            let _ = engine.commit_txn(tx);
                        }
                    }
                }));
            }
            for t in threads {
                t.join().expect("join thread");
            }

            let count = engine
                .execute("SELECT COUNT(*) FROM t")
                .expect("count rows");
            assert_eq!(extract_single_i64(&count), 16);

            let out = engine
                .execute("SELECT v FROM t ORDER BY id")
                .expect("values");
            match &out[0] {
                QueryOutput::Rows { rows, .. } => {
                    assert_eq!(rows.len(), 16);
                    for row in rows {
                        match row.first() {
                            Some(Value::Int32(v)) => assert!(*v >= 0),
                            other => panic!("expected Int32 value, got {other:?}"),
                        }
                    }
                }
                _ => panic!("expected rows"),
            }
        },
    );
}

#[test]
fn stale_snapshot_update_on_recently_deleted_version_conflicts() {
    let engine = setup_engine();
    engine
        .execute("CREATE TABLE t (id INT, v INT)")
        .expect("create table");
    engine
        .execute("INSERT INTO t VALUES (1, 0)")
        .expect("seed row");

    let tx_old_snapshot = engine.begin_txn();

    let tx_delete = engine.begin_txn();
    engine
        .execute_in_txn(&tx_delete, "UPDATE t SET v = 1 WHERE id = 1")
        .expect("concurrent update");
    engine
        .commit_txn(tx_delete)
        .expect("commit concurrent update");

    let err = engine
        .execute_in_txn(&tx_old_snapshot, "UPDATE t SET v = 2 WHERE id = 1")
        .expect_err("stale-snapshot write should conflict");
    assert!(err.to_string().contains("write-write conflict"));
    engine.abort_txn(tx_old_snapshot);

    let out = engine
        .execute("SELECT COUNT(*) FROM t")
        .expect("final count");
    assert_eq!(extract_single_i64(&out), 1);
}

#[test]
fn concurrent_same_row_updates_preserve_single_visible_row() {
    run_with_timeout(
        "concurrent_same_row_updates_preserve_single_visible_row",
        Duration::from_secs(8),
        || {
            let engine = Arc::new(setup_engine());
            engine
                .execute("CREATE TABLE t (id INT, v INT)")
                .expect("create table");
            engine
                .execute("INSERT INTO t VALUES (1, 0)")
                .expect("seed row");

            let barrier = Arc::new(Barrier::new(2));
            let mut threads = Vec::new();
            for _ in 0..2 {
                let engine = Arc::clone(&engine);
                let barrier = Arc::clone(&barrier);
                threads.push(thread::spawn(move || {
                    barrier.wait();
                    let tx = engine.begin_txn();
                    let _ = engine.execute_in_txn(&tx, "UPDATE t SET v = v + 1 WHERE id = 1");
                    let _ = engine.commit_txn(tx);
                }));
            }
            for t in threads {
                t.join().expect("join thread");
            }

            let count = engine
                .execute("SELECT COUNT(*) FROM t")
                .expect("count rows");
            assert_eq!(extract_single_i64(&count), 1);
        },
    );
}
