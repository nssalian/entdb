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
use entdb::query::optimizer::OptimizerConfig;
use entdb::query::polyglot::{transpile, PolyglotOptions};
use entdb::query::QueryEngine;
use entdb::storage::btree::{BTree, KeySchema};
use entdb::storage::buffer_pool::BufferPool;
use entdb::storage::disk_manager::DiskManager;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[test]
#[ignore = "perf regression test"]
fn perf_btree_insert_and_lookup_budget() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("perf_btree.db");

    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(256, Arc::clone(&dm)));
    let tree = BTree::create_with_order(Arc::clone(&bp), KeySchema, 8).expect("create btree");

    let n = 20_000_u32;

    let start_insert = Instant::now();
    for k in 0..n {
        tree.insert(&k.to_be_bytes(), (k, 0)).expect("insert");
    }
    let insert_elapsed = start_insert.elapsed();

    let start_lookup = Instant::now();
    for k in 0..n {
        let got = tree.search(&k.to_be_bytes()).expect("search");
        assert_eq!(got, Some((k, 0)));
    }
    let lookup_elapsed = start_lookup.elapsed();

    // Conservative defaults; tunable per environment via env vars.
    let insert_budget = budget_secs("ENTDB_PERF_BTREE_INSERT_MAX_SECS", 16);
    let lookup_budget = budget_secs("ENTDB_PERF_BTREE_LOOKUP_MAX_SECS", 6);
    assert!(
        insert_elapsed < insert_budget,
        "insert regression: {:?}",
        insert_elapsed
    );
    assert!(
        lookup_elapsed < lookup_budget,
        "lookup regression: {:?}",
        lookup_elapsed
    );
}

#[test]
#[ignore = "perf regression test"]
fn perf_buffer_pool_fetch_budget() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("perf_bp.db");

    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(128, Arc::clone(&dm)));

    let mut ids = Vec::new();
    for i in 0..1024_u32 {
        let mut p = bp.new_page().expect("new page");
        p.body_mut()[0..4].copy_from_slice(&i.to_le_bytes());
        ids.push(p.page_id());
    }

    let start = Instant::now();
    for i in 0..100_000_usize {
        let id = ids[i % ids.len()];
        let g = bp.fetch_page(id).expect("fetch");
        assert_eq!(g.page_id(), id);
    }
    let elapsed = start.elapsed();
    let budget = budget_secs("ENTDB_PERF_BP_FETCH_MAX_SECS", 6);

    assert!(
        elapsed < budget,
        "buffer-pool fetch regression: {:?}",
        elapsed
    );
}

#[test]
#[ignore = "perf regression test"]
fn perf_query_join_order_count_budget() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("perf_query.db");
    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(256, Arc::clone(&dm)));
    let catalog = Arc::new(Catalog::init(Arc::clone(&bp)).expect("catalog init"));
    let engine = QueryEngine::new(catalog);

    engine
        .execute("CREATE TABLE users (id INT, age INT)")
        .expect("create users");
    engine
        .execute("CREATE TABLE orders (id INT, user_id INT)")
        .expect("create orders");

    for i in 0..2000_u32 {
        let sql = format!(
            "INSERT INTO users VALUES ({}, {}); INSERT INTO orders VALUES ({}, {})",
            i,
            (i % 80),
            i,
            i
        );
        engine.execute(&sql).expect("insert row pair");
    }

    let start = Instant::now();
    let out = engine
        .execute(
            "SELECT COUNT(*) FROM users INNER JOIN orders ON users.id = orders.user_id WHERE users.age > 20 ORDER BY users.id",
        )
        .expect("query perf");
    let elapsed = start.elapsed();
    assert_eq!(out.len(), 1);

    let budget = budget_secs("ENTDB_PERF_QUERY_JOIN_ORDER_COUNT_MAX_SECS", 4);
    assert!(elapsed < budget, "query perf regression: {:?}", elapsed);
}

#[test]
#[ignore = "perf regression test"]
fn perf_polyglot_transpile_budget() {
    let start = Instant::now();
    for i in 0..50_000_u32 {
        let sql = format!(
            "SELECT `id` FROM `users` WHERE `id` > {} ORDER BY `id` LIMIT 1, 20",
            i % 100
        );
        let out = transpile(&sql, PolyglotOptions { enabled: true }).expect("transpile");
        assert!(out.contains("OFFSET"));
    }
    let elapsed = start.elapsed();
    let budget = budget_secs("ENTDB_PERF_POLYGLOT_TRANSPILE_MAX_SECS", 2);
    assert!(
        elapsed < budget,
        "polyglot transpile regression: {:?}",
        elapsed
    );
}

fn budget_secs(env_key: &str, default_secs: u64) -> Duration {
    std::env::var(env_key)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(default_secs))
}

fn budget_millis(env_key: &str, default_ms: u64) -> Duration {
    std::env::var(env_key)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(default_ms))
}

#[test]
#[ignore = "perf regression test"]
fn perf_optimizer_planning_latency_budget() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("perf_optimizer_plan.db");
    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(256, Arc::clone(&dm)));
    let catalog = Arc::new(Catalog::init(Arc::clone(&bp)).expect("catalog init"));
    let engine = QueryEngine::new(catalog);
    engine.set_optimizer_config(OptimizerConfig {
        cbo_enabled: true,
        hbo_enabled: true,
        max_search_ms: 200,
        max_join_relations: 8,
    });

    engine
        .execute("CREATE TABLE a (id INT, v INT)")
        .expect("create a");
    engine
        .execute("CREATE TABLE b (id INT, v INT)")
        .expect("create b");
    engine
        .execute("CREATE TABLE c (id INT, v INT)")
        .expect("create c");
    for i in 0..500_u32 {
        let sql = format!(
            "INSERT INTO a VALUES ({0}, {1}); \
             INSERT INTO b VALUES ({0}, {2}); \
             INSERT INTO c VALUES ({0}, {3});",
            i,
            i % 50,
            (i + 1) % 50,
            (i + 2) % 50
        );
        engine.execute(&sql).expect("seed row");
    }

    let query = "SELECT a.id FROM a \
                 INNER JOIN b ON a.id = b.id \
                 INNER JOIN c ON b.id = c.id \
                 WHERE a.v > 10 \
                 ORDER BY a.id LIMIT 20";

    let loops = 200;
    let start = Instant::now();
    for _ in 0..loops {
        let traces = engine.explain_optimizer_trace(query).expect("trace");
        assert_eq!(traces.len(), 1);
    }
    let elapsed = start.elapsed();
    let budget = budget_millis("ENTDB_PERF_OPTIMIZER_PLAN_MAX_MS", 2_000);
    assert!(
        elapsed < budget,
        "optimizer planning latency regression: {:?} for {} loops",
        elapsed,
        loops
    );
}

#[test]
#[ignore = "perf regression test"]
fn perf_optimizer_repeated_query_hbo_signal_budget() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("perf_optimizer_hbo.db");
    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(256, Arc::clone(&dm)));
    let catalog = Arc::new(Catalog::init(Arc::clone(&bp)).expect("catalog init"));
    let engine = QueryEngine::new(catalog);
    engine.set_optimizer_config(OptimizerConfig {
        cbo_enabled: true,
        hbo_enabled: true,
        max_search_ms: 200,
        max_join_relations: 8,
    });

    engine
        .execute("CREATE TABLE t (id INT, v INT)")
        .expect("create");
    for i in 0..3000_u32 {
        let sql = format!("INSERT INTO t VALUES ({}, {})", i, i % 100);
        engine.execute(&sql).expect("insert");
    }

    let q = "SELECT id FROM t WHERE v > 10 ORDER BY id LIMIT 100";
    engine.execute(q).expect("first run");
    std::thread::sleep(Duration::from_millis(100));
    let first = engine.last_optimizer_trace().expect("first trace");
    engine.execute(q).expect("second run");
    std::thread::sleep(Duration::from_millis(100));
    let second = engine.last_optimizer_trace().expect("second trace");

    assert!(
        second.history_matches >= first.history_matches,
        "expected HBO history signal to be non-decreasing"
    );
    let budget = budget_millis("ENTDB_PERF_OPTIMIZER_REPEATED_MAX_MS", 2_000);
    let start = Instant::now();
    for _ in 0..50 {
        engine.execute(q).expect("repeated run");
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < budget,
        "repeated-query HBO perf regression: {:?}",
        elapsed
    );
}

#[test]
#[ignore = "perf regression test"]
fn perf_optimizer_cold_start_no_regression_budget() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("perf_optimizer_cold.db");
    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(256, Arc::clone(&dm)));
    let catalog = Arc::new(Catalog::init(Arc::clone(&bp)).expect("catalog init"));
    let engine = QueryEngine::new(catalog);

    engine
        .execute("CREATE TABLE t (id INT, v INT)")
        .expect("create");
    for i in 0..1000_u32 {
        let sql = format!("INSERT INTO t VALUES ({}, {})", i, i % 100);
        engine.execute(&sql).expect("insert");
    }
    let q = "SELECT id FROM t WHERE v > 5 ORDER BY id LIMIT 200";

    engine.set_optimizer_config(OptimizerConfig {
        cbo_enabled: true,
        hbo_enabled: false,
        max_search_ms: 100,
        max_join_relations: 8,
    });
    let baseline_start = Instant::now();
    for _ in 0..50 {
        let out = engine.execute(q).expect("baseline query");
        assert_eq!(out.len(), 1);
    }
    let baseline = baseline_start.elapsed();

    engine.set_optimizer_config(OptimizerConfig {
        cbo_enabled: true,
        hbo_enabled: true,
        max_search_ms: 100,
        max_join_relations: 8,
    });
    let cold_start = Instant::now();
    for _ in 0..50 {
        let out = engine.execute(q).expect("cold-start query");
        assert_eq!(out.len(), 1);
    }
    let hbo_cold = cold_start.elapsed();

    let max_slowdown = std::env::var("ENTDB_PERF_OPTIMIZER_COLD_START_MAX_SLOWDOWN_PCT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(30) as f64;
    let max_overhead_ms = std::env::var("ENTDB_PERF_OPTIMIZER_COLD_START_MAX_OVERHEAD_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1500) as f64;
    let allowed = (baseline.as_secs_f64() * (1.0 + max_slowdown / 100.0))
        .max(baseline.as_secs_f64() + max_overhead_ms / 1000.0);
    assert!(
        hbo_cold.as_secs_f64() <= allowed,
        "cold-start regression: baseline={:?} hbo_cold={:?} allowed={:.4}s max_slowdown={} max_overhead_ms={}",
        baseline,
        hbo_cold,
        allowed,
        max_slowdown,
        max_overhead_ms
    );
}
