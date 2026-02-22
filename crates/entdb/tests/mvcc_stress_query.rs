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
use entdb::query::QueryEngine;
use entdb::storage::buffer_pool::BufferPool;
use entdb::storage::disk_manager::DiskManager;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn setup_engine() -> QueryEngine {
    let unique = format!(
        "entdb-mvcc-stress-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    );
    let db_path = std::env::temp_dir().join(unique);
    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(256, Arc::clone(&dm)));
    let catalog = Arc::new(Catalog::init(Arc::clone(&bp)).expect("catalog init"));
    QueryEngine::new(catalog)
}

#[test]
#[ignore = "long-running stress test"]
fn mvcc_query_stress_abort_storm_with_predicates_and_join_reads() {
    let engine = Arc::new(setup_engine());
    engine
        .execute("CREATE TABLE users (id INT, age INT)")
        .expect("create users");
    engine
        .execute("CREATE TABLE scores (user_id INT, score INT)")
        .expect("create scores");

    for id in 0..200_i32 {
        engine
            .execute(&format!(
                "INSERT INTO users VALUES ({id}, {})",
                18 + (id % 50)
            ))
            .expect("insert user");
        engine
            .execute(&format!("INSERT INTO scores VALUES ({id}, {})", id * 10))
            .expect("insert score");
    }

    let deadline = Instant::now() + Duration::from_secs(read_env_u64("ENTDB_MVCC_STRESS_SECS", 20));
    let mut handles = Vec::new();

    for w in 0..8_u64 {
        let engine = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(0xD00D0000 + w);
            while Instant::now() < deadline {
                let id = rng.gen_range(0..200_i32);
                if rng.gen_bool(0.55) {
                    engine.execute("BEGIN").expect("begin");
                    let _ = engine.execute(&format!(
                        "UPDATE users SET age = age + 1 WHERE id = {id} AND (age * 2) > 20"
                    ));
                    let _ = engine.execute(&format!(
                        "UPDATE scores SET score = score + 3 WHERE user_id = {id}"
                    ));
                    engine.execute("ROLLBACK").expect("rollback");
                } else {
                    engine.execute("BEGIN").expect("begin");
                    let _ = engine.execute(&format!(
                        "UPDATE users SET age = age + 1 WHERE id = {id} AND (age / 2) >= 5"
                    ));
                    let _ = engine.execute(&format!(
                        "UPDATE scores SET score = score + 1 WHERE user_id = {id}"
                    ));
                    let _ = engine.execute(
                        "SELECT COUNT(*) FROM users INNER JOIN scores ON users.id = scores.user_id WHERE users.age > 20",
                    );
                    engine.execute("COMMIT").expect("commit");
                }
            }
        }));
    }

    for h in handles {
        h.join().expect("join worker");
    }

    let out = engine
        .execute(
            "SELECT COUNT(*) FROM users INNER JOIN scores ON users.id = scores.user_id WHERE users.id >= 0",
        )
        .expect("final count");
    assert!(!out.is_empty());
}

fn read_env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}
