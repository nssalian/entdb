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
use crate::query::{QueryEngine, QueryOutput};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::disk_manager::DiskManager;
use crate::types::Value;
use proptest::prelude::*;
use std::sync::Arc;

fn setup_engine() -> QueryEngine {
    let unique = format!(
        "entdb-query-prop-{}-{}.db",
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn order_by_numeric_produces_sorted_output(values in proptest::collection::vec(any::<i16>(), 1..40)) {
        let engine = setup_engine();
        engine.execute("CREATE TABLE t (id INT, age INT)").expect("create table");

        let mut id = 1_i32;
        for v in &values {
            let sql = format!("INSERT INTO t VALUES ({id}, {v})");
            engine.execute(&sql).expect("insert");
            id += 1;
        }

        let out = engine.execute("SELECT age FROM t ORDER BY age ASC").expect("select asc");
        let rows = match &out[0] {
            QueryOutput::Rows { rows, .. } => rows,
            _ => panic!("expected row output"),
        };

        let mut actual = Vec::new();
        for row in rows {
            match &row[0] {
                Value::Int32(v) => actual.push(*v),
                other => panic!("unexpected value type: {other:?}"),
            }
        }

        let mut expected = values.iter().map(|v| *v as i32).collect::<Vec<_>>();
        expected.sort();
        prop_assert_eq!(actual, expected);
    }
}
