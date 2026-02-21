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

#![no_main]

use entdb::catalog::{Catalog, Column, Schema};
use entdb::query::binder::Binder;
use entdb::storage::buffer_pool::BufferPool;
use entdb::storage::disk_manager::DiskManager;
use entdb::types::DataType;
use libfuzzer_sys::fuzz_target;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::sync::{Arc, OnceLock};

fn fuzz_catalog() -> Arc<Catalog> {
    static CATALOG: OnceLock<Arc<Catalog>> = OnceLock::new();
    CATALOG
        .get_or_init(|| {
            let path = std::env::temp_dir().join("entdb_fuzz_query_bind.db");
            let dm = Arc::new(DiskManager::new(&path).expect("disk manager"));
            let bp = Arc::new(BufferPool::new(16, Arc::clone(&dm)));
            let catalog = Arc::new(Catalog::load(Arc::clone(&bp)).expect("catalog load"));
            if catalog.get_table("t").is_none() {
                let _ = catalog.create_table(
                    "t",
                    Schema::new(vec![
                        Column {
                            name: "a".to_string(),
                            data_type: DataType::Int32,
                            nullable: true,
                            default: None,
                            primary_key: false,
                        },
                        Column {
                            name: "b".to_string(),
                            data_type: DataType::Text,
                            nullable: true,
                            default: None,
                            primary_key: false,
                        },
                    ]),
                );
            }
            catalog
        })
        .clone()
}

fuzz_target!(|data: &[u8]| {
    let Ok(sql) = std::str::from_utf8(data) else {
        return;
    };

    let dialect = PostgreSqlDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
        return;
    };

    let binder = Binder::new(fuzz_catalog());
    for stmt in &statements {
        let _ = binder.bind(stmt);
    }
});
