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

use crate::catalog::{Catalog, Column, Schema};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::disk_manager::DiskManager;
use crate::types::DataType;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn catalog_create_index_persist_reload() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("cat.db");

    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(16, Arc::clone(&dm)));
    let catalog = Catalog::init(Arc::clone(&bp)).expect("init catalog");

    let schema = Schema::new(vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            default: None,
            primary_key: true,
        },
        Column {
            name: "name".to_string(),
            data_type: DataType::Text,
            nullable: false,
            default: None,
            primary_key: false,
        },
    ]);

    let users = catalog
        .create_table("users", schema.clone())
        .expect("create users table");
    assert_eq!(users.name, "users");

    let idx = catalog
        .create_index("users", "idx_users_id", &["id".to_string()], true)
        .expect("create index");
    assert_eq!(idx.name, "idx_users_id");
    assert_eq!(idx.column_indices, vec![0]);

    drop(catalog);
    drop(bp);
    drop(dm);

    let dm2 = Arc::new(DiskManager::new(&db_path).expect("reopen disk manager"));
    let bp2 = Arc::new(BufferPool::new(16, Arc::clone(&dm2)));
    let loaded = Catalog::load(Arc::clone(&bp2)).expect("load catalog");

    let table = loaded.get_table("users").expect("table exists");
    assert_eq!(table.schema, schema);
    assert_eq!(table.indexes.len(), 1);
    assert_eq!(table.indexes[0].name, "idx_users_id");
}

#[test]
fn catalog_drop_and_list_tables() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("cat2.db");

    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(16, Arc::clone(&dm)));
    let catalog = Catalog::init(Arc::clone(&bp)).expect("init catalog");

    let schema = Schema::new(vec![Column {
        name: "id".to_string(),
        data_type: DataType::Int64,
        nullable: false,
        default: None,
        primary_key: true,
    }]);

    catalog.create_table("a", schema.clone()).expect("create a");
    catalog.create_table("b", schema.clone()).expect("create b");

    let listed = catalog.list_tables();
    assert_eq!(listed.len(), 2);

    catalog.drop_table("a").expect("drop a");
    assert!(catalog.get_table("a").is_none());
    assert!(catalog.get_table("b").is_some());
}

#[test]
fn catalog_drop_index_and_alter_schema_ops() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("cat3.db");

    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(16, Arc::clone(&dm)));
    let catalog = Catalog::init(Arc::clone(&bp)).expect("init catalog");

    let schema = Schema::new(vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            default: None,
            primary_key: true,
        },
        Column {
            name: "name".to_string(),
            data_type: DataType::Text,
            nullable: true,
            default: None,
            primary_key: false,
        },
    ]);

    catalog
        .create_table("users", schema)
        .expect("create users table");
    catalog
        .create_index("users", "idx_users_id", &["id".to_string()], true)
        .expect("create index");
    assert!(catalog.drop_index("idx_users_id").expect("drop index"));

    let changed = catalog
        .add_column(
            "users",
            Column {
                name: "age".to_string(),
                data_type: DataType::Int32,
                nullable: true,
                default: None,
                primary_key: false,
            },
            false,
        )
        .expect("add column");
    assert!(changed);
    catalog
        .rename_column("users", "name", "full_name")
        .expect("rename column");
    let dropped = catalog
        .drop_column("users", "age", false)
        .expect("drop column");
    assert_eq!(dropped, Some(2));

    catalog
        .rename_table("users", "users_v2")
        .expect("rename table");
    assert!(catalog.get_table("users").is_none());
    assert!(catalog.get_table("users_v2").is_some());
}

#[test]
fn catalog_stats_persist_and_stale_marking_round_trip() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("cat4.db");

    let dm = Arc::new(DiskManager::new(&db_path).expect("disk manager"));
    let bp = Arc::new(BufferPool::new(16, Arc::clone(&dm)));
    let catalog = Catalog::init(Arc::clone(&bp)).expect("init catalog");

    let schema = Schema::new(vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            default: None,
            primary_key: true,
        },
        Column {
            name: "name".to_string(),
            data_type: DataType::Text,
            nullable: true,
            default: None,
            primary_key: false,
        },
    ]);

    catalog
        .create_table("users", schema)
        .expect("create users table");
    catalog
        .update_table_stats(
            "users",
            5,
            vec![
                crate::catalog::ColumnStats {
                    name: "id".to_string(),
                    ndv: Some(5),
                    null_fraction: 0.0,
                    min: Some(crate::types::Value::Int32(1)),
                    max: Some(crate::types::Value::Int32(5)),
                },
                crate::catalog::ColumnStats {
                    name: "name".to_string(),
                    ndv: Some(5),
                    null_fraction: 0.2,
                    min: Some(crate::types::Value::Text("a".to_string())),
                    max: Some(crate::types::Value::Text("z".to_string())),
                },
            ],
            1234,
        )
        .expect("update stats");
    catalog
        .mark_table_stats_stale("users", 2)
        .expect("mark stale");

    drop(catalog);
    drop(bp);
    drop(dm);

    let dm2 = Arc::new(DiskManager::new(&db_path).expect("reopen disk manager"));
    let bp2 = Arc::new(BufferPool::new(16, Arc::clone(&dm2)));
    let loaded = Catalog::load(Arc::clone(&bp2)).expect("load catalog");
    let users = loaded.get_table("users").expect("users table exists");
    assert_eq!(users.stats.row_count, 5);
    assert_eq!(users.stats.last_analyzed_at_ms, Some(1234));
    assert_eq!(users.stats.columns.len(), 2);
    assert!(users.stats.stale);
    assert_eq!(users.stats.mutation_count_since_analyze, 2);
}
