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

use crate::catalog::{Catalog, IndexInfo, IndexType, TableInfo};
use crate::error::Result;
use crate::query::executor::{decode_stored_row, DecodedRow};
use crate::storage::bm25::{tuple_id_key, Bm25Index};
use crate::storage::table::Table;
use crate::storage::tuple::TupleId;
use crate::types::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

fn lock_registry() -> &'static Mutex<HashMap<String, Arc<Mutex<()>>>> {
    static REG: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(HashMap::new()))
}

fn index_lock(name: &str) -> Arc<Mutex<()>> {
    let mut reg = lock_registry().lock().expect("bm25 lock registry");
    reg.entry(name.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

pub fn rebuild_bm25_index(catalog: &Catalog, table: &TableInfo, index: &IndexInfo) -> Result<()> {
    let IndexType::Bm25 { text_config } = &index.index_type else {
        return Ok(());
    };
    if index.column_indices.len() != 1 {
        return Ok(());
    }
    let guard = index_lock(&index.name);
    let _guard = guard.lock().expect("bm25 index lock");

    let db_path = catalog.buffer_pool().disk_path().to_path_buf();
    let mut bm = Bm25Index::load_or_create(&db_path, &index.name, text_config)?;
    bm.clear();

    let table_store = Table::open(table.table_id, table.first_page_id, catalog.buffer_pool());
    let col_idx = index.column_indices[0];
    for (tid, tuple) in table_store.scan() {
        let version = match decode_stored_row(&tuple.data)? {
            DecodedRow::Legacy(values) => crate::query::executor::MvccRow {
                values,
                created_txn: 0,
                deleted_txn: None,
            },
            DecodedRow::Versioned(v) => v,
        };
        if version.deleted_txn.is_some() {
            continue;
        }
        if let Some(Value::Text(text)) = version.values.get(col_idx) {
            bm.index_document(tuple_id_key(tid), text);
        }
    }
    bm.persist(&db_path)
}

pub fn on_insert(catalog: &Catalog, table: &TableInfo, row: &[Value], tid: TupleId) -> Result<()> {
    let db_path = catalog.buffer_pool().disk_path().to_path_buf();
    for idx in &table.indexes {
        let IndexType::Bm25 { text_config } = &idx.index_type else {
            continue;
        };
        if idx.column_indices.len() != 1 {
            continue;
        }
        let guard = index_lock(&idx.name);
        let _guard = guard.lock().expect("bm25 index lock");
        let col_idx = idx.column_indices[0];
        let Some(Value::Text(text)) = row.get(col_idx) else {
            continue;
        };
        let mut bm = Bm25Index::load_or_create(&db_path, &idx.name, text_config)?;
        bm.index_document(tuple_id_key(tid), text);
        bm.persist(&db_path)?;
    }
    Ok(())
}

pub fn on_delete(catalog: &Catalog, table: &TableInfo, tid: TupleId) -> Result<()> {
    let db_path = catalog.buffer_pool().disk_path().to_path_buf();
    for idx in &table.indexes {
        let IndexType::Bm25 { text_config } = &idx.index_type else {
            continue;
        };
        let guard = index_lock(&idx.name);
        let _guard = guard.lock().expect("bm25 index lock");
        let mut bm = Bm25Index::load_or_create(&db_path, &idx.name, text_config)?;
        bm.remove_document(&tuple_id_key(tid));
        bm.persist(&db_path)?;
    }
    Ok(())
}
