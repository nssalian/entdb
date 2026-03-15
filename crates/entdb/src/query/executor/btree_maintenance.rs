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
use crate::query::executor::{
    decode_stored_row, row_visible, DecodedRow, MvccRow, TxExecutionContext,
};
use crate::storage::btree::{BTree, KeySchema};
use crate::storage::table::Table;
use crate::storage::tuple::TupleId;
use crate::types::Value;

pub fn rebuild_btree_index(catalog: &Catalog, table: &TableInfo, index: &IndexInfo) -> Result<()> {
    rebuild_btree_index_with_tx(catalog, table, index, None)
}

pub fn rebuild_btree_index_with_tx(
    catalog: &Catalog,
    table: &TableInfo,
    index: &IndexInfo,
    tx: Option<&TxExecutionContext>,
) -> Result<()> {
    let Some(col_idx) = indexed_column(index) else {
        return Ok(());
    };

    let tree = BTree::open(index.root_page_id, catalog.buffer_pool(), KeySchema);
    let table_store = Table::open(table.table_id, table.first_page_id, catalog.buffer_pool());

    for (tid, tuple) in table_store.scan() {
        let version = match decode_stored_row(&tuple.data)? {
            DecodedRow::Legacy(values) => MvccRow {
                values,
                created_txn: 0,
                deleted_txn: None,
            },
            DecodedRow::Versioned(v) => v,
        };
        if !row_visible(&version, tx) {
            continue;
        }
        if let Some(key) = index_key_from_row(&version.values, col_idx) {
            tree.insert(&key, tid)?;
        }
    }

    Ok(())
}

pub fn on_insert(catalog: &Catalog, table: &TableInfo, row: &[Value], tid: TupleId) -> Result<()> {
    for index in table
        .indexes
        .iter()
        .filter_map(as_single_column_btree_index)
    {
        let Some(key) = index_key_from_row(row, index.column_indices[0]) else {
            continue;
        };
        let tree = BTree::open(index.root_page_id, catalog.buffer_pool(), KeySchema);
        tree.insert(&key, tid)?;
    }
    Ok(())
}

pub fn on_delete(catalog: &Catalog, table: &TableInfo, row: &[Value]) -> Result<()> {
    for index in table
        .indexes
        .iter()
        .filter_map(as_single_column_btree_index)
    {
        let Some(key) = index_key_from_row(row, index.column_indices[0]) else {
            continue;
        };
        let tree = BTree::open(index.root_page_id, catalog.buffer_pool(), KeySchema);
        tree.delete(&key)?;
    }
    Ok(())
}

pub fn lookup_visible_rows(
    catalog: &Catalog,
    table: &TableInfo,
    col_idx: usize,
    value: &Value,
    tx: Option<&TxExecutionContext>,
) -> Result<Option<Vec<(TupleId, MvccRow, Vec<u8>)>>> {
    let Some(index) = find_btree_index(table, col_idx) else {
        return Ok(None);
    };

    let tree = BTree::open(index.root_page_id, catalog.buffer_pool(), KeySchema);
    let column_type = &table.schema.columns[col_idx].data_type;
    let key = value.cast_to(column_type)?.serialize();
    let table_store = Table::open(table.table_id, table.first_page_id, catalog.buffer_pool());
    let mut matches = Vec::new();

    for (_key, tid) in tree.range_scan(Some(&key), Some(&key))? {
        let tuple = table_store.get(tid)?;
        let version = match decode_stored_row(&tuple.data)? {
            DecodedRow::Legacy(values) => MvccRow {
                values,
                created_txn: 0,
                deleted_txn: None,
            },
            DecodedRow::Versioned(v) => v,
        };
        if !row_visible(&version, tx) {
            continue;
        }
        let Some(actual) = version.values.get(col_idx) else {
            continue;
        };
        if !actual.eq(value)? {
            continue;
        }
        matches.push((tid, version, tuple.data));
    }

    Ok(Some(matches))
}

pub fn has_btree_index_on_column(table: &TableInfo, col_idx: usize) -> bool {
    find_btree_index(table, col_idx).is_some()
}

fn indexed_column(index: &IndexInfo) -> Option<usize> {
    as_single_column_btree_index(index).map(|idx| idx.column_indices[0])
}

fn as_single_column_btree_index(index: &IndexInfo) -> Option<&IndexInfo> {
    if matches!(index.index_type, IndexType::BTree) && index.column_indices.len() == 1 {
        Some(index)
    } else {
        None
    }
}

fn find_btree_index(table: &TableInfo, col_idx: usize) -> Option<&IndexInfo> {
    table.indexes.iter().find(|index| {
        matches!(index.index_type, IndexType::BTree)
            && index.column_indices.len() == 1
            && index.column_indices[0] == col_idx
    })
}

fn index_key_from_row(row: &[Value], col_idx: usize) -> Option<Vec<u8>> {
    row.get(col_idx).map(Value::serialize)
}
