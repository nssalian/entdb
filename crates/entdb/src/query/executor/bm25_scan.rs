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

use crate::catalog::{Catalog, Schema, TableInfo};
use crate::error::Result;
use crate::query::executor::{
    decode_stored_row, row_visible, DecodedRow, Executor, TxExecutionContext,
};
use crate::storage::bm25::{tuple_id_key, Bm25Index};
use crate::storage::table::Table;
use crate::types::Value;
use std::sync::Arc;

pub struct Bm25ScanExecutor {
    table_info: TableInfo,
    index_name: String,
    terms: Vec<String>,
    output_schema: Schema,
    catalog: Arc<Catalog>,
    tx: Option<TxExecutionContext>,
    rows: Vec<Vec<Value>>,
    pos: usize,
}

impl Bm25ScanExecutor {
    pub fn new(
        table_info: TableInfo,
        index_name: String,
        terms: Vec<String>,
        output_schema: Schema,
        catalog: Arc<Catalog>,
        tx: Option<TxExecutionContext>,
    ) -> Self {
        Self {
            table_info,
            index_name,
            terms,
            output_schema,
            catalog,
            tx,
            rows: Vec::new(),
            pos: 0,
        }
    }
}

impl Executor for Bm25ScanExecutor {
    fn open(&mut self) -> Result<()> {
        self.rows.clear();
        self.pos = 0;

        let db_path = self.catalog.buffer_pool().disk_path().to_path_buf();
        let bm25 = Bm25Index::load(&db_path, &self.index_name)?;
        let scores = bm25.score_all(&self.terms);

        let table = Table::open(
            self.table_info.table_id,
            self.table_info.first_page_id,
            self.catalog.buffer_pool(),
        );
        for (tid, tuple) in table.scan() {
            match decode_stored_row(&tuple.data)? {
                DecodedRow::Legacy(mut row) => {
                    let score = scores.get(&tuple_id_key(tid)).copied().unwrap_or(0.0);
                    row.push(Value::Float64(score));
                    self.rows.push(row);
                }
                DecodedRow::Versioned(version) => {
                    if row_visible(&version, self.tx.as_ref()) {
                        let mut row = version.values;
                        let score = scores.get(&tuple_id_key(tid)).copied().unwrap_or(0.0);
                        row.push(Value::Float64(score));
                        self.rows.push(row);
                    }
                }
            }
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        if self.pos >= self.rows.len() {
            return Ok(None);
        }
        let out = self.rows[self.pos].clone();
        self.pos += 1;
        Ok(Some(out))
    }

    fn close(&mut self) -> Result<()> {
        self.rows.clear();
        self.pos = 0;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }
}
