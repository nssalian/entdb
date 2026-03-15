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
use crate::storage::table::Table;
use crate::types::Value;
use std::sync::Arc;

pub struct SeqScanExecutor {
    table_info: TableInfo,
    catalog: Arc<Catalog>,
    iter: Option<crate::storage::table::TableIterator>,
    tx: Option<TxExecutionContext>,
    eq_filter: Option<(usize, Value)>,
    stop_after_first_match: bool,
    seen_match: bool,
}

impl SeqScanExecutor {
    pub fn new(
        table_info: TableInfo,
        catalog: Arc<Catalog>,
        tx: Option<TxExecutionContext>,
    ) -> Self {
        Self::new_with_eq_filter(table_info, catalog, tx, None, false)
    }

    pub fn new_with_eq_filter(
        table_info: TableInfo,
        catalog: Arc<Catalog>,
        tx: Option<TxExecutionContext>,
        eq_filter: Option<(usize, Value)>,
        stop_after_first_match: bool,
    ) -> Self {
        Self {
            table_info,
            catalog,
            iter: None,
            tx,
            eq_filter,
            stop_after_first_match,
            seen_match: false,
        }
    }
}

impl Executor for SeqScanExecutor {
    fn open(&mut self) -> Result<()> {
        let table = Table::open(
            self.table_info.table_id,
            self.table_info.first_page_id,
            self.catalog.buffer_pool(),
        );
        self.iter = Some(table.scan());
        self.seen_match = false;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        let Some(iter) = self.iter.as_mut() else {
            return Ok(None);
        };
        if self.stop_after_first_match && self.seen_match {
            return Ok(None);
        }

        loop {
            match iter.next() {
                Some((_tid, tuple)) => match decode_stored_row(&tuple.data)? {
                    DecodedRow::Legacy(row) => {
                        if !matches_eq_filter(&self.eq_filter, &row)? {
                            continue;
                        }
                        self.seen_match = true;
                        return Ok(Some(row));
                    }
                    DecodedRow::Versioned(version) => {
                        if row_visible(&version, self.tx.as_ref()) {
                            if !matches_eq_filter(&self.eq_filter, &version.values)? {
                                continue;
                            }
                            self.seen_match = true;
                            return Ok(Some(version.values));
                        }
                    }
                },
                None => return Ok(None),
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.iter = None;
        self.seen_match = false;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.table_info.schema
    }
}

fn matches_eq_filter(filter: &Option<(usize, Value)>, row: &[Value]) -> Result<bool> {
    let Some((idx, needle)) = filter else {
        return Ok(true);
    };
    let Some(actual) = row.get(*idx) else {
        return Ok(false);
    };
    actual.eq(needle)
}
