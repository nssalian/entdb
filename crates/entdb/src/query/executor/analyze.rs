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

use crate::catalog::{ColumnStats, Schema, TableInfo};
use crate::error::{EntDbError, Result};
use crate::query::executor::{
    decode_stored_row, row_visible, DecodedRow, ExecutionContext, Executor,
};
use crate::storage::table::Table;
use crate::types::Value;
use std::collections::HashSet;

struct ColumnState {
    nulls: u64,
    non_nulls: u64,
    distinct: HashSet<Vec<u8>>,
    min: Option<Value>,
    max: Option<Value>,
}

impl ColumnState {
    fn new() -> Self {
        Self {
            nulls: 0,
            non_nulls: 0,
            distinct: HashSet::new(),
            min: None,
            max: None,
        }
    }
}

pub struct AnalyzeExecutor {
    table: TableInfo,
    ctx: ExecutionContext,
    affected_rows: u64,
}

impl AnalyzeExecutor {
    pub fn new(table: TableInfo, ctx: &ExecutionContext) -> Self {
        Self {
            table,
            ctx: ExecutionContext {
                catalog: std::sync::Arc::clone(&ctx.catalog),
                tx: ctx.tx.clone(),
            },
            affected_rows: 0,
        }
    }
}

impl Executor for AnalyzeExecutor {
    fn open(&mut self) -> Result<()> {
        let table = Table::open(
            self.table.table_id,
            self.table.first_page_id,
            self.ctx.catalog.buffer_pool(),
        );
        let mut row_count = 0_u64;
        let mut states: Vec<ColumnState> = (0..self.table.schema.columns.len())
            .map(|_| ColumnState::new())
            .collect();

        for (_, tuple) in table.scan() {
            let stored = decode_stored_row(&tuple.data)?;
            let row = match stored {
                DecodedRow::Legacy(r) => r,
                DecodedRow::Versioned(v) => {
                    if !row_visible(&v, self.ctx.tx.as_ref()) {
                        continue;
                    }
                    v.values
                }
            };
            row_count = row_count.saturating_add(1);
            for (idx, val) in row.into_iter().enumerate() {
                let state = states.get_mut(idx).ok_or_else(|| {
                    EntDbError::Query("row width exceeded table schema while analyzing".to_string())
                })?;
                match val {
                    Value::Null => state.nulls = state.nulls.saturating_add(1),
                    v => {
                        state.non_nulls = state.non_nulls.saturating_add(1);
                        state.distinct.insert(v.serialize());
                        if let Some(current) = &state.min {
                            if v.lt(current)? {
                                state.min = Some(v.clone());
                            }
                        } else {
                            state.min = Some(v.clone());
                        }
                        if let Some(current) = &state.max {
                            if v.gt(current)? {
                                state.max = Some(v.clone());
                            }
                        } else {
                            state.max = Some(v.clone());
                        }
                    }
                }
            }
        }

        let mut column_stats = Vec::with_capacity(self.table.schema.columns.len());
        for (col, state) in self.table.schema.columns.iter().zip(states.into_iter()) {
            let total = state.nulls.saturating_add(state.non_nulls);
            let null_fraction = if total == 0 {
                0.0
            } else {
                state.nulls as f64 / total as f64
            };
            column_stats.push(ColumnStats {
                name: col.name.clone(),
                ndv: Some(state.distinct.len() as u64),
                null_fraction,
                min: state.min,
                max: state.max,
            });
        }

        let analyzed_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        self.ctx.catalog.update_table_stats(
            &self.table.name,
            row_count,
            column_stats,
            analyzed_at_ms,
        )?;
        self.affected_rows = 1;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        Ok(None)
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        static EMPTY_SCHEMA: std::sync::LazyLock<Schema> = std::sync::LazyLock::new(|| Schema {
            columns: Vec::new(),
        });
        &EMPTY_SCHEMA
    }

    fn affected_rows(&self) -> Option<u64> {
        Some(self.affected_rows)
    }
}
