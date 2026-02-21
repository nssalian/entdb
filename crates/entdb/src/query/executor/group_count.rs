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

use crate::catalog::{Column, Schema};
use crate::error::{EntDbError, Result};
use crate::query::executor::Executor;
use crate::types::{DataType, Value};
use std::collections::HashMap;

pub struct GroupCountExecutor {
    child: Box<dyn Executor>,
    out_schema: Schema,
    group_col_idx: usize,
    rows: Vec<Vec<Value>>,
    pos: usize,
}

impl GroupCountExecutor {
    pub fn new(
        group_col_idx: usize,
        group_col_name: String,
        group_col_type: DataType,
        count_alias: String,
        child: Box<dyn Executor>,
    ) -> Self {
        Self {
            child,
            out_schema: Schema {
                columns: vec![
                    Column {
                        name: group_col_name,
                        data_type: group_col_type,
                        nullable: true,
                        default: None,
                        primary_key: false,
                    },
                    Column {
                        name: count_alias,
                        data_type: DataType::Int64,
                        nullable: false,
                        default: None,
                        primary_key: false,
                    },
                ],
            },
            group_col_idx,
            rows: Vec::new(),
            pos: 0,
        }
    }
}

impl Executor for GroupCountExecutor {
    fn open(&mut self) -> Result<()> {
        self.child.open()?;
        self.rows.clear();
        self.pos = 0;

        let mut grouped: Vec<(Value, i64)> = Vec::new();
        let mut key_to_pos: HashMap<Vec<u8>, usize> = HashMap::new();
        while let Some(row) = self.child.next()? {
            let key_val = row.get(self.group_col_idx).cloned().ok_or_else(|| {
                EntDbError::Query("GROUP BY column index out of bounds".to_string())
            })?;
            let key_bytes = key_val.serialize();
            if let Some(pos) = key_to_pos.get(&key_bytes).copied() {
                grouped[pos].1 = grouped[pos].1.saturating_add(1);
            } else {
                key_to_pos.insert(key_bytes, grouped.len());
                grouped.push((key_val, 1));
            }
        }

        self.rows = grouped
            .into_iter()
            .map(|(group_val, count)| vec![group_val, Value::Int64(count)])
            .collect();
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        if self.pos >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.pos].clone();
        self.pos += 1;
        Ok(Some(row))
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        &self.out_schema
    }
}
