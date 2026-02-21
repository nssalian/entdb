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

use crate::catalog::Schema;
use crate::error::{EntDbError, Result};
use crate::query::executor::Executor;
use crate::types::Value;

pub struct NestedLoopJoinExecutor {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    left_on: usize,
    right_on: usize,
    output_schema: Schema,
    right_rows: Vec<Vec<Value>>,
    current_left: Option<Vec<Value>>,
    right_pos: usize,
}

impl NestedLoopJoinExecutor {
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        left_on: usize,
        right_on: usize,
        output_schema: Schema,
    ) -> Self {
        Self {
            left,
            right,
            left_on,
            right_on,
            output_schema,
            right_rows: Vec::new(),
            current_left: None,
            right_pos: 0,
        }
    }
}

impl Executor for NestedLoopJoinExecutor {
    fn open(&mut self) -> Result<()> {
        self.left.open()?;
        self.right.open()?;

        self.right_rows.clear();
        while let Some(row) = self.right.next()? {
            self.right_rows.push(row);
        }
        self.current_left = None;
        self.right_pos = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        loop {
            if self.current_left.is_none() {
                self.current_left = self.left.next()?;
                self.right_pos = 0;
            }
            let Some(left_row) = self.current_left.as_ref() else {
                return Ok(None);
            };

            while self.right_pos < self.right_rows.len() {
                let right_row = &self.right_rows[self.right_pos];
                self.right_pos += 1;
                let left_key = left_row.get(self.left_on).ok_or_else(|| {
                    EntDbError::Query("left join key index out of bounds".to_string())
                })?;
                let right_key = right_row.get(self.right_on).ok_or_else(|| {
                    EntDbError::Query("right join key index out of bounds".to_string())
                })?;
                if left_key.eq(right_key)? {
                    let mut out = left_row.clone();
                    out.extend(right_row.clone());
                    return Ok(Some(out));
                }
            }

            self.current_left = None;
        }
    }

    fn close(&mut self) -> Result<()> {
        self.left.close()?;
        self.right.close()?;
        self.right_rows.clear();
        self.current_left = None;
        self.right_pos = 0;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }
}
