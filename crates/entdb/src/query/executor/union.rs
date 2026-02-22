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
use crate::error::Result;
use crate::query::executor::Executor;
use crate::types::Value;
use std::collections::HashSet;

pub struct UnionExecutor {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    all: bool,
    output_schema: Schema,
    rows: Vec<Vec<Value>>,
    pos: usize,
}

impl UnionExecutor {
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        all: bool,
        output_schema: Schema,
    ) -> Self {
        Self {
            left,
            right,
            all,
            output_schema,
            rows: Vec::new(),
            pos: 0,
        }
    }
}

impl Executor for UnionExecutor {
    fn open(&mut self) -> Result<()> {
        self.left.open()?;
        self.right.open()?;
        self.rows.clear();
        self.pos = 0;

        if self.all {
            while let Some(row) = self.left.next()? {
                self.rows.push(row);
            }
            while let Some(row) = self.right.next()? {
                self.rows.push(row);
            }
        } else {
            let mut seen = HashSet::new();
            while let Some(row) = self.left.next()? {
                let key = row
                    .iter()
                    .flat_map(|v| {
                        let enc = v.serialize();
                        let mut out = Vec::with_capacity(enc.len() + 4);
                        out.extend((enc.len() as u32).to_le_bytes());
                        out.extend(enc);
                        out
                    })
                    .collect::<Vec<u8>>();
                if seen.insert(key) {
                    self.rows.push(row);
                }
            }
            while let Some(row) = self.right.next()? {
                let key = row
                    .iter()
                    .flat_map(|v| {
                        let enc = v.serialize();
                        let mut out = Vec::with_capacity(enc.len() + 4);
                        out.extend((enc.len() as u32).to_le_bytes());
                        out.extend(enc);
                        out
                    })
                    .collect::<Vec<u8>>();
                if seen.insert(key) {
                    self.rows.push(row);
                }
            }
        }
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
        self.left.close()?;
        self.right.close()?;
        self.rows.clear();
        self.pos = 0;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }
}
