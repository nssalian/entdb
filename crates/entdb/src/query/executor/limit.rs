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

pub struct LimitExecutor {
    initial_count: usize,
    initial_offset: usize,
    remaining: usize,
    offset_remaining: usize,
    child: Box<dyn Executor>,
}

impl LimitExecutor {
    pub fn new(count: usize, offset: usize, child: Box<dyn Executor>) -> Self {
        Self {
            initial_count: count,
            initial_offset: offset,
            remaining: count,
            offset_remaining: offset,
            child,
        }
    }
}

impl Executor for LimitExecutor {
    fn open(&mut self) -> Result<()> {
        self.remaining = self.initial_count;
        self.offset_remaining = self.initial_offset;
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        while self.offset_remaining > 0 {
            match self.child.next()? {
                Some(_) => self.offset_remaining -= 1,
                None => return Ok(None),
            }
        }

        if self.remaining == 0 {
            return Ok(None);
        }
        let row = self.child.next()?;
        if row.is_some() {
            self.remaining -= 1;
        }
        Ok(row)
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }
}
