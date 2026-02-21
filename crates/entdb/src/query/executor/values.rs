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

pub struct ValuesExecutor {
    rows: Vec<Vec<Value>>,
    schema: Schema,
    pos: usize,
}

impl ValuesExecutor {
    pub fn new(rows: Vec<Vec<Value>>, schema: Schema) -> Self {
        Self {
            rows,
            schema,
            pos: 0,
        }
    }
}

impl Executor for ValuesExecutor {
    fn open(&mut self) -> Result<()> {
        self.pos = 0;
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
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
