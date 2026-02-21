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

use crate::catalog::{Catalog, Schema};
use crate::error::Result;
use crate::query::executor::Executor;
use crate::types::Value;
use std::sync::Arc;

pub struct CreateTableExecutor {
    name: String,
    schema: Schema,
    catalog: Arc<Catalog>,
    done: bool,
    out_schema: Schema,
    affected_rows: u64,
}

impl CreateTableExecutor {
    pub fn new(name: String, schema: Schema, catalog: Arc<Catalog>) -> Self {
        Self {
            name,
            schema,
            catalog,
            done: false,
            out_schema: Schema { columns: vec![] },
            affected_rows: 0,
        }
    }
}

impl Executor for CreateTableExecutor {
    fn open(&mut self) -> Result<()> {
        self.done = false;
        self.affected_rows = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        if self.done {
            return Ok(None);
        }
        self.catalog.create_table(&self.name, self.schema.clone())?;
        self.affected_rows = 1;
        self.done = true;
        Ok(None)
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.out_schema
    }

    fn affected_rows(&self) -> Option<u64> {
        Some(self.affected_rows)
    }
}
