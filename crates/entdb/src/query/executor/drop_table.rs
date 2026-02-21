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
use crate::error::{EntDbError, Result};
use crate::query::executor::Executor;
use crate::types::Value;
use std::sync::Arc;

pub struct DropTableExecutor {
    name: String,
    if_exists: bool,
    catalog: Arc<Catalog>,
    done: bool,
    out_schema: Schema,
    affected_rows: u64,
}

impl DropTableExecutor {
    pub fn new(name: String, if_exists: bool, catalog: Arc<Catalog>) -> Self {
        Self {
            name,
            if_exists,
            catalog,
            done: false,
            out_schema: Schema { columns: vec![] },
            affected_rows: 0,
        }
    }
}

impl Executor for DropTableExecutor {
    fn open(&mut self) -> Result<()> {
        self.done = false;
        self.affected_rows = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        if self.done {
            return Ok(None);
        }
        let exists = self.catalog.get_table(&self.name).is_some();
        if !exists && !self.if_exists {
            return Err(EntDbError::Query(format!(
                "table '{}' does not exist",
                self.name
            )));
        }
        if exists {
            self.catalog.drop_table(&self.name)?;
            self.affected_rows = 1;
        }
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
