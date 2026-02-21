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

pub struct CreateIndexExecutor {
    table_name: String,
    index_name: String,
    columns: Vec<String>,
    unique: bool,
    if_not_exists: bool,
    catalog: Arc<Catalog>,
    done: bool,
    out_schema: Schema,
    affected_rows: u64,
}

impl CreateIndexExecutor {
    pub fn new(
        table_name: String,
        index_name: String,
        columns: Vec<String>,
        unique: bool,
        if_not_exists: bool,
        catalog: Arc<Catalog>,
    ) -> Self {
        Self {
            table_name,
            index_name,
            columns,
            unique,
            if_not_exists,
            catalog,
            done: false,
            out_schema: Schema { columns: vec![] },
            affected_rows: 0,
        }
    }
}

impl Executor for CreateIndexExecutor {
    fn open(&mut self) -> Result<()> {
        self.done = false;
        self.affected_rows = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        if self.done {
            return Ok(None);
        }
        if self.if_not_exists
            && self
                .catalog
                .get_table(&self.table_name)
                .map(|t| t.indexes.iter().any(|i| i.name == self.index_name))
                .unwrap_or(false)
        {
            self.done = true;
            self.affected_rows = 0;
            return Ok(None);
        }
        self.catalog.create_index(
            &self.table_name,
            &self.index_name,
            &self.columns,
            self.unique,
        )?;
        self.done = true;
        self.affected_rows = 1;
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
