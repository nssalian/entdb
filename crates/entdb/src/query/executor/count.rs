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
use crate::error::Result;
use crate::query::executor::Executor;
use crate::types::{DataType, Value};

pub struct CountExecutor {
    child: Box<dyn Executor>,
    out_schema: Schema,
    done: bool,
}

impl CountExecutor {
    pub fn new(alias: String, child: Box<dyn Executor>) -> Self {
        Self {
            child,
            out_schema: Schema {
                columns: vec![Column {
                    name: alias,
                    data_type: DataType::Int64,
                    nullable: false,
                    default: None,
                    primary_key: false,
                }],
            },
            done: false,
        }
    }
}

impl Executor for CountExecutor {
    fn open(&mut self) -> Result<()> {
        self.done = false;
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        if self.done {
            return Ok(None);
        }

        let mut count = 0_i64;
        while self.child.next()?.is_some() {
            count = count.saturating_add(1);
        }

        self.done = true;
        Ok(Some(vec![Value::Int64(count)]))
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        &self.out_schema
    }
}
