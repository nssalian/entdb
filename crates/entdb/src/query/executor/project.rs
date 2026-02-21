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

pub struct ProjectExecutor {
    projection: Option<Vec<usize>>,
    _projection_names: Vec<String>,
    output_schema: Schema,
    child: Box<dyn Executor>,
}

impl ProjectExecutor {
    pub fn new(
        projection: Option<Vec<usize>>,
        projection_names: Vec<String>,
        output_schema: Schema,
        child: Box<dyn Executor>,
    ) -> Self {
        Self {
            projection,
            _projection_names: projection_names,
            output_schema,
            child,
        }
    }
}

impl Executor for ProjectExecutor {
    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        let Some(row) = self.child.next()? else {
            return Ok(None);
        };

        match &self.projection {
            None => Ok(Some(row)),
            Some(idxs) => {
                let mut out = Vec::with_capacity(idxs.len());
                for idx in idxs {
                    out.push(row[*idx].clone());
                }
                Ok(Some(out))
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }
}
