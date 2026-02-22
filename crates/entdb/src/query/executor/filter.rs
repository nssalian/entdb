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
use crate::query::expression::{eval_predicate, BoundExpr};
use crate::types::Value;

pub struct FilterExecutor {
    predicate: BoundExpr,
    child: Box<dyn Executor>,
}

impl FilterExecutor {
    pub fn new(predicate: BoundExpr, child: Box<dyn Executor>) -> Self {
        Self { predicate, child }
    }
}

impl Executor for FilterExecutor {
    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        loop {
            let Some(row) = self.child.next()? else {
                return Ok(None);
            };
            if eval_predicate(&self.predicate, &row)? {
                return Ok(Some(row));
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }
}
