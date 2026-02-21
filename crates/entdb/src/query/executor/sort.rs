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
use crate::query::plan::SortKey;
use crate::types::Value;
use std::cmp::Ordering;

pub struct SortExecutor {
    keys: Vec<SortKey>,
    child: Box<dyn Executor>,
    rows: Vec<Vec<Value>>,
    pos: usize,
}

impl SortExecutor {
    pub fn new(keys: Vec<SortKey>, child: Box<dyn Executor>) -> Self {
        Self {
            keys,
            child,
            rows: Vec::new(),
            pos: 0,
        }
    }
}

impl Executor for SortExecutor {
    fn open(&mut self) -> Result<()> {
        self.child.open()?;
        self.rows.clear();
        self.pos = 0;

        while let Some(row) = self.child.next()? {
            self.rows.push(row);
        }

        sort_rows(&self.keys, &mut self.rows)?;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Vec<Value>>> {
        if self.pos >= self.rows.len() {
            return Ok(None);
        }
        let out = self.rows[self.pos].clone();
        self.pos += 1;
        Ok(Some(out))
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }
}

fn sort_rows(keys: &[SortKey], rows: &mut [Vec<Value>]) -> Result<()> {
    for i in 1..rows.len() {
        let mut j = i;
        while j > 0 {
            let ord = compare_rows(keys, &rows[j - 1], &rows[j])?;
            if ord == Ordering::Greater {
                rows.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
    }
    Ok(())
}

fn compare_rows(keys: &[SortKey], left: &[Value], right: &[Value]) -> Result<Ordering> {
    for key in keys {
        let l = left
            .get(key.col_idx)
            .ok_or_else(|| EntDbError::Query("ORDER BY column index out of bounds".to_string()))?;
        let r = right
            .get(key.col_idx)
            .ok_or_else(|| EntDbError::Query("ORDER BY column index out of bounds".to_string()))?;

        let ord = compare_values(l, r)?;
        if ord != Ordering::Equal {
            return Ok(if key.asc { ord } else { ord.reverse() });
        }
    }
    Ok(Ordering::Equal)
}

fn compare_values(left: &Value, right: &Value) -> Result<Ordering> {
    if left.eq(right)? {
        return Ok(Ordering::Equal);
    }
    if left.lt(right)? {
        return Ok(Ordering::Less);
    }
    Ok(Ordering::Greater)
}
