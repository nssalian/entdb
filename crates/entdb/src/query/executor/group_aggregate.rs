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
use crate::error::{EntDbError, Result};
use crate::query::executor::Executor;
use crate::query::plan::{AggregateExpr, AggregateFn, GroupKey};
use crate::types::{DataType, Value};
use std::collections::HashMap;

pub struct GroupAggregateExecutor {
    child: Box<dyn Executor>,
    out_schema: Schema,
    group_keys: Vec<GroupKey>,
    aggregates: Vec<AggregateExpr>,
    rows: Vec<Vec<Value>>,
    pos: usize,
}

impl GroupAggregateExecutor {
    pub fn new(
        group_keys: Vec<GroupKey>,
        aggregates: Vec<AggregateExpr>,
        child: Box<dyn Executor>,
    ) -> Self {
        let mut columns = Vec::new();
        for key in &group_keys {
            columns.push(Column {
                name: key.col_name.clone(),
                data_type: key.col_type.clone(),
                nullable: true,
                default: None,
                primary_key: false,
            });
        }
        for agg in &aggregates {
            let agg_type = match agg.agg_fn {
                AggregateFn::CountStar | AggregateFn::Count => DataType::Int64,
                AggregateFn::Sum => match agg.agg_col_type {
                    Some(DataType::Float32) | Some(DataType::Float64) => DataType::Float64,
                    _ => DataType::Int64,
                },
                AggregateFn::Avg => DataType::Float64,
                AggregateFn::Min | AggregateFn::Max => {
                    agg.agg_col_type.clone().unwrap_or(DataType::Null)
                }
            };
            columns.push(Column {
                name: agg.agg_alias.clone(),
                data_type: agg_type,
                nullable: matches!(
                    agg.agg_fn,
                    AggregateFn::Sum | AggregateFn::Avg | AggregateFn::Min | AggregateFn::Max
                ),
                default: None,
                primary_key: false,
            });
        }

        Self {
            child,
            out_schema: Schema { columns },
            group_keys,
            aggregates,
            rows: Vec::new(),
            pos: 0,
        }
    }
}

#[derive(Clone)]
struct AggState {
    count: i64,
    sum_i64: i64,
    sum_f64: f64,
    float_seen: bool,
    min: Option<Value>,
    max: Option<Value>,
}

impl AggState {
    fn new() -> Self {
        Self {
            count: 0,
            sum_i64: 0,
            sum_f64: 0.0,
            float_seen: false,
            min: None,
            max: None,
        }
    }
}

#[derive(Clone)]
struct GroupState {
    slots: Vec<AggState>,
}

impl GroupState {
    fn new(num_slots: usize) -> Self {
        Self {
            slots: (0..num_slots).map(|_| AggState::new()).collect(),
        }
    }
}

fn to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int16(i) => Some(*i as f64),
        Value::Int32(i) => Some(*i as f64),
        Value::Int64(i) => Some(*i as f64),
        Value::Float32(f) => Some(*f as f64),
        Value::Float64(f) => Some(*f),
        _ => None,
    }
}

fn to_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Int16(i) => Some(*i as i64),
        Value::Int32(i) => Some(*i as i64),
        Value::Int64(i) => Some(*i),
        _ => None,
    }
}

impl Executor for GroupAggregateExecutor {
    fn open(&mut self) -> Result<()> {
        self.child.open()?;
        self.rows.clear();
        self.pos = 0;

        let mut grouped: Vec<(Vec<Value>, GroupState)> = Vec::new();
        let mut key_to_pos: HashMap<Vec<u8>, usize> = HashMap::new();

        while let Some(row) = self.child.next()? {
            let mut group_key = Vec::with_capacity(self.group_keys.len());
            for key in &self.group_keys {
                group_key.push(row.get(key.col_idx).cloned().ok_or_else(|| {
                    EntDbError::Query("GROUP BY column index out of bounds".to_string())
                })?);
            }
            let mut group_bytes = Vec::new();
            if group_key.is_empty() {
                group_bytes.push(0);
            } else {
                for v in &group_key {
                    let enc = v.serialize();
                    group_bytes.extend((enc.len() as u32).to_le_bytes());
                    group_bytes.extend(enc);
                }
            }

            let pos = if let Some(existing) = key_to_pos.get(&group_bytes).copied() {
                existing
            } else {
                let idx = grouped.len();
                key_to_pos.insert(group_bytes, idx);
                grouped.push((group_key.clone(), GroupState::new(self.aggregates.len())));
                idx
            };

            let gstate = &mut grouped[pos].1;
            for (agg, state) in self.aggregates.iter().zip(gstate.slots.iter_mut()) {
                match agg.agg_fn {
                    AggregateFn::CountStar => {
                        state.count = state.count.saturating_add(1);
                    }
                    AggregateFn::Count => {
                        let agg_idx = agg.agg_col_idx.ok_or_else(|| {
                            EntDbError::Query("aggregate column index out of bounds".to_string())
                        })?;
                        let val = row.get(agg_idx).cloned().ok_or_else(|| {
                            EntDbError::Query("aggregate column index out of bounds".to_string())
                        })?;
                        if !matches!(val, Value::Null) {
                            state.count = state.count.saturating_add(1);
                        }
                    }
                    AggregateFn::Sum | AggregateFn::Avg => {
                        let agg_idx = agg.agg_col_idx.ok_or_else(|| {
                            EntDbError::Query("aggregate column index out of bounds".to_string())
                        })?;
                        let val = row.get(agg_idx).cloned().ok_or_else(|| {
                            EntDbError::Query("aggregate column index out of bounds".to_string())
                        })?;
                        if matches!(val, Value::Null) {
                            continue;
                        }
                        state.count = state.count.saturating_add(1);
                        if let Some(i) = to_i64(&val) {
                            state.sum_i64 = state.sum_i64.saturating_add(i);
                        } else if let Some(f) = to_f64(&val) {
                            state.float_seen = true;
                            state.sum_f64 += f;
                        } else {
                            return Err(EntDbError::Query(
                                "SUM/AVG require numeric values".to_string(),
                            ));
                        }
                    }
                    AggregateFn::Min => {
                        let agg_idx = agg.agg_col_idx.ok_or_else(|| {
                            EntDbError::Query("aggregate column index out of bounds".to_string())
                        })?;
                        let val = row.get(agg_idx).cloned().ok_or_else(|| {
                            EntDbError::Query("aggregate column index out of bounds".to_string())
                        })?;
                        if matches!(val, Value::Null) {
                            continue;
                        }
                        if let Some(cur) = &state.min {
                            if val.lt(cur)? {
                                state.min = Some(val);
                            }
                        } else {
                            state.min = Some(val);
                        }
                    }
                    AggregateFn::Max => {
                        let agg_idx = agg.agg_col_idx.ok_or_else(|| {
                            EntDbError::Query("aggregate column index out of bounds".to_string())
                        })?;
                        let val = row.get(agg_idx).cloned().ok_or_else(|| {
                            EntDbError::Query("aggregate column index out of bounds".to_string())
                        })?;
                        if matches!(val, Value::Null) {
                            continue;
                        }
                        if let Some(cur) = &state.max {
                            if val.gt(cur)? {
                                state.max = Some(val);
                            }
                        } else {
                            state.max = Some(val);
                        }
                    }
                }
            }
        }

        if self.group_keys.is_empty() && grouped.is_empty() {
            grouped.push((Vec::new(), GroupState::new(self.aggregates.len())));
        }

        self.rows = grouped
            .into_iter()
            .map(|(group_val, gstate)| {
                let mut out = Vec::new();
                for group_value in group_val {
                    out.push(group_value);
                }

                for (agg, st) in self.aggregates.iter().zip(gstate.slots.into_iter()) {
                    let agg_val = match agg.agg_fn {
                        AggregateFn::CountStar | AggregateFn::Count => Value::Int64(st.count),
                        AggregateFn::Sum => {
                            if st.count == 0 {
                                Value::Null
                            } else if st.float_seen {
                                Value::Float64(st.sum_f64 + st.sum_i64 as f64)
                            } else {
                                Value::Int64(st.sum_i64)
                            }
                        }
                        AggregateFn::Avg => {
                            if st.count == 0 {
                                Value::Null
                            } else {
                                Value::Float64((st.sum_f64 + st.sum_i64 as f64) / st.count as f64)
                            }
                        }
                        AggregateFn::Min => st.min.unwrap_or(Value::Null),
                        AggregateFn::Max => st.max.unwrap_or(Value::Null),
                    };
                    out.push(agg_val);
                }
                out
            })
            .collect();

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
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        &self.out_schema
    }
}
