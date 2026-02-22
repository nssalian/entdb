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

use crate::error::{EntDbError, Result};
use crate::query::plan::SortKey;
use crate::types::Value;

#[derive(Debug, Clone)]
pub enum BoundExpr {
    ColumnRef {
        col_idx: usize,
    },
    Literal(Value),
    BinaryOp {
        op: BinaryOp,
        left: Box<BoundExpr>,
        right: Box<BoundExpr>,
    },
    Function {
        name: String,
        args: Vec<BoundExpr>,
    },
    WindowRowNumber {
        order_by: Vec<SortKey>,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum BinaryOp {
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    Add,
    Sub,
    Mul,
    Div,
    And,
    Or,
}

pub fn eval_expr(expr: &BoundExpr, row: &[Value]) -> Result<Value> {
    match expr {
        BoundExpr::ColumnRef { col_idx } => row
            .get(*col_idx)
            .cloned()
            .ok_or_else(|| EntDbError::Query(format!("column index out of bounds: {col_idx}"))),
        BoundExpr::Literal(v) => Ok(v.clone()),
        BoundExpr::BinaryOp { op, left, right } => {
            let l = eval_expr(left, row)?;
            let r = eval_expr(right, row)?;

            match op {
                BinaryOp::Eq => Ok(Value::Boolean(l.eq(&r)?)),
                BinaryOp::Neq => Ok(Value::Boolean(!l.eq(&r)?)),
                BinaryOp::Gt => Ok(Value::Boolean(l.gt(&r)?)),
                BinaryOp::Lt => Ok(Value::Boolean(l.lt(&r)?)),
                BinaryOp::Gte => Ok(Value::Boolean(l.gt(&r)? || l.eq(&r)?)),
                BinaryOp::Lte => Ok(Value::Boolean(l.lt(&r)? || l.eq(&r)?)),
                BinaryOp::Add => eval_add(&l, &r),
                BinaryOp::Sub => eval_sub(&l, &r),
                BinaryOp::Mul => eval_mul(&l, &r),
                BinaryOp::Div => eval_div(&l, &r),
                BinaryOp::And => eval_and(&l, &r),
                BinaryOp::Or => eval_or(&l, &r),
            }
        }
        BoundExpr::Function { name, args } => eval_function(name, args, row),
        BoundExpr::WindowRowNumber { .. } => Err(EntDbError::Query(
            "window function requires window-aware executor".to_string(),
        )),
    }
}

pub fn eval_predicate(expr: &BoundExpr, row: &[Value]) -> Result<bool> {
    let v = eval_expr(expr, row)?;
    match v {
        Value::Boolean(b) => Ok(b),
        _ => Err(EntDbError::Query(
            "predicate did not evaluate to boolean".to_string(),
        )),
    }
}

fn eval_add(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(l.saturating_add(*r))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l.saturating_add(*r))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l.saturating_add(*r))),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32((*l as i32).saturating_add(*r))),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(l.saturating_add(*r as i32))),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64).saturating_add(*r))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(l.saturating_add(*r as i64))),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(*l + *r)),
        (Value::Int32(l), Value::Float64(r)) => Ok(Value::Float64((*l as f64) + *r)),
        (Value::Float64(l), Value::Int32(r)) => Ok(Value::Float64(*l + (*r as f64))),
        (Value::Int64(l), Value::Float64(r)) => Ok(Value::Float64((*l as f64) + *r)),
        (Value::Float64(l), Value::Int64(r)) => Ok(Value::Float64(*l + (*r as f64))),
        _ => Err(EntDbError::Query(format!(
            "unsupported '+' operands: {:?} and {:?}",
            left.data_type(),
            right.data_type()
        ))),
    }
}

fn eval_sub(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(l.saturating_sub(*r))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l.saturating_sub(*r))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l.saturating_sub(*r))),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32((*l as i32).saturating_sub(*r))),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(l.saturating_sub(*r as i32))),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64).saturating_sub(*r))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(l.saturating_sub(*r as i64))),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(*l - *r)),
        (Value::Int32(l), Value::Float64(r)) => Ok(Value::Float64((*l as f64) - *r)),
        (Value::Float64(l), Value::Int32(r)) => Ok(Value::Float64(*l - (*r as f64))),
        (Value::Int64(l), Value::Float64(r)) => Ok(Value::Float64((*l as f64) - *r)),
        (Value::Float64(l), Value::Int64(r)) => Ok(Value::Float64(*l - (*r as f64))),
        _ => Err(EntDbError::Query(format!(
            "unsupported '-' operands: {:?} and {:?}",
            left.data_type(),
            right.data_type()
        ))),
    }
}

fn eval_mul(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(l.saturating_mul(*r))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l.saturating_mul(*r))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l.saturating_mul(*r))),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32((*l as i32).saturating_mul(*r))),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(l.saturating_mul(*r as i32))),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64).saturating_mul(*r))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(l.saturating_mul(*r as i64))),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(*l * *r)),
        (Value::Int32(l), Value::Float64(r)) => Ok(Value::Float64((*l as f64) * *r)),
        (Value::Float64(l), Value::Int32(r)) => Ok(Value::Float64(*l * (*r as f64))),
        (Value::Int64(l), Value::Float64(r)) => Ok(Value::Float64((*l as f64) * *r)),
        (Value::Float64(l), Value::Int64(r)) => Ok(Value::Float64(*l * (*r as f64))),
        _ => Err(EntDbError::Query(format!(
            "unsupported '*' operands: {:?} and {:?}",
            left.data_type(),
            right.data_type()
        ))),
    }
}

fn eval_div(left: &Value, right: &Value) -> Result<Value> {
    let is_zero = match right {
        Value::Int16(v) => *v == 0,
        Value::Int32(v) => *v == 0,
        Value::Int64(v) => *v == 0,
        Value::Float64(v) => *v == 0.0,
        _ => false,
    };
    if is_zero {
        return Err(EntDbError::Query("division by zero".to_string()));
    }

    match (left, right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(l.wrapping_div(*r))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l.wrapping_div(*r))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l.wrapping_div(*r))),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32((*l as i32).wrapping_div(*r))),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(l.wrapping_div(*r as i32))),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64((*l as i64).wrapping_div(*r))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(l.wrapping_div(*r as i64))),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(*l / *r)),
        (Value::Int32(l), Value::Float64(r)) => Ok(Value::Float64((*l as f64) / *r)),
        (Value::Float64(l), Value::Int32(r)) => Ok(Value::Float64(*l / (*r as f64))),
        (Value::Int64(l), Value::Float64(r)) => Ok(Value::Float64((*l as f64) / *r)),
        (Value::Float64(l), Value::Int64(r)) => Ok(Value::Float64(*l / (*r as f64))),
        _ => Err(EntDbError::Query(format!(
            "unsupported '/' operands: {:?} and {:?}",
            left.data_type(),
            right.data_type()
        ))),
    }
}

fn eval_and(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Boolean(l), Value::Boolean(r)) => Ok(Value::Boolean(*l && *r)),
        _ => Err(EntDbError::Query(
            "AND requires boolean operands".to_string(),
        )),
    }
}

fn eval_or(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Boolean(l), Value::Boolean(r)) => Ok(Value::Boolean(*l || *r)),
        _ => Err(EntDbError::Query(
            "OR requires boolean operands".to_string(),
        )),
    }
}

fn eval_function(name: &str, args: &[BoundExpr], row: &[Value]) -> Result<Value> {
    let mut vals = Vec::with_capacity(args.len());
    for arg in args {
        vals.push(eval_expr(arg, row)?);
    }
    match name {
        "lower" => one_text(name, &vals).map(|s| Value::Text(s.to_lowercase())),
        "upper" => one_text(name, &vals).map(|s| Value::Text(s.to_uppercase())),
        "trim" => one_text(name, &vals).map(|s| Value::Text(s.trim().to_string())),
        "length" => one_text(name, &vals).map(|s| Value::Int64(s.chars().count() as i64)),
        "abs" => one_numeric(name, &vals).map(|v| match v {
            Value::Int16(n) => Value::Int16(n.abs()),
            Value::Int32(n) => Value::Int32(n.abs()),
            Value::Int64(n) => Value::Int64(n.abs()),
            Value::Float32(n) => Value::Float32(n.abs()),
            Value::Float64(n) => Value::Float64(n.abs()),
            _ => Value::Null,
        }),
        "concat" => {
            let mut out = String::new();
            for v in &vals {
                match v {
                    Value::Null => {}
                    Value::Text(s) => out.push_str(s),
                    _ => out.push_str(&format!("{v:?}")),
                }
            }
            Ok(Value::Text(out))
        }
        "coalesce" => {
            for v in vals {
                if !matches!(v, Value::Null) {
                    return Ok(v);
                }
            }
            Ok(Value::Null)
        }
        _ => Err(EntDbError::Query(format!("unsupported function '{name}'"))),
    }
}

fn one_text<'a>(name: &str, vals: &'a [Value]) -> Result<&'a str> {
    if vals.len() != 1 {
        return Err(EntDbError::Query(format!("{name} expects 1 argument")));
    }
    match &vals[0] {
        Value::Text(v) => Ok(v),
        _ => Err(EntDbError::Query(format!("{name} expects text argument"))),
    }
}

fn one_numeric(name: &str, vals: &[Value]) -> Result<Value> {
    if vals.len() != 1 {
        return Err(EntDbError::Query(format!("{name} expects 1 argument")));
    }
    match vals[0] {
        Value::Int16(_)
        | Value::Int32(_)
        | Value::Int64(_)
        | Value::Float32(_)
        | Value::Float64(_) => Ok(vals[0].clone()),
        _ => Err(EntDbError::Query(format!(
            "{name} expects numeric argument"
        ))),
    }
}
