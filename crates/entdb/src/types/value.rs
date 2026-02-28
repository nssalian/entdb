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
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, PrimitiveDateTime};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Text,
    Varchar(u32),
    Timestamp,
    Vector(u32),
    Bm25Query,
    Null,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Text(String),
    Timestamp(i64),
    Vector(Vec<f32>),
    Bm25Query {
        terms: Vec<String>,
        index_name: String,
    },
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Boolean(_) => DataType::Boolean,
            Self::Int16(_) => DataType::Int16,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Text(_) => DataType::Text,
            Self::Timestamp(_) => DataType::Timestamp,
            Self::Vector(v) => DataType::Vector(v.len() as u32),
            Self::Bm25Query { .. } => DataType::Bm25Query,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Self::Null => vec![0],
            Self::Boolean(v) => vec![1, u8::from(*v)],
            Self::Int16(v) => {
                let mut out = vec![2];
                out.extend_from_slice(&v.to_le_bytes());
                out
            }
            Self::Int32(v) => {
                let mut out = vec![3];
                out.extend_from_slice(&v.to_le_bytes());
                out
            }
            Self::Int64(v) => {
                let mut out = vec![4];
                out.extend_from_slice(&v.to_le_bytes());
                out
            }
            Self::Float32(v) => {
                let mut out = vec![5];
                out.extend_from_slice(&v.to_le_bytes());
                out
            }
            Self::Float64(v) => {
                let mut out = vec![6];
                out.extend_from_slice(&v.to_le_bytes());
                out
            }
            Self::Text(v) => {
                let mut out = vec![7];
                let b = v.as_bytes();
                out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                out.extend_from_slice(b);
                out
            }
            Self::Timestamp(v) => {
                let mut out = vec![8];
                out.extend_from_slice(&v.to_le_bytes());
                out
            }
            Self::Vector(values) => {
                let mut out = vec![9];
                out.extend_from_slice(&(values.len() as u32).to_le_bytes());
                for v in values {
                    out.extend_from_slice(&v.to_le_bytes());
                }
                out
            }
            Self::Bm25Query { terms, index_name } => {
                let mut out = vec![10];
                let payload =
                    serde_json::to_vec(&(terms, index_name)).expect("serialize bm25 query payload");
                out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                out.extend_from_slice(&payload);
                out
            }
        }
    }

    pub fn deserialize(data: &[u8], dt: &DataType) -> Result<Self> {
        match dt {
            DataType::Null => Ok(Self::Null),
            DataType::Boolean => {
                let b = data.first().ok_or_else(|| {
                    EntDbError::InvalidPage("bool decode: missing byte".to_string())
                })?;
                Ok(Self::Boolean(*b != 0))
            }
            DataType::Int16 => {
                if data.len() < 2 {
                    return Err(EntDbError::InvalidPage(
                        "int16 decode: too short".to_string(),
                    ));
                }
                Ok(Self::Int16(i16::from_le_bytes(
                    data[0..2].try_into().expect("i16 bytes"),
                )))
            }
            DataType::Int32 => {
                if data.len() < 4 {
                    return Err(EntDbError::InvalidPage(
                        "int32 decode: too short".to_string(),
                    ));
                }
                Ok(Self::Int32(i32::from_le_bytes(
                    data[0..4].try_into().expect("i32 bytes"),
                )))
            }
            DataType::Int64 | DataType::Timestamp => {
                if data.len() < 8 {
                    return Err(EntDbError::InvalidPage(
                        "int64 decode: too short".to_string(),
                    ));
                }
                let v = i64::from_le_bytes(data[0..8].try_into().expect("i64 bytes"));
                if matches!(dt, DataType::Timestamp) {
                    Ok(Self::Timestamp(v))
                } else {
                    Ok(Self::Int64(v))
                }
            }
            DataType::Float32 => {
                if data.len() < 4 {
                    return Err(EntDbError::InvalidPage(
                        "float32 decode: too short".to_string(),
                    ));
                }
                Ok(Self::Float32(f32::from_le_bytes(
                    data[0..4].try_into().expect("f32 bytes"),
                )))
            }
            DataType::Float64 => {
                if data.len() < 8 {
                    return Err(EntDbError::InvalidPage(
                        "float64 decode: too short".to_string(),
                    ));
                }
                Ok(Self::Float64(f64::from_le_bytes(
                    data[0..8].try_into().expect("f64 bytes"),
                )))
            }
            DataType::Text | DataType::Varchar(_) => {
                let text_bytes = if data.len() >= 4 {
                    let declared =
                        u32::from_le_bytes(data[0..4].try_into().expect("text len bytes")) as usize;
                    if 4 + declared == data.len() {
                        &data[4..]
                    } else {
                        data
                    }
                } else {
                    data
                };

                let s = std::str::from_utf8(text_bytes).map_err(|_| {
                    EntDbError::InvalidPage("text decode: invalid utf8".to_string())
                })?;
                if let DataType::Varchar(limit) = dt {
                    if s.len() > *limit as usize {
                        return Err(EntDbError::InvalidPage(
                            "varchar length exceeded".to_string(),
                        ));
                    }
                }
                Ok(Self::Text(s.to_string()))
            }
            DataType::Vector(dim) => {
                let expected = (*dim as usize) * 4;
                let payload = if data.len() == expected {
                    data
                } else if data.len() == expected + 4 {
                    let declared =
                        u32::from_le_bytes(data[0..4].try_into().expect("vector dim bytes"));
                    if declared != *dim {
                        return Err(EntDbError::InvalidPage(format!(
                            "vector decode: expected dimension {dim}, got {declared}"
                        )));
                    }
                    &data[4..]
                } else {
                    return Err(EntDbError::InvalidPage(format!(
                        "vector decode: expected {expected} or {} bytes, got {}",
                        expected + 4,
                        data.len()
                    )));
                };
                if payload.len() != expected {
                    return Err(EntDbError::InvalidPage(format!(
                        "vector decode: expected {expected} bytes, got {}",
                        payload.len()
                    )));
                }
                let mut out = Vec::with_capacity(*dim as usize);
                for chunk in payload.chunks_exact(4) {
                    out.push(f32::from_le_bytes(chunk.try_into().expect("f32 bytes")));
                }
                Ok(Self::Vector(out))
            }
            DataType::Bm25Query => {
                let payload = if data.len() >= 4 {
                    let declared =
                        u32::from_le_bytes(data[0..4].try_into().expect("bm25 query len bytes"))
                            as usize;
                    if 4 + declared == data.len() {
                        &data[4..]
                    } else {
                        data
                    }
                } else {
                    data
                };
                let (terms, index_name): (Vec<String>, String) = serde_json::from_slice(payload)
                    .map_err(|e| {
                        EntDbError::InvalidPage(format!("bm25 query decode failed: {e}"))
                    })?;
                Ok(Self::Bm25Query { terms, index_name })
            }
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Self::Null => 0,
            Self::Boolean(_) => 1,
            Self::Int16(_) => 2,
            Self::Int32(_) => 4,
            Self::Int64(_) => 8,
            Self::Float32(_) => 4,
            Self::Float64(_) => 8,
            Self::Text(s) => s.len(),
            Self::Timestamp(_) => 8,
            Self::Vector(values) => values.len() * 4,
            Self::Bm25Query { terms, index_name } => {
                terms.iter().map(|t| t.len()).sum::<usize>() + index_name.len()
            }
        }
    }

    pub fn cast_to(&self, target: &DataType) -> Result<Value> {
        if matches!(self, Value::Null) {
            return Ok(Value::Null);
        }

        let out = match (self, target) {
            (Value::Boolean(v), DataType::Boolean) => Value::Boolean(*v),
            (Value::Int16(v), DataType::Int16) => Value::Int16(*v),
            (Value::Int32(v), DataType::Int32) => Value::Int32(*v),
            (Value::Int64(v), DataType::Int64) => Value::Int64(*v),
            (Value::Float32(v), DataType::Float32) => Value::Float32(*v),
            (Value::Float64(v), DataType::Float64) => Value::Float64(*v),
            (Value::Timestamp(v), DataType::Timestamp) => Value::Timestamp(*v),
            (Value::Text(v), DataType::Text | DataType::Varchar(_)) => Value::Text(v.clone()),
            (Value::Vector(values), DataType::Vector(dim)) => {
                if values.len() != *dim as usize {
                    return Err(EntDbError::InvalidPage(format!(
                        "vector dimension mismatch: expected {dim}, got {}",
                        values.len()
                    )));
                }
                Value::Vector(values.clone())
            }

            (Value::Int16(v), DataType::Int32) => Value::Int32(*v as i32),
            (Value::Int16(v), DataType::Int64) => Value::Int64(*v as i64),
            (Value::Int32(v), DataType::Int64) => Value::Int64(*v as i64),
            (Value::Int64(v), DataType::Int32) => {
                let narrowed = i32::try_from(*v).map_err(|_| {
                    EntDbError::InvalidPage("cast int64->int32 overflow".to_string())
                })?;
                Value::Int32(narrowed)
            }
            (Value::Int64(v), DataType::Int16) => {
                let narrowed = i16::try_from(*v).map_err(|_| {
                    EntDbError::InvalidPage("cast int64->int16 overflow".to_string())
                })?;
                Value::Int16(narrowed)
            }
            (Value::Int32(v), DataType::Int16) => {
                let narrowed = i16::try_from(*v).map_err(|_| {
                    EntDbError::InvalidPage("cast int32->int16 overflow".to_string())
                })?;
                Value::Int16(narrowed)
            }
            (Value::Int32(v), DataType::Float64) => Value::Float64(*v as f64),
            (Value::Int64(v), DataType::Float64) => Value::Float64(*v as f64),
            (Value::Float32(v), DataType::Float64) => Value::Float64(*v as f64),
            (Value::Float64(v), DataType::Float32) => {
                if !v.is_finite() {
                    return Err(EntDbError::InvalidPage(
                        "cast float64->float32 non-finite".to_string(),
                    ));
                }
                if *v < f32::MIN as f64 || *v > f32::MAX as f64 {
                    return Err(EntDbError::InvalidPage(
                        "cast float64->float32 overflow".to_string(),
                    ));
                }
                Value::Float32(*v as f32)
            }

            (Value::Text(v), DataType::Int32) => Value::Int32(
                v.parse()
                    .map_err(|_| EntDbError::InvalidPage("cast text->int32 failed".to_string()))?,
            ),
            (Value::Text(v), DataType::Int64) => Value::Int64(
                v.parse()
                    .map_err(|_| EntDbError::InvalidPage("cast text->int64 failed".to_string()))?,
            ),
            (Value::Text(v), DataType::Float64) => {
                Value::Float64(v.parse().map_err(|_| {
                    EntDbError::InvalidPage("cast text->float64 failed".to_string())
                })?)
            }
            (Value::Text(v), DataType::Timestamp) => Value::Timestamp(parse_timestamp_text(v)?),
            (Value::Text(v), DataType::Vector(dim)) => {
                let parsed = parse_vector_text(v)?;
                if parsed.len() != *dim as usize {
                    return Err(EntDbError::InvalidPage(format!(
                        "vector dimension mismatch: expected {dim}, got {}",
                        parsed.len()
                    )));
                }
                Value::Vector(parsed)
            }
            (Value::Vector(values), DataType::Text | DataType::Varchar(_)) => {
                Value::Text(format_vector_text(values))
            }
            (Value::Bm25Query { terms, index_name }, DataType::Text | DataType::Varchar(_)) => {
                Value::Text(format!(
                    "bm25query(index={},terms={})",
                    index_name,
                    terms.join(" ")
                ))
            }

            (v, DataType::Text | DataType::Varchar(_)) => Value::Text(format!("{v:?}")),

            _ => {
                return Err(EntDbError::InvalidPage(format!(
                    "unsupported cast from {:?} to {:?}",
                    self.data_type(),
                    target
                )));
            }
        };

        if let DataType::Varchar(limit) = target {
            if let Value::Text(s) = &out {
                if s.len() > *limit as usize {
                    return Err(EntDbError::InvalidPage(
                        "varchar length exceeded".to_string(),
                    ));
                }
            }
        }

        Ok(out)
    }

    pub fn eq(&self, other: &Value) -> Result<bool> {
        let (a, b) = align_numeric(self, other)?;
        Ok(a == b)
    }

    pub fn lt(&self, other: &Value) -> Result<bool> {
        let (a, b) = align_numeric(self, other)?;
        compare_aligned_lt(&a, &b)
    }

    pub fn gt(&self, other: &Value) -> Result<bool> {
        let (a, b) = align_numeric(self, other)?;
        compare_aligned_gt(&a, &b)
    }
}

fn parse_timestamp_text(v: &str) -> Result<i64> {
    if let Ok(epoch) = v.parse::<i64>() {
        return Ok(epoch);
    }

    if let Ok(dt) = OffsetDateTime::parse(v, &Rfc3339) {
        return Ok(dt.unix_timestamp());
    }

    // Accept common SQL text literal forms without timezone, interpreted as UTC.
    let formats = [
        "[year]-[month]-[day] [hour]:[minute]:[second]",
        "[year]-[month]-[day]T[hour]:[minute]:[second]",
    ];
    for fmt in formats {
        if let Ok(desc) = time::format_description::parse(fmt) {
            if let Ok(dt) = PrimitiveDateTime::parse(v, &desc) {
                return Ok(dt.assume_utc().unix_timestamp());
            }
        }
    }

    Err(EntDbError::InvalidPage(
        "cast text->timestamp failed".to_string(),
    ))
}

pub fn parse_vector_text(v: &str) -> Result<Vec<f32>> {
    let trimmed = v.trim();
    if !(trimmed.starts_with('[') && trimmed.ends_with(']')) {
        return Err(EntDbError::InvalidPage(
            "cast text->vector failed".to_string(),
        ));
    }
    let body = &trimmed[1..trimmed.len() - 1];
    if body.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    for token in body.split(',') {
        let value = token
            .trim()
            .parse::<f32>()
            .map_err(|_| EntDbError::InvalidPage("cast text->vector failed".to_string()))?;
        out.push(value);
    }
    Ok(out)
}

pub fn format_vector_text(values: &[f32]) -> String {
    let inner = values
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("[{inner}]")
}

fn align_numeric(left: &Value, right: &Value) -> Result<(Value, Value)> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Err(EntDbError::InvalidPage(
            "cannot compare NULL values".to_string(),
        )),
        (Value::Int16(l), Value::Int16(r)) => Ok((Value::Int16(*l), Value::Int16(*r))),
        (Value::Int32(l), Value::Int32(r)) => Ok((Value::Int32(*l), Value::Int32(*r))),
        (Value::Int64(l), Value::Int64(r)) => Ok((Value::Int64(*l), Value::Int64(*r))),
        (Value::Float64(l), Value::Float64(r)) => Ok((Value::Float64(*l), Value::Float64(*r))),
        (Value::Text(l), Value::Text(r)) => Ok((Value::Text(l.clone()), Value::Text(r.clone()))),

        (Value::Int16(l), Value::Int32(r)) => Ok((Value::Int32(*l as i32), Value::Int32(*r))),
        (Value::Int32(l), Value::Int16(r)) => Ok((Value::Int32(*l), Value::Int32(*r as i32))),
        (Value::Int32(l), Value::Int64(r)) => Ok((Value::Int64(*l as i64), Value::Int64(*r))),
        (Value::Int64(l), Value::Int32(r)) => Ok((Value::Int64(*l), Value::Int64(*r as i64))),
        (Value::Int64(l), Value::Float64(r)) => Ok((Value::Float64(*l as f64), Value::Float64(*r))),
        (Value::Float64(l), Value::Int64(r)) => Ok((Value::Float64(*l), Value::Float64(*r as f64))),
        (Value::Int32(l), Value::Float64(r)) => Ok((Value::Float64(*l as f64), Value::Float64(*r))),
        (Value::Float64(l), Value::Int32(r)) => Ok((Value::Float64(*l), Value::Float64(*r as f64))),

        _ => Err(EntDbError::InvalidPage(format!(
            "cannot compare {:?} and {:?}",
            left.data_type(),
            right.data_type()
        ))),
    }
}

fn compare_aligned_lt(left: &Value, right: &Value) -> Result<bool> {
    let out = match (left, right) {
        (Value::Int16(l), Value::Int16(r)) => l < r,
        (Value::Int32(l), Value::Int32(r)) => l < r,
        (Value::Int64(l), Value::Int64(r)) => l < r,
        (Value::Float64(l), Value::Float64(r)) => l < r,
        (Value::Text(l), Value::Text(r)) => l < r,
        _ => {
            return Err(EntDbError::InvalidPage(
                "aligned comparison types mismatch for lt".to_string(),
            ))
        }
    };
    Ok(out)
}

fn compare_aligned_gt(left: &Value, right: &Value) -> Result<bool> {
    let out = match (left, right) {
        (Value::Int16(l), Value::Int16(r)) => l > r,
        (Value::Int32(l), Value::Int32(r)) => l > r,
        (Value::Int64(l), Value::Int64(r)) => l > r,
        (Value::Float64(l), Value::Float64(r)) => l > r,
        (Value::Text(l), Value::Text(r)) => l > r,
        _ => {
            return Err(EntDbError::InvalidPage(
                "aligned comparison types mismatch for gt".to_string(),
            ))
        }
    };
    Ok(out)
}
