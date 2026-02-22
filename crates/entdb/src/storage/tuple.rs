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
use crate::storage::page::PageId;

pub type SlotId = u16;
pub type TupleId = (PageId, SlotId);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Boolean,
    Int32,
    Int64,
    Text,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tuple {
    pub data: Vec<u8>,
}

impl Tuple {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn serialize(values: &[Value], schema: &Schema) -> Result<Self> {
        if values.len() != schema.columns.len() {
            return Err(EntDbError::InvalidPage(format!(
                "value count {} does not match schema column count {}",
                values.len(),
                schema.columns.len()
            )));
        }

        let col_count = schema.columns.len();
        let bitmap_len = col_count.div_ceil(8);
        let mut null_bitmap = vec![0_u8; bitmap_len];
        let mut fixed_area = Vec::new();
        let mut var_area = Vec::new();

        for (idx, (value, column)) in values.iter().zip(&schema.columns).enumerate() {
            let is_null = matches!(value, Value::Null);
            if is_null {
                if !column.nullable {
                    return Err(EntDbError::InvalidPage(format!(
                        "column '{}' is not nullable",
                        column.name
                    )));
                }
                null_bitmap[idx / 8] |= 1 << (idx % 8);
            }

            match (&column.data_type, value) {
                (_, Value::Null) => match column.data_type {
                    DataType::Boolean => fixed_area.push(0),
                    DataType::Int32 => fixed_area.extend_from_slice(&0_i32.to_le_bytes()),
                    DataType::Int64 => fixed_area.extend_from_slice(&0_i64.to_le_bytes()),
                    DataType::Text => {
                        fixed_area.extend_from_slice(&0_u32.to_le_bytes());
                        fixed_area.extend_from_slice(&0_u32.to_le_bytes());
                    }
                },
                (DataType::Boolean, Value::Boolean(v)) => fixed_area.push(u8::from(*v)),
                (DataType::Int32, Value::Int32(v)) => {
                    fixed_area.extend_from_slice(&v.to_le_bytes())
                }
                (DataType::Int64, Value::Int64(v)) => {
                    fixed_area.extend_from_slice(&v.to_le_bytes())
                }
                (DataType::Text, Value::Text(v)) => {
                    let offset = var_area.len() as u32;
                    let bytes = v.as_bytes();
                    let len = bytes.len() as u32;
                    fixed_area.extend_from_slice(&offset.to_le_bytes());
                    fixed_area.extend_from_slice(&len.to_le_bytes());
                    var_area.extend_from_slice(bytes);
                }
                _ => {
                    return Err(EntDbError::InvalidPage(format!(
                        "type mismatch for column '{}'",
                        column.name
                    )));
                }
            }
        }

        let mut data = Vec::with_capacity(bitmap_len + fixed_area.len() + var_area.len());
        data.extend_from_slice(&null_bitmap);
        data.extend_from_slice(&fixed_area);
        data.extend_from_slice(&var_area);

        Ok(Self { data })
    }

    pub fn deserialize(&self, schema: &Schema) -> Result<Vec<Value>> {
        let col_count = schema.columns.len();
        let bitmap_len = col_count.div_ceil(8);
        if self.data.len() < bitmap_len {
            return Err(EntDbError::Corruption(
                "tuple data shorter than null bitmap".to_string(),
            ));
        }

        let bitmap = &self.data[..bitmap_len];
        let mut cursor = bitmap_len;

        let mut fixed_offsets = Vec::with_capacity(col_count);
        for col in &schema.columns {
            let field_size = match col.data_type {
                DataType::Boolean => 1,
                DataType::Int32 => 4,
                DataType::Int64 => 8,
                DataType::Text => 8,
            };
            if cursor + field_size > self.data.len() {
                return Err(EntDbError::Corruption(
                    "tuple fixed-length region is truncated".to_string(),
                ));
            }
            fixed_offsets.push((cursor, field_size));
            cursor += field_size;
        }

        let var_base = cursor;
        let mut out = Vec::with_capacity(col_count);

        for (idx, col) in schema.columns.iter().enumerate() {
            let is_null = (bitmap[idx / 8] & (1 << (idx % 8))) != 0;
            if is_null {
                out.push(Value::Null);
                continue;
            }

            let (off, _) = fixed_offsets[idx];
            let value = match col.data_type {
                DataType::Boolean => Value::Boolean(self.data[off] != 0),
                DataType::Int32 => {
                    let v =
                        i32::from_le_bytes(self.data[off..off + 4].try_into().expect("i32 bytes"));
                    Value::Int32(v)
                }
                DataType::Int64 => {
                    let v =
                        i64::from_le_bytes(self.data[off..off + 8].try_into().expect("i64 bytes"));
                    Value::Int64(v)
                }
                DataType::Text => {
                    let rel = u32::from_le_bytes(
                        self.data[off..off + 4]
                            .try_into()
                            .expect("text offset bytes"),
                    ) as usize;
                    let len = u32::from_le_bytes(
                        self.data[off + 4..off + 8]
                            .try_into()
                            .expect("text length bytes"),
                    ) as usize;

                    let start = var_base + rel;
                    let end = start + len;
                    if end > self.data.len() {
                        return Err(EntDbError::Corruption(
                            "tuple variable-length field is out of bounds".to_string(),
                        ));
                    }

                    let s = std::str::from_utf8(&self.data[start..end]).map_err(|_| {
                        EntDbError::Corruption(
                            "tuple text field contains invalid utf-8".to_string(),
                        )
                    })?;
                    Value::Text(s.to_string())
                }
            };
            out.push(value);
        }

        Ok(out)
    }
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }
}
