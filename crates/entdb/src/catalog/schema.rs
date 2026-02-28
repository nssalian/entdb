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

use crate::types::{DataType, Value};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.column_index(name).map(|idx| &self.columns[idx])
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn tuple_size_estimate(&self) -> usize {
        self.columns
            .iter()
            .map(|c| match c.data_type {
                DataType::Boolean => 1,
                DataType::Int16 => 2,
                DataType::Int32 => 4,
                DataType::Int64 => 8,
                DataType::Float32 => 4,
                DataType::Float64 => 8,
                DataType::Timestamp => 8,
                DataType::Vector(dim) => (dim as usize) * 4,
                DataType::Varchar(n) => n as usize,
                DataType::Text => 24,
                DataType::Bm25Query => 24,
                DataType::Null => 0,
            })
            .sum()
    }
}
