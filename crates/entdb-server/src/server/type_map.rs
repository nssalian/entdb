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

use entdb::error::{EntDbError, Result};
use entdb::types::value::{format_vector_text, parse_vector_text};
use entdb::types::{DataType, Value};
use pgwire::api::results::{DataRowEncoder, FieldFormat};
use pgwire::api::Type;
use pgwire::error::PgWireResult;

pub fn ent_to_pg_type(dt: &DataType) -> Type {
    match dt {
        DataType::Boolean => Type::BOOL,
        DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Text => Type::TEXT,
        DataType::Varchar(_) => Type::VARCHAR,
        DataType::Timestamp => Type::TIMESTAMP,
        DataType::Vector(_) => Type::TEXT,
        DataType::Bm25Query => Type::TEXT,
        DataType::Null => Type::UNKNOWN,
    }
}

pub fn value_to_text(val: &Value) -> Option<String> {
    match val {
        Value::Null => None,
        Value::Boolean(v) => Some(if *v { "t".to_string() } else { "f".to_string() }),
        Value::Int16(v) => Some(v.to_string()),
        Value::Int32(v) => Some(v.to_string()),
        Value::Int64(v) => Some(v.to_string()),
        Value::Float32(v) => Some(v.to_string()),
        Value::Float64(v) => Some(v.to_string()),
        Value::Text(v) => Some(v.clone()),
        Value::Timestamp(v) => Some(v.to_string()),
        Value::Vector(v) => Some(format_vector_text(v)),
        Value::Bm25Query { terms, index_name } => Some(format!(
            "bm25query(index={},terms={})",
            index_name,
            terms.join(" ")
        )),
    }
}

pub fn value_to_binary(val: &Value) -> Option<Vec<u8>> {
    match val {
        Value::Null => None,
        Value::Boolean(v) => Some(vec![u8::from(*v)]),
        Value::Int16(v) => Some(v.to_be_bytes().to_vec()),
        Value::Int32(v) => Some(v.to_be_bytes().to_vec()),
        Value::Int64(v) => Some(v.to_be_bytes().to_vec()),
        Value::Float32(v) => Some(v.to_be_bytes().to_vec()),
        Value::Float64(v) => Some(v.to_be_bytes().to_vec()),
        Value::Text(v) => Some(v.as_bytes().to_vec()),
        Value::Timestamp(v) => Some(v.to_be_bytes().to_vec()),
        Value::Vector(v) => Some(format_vector_text(v).into_bytes()),
        Value::Bm25Query { terms, index_name } => {
            Some(format!("bm25query(index={},terms={})", index_name, terms.join(" ")).into_bytes())
        }
    }
}

pub fn text_to_value(text: &str, dt: &DataType) -> Result<Value> {
    match dt {
        DataType::Boolean => text
            .parse::<bool>()
            .map(Value::Boolean)
            .map_err(|e| EntDbError::Query(format!("invalid BOOL literal: {e}"))),
        DataType::Int16 => text
            .parse::<i16>()
            .map(Value::Int16)
            .map_err(|e| EntDbError::Query(format!("invalid INT2 literal: {e}"))),
        DataType::Int32 => text
            .parse::<i32>()
            .map(Value::Int32)
            .map_err(|e| EntDbError::Query(format!("invalid INT4 literal: {e}"))),
        DataType::Int64 => text
            .parse::<i64>()
            .map(Value::Int64)
            .map_err(|e| EntDbError::Query(format!("invalid INT8 literal: {e}"))),
        DataType::Float32 => text
            .parse::<f32>()
            .map(Value::Float32)
            .map_err(|e| EntDbError::Query(format!("invalid FLOAT4 literal: {e}"))),
        DataType::Float64 => text
            .parse::<f64>()
            .map(Value::Float64)
            .map_err(|e| EntDbError::Query(format!("invalid FLOAT8 literal: {e}"))),
        DataType::Text | DataType::Varchar(_) => Ok(Value::Text(text.to_string())),
        DataType::Timestamp => text
            .parse::<i64>()
            .map(Value::Timestamp)
            .map_err(|e| EntDbError::Query(format!("invalid TIMESTAMP literal: {e}"))),
        DataType::Vector(dim) => {
            let parsed = parse_vector_text(text)
                .map_err(|e| EntDbError::Query(format!("invalid VECTOR literal: {e}")))?;
            if parsed.len() != *dim as usize {
                return Err(EntDbError::Query(format!(
                    "invalid VECTOR literal: expected dimension {}, got {}",
                    dim,
                    parsed.len()
                )));
            }
            Ok(Value::Vector(parsed))
        }
        DataType::Null => Ok(Value::Null),
        DataType::Bm25Query => Err(EntDbError::Query(
            "BM25 query values are not supported as direct client literals".to_string(),
        )),
    }
}

pub fn encode_value(
    encoder: &mut DataRowEncoder,
    value: &Value,
    data_type: &Type,
    format: FieldFormat,
) -> PgWireResult<()> {
    match value {
        Value::Null => encoder.encode_field_with_type_and_format(&None::<i32>, data_type, format),
        Value::Boolean(v) => {
            encoder.encode_field_with_type_and_format(&Some(*v), data_type, format)
        }
        Value::Int16(v) => encoder.encode_field_with_type_and_format(&Some(*v), data_type, format),
        Value::Int32(v) => encoder.encode_field_with_type_and_format(&Some(*v), data_type, format),
        Value::Int64(v) => encoder.encode_field_with_type_and_format(&Some(*v), data_type, format),
        Value::Float32(v) => {
            encoder.encode_field_with_type_and_format(&Some(*v), data_type, format)
        }
        Value::Float64(v) => {
            encoder.encode_field_with_type_and_format(&Some(*v), data_type, format)
        }
        Value::Text(v) => {
            encoder.encode_field_with_type_and_format(&Some(v.as_str()), data_type, format)
        }
        Value::Timestamp(v) => {
            let text = v.to_string();
            encoder.encode_field_with_type_and_format(&Some(text), data_type, format)
        }
        Value::Vector(v) => {
            let text = format_vector_text(v);
            encoder.encode_field_with_type_and_format(&Some(text), data_type, format)
        }
        Value::Bm25Query { terms, index_name } => {
            let text = format!("bm25query(index={},terms={})", index_name, terms.join(" "));
            encoder.encode_field_with_type_and_format(&Some(text), data_type, format)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ent_to_pg_type, text_to_value, value_to_binary, value_to_text};
    use entdb::types::{DataType, Value};
    use pgwire::api::Type;

    #[test]
    fn data_type_mapping_covers_supported_set() {
        assert_eq!(ent_to_pg_type(&DataType::Boolean), Type::BOOL);
        assert_eq!(ent_to_pg_type(&DataType::Int16), Type::INT2);
        assert_eq!(ent_to_pg_type(&DataType::Int32), Type::INT4);
        assert_eq!(ent_to_pg_type(&DataType::Int64), Type::INT8);
        assert_eq!(ent_to_pg_type(&DataType::Float32), Type::FLOAT4);
        assert_eq!(ent_to_pg_type(&DataType::Float64), Type::FLOAT8);
        assert_eq!(ent_to_pg_type(&DataType::Text), Type::TEXT);
        assert_eq!(ent_to_pg_type(&DataType::Varchar(64)), Type::VARCHAR);
        assert_eq!(ent_to_pg_type(&DataType::Timestamp), Type::TIMESTAMP);
        assert_eq!(ent_to_pg_type(&DataType::Vector(3)), Type::TEXT);
    }

    #[test]
    fn text_value_round_trip_for_core_types() {
        let cases = vec![
            ("true", DataType::Boolean, Value::Boolean(true)),
            ("12", DataType::Int16, Value::Int16(12)),
            ("123", DataType::Int32, Value::Int32(123)),
            ("1234", DataType::Int64, Value::Int64(1234)),
            ("1.5", DataType::Float64, Value::Float64(1.5)),
            ("ent", DataType::Text, Value::Text("ent".to_string())),
            ("99", DataType::Timestamp, Value::Timestamp(99)),
            (
                "[0.1,0.2,0.3]",
                DataType::Vector(3),
                Value::Vector(vec![0.1, 0.2, 0.3]),
            ),
        ];

        for (input, dt, expected) in cases {
            let parsed = text_to_value(input, &dt).expect("parse value");
            assert_eq!(parsed, expected);
            assert_eq!(value_to_text(&parsed), value_to_text(&expected));
        }
    }

    #[test]
    fn binary_encoding_returns_none_for_null() {
        assert!(value_to_binary(&Value::Null).is_none());
        assert_eq!(value_to_binary(&Value::Int32(7)).expect("bytes").len(), 4);
    }
}
