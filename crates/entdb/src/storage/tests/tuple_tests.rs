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

use crate::storage::tuple::{Column, DataType, Schema, Tuple, Value};

#[test]
fn tuple_round_trip_mixed_types() {
    let schema = Schema::new(vec![
        Column {
            name: "active".to_string(),
            data_type: DataType::Boolean,
            nullable: false,
        },
        Column {
            name: "age".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        },
        Column {
            name: "balance".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        },
        Column {
            name: "name".to_string(),
            data_type: DataType::Text,
            nullable: true,
        },
    ]);

    let values = vec![
        Value::Boolean(true),
        Value::Int32(42),
        Value::Int64(9_999_999),
        Value::Text("frodo".to_string()),
    ];

    let tuple = Tuple::serialize(&values, &schema).expect("serialize tuple");
    let decoded = tuple.deserialize(&schema).expect("deserialize tuple");

    assert_eq!(decoded, values);
}

#[test]
fn tuple_round_trip_with_null_text() {
    let schema = Schema::new(vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        },
        Column {
            name: "note".to_string(),
            data_type: DataType::Text,
            nullable: true,
        },
    ]);

    let values = vec![Value::Int32(7), Value::Null];
    let tuple = Tuple::serialize(&values, &schema).expect("serialize tuple");
    let decoded = tuple.deserialize(&schema).expect("deserialize tuple");

    assert_eq!(decoded, values);
}

#[test]
fn tuple_rejects_non_nullable_null() {
    let schema = Schema::new(vec![Column {
        name: "id".to_string(),
        data_type: DataType::Int32,
        nullable: false,
    }]);
    let err = Tuple::serialize(&[Value::Null], &schema).expect_err("must reject null");
    assert!(err.to_string().contains("not nullable"));
}

#[test]
fn tuple_rejects_type_mismatch() {
    let schema = Schema::new(vec![Column {
        name: "id".to_string(),
        data_type: DataType::Int32,
        nullable: false,
    }]);
    let err = Tuple::serialize(&[Value::Text("x".to_string())], &schema)
        .expect_err("must reject mismatched type");
    assert!(err.to_string().contains("type mismatch"));
}

#[test]
fn tuple_deserialize_rejects_corrupt_utf8() {
    let schema = Schema::new(vec![Column {
        name: "txt".to_string(),
        data_type: DataType::Text,
        nullable: false,
    }]);

    // null bitmap(1 byte) + text pointer(offset=0,len=1) + invalid utf8(0xFF)
    let mut raw = vec![0_u8];
    raw.extend_from_slice(&0_u32.to_le_bytes());
    raw.extend_from_slice(&1_u32.to_le_bytes());
    raw.push(0xFF);

    let tuple = Tuple::new(raw);
    let err = tuple
        .deserialize(&schema)
        .expect_err("invalid utf8 must fail");
    assert!(err.to_string().contains("utf-8"));
}
