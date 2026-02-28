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

#[test]
fn value_round_trip_serialize_deserialize() {
    let cases = vec![
        (Value::Boolean(true), DataType::Boolean),
        (Value::Int16(7), DataType::Int16),
        (Value::Int32(99), DataType::Int32),
        (Value::Int64(777), DataType::Int64),
        (Value::Float32(1.5), DataType::Float32),
        (Value::Float64(2.5), DataType::Float64),
        (Value::Text("ent".to_string()), DataType::Text),
        (Value::Timestamp(1_700_000_000), DataType::Timestamp),
        (Value::Vector(vec![0.1, 0.2, 0.3]), DataType::Vector(3)),
        (
            Value::Bm25Query {
                terms: vec!["database".to_string(), "search".to_string()],
                index_name: "idx_docs".to_string(),
            },
            DataType::Bm25Query,
        ),
    ];

    for (v, dt) in cases {
        let bytes = v.serialize();
        let payload = &bytes[1..];
        let decoded = Value::deserialize(payload, &dt).expect("decode");
        assert_eq!(decoded, v);
    }
}

#[test]
fn value_casts_and_comparisons_work() {
    let v = Value::Int32(42);
    let casted = v.cast_to(&DataType::Int64).expect("cast int32->int64");
    assert_eq!(casted, Value::Int64(42));

    let text = Value::Text("123".to_string());
    let parsed = text.cast_to(&DataType::Int32).expect("cast text->int32");
    assert_eq!(parsed, Value::Int32(123));

    assert!(Value::Int32(1).lt(&Value::Int64(2)).expect("lt"));
    assert!(Value::Int64(2).gt(&Value::Int32(1)).expect("gt"));
    assert!(Value::Text("a".to_string())
        .eq(&Value::Text("a".to_string()))
        .expect("eq"));
}

#[test]
fn value_invalid_cast_fails() {
    let err = Value::Text("abc".to_string())
        .cast_to(&DataType::Int32)
        .expect_err("invalid cast should fail");
    assert!(err.to_string().contains("cast text->int32 failed"));
}

#[test]
fn value_float64_to_float32_cast_works_and_checks_overflow() {
    let v = Value::Float64(1.25);
    let casted = v
        .cast_to(&DataType::Float32)
        .expect("cast float64->float32");
    assert_eq!(casted, Value::Float32(1.25_f32));

    let overflow = Value::Float64(f64::MAX)
        .cast_to(&DataType::Float32)
        .expect_err("overflow cast should fail");
    assert!(overflow
        .to_string()
        .contains("cast float64->float32 overflow"));
}

#[test]
fn value_text_to_timestamp_cast_supports_common_formats() {
    let ts = Value::Text("2024-01-01 00:00:00".to_string())
        .cast_to(&DataType::Timestamp)
        .expect("cast timestamp text");
    assert_eq!(ts, Value::Timestamp(1_704_067_200));

    let ts_rfc3339 = Value::Text("2024-01-01T00:00:00Z".to_string())
        .cast_to(&DataType::Timestamp)
        .expect("cast rfc3339 timestamp");
    assert_eq!(ts_rfc3339, Value::Timestamp(1_704_067_200));
}

#[test]
fn value_text_vector_cast_round_trip() {
    let v = Value::Text("[1.0, 2.5,3]".to_string())
        .cast_to(&DataType::Vector(3))
        .expect("cast text->vector");
    assert_eq!(v, Value::Vector(vec![1.0, 2.5, 3.0]));

    let text = v.cast_to(&DataType::Text).expect("cast vector->text");
    assert_eq!(text, Value::Text("[1,2.5,3]".to_string()));
}

#[test]
fn value_vector_cast_dimension_mismatch_fails() {
    let err = Value::Text("[1,2]".to_string())
        .cast_to(&DataType::Vector(3))
        .expect_err("dimension mismatch should fail");
    assert!(err.to_string().contains("vector dimension mismatch"));
}
