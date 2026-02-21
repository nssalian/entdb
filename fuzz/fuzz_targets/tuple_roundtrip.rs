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

#![no_main]

use entdb::storage::tuple::{Column, DataType, Schema, Tuple, Value};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data).to_string();
    let schema = Schema::new(vec![
        Column {
            name: "t".to_string(),
            data_type: DataType::Text,
            nullable: false,
        },
        Column {
            name: "n".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        },
    ]);

    let values = vec![Value::Text(text), Value::Int32(data.len() as i32)];
    if let Ok(tuple) = Tuple::serialize(&values, &schema) {
        let _ = tuple.deserialize(&schema);
    }
});
