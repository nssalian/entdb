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
use proptest::prelude::*;

proptest! {
    #[test]
    fn value_round_trip_i64(v in any::<i64>()) {
        let val = Value::Int64(v);
        let enc = val.serialize();
        let dec = Value::deserialize(&enc[1..], &DataType::Int64).expect("decode int64");
        prop_assert_eq!(dec, val);
    }

    #[test]
    fn value_round_trip_text(s in ".{0,256}") {
        let val = Value::Text(s.clone());
        let enc = val.serialize();
        let dec = Value::deserialize(&enc[1..], &DataType::Text).expect("decode text");
        prop_assert_eq!(dec, Value::Text(s));
    }

    #[test]
    fn varchar_cast_respects_length(s in ".{0,64}") {
        let val = Value::Text(s.clone());
        let limit = 16_u32;
        let cast = val.cast_to(&DataType::Varchar(limit));

        if s.len() <= limit as usize {
            prop_assert!(cast.is_ok());
        } else {
            prop_assert!(cast.is_err());
        }
    }
}
