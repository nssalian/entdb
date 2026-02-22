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

use crate::storage::page::Page;
use crate::storage::slotted_page::SlottedPage;
use crate::storage::tuple::{Column, DataType, Schema, Tuple, Value};
use proptest::prelude::*;

proptest! {
    #[test]
    fn tuple_round_trip_property(
        active in any::<bool>(),
        age in any::<i32>(),
        balance in any::<i64>(),
        name in ".{0,64}",
    ) {
        let schema = Schema::new(vec![
            Column { name: "active".to_string(), data_type: DataType::Boolean, nullable: false },
            Column { name: "age".to_string(), data_type: DataType::Int32, nullable: false },
            Column { name: "balance".to_string(), data_type: DataType::Int64, nullable: false },
            Column { name: "name".to_string(), data_type: DataType::Text, nullable: true },
        ]);

        let values = vec![
            Value::Boolean(active),
            Value::Int32(age),
            Value::Int64(balance),
            Value::Text(name.clone()),
        ];

        let tuple = Tuple::serialize(&values, &schema).expect("serialize tuple");
        let decoded = tuple.deserialize(&schema).expect("deserialize tuple");
        prop_assert_eq!(decoded, values);
    }

    #[test]
    fn slotted_page_insert_get_property(payload in proptest::collection::vec(any::<u8>(), 1..256)) {
        let mut page = Page::new(1);
        let mut sp = SlottedPage::init(&mut page);
        let slot = sp.insert(&payload).expect("insert");
        let got = sp.get(slot).expect("get");
        prop_assert_eq!(got, payload.as_slice());
    }
}
