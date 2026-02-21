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

use crate::wal::log_record::LogRecord;
use proptest::prelude::*;

proptest! {
    #[test]
    fn wal_update_record_round_trip(
        txn_id in any::<u64>(),
        page_id in any::<u32>(),
        offset in any::<u16>(),
        old_data in proptest::collection::vec(any::<u8>(), 0..256),
        new_data in proptest::collection::vec(any::<u8>(), 0..256),
    ) {
        let rec = LogRecord::Update {
            txn_id,
            page_id,
            offset,
            old_data: old_data.clone(),
            new_data: new_data.clone(),
        };

        let entry = LogRecord::encode_entry(42, &rec);
        let (lsn, decoded) = LogRecord::decode_entry(&entry).expect("decode entry");
        prop_assert_eq!(lsn, 42);
        prop_assert_eq!(decoded, rec);
    }

    #[test]
    fn wal_crc_detects_any_single_byte_flip(
        txn_id in any::<u64>(),
        page_id in any::<u32>(),
        offset in any::<u16>(),
        payload in proptest::collection::vec(any::<u8>(), 1..128),
        flip_index in 0usize..64usize,
    ) {
        let rec = LogRecord::Update {
            txn_id,
            page_id,
            offset,
            old_data: payload.clone(),
            new_data: payload,
        };

        let mut entry = LogRecord::encode_entry(7, &rec);
        // avoid mutating length prefix to keep shape valid and target crc validation
        if entry.len() > 8 {
            let idx = 4 + (flip_index % (entry.len() - 8));
            entry[idx] ^= 0x01;
            let res = LogRecord::decode_entry(&entry);
            prop_assert!(res.is_err());
        }
    }
}
