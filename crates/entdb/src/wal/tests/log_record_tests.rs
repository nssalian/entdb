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

#[test]
fn log_record_round_trip_all_types() {
    let cases = vec![
        LogRecord::Begin { txn_id: 1 },
        LogRecord::Commit { txn_id: 1 },
        LogRecord::Abort { txn_id: 2 },
        LogRecord::Update {
            txn_id: 3,
            page_id: 99,
            offset: 7,
            old_data: vec![1, 2, 3],
            new_data: vec![4, 5, 6],
        },
        LogRecord::NewPage {
            txn_id: 4,
            page_id: 100,
        },
        LogRecord::Checkpoint {
            active_txns: vec![10, 11, 12],
        },
        LogRecord::Clr {
            txn_id: 9,
            undo_next_lsn: 42,
        },
        LogRecord::BtreeStructure {
            txn_id: 11,
            event: 2,
            page_id: 123,
            related_page_id: Some(124),
        },
    ];

    for (idx, record) in cases.into_iter().enumerate() {
        let entry = LogRecord::encode_entry((idx + 1) as u64, &record);
        let (lsn, decoded) = LogRecord::decode_entry(&entry).expect("decode entry");
        assert_eq!(lsn, (idx + 1) as u64);
        assert_eq!(decoded, record);
    }
}

#[test]
fn log_record_detects_crc_corruption() {
    let entry = LogRecord::encode_entry(99, &LogRecord::Begin { txn_id: 1 });
    let mut tampered = entry.clone();
    tampered[12] ^= 0xAA;
    let err = LogRecord::decode_entry(&tampered).expect_err("crc mismatch expected");
    assert!(err.to_string().contains("crc mismatch"));
}

#[test]
fn log_record_detects_truncated_entry() {
    let entry = LogRecord::encode_entry(10, &LogRecord::Commit { txn_id: 5 });
    let truncated = &entry[..entry.len() - 2];
    let err = LogRecord::decode_entry(truncated).expect_err("length mismatch expected");
    assert!(err.to_string().contains("length mismatch"));
}
