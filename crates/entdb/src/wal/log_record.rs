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
use crc32fast::Hasher;

pub type Lsn = u64;

const TYPE_BEGIN: u8 = 1;
const TYPE_COMMIT: u8 = 2;
const TYPE_ABORT: u8 = 3;
const TYPE_UPDATE: u8 = 4;
const TYPE_NEW_PAGE: u8 = 5;
const TYPE_CHECKPOINT: u8 = 6;
const TYPE_CLR: u8 = 7;
const TYPE_BTREE_STRUCTURE: u8 = 8;

const NO_PAGE: u32 = u32::MAX;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogRecord {
    Begin {
        txn_id: u64,
    },
    Commit {
        txn_id: u64,
    },
    Abort {
        txn_id: u64,
    },
    Update {
        txn_id: u64,
        page_id: PageId,
        offset: u16,
        old_data: Vec<u8>,
        new_data: Vec<u8>,
    },
    NewPage {
        txn_id: u64,
        page_id: PageId,
    },
    Checkpoint {
        active_txns: Vec<u64>,
    },
    Clr {
        txn_id: u64,
        undo_next_lsn: Lsn,
    },
    BtreeStructure {
        txn_id: u64,
        event: u8,
        page_id: PageId,
        related_page_id: Option<PageId>,
    },
}

impl LogRecord {
    pub fn serialize(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        match self {
            Self::Begin { txn_id } => {
                payload.push(TYPE_BEGIN);
                payload.extend_from_slice(&txn_id.to_le_bytes());
            }
            Self::Commit { txn_id } => {
                payload.push(TYPE_COMMIT);
                payload.extend_from_slice(&txn_id.to_le_bytes());
            }
            Self::Abort { txn_id } => {
                payload.push(TYPE_ABORT);
                payload.extend_from_slice(&txn_id.to_le_bytes());
            }
            Self::Update {
                txn_id,
                page_id,
                offset,
                old_data,
                new_data,
            } => {
                payload.push(TYPE_UPDATE);
                payload.extend_from_slice(&txn_id.to_le_bytes());
                payload.extend_from_slice(&page_id.to_le_bytes());
                payload.extend_from_slice(&offset.to_le_bytes());
                payload.extend_from_slice(&(old_data.len() as u32).to_le_bytes());
                payload.extend_from_slice(&(new_data.len() as u32).to_le_bytes());
                payload.extend_from_slice(old_data);
                payload.extend_from_slice(new_data);
            }
            Self::NewPage { txn_id, page_id } => {
                payload.push(TYPE_NEW_PAGE);
                payload.extend_from_slice(&txn_id.to_le_bytes());
                payload.extend_from_slice(&page_id.to_le_bytes());
            }
            Self::Checkpoint { active_txns } => {
                payload.push(TYPE_CHECKPOINT);
                payload.extend_from_slice(&(active_txns.len() as u32).to_le_bytes());
                for txn_id in active_txns {
                    payload.extend_from_slice(&txn_id.to_le_bytes());
                }
            }
            Self::Clr {
                txn_id,
                undo_next_lsn,
            } => {
                payload.push(TYPE_CLR);
                payload.extend_from_slice(&txn_id.to_le_bytes());
                payload.extend_from_slice(&undo_next_lsn.to_le_bytes());
            }
            Self::BtreeStructure {
                txn_id,
                event,
                page_id,
                related_page_id,
            } => {
                payload.push(TYPE_BTREE_STRUCTURE);
                payload.extend_from_slice(&txn_id.to_le_bytes());
                payload.push(*event);
                payload.extend_from_slice(&page_id.to_le_bytes());
                payload.extend_from_slice(&related_page_id.unwrap_or(NO_PAGE).to_le_bytes());
            }
        }

        payload
    }

    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(EntDbError::Wal("empty wal payload".to_string()));
        }
        let record_type = data[0];
        let mut cursor = 1;

        let take_u16 = |buf: &[u8], cur: &mut usize| -> Result<u16> {
            if *cur + 2 > buf.len() {
                return Err(EntDbError::Wal("truncated u16 in wal payload".to_string()));
            }
            let v = u16::from_le_bytes(buf[*cur..*cur + 2].try_into().expect("u16 bytes"));
            *cur += 2;
            Ok(v)
        };

        let take_u32 = |buf: &[u8], cur: &mut usize| -> Result<u32> {
            if *cur + 4 > buf.len() {
                return Err(EntDbError::Wal("truncated u32 in wal payload".to_string()));
            }
            let v = u32::from_le_bytes(buf[*cur..*cur + 4].try_into().expect("u32 bytes"));
            *cur += 4;
            Ok(v)
        };

        let take_u64 = |buf: &[u8], cur: &mut usize| -> Result<u64> {
            if *cur + 8 > buf.len() {
                return Err(EntDbError::Wal("truncated u64 in wal payload".to_string()));
            }
            let v = u64::from_le_bytes(buf[*cur..*cur + 8].try_into().expect("u64 bytes"));
            *cur += 8;
            Ok(v)
        };

        let record = match record_type {
            TYPE_BEGIN => Self::Begin {
                txn_id: take_u64(data, &mut cursor)?,
            },
            TYPE_COMMIT => Self::Commit {
                txn_id: take_u64(data, &mut cursor)?,
            },
            TYPE_ABORT => Self::Abort {
                txn_id: take_u64(data, &mut cursor)?,
            },
            TYPE_UPDATE => {
                let txn_id = take_u64(data, &mut cursor)?;
                let page_id = take_u32(data, &mut cursor)?;
                let offset = take_u16(data, &mut cursor)?;
                let old_len = take_u32(data, &mut cursor)? as usize;
                let new_len = take_u32(data, &mut cursor)? as usize;
                if cursor + old_len + new_len > data.len() {
                    return Err(EntDbError::Wal("truncated update wal payload".to_string()));
                }
                let old_data = data[cursor..cursor + old_len].to_vec();
                cursor += old_len;
                let new_data = data[cursor..cursor + new_len].to_vec();
                Self::Update {
                    txn_id,
                    page_id,
                    offset,
                    old_data,
                    new_data,
                }
            }
            TYPE_NEW_PAGE => Self::NewPage {
                txn_id: take_u64(data, &mut cursor)?,
                page_id: take_u32(data, &mut cursor)?,
            },
            TYPE_CHECKPOINT => {
                let cnt = take_u32(data, &mut cursor)? as usize;
                let mut active_txns = Vec::with_capacity(cnt);
                for _ in 0..cnt {
                    active_txns.push(take_u64(data, &mut cursor)?);
                }
                Self::Checkpoint { active_txns }
            }
            TYPE_CLR => Self::Clr {
                txn_id: take_u64(data, &mut cursor)?,
                undo_next_lsn: take_u64(data, &mut cursor)?,
            },
            TYPE_BTREE_STRUCTURE => {
                let txn_id = take_u64(data, &mut cursor)?;
                if cursor >= data.len() {
                    return Err(EntDbError::Wal(
                        "truncated btree structure wal payload".to_string(),
                    ));
                }
                let event = data[cursor];
                cursor += 1;
                let page_id = take_u32(data, &mut cursor)?;
                let related_raw = take_u32(data, &mut cursor)?;
                Self::BtreeStructure {
                    txn_id,
                    event,
                    page_id,
                    related_page_id: if related_raw == NO_PAGE {
                        None
                    } else {
                        Some(related_raw)
                    },
                }
            }
            _ => {
                return Err(EntDbError::Wal(format!(
                    "unknown wal record type: {record_type}"
                )))
            }
        };

        Ok(record)
    }

    pub fn encode_entry(lsn: Lsn, record: &LogRecord) -> Vec<u8> {
        let payload = record.serialize();
        let total_len = 8 + payload.len() + 4;

        let mut entry = Vec::with_capacity(4 + total_len);
        entry.extend_from_slice(&(total_len as u32).to_le_bytes());
        entry.extend_from_slice(&lsn.to_le_bytes());
        entry.extend_from_slice(&payload);

        let crc = checksum(&entry[4..]);
        entry.extend_from_slice(&crc.to_le_bytes());
        entry
    }

    pub fn decode_entry(entry: &[u8]) -> Result<(Lsn, LogRecord)> {
        if entry.len() < 4 + 8 + 1 + 4 {
            return Err(EntDbError::Wal("wal entry too short".to_string()));
        }

        let declared = u32::from_le_bytes(entry[0..4].try_into().expect("len bytes")) as usize;
        if declared + 4 != entry.len() {
            return Err(EntDbError::Wal("wal entry length mismatch".to_string()));
        }

        let crc_start = entry.len() - 4;
        let expected_crc = u32::from_le_bytes(entry[crc_start..].try_into().expect("crc bytes"));
        let actual_crc = checksum(&entry[4..crc_start]);
        if expected_crc != actual_crc {
            return Err(EntDbError::Wal("wal crc mismatch".to_string()));
        }

        let lsn = u64::from_le_bytes(entry[4..12].try_into().expect("lsn bytes"));
        let record = Self::deserialize(&entry[12..crc_start])?;
        Ok((lsn, record))
    }
}

fn checksum(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}
