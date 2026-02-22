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
use crate::fault;
use crate::wal::log_record::{LogRecord, Lsn};
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone)]
pub struct LogRecordEntry {
    pub lsn: Lsn,
    pub record: LogRecord,
}

pub struct LogManager {
    log_path: PathBuf,
    file: Mutex<File>,
    buffer: Mutex<Vec<u8>>,
    buffer_capacity: usize,
    current_lsn: AtomicU64,
    flushed_lsn: AtomicU64,
}

impl LogManager {
    pub fn new(path: impl AsRef<Path>, buffer_capacity: usize) -> Result<Self> {
        let log_path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&log_path)?;

        let entries = read_entries(&mut file)?;
        let current_lsn = entries.last().map(|e| e.lsn + 1).unwrap_or(1);
        let flushed_lsn = entries.last().map(|e| e.lsn).unwrap_or(0);

        Ok(Self {
            log_path,
            file: Mutex::new(file),
            buffer: Mutex::new(Vec::new()),
            buffer_capacity: buffer_capacity.max(1),
            current_lsn: AtomicU64::new(current_lsn),
            flushed_lsn: AtomicU64::new(flushed_lsn),
        })
    }

    pub fn append(&self, record: LogRecord) -> Result<Lsn> {
        if fault::should_fail("wal.append") {
            return Err(EntDbError::Wal("failpoint: wal.append".to_string()));
        }
        let lsn = self.current_lsn.fetch_add(1, Ordering::SeqCst);
        let entry = LogRecord::encode_entry(lsn, &record);

        {
            let mut buf = self.buffer.lock();
            buf.extend_from_slice(&entry);
            if buf.len() >= self.buffer_capacity {
                drop(buf);
                self.flush()?;
            }
        }

        Ok(lsn)
    }

    pub fn flush(&self) -> Result<()> {
        if fault::should_fail("wal.flush") {
            return Err(EntDbError::Wal("failpoint: wal.flush".to_string()));
        }
        let mut buf = self.buffer.lock();
        if buf.is_empty() {
            return Ok(());
        }

        let last_lsn = last_lsn_in_buffer(&buf)?;

        let mut file = self.file.lock();
        file.seek(SeekFrom::End(0))?;
        if fault::should_fail("wal.write") {
            return Err(EntDbError::Wal("failpoint: wal.write".to_string()));
        }
        file.write_all(&buf)?;
        if fault::should_fail("wal.sync") {
            return Err(EntDbError::Wal("failpoint: wal.sync".to_string()));
        }
        file.sync_all()?;

        buf.clear();
        self.flushed_lsn.store(last_lsn, Ordering::Release);
        Ok(())
    }

    pub fn flush_up_to(&self, lsn: Lsn) -> Result<()> {
        if self.flushed_lsn() >= lsn {
            return Ok(());
        }
        self.flush()
    }

    pub fn current_lsn(&self) -> Lsn {
        self.current_lsn.load(Ordering::Acquire).saturating_sub(1)
    }

    pub fn flushed_lsn(&self) -> Lsn {
        self.flushed_lsn.load(Ordering::Acquire)
    }

    pub fn iter_from(&self, start_lsn: Lsn) -> Result<LogIterator> {
        self.flush()?;
        let mut file = self.file.lock();
        let entries = read_entries(&mut file)?;
        let filtered = entries
            .into_iter()
            .filter(|e| e.lsn >= start_lsn)
            .collect::<Vec<_>>();
        Ok(LogIterator {
            entries: filtered,
            pos: 0,
        })
    }

    pub fn log_path(&self) -> &Path {
        &self.log_path
    }
}

pub struct LogIterator {
    entries: Vec<LogRecordEntry>,
    pos: usize,
}

impl Iterator for LogIterator {
    type Item = LogRecordEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.entries.len() {
            return None;
        }
        let out = self.entries[self.pos].clone();
        self.pos += 1;
        Some(out)
    }
}

fn read_entries(file: &mut File) -> Result<Vec<LogRecordEntry>> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;

    let mut cursor = 0;
    let mut entries = Vec::new();

    while cursor < bytes.len() {
        if cursor + 4 > bytes.len() {
            return Err(EntDbError::Wal("truncated wal length prefix".to_string()));
        }

        let len =
            u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().expect("len bytes")) as usize;
        let end = cursor + 4 + len;
        if end > bytes.len() {
            return Err(EntDbError::Wal("truncated wal entry".to_string()));
        }

        let entry = &bytes[cursor..end];
        let (lsn, record) = LogRecord::decode_entry(entry)?;
        entries.push(LogRecordEntry { lsn, record });
        cursor = end;
    }

    Ok(entries)
}

fn last_lsn_in_buffer(buf: &[u8]) -> Result<Lsn> {
    let mut cursor = 0;
    let mut last = 0;

    while cursor < buf.len() {
        if cursor + 4 > buf.len() {
            return Err(EntDbError::Wal("truncated buffered wal length".to_string()));
        }
        let len =
            u32::from_le_bytes(buf[cursor..cursor + 4].try_into().expect("len bytes")) as usize;
        let end = cursor + 4 + len;
        if end > buf.len() {
            return Err(EntDbError::Wal("truncated buffered wal entry".to_string()));
        }

        let (lsn, _) = LogRecord::decode_entry(&buf[cursor..end])?;
        last = lsn;
        cursor = end;
    }

    Ok(last)
}
