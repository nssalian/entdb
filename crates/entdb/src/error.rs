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

use crate::storage::page::PageId;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EntDbError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    #[error("page {0} not found")]
    PageNotFound(PageId),

    #[error("buffer pool full: no evictable pages")]
    BufferPoolFull,

    #[error("invalid page data: {0}")]
    InvalidPage(String),

    #[error("page {0} is pinned")]
    PagePinned(PageId),

    #[error("page {0} already exists in buffer pool")]
    PageAlreadyPresent(PageId),

    #[error("corruption detected: {0}")]
    Corruption(String),

    #[error("wal error: {0}")]
    Wal(String),

    #[error("query error: {0}")]
    Query(String),
}

pub type Result<T> = std::result::Result<T, EntDbError>;
