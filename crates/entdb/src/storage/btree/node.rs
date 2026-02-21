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
use crate::storage::page::{Page, PageId};
use crate::storage::tuple::TupleId;

const NODE_TYPE_INTERNAL: u8 = 1;
const NODE_TYPE_LEAF: u8 = 2;
const NO_PAGE: u32 = u32::MAX;

#[derive(Debug, Clone)]
pub struct InternalNode {
    pub keys: Vec<Vec<u8>>,
    pub children: Vec<PageId>,
}

#[derive(Debug, Clone)]
pub struct LeafNode {
    pub entries: Vec<(Vec<u8>, TupleId)>,
    pub next_leaf: Option<PageId>,
    pub prev_leaf: Option<PageId>,
}

#[derive(Debug, Clone)]
pub enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

impl InternalNode {
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            children: Vec::new(),
        }
    }

    pub fn child_index_for_key(&self, key: &[u8]) -> usize {
        self.keys
            .binary_search_by(|probe| probe.as_slice().cmp(key))
            .map(|idx| idx + 1)
            .unwrap_or_else(|idx| idx)
    }
}

impl LeafNode {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_leaf: None,
            prev_leaf: None,
        }
    }

    pub fn lower_bound(&self, key: &[u8]) -> usize {
        self.entries
            .binary_search_by(|(k, _)| k.as_slice().cmp(key))
            .unwrap_or_else(|idx| idx)
    }
}

impl Node {
    pub fn decode_from_page(page: &Page) -> Result<Self> {
        let body = page.body();
        let mut cur = Cursor::new(body);

        let node_type = cur.read_u8()?;
        match node_type {
            NODE_TYPE_INTERNAL => {
                let key_count = cur.read_u16()? as usize;
                let child_count = cur.read_u16()? as usize;

                let mut keys = Vec::with_capacity(key_count);
                for _ in 0..key_count {
                    keys.push(cur.read_bytes_with_u16_len()?);
                }

                let mut children = Vec::with_capacity(child_count);
                for _ in 0..child_count {
                    children.push(cur.read_u32()?);
                }

                Ok(Node::Internal(InternalNode { keys, children }))
            }
            NODE_TYPE_LEAF => {
                let entry_count = cur.read_u16()? as usize;
                let next = decode_optional_page(cur.read_u32()?);
                let prev = decode_optional_page(cur.read_u32()?);

                let mut entries = Vec::with_capacity(entry_count);
                for _ in 0..entry_count {
                    let key = cur.read_bytes_with_u16_len()?;
                    let page_id = cur.read_u32()?;
                    let slot_id = cur.read_u16()?;
                    entries.push((key, (page_id, slot_id)));
                }

                Ok(Node::Leaf(LeafNode {
                    entries,
                    next_leaf: next,
                    prev_leaf: prev,
                }))
            }
            _ => Err(EntDbError::Corruption(format!(
                "unknown btree node type: {node_type}"
            ))),
        }
    }

    pub fn encode_into_page(&self, page: &mut Page) -> Result<()> {
        let mut out = Vec::new();
        match self {
            Node::Internal(node) => {
                out.push(NODE_TYPE_INTERNAL);
                write_u16(&mut out, node.keys.len())?;
                write_u16(&mut out, node.children.len())?;

                for key in &node.keys {
                    write_u16(&mut out, key.len())?;
                    out.extend_from_slice(key);
                }
                for child in &node.children {
                    out.extend_from_slice(&child.to_le_bytes());
                }
            }
            Node::Leaf(node) => {
                out.push(NODE_TYPE_LEAF);
                write_u16(&mut out, node.entries.len())?;
                out.extend_from_slice(&encode_optional_page(node.next_leaf).to_le_bytes());
                out.extend_from_slice(&encode_optional_page(node.prev_leaf).to_le_bytes());

                for (key, (page_id, slot_id)) in &node.entries {
                    write_u16(&mut out, key.len())?;
                    out.extend_from_slice(key);
                    out.extend_from_slice(&page_id.to_le_bytes());
                    out.extend_from_slice(&slot_id.to_le_bytes());
                }
            }
        }

        if out.len() > page.body().len() {
            return Err(EntDbError::InvalidPage(format!(
                "btree node too large for page: {} bytes",
                out.len()
            )));
        }

        let body = page.body_mut();
        body.fill(0);
        body[..out.len()].copy_from_slice(&out);
        Ok(())
    }
}

fn write_u16(out: &mut Vec<u8>, value: usize) -> Result<()> {
    let v = u16::try_from(value)
        .map_err(|_| EntDbError::InvalidPage("btree node item count overflow".to_string()))?;
    out.extend_from_slice(&v.to_le_bytes());
    Ok(())
}

fn encode_optional_page(page_id: Option<PageId>) -> u32 {
    page_id.unwrap_or(NO_PAGE)
}

fn decode_optional_page(raw: u32) -> Option<PageId> {
    if raw == NO_PAGE {
        None
    } else {
        Some(raw)
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        if self.pos + 1 > self.bytes.len() {
            return Err(EntDbError::Corruption(
                "truncated btree node (u8)".to_string(),
            ));
        }
        let out = self.bytes[self.pos];
        self.pos += 1;
        Ok(out)
    }

    fn read_u16(&mut self) -> Result<u16> {
        if self.pos + 2 > self.bytes.len() {
            return Err(EntDbError::Corruption(
                "truncated btree node (u16)".to_string(),
            ));
        }
        let out = u16::from_le_bytes(
            self.bytes[self.pos..self.pos + 2]
                .try_into()
                .expect("u16 bytes"),
        );
        self.pos += 2;
        Ok(out)
    }

    fn read_u32(&mut self) -> Result<u32> {
        if self.pos + 4 > self.bytes.len() {
            return Err(EntDbError::Corruption(
                "truncated btree node (u32)".to_string(),
            ));
        }
        let out = u32::from_le_bytes(
            self.bytes[self.pos..self.pos + 4]
                .try_into()
                .expect("u32 bytes"),
        );
        self.pos += 4;
        Ok(out)
    }

    fn read_bytes_with_u16_len(&mut self) -> Result<Vec<u8>> {
        let len = self.read_u16()? as usize;
        if self.pos + len > self.bytes.len() {
            return Err(EntDbError::Corruption(
                "truncated btree node (bytes)".to_string(),
            ));
        }
        let out = self.bytes[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(out)
    }
}
