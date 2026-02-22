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
use crate::storage::btree::iterator::BTreeIterator;
use crate::storage::btree::node::{InternalNode, LeafNode, Node};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::page::{PageId, PageType};
use crate::storage::tuple::TupleId;
use crate::wal::log_manager::LogManager;
use crate::wal::log_record::LogRecord;
use parking_lot::RwLock;
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

const BTREE_META_MAGIC: u32 = 0x4254_5245; // "BTRE"
const ROOT_UPDATE_CLEAN: u8 = 0;
const ROOT_UPDATE_PENDING: u8 = 1;
const INVALID_NODE_PAGE: u32 = u32::MAX;

#[derive(Debug, Clone)]
pub struct KeySchema;

impl KeySchema {
    pub fn compare(&self, left: &[u8], right: &[u8]) -> std::cmp::Ordering {
        left.cmp(right)
    }
}

#[derive(Debug, Clone, Copy)]
struct BTreeMeta {
    root_page_id: PageId,
    pending_root_page_id: Option<PageId>,
    root_update_state: u8,
}

pub struct BTree {
    root_page_id: AtomicU32,
    meta_page_id: Option<PageId>,
    buffer_pool: Arc<BufferPool>,
    key_schema: KeySchema,
    order: usize,
    latch: RwLock<()>,
    log_manager: Option<Arc<LogManager>>,
}

impl BTree {
    pub fn create(buffer_pool: Arc<BufferPool>, key_schema: KeySchema) -> Result<Self> {
        let root_page_id = {
            let mut root_page = buffer_pool.new_page()?;
            let root_id = root_page.page_id();
            Node::Leaf(LeafNode::new()).encode_into_page(&mut root_page)?;
            root_page.set_page_type(PageType::BTreeLeaf);
            root_page.mark_dirty();
            root_id
        };

        let meta_page_id = {
            let mut meta_page = buffer_pool.new_page()?;
            let meta_id = meta_page.page_id();
            Self::write_meta_page(
                &mut meta_page,
                BTreeMeta {
                    root_page_id,
                    pending_root_page_id: None,
                    root_update_state: ROOT_UPDATE_CLEAN,
                },
            )?;
            meta_page.mark_dirty();
            meta_id
        };

        Ok(Self {
            root_page_id: AtomicU32::new(root_page_id),
            meta_page_id: Some(meta_page_id),
            buffer_pool,
            key_schema,
            order: 32,
            latch: RwLock::new(()),
            log_manager: None,
        })
    }

    pub fn create_with_order(
        buffer_pool: Arc<BufferPool>,
        key_schema: KeySchema,
        order: usize,
    ) -> Result<Self> {
        let mut tree = Self::create(buffer_pool, key_schema)?;
        tree.order = order.max(3);
        Ok(tree)
    }

    pub fn open(
        root_or_meta_page_id: PageId,
        buffer_pool: Arc<BufferPool>,
        key_schema: KeySchema,
    ) -> Self {
        let maybe_meta = Self::read_meta_page_if_present(&buffer_pool, root_or_meta_page_id)
            .ok()
            .flatten();

        if let Some(mut meta) = maybe_meta {
            // Recovery rule for partially-applied root updates:
            // if pending root is recorded, treat it as committed and finalize metadata.
            if meta.root_update_state == ROOT_UPDATE_PENDING {
                if let Some(pending_root) = meta.pending_root_page_id {
                    meta.root_page_id = pending_root;
                }
                meta.pending_root_page_id = None;
                meta.root_update_state = ROOT_UPDATE_CLEAN;
                if let Ok(mut page) = buffer_pool.fetch_page(root_or_meta_page_id) {
                    let _ = Self::write_meta_page(&mut page, meta);
                    page.mark_dirty();
                    drop(page);
                    let _ = buffer_pool.flush_page(root_or_meta_page_id);
                }
            }

            return Self {
                root_page_id: AtomicU32::new(meta.root_page_id),
                meta_page_id: Some(root_or_meta_page_id),
                buffer_pool,
                key_schema,
                order: 32,
                latch: RwLock::new(()),
                log_manager: None,
            };
        }

        // Backward-compatible fallback for trees persisted before metadata page support.
        Self {
            root_page_id: AtomicU32::new(root_or_meta_page_id),
            meta_page_id: None,
            buffer_pool,
            key_schema,
            order: 32,
            latch: RwLock::new(()),
            log_manager: None,
        }
    }

    pub fn create_with_wal(
        buffer_pool: Arc<BufferPool>,
        key_schema: KeySchema,
        log_manager: Arc<LogManager>,
    ) -> Result<Self> {
        let mut tree = Self::create(buffer_pool, key_schema)?;
        tree.log_manager = Some(log_manager);
        Ok(tree)
    }

    pub fn open_with_wal(
        root_or_meta_page_id: PageId,
        buffer_pool: Arc<BufferPool>,
        key_schema: KeySchema,
        log_manager: Arc<LogManager>,
    ) -> Self {
        let mut tree = Self::open(root_or_meta_page_id, buffer_pool, key_schema);
        tree.log_manager = Some(log_manager);
        tree
    }

    pub fn insert(&self, key: &[u8], tuple_id: TupleId) -> Result<()> {
        let _guard = self.latch.write();
        let root = self.root_page_id.load(Ordering::Acquire);

        let split = self.insert_into_node(root, key.to_vec(), tuple_id)?;
        if let Some((separator, right_page_id)) = split {
            let old_root = self.root_page_id.load(Ordering::Acquire);
            let new_root = self.allocate_empty_node_page()?;

            let mut root_node = InternalNode::new();
            root_node.keys.push(separator);
            root_node.children.push(old_root);
            root_node.children.push(right_page_id);
            if fault::should_fail("btree.before_root_update") {
                return Err(EntDbError::Corruption(
                    "failpoint: btree.before_root_update".to_string(),
                ));
            }
            self.store_node(new_root, &Node::Internal(root_node))?;
            self.buffer_pool.flush_page(new_root)?;

            self.log_structure(4, new_root, Some(old_root))?;
            self.stage_root_update(new_root)?;

            if fault::should_fail("btree.after_root_prepare") {
                return Err(EntDbError::Corruption(
                    "failpoint: btree.after_root_prepare".to_string(),
                ));
            }

            self.commit_root_update(new_root)?;
        }

        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let _guard = self.latch.write();
        let root = self.root_page_id.load(Ordering::Acquire);

        let leaf_id = self.find_leaf_node_id(key, root)?;
        let mut leaf = match self.load_node(leaf_id)? {
            Node::Leaf(leaf) => leaf,
            _ => return Err(EntDbError::Corruption("leaf node not found".to_string())),
        };

        if let Ok(idx) = leaf
            .entries
            .binary_search_by(|(k, _)| self.key_schema.compare(k.as_slice(), key))
        {
            leaf.entries.remove(idx);
            self.store_node(leaf_id, &Node::Leaf(leaf))?;
        }

        Ok(())
    }

    pub fn search(&self, key: &[u8]) -> Result<Option<TupleId>> {
        let _guard = self.latch.read();
        let root = self.root_page_id.load(Ordering::Acquire);
        let leaf_id = self.find_leaf_node_id(key, root)?;

        match self.load_node(leaf_id)? {
            Node::Leaf(leaf) => {
                let found = leaf
                    .entries
                    .binary_search_by(|(k, _)| self.key_schema.compare(k.as_slice(), key))
                    .ok()
                    .map(|idx| leaf.entries[idx].1);
                Ok(found)
            }
            _ => Err(EntDbError::Corruption("leaf node not found".to_string())),
        }
    }

    pub fn range_scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<BTreeIterator> {
        let _guard = self.latch.read();
        let root = self.root_page_id.load(Ordering::Acquire);

        let mut current_leaf_id = if let Some(start_key) = start {
            self.find_leaf_node_id(start_key, root)?
        } else {
            self.leftmost_leaf_id(root)?
        };

        let mut out = Vec::new();
        loop {
            let leaf = match self.load_node(current_leaf_id)? {
                Node::Leaf(leaf) => leaf,
                _ => {
                    return Err(EntDbError::Corruption(
                        "range scan expected leaf node".to_string(),
                    ));
                }
            };

            for (k, v) in &leaf.entries {
                if let Some(start_key) = start {
                    if self.key_schema.compare(k.as_slice(), start_key).is_lt() {
                        continue;
                    }
                }
                if let Some(end_key) = end {
                    if self.key_schema.compare(k.as_slice(), end_key).is_gt() {
                        return Ok(BTreeIterator::new(out));
                    }
                }
                out.push((k.clone(), *v));
            }

            match leaf.next_leaf {
                Some(next) => current_leaf_id = next,
                None => break,
            }
        }

        Ok(BTreeIterator::new(out))
    }

    pub fn root_page_id(&self) -> PageId {
        self.meta_page_id
            .unwrap_or_else(|| self.root_page_id.load(Ordering::Acquire))
    }

    pub fn check_integrity(&self) -> Result<()> {
        let _guard = self.latch.read();
        let root = self.root_page_id.load(Ordering::Acquire);

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(root);

        while let Some(node_id) = queue.pop_front() {
            if !visited.insert(node_id) {
                return Err(EntDbError::Corruption(format!(
                    "cycle detected in btree at node {node_id}"
                )));
            }

            match self.load_node(node_id)? {
                Node::Internal(internal) => {
                    if internal.children.len() != internal.keys.len() + 1 {
                        return Err(EntDbError::Corruption(format!(
                            "internal node {node_id} has invalid child/key cardinality"
                        )));
                    }
                    if !is_sorted(&internal.keys, |a, b| self.key_schema.compare(a, b)) {
                        return Err(EntDbError::Corruption(format!(
                            "internal node {node_id} keys are not sorted"
                        )));
                    }
                    for child in internal.children {
                        queue.push_back(child);
                    }
                }
                Node::Leaf(leaf) => {
                    if !is_sorted_entries(&leaf.entries, |a, b| self.key_schema.compare(a, b)) {
                        return Err(EntDbError::Corruption(format!(
                            "leaf node {node_id} entries are not sorted"
                        )));
                    }
                }
            }
        }

        // Verify leaf chain is globally ordered and links are coherent.
        let leftmost = self.leftmost_leaf_id(root)?;
        let mut chain = Vec::new();
        let mut current = Some(leftmost);
        let mut prev: Option<PageId> = None;
        let mut seen = HashSet::new();

        while let Some(leaf_id) = current {
            if !seen.insert(leaf_id) {
                return Err(EntDbError::Corruption(
                    "cycle detected in leaf sibling chain".to_string(),
                ));
            }
            let leaf = match self.load_node(leaf_id)? {
                Node::Leaf(leaf) => leaf,
                _ => {
                    return Err(EntDbError::Corruption(
                        "leaf chain points to non-leaf".to_string(),
                    ))
                }
            };

            if leaf.prev_leaf != prev {
                return Err(EntDbError::Corruption(format!(
                    "leaf prev pointer mismatch at {leaf_id}"
                )));
            }

            for (k, _) in &leaf.entries {
                chain.push(k.clone());
            }

            prev = Some(leaf_id);
            current = leaf.next_leaf;
        }

        if !is_sorted(&chain, |a, b| self.key_schema.compare(a, b)) {
            return Err(EntDbError::Corruption(
                "global leaf key order invariant violated".to_string(),
            ));
        }

        Ok(())
    }

    pub fn rebuild_leaf_links(&self) -> Result<()> {
        let _guard = self.latch.write();
        let root = self.root_page_id.load(Ordering::Acquire);
        let mut leaves = self.collect_leaves(root)?;

        leaves.sort_by(|(a_id, a), (b_id, b)| {
            let a_key = a.entries.first().map(|(k, _)| k.as_slice()).unwrap_or(&[]);
            let b_key = b.entries.first().map(|(k, _)| k.as_slice()).unwrap_or(&[]);
            self.key_schema
                .compare(a_key, b_key)
                .then_with(|| a_id.cmp(b_id))
        });

        for i in 0..leaves.len() {
            let prev = if i == 0 { None } else { Some(leaves[i - 1].0) };
            let next = if i + 1 < leaves.len() {
                Some(leaves[i + 1].0)
            } else {
                None
            };
            let (leaf_id, mut leaf) = leaves[i].clone();
            leaf.prev_leaf = prev;
            leaf.next_leaf = next;
            self.store_node(leaf_id, &Node::Leaf(leaf))?;
        }

        self.buffer_pool.flush_all()?;
        Ok(())
    }

    fn collect_leaves(&self, start: PageId) -> Result<Vec<(PageId, LeafNode)>> {
        let mut out = Vec::new();
        let mut stack = vec![start];
        let mut seen = HashSet::new();

        while let Some(node_id) = stack.pop() {
            if !seen.insert(node_id) {
                continue;
            }
            match self.load_node(node_id)? {
                Node::Leaf(leaf) => out.push((node_id, leaf)),
                Node::Internal(internal) => {
                    for child in internal.children {
                        stack.push(child);
                    }
                }
            }
        }

        Ok(out)
    }

    fn insert_into_node(
        &self,
        node_id: PageId,
        key: Vec<u8>,
        tuple_id: TupleId,
    ) -> Result<Option<(Vec<u8>, PageId)>> {
        let node = self.load_node(node_id)?;

        match node {
            Node::Leaf(mut leaf) => {
                let pos = leaf.lower_bound(&key);
                if pos < leaf.entries.len()
                    && self.key_schema.compare(&leaf.entries[pos].0, &key).is_eq()
                {
                    leaf.entries[pos].1 = tuple_id;
                } else {
                    leaf.entries.insert(pos, (key, tuple_id));
                }

                if leaf.entries.len() <= self.order {
                    self.store_node(node_id, &Node::Leaf(leaf))?;
                    return Ok(None);
                }

                if fault::should_fail("btree.before_leaf_split") {
                    return Err(EntDbError::Corruption(
                        "failpoint: btree.before_leaf_split".to_string(),
                    ));
                }
                let split_idx = leaf.entries.len() / 2;
                let right_entries = leaf.entries.split_off(split_idx);
                let separator = right_entries[0].0.clone();

                let right_leaf_id = self.allocate_empty_node_page()?;
                let mut right_leaf = LeafNode::new();
                right_leaf.entries = right_entries;
                right_leaf.next_leaf = leaf.next_leaf;
                right_leaf.prev_leaf = Some(node_id);

                if let Some(next_id) = leaf.next_leaf {
                    if fault::should_fail("btree.before_leaf_link_update") {
                        return Err(EntDbError::Corruption(
                            "failpoint: btree.before_leaf_link_update".to_string(),
                        ));
                    }
                    let mut next_leaf = match self.load_node(next_id)? {
                        Node::Leaf(leaf) => leaf,
                        _ => {
                            return Err(EntDbError::Corruption(
                                "leaf sibling pointer references non-leaf".to_string(),
                            ))
                        }
                    };
                    next_leaf.prev_leaf = Some(right_leaf_id);
                    self.store_node(next_id, &Node::Leaf(next_leaf))?;
                    self.log_structure(3, next_id, Some(right_leaf_id))?;
                }

                leaf.next_leaf = Some(right_leaf_id);

                self.store_node(node_id, &Node::Leaf(leaf))?;
                self.store_node(right_leaf_id, &Node::Leaf(right_leaf))?;
                self.log_structure(1, node_id, Some(right_leaf_id))?;
                Ok(Some((separator, right_leaf_id)))
            }
            Node::Internal(mut internal) => {
                if internal.children.is_empty() {
                    return Err(EntDbError::Corruption(
                        "internal node has no children".to_string(),
                    ));
                }

                let child_idx = internal.child_index_for_key(&key);
                let child_id = *internal.children.get(child_idx).ok_or_else(|| {
                    EntDbError::Corruption("child index out of bounds".to_string())
                })?;

                let child_split = self.insert_into_node(child_id, key, tuple_id)?;
                if let Some((child_separator, right_child_id)) = child_split {
                    let key_insert_idx = internal
                        .keys
                        .binary_search_by(|probe| self.key_schema.compare(probe, &child_separator))
                        .map(|idx| idx + 1)
                        .unwrap_or_else(|idx| idx);
                    internal.keys.insert(key_insert_idx, child_separator);
                    internal.children.insert(key_insert_idx + 1, right_child_id);
                }

                if internal.keys.len() <= self.order {
                    self.store_node(node_id, &Node::Internal(internal))?;
                    return Ok(None);
                }

                if fault::should_fail("btree.before_internal_split") {
                    return Err(EntDbError::Corruption(
                        "failpoint: btree.before_internal_split".to_string(),
                    ));
                }
                let mid = internal.keys.len() / 2;
                let separator = internal.keys[mid].clone();

                let right_keys = internal.keys.split_off(mid + 1);
                internal.keys.pop();
                let right_children = internal.children.split_off(mid + 1);

                let right_node_id = self.allocate_empty_node_page()?;
                let right_internal = InternalNode {
                    keys: right_keys,
                    children: right_children,
                };

                self.store_node(node_id, &Node::Internal(internal))?;
                self.store_node(right_node_id, &Node::Internal(right_internal))?;
                self.log_structure(2, node_id, Some(right_node_id))?;

                Ok(Some((separator, right_node_id)))
            }
        }
    }

    fn find_leaf_node_id(&self, key: &[u8], start: PageId) -> Result<PageId> {
        let mut current = start;
        loop {
            match self.load_node(current)? {
                Node::Leaf(_) => return Ok(current),
                Node::Internal(internal) => {
                    let idx = internal.child_index_for_key(key);
                    current = *internal.children.get(idx).ok_or_else(|| {
                        EntDbError::Corruption("invalid btree internal child index".to_string())
                    })?;
                }
            }
        }
    }

    fn leftmost_leaf_id(&self, start: PageId) -> Result<PageId> {
        let mut current = start;
        loop {
            match self.load_node(current)? {
                Node::Leaf(_) => return Ok(current),
                Node::Internal(internal) => {
                    current = *internal.children.first().ok_or_else(|| {
                        EntDbError::Corruption("internal node has no left child".to_string())
                    })?;
                }
            }
        }
    }

    fn load_node(&self, node_id: PageId) -> Result<Node> {
        let page = self.buffer_pool.fetch_page(node_id)?;
        Node::decode_from_page(&page)
    }

    fn store_node(&self, node_id: PageId, node: &Node) -> Result<()> {
        let mut page = self.buffer_pool.fetch_page(node_id)?;
        node.encode_into_page(&mut page)?;
        page.set_page_type(match node {
            Node::Internal(_) => PageType::BTreeInternal,
            Node::Leaf(_) => PageType::BTreeLeaf,
        });
        page.mark_dirty();
        Ok(())
    }

    fn allocate_empty_node_page(&self) -> Result<PageId> {
        let page = self.buffer_pool.new_page()?;
        Ok(page.page_id())
    }

    fn log_structure(
        &self,
        event: u8,
        page_id: PageId,
        related_page_id: Option<PageId>,
    ) -> Result<()> {
        let Some(log_manager) = &self.log_manager else {
            return Ok(());
        };
        let lsn = log_manager.append(LogRecord::BtreeStructure {
            txn_id: 0,
            event,
            page_id,
            related_page_id,
        })?;
        log_manager.flush_up_to(lsn)?;
        Ok(())
    }

    fn stage_root_update(&self, new_root: PageId) -> Result<()> {
        let Some(meta_page_id) = self.meta_page_id else {
            // Legacy fallback.
            self.root_page_id.store(new_root, Ordering::Release);
            return Ok(());
        };

        let mut page = self.buffer_pool.fetch_page(meta_page_id)?;
        let mut meta = Self::read_meta_page(&page)?;
        meta.pending_root_page_id = Some(new_root);
        meta.root_update_state = ROOT_UPDATE_PENDING;
        Self::write_meta_page(&mut page, meta)?;
        page.mark_dirty();
        drop(page);
        self.buffer_pool.flush_page(meta_page_id)?;
        self.log_structure(5, meta_page_id, Some(new_root))?;
        Ok(())
    }

    fn commit_root_update(&self, new_root: PageId) -> Result<()> {
        if let Some(meta_page_id) = self.meta_page_id {
            let mut page = self.buffer_pool.fetch_page(meta_page_id)?;
            let mut meta = Self::read_meta_page(&page)?;
            meta.root_page_id = new_root;
            meta.pending_root_page_id = None;
            meta.root_update_state = ROOT_UPDATE_CLEAN;
            Self::write_meta_page(&mut page, meta)?;
            page.mark_dirty();
            drop(page);
            self.buffer_pool.flush_page(meta_page_id)?;
            self.log_structure(6, meta_page_id, Some(new_root))?;
        }

        self.root_page_id.store(new_root, Ordering::Release);
        Ok(())
    }

    fn read_meta_page_if_present(
        buffer_pool: &Arc<BufferPool>,
        page_id: PageId,
    ) -> Result<Option<BTreeMeta>> {
        let page = buffer_pool.fetch_page(page_id)?;
        let body = page.body();
        if body.len() < 16 {
            return Ok(None);
        }
        let magic = u32::from_le_bytes(body[0..4].try_into().expect("meta magic bytes"));
        if magic != BTREE_META_MAGIC {
            return Ok(None);
        }
        Ok(Some(Self::read_meta_page(&page)?))
    }

    fn read_meta_page(page: &crate::storage::page::Page) -> Result<BTreeMeta> {
        let body = page.body();
        if body.len() < 16 {
            return Err(EntDbError::Corruption(
                "btree meta page too small".to_string(),
            ));
        }

        let magic = u32::from_le_bytes(body[0..4].try_into().expect("meta magic bytes"));
        if magic != BTREE_META_MAGIC {
            return Err(EntDbError::Corruption(
                "invalid btree meta magic".to_string(),
            ));
        }

        let root_page_id = u32::from_le_bytes(body[4..8].try_into().expect("meta root bytes"));
        let pending_raw = u32::from_le_bytes(body[8..12].try_into().expect("meta pending bytes"));
        let state = body[12];

        Ok(BTreeMeta {
            root_page_id,
            pending_root_page_id: if pending_raw == INVALID_NODE_PAGE {
                None
            } else {
                Some(pending_raw)
            },
            root_update_state: state,
        })
    }

    fn write_meta_page(page: &mut crate::storage::page::Page, meta: BTreeMeta) -> Result<()> {
        let body = page.body_mut();
        if body.len() < 16 {
            return Err(EntDbError::Corruption(
                "btree meta page too small".to_string(),
            ));
        }
        body.fill(0);
        body[0..4].copy_from_slice(&BTREE_META_MAGIC.to_le_bytes());
        body[4..8].copy_from_slice(&meta.root_page_id.to_le_bytes());
        body[8..12].copy_from_slice(
            &meta
                .pending_root_page_id
                .unwrap_or(INVALID_NODE_PAGE)
                .to_le_bytes(),
        );
        body[12] = meta.root_update_state;
        Ok(())
    }
}

fn is_sorted<T, F>(items: &[T], mut cmp: F) -> bool
where
    F: FnMut(&T, &T) -> std::cmp::Ordering,
{
    items.windows(2).all(|w| !cmp(&w[0], &w[1]).is_gt())
}

fn is_sorted_entries<F>(entries: &[(Vec<u8>, TupleId)], mut cmp: F) -> bool
where
    F: FnMut(&[u8], &[u8]) -> std::cmp::Ordering,
{
    entries
        .windows(2)
        .all(|w| !cmp(w[0].0.as_slice(), w[1].0.as_slice()).is_gt())
}
