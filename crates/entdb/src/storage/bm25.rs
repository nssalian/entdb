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
use crate::storage::tuple::TupleId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

pub const BM25_SIDECAR_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Bm25Index {
    pub index_name: String,
    pub text_config: String,
    pub documents: HashMap<String, String>,
    pub doc_lengths: HashMap<String, usize>,
    pub postings: HashMap<String, HashMap<String, u32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Bm25SidecarFile {
    version: u32,
    index: Bm25Index,
}

impl Bm25Index {
    pub fn load_or_create(db_path: &Path, index_name: &str, text_config: &str) -> Result<Self> {
        let path = sidecar_path(db_path, index_name);
        if path.exists() {
            let bytes = fs::read(&path)?;
            let mut out = match serde_json::from_slice::<Bm25SidecarFile>(&bytes) {
                Ok(wrapper) => {
                    if wrapper.version != BM25_SIDECAR_VERSION {
                        return Err(EntDbError::Corruption(format!(
                            "unsupported bm25 sidecar version {} for '{}'",
                            wrapper.version,
                            path.display()
                        )));
                    }
                    wrapper.index
                }
                Err(_) => serde_json::from_slice::<Self>(&bytes).map_err(|e| {
                    EntDbError::Corruption(format!(
                        "invalid bm25 index sidecar '{}': {e}",
                        path.display()
                    ))
                })?,
            };
            out.index_name = index_name.to_string();
            if out.text_config.is_empty() {
                out.text_config = text_config.to_string();
            }
            return Ok(out);
        }
        Ok(Self {
            index_name: index_name.to_string(),
            text_config: text_config.to_string(),
            documents: HashMap::new(),
            doc_lengths: HashMap::new(),
            postings: HashMap::new(),
        })
    }

    pub fn load(db_path: &Path, index_name: &str) -> Result<Self> {
        Self::load_or_create(db_path, index_name, "english")
    }

    pub fn persist(&self, db_path: &Path) -> Result<()> {
        let path = sidecar_path(db_path, &self.index_name);
        let wrapper = Bm25SidecarFile {
            version: BM25_SIDECAR_VERSION,
            index: self.clone(),
        };
        let bytes = serde_json::to_vec_pretty(&wrapper)
            .map_err(|e| EntDbError::Wal(format!("bm25 sidecar encode failed: {e}")))?;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let tmp = path.with_extension(format!(
            "{}.{}.{}.tmp",
            path.extension().unwrap_or_default().to_string_lossy(),
            std::process::id(),
            nanos
        ));
        if fault::should_fail("bm25.persist.write") {
            return Err(EntDbError::Io(std::io::Error::other(
                "failpoint: bm25.persist.write",
            )));
        }
        fs::write(&tmp, bytes)?;
        if fault::should_fail("bm25.persist.rename") {
            return Err(EntDbError::Io(std::io::Error::other(
                "failpoint: bm25.persist.rename",
            )));
        }
        fs::rename(tmp, path)?;
        Ok(())
    }

    pub fn clear(&mut self) {
        self.documents.clear();
        self.doc_lengths.clear();
        self.postings.clear();
    }

    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    pub fn document_frequency(&self, term: &str) -> usize {
        self.postings.get(term).map(|p| p.len()).unwrap_or(0)
    }

    pub fn score_all(&self, terms: &[String]) -> HashMap<String, f64> {
        let n_docs = self.documents.len() as f64;
        if n_docs == 0.0 {
            return HashMap::new();
        }
        let avgdl = if self.doc_lengths.is_empty() {
            1.0
        } else {
            self.doc_lengths.values().sum::<usize>() as f64 / self.doc_lengths.len() as f64
        };
        let k1 = 1.2_f64;
        let b = 0.75_f64;

        let mut scores: HashMap<String, f64> = HashMap::new();
        let normalized_terms = normalize_query_terms(&self.text_config, terms);
        for term in normalized_terms {
            let Some(postings) = self.postings.get(&term) else {
                continue;
            };
            let df = postings.len() as f64;
            if df == 0.0 {
                continue;
            }
            let idf = ((n_docs - df + 0.5) / (df + 0.5) + 1.0).ln();
            for (doc_id, tf_u32) in postings {
                let tf = *tf_u32 as f64;
                let dl = *self.doc_lengths.get(doc_id).unwrap_or(&1) as f64;
                let denom = tf + k1 * (1.0 - b + b * dl / avgdl.max(1e-9));
                let contrib = idf * (tf * (k1 + 1.0)) / denom.max(1e-9);
                *scores.entry(doc_id.clone()).or_insert(0.0) += contrib;
            }
        }
        scores
    }

    pub fn index_document(&mut self, doc_id: String, text: &str) {
        if self.documents.contains_key(&doc_id) {
            self.remove_document(&doc_id);
        }

        let tokens = tokenize_with_config(&self.text_config, text);
        if tokens.is_empty() {
            self.documents.insert(doc_id, text.to_string());
            return;
        }

        let mut tf: HashMap<String, u32> = HashMap::new();
        for t in &tokens {
            *tf.entry(t.clone()).or_insert(0) += 1;
        }
        for (term, freq) in tf {
            self.postings
                .entry(term)
                .or_default()
                .insert(doc_id.clone(), freq);
        }
        self.doc_lengths.insert(doc_id.clone(), tokens.len());
        self.documents.insert(doc_id, text.to_string());
    }

    pub fn remove_document(&mut self, doc_id: &str) {
        let Some(text) = self.documents.remove(doc_id) else {
            return;
        };
        let tokens = tokenize_with_config(&self.text_config, &text);
        for term in tokens {
            if let Some(postings) = self.postings.get_mut(&term) {
                postings.remove(doc_id);
                if postings.is_empty() {
                    self.postings.remove(&term);
                }
            }
        }
        self.doc_lengths.remove(doc_id);
    }
}

pub fn sidecar_path(db_path: &Path, index_name: &str) -> PathBuf {
    let mut out = db_path.to_path_buf();
    let mut safe_name = String::with_capacity(index_name.len());
    for c in index_name.chars() {
        if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
            safe_name.push(c);
        } else {
            safe_name.push('_');
        }
    }
    out.set_extension(format!("bm25.{safe_name}.json"));
    out
}

pub fn tuple_id_key(tid: TupleId) -> String {
    format!("{}:{}", tid.0, tid.1)
}

fn tokenize_with_config(text_config: &str, text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_ascii_lowercase())
        .filter(|t| !is_stopword(text_config, t))
        .collect()
}

fn normalize_query_terms(text_config: &str, terms: &[String]) -> Vec<String> {
    terms
        .iter()
        .map(|t| t.to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .filter(|t| !is_stopword(text_config, t))
        .collect()
}

fn is_stopword(text_config: &str, term: &str) -> bool {
    if !text_config.eq_ignore_ascii_case("english") {
        return false;
    }
    matches!(
        term,
        "a" | "an" | "the" | "and" | "or" | "of" | "to" | "in" | "on" | "for" | "with"
    )
}
