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

use entdb::fault;
use entdb::storage::bm25::{sidecar_path, Bm25Index, BM25_SIDECAR_VERSION};
use std::fs;
use tempfile::tempdir;

#[test]
fn bm25_sidecar_loads_legacy_unversioned_format() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("legacy_bm25.db");
    let sidecar = sidecar_path(&db_path, "idx_docs");
    let legacy = r#"{
      "index_name":"idx_docs",
      "text_config":"english",
      "documents":{"1:1":"database systems"},
      "doc_lengths":{"1:1":2},
      "postings":{"database":{"1:1":1},"systems":{"1:1":1}}
    }"#;
    fs::write(&sidecar, legacy).expect("write legacy sidecar");

    let loaded = Bm25Index::load(&db_path, "idx_docs").expect("load legacy");
    assert_eq!(loaded.document_count(), 1);
    assert_eq!(loaded.document_frequency("database"), 1);
}

#[test]
fn bm25_sidecar_rejects_unknown_version() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("badver_bm25.db");
    let sidecar = sidecar_path(&db_path, "idx_docs");
    let bad = format!(
        r#"{{"version":{},"index":{{"index_name":"idx_docs","text_config":"english","documents":{{}},"doc_lengths":{{}},"postings":{{}}}}}}"#,
        BM25_SIDECAR_VERSION + 100
    );
    fs::write(&sidecar, bad).expect("write bad version sidecar");
    let err = Bm25Index::load(&db_path, "idx_docs").expect_err("must fail unknown version");
    assert!(err.to_string().contains("unsupported bm25 sidecar version"));
}

#[test]
fn bm25_sidecar_persist_failpoint_keeps_previous_file() {
    fault::clear_all_failpoints();
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("atomic_bm25.db");

    let mut idx = Bm25Index::load_or_create(&db_path, "idx_docs", "english").expect("create");
    idx.index_document("1:1".to_string(), "database systems");
    idx.persist(&db_path).expect("persist base");
    let sidecar = sidecar_path(&db_path, "idx_docs");
    let before = fs::read(&sidecar).expect("read baseline");

    idx.index_document("2:1".to_string(), "database retrieval");
    fault::set_failpoint("bm25.persist.rename", 0);
    let err = idx.persist(&db_path).expect_err("rename failpoint");
    assert!(err.to_string().contains("failpoint"));
    fault::clear_all_failpoints();

    let after = fs::read(&sidecar).expect("read after fail");
    assert_eq!(before, after, "sidecar content must remain unchanged");
}
