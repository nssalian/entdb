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

use crate::query::history::{
    OptimizerHistoryRecord, OptimizerHistoryRecorder, OptimizerHistoryStore,
};
use tempfile::tempdir;

fn mk_rec(fingerprint: &str, plan_sig: &str, at: u64, latency_ms: u64) -> OptimizerHistoryRecord {
    OptimizerHistoryRecord {
        fingerprint: fingerprint.to_string(),
        plan_signature: plan_sig.to_string(),
        schema_hash: "optimizer_history_schema_v1_planner_v1".to_string(),
        captured_at_ms: at,
        rowcount_observed_json: "{\"root\":1}".to_string(),
        latency_ms,
        memory_peak_bytes: 0,
        success: true,
        error_class: None,
        confidence: 1.0,
    }
}

#[test]
fn optimizer_history_persists_and_reloads() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("history.json");
    let mut store =
        OptimizerHistoryStore::load_or_create(&path, "optimizer_history_schema_v1_planner_v1", 16)
            .expect("load");
    store.record(mk_rec("q1", "p1", 1000, 10)).expect("record");
    store.record(mk_rec("q1", "p2", 2000, 20)).expect("record");
    drop(store);

    let reloaded =
        OptimizerHistoryStore::load_or_create(&path, "optimizer_history_schema_v1_planner_v1", 16)
            .expect("reload");
    assert_eq!(reloaded.len(), 2);
    let rows = reloaded.by_fingerprint("q1");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].plan_signature, "p2");
}

#[test]
fn optimizer_history_version_or_schema_mismatch_resets() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("history_mismatch.json");
    std::fs::write(
        &path,
        r#"{"version":0,"schema_hash":"old","entries":[{"fingerprint":"q","plan_signature":"p","schema_hash":"old","captured_at_ms":1,"rowcount_observed_json":"{}","latency_ms":1,"memory_peak_bytes":0,"success":true,"error_class":null,"confidence":1.0}]}"#,
    )
    .expect("write mismatched state");

    let store =
        OptimizerHistoryStore::load_or_create(&path, "optimizer_history_schema_v1_planner_v1", 16)
            .expect("load");
    assert_eq!(store.len(), 0);
}

#[test]
fn optimizer_history_corruption_is_reported() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("history_corrupt.json");
    std::fs::write(&path, "{ not json").expect("write corrupt");
    match OptimizerHistoryStore::load_or_create(&path, "optimizer_history_schema_v1_planner_v1", 16)
    {
        Ok(_) => panic!("corrupt history should fail"),
        Err(err) => assert!(err.to_string().contains("invalid optimizer history file")),
    }
}

#[test]
fn optimizer_history_compacts_per_fingerprint_with_bound() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("history_compact.json");
    let mut store =
        OptimizerHistoryStore::load_or_create(&path, "optimizer_history_schema_v1_planner_v1", 6)
            .expect("load");
    for i in 0..20 {
        store
            .record(mk_rec("q1", &format!("p{i}"), 1000 + i, 50 + i))
            .expect("record");
    }
    let q1 = store.by_fingerprint("q1");
    assert!(q1.len() <= 6);
    assert_eq!(q1[0].plan_signature, "p19");
}

#[test]
fn optimizer_history_recorder_concurrent_writes_persist() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("history_async.json");
    let recorder = std::sync::Arc::new(
        OptimizerHistoryRecorder::new(&path, "optimizer_history_schema_v1_planner_v1", 32, 64)
            .expect("recorder"),
    );

    let mut handles = Vec::new();
    for t in 0..8 {
        let rec = std::sync::Arc::clone(&recorder);
        handles.push(std::thread::spawn(move || {
            for i in 0..50 {
                rec.try_record(mk_rec(
                    "q_async",
                    &format!("p{t}_{i}"),
                    1_000 + (t * 100 + i) as u64,
                    10,
                ));
            }
        }));
    }
    for h in handles {
        h.join().expect("join");
    }

    std::thread::sleep(std::time::Duration::from_millis(200));
    let rows = recorder.read_for_fingerprint("q_async");
    assert!(!rows.is_empty());
    assert!(rows.len() <= 32);
}

#[test]
fn optimizer_history_recorder_backpressure_tracks_drops() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("history_backpressure.json");
    let recorder =
        OptimizerHistoryRecorder::new(&path, "optimizer_history_schema_v1_planner_v1", 32, 1)
            .expect("recorder");

    for i in 0..5_000 {
        recorder.try_record(mk_rec("q_drop", &format!("p{i}"), i as u64, 5));
    }
    std::thread::sleep(std::time::Duration::from_millis(100));
    let (dropped, worker_errors) = (recorder.dropped_count(), recorder.worker_error_count());
    assert_eq!(worker_errors, 0);
    assert!(dropped > 0);
}

#[test]
fn optimizer_history_recorder_reports_worker_write_errors_when_store_path_invalid() {
    let dir = tempdir().expect("tempdir");
    let bad_store_root = dir.path().join("does_not_exist").join("nested");
    let recorder = OptimizerHistoryRecorder::new_with_store_path(
        dir.path().join("history_failpoint.json"),
        "optimizer_history_schema_v1_planner_v1",
        16,
        16,
        &bad_store_root,
    )
    .expect("recorder");
    for i in 0..50 {
        recorder.try_record(mk_rec("q_fail", &format!("p{i}"), i as u64, 2));
    }
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(recorder.worker_error_count() > 0 || recorder.dropped_count() > 0);
}

#[test]
fn optimizer_history_restart_consistency_roundtrip() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("history_restart.json");
    {
        let mut store = OptimizerHistoryStore::load_or_create(
            &path,
            "optimizer_history_schema_v1_planner_v1",
            16,
        )
        .expect("load");
        for i in 0..10 {
            store
                .record(mk_rec("q_restart", &format!("p{i}"), 100 + i, 10 + i))
                .expect("record");
        }
    }
    let after1 =
        OptimizerHistoryStore::load_or_create(&path, "optimizer_history_schema_v1_planner_v1", 16)
            .expect("reload1");
    let rows1 = after1.by_fingerprint("q_restart");
    assert!(!rows1.is_empty());

    let after2 =
        OptimizerHistoryStore::load_or_create(&path, "optimizer_history_schema_v1_planner_v1", 16)
            .expect("reload2");
    let rows2 = after2.by_fingerprint("q_restart");
    assert_eq!(rows1, rows2);
}
