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

use entdb::storage::buffer_pool::BufferPoolStats;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Default)]
pub struct ServerMetricsSnapshot {
    pub connections_accepted: u64,
    pub connections_refused: u64,
    pub active_connections: u64,
    pub peak_connections: u64,
    pub queries_total: u64,
    pub queries_succeeded: u64,
    pub queries_failed: u64,
    pub query_timeouts: u64,
    pub query_latency_total_ns: u64,
    pub query_latency_max_ns: u64,
    pub sqlstate_errors: HashMap<String, u64>,
    pub shutdown_flush_total_ns: u64,
    pub shutdown_persist_total_ns: u64,
    pub wal_flushes: u64,
    pub buffer_pool_pressure: Option<BufferPoolStats>,
}

#[derive(Debug, Default)]
pub struct ServerMetrics {
    connections_accepted: AtomicU64,
    connections_refused: AtomicU64,
    active_connections: AtomicU64,
    peak_connections: AtomicU64,
    queries_total: AtomicU64,
    queries_succeeded: AtomicU64,
    queries_failed: AtomicU64,
    query_timeouts: AtomicU64,
    query_latency_total_ns: AtomicU64,
    query_latency_max_ns: AtomicU64,
    shutdown_flush_total_ns: AtomicU64,
    shutdown_persist_total_ns: AtomicU64,
    wal_flushes: AtomicU64,
    sqlstate_errors: Mutex<HashMap<String, u64>>,
    buffer_pool_pressure: Mutex<Option<BufferPoolStats>>,
}

impl ServerMetrics {
    pub fn on_connection_accepted(&self) {
        self.connections_accepted.fetch_add(1, Ordering::Relaxed);
        let active = self.active_connections.fetch_add(1, Ordering::Relaxed) + 1;
        let mut peak = self.peak_connections.load(Ordering::Relaxed);
        while active > peak {
            match self.peak_connections.compare_exchange(
                peak,
                active,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => peak = actual,
            }
        }
    }

    pub fn on_connection_refused(&self) {
        self.connections_refused.fetch_add(1, Ordering::Relaxed);
    }

    pub fn on_connection_closed(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn on_query_finished(&self, elapsed_ns: u64, sqlstate: Option<&str>) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
        self.query_latency_total_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        self.update_latency_max(elapsed_ns);

        match sqlstate {
            None => {
                self.queries_succeeded.fetch_add(1, Ordering::Relaxed);
            }
            Some(code) => {
                self.queries_failed.fetch_add(1, Ordering::Relaxed);
                if code == "57014" {
                    self.query_timeouts.fetch_add(1, Ordering::Relaxed);
                }
                let mut map = self.sqlstate_errors.lock();
                *map.entry(code.to_string()).or_insert(0) += 1;
            }
        }
    }

    pub fn on_shutdown_flush(&self, elapsed_ns: u64) {
        self.shutdown_flush_total_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        self.wal_flushes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn on_shutdown_persist(&self, elapsed_ns: u64) {
        self.shutdown_persist_total_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
    }

    pub fn set_buffer_pool_pressure(&self, stats: BufferPoolStats) {
        *self.buffer_pool_pressure.lock() = Some(stats);
    }

    pub fn snapshot(&self) -> ServerMetricsSnapshot {
        ServerMetricsSnapshot {
            connections_accepted: self.connections_accepted.load(Ordering::Relaxed),
            connections_refused: self.connections_refused.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            peak_connections: self.peak_connections.load(Ordering::Relaxed),
            queries_total: self.queries_total.load(Ordering::Relaxed),
            queries_succeeded: self.queries_succeeded.load(Ordering::Relaxed),
            queries_failed: self.queries_failed.load(Ordering::Relaxed),
            query_timeouts: self.query_timeouts.load(Ordering::Relaxed),
            query_latency_total_ns: self.query_latency_total_ns.load(Ordering::Relaxed),
            query_latency_max_ns: self.query_latency_max_ns.load(Ordering::Relaxed),
            sqlstate_errors: self.sqlstate_errors.lock().clone(),
            shutdown_flush_total_ns: self.shutdown_flush_total_ns.load(Ordering::Relaxed),
            shutdown_persist_total_ns: self.shutdown_persist_total_ns.load(Ordering::Relaxed),
            wal_flushes: self.wal_flushes.load(Ordering::Relaxed),
            buffer_pool_pressure: *self.buffer_pool_pressure.lock(),
        }
    }

    fn update_latency_max(&self, elapsed_ns: u64) {
        let mut current = self.query_latency_max_ns.load(Ordering::Relaxed);
        while elapsed_ns > current {
            match self.query_latency_max_ns.compare_exchange(
                current,
                elapsed_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ServerMetrics;

    #[test]
    fn metrics_track_connections_queries_and_errors() {
        let m = ServerMetrics::default();

        m.on_connection_accepted();
        m.on_connection_accepted();
        m.on_connection_refused();
        m.on_connection_closed();

        m.on_query_finished(10, None);
        m.on_query_finished(20, Some("22000"));
        m.on_query_finished(30, Some("57014"));

        let s = m.snapshot();
        assert_eq!(s.connections_accepted, 2);
        assert_eq!(s.connections_refused, 1);
        assert_eq!(s.active_connections, 1);
        assert_eq!(s.peak_connections, 2);
        assert_eq!(s.queries_total, 3);
        assert_eq!(s.queries_succeeded, 1);
        assert_eq!(s.queries_failed, 2);
        assert_eq!(s.query_timeouts, 1);
        assert_eq!(s.query_latency_total_ns, 60);
        assert_eq!(s.query_latency_max_ns, 30);
        assert_eq!(s.sqlstate_errors.get("22000"), Some(&1));
        assert_eq!(s.sqlstate_errors.get("57014"), Some(&1));
    }
}
