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

use entdb::catalog::Catalog;
use entdb::query::binder::Binder;
use entdb::query::history::OptimizerHistoryRecord;
use entdb::query::optimizer::{Optimizer, OptimizerConfig};
use entdb::query::planner::Planner;
use entdb::storage::buffer_pool::BufferPool;
use entdb::storage::disk_manager::DiskManager;
use entdb_server::server::{serve, Database, ServerConfig};
use rcgen::generate_simple_self_signed;
use rustls_pemfile::{certs, pkcs8_private_keys};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_postgres::error::SqlState;
use tokio_postgres::NoTls;
use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig as RustlsServerConfig};
use tokio_rustls::TlsAcceptor;

struct TestServer {
    _dir: Option<TempDir>,
    addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    join: JoinHandle<()>,
}

impl TestServer {
    async fn start() -> Self {
        let cfg = ServerConfig {
            data_path: std::env::temp_dir().join("placeholder.db"),
            host: "127.0.0.1".to_string(),
            port: 0,
            buffer_pool_size: 128,
            max_connections: 64,
            max_statement_bytes: 1024 * 1024,
            query_timeout_ms: 30_000,
            auth_method: entdb_server::server::auth::AuthMethod::Md5,
            scram_iterations: 4096,
            auth_user: "entdb".to_string(),
            auth_password: "entdb".to_string(),
            tls_cert: None,
            tls_key: None,
        };
        Self::start_with_config(cfg).await
    }

    async fn start_with_config(mut cfg: ServerConfig) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let data_path = dir.path().join("entdb-server-test.db");
        cfg.data_path = data_path.clone();
        Self::start_at_path(cfg, &data_path, None, Some(dir)).await
    }

    async fn start_with_tls_config(mut cfg: ServerConfig) -> (Self, PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let data_path = dir.path().join("entdb-server-test.db");
        cfg.data_path = data_path.clone();

        let cert_path = dir.path().join("server.crt");
        let key_path = dir.path().join("server.key");
        let (cert_pem, key_pem) = generate_test_cert_pair();
        std::fs::write(&cert_path, cert_pem).expect("write cert");
        std::fs::write(&key_path, key_pem).expect("write key");
        let tls_acceptor = build_test_tls_acceptor(&cert_path, &key_path);

        (
            Self::start_at_path(cfg, &data_path, Some(tls_acceptor), Some(dir)).await,
            cert_path,
        )
    }

    async fn start_with_existing_path(data_path: &Path, cfg: ServerConfig) -> Self {
        Self::start_at_path(cfg, data_path, None, None).await
    }

    async fn start_at_path(
        mut cfg: ServerConfig,
        data_path: &Path,
        tls_acceptor: Option<Arc<TlsAcceptor>>,
        dir: Option<TempDir>,
    ) -> Self {
        cfg.data_path = data_path.to_path_buf();
        let db = Arc::new(Database::open(data_path, 128).expect("open db"));
        let config = Arc::new(cfg);
        let tls_acceptor = if let Some(acceptor) = tls_acceptor {
            Some(acceptor)
        } else if let (Some(cert), Some(key)) = (&config.tls_cert, &config.tls_key) {
            Some(build_test_tls_acceptor(cert, key))
        } else {
            None
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");

        let (tx, rx) = oneshot::channel();
        let join = tokio::spawn(async move {
            serve(listener, config, db, tls_acceptor, Some(rx))
                .await
                .expect("serve");
        });

        Self {
            _dir: dir,
            addr,
            shutdown: Some(tx),
            join,
        }
    }

    async fn shutdown_gracefully(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = (&mut self.join).await;
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        self.join.abort();
    }
}

fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .expect("env lock")
}

#[derive(serde::Deserialize)]
struct HistoryState {
    entries: Vec<OptimizerHistoryRecord>,
}

fn optimizer_history_path_for_data_path(path: &Path) -> PathBuf {
    let mut p = path.to_path_buf();
    p.set_extension("optimizer_history.json");
    p
}

fn read_history_entries(path: &Path) -> Vec<OptimizerHistoryRecord> {
    let bytes = std::fs::read(path).expect("read optimizer history");
    serde_json::from_slice::<HistoryState>(&bytes)
        .expect("decode optimizer history")
        .entries
}

fn optimizer_schema_hash() -> &'static str {
    "optimizer_history_schema_v1_planner_v1"
}

fn generate_test_cert_pair() -> (String, String) {
    let cert = generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
        .expect("generate cert");
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();
    (cert_pem, key_pem)
}

fn build_test_tls_acceptor(cert_path: &Path, key_path: &Path) -> Arc<TlsAcceptor> {
    let cert_file = std::fs::File::open(cert_path).expect("open cert");
    let mut cert_reader = std::io::BufReader::new(cert_file);
    let cert_chain = certs(&mut cert_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("parse cert");

    let key_file = std::fs::File::open(key_path).expect("open key");
    let mut key_reader = std::io::BufReader::new(key_file);
    let mut keys = pkcs8_private_keys(&mut key_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("parse key");
    let key = keys.pop().expect("at least one key");

    let rustls = RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key.into())
        .expect("build rustls server config");
    Arc::new(TlsAcceptor::from(Arc::new(rustls)))
}

fn tls_connector_for_cert(cert_path: &Path) -> MakeRustlsConnect {
    let cert_file = std::fs::File::open(cert_path).expect("open cert");
    let mut cert_reader = std::io::BufReader::new(cert_file);
    let cert_chain = certs(&mut cert_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("parse cert");

    let mut roots = RootCertStore::empty();
    for cert in cert_chain {
        roots.add(cert).expect("add root cert");
    }

    let client_cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    MakeRustlsConnect::new(client_cfg)
}

fn base_config(data_path: PathBuf) -> ServerConfig {
    ServerConfig {
        data_path,
        host: "127.0.0.1".to_string(),
        port: 0,
        buffer_pool_size: 128,
        max_connections: 64,
        max_statement_bytes: 1024 * 1024,
        query_timeout_ms: 30_000,
        auth_method: entdb_server::server::auth::AuthMethod::Md5,
        scram_iterations: 4096,
        auth_user: "entdb".to_string(),
        auth_password: "entdb".to_string(),
        tls_cert: None,
        tls_key: None,
    }
}

#[tokio::test]
async fn pgwire_simple_query_end_to_end() {
    let server = TestServer::start().await;
    let dsn = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server.addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            "\
            CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);
            INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35);
            ",
        )
        .await
        .expect("setup");

    let rows = client
        .query(
            "SELECT id, name FROM users WHERE age > 27 ORDER BY id LIMIT 2",
            &[],
        )
        .await
        .expect("select");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, String>(1), "alice");
    assert_eq!(rows[1].get::<_, i32>(0), 3);

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .execute("UPDATE users SET age = 26 WHERE id = 2", &[])
        .await
        .expect("update in tx");
    let age_in_tx = client
        .query_one("SELECT age FROM users WHERE id = 2", &[])
        .await
        .expect("select in tx")
        .get::<_, i32>(0);
    assert_eq!(age_in_tx, 26);

    client.batch_execute("ROLLBACK").await.expect("rollback");
    let age_after = client
        .query_one("SELECT age FROM users WHERE id = 2", &[])
        .await
        .expect("select after rollback")
        .get::<_, i32>(0);
    assert_eq!(age_after, 25);
}

#[tokio::test]
async fn pgwire_extended_prepared_statement_works() {
    let server = TestServer::start().await;
    let dsn = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server.addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            "\
            CREATE TABLE items (id INTEGER, score INTEGER);
            INSERT INTO items VALUES (1, 10), (2, 20), (3, 30), (4, 40);
            ",
        )
        .await
        .expect("setup");

    let stmt = client
        .prepare("SELECT id FROM items WHERE score > $1 ORDER BY id LIMIT $2")
        .await
        .expect("prepare");

    let rows = client
        .query(&stmt, &[&15_i32, &2_i32])
        .await
        .expect("query prepared");

    let ids = rows
        .into_iter()
        .map(|r| r.get::<_, i32>(0))
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![2, 3]);
}

#[tokio::test]
async fn pgwire_optimizer_history_records_entries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_path = dir.path().join("optimizer-history.db");
    let cfg = base_config(data_path.clone());
    let server = TestServer::start_with_existing_path(&data_path, cfg).await;
    let dsn = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server.addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            "\
            CREATE TABLE users (id INTEGER, age INTEGER);
            INSERT INTO users VALUES (1, 30), (2, 20), (3, 40);
            ",
        )
        .await
        .expect("setup");

    let _ = client
        .query("SELECT id FROM users WHERE age >= 30 ORDER BY id", &[])
        .await
        .expect("query");

    let history_path = optimizer_history_path_for_data_path(&data_path);
    let entries = read_history_entries(&history_path);
    assert!(
        !entries.is_empty(),
        "optimizer history should contain server-executed entries"
    );
    assert!(
        entries.iter().any(|e| e.success),
        "expected at least one successful optimizer feedback entry"
    );
}

#[tokio::test]
async fn pgwire_hbo_can_change_chosen_plan_with_seeded_history() {
    let _guard = env_lock();
    let old_hbo = std::env::var("ENTDB_HBO").ok();
    std::env::set_var("ENTDB_HBO", "1");

    let dir = tempfile::tempdir().expect("tempdir");
    let data_path = dir.path().join("hbo-seeded.db");
    let cfg = base_config(data_path.clone());
    let server = TestServer::start_with_existing_path(&data_path, cfg).await;
    let dsn = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server.addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            "\
            CREATE TABLE users (id INTEGER, name TEXT);
            CREATE TABLE orders (id INTEGER, user_id INTEGER);
            CREATE TABLE payments (id INTEGER, order_id INTEGER);
            INSERT INTO users VALUES (1, 'alice'), (2, 'bob');
            INSERT INTO orders VALUES (10, 1), (11, 1), (12, 2);
            INSERT INTO payments VALUES (100, 10), (101, 12);
            ",
        )
        .await
        .expect("setup");

    let sql = "SELECT users.name, orders.id \
               FROM users INNER JOIN orders ON users.id = orders.user_id \
               INNER JOIN payments ON orders.id = payments.order_id \
               ORDER BY orders.id";

    let dm = Arc::new(DiskManager::new(&data_path).expect("open dm"));
    let bp = Arc::new(BufferPool::new(128, Arc::clone(&dm)));
    let catalog = Arc::new(Catalog::load(Arc::clone(&bp)).expect("load catalog"));
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let stmt = sqlparser::parser::Parser::parse_sql(&dialect, sql)
        .expect("parse")
        .remove(0);
    let binder = Binder::new(Arc::clone(&catalog));
    let planner = Planner;
    let bound = binder.bind(&stmt).expect("bind");
    let fingerprint = Optimizer::fingerprint_bound_statement(&bound);
    let plan = planner.plan(bound).expect("plan");
    let mut cfg = OptimizerConfig::default();
    cfg.hbo_enabled = true;
    let baseline = Optimizer::optimize_with_trace_and_history(plan.clone(), &fingerprint, cfg, &[]);
    let baseline_fingerprint = baseline.trace.fingerprint.clone();
    let baseline_sig = baseline
        .trace
        .chosen_plan_signature
        .expect("baseline signature");
    let candidates = Optimizer::enumerate_candidate_signatures(&plan, cfg);
    let alternate_sig = candidates
        .into_iter()
        .find(|s| s != &baseline_sig)
        .expect("alternate candidate signature");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("epoch")
        .as_millis() as u64;
    let seeded = serde_json::json!({
        "version": 1,
        "schema_hash": optimizer_schema_hash(),
        "entries": [
            {
                "fingerprint": fingerprint,
                "plan_signature": alternate_sig,
                "schema_hash": optimizer_schema_hash(),
                "captured_at_ms": now,
                "rowcount_observed_json": "{\"root\":2}",
                "latency_ms": 1,
                "memory_peak_bytes": 0,
                "success": true,
                "error_class": null,
                "confidence": 1.0
            },
            {
                "fingerprint": fingerprint,
                "plan_signature": alternate_sig,
                "schema_hash": optimizer_schema_hash(),
                "captured_at_ms": now.saturating_sub(1),
                "rowcount_observed_json": "{\"root\":2}",
                "latency_ms": 2,
                "memory_peak_bytes": 0,
                "success": true,
                "error_class": null,
                "confidence": 1.0
            },
            {
                "fingerprint": baseline_fingerprint.clone(),
                "plan_signature": baseline_sig,
                "schema_hash": optimizer_schema_hash(),
                "captured_at_ms": now,
                "rowcount_observed_json": "{\"root\":2}",
                "latency_ms": 5000,
                "memory_peak_bytes": 0,
                "success": true,
                "error_class": null,
                "confidence": 1.0
            },
            {
                "fingerprint": baseline_fingerprint.clone(),
                "plan_signature": baseline_sig,
                "schema_hash": optimizer_schema_hash(),
                "captured_at_ms": now.saturating_sub(1),
                "rowcount_observed_json": "{\"root\":2}",
                "latency_ms": 4000,
                "memory_peak_bytes": 0,
                "success": true,
                "error_class": null,
                "confidence": 1.0
            }
        ]
    });
    let history_path = optimizer_history_path_for_data_path(&data_path);
    std::fs::write(
        &history_path,
        serde_json::to_vec_pretty(&seeded).expect("encode seeded history"),
    )
    .expect("write seeded history");

    let _rows = client
        .query(sql, &[])
        .await
        .expect("query with seeded history");
    let entries = read_history_entries(&history_path);
    let latest = entries
        .iter()
        .filter(|e| e.fingerprint == baseline_fingerprint && e.success)
        .max_by_key(|e| e.captured_at_ms)
        .expect("latest successful fingerprint entry");
    assert_eq!(
        latest.plan_signature,
        seeded["entries"][0]["plan_signature"]
            .as_str()
            .unwrap_or_default(),
        "expected hbo to choose seeded alternate plan signature"
    );

    if let Some(v) = old_hbo {
        std::env::set_var("ENTDB_HBO", v);
    } else {
        std::env::remove_var("ENTDB_HBO");
    }
}

#[tokio::test]
async fn pgwire_history_file_corruption_does_not_break_query_execution() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_path = dir.path().join("history-corrupt.db");
    let history_path = optimizer_history_path_for_data_path(&data_path);
    std::fs::write(&history_path, b"{ this is not valid json").expect("write corrupt history");

    let cfg = base_config(data_path.clone());
    let server = TestServer::start_with_existing_path(&data_path, cfg).await;
    let dsn = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server.addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute("CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1);")
        .await
        .expect("setup");
    let row = client
        .query_one("SELECT id FROM t LIMIT 1", &[])
        .await
        .expect("query should still succeed with broken history file");
    assert_eq!(row.get::<_, i32>(0), 1);
}

#[tokio::test]
async fn pgwire_returns_syntax_error_sqlstate_for_bad_sql() {
    let server = TestServer::start().await;
    let dsn = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server.addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let err = client
        .batch_execute("SELEC FROM")
        .await
        .expect_err("bad sql should fail");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code(), &SqlState::SYNTAX_ERROR);
}

#[tokio::test]
async fn pgwire_returns_txn_sqlstate_for_invalid_commit_flow() {
    let server = TestServer::start().await;
    let dsn = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server.addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let err = client
        .batch_execute("COMMIT")
        .await
        .expect_err("commit without active txn should fail");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "25000");
}

#[tokio::test]
async fn pgwire_returns_data_exception_for_invalid_cast_insert() {
    let server = TestServer::start().await;
    let dsn = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server.addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute("CREATE TABLE bad_cast (id INTEGER)")
        .await
        .expect("create table");

    let err = client
        .batch_execute("INSERT INTO bad_cast VALUES ('abc')")
        .await
        .expect_err("invalid cast should fail");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "22000");
}

#[tokio::test]
async fn pgwire_rejects_invalid_password() {
    let server = TestServer::start().await;
    let dsn = format!(
        "host=127.0.0.1 port={} user=entdb password=wrong dbname=entdb",
        server.addr.port()
    );
    let err = match tokio_postgres::connect(&dsn, NoTls).await {
        Ok(_) => panic!("connect should fail with bad password"),
        Err(e) => e,
    };
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "28P01");
}

#[tokio::test]
async fn pgwire_scram_auth_connects_and_queries() {
    let server = TestServer::start_with_config(ServerConfig {
        data_path: std::env::temp_dir().join("placeholder.db"),
        host: "127.0.0.1".to_string(),
        port: 0,
        buffer_pool_size: 128,
        max_connections: 64,
        max_statement_bytes: 1024 * 1024,
        query_timeout_ms: 30_000,
        auth_method: entdb_server::server::auth::AuthMethod::ScramSha256,
        scram_iterations: 4096,
        auth_user: "entdb".to_string(),
        auth_password: "entdb".to_string(),
        tls_cert: None,
        tls_key: None,
    })
    .await;

    let dsn = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server.addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            "CREATE TABLE scram_health (id INTEGER); INSERT INTO scram_health VALUES (1);",
        )
        .await
        .expect("setup");
    let row = client
        .query_one("SELECT id FROM scram_health LIMIT 1", &[])
        .await
        .expect("select");
    assert_eq!(row.get::<_, i32>(0), 1);
}

#[tokio::test]
async fn pgwire_multi_client_txn_visibility_and_conflict() {
    let server = TestServer::start().await;
    let dsn = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server.addr.port()
    );

    let (c1, conn1) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .expect("connect c1");
    tokio::spawn(async move {
        let _ = conn1.await;
    });
    let (c2, conn2) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .expect("connect c2");
    tokio::spawn(async move {
        let _ = conn2.await;
    });

    c1.batch_execute("CREATE TABLE mvcc_wire (id INTEGER, v INTEGER);")
        .await
        .expect("create");
    c1.batch_execute("INSERT INTO mvcc_wire VALUES (1, 0);")
        .await
        .expect("seed");

    c1.batch_execute("BEGIN").await.expect("c1 begin");
    c1.execute("UPDATE mvcc_wire SET v = 1 WHERE id = 1", &[])
        .await
        .expect("c1 update");

    c2.batch_execute("BEGIN").await.expect("c2 begin");
    let err = c2
        .execute("UPDATE mvcc_wire SET v = 2 WHERE id = 1", &[])
        .await
        .expect_err("c2 conflicting update should fail");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "40001");
    c2.batch_execute("ROLLBACK").await.expect("c2 rollback");

    c1.batch_execute("COMMIT").await.expect("c1 commit");

    let row = c2
        .query_one("SELECT v FROM mvcc_wire WHERE id = 1", &[])
        .await
        .expect("read committed");
    assert_eq!(row.get::<_, i32>(0), 1);
}

#[tokio::test]
async fn pgwire_connection_limit_refuses_excess_clients() {
    let server = TestServer::start_with_config(ServerConfig {
        data_path: std::env::temp_dir().join("placeholder.db"),
        host: "127.0.0.1".to_string(),
        port: 0,
        buffer_pool_size: 128,
        max_connections: 1,
        max_statement_bytes: 1024 * 1024,
        query_timeout_ms: 30_000,
        auth_method: entdb_server::server::auth::AuthMethod::Md5,
        scram_iterations: 4096,
        auth_user: "entdb".to_string(),
        auth_password: "entdb".to_string(),
        tls_cert: None,
        tls_key: None,
    })
    .await;

    let dsn = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server.addr.port()
    );
    let (_c1, conn1) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .expect("first connect");
    let _hold = tokio::spawn(async move {
        let _ = conn1.await;
    });

    let second = tokio_postgres::connect(&dsn, NoTls).await;
    assert!(
        second.is_err(),
        "second connection should be refused when max_connections=1"
    );
}

#[tokio::test]
async fn pgwire_two_session_commit_and_rollback_matrix() {
    let server = TestServer::start().await;
    let dsn = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server.addr.port()
    );
    let (c1, conn1) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .expect("connect c1");
    tokio::spawn(async move {
        let _ = conn1.await;
    });
    let (c2, conn2) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .expect("connect c2");
    tokio::spawn(async move {
        let _ = conn2.await;
    });

    c1.batch_execute("CREATE TABLE tx_wire (id INTEGER, v INTEGER);")
        .await
        .expect("create");
    c1.batch_execute("INSERT INTO tx_wire VALUES (1, 0);")
        .await
        .expect("seed");

    c1.batch_execute("BEGIN").await.expect("c1 begin");
    c1.execute("UPDATE tx_wire SET v = 5 WHERE id = 1", &[])
        .await
        .expect("c1 update");
    c1.batch_execute("ROLLBACK").await.expect("c1 rollback");

    let seen = c2
        .query_one("SELECT v FROM tx_wire WHERE id = 1", &[])
        .await
        .expect("c2 read after rollback");
    assert_eq!(seen.get::<_, i32>(0), 0);

    c1.batch_execute("BEGIN").await.expect("c1 begin commit");
    c1.execute("UPDATE tx_wire SET v = 9 WHERE id = 1", &[])
        .await
        .expect("c1 update commit");
    c1.batch_execute("COMMIT").await.expect("c1 commit");

    let seen_after = c2
        .query_one("SELECT v FROM tx_wire WHERE id = 1", &[])
        .await
        .expect("c2 read after commit");
    assert_eq!(seen_after.get::<_, i32>(0), 9);
}

#[tokio::test]
async fn pgwire_tls_handshake_and_query_works() {
    let cfg = ServerConfig {
        data_path: std::env::temp_dir().join("placeholder.db"),
        host: "127.0.0.1".to_string(),
        port: 0,
        buffer_pool_size: 128,
        max_connections: 64,
        max_statement_bytes: 1024 * 1024,
        query_timeout_ms: 30_000,
        auth_method: entdb_server::server::auth::AuthMethod::Md5,
        scram_iterations: 4096,
        auth_user: "entdb".to_string(),
        auth_password: "entdb".to_string(),
        tls_cert: None,
        tls_key: None,
    };
    let (server, cert_path) = TestServer::start_with_tls_config(cfg).await;
    let dsn = format!(
        "host=localhost port={} user=entdb password=entdb dbname=entdb sslmode=require",
        server.addr.port()
    );
    let connector = tls_connector_for_cert(&cert_path);
    let (client, connection) = tokio_postgres::connect(&dsn, connector)
        .await
        .expect("connect with tls");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute("CREATE TABLE tls_health (id INTEGER);")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO tls_health VALUES (1);")
        .await
        .expect("insert row");
    let row = client
        .query_one("SELECT id FROM tls_health LIMIT 1", &[])
        .await
        .expect("select");
    assert_eq!(row.get::<_, i32>(0), 1);
}

#[tokio::test]
async fn pgwire_tls_certificate_rotation_via_restart_works() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_path = dir.path().join("tls-rotate.db");
    let cert_path = dir.path().join("server.crt");
    let key_path = dir.path().join("server.key");

    let (cert1, key1) = generate_test_cert_pair();
    std::fs::write(&cert_path, cert1).expect("write cert1");
    std::fs::write(&key_path, key1).expect("write key1");

    let cfg = ServerConfig {
        data_path: data_path.clone(),
        host: "127.0.0.1".to_string(),
        port: 0,
        buffer_pool_size: 128,
        max_connections: 64,
        max_statement_bytes: 1024 * 1024,
        query_timeout_ms: 30_000,
        auth_method: entdb_server::server::auth::AuthMethod::Md5,
        scram_iterations: 4096,
        auth_user: "entdb".to_string(),
        auth_password: "entdb".to_string(),
        tls_cert: Some(cert_path.clone()),
        tls_key: Some(key_path.clone()),
    };

    let server1 = TestServer::start_with_existing_path(&data_path, cfg.clone()).await;
    let dsn1 = format!(
        "host=localhost port={} user=entdb password=entdb dbname=entdb sslmode=require",
        server1.addr.port()
    );
    let connector1 = tls_connector_for_cert(&cert_path);
    let (client1, conn1) = tokio_postgres::connect(&dsn1, connector1)
        .await
        .expect("connect tls with cert1");
    tokio::spawn(async move {
        let _ = conn1.await;
    });
    client1
        .batch_execute("CREATE TABLE tls_rotate_health (id INTEGER); INSERT INTO tls_rotate_health VALUES (1);")
        .await
        .expect("seed");
    server1.shutdown_gracefully().await;

    let (cert2, key2) = generate_test_cert_pair();
    std::fs::write(&cert_path, cert2).expect("write cert2");
    std::fs::write(&key_path, key2).expect("write key2");

    let server2 = TestServer::start_with_existing_path(&data_path, cfg).await;
    let dsn2 = format!(
        "host=localhost port={} user=entdb password=entdb dbname=entdb sslmode=require",
        server2.addr.port()
    );
    let connector2 = tls_connector_for_cert(&cert_path);
    let (client2, conn2) = tokio_postgres::connect(&dsn2, connector2)
        .await
        .expect("connect tls with rotated cert");
    tokio::spawn(async move {
        let _ = conn2.await;
    });

    let row = client2
        .query_one("SELECT id FROM tls_rotate_health LIMIT 1", &[])
        .await
        .expect("query after cert rotation restart");
    assert_eq!(row.get::<_, i32>(0), 1);
}

#[tokio::test]
async fn graceful_shutdown_persists_state_across_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_path = dir.path().join("restart.db");
    let cfg = ServerConfig {
        data_path: data_path.clone(),
        host: "127.0.0.1".to_string(),
        port: 0,
        buffer_pool_size: 128,
        max_connections: 64,
        max_statement_bytes: 1024 * 1024,
        query_timeout_ms: 30_000,
        auth_method: entdb_server::server::auth::AuthMethod::Md5,
        scram_iterations: 4096,
        auth_user: "entdb".to_string(),
        auth_password: "entdb".to_string(),
        tls_cert: None,
        tls_key: None,
    };

    let server1 = TestServer::start_with_existing_path(&data_path, cfg.clone()).await;
    let dsn1 = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server1.addr.port()
    );
    let (c1, conn1) = tokio_postgres::connect(&dsn1, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = conn1.await;
    });
    c1.batch_execute("CREATE TABLE restart_t (id INTEGER, v INTEGER);")
        .await
        .expect("create");
    c1.batch_execute("INSERT INTO restart_t VALUES (1, 42);")
        .await
        .expect("insert");
    server1.shutdown_gracefully().await;

    let server2 = TestServer::start_with_existing_path(&data_path, cfg).await;
    let dsn2 = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server2.addr.port()
    );
    let (c2, conn2) = tokio_postgres::connect(&dsn2, NoTls)
        .await
        .expect("reconnect");
    tokio::spawn(async move {
        let _ = conn2.await;
    });
    let row = c2
        .query_one("SELECT v FROM restart_t WHERE id = 1", &[])
        .await
        .expect("select persisted row");
    assert_eq!(row.get::<_, i32>(0), 42);
}

#[tokio::test]
async fn crash_restart_inflight_txn_does_not_leak_uncommitted_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_path = dir.path().join("crash-restart.db");
    let cfg = ServerConfig {
        data_path: data_path.clone(),
        host: "127.0.0.1".to_string(),
        port: 0,
        buffer_pool_size: 128,
        max_connections: 64,
        max_statement_bytes: 1024 * 1024,
        query_timeout_ms: 30_000,
        auth_method: entdb_server::server::auth::AuthMethod::Md5,
        scram_iterations: 4096,
        auth_user: "entdb".to_string(),
        auth_password: "entdb".to_string(),
        tls_cert: None,
        tls_key: None,
    };

    let server0 = TestServer::start_with_existing_path(&data_path, cfg.clone()).await;
    let dsn0 = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server0.addr.port()
    );
    let (c0, conn0) = tokio_postgres::connect(&dsn0, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = conn0.await;
    });

    c0.batch_execute("CREATE TABLE inflight_t (id INTEGER, v INTEGER);")
        .await
        .expect("create");
    c0.batch_execute("INSERT INTO inflight_t VALUES (1, 1);")
        .await
        .expect("seed");
    server0.shutdown_gracefully().await;

    let mut server1 = TestServer::start_with_existing_path(&data_path, cfg.clone()).await;
    let dsn1 = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server1.addr.port()
    );
    let (c1, conn1) = tokio_postgres::connect(&dsn1, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = conn1.await;
    });

    c1.batch_execute("BEGIN").await.expect("begin");
    c1.execute("UPDATE inflight_t SET v = 99 WHERE id = 1", &[])
        .await
        .expect("update uncommitted");

    if let Some(tx) = server1.shutdown.take() {
        let _ = tx.send(());
    }
    server1.join.abort();

    let server2 = TestServer::start_with_existing_path(&data_path, cfg).await;
    let dsn2 = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server2.addr.port()
    );
    let (c2, conn2) = tokio_postgres::connect(&dsn2, NoTls)
        .await
        .expect("reconnect");
    tokio::spawn(async move {
        let _ = conn2.await;
    });
    let row = c2
        .query_one("SELECT v FROM inflight_t WHERE id = 1", &[])
        .await
        .expect("select after restart");
    assert_eq!(row.get::<_, i32>(0), 1);
}

#[tokio::test]
async fn crash_restart_after_rollback_keeps_last_committed_value() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_path = dir.path().join("crash-rollback.db");
    let cfg = ServerConfig {
        data_path: data_path.clone(),
        host: "127.0.0.1".to_string(),
        port: 0,
        buffer_pool_size: 128,
        max_connections: 64,
        max_statement_bytes: 1024 * 1024,
        query_timeout_ms: 30_000,
        auth_method: entdb_server::server::auth::AuthMethod::Md5,
        scram_iterations: 4096,
        auth_user: "entdb".to_string(),
        auth_password: "entdb".to_string(),
        tls_cert: None,
        tls_key: None,
    };

    let server0 = TestServer::start_with_existing_path(&data_path, cfg.clone()).await;
    let dsn0 = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server0.addr.port()
    );
    let (c0, conn0) = tokio_postgres::connect(&dsn0, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = conn0.await;
    });

    c0.batch_execute("CREATE TABLE rollback_t (id INTEGER, v INTEGER);")
        .await
        .expect("create");
    c0.batch_execute("INSERT INTO rollback_t VALUES (1, 7);")
        .await
        .expect("seed");
    server0.shutdown_gracefully().await;

    let mut server1 = TestServer::start_with_existing_path(&data_path, cfg.clone()).await;
    let dsn1 = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server1.addr.port()
    );
    let (c1, conn1) = tokio_postgres::connect(&dsn1, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = conn1.await;
    });

    c1.batch_execute("BEGIN").await.expect("begin");
    c1.execute("UPDATE rollback_t SET v = 8 WHERE id = 1", &[])
        .await
        .expect("update");
    c1.batch_execute("ROLLBACK").await.expect("rollback");

    if let Some(tx) = server1.shutdown.take() {
        let _ = tx.send(());
    }
    server1.join.abort();

    let server2 = TestServer::start_with_existing_path(&data_path, cfg).await;
    let dsn2 = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server2.addr.port()
    );
    let (c2, conn2) = tokio_postgres::connect(&dsn2, NoTls)
        .await
        .expect("reconnect");
    tokio::spawn(async move {
        let _ = conn2.await;
    });
    let row = c2
        .query_one("SELECT v FROM rollback_t WHERE id = 1", &[])
        .await
        .expect("select after restart");
    assert_eq!(row.get::<_, i32>(0), 7);
}

#[tokio::test]
async fn query_timeout_enforced_for_heavy_sort_statement() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_path = dir.path().join("timeout-heavy.db");
    let setup_cfg = ServerConfig {
        data_path: data_path.clone(),
        host: "127.0.0.1".to_string(),
        port: 0,
        buffer_pool_size: 128,
        max_connections: 64,
        max_statement_bytes: 1024 * 1024,
        query_timeout_ms: 30_000,
        auth_method: entdb_server::server::auth::AuthMethod::Md5,
        scram_iterations: 4096,
        auth_user: "entdb".to_string(),
        auth_password: "entdb".to_string(),
        tls_cert: None,
        tls_key: None,
    };
    let server_setup = TestServer::start_with_existing_path(&data_path, setup_cfg.clone()).await;
    let dsn_setup = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server_setup.addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&dsn_setup, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute("CREATE TABLE timeout_t (id INTEGER, v INTEGER);")
        .await
        .expect("create");
    for i in 0..1500_i32 {
        client
            .execute("INSERT INTO timeout_t VALUES ($1, $2)", &[&i, &(1500 - i)])
            .await
            .expect("insert");
    }
    server_setup.shutdown_gracefully().await;

    let timeout_cfg = ServerConfig {
        query_timeout_ms: 1,
        ..setup_cfg
    };
    let server_query = TestServer::start_with_existing_path(&data_path, timeout_cfg).await;
    let dsn_query = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        server_query.addr.port()
    );
    let (client2, connection2) = tokio_postgres::connect(&dsn_query, NoTls)
        .await
        .expect("connect timeout server");
    tokio::spawn(async move {
        let _ = connection2.await;
    });

    let err = client2
        .query("SELECT id FROM timeout_t ORDER BY v", &[])
        .await
        .expect_err("query should timeout");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "57014");
}
