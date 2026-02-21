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

pub mod auth;
pub mod handler;
pub mod metrics;
pub mod type_map;

use crate::server::handler::{scan_max_txn_id_from_storage, EntHandler};
use crate::server::metrics::ServerMetrics;
use entdb::catalog::Catalog;
use entdb::error::{EntDbError, Result};
use entdb::query::history::OptimizerHistoryRecorder;
use entdb::query::optimizer::OptimizerConfig;
use entdb::storage::buffer_pool::BufferPool;
use entdb::storage::buffer_pool::BufferPoolStats;
use entdb::storage::disk_manager::DiskManager;
use entdb::tx::TransactionManager;
use entdb::wal::log_manager::LogManager;
use entdb::wal::recovery::RecoveryManager;
use futures::Sink;
use pgwire::api::auth::md5pass::Md5PasswordAuthStartupHandler;
use pgwire::api::auth::scram::SASLScramAuthStartupHandler;
use pgwire::api::auth::DefaultServerParameterProvider;
use pgwire::api::auth::StartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::ClientInfo;
use pgwire::api::NoopErrorHandler;
use pgwire::api::PgWireServerHandlers;
use pgwire::error::PgWireError;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;
use rustls_pemfile::{certs, pkcs8_private_keys};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
use tokio_rustls::rustls::ServerConfig as RustlsServerConfig;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info};

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub data_path: PathBuf,
    pub host: String,
    pub port: u16,
    pub buffer_pool_size: usize,
    pub max_connections: usize,
    pub max_statement_bytes: usize,
    pub query_timeout_ms: u64,
    pub auth_method: crate::server::auth::AuthMethod,
    pub scram_iterations: usize,
    pub auth_user: String,
    pub auth_password: String,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
}

impl ServerConfig {
    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn validate(&self) -> Result<()> {
        if self.host.trim().is_empty() {
            return Err(EntDbError::Query("host cannot be empty".to_string()));
        }
        if self.port == 0 {
            return Err(EntDbError::Query("port must be > 0".to_string()));
        }
        if self.buffer_pool_size == 0 {
            return Err(EntDbError::Query(
                "buffer_pool_size must be > 0".to_string(),
            ));
        }
        if self.max_connections == 0 {
            return Err(EntDbError::Query("max_connections must be > 0".to_string()));
        }
        if self.max_statement_bytes == 0 {
            return Err(EntDbError::Query(
                "max_statement_bytes must be > 0".to_string(),
            ));
        }
        if self.query_timeout_ms == 0 {
            return Err(EntDbError::Query(
                "query_timeout_ms must be > 0".to_string(),
            ));
        }
        if self.auth_user.trim().is_empty() {
            return Err(EntDbError::Query("auth_user cannot be empty".to_string()));
        }
        if self.auth_password.is_empty() {
            return Err(EntDbError::Query(
                "auth_password cannot be empty".to_string(),
            ));
        }
        if matches!(
            self.auth_method,
            crate::server::auth::AuthMethod::ScramSha256
        ) && self.scram_iterations < 4096
        {
            return Err(EntDbError::Query(
                "scram_iterations must be >= 4096".to_string(),
            ));
        }
        match (&self.tls_cert, &self.tls_key) {
            (Some(cert), Some(key)) => {
                if !cert.exists() {
                    return Err(EntDbError::Query(format!(
                        "tls_cert path does not exist: {}",
                        cert.display()
                    )));
                }
                if !key.exists() {
                    return Err(EntDbError::Query(format!(
                        "tls_key path does not exist: {}",
                        key.display()
                    )));
                }
            }
            (None, None) => {}
            _ => {
                return Err(EntDbError::Query(
                    "tls_cert and tls_key must both be provided together".to_string(),
                ))
            }
        }
        Ok(())
    }
}

pub struct Database {
    pub disk_manager: Arc<DiskManager>,
    pub log_manager: Arc<LogManager>,
    pub buffer_pool: Arc<BufferPool>,
    pub catalog: Arc<Catalog>,
    pub txn_manager: Arc<TransactionManager>,
    pub optimizer_history: Arc<OptimizerHistoryRecorder>,
    pub optimizer_config: OptimizerConfig,
}

impl Database {
    pub fn open(data_path: &Path, buffer_pool_size: usize) -> Result<Self> {
        let disk_manager = Arc::new(DiskManager::new(data_path)?);

        let mut wal_path = data_path.to_path_buf();
        wal_path.set_extension("wal");
        let log_manager = Arc::new(LogManager::new(wal_path, 4096)?);

        let buffer_pool = Arc::new(BufferPool::with_log_manager(
            buffer_pool_size,
            Arc::clone(&disk_manager),
            Arc::clone(&log_manager),
        ));

        RecoveryManager::new(Arc::clone(&log_manager), Arc::clone(&buffer_pool)).recover()?;

        let catalog = Arc::new(Catalog::load(Arc::clone(&buffer_pool))?);
        validate_catalog_page_references(&catalog)?;

        let mut txn_state_path = data_path.to_path_buf();
        txn_state_path.set_extension("txn.json");
        let mut txn_wal_path = data_path.to_path_buf();
        txn_wal_path.set_extension("txn.wal");

        let txn_manager = TransactionManager::with_wal_persistence(&txn_state_path, &txn_wal_path)
            .or_else(|_| TransactionManager::with_persistence(&txn_state_path))
            .unwrap_or_else(|_| TransactionManager::new());

        if let Ok(max_txn) = scan_max_txn_id_from_storage(&catalog) {
            txn_manager.ensure_min_next_txn_id(max_txn.saturating_add(1));
        }

        let optimizer_history_path = optimizer_history_path_for_data_path(data_path);
        let optimizer_history = OptimizerHistoryRecorder::new(
            optimizer_history_path,
            optimizer_history_schema_hash(),
            16,
            1024,
        )
        .or_else(|_| {
            OptimizerHistoryRecorder::new(
                std::env::temp_dir().join("entdb.optimizer_history.server.fallback.json"),
                optimizer_history_schema_hash(),
                16,
                1024,
            )
        })?;

        let mut optimizer_config = OptimizerConfig::default();
        if let Ok(v) = std::env::var("ENTDB_CBO") {
            optimizer_config.cbo_enabled = v == "1" || v.eq_ignore_ascii_case("true");
        }
        if let Ok(v) = std::env::var("ENTDB_HBO") {
            optimizer_config.hbo_enabled = v == "1" || v.eq_ignore_ascii_case("true");
        }
        if let Ok(v) = std::env::var("ENTDB_OPT_MAX_SEARCH_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                optimizer_config.max_search_ms = ms;
            }
        }
        if let Ok(v) = std::env::var("ENTDB_OPT_MAX_JOIN_RELATIONS") {
            if let Ok(n) = v.parse::<usize>() {
                optimizer_config.max_join_relations = n;
            }
        }
        let optimizer_config = optimizer_config.sanitize();

        Ok(Self {
            disk_manager,
            log_manager,
            buffer_pool,
            catalog,
            txn_manager: Arc::new(txn_manager),
            optimizer_history: Arc::new(optimizer_history),
            optimizer_config,
        })
    }
}

pub fn optimizer_history_path_for_data_path(data_path: &Path) -> PathBuf {
    let mut p = data_path.to_path_buf();
    p.set_extension("optimizer_history.json");
    p
}

pub fn optimizer_history_schema_hash() -> &'static str {
    "optimizer_history_schema_v1_planner_v1"
}

fn validate_catalog_page_references(catalog: &Catalog) -> Result<()> {
    let bp = catalog.buffer_pool();
    for table in catalog.list_tables() {
        bp.fetch_page(table.first_page_id).map_err(|e| {
            EntDbError::Corruption(format!(
                "catalog table '{}' references missing first_page_id {}: {e}",
                table.name, table.first_page_id
            ))
        })?;

        for idx in &table.indexes {
            bp.fetch_page(idx.root_page_id).map_err(|e| {
                EntDbError::Corruption(format!(
                    "catalog index '{}.{}' references missing root_page_id {}: {e}",
                    table.name, idx.name, idx.root_page_id
                ))
            })?;
        }
    }
    Ok(())
}

struct EntHandlerFactory {
    startup_handler: Arc<EntStartupHandler>,
    query_handler: Arc<EntHandler>,
}

pub enum EntStartupHandler {
    Md5(
        Md5PasswordAuthStartupHandler<
            crate::server::auth::EntAuthSource,
            DefaultServerParameterProvider,
        >,
    ),
    Scram(
        SASLScramAuthStartupHandler<
            crate::server::auth::EntAuthSource,
            DefaultServerParameterProvider,
        >,
    ),
}

#[async_trait::async_trait]
impl StartupHandler for EntStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> pgwire::error::PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match self {
            EntStartupHandler::Md5(h) => h.on_startup(client, message).await,
            EntStartupHandler::Scram(h) => h.on_startup(client, message).await,
        }
    }
}

impl EntHandlerFactory {
    fn new(config: Arc<ServerConfig>, db: Arc<Database>, metrics: Arc<ServerMetrics>) -> Self {
        let auth_source = Arc::new(crate::server::auth::EntAuthSource {
            method: config.auth_method,
            expected_user: config.auth_user.clone(),
            expected_password: config.auth_password.clone(),
            scram_iterations: config.scram_iterations,
        });
        let params = Arc::new(DefaultServerParameterProvider::default());
        let startup_handler = match config.auth_method {
            crate::server::auth::AuthMethod::Md5 => Arc::new(EntStartupHandler::Md5(
                Md5PasswordAuthStartupHandler::new(auth_source, params),
            )),
            crate::server::auth::AuthMethod::ScramSha256 => {
                let mut scram = SASLScramAuthStartupHandler::new(auth_source, params);
                scram.set_iterations(config.scram_iterations);
                Arc::new(EntStartupHandler::Scram(scram))
            }
        };
        Self {
            startup_handler,
            query_handler: Arc::new(EntHandler::new(
                db,
                config.max_statement_bytes,
                config.query_timeout_ms,
                metrics,
            )),
        }
    }
}

impl PgWireServerHandlers for EntHandlerFactory {
    type StartupHandler = EntStartupHandler;
    type SimpleQueryHandler = EntHandler;
    type ExtendedQueryHandler = EntHandler;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        Arc::clone(&self.query_handler)
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::clone(&self.query_handler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::clone(&self.startup_handler)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

pub async fn run(config: ServerConfig) -> Result<()> {
    config.validate()?;
    let config = Arc::new(config);
    let database = Arc::new(Database::open(&config.data_path, config.buffer_pool_size)?);
    let tls_acceptor = build_tls_acceptor(&config)?;
    let listener = TcpListener::bind(config.listen_addr()).await?;
    serve(listener, config, database, tls_acceptor, None).await
}

pub async fn serve(
    listener: TcpListener,
    config: Arc<ServerConfig>,
    database: Arc<Database>,
    tls_acceptor: Option<Arc<TlsAcceptor>>,
    mut shutdown: Option<oneshot::Receiver<()>>,
) -> Result<()> {
    let addr = listener.local_addr()?;
    info!(%addr, "entdb server listening");
    let conn_limit = Arc::new(Semaphore::new(config.max_connections));
    let metrics = Arc::new(ServerMetrics::default());

    loop {
        tokio::select! {
            _ = async {
                if let Some(rx) = &mut shutdown {
                    let _ = rx.await;
                }
            }, if shutdown.is_some() => {
                info!(%addr, "shutdown signal received");
                let bp_stats: BufferPoolStats = database.buffer_pool.stats();
                metrics.set_buffer_pool_pressure(bp_stats);

                let flush_started = std::time::Instant::now();
                database.buffer_pool.flush_all()?;
                metrics.on_shutdown_flush(flush_started.elapsed().as_nanos() as u64);

                let persist_started = std::time::Instant::now();
                database.txn_manager.persist_state()?;
                metrics.on_shutdown_persist(persist_started.elapsed().as_nanos() as u64);
                info!(?bp_stats, metrics=?metrics.snapshot(), "server shutdown metrics");
                break;
            }
            accepted = listener.accept() => {
                let (socket, peer) = accepted?;
                let permit = match Arc::clone(&conn_limit).try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        metrics.on_connection_refused();
                        info!(%peer, "connection refused due to max_connections limit");
                        continue;
                    }
                };
                metrics.on_connection_accepted();
                let factory = Arc::new(EntHandlerFactory::new(
                    Arc::clone(&config),
                    Arc::clone(&database),
                    Arc::clone(&metrics),
                ));
                let tls_for_conn = tls_acceptor.clone();
                let metrics_for_conn = Arc::clone(&metrics);
                info!(%peer, "accepted connection");
                tokio::spawn(async move {
                    let _permit: OwnedSemaphorePermit = permit;
                    if let Err(err) = process_socket(socket, tls_for_conn, factory).await {
                        error!(%peer, error = %err, "connection processing error");
                    }
                    metrics_for_conn.on_connection_closed();
                });
            }
        }
    }

    Ok(())
}

fn build_tls_acceptor(config: &ServerConfig) -> Result<Option<Arc<TlsAcceptor>>> {
    let (Some(cert_path), Some(key_path)) = (&config.tls_cert, &config.tls_key) else {
        return Ok(None);
    };

    let cert_file = std::fs::File::open(cert_path)?;
    let mut cert_reader = std::io::BufReader::new(cert_file);
    let cert_chain = certs(&mut cert_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| EntDbError::Query(format!("invalid tls cert PEM: {e}")))?;

    let key_file = std::fs::File::open(key_path)?;
    let mut key_reader = std::io::BufReader::new(key_file);
    let mut keys = pkcs8_private_keys(&mut key_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| EntDbError::Query(format!("invalid tls key PEM: {e}")))?;
    let Some(key) = keys.pop() else {
        return Err(EntDbError::Query(
            "tls key PEM has no PKCS8 private key".to_string(),
        ));
    };

    let rustls = RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key.into())
        .map_err(|e| EntDbError::Query(format!("failed to build tls config: {e}")))?;

    Ok(Some(Arc::new(TlsAcceptor::from(Arc::new(rustls)))))
}

#[cfg(test)]
mod tests {
    use super::{serve, Database, ServerConfig};
    use entdb::catalog::{Column, Schema};
    use entdb::types::DataType;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn server_accepts_and_stops_with_shutdown_signal() {
        let dir = tempdir().expect("tempdir");
        let data_path = dir.path().join("server.db");
        let db = Arc::new(Database::open(&data_path, 64).expect("open db"));
        let cfg = Arc::new(ServerConfig {
            data_path,
            host: "127.0.0.1".to_string(),
            port: 0,
            buffer_pool_size: 64,
            max_connections: 4,
            max_statement_bytes: 1024 * 1024,
            query_timeout_ms: 30_000,
            auth_method: crate::server::auth::AuthMethod::Md5,
            scram_iterations: 4096,
            auth_user: "entdb".to_string(),
            auth_password: "entdb".to_string(),
            tls_cert: None,
            tls_key: None,
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");

        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            serve(listener, cfg, db, None, Some(rx))
                .await
                .expect("serve")
        });

        tx.send(()).expect("signal shutdown");
        handle.await.expect("join");
    }

    #[test]
    fn server_config_validation_rejects_invalid_limits() {
        let cfg = ServerConfig {
            data_path: "x.db".into(),
            host: "".to_string(),
            port: 0,
            buffer_pool_size: 0,
            max_connections: 0,
            max_statement_bytes: 0,
            query_timeout_ms: 0,
            auth_method: crate::server::auth::AuthMethod::Md5,
            scram_iterations: 0,
            auth_user: "".to_string(),
            auth_password: "".to_string(),
            tls_cert: None,
            tls_key: None,
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn database_open_rejects_catalog_with_missing_table_page() {
        let dir = tempdir().expect("tempdir");
        let data_path = dir.path().join("server-corrupt.db");
        let db = Database::open(&data_path, 64).expect("open db");
        let schema = Schema::new(vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                default: None,
                primary_key: false,
            },
            Column {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                default: None,
                primary_key: false,
            },
        ]);
        let table = db
            .catalog
            .create_table("users", schema)
            .expect("create table");

        db.buffer_pool
            .delete_page(table.first_page_id)
            .expect("delete table root page to simulate corruption");
        drop(db);

        let err = match Database::open(&data_path, 64) {
            Ok(_) => panic!("expected startup integrity validation failure"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("references missing first_page_id"),
            "unexpected error: {err}"
        );
    }
}
