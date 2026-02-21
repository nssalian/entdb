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

use entdb_server::server::{serve, Database, ServerConfig};
use rand::{rngs::StdRng, RngCore, SeedableRng};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_postgres::NoTls;

#[tokio::test]
async fn malformed_startup_packets_do_not_take_down_server() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_path = dir.path().join("ingress.db");
    let db = Arc::new(Database::open(&data_path, 128).expect("open db"));
    let cfg = Arc::new(ServerConfig {
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
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let (tx, rx) = oneshot::channel();
    let join = tokio::spawn(async move {
        serve(listener, cfg, db, None, Some(rx))
            .await
            .expect("serve");
    });

    let mut rng = StdRng::seed_from_u64(0xE17DB);
    for _ in 0..64 {
        let mut stream = TcpStream::connect(addr).await.expect("connect raw");
        let mut payload = vec![0_u8; 1 + (rng.next_u32() as usize % 96)];
        rng.fill_bytes(&mut payload);
        stream.write_all(&payload).await.expect("write malformed");
        let _ = stream.shutdown().await;
    }

    let dsn = format!(
        "host=127.0.0.1 port={} user=entdb password=entdb dbname=entdb",
        addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .expect("connect pg");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("CREATE TABLE ingress_health (id INTEGER);")
        .await
        .expect("create health table");
    client
        .batch_execute("INSERT INTO ingress_health VALUES (1);")
        .await
        .expect("insert health row");
    let row = client
        .query_one("SELECT id FROM ingress_health LIMIT 1", &[])
        .await
        .expect("query");
    assert_eq!(row.get::<_, i32>(0), 1);

    let _ = tx.send(());
    let _ = join.await;
}
