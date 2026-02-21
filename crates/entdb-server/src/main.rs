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

use clap::Parser;
use entdb_server::cli::Cli;
use entdb_server::server::auth::AuthMethod;
use entdb_server::server::{run, ServerConfig};

fn resolve_auth_password(cli: &Cli) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(path) = &cli.auth_password_file {
        let content = std::fs::read_to_string(path)?;
        let pw = content.trim_end().to_string();
        if pw.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("empty password in --auth-password-file '{}'", path),
            )
            .into());
        }
        return Ok(pw);
    }
    if let Some(env_name) = &cli.auth_password_env {
        let pw = std::env::var(env_name).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("env var '{}' not set for --auth-password-env", env_name),
            )
        })?;
        if pw.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("env var '{}' is empty", env_name),
            )
            .into());
        }
        return Ok(pw);
    }
    Ok(cli.auth_password.clone())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let auth_method = match cli.auth_method.to_ascii_lowercase().as_str() {
        "md5" => AuthMethod::Md5,
        "scram" | "scram-sha-256" | "scram_sha_256" => AuthMethod::ScramSha256,
        other => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unsupported --auth-method '{other}', expected 'md5' or 'scram-sha-256'"),
            )
            .into());
        }
    };
    let auth_password = resolve_auth_password(&cli)?;

    let config = ServerConfig {
        data_path: cli.data_path.into(),
        host: cli.host,
        port: cli.port,
        buffer_pool_size: cli.buffer_pool_size,
        max_connections: cli.max_connections,
        max_statement_bytes: cli.max_statement_bytes,
        query_timeout_ms: cli.query_timeout_ms,
        auth_method,
        scram_iterations: cli.scram_iterations,
        auth_user: cli.auth_user,
        auth_password,
        tls_cert: cli.tls_cert.map(Into::into),
        tls_key: cli.tls_key.map(Into::into),
    };

    run(config).await?;
    Ok(())
}
