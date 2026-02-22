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

#[derive(Debug, Clone, Parser)]
#[command(name = "entdb", about = "EntDB PostgreSQL-compatible server")]
pub struct Cli {
    #[arg(
        short = 'd',
        long = "data-path",
        alias = "data",
        default_value = "ent.db"
    )]
    pub data_path: String,

    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    #[arg(short, long, default_value_t = 5433)]
    pub port: u16,

    #[arg(long, default_value_t = 1024)]
    pub buffer_pool_size: usize,

    #[arg(long, default_value_t = 256)]
    pub max_connections: usize,

    #[arg(long, default_value_t = 1_048_576)]
    pub max_statement_bytes: usize,

    #[arg(long, default_value_t = 30_000)]
    pub query_timeout_ms: u64,

    #[arg(long, default_value = "md5")]
    pub auth_method: String,

    #[arg(long, default_value_t = 4096)]
    pub scram_iterations: usize,

    #[arg(long, default_value = "entdb")]
    pub auth_user: String,

    #[arg(long, default_value = "entdb")]
    pub auth_password: String,

    #[arg(long)]
    pub auth_password_env: Option<String>,

    #[arg(long)]
    pub auth_password_file: Option<String>,

    #[arg(long)]
    pub tls_cert: Option<String>,

    #[arg(long)]
    pub tls_key: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::Cli;
    use clap::Parser;

    #[test]
    fn cli_parses_data_path_long_flag() {
        let cli = Cli::parse_from(["entdb", "--data-path", "./x.db"]);
        assert_eq!(cli.data_path, "./x.db");
    }

    #[test]
    fn cli_parses_data_alias_flag() {
        let cli = Cli::parse_from(["entdb", "--data", "./y.db"]);
        assert_eq!(cli.data_path, "./y.db");
    }

    #[test]
    fn cli_parses_host_without_short_conflict() {
        let cli = Cli::parse_from(["entdb", "--host", "0.0.0.0"]);
        assert_eq!(cli.host, "0.0.0.0");
    }
}
