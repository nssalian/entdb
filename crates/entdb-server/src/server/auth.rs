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

use async_trait::async_trait;
use pgwire::api::auth::md5pass::hash_md5_password;
use pgwire::api::auth::scram::gen_salted_password;
use pgwire::api::auth::{AuthSource, LoginInfo, Password};
use pgwire::error::PgWireResult;

pub fn random_salt() -> [u8; 4] {
    rand::random::<[u8; 4]>()
}

pub fn random_scram_salt() -> [u8; 16] {
    rand::random::<[u8; 16]>()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    Md5,
    ScramSha256,
}

impl AuthMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            AuthMethod::Md5 => "md5",
            AuthMethod::ScramSha256 => "scram-sha-256",
        }
    }
}

#[derive(Clone)]
pub struct EntAuthSource {
    pub method: AuthMethod,
    pub expected_user: String,
    pub expected_password: String,
    pub scram_iterations: usize,
}

#[async_trait]
impl AuthSource for EntAuthSource {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        let user = login_info.user().unwrap_or_default().to_string();
        let password = if user == self.expected_user {
            self.expected_password.as_str()
        } else {
            // Keep auth-time shape uniform for unknown users.
            "invalid-user"
        };

        match self.method {
            AuthMethod::Md5 => {
                let salt = random_salt();
                let md5 = hash_md5_password(&user, password, &salt);
                Ok(Password::new(Some(salt.to_vec()), md5.into_bytes()))
            }
            AuthMethod::ScramSha256 => {
                let salt = random_scram_salt();
                let salted = gen_salted_password(password, &salt, self.scram_iterations);
                Ok(Password::new(Some(salt.to_vec()), salted))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthMethod, EntAuthSource};
    use pgwire::api::auth::{AuthSource, LoginInfo};

    #[tokio::test]
    async fn auth_source_returns_md5_password_bytes() {
        let source = EntAuthSource {
            method: AuthMethod::Md5,
            expected_user: "entdb".to_string(),
            expected_password: "entdb".to_string(),
            scram_iterations: 4096,
        };
        let login = LoginInfo::new(Some("entdb"), None, "127.0.0.1".to_string());
        let pass = source.get_password(&login).await.expect("password");
        assert!(pass.salt().is_some());
        let s = String::from_utf8(pass.password().to_vec()).expect("utf8");
        assert!(s.starts_with("md5"));
    }

    #[tokio::test]
    async fn auth_source_returns_scram_salted_password_bytes() {
        let source = EntAuthSource {
            method: AuthMethod::ScramSha256,
            expected_user: "entdb".to_string(),
            expected_password: "entdb".to_string(),
            scram_iterations: 4096,
        };
        let login = LoginInfo::new(Some("entdb"), None, "127.0.0.1".to_string());
        let pass = source.get_password(&login).await.expect("password");
        assert_eq!(pass.salt().map(|s| s.len()), Some(16));
        // SHA-256 salted password bytes
        assert_eq!(pass.password().len(), 32);
    }
}
