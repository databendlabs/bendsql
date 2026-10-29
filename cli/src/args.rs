// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use databend_client::{is_cloud_webapi_host, SensitiveString, DEFAULT_CLOUD_WEBAPI_HOST};
use percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC};

#[cfg(test)]
mod cloud_api_token_tests;

#[derive(Clone, PartialEq, Default)]
pub struct ConnectionArgs {
    pub host: String,
    pub port: Option<u16>,
    pub user: String,
    pub password: SensitiveString,
    pub database: Option<String>,
    pub flight: bool,
    // Keep explicit transport intent until CLI/config/environment auth is merged.
    pub dsn_scheme: Option<String>,
    pub args: BTreeMap<String, String>,
}

impl std::fmt::Debug for ConnectionArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let args: BTreeMap<_, _> = self
            .args
            .iter()
            .map(|(key, value)| {
                let value = if matches!(
                    key.as_str(),
                    "cloud_api_token" | "access_token" | "session_token"
                ) {
                    "**REDACTED**"
                } else {
                    value.as_str()
                };
                (key.as_str(), value)
            })
            .collect();
        f.debug_struct("ConnectionArgs")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &self.password)
            .field("database", &self.database)
            .field("flight", &self.flight)
            .field("dsn_scheme", &self.dsn_scheme)
            .field("args", &args)
            .finish()
    }
}

impl ConnectionArgs {
    pub fn is_native_auth_option(key: &str) -> bool {
        matches!(
            key,
            "access_token"
                | "access_token_file"
                | "private_key_file"
                | "private_key_passphrase_file"
        )
    }

    pub fn apply_cloud_api_token(
        &mut self,
        command_line: Option<SensitiveString>,
        environment: Option<SensitiveString>,
    ) {
        if let Some(token) = command_line {
            self.args.insert("cloud_api_token".into(), token.into());
        } else if !self.args.contains_key("cloud_api_token") {
            if let Some(token) = environment.filter(|token| !token.inner().is_empty()) {
                self.args.insert("cloud_api_token".into(), token.into());
            }
        }
    }

    pub fn validate_cloud_options(&self, enable_ui: bool) -> Result<()> {
        if !self.args.contains_key("org") {
            if self.args.contains_key("cloud_api_token") {
                return Err(anyhow!("cloud_api_token requires org"));
            }
            return Ok(());
        }
        if !is_cloud_webapi_host(&self.host) || self.port.is_some_and(|port| port != 443) {
            return Err(anyhow!(
                "Cloud WebAPI requires an official HTTPS API host on port 443"
            ));
        }
        if self
            .args
            .get("sslmode")
            .is_some_and(|mode| mode == "disable")
        {
            return Err(anyhow!(
                "Cloud WebAPI requires HTTPS; sslmode=disable is not supported"
            ));
        }
        if self.args.contains_key("org_slug") {
            return Err(anyhow!(
                "org_slug is not supported; use org (Organization ID)"
            ));
        }
        if self.args.keys().any(|key| Self::is_native_auth_option(key)) {
            return Err(anyhow!(
                "cloud_api_token cannot be combined with Access Token or Key Pair credentials"
            ));
        }
        if self.args.contains_key("tenant") {
            return Err(anyhow!(
                "cloud_api_token uses org, not a separate tenant parameter"
            ));
        }
        if enable_ui {
            return Err(anyhow!(
                "--ui is not supported with cloud_api_token authentication: the web Python runner can expose account credentials"
            ));
        }
        if self.dsn_scheme.as_deref() == Some("databend+http") {
            return Err(anyhow!(
                "cloud_api_token authentication requires HTTPS; databend+http is not supported"
            ));
        }
        if self.flight || self.dsn_scheme.as_deref() == Some("databend+grpc") {
            return Err(anyhow!(
                "cloud_api_token authentication is not supported with FlightSQL"
            ));
        }
        Ok(())
    }

    pub fn get_dsn(self) -> Result<String> {
        self.validate_cloud_options(false)?;
        if self.flight && self.args.contains_key("private_key_file") {
            return Err(anyhow!(
                "key-pair authentication is not supported with FlightSQL"
            ));
        }

        let mut dsn = url::Url::parse("databend://")?;
        dsn.set_host(Some(&self.host))?;
        _ = dsn.set_port(self.port);
        if !self.args.contains_key("cloud_api_token") {
            _ = dsn.set_username(&self.user);
            let password = utf8_percent_encode(self.password.inner(), NON_ALPHANUMERIC).to_string();
            _ = dsn.set_password(Some(&password));
        }
        if let Some(database) = self.database {
            dsn.set_path(&database);
        }
        if self.flight {
            _ = dsn.set_scheme("databend+flight");
        }
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        if !self.args.is_empty() {
            for (k, v) in self.args {
                query.append_pair(&k, &v);
            }
        }
        dsn.set_query(Some(&query.finish()));
        Ok(dsn.to_string())
    }

    pub fn from_dsn(dsn: &str) -> Result<Self> {
        let u = url::Url::parse(dsn)?;
        let mut args = BTreeMap::new();
        if let Some(query) = u.query() {
            for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
                if matches!(k.as_ref(), "cloud_api_token" | "org") && args.contains_key(k.as_ref())
                {
                    return Err(anyhow!(
                        "cloud_api_token and org must each be specified only once"
                    ));
                }
                args.insert(k.to_string(), v.to_string());
            }
        }
        if args.contains_key("org") && u.scheme().ends_with("+grpc") {
            return Err(anyhow!(
                "cloud_api_token authentication is not supported with FlightSQL"
            ));
        }
        let flight = u.scheme().ends_with("+flight");
        let host = u
            .host_str()
            .or_else(|| {
                args.contains_key("org")
                    .then_some(DEFAULT_CLOUD_WEBAPI_HOST)
            })
            .ok_or(anyhow!("missing host"))?
            .to_string();
        let port = u.port();
        let user = u.username().to_string();
        let password = u.password().unwrap_or_default();
        let password = if args.contains_key("org")
            || args.contains_key("cloud_api_token")
            || is_cloud_webapi_host(&host)
        {
            String::new()
        } else {
            percent_decode_str(password).decode_utf8()?.to_string()
        };
        let password = SensitiveString::from(password);
        let database = u.path().strip_prefix('/').map(|s| s.to_string());
        Ok(Self {
            host,
            port,
            user,
            password,
            database,
            flight,
            dsn_scheme: Some(u.scheme().to_string()),
            args,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_dsn() -> Result<()> {
        let dsn = "databend://username:3a%40SC%28nYE%25a1k%3D%7B%7BR@app.databend.com/test?wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&warehouse=wh&sslmode=disable";
        let args = ConnectionArgs::from_dsn(dsn)?;
        let password_str = "3a@SC(nYE%a1k={{R";
        assert_eq!(args.password.inner(), password_str);
        let expected = ConnectionArgs {
            host: "app.databend.com".to_string(),
            port: None,
            dsn_scheme: Some("databend".to_string()),
            user: "username".to_string(),
            password: SensitiveString::from(password_str),
            database: Some("test".to_string()),
            flight: false,
            args: {
                let mut args = BTreeMap::new();
                args.insert("wait_time_secs".to_string(), "10".to_string());
                args.insert("max_rows_in_buffer".to_string(), "5000000".to_string());
                args.insert("max_rows_per_page".to_string(), "10000".to_string());
                args.insert("warehouse".to_string(), "wh".to_string());
                args.insert("sslmode".to_string(), "disable".to_string());
                args
            },
        };
        assert_eq!(args, expected);
        Ok(())
    }

    #[test]
    fn reject_keypair_flight_dsn() {
        let args = ConnectionArgs {
            host: "app.databend.com".to_string(),
            port: Some(443),
            user: "username".to_string(),
            password: SensitiveString::from(""),
            database: None,
            flight: true,
            dsn_scheme: None,
            args: {
                let mut args = BTreeMap::new();
                args.insert("private_key_file".to_string(), "key.pem".to_string());
                args
            },
        };

        let err = args
            .get_dsn()
            .expect_err("key-pair auth with FlightSQL should be rejected");
        assert!(
            err.to_string()
                .contains("key-pair authentication is not supported with FlightSQL"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn format_dsn() -> Result<()> {
        let password_str = "3a@SC(nYE%a1k={{R";
        let args = ConnectionArgs {
            host: "app.databend.com".to_string(),
            port: Some(443),
            dsn_scheme: None,
            user: "username".to_string(),
            password: SensitiveString::from(password_str),
            database: Some("test".to_string()),
            flight: false,
            args: {
                let mut args = BTreeMap::new();
                args.insert("wait_time_secs".to_string(), "10".to_string());
                args.insert("max_rows_in_buffer".to_string(), "5000000".to_string());
                args.insert("max_rows_per_page".to_string(), "10000".to_string());
                args.insert("warehouse".to_string(), "wh".to_string());
                args.insert("sslmode".to_string(), "disable".to_string());
                args
            },
        };
        let dsn = args.get_dsn()?;
        let expected = "databend://username:3a%40SC%28nYE%25a1k%3D%7B%7BR@app.databend.com:443/test?max_rows_in_buffer=5000000&max_rows_per_page=10000&sslmode=disable&wait_time_secs=10&warehouse=wh";
        assert_eq!(dsn, expected);
        Ok(())
    }
}
