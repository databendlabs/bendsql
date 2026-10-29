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

use std::ffi::OsString;

use anyhow::{anyhow, Result};
use databend_client::{is_cloud_webapi_host, SensitiveString, DEFAULT_CLOUD_WEBAPI_HOST};

use crate::{args::ConnectionArgs, config::ConnectionConfig, Args};

#[cfg(test)]
mod tests;

#[derive(Default)]
pub struct ConnectionEnvironment {
    pub dsn: Option<OsString>,
    pub password: Option<OsString>,
    pub cloud_api_token: Option<OsString>,
}

impl ConnectionEnvironment {
    pub fn capture() -> Self {
        Self {
            dsn: std::env::var_os("BENDSQL_DSN"),
            password: std::env::var_os("BENDSQL_PASSWORD"),
            cloud_api_token: std::env::var_os("BENDSQL_CLOUD_API_TOKEN"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Target {
    Native,
    Cloud { org: String },
}

pub struct ResolvedConnection {
    pub target: Target,
    pub args: ConnectionArgs,
    pub warnings: Vec<String>,
}

fn environment_value<'a>(value: &'a Option<OsString>, name: &str) -> Result<Option<&'a str>> {
    value
        .as_ref()
        .map(|value| {
            value
                .to_str()
                .ok_or_else(|| anyhow!("{name} must be UTF-8"))
        })
        .transpose()
}

/// Resolve CLI/environment/config without I/O, logging, or modifying the inputs.
/// Only --org or a DSN org parameter selects Cloud. Credentials never select it.
pub fn resolve_connection(
    cli: &Args,
    env: &ConnectionEnvironment,
    config: &ConnectionConfig,
) -> Result<ResolvedConnection> {
    for key in ["org", "org_slug", "cloud_api_token"] {
        if config.args.contains_key(key) {
            return Err(anyhow!(
                "Cloud options are not supported in config [connection]; use --org or a Cloud DSN"
            ));
        }
        if cli.set.iter().any(|(name, _)| name == key) {
            return Err(anyhow!(
                "--set {key} is not supported; use --org or --cloud-api-token, or a Cloud DSN"
            ));
        }
    }
    let mut warnings = Vec::new();
    let dsn = if let Some(dsn) = &cli.dsn {
        Some(dsn.inner())
    } else if cli.org.is_some() {
        // --org may override an ambient private (even malformed/non-UTF8) DSN.
        let cloud_dsn = env
            .dsn
            .as_ref()
            .and_then(|dsn| dsn.to_str())
            .and_then(|dsn| url::Url::parse(dsn).ok())
            .is_some_and(|dsn| dsn.query_pairs().any(|(key, _)| key == "org"));
        if cloud_dsn {
            environment_value(&env.dsn, "BENDSQL_DSN")?
        } else {
            if env.dsn.is_some() {
                warnings.push("BENDSQL_DSN ignored when --org is set".into());
            }
            None
        }
    } else {
        environment_value(&env.dsn, "BENDSQL_DSN")?
    };
    let parsed = dsn.map(ConnectionArgs::from_dsn).transpose()?;
    let org = cli.org.clone().or_else(|| {
        parsed
            .as_ref()
            .and_then(|args| args.args.get("org").cloned())
    });
    let cloud = org.is_some();
    if let Some(org) = &org {
        if org.is_empty()
            || org.trim() != org
            || org.len() > 256
            || org.bytes().any(|byte| byte < 32 || byte == 127)
        {
            return Err(anyhow!("org requires a valid Organization ID"));
        }
    }
    let mut args = if cloud {
        for (name, present) in [
            ("user", cli.user.is_some()),
            ("password", cli.password.is_some()),
            ("private-key-file", cli.private_key_file.is_some()),
            (
                "private-key-passphrase-file",
                cli.private_key_passphrase_file.is_some(),
            ),
            ("flight", cli.flight),
        ] {
            if present {
                return Err(anyhow!("Cloud mode cannot be combined with --{name}"));
            }
        }
        if cli
            .set
            .iter()
            .any(|(key, _)| ConnectionArgs::is_native_auth_option(key) || key == "tenant")
        {
            return Err(anyhow!(
                "Cloud mode cannot be combined with native authentication or tenant in --set"
            ));
        }
        if cli
            .host
            .as_deref()
            .is_some_and(|host| !is_cloud_webapi_host(host))
        {
            return Err(anyhow!("Cloud --host must be an official WebAPI host"));
        }
        if cli.port.is_some_and(|port| port != 443)
            || cli.tls == Some(false)
            || cli
                .set
                .iter()
                .any(|(key, value)| key == "sslmode" && value == "disable")
        {
            return Err(anyhow!("Cloud WebAPI requires HTTPS on port 443"));
        }
        if config.host.is_some()
            || config.port.is_some()
            || config.user.is_some()
            || config.database.is_some()
            || config.tls.is_some()
            || !config.args.is_empty()
        {
            warnings.push(
                if cli.org.is_some() {
                    "config [connection] ignored when --org is set"
                } else {
                    "config [connection] ignored for Cloud DSN"
                }
                .into(),
            );
        }
        if env.password.is_some() {
            warnings.push("BENDSQL_PASSWORD ignored in Cloud mode".into());
        }
        if let Some(args) = parsed {
            if !is_cloud_webapi_host(&args.host) {
                return Err(if cli.org.is_some() {
                    anyhow!("--org conflicts with the Gateway/private target in --dsn")
                } else {
                    anyhow!("Cloud WebAPI requires an official HTTPS API host on port 443")
                });
            }
            args
        } else {
            ConnectionArgs {
                host: cli
                    .host
                    .clone()
                    .unwrap_or_else(|| DEFAULT_CLOUD_WEBAPI_HOST.into()),
                port: Some(443),
                ..Default::default()
            }
        }
    } else if let Some(args) = parsed {
        args
    } else {
        let mut args = ConnectionArgs {
            host: cli
                .host
                .clone()
                .or_else(|| config.host.clone())
                .unwrap_or_else(|| "localhost".into()),
            port: cli.port.or(config.port).or(Some(8000)),
            user: cli
                .user
                .clone()
                .or_else(|| config.user.clone())
                .unwrap_or_else(|| "root".into()),
            database: config.database.clone(),
            flight: cli.flight,
            args: config.args.clone(),
            ..Default::default()
        };
        if !cli.tls.or(config.tls).unwrap_or(false) {
            args.args.insert("sslmode".into(), "disable".into());
        }
        args
    };
    if dsn.is_some() {
        for (name, present) in [
            ("host", cli.host.is_some()),
            ("port", cli.port.is_some()),
            ("tls", cli.tls.is_some()),
            ("set", !cli.set.is_empty()),
            ("flight", cli.flight),
        ] {
            if present {
                warnings.push(format!("--{name} is ignored when --dsn is set"));
            }
        }
    } else {
        args.args.extend(cli.set.clone());
    }
    if let Some(database) = &cli.database {
        args.database = Some(database.clone());
    }
    if let Some(role) = &cli.role {
        args.args.insert("role".into(), role.clone());
    }
    if cloud {
        args.args.insert("org".into(), org.clone().unwrap());
        args.apply_cloud_api_token(
            cli.cloud_api_token.clone(),
            if cli.cloud_api_token.is_none() && !args.args.contains_key("cloud_api_token") {
                environment_value(&env.cloud_api_token, "BENDSQL_CLOUD_API_TOKEN")?
                    .map(SensitiveString::from)
            } else {
                None
            },
        );
        if !args.args.contains_key("cloud_api_token") {
            return Err(anyhow!("Cloud mode requires cloud_api_token in DSN, --cloud-api-token or BENDSQL_CLOUD_API_TOKEN"));
        }
        if args.args["cloud_api_token"].trim().is_empty() {
            return Err(anyhow!("cloud_api_token must not be empty"));
        }
    } else {
        if cli.cloud_api_token.is_some() || args.args.contains_key("cloud_api_token") {
            return Err(anyhow!(
                "cloud_api_token requires org; use --org <ORG> or org in a Cloud DSN"
            ));
        }
        if is_cloud_webapi_host(&args.host) {
            return Err(anyhow!(
                "Cloud WebAPI endpoint requires org; use --org <ORG> or org in a Cloud DSN"
            ));
        }
        if let Some(user) = &cli.user {
            args.user = user.clone();
        }
        if let Some(password) = &cli.password {
            args.password = password.clone();
        } else if let Some(password) = environment_value(&env.password, "BENDSQL_PASSWORD")? {
            args.password = password.into();
        }
        if let Some(key) = &cli.private_key_file {
            args.args.insert("private_key_file".into(), key.clone());
            if cli.private_key_passphrase_file.is_none() {
                args.args.remove("private_key_passphrase_file");
            }
        }
        if let Some(key) = &cli.private_key_passphrase_file {
            args.args
                .insert("private_key_passphrase_file".into(), key.clone());
        }
    }
    args.validate_cloud_options(cli.ui)?;
    Ok(ResolvedConnection {
        target: org.map_or(Target::Native, |org| Target::Cloud { org }),
        args,
        warnings,
    })
}
