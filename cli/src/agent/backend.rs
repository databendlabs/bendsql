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

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use super::acp::AcpBackend;
use super::cli::CliBackend;
use super::config::{AgentConfig, BackendConfig, ENV_BACKEND};
use super::llm::{LlmClient, Message};

/// Backends transport messages only. Query evidence and bounded conversation
/// remain owned by BendSQL; no backend receives a BendSQL execution tool.
/// External CLI backends may have their own tools, governed by explicit opt-in.
#[async_trait]
pub trait ChatBackend: Send + Sync {
    async fn complete(&self, messages: &[Message]) -> Result<String>;
}

pub fn build(config: &AgentConfig, name: &str) -> Result<Box<dyn ChatBackend>> {
    let settings = config.settings()?;
    if name == ENV_BACKEND {
        return Ok(Box::new(LlmClient::from_env(settings.timeout_secs)?));
    }
    if name.is_empty() {
        bail!("Select an AI backend using agent.backend, --backend, or /backend <name>. SQL remains available.");
    }
    let backend = settings.backend(name).ok_or_else(|| {
        anyhow!("Unknown AI backend; use /backend to list built-in and configured names")
    })?;
    match backend.as_ref() {
        BackendConfig::OpenAiCompatible {
            base_url,
            model,
            api_key_env,
            headers,
            header_envs,
            max_tokens,
        } => {
            let key = api_key_env.as_deref().map(read_secret).transpose()?;
            let mut map = HeaderMap::new();
            for (name, value) in headers {
                insert_header(&mut map, name, value)?;
            }
            for (name, variable) in header_envs {
                insert_header(&mut map, name, &read_secret(variable)?)?;
            }
            if key.is_some() && map.contains_key(reqwest::header::AUTHORIZATION) {
                bail!("Configure either api_key_env or an Authorization header, not both");
            }
            Ok(Box::new(LlmClient::configured(
                base_url,
                model.clone(),
                key,
                settings.timeout_secs,
                *max_tokens,
                map,
            )?))
        }
        BackendConfig::Cli {
            adapter,
            command,
            model,
            env_allowlist,
            allow_external_agent,
        } => Ok(Box::new(CliBackend::new(
            *adapter,
            command.as_deref(),
            model.as_deref(),
            env_allowlist,
            *allow_external_agent,
        )?)),
        BackendConfig::Acp {
            command,
            args,
            env_allowlist,
            allow_external_agent,
        } => Ok(Box::new(AcpBackend::new(
            command,
            args,
            env_allowlist,
            *allow_external_agent,
        )?)),
    }
}

fn read_secret(variable: &str) -> Result<String> {
    if variable.is_empty()
        || !variable
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        bail!(
            "Credential environment variable names must contain only ASCII letters, digits or '_'"
        );
    }
    std::env::var(variable).ok().filter(|v| !v.trim().is_empty())
        .ok_or_else(|| anyhow!("A credential environment variable is missing or empty; check the selected backend configuration"))
}

fn insert_header(map: &mut HeaderMap, name: &str, value: &str) -> Result<()> {
    let name = HeaderName::from_bytes(name.as_bytes())
        .map_err(|_| anyhow!("Invalid custom HTTP header name"))?;
    if matches!(
        name.as_str(),
        "host" | "content-length" | "transfer-encoding" | "connection" | "content-type"
    ) {
        bail!("Custom headers cannot override HTTP transport or JSON content headers");
    }
    if map.contains_key(&name) {
        bail!("Duplicate custom HTTP header; check headers and header_envs (names are case-insensitive)");
    }
    let mut value =
        HeaderValue::from_str(value).map_err(|_| anyhow!("Invalid custom HTTP header value"))?;
    value.set_sensitive(true);
    map.insert(name, value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn validates_headers_without_exposing_values() {
        let mut map = HeaderMap::new();
        insert_header(&mut map, "X-API-Key", "secret").unwrap();
        assert!(map["x-api-key"].is_sensitive());
        assert!(insert_header(&mut map, "x-api-key", "other").is_err());
        for (name, value) in [
            ("Host", "secret"),
            ("Content-Type", "secret"),
            ("bad header", "secret"),
            ("x-test", "secret\r\n"),
        ] {
            let error = insert_header(&mut HeaderMap::new(), name, value).unwrap_err();
            assert!(!error.to_string().contains("secret"));
        }
    }

    #[test]
    fn unknown_and_invalid_backends_never_fall_back() {
        let config = AgentConfig::default();
        assert!(build(&config, "missing").is_err());
        assert!(build(&AgentConfig::invalid(), ENV_BACKEND).is_err());
        let config: Config = toml::from_str(
            r#"
[agent]
backend = "broken"
[agent.backends.broken]
type = "openai-compatible"
base_url = "https://user:secret@example.com/v1"
model = "m"
"#,
        )
        .unwrap();
        let error = build(&config.agent, "broken").err().unwrap();
        assert!(!error.to_string().contains("secret"));
        assert!(read_secret("BENDSQL_TEST_MISSING_SECRET_9B742D").is_err());
    }
}
