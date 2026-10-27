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

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;

use anyhow::{bail, Result};
use serde::{de::IntoDeserializer, Deserialize, Deserializer};

pub const ENV_BACKEND: &str = "env";
pub const BUILTIN_BACKENDS: [&str; 4] = ["local-claude", "local-codex", "local-evot", "local-pi"];

fn default_timeout() -> u64 {
    60
}

fn default_max_tokens() -> u32 {
    2048
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSettings {
    pub backend: Option<String>,
    /// Explicit permission to send questions and subsequently captured data.
    #[serde(default)]
    pub allow_data_sharing: bool,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    #[serde(default)]
    pub backends: BTreeMap<String, BackendConfig>,
}

impl AgentSettings {
    pub fn backend(&self, name: &str) -> Option<Cow<'_, BackendConfig>> {
        // Exact custom names win; aliases also honor canonical-name overrides.
        let custom = self
            .backends
            .get(name)
            .or_else(|| canonical_builtin(name).and_then(|canonical| self.backends.get(canonical)));
        custom
            .map(Cow::Borrowed)
            .or_else(|| builtin_backend(name).map(Cow::Owned))
    }
}

impl Default for AgentSettings {
    fn default() -> Self {
        Self {
            backend: None,
            allow_data_sharing: false,
            timeout_secs: default_timeout(),
            backends: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CliAdapter {
    Codex,
    ClaudeCode,
    Pi,
    /// evot speaks ACP (`evot acp`) rather than a CLI-specific wire format.
    Evot,
}

#[derive(Clone, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum BackendConfig {
    #[serde(rename = "openai-compatible")]
    OpenAiCompatible {
        base_url: String,
        model: String,
        api_key_env: Option<String>,
        #[serde(default)]
        headers: BTreeMap<String, String>,
        #[serde(default)]
        header_envs: BTreeMap<String, String>,
        #[serde(default = "default_max_tokens")]
        max_tokens: u32,
    },
    #[serde(rename = "cli")]
    Cli {
        adapter: CliAdapter,
        command: Option<String>,
        model: Option<String>,
        #[serde(default)]
        env_allowlist: Vec<String>,
        #[serde(default)]
        allow_external_agent: bool,
    },
    #[serde(rename = "acp")]
    Acp {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env_allowlist: Vec<String>,
        #[serde(default)]
        allow_external_agent: bool,
    },
}

fn canonical_builtin(name: &str) -> Option<&'static str> {
    match name {
        "local-claude" | "claude" | "claude-code" => Some("local-claude"),
        "local-codex" | "codex" => Some("local-codex"),
        "local-pi" | "pi" => Some("local-pi"),
        "local-evot" | "evot" => Some("local-evot"),
        _ => None,
    }
}

/// Store built-in overrides written under an alias (`[agent.backends.codex]`)
/// under the canonical name, so they apply however the backend is selected,
/// including discovery. Alias sections with a non-CLI type or another adapter
/// stay ordinary named backends. Two sections for one built-in are rejected
/// rather than merged or picked silently; the error is the config path.
fn canonicalize_builtin_overrides(backends: &mut toml::Table) -> std::result::Result<(), String> {
    let aliases: Vec<(String, &'static str)> = backends
        .iter()
        .filter_map(|(name, value)| {
            let canonical = canonical_builtin(name).filter(|canonical| canonical != name)?;
            let Some(BackendConfig::Cli { adapter, .. }) = builtin_backend(name) else {
                return None;
            };
            let table = value.as_table()?;
            let is_override = table.get("type").and_then(toml::Value::as_str) == Some("cli")
                && table.get("adapter").and_then(toml::Value::as_str)
                    == Some(adapter_name(adapter));
            is_override.then(|| (name.clone(), canonical))
        })
        .collect();
    for (alias, canonical) in aliases {
        if backends.contains_key(canonical) {
            return Err(format!(
                "agent.backends.{canonical} (also configured as agent.backends.{alias}; keep one)"
            ));
        }
        if let Some(value) = backends.remove(&alias) {
            backends.insert(canonical.to_string(), value);
        }
    }
    Ok(())
}

fn adapter_name(adapter: CliAdapter) -> &'static str {
    match adapter {
        CliAdapter::Codex => "codex",
        CliAdapter::ClaudeCode => "claude-code",
        CliAdapter::Pi => "pi",
        CliAdapter::Evot => "evot",
    }
}

fn builtin_backend(name: &str) -> Option<BackendConfig> {
    let adapter = match canonical_builtin(name)? {
        "local-claude" => CliAdapter::ClaudeCode,
        "local-codex" => CliAdapter::Codex,
        "local-pi" => CliAdapter::Pi,
        "local-evot" => CliAdapter::Evot,
        _ => unreachable!("canonical names are fixed"),
    };
    Some(BackendConfig::Cli {
        adapter,
        command: None,
        model: None,
        env_allowlist: Vec::new(),
        // This enables the built-in execution profile, not data-sharing consent.
        // AgentSession gates every request on explicit config/session permission.
        allow_external_agent: true,
    })
}

/// Invalid agent configuration must not reset database configuration or silently
/// fall back to an environment backend. Keep the error local to AI operations.
#[derive(Clone, Default)]
pub struct AgentConfig {
    settings: AgentSettings,
    invalid: bool,
    invalid_path: Option<String>,
}

impl AgentConfig {
    pub fn invalid() -> Self {
        Self {
            invalid: true,
            ..Self::default()
        }
    }

    pub fn settings(&self) -> Result<&AgentSettings> {
        if self.invalid {
            bail!("AI configuration is invalid at {}; check config.toml. SQL remains available. No fallback backend will be used.", self.invalid_path.as_deref().unwrap_or("agent"));
        }
        if !(1..=3600).contains(&self.settings.timeout_secs) {
            bail!("agent.timeout_secs must be between 1 and 3600");
        }
        if self.settings.backends.keys().any(|name| {
            name == ENV_BACKEND
                || name.is_empty()
                || name.len() > 64
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        }) {
            bail!("Backend names must be 1-64 ASCII letters, digits, '-', '_' or '.'; 'env' is reserved");
        }
        Ok(&self.settings)
    }

    pub fn selected(&self) -> Option<String> {
        // Absence means local CLI discovery, never implicit HTTP/env selection.
        self.settings.backend.clone()
    }
}

// Never expose URLs, headers, model configuration or secret references through
// the enclosing Config's derived Debug implementation.
impl fmt::Debug for AgentConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentConfig")
            .field("invalid", &self.invalid)
            .field("backend_count", &self.settings.backends.len())
            .finish()
    }
}

impl<'de> Deserialize<'de> for AgentConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let mut raw = toml::Value::deserialize(deserializer)?;
        if let Some(backends) = raw.get_mut("backends").and_then(toml::Value::as_table_mut) {
            for (name, value) in backends.iter_mut() {
                let Some(BackendConfig::Cli { adapter, .. }) = builtin_backend(name) else {
                    continue;
                };
                let Some(table) = value.as_table_mut() else {
                    continue;
                };
                // Built-in overrides need only the fields being customized.
                // Explicit non-CLI types retain ordinary named-backend behavior.
                if table
                    .get("type")
                    .is_none_or(|ty| ty.as_str() == Some("cli"))
                {
                    table.entry("type").or_insert_with(|| "cli".into());
                    table
                        .entry("adapter")
                        .or_insert_with(|| adapter_name(adapter).into());
                    table
                        .entry("allow_external_agent")
                        .or_insert_with(|| true.into());
                }
            }
            if let Err(duplicate) = canonicalize_builtin_overrides(backends) {
                return Ok(Self {
                    invalid: true,
                    invalid_path: Some(duplicate),
                    ..Self::default()
                });
            }
        }
        Ok(
            match serde_path_to_error::deserialize::<_, AgentSettings>(raw.into_deserializer()) {
                Ok(settings) => Self {
                    settings,
                    invalid: false,
                    invalid_path: None,
                },
                Err(error) => {
                    // Path only: serde/toml diagnostic messages can quote secret values.
                    let mut path = String::from("agent");
                    let mut dynamic = false;
                    for segment in error.path().iter() {
                        let key = match segment {
                            serde_path_to_error::Segment::Map { key } => key.as_str(),
                            _ => "*",
                        };
                        let known = matches!(
                            key,
                            "backend"
                                | "backends"
                                | "allow_data_sharing"
                                | "timeout_secs"
                                | "type"
                                | "base_url"
                                | "model"
                                | "api_key_env"
                                | "headers"
                                | "header_envs"
                                | "max_tokens"
                                | "adapter"
                                | "command"
                                | "args"
                                | "env_allowlist"
                                | "allow_external_agent"
                        );
                        path.push('.');
                        path.push_str(if known && !dynamic { key } else { "*" });
                        dynamic = matches!(key, "backends" | "headers" | "header_envs");
                    }
                    Self {
                        invalid: true,
                        invalid_path: Some(path),
                        ..Self::default()
                    }
                }
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn sharing_is_opt_in_and_configuration_errors_have_redacted_key_paths() {
        assert!(
            !AgentConfig::default()
                .settings()
                .unwrap()
                .allow_data_sharing
        );
        let config: Config =
            toml::from_str("[agent]\nallow_data_sharing='private-secret'").unwrap();
        let error = config.agent.settings().err().unwrap().to_string();
        assert!(error.contains("agent.allow_data_sharing"));
        assert!(!error.contains("private-secret"));
        let config: Config = toml::from_str("[agent.backends.private-name]\ntype='openai-compatible'\nbase_url='http://localhost/v1'\nmodel=123").unwrap();
        let error = config.agent.settings().err().unwrap().to_string();
        assert!(error.contains("agent.backends.*"));
        assert!(!error.contains("private-name"));
    }

    #[test]
    fn builtin_clis_need_no_configuration_and_selection_defaults_to_discovery() {
        let config = AgentConfig::default();
        let settings = config.settings().unwrap();
        assert!(settings.backends.is_empty());
        assert_eq!(config.selected(), None);
        for (name, expected) in [
            ("local-codex", CliAdapter::Codex),
            ("codex", CliAdapter::Codex),
            ("local-claude", CliAdapter::ClaudeCode),
            ("claude", CliAdapter::ClaudeCode),
            ("claude-code", CliAdapter::ClaudeCode),
            ("local-pi", CliAdapter::Pi),
            ("pi", CliAdapter::Pi),
            ("local-evot", CliAdapter::Evot),
            ("evot", CliAdapter::Evot),
        ] {
            let backend = settings.backend(name).unwrap();
            match backend.as_ref() {
                BackendConfig::Cli {
                    adapter,
                    command,
                    model,
                    env_allowlist,
                    allow_external_agent,
                } => {
                    assert!(*adapter == expected);
                    assert!(command.is_none() && model.is_none() && env_allowlist.is_empty());
                    assert!(*allow_external_agent);
                }
                _ => panic!("expected CLI"),
            }
        }
        assert!(settings.backend("unsupported").is_none());
        assert!(settings.backend("amp").is_none());
    }

    #[test]
    fn builtin_overrides_inherit_defaults_and_aliases_use_them() {
        let config: Config = toml::from_str(
            r#"
[agent]
backend = "local-codex"
[agent.backends.local-codex]
command = "/custom/codex"
model = "my-model"
[agent.backends.local-claude]
allow_external_agent = false
"#,
        )
        .unwrap();
        let settings = config.agent.settings().unwrap();
        match settings.backend("codex").unwrap().as_ref() {
            BackendConfig::Cli {
                adapter,
                command,
                model,
                allow_external_agent,
                ..
            } => {
                assert!(*adapter == CliAdapter::Codex);
                assert_eq!(command.as_deref(), Some("/custom/codex"));
                assert_eq!(model.as_deref(), Some("my-model"));
                assert!(*allow_external_agent);
            }
            _ => panic!("expected CLI"),
        }
        assert!(matches!(
            settings.backend("claude").unwrap().as_ref(),
            BackendConfig::Cli {
                allow_external_agent: false,
                ..
            }
        ));
    }

    #[test]
    fn malformed_builtin_override_does_not_fall_back_to_defaults() {
        for options in [
            "model=123",
            "unknown='secret'",
            "env_allowlist='bad'",
            "type='unsupported'",
        ] {
            let config: Config =
                toml::from_str(&format!("[agent.backends.local-codex]\n{options}")).unwrap();
            assert!(config.agent.settings().is_err());
        }
    }

    #[test]
    fn alias_named_overrides_apply_under_every_name() {
        for (section, names) in [
            ("codex", ["codex", "local-codex"]),
            ("claude", ["claude", "local-claude"]),
            ("claude-code", ["claude-code", "local-claude"]),
            ("pi", ["pi", "local-pi"]),
            ("evot", ["evot", "local-evot"]),
        ] {
            let config: Config = toml::from_str(&format!(
                "[agent.backends.{section}]\ncommand = \"/custom/{section}\"\nmodel = \"m\""
            ))
            .unwrap();
            let settings = config.agent.settings().unwrap();
            // Discovery and status use canonical names, so the override lives there.
            assert!(!settings.backends.contains_key(section), "{section}");
            for name in names {
                match settings.backend(name).unwrap().as_ref() {
                    BackendConfig::Cli { command, model, .. } => {
                        assert_eq!(
                            command.as_deref(),
                            Some(format!("/custom/{section}").as_str())
                        );
                        assert_eq!(model.as_deref(), Some("m"));
                    }
                    _ => panic!("expected CLI for {name}"),
                }
            }
        }

        // Disabling through the alias also disables the canonical name.
        let config: Config =
            toml::from_str("[agent.backends.evot]\nallow_external_agent = false").unwrap();
        let settings = config.agent.settings().unwrap();
        assert!(matches!(
            settings.backend("local-evot").unwrap().as_ref(),
            BackendConfig::Cli {
                allow_external_agent: false,
                ..
            }
        ));
    }

    #[test]
    fn one_builtin_configured_under_two_names_is_rejected() {
        for sections in [
            "[agent.backends.codex]\nmodel='a'\n[agent.backends.local-codex]\nmodel='b'",
            "[agent.backends.claude]\nmodel='a'\n[agent.backends.claude-code]\nmodel='b'",
        ] {
            let config: Config = toml::from_str(sections).unwrap();
            let error = config.agent.settings().err().unwrap().to_string();
            assert!(error.contains("keep one"), "{error}");
            assert!(!error.contains("model"), "{error}");
        }
    }

    #[test]
    fn alias_named_custom_backend_with_other_adapter_stays_custom() {
        let config: Config = toml::from_str(
            "[agent.backends.codex]\ntype = \"cli\"\nadapter = \"pi\"\ncommand = \"/custom/pi\"",
        )
        .unwrap();
        let settings = config.agent.settings().unwrap();
        assert!(matches!(
            settings.backend("codex").unwrap().as_ref(),
            BackendConfig::Cli {
                adapter: CliAdapter::Pi,
                ..
            }
        ));
        assert!(matches!(
            settings.backend("local-codex").unwrap().as_ref(),
            BackendConfig::Cli {
                adapter: CliAdapter::Codex,
                command: None,
                ..
            }
        ));
    }

    #[test]
    fn exact_custom_name_takes_precedence_over_builtin_alias() {
        let config: Config = toml::from_str(
            r#"
[agent.backends.codex]
type = "openai-compatible"
base_url = "http://localhost/v1"
model = "api-model"
"#,
        )
        .unwrap();
        let settings = config.agent.settings().unwrap();
        assert!(matches!(
            settings.backend("codex").unwrap().as_ref(),
            BackendConfig::OpenAiCompatible { .. }
        ));
        assert!(matches!(
            settings.backend("local-codex").unwrap().as_ref(),
            BackendConfig::Cli { .. }
        ));
    }

    #[test]
    fn parses_named_backends_and_keeps_debug_private() {
        let config: Config = toml::from_str(
            r#"
[agent]
backend = "private-api"
timeout_secs = 30
[agent.backends.private-api]
type = "openai-compatible"
base_url = "https://private.example/v1"
model = "private-model"
api_key_env = "PRIVATE_KEY"
headers = { "x-project" = "secret-value" }
header_envs = { "x-api-key" = "CUSTOM_KEY" }
max_tokens = 1024
"#,
        )
        .unwrap();
        assert_eq!(config.agent.selected().as_deref(), Some("private-api"));
        assert_eq!(config.agent.settings().unwrap().timeout_secs, 30);
        let debug = format!("{config:?}");
        for secret in [
            "private.example",
            "private-model",
            "PRIVATE_KEY",
            "secret-value",
        ] {
            assert!(!debug.contains(secret));
        }
    }

    #[test]
    fn invalid_agent_config_does_not_reset_database_or_enable_fallback() {
        for agent in [
            "backend = 123",
            "timeout_secs = 'bad'",
            "unknown = true",
            "[agent.backends.broken]\ntype = 'cli'\ncommand = 'claude'",
            "[agent.backends.broken]\ntype = 'openai-compatible'\nbase_url = 'http://localhost'",
        ] {
            let text = format!("[connection]\nhost = 'database.example'\n[agent]\n{agent}");
            let config: Config = toml::from_str(&text).unwrap();
            assert_eq!(config.connection.host, "database.example");
            assert!(
                config.agent.settings().is_err(),
                "unexpectedly accepted: {agent}"
            );
        }
    }

    #[test]
    fn no_implicit_selection_when_named_backends_exist() {
        let config: Config = toml::from_str(
            r#"
[agent.backends.api]
type = "openai-compatible"
base_url = "http://localhost/v1"
model = "model"
"#,
        )
        .unwrap();
        assert_eq!(config.agent.selected(), None);
        assert_eq!(AgentConfig::default().selected(), None);
    }

    #[test]
    fn acp_config_is_explicit_and_credentials_stay_out_of_debug() {
        let config: Config = toml::from_str(
            r#"
[agent]
backend = "adapter"
[agent.backends.adapter]
type = "acp"
command = "/private/adapter"
args = ["--stdio", "private-arg"]
env_allowlist = ["PRIVATE_AUTH"]
allow_external_agent = true
"#,
        )
        .unwrap();
        match config
            .agent
            .settings()
            .unwrap()
            .backend("adapter")
            .unwrap()
            .as_ref()
        {
            BackendConfig::Acp {
                command,
                args,
                env_allowlist,
                allow_external_agent,
            } => {
                assert_eq!(command, "/private/adapter");
                assert_eq!(args, &["--stdio", "private-arg"]);
                assert_eq!(env_allowlist, &["PRIVATE_AUTH"]);
                assert!(*allow_external_agent);
            }
            _ => panic!("expected ACP"),
        }
        let debug = format!("{config:?}");
        assert!(!debug.contains("private"));
        assert!(!debug.contains("PRIVATE_AUTH"));
        for body in [
            "args=['--stdio']",
            "command='adapter'\nargs='bad'",
            "command='adapter'\nmodel='not-supported'",
            "command='adapter'\nallow_external_agent='true'",
        ] {
            let config: Config =
                toml::from_str(&format!("[agent.backends.bad]\ntype='acp'\n{body}")).unwrap();
            assert!(config.agent.settings().is_err());
        }
    }

    #[test]
    fn parses_cli_adapters_and_rejects_unsafe_or_unsupported_options() {
        for adapter in ["codex", "claude-code", "pi", "evot"] {
            let text = format!("[agent.backends.cli]\ntype='cli'\nadapter='{adapter}'\nallow_external_agent=true\nenv_allowlist=['CUSTOM_AUTH']");
            let config: Config = toml::from_str(&text).unwrap();
            assert!(config.agent.settings().is_ok());
        }
        for options in [
            "adapter='unsupported'",
            "adapter='amp'",
            "adapter='codex'\nargs=['--dangerously-bypass-approvals-and-sandbox']",
            "adapter='codex'\nallow_external_agent='true'",
        ] {
            let config: Config =
                toml::from_str(&format!("[agent.backends.cli]\ntype='cli'\n{options}")).unwrap();
            assert!(config.agent.settings().is_err());
        }
    }

    #[test]
    fn validates_timeout_and_backend_names() {
        for text in [
            "[agent]\ntimeout_secs = 0", "[agent]\ntimeout_secs = 3601",
            "[agent.backends.env]\ntype='openai-compatible'\nbase_url='http://localhost'\nmodel='m'",
            "[agent.backends.'bad name']\ntype='openai-compatible'\nbase_url='http://localhost'\nmodel='m'",
        ] {
            let config: Config = toml::from_str(text).unwrap();
            assert!(config.agent.settings().is_err());
        }
    }
}
