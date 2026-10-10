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
use serde::{Deserialize, Deserializer};

pub const ENV_BACKEND: &str = "env";
pub const BUILTIN_BACKENDS: [&str; 4] = ["local-claude", "local-codex", "local-pi", "local-amp"];

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
    Amp,
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
        "local-amp" | "amp" => Some("local-amp"),
        _ => None,
    }
}

fn builtin_backend(name: &str) -> Option<BackendConfig> {
    let adapter = match canonical_builtin(name)? {
        "local-claude" => CliAdapter::ClaudeCode,
        "local-codex" => CliAdapter::Codex,
        "local-pi" => CliAdapter::Pi,
        "local-amp" => CliAdapter::Amp,
        _ => unreachable!("canonical names are fixed"),
    };
    Some(BackendConfig::Cli {
        adapter,
        command: None,
        model: None,
        env_allowlist: Vec::new(),
        // --agent opts into local CLI discovery when no backend is selected.
        // Discovery checks executables only; a question starts the process.
        allow_external_agent: true,
    })
}

/// Invalid agent configuration must not reset database configuration or silently
/// fall back to an environment backend. Keep the error local to AI operations.
#[derive(Clone, Default)]
pub struct AgentConfig {
    settings: AgentSettings,
    invalid: bool,
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
            bail!("AI configuration is invalid; check [agent] in config.toml. SQL remains available. No fallback backend will be used.");
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
            for (name, value) in backends {
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
                    table.entry("adapter").or_insert_with(|| match adapter {
                        CliAdapter::Codex => "codex".into(),
                        CliAdapter::ClaudeCode => "claude-code".into(),
                        CliAdapter::Pi => "pi".into(),
                        CliAdapter::Amp => "amp".into(),
                    });
                    table
                        .entry("allow_external_agent")
                        .or_insert_with(|| true.into());
                }
            }
        }
        Ok(match raw.try_into::<AgentSettings>() {
            Ok(settings) => Self {
                settings,
                invalid: false,
            },
            Err(_) => Self::invalid(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

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
            ("local-amp", CliAdapter::Amp),
            ("amp", CliAdapter::Amp),
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
        for adapter in ["codex", "claude-code", "pi", "amp"] {
            let text = format!("[agent.backends.cli]\ntype='cli'\nadapter='{adapter}'\nallow_external_agent=true\nenv_allowlist=['CUSTOM_AUTH']");
            let config: Config = toml::from_str(&text).unwrap();
            assert!(config.agent.settings().is_ok());
        }
        for options in [
            "adapter='unsupported'",
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
