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

mod acp;
pub mod backend;
mod cli;
pub mod config;
pub mod llm;
pub mod memory;
pub mod router;

use anyhow::{anyhow, bail, Result};
use backend::ChatBackend;
use clap::ValueEnum;
use config::{AgentConfig, ENV_BACKEND};
use llm::Conversation;
use memory::{Memory, MAX_TEXT_BYTES};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum InteractionMode {
    #[default]
    Smart,
    Sql,
    Agent,
}

impl InteractionMode {
    pub fn name(self) -> &'static str {
        match self {
            Self::Smart => "smart",
            Self::Sql => "sql",
            Self::Agent => "agent",
        }
    }

    pub fn prompt(self, pending_sql: bool) -> String {
        if pending_sql {
            format!("{}(sql)> ", self.name())
        } else {
            format!("{}> ", self.name())
        }
    }

    pub fn route(self, line: &str, delimiter: char) -> router::Input<'_> {
        let line = line.trim();
        if line.is_empty() {
            return router::Input::Empty;
        }
        if (line.starts_with('/') && !line.starts_with("/*"))
            || line.starts_with('!')
            || matches!(line, "exit" | "quit")
        {
            return router::Input::Command(line);
        }
        match self {
            Self::Smart => router::classify(line, delimiter),
            Self::Sql => router::Input::Sql(line),
            Self::Agent => router::Input::Question(line),
        }
    }
}

pub struct AgentSession {
    pub mode: InteractionMode,
    pub memory: Memory,
    conversation: Conversation,
    config: AgentConfig,
    selected: Option<String>,
    backend: Option<Box<dyn ChatBackend>>,
    notice_shown: bool,
}

impl Default for AgentSession {
    fn default() -> Self {
        Self::new(AgentConfig::default(), None)
    }
}

impl AgentSession {
    pub fn new(config: AgentConfig, selected: Option<String>) -> Self {
        let selected = selected
            .or_else(|| config.selected())
            .or_else(|| backend::detect_local(&config).ok());
        Self {
            mode: InteractionMode::default(),
            memory: Memory::default(),
            conversation: Conversation::default(),
            config,
            selected,
            backend: None,
            notice_shown: false,
        }
    }

    pub fn clear(&mut self) {
        self.memory.clear();
        self.conversation.clear();
        self.backend = None;
    }

    pub fn backend_summary(&self) -> Result<String> {
        let settings = self.config.settings()?;
        let detected = self
            .selected
            .clone()
            .or_else(|| backend::detect_local(&self.config).ok());
        let name = detected.as_deref().unwrap_or("");
        let backend = settings.backend(name);
        let current = if name == ENV_BACKEND || backend.is_some() {
            name
        } else {
            "(no local CLI available; select a backend)"
        };
        let names = settings
            .backends
            .keys()
            .map(String::as_str)
            .chain(config::BUILTIN_BACKENDS)
            .chain(std::iter::once(ENV_BACKEND))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(", ");
        let notice = if matches!(
            backend.as_deref(),
            Some(config::BackendConfig::Cli { .. } | config::BackendConfig::Acp { .. })
        ) {
            "\nExternal agent backend: a trusted program; local file access and its own data retention may apply. Capability denial and a temporary directory are not security sandboxes."
        } else {
            ""
        };
        let retention = if matches!(
            backend.as_deref(),
            Some(config::BackendConfig::Cli {
                adapter: config::CliAdapter::Amp,
                ..
            })
        ) {
            "\nAmp may retain threads locally/remotely; no verified ephemeral option is used. /clear removes BendSQL context, not external history."
        } else {
            ""
        };
        Ok(format!("Current backend: {current}\nAvailable backends: {names}\nDefault discovery: codex, then claude, then pi. Explicit --backend/agent.backend selection takes precedence; runtime failures never switch services.\nBuilt-in aliases: claude, claude-code, codex, pi, amp. env explicitly uses BENDSQL_AGENT_BASE_URL, BENDSQL_AGENT_MODEL and optional BENDSQL_AGENT_API_KEY.{notice}{retention}"))
    }

    pub fn select_backend(&mut self, name: &str) -> Result<()> {
        // Validate before discarding the active backend or its conversation.
        // Building a backend must not contact a service or start a process.
        let backend = backend::build(&self.config, name)?;
        self.backend = Some(backend);
        self.selected = Some(name.into());
        self.notice_shown = false;
        self.conversation.clear();
        Ok(())
    }

    pub async fn ask(&mut self, question: &str) -> Result<String> {
        if question.trim().is_empty() {
            bail!("Question must not be empty");
        }
        if question.len() > MAX_TEXT_BYTES {
            bail!("Question exceeds the 8 KiB limit; please shorten it");
        }
        // Discovery inspects executables only. Fix the selected backend before
        // building/sending a request; never try another service on failure.
        self.config.settings()?;
        if self.selected.is_none() {
            self.selected = Some(backend::detect_local(&self.config)?);
        }
        let selected = self.selected.as_deref().unwrap();
        if self.backend.is_none() {
            self.backend = Some(backend::build(&self.config, selected)?);
        }
        if !self.notice_shown {
            match self.config.settings()?.backend(selected).as_deref() {
                Some(config::BackendConfig::Cli {
                    adapter: config::CliAdapter::Amp,
                    ..
                }) => {
                    eprintln!("AI backend: {selected}. External agent receives query context and may retain threads; /clear does not erase external history.");
                }
                Some(config::BackendConfig::Cli { .. } | config::BackendConfig::Acp { .. }) => {
                    eprintln!("AI backend: {selected}. Query context is shared with this trusted external agent; file access/retention policies apply.");
                }
                _ => eprintln!(
                    "AI backend: {selected}. Selected query context is sent to the model service."
                ),
            }
            self.notice_shown = true;
        }
        let messages = self.conversation.messages(question, &self.memory);
        let timeout = std::time::Duration::from_secs(self.config.settings()?.timeout_secs);
        let answer =
            tokio::time::timeout(timeout, self.backend.as_ref().unwrap().complete(&messages))
                .await
                .map_err(|_| anyhow!("AI backend request timed out. SQL remains available."))??;
        // Failures/cancellation do not enter conversation history.
        self.conversation.remember(question.into(), answer.clone());
        Ok(answer)
    }
}

pub const HELP: &str = "Interactive modes (default: smart):\n\
smart>  Whole-batch AST validation; mixed SQL/prose executes nothing\n\
sql>    Input is SQL only; finish with the configured delimiter\n\
agent>  Input goes to AI, with cached query context; no SQL is executed\n\
/mode [smart|sql|agent]  Show or change the interaction mode\n\
/backend [name]   List backends or switch (clears conversation, retains query memory)\n\
/sql <SQL>       Execute explicitly (no trailing delimiter required)\n\
/ask <question>  Ask AI explicitly; suggested SQL is never executed\n\
/context         List cached queries and preview completeness\n\
/clear           Clear query memory and conversation\n\
/help            Show this help\n\
exit / quit      Exit; Ctrl+C clears pending SQL or interrupts work\n\
Questions send selected SQL, result previews, errors and conversation to the configured model backend.\n\
BendSQL history is not saved to disk; external CLIs/services have their own access and retention policies.\n\
Use /clear before discussing unrelated or sensitive data.";

#[cfg(test)]
mod tests {
    use super::*;
    use router::Input;

    struct FakeBackend {
        fail: bool,
        requests: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    }

    #[async_trait::async_trait]
    impl ChatBackend for FakeBackend {
        async fn complete(&self, messages: &[llm::Message]) -> Result<String> {
            self.requests
                .lock()
                .unwrap()
                .push(serde_json::to_value(messages).unwrap());
            if self.fail {
                bail!("test backend failure");
            }
            Ok("answer".into())
        }
    }

    fn configured_session() -> AgentSession {
        let config: crate::config::Config = toml::from_str(
            r#"
[agent]
backend = "first"
timeout_secs = 1
[agent.backends.first]
type = "openai-compatible"
base_url = "http://localhost:1/v1"
model = "first-model"
[agent.backends.second]
type = "openai-compatible"
base_url = "http://localhost:1/v1"
model = "second-model"
"#,
        )
        .unwrap();
        AgentSession::new(config.agent, None)
    }

    #[tokio::test]
    async fn switching_preserves_query_evidence_but_resets_conversation() {
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut session = configured_session();
        session.memory.push(memory::QueryRecord::new("SELECT 1"));
        session.backend = Some(Box::new(FakeBackend {
            fail: false,
            requests: requests.clone(),
        }));
        session.ask("Q1?").await.unwrap();
        session.ask("follow up").await.unwrap();
        assert_eq!(requests.lock().unwrap()[1].as_array().unwrap().len(), 4);
        assert!(session.select_backend("missing").is_err());
        assert_eq!(session.selected.as_deref(), Some("first"));
        session.ask("still here").await.unwrap();
        assert_eq!(requests.lock().unwrap()[2].as_array().unwrap().len(), 6);
        session.select_backend("second").unwrap();
        assert_eq!(session.selected.as_deref(), Some("second"));
        assert!(!session.memory.summary().is_empty());
        assert_eq!(
            session.conversation.messages("next", &session.memory).len(),
            2
        );
        session.clear();
        assert!(session.backend.is_none());
        assert!(session.memory.summary().is_empty());
        assert_eq!(session.selected.as_deref(), Some("second"));
    }

    #[tokio::test]
    async fn failures_and_oversized_questions_do_not_change_conversation() {
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut session = configured_session();
        session.backend = Some(Box::new(FakeBackend {
            fail: true,
            requests: requests.clone(),
        }));
        assert!(session.ask("question").await.is_err());
        assert_eq!(
            session.conversation.messages("next", &session.memory).len(),
            2
        );
        assert!(session.ask(&"x".repeat(MAX_TEXT_BYTES + 1)).await.is_err());
        assert!(session.ask("   ").await.is_err());
        assert_eq!(requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn cancellation_and_timeout_do_not_record_a_turn() {
        struct PendingBackend;
        #[async_trait::async_trait]
        impl ChatBackend for PendingBackend {
            async fn complete(&self, _: &[llm::Message]) -> Result<String> {
                std::future::pending().await
            }
        }
        let mut session = configured_session();
        session.backend = Some(Box::new(PendingBackend));
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(10),
            session.ask("question")
        )
        .await
        .is_err());
        assert_eq!(
            session.conversation.messages("next", &session.memory).len(),
            2
        );
        let error = session.ask("question").await.unwrap_err();
        assert!(error.to_string().contains("timed out"));
        assert_eq!(
            session.conversation.messages("next", &session.memory).len(),
            2
        );
    }

    #[test]
    fn explicit_selection_beats_config_and_never_falls_back_to_discovery() {
        let config: crate::config::Config =
            toml::from_str("[agent]\nbackend='configured'").unwrap();
        let session = AgentSession::new(config.agent.clone(), Some("requested".into()));
        assert_eq!(session.selected.as_deref(), Some("requested"));
        let session = AgentSession::new(config.agent, None);
        assert_eq!(session.selected.as_deref(), Some("configured"));
        let session = AgentSession::new(AgentConfig::default(), Some(String::new()));
        assert_eq!(session.selected.as_deref(), Some(""));
    }

    #[test]
    fn modes_control_routing_and_prompts() {
        let sql = "SELECT 1;";
        let question = "What does the last result mean?";
        assert_eq!(InteractionMode::default(), InteractionMode::Smart);
        assert_eq!(InteractionMode::Smart.route(sql, ';'), Input::Sql(sql));
        assert_eq!(
            InteractionMode::Smart.route(question, ';'),
            Input::Question(question)
        );
        assert_eq!(
            InteractionMode::Sql.route(question, ';'),
            Input::Sql(question)
        );
        assert_eq!(InteractionMode::Agent.route(sql, ';'), Input::Question(sql));
        for mode in [
            InteractionMode::Smart,
            InteractionMode::Sql,
            InteractionMode::Agent,
        ] {
            assert_eq!(
                mode.route("/mode smart", ';'),
                Input::Command("/mode smart")
            );
            assert_eq!(mode.prompt(false), format!("{}> ", mode.name()));
            assert_eq!(mode.prompt(true), format!("{}(sql)> ", mode.name()));
        }
    }
}
