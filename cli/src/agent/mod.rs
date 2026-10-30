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
mod codex;
pub mod config;
pub(crate) mod live;
pub mod llm;
pub(crate) mod markdown;
pub mod memory;
pub(crate) mod privacy;
pub(crate) mod progress;
pub mod router;
mod skills;

use anyhow::{anyhow, bail, Result};
use backend::ChatBackend;
use clap::ValueEnum;
use config::{AgentConfig, ENV_BACKEND};
use llm::Conversation;
use memory::{Memory, MAX_TEXT_BYTES};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum InteractionMode {
    #[default]
    Sql,
    Smart,
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

    pub fn route(self, line: &str, delimiter: char) -> router::Input<'_> {
        let line = line.trim();
        if line.is_empty() {
            return router::Input::Empty;
        }
        if line.starts_with('!') || matches!(line, "exit" | "quit") {
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
    sharing_allowed: bool,
    configured_consent: bool,
    conversation: Conversation,
    config: AgentConfig,
    selected: Option<String>,
    backend: Option<Box<dyn ChatBackend>>,
    skills: Option<skills::EmbeddedSkills>,
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
        let configured_consent = config.settings().is_ok_and(|s| s.allow_data_sharing);
        Self {
            mode: InteractionMode::default(),
            sharing_allowed: configured_consent,
            configured_consent,
            memory: Memory::default(),
            conversation: Conversation::default(),
            config,
            selected,
            backend: None,
            skills: None,
        }
    }

    pub fn validate_config(&self) -> Result<()> {
        self.config.settings().map(|_| ())
    }

    pub fn consent_backend_label(&self) -> String {
        match self.selected.as_deref() {
            Some(name)
                if name == ENV_BACKEND
                    || self
                        .config
                        .settings()
                        .is_ok_and(|s| s.backend(name).is_some()) =>
            {
                name.into()
            }
            _ => "the configured/discovered backend".into(),
        }
    }

    pub fn sharing_allowed(&self) -> bool {
        self.sharing_allowed
    }

    pub fn approve_sharing(&mut self) {
        self.sharing_allowed = true;
    }

    pub fn captures_queries(&self) -> bool {
        self.sharing_allowed && self.mode != InteractionMode::Sql
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
        Ok(format!("Current backend: {current}\nAvailable backends: {names}\nDefault discovery: codex, then claude, then pi, then evot. Explicit --backend/agent.backend selection takes precedence; runtime failures never switch services.\nBuilt-in aliases: claude, claude-code, codex, evot, pi. env explicitly uses BENDSQL_AGENT_BASE_URL, BENDSQL_AGENT_MODEL and optional BENDSQL_AGENT_API_KEY.{notice}"))
    }

    pub fn select_backend(&mut self, name: &str) -> Result<()> {
        // Validate before discarding the active backend or its conversation.
        // Building a backend must not contact a service or start a process.
        let backend = backend::build(&self.config, name)?;
        self.backend = Some(backend);
        self.selected = Some(name.into());
        self.sharing_allowed = self.configured_consent;
        self.conversation.clear();
        Ok(())
    }

    /// `output` receives display-only deltas from streaming backends; the
    /// returned answer is authoritative and the only text recorded on success.
    pub async fn ask(
        &mut self,
        question: &str,
        output: Option<&backend::AnswerStream>,
    ) -> Result<String> {
        if question.trim().is_empty() {
            bail!("Question must not be empty");
        }
        if question.len() > MAX_TEXT_BYTES {
            bail!("Question exceeds the 8 KiB limit; please shorten it");
        }
        if !self.sharing_allowed {
            bail!("AI data sharing is not approved. Confirm in the REPL or configure agent.allow_data_sharing=true; no request was sent.");
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
        if self.skills.is_none() {
            self.skills = Some(skills::EmbeddedSkills::new()?);
        }
        let mut messages = self.conversation.messages(question, &self.memory);
        messages[0]
            .content
            .push_str(self.skills.as_ref().unwrap().prompt());
        let timeout = std::time::Duration::from_secs(self.config.settings()?.timeout_secs);
        let backend = self.backend.as_ref().unwrap();
        let request = match output {
            Some(output) => backend.stream(&messages, output),
            None => backend.complete(&messages),
        };
        let answer = tokio::time::timeout(timeout, request)
            .await
            .map_err(|_| {
                match backend.diagnostic_context() {
                    Some(phase) => anyhow!("AI backend request timed out while {phase}. Check native CLI provider/network configuration; SQL remains available."),
                    None => anyhow!("AI backend request timed out. SQL remains available."),
                }
            })??;
        // Failures/cancellation do not enter conversation history.
        self.conversation.remember(question.into(), answer.clone());
        Ok(answer)
    }
}

pub const HELP: &str = "Interactive modes (default: sql):\n\
sql>    SQL input with autocomplete; finish with the configured delimiter\n\
smart>  Whole-batch AST routing; mixed SQL/prose executes nothing; no SQL autocomplete\n\
agent>  AI questions with cached query context; no SQL execution or autocomplete\n\
!mode [sql|smart|agent]  Show choices or change mode (Tab completes the choices)\n\
!backend [name]  List backends or switch (clears conversation, retains query memory)\n\
!sql <SQL>       Execute explicitly (no trailing delimiter required)\n\
!ask <question>  Ask AI explicitly; suggested SQL is never executed\n\
!context [clear] List AI-mode query summaries, or clear query/conversation memory\n\
SQL mode never captures query data. First AI use requires confirmation unless allow_data_sharing is configured.\n\
!help           Show this help\n\
!configs        Show current settings\n\
!set            Set settings (ai_output: markdown|plain, ai_stream: true|false)\n\
!source <file>  Execute SQL from a file\n\
!exit / !quit   Exit; Ctrl+C clears pending SQL or interrupts work\n\
Questions send selected SQL, result previews, errors and conversation to the configured model backend.\n\
SQL-only history is saved by default. AI activation stops disk history for this session; external services have their own retention policies.\n\
Use !context clear before discussing unrelated or sensitive data.";

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
allow_data_sharing = true
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
        session
            .memory
            .push(memory::QueryRecord::new("SELECT 1"))
            .unwrap();
        session.backend = Some(Box::new(FakeBackend {
            fail: false,
            requests: requests.clone(),
        }));
        session.ask("Q1?", None).await.unwrap();
        session.ask("follow up", None).await.unwrap();
        assert_eq!(requests.lock().unwrap()[1].as_array().unwrap().len(), 4);
        assert!(session.select_backend("missing").is_err());
        assert_eq!(session.selected.as_deref(), Some("first"));
        session.ask("still here", None).await.unwrap();
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
        assert!(session.ask("question", None).await.is_err());
        assert_eq!(
            session.conversation.messages("next", &session.memory).len(),
            2
        );
        assert!(session
            .ask(&"x".repeat(MAX_TEXT_BYTES + 1), None)
            .await
            .is_err());
        assert!(session.ask("   ", None).await.is_err());
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
            session.ask("question", None)
        )
        .await
        .is_err());
        assert_eq!(
            session.conversation.messages("next", &session.memory).len(),
            2
        );
        let error = session.ask("question", None).await.unwrap_err();
        assert!(error.to_string().contains("timed out"));
        assert_eq!(
            session.conversation.messages("next", &session.memory).len(),
            2
        );
    }

    #[tokio::test]
    async fn streamed_deltas_are_display_only_and_final_answer_is_recorded() {
        struct StreamingBackend {
            fail: bool,
        }
        #[async_trait::async_trait]
        impl ChatBackend for StreamingBackend {
            async fn complete(&self, _: &[llm::Message]) -> Result<String> {
                bail!("streaming test backend must use stream()")
            }
            async fn stream(
                &self,
                _: &[llm::Message],
                output: &backend::AnswerStream,
            ) -> Result<String> {
                output.text("partial ");
                output.text("answer");
                if self.fail {
                    bail!("stream broke");
                }
                Ok("final answer".into())
            }
        }
        let mut session = configured_session();
        let (output, mut deltas) = backend::AnswerStream::channel();
        session.backend = Some(Box::new(StreamingBackend { fail: true }));
        assert!(session.ask("question", Some(&output)).await.is_err());
        assert_eq!(deltas.recv().await.unwrap(), "partial ");
        assert_eq!(deltas.recv().await.unwrap(), "answer");
        assert_eq!(
            session.conversation.messages("next", &session.memory).len(),
            2,
            "a failed stream must not enter the conversation"
        );
        session.backend = Some(Box::new(StreamingBackend { fail: false }));
        assert_eq!(
            session.ask("question", Some(&output)).await.unwrap(),
            "final answer"
        );
        let messages = session.conversation.messages("next", &session.memory);
        assert_eq!(messages[2].content, "final answer");
        // Without a stream handle the non-streaming path is used.
        assert!(session.ask("question", None).await.is_err());
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

    #[tokio::test]
    async fn sharing_requires_explicit_permission_and_sql_never_captures() {
        let mut session = AgentSession::new(AgentConfig::default(), None);
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        session.backend = Some(Box::new(FakeBackend {
            fail: false,
            requests: requests.clone(),
        }));
        session.mode = InteractionMode::Smart;
        assert!(!session.captures_queries());
        assert!(session
            .ask("hello", None)
            .await
            .unwrap_err()
            .to_string()
            .contains("not approved"));
        assert!(requests.lock().unwrap().is_empty());
        session.approve_sharing();
        assert!(session.captures_queries());
        // Give the fake backend a fixed selected label; no external discovery.
        session.selected = Some("env".into());
        session.ask("hello", None).await.unwrap();
        assert_eq!(requests.lock().unwrap().len(), 1);
        session.mode = InteractionMode::Sql;
        assert!(!session.captures_queries());
    }

    #[test]
    fn modes_control_routing_and_prompts() {
        let sql = "SELECT 1;";
        let question = "What does the last result mean?";
        assert_eq!(InteractionMode::default(), InteractionMode::Sql);
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
                mode.route("!mode smart", ';'),
                Input::Command("!mode smart")
            );
        }
    }
}
