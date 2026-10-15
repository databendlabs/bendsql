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
use std::sync::Arc;

use rustyline::completion::{Completer, FilenameCompleter, Pair};
use rustyline::highlight::{CmdKind, Highlighter};
use rustyline::hint::{Hint, Hinter};
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Context, Helper, Result};

use crate::agent::InteractionMode;
use crate::ast::highlight_query;

pub struct CliHelper {
    completer: FilenameCompleter,
    keywords: Option<Arc<sled::Db>>,
    mode: InteractionMode,
}

pub struct CliHint {
    text: String,
    completion: Option<String>,
}

impl Hint for CliHint {
    fn display(&self) -> &str {
        &self.text
    }
    fn completion(&self) -> Option<&str> {
        self.completion.as_deref()
    }
}

impl CliHelper {
    pub fn new(keywords: Option<Arc<sled::Db>>) -> Self {
        Self {
            completer: FilenameCompleter::new(),
            keywords,
            mode: InteractionMode::Sql,
        }
    }

    pub fn set_mode(&mut self, mode: InteractionMode) {
        self.mode = mode;
    }
}

/// Only the !mode command is completed in non-SQL modes. Completion replaces
/// the argument, never the command or arbitrary question text.
fn mode_candidates(line: &str, pos: usize) -> Option<(usize, Vec<Pair>)> {
    let prefix = line.get(..pos)?;
    let text = prefix.trim_start();
    let argument = text.strip_prefix("!mode")?;
    if !argument.is_empty() && !argument.starts_with(char::is_whitespace) {
        return None;
    }
    let word = argument.trim_start();
    if word.chars().any(char::is_whitespace) {
        return Some((pos, Vec::new()));
    }
    let partial = word.to_ascii_lowercase();
    let separator = if argument.is_empty() { " " } else { "" };
    let pairs = ["sql", "smart", "agent"]
        .into_iter()
        .filter(|mode| mode.starts_with(&partial))
        .map(|mode| Pair {
            display: mode.into(),
            replacement: format!("{separator}{mode}"),
        })
        .collect();
    Some((pos - word.len(), pairs))
}

impl Highlighter for CliHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        // Highlighting is presentation, not a SQL size limit. The AST formatter
        // can exhaust its stack on very large pasted literals; keep them verbatim.
        if self.mode == InteractionMode::Sql && line.len() <= 8192 {
            Cow::Owned(highlight_query(line))
        } else {
            Cow::Borrowed(line)
        }
    }
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        _default: bool,
    ) -> Cow<'b, str> {
        Cow::Borrowed(prompt)
    }
    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Cow::Owned(format!("\x1b[1m{hint}\x1b[m"))
    }
    fn highlight_candidate<'c>(
        &self,
        candidate: &'c str,
        _completion: rustyline::CompletionType,
    ) -> Cow<'c, str> {
        Cow::Borrowed(candidate)
    }
    fn highlight_char(&self, line: &str, _pos: usize, _kind: CmdKind) -> bool {
        self.mode == InteractionMode::Sql && !line.is_empty()
    }
}

impl Hinter for CliHelper {
    type Hint = CliHint;
    fn hint(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Option<CliHint> {
        if pos != line.len() {
            return None;
        }
        if mode_candidates(line, pos).is_some() {
            let argument = line.trim_start().strip_prefix("!mode")?;
            if argument.trim().is_empty() {
                let separator = if argument.is_empty() { " " } else { "" };
                return Some(CliHint {
                    text: format!("{separator}sql | smart | agent"),
                    completion: None,
                });
            }
            return None;
        }
        if self.mode != InteractionMode::Sql {
            return None;
        }
        let last_word = line
            .split(|p: char| p.is_whitespace() || p == '.')
            .next_back()
            .unwrap_or(line);
        if last_word.is_empty() {
            return None;
        }
        let (_, results) = KeyWordCompleter::complete(line, pos, self.keywords.as_ref()?);
        let suffix = results
            .first()?
            .replacement
            .get(last_word.len()..)?
            .to_owned();
        if suffix.is_empty() {
            return None;
        }
        Some(CliHint {
            text: suffix.clone(),
            completion: Some(suffix),
        })
    }
}

impl Completer for CliHelper {
    type Candidate = Pair;
    fn complete(&self, line: &str, pos: usize, ctx: &Context<'_>) -> Result<(usize, Vec<Pair>)> {
        if let Some(result) = mode_candidates(line, pos) {
            return Ok(result);
        }
        if self.mode != InteractionMode::Sql {
            return Ok((pos, Vec::new()));
        }
        if let Some(ref keywords) = self.keywords {
            let result = KeyWordCompleter::complete(line, pos, keywords);
            if !result.1.is_empty() {
                return Ok(result);
            }
        }
        self.completer.complete(line, pos, ctx)
    }
}

impl Validator for CliHelper {
    fn validate(&self, ctx: &mut ValidationContext<'_>) -> Result<ValidationResult> {
        if self.mode == InteractionMode::Sql && ctx.input().trim_end().ends_with('\\') {
            Ok(ValidationResult::Incomplete)
        } else {
            Ok(ValidationResult::Valid(None))
        }
    }
}
impl Helper for CliHelper {}

struct KeyWordCompleter;
impl KeyWordCompleter {
    fn complete(text: &str, pos: usize, db: &sled::Db) -> (usize, Vec<Pair>) {
        let Some(prefix) = text.get(..pos) else {
            return (pos, Vec::new());
        };
        let word = prefix
            .split(|c: char| c.is_whitespace() || c == '.')
            .next_back()
            .unwrap_or(prefix);
        let hint = word.to_ascii_lowercase();
        let results = db
            .scan_prefix(&hint)
            .filter_map(|entry| entry.ok())
            .map(|(word, category)| {
                let word = String::from_utf8_lossy(&word);
                Pair {
                    display: format!("{word}({})", String::from_utf8_lossy(&category)),
                    replacement: word.to_string(),
                }
            })
            .collect();
        (pos - word.len(), results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustyline::history::DefaultHistory;

    #[test]
    fn sql_hints_and_completion_are_disabled_in_other_modes() {
        let db = Arc::new(sled::Config::new().temporary(true).open().unwrap());
        db.insert("select", "k").unwrap();
        let mut helper = CliHelper::new(Some(db));
        let history = DefaultHistory::new();
        let ctx = Context::new(&history);
        let prefix = &"select"[..5];
        assert_eq!(
            helper
                .hint(prefix, prefix.len(), &ctx)
                .unwrap()
                .completion(),
            Some("t")
        );
        assert!(!helper.complete("sel", 3, &ctx).unwrap().1.is_empty());
        for mode in [InteractionMode::Smart, InteractionMode::Agent] {
            helper.set_mode(mode);
            assert!(helper.hint("sel", 3, &ctx).is_none());
            assert!(helper.complete("sel", 3, &ctx).unwrap().1.is_empty());
            assert!(helper.complete("/tmp/", 5, &ctx).unwrap().1.is_empty());
            assert_eq!(helper.highlight("select", 6), "select");
        }
        helper.set_mode(InteractionMode::Sql);
        assert!(helper.hint("sel", 3, &ctx).is_some());
    }

    #[test]
    fn mode_choices_are_available_in_every_mode_and_are_not_inserted_as_one_hint() {
        let mut helper = CliHelper::new(None);
        let history = DefaultHistory::new();
        let ctx = Context::new(&history);
        for mode in [
            InteractionMode::Sql,
            InteractionMode::Smart,
            InteractionMode::Agent,
        ] {
            helper.set_mode(mode);
            let hint = helper.hint("!mode", 5, &ctx).unwrap();
            assert_eq!(hint.display(), " sql | smart | agent");
            assert!(hint.completion().is_none());
            let (_, choices) = helper.complete("!mode ", 6, &ctx).unwrap();
            assert_eq!(
                choices
                    .iter()
                    .map(|p| p.replacement.as_str())
                    .collect::<Vec<_>>(),
                ["sql", "smart", "agent"]
            );
            let (start, choices) = helper.complete("!mode sm", 8, &ctx).unwrap();
            assert_eq!(start, 6);
            assert_eq!(choices[0].replacement, "smart");
            assert!(helper
                .complete("!mode sql extra", 15, &ctx)
                .unwrap()
                .1
                .is_empty());
            assert!(mode_candidates("!model", 6).is_none());
        }
    }
}
