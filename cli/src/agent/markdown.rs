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

use std::io::{self, IsTerminal};

use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};

use crate::config::AiOutput;

/// Presentation only: never send ANSI-rendered text back into the conversation.
/// No shell, URL opener, syntax highlighter or image loader is involved.
pub(crate) fn format_answer(answer: &str, mode: AiOutput) -> String {
    render(answer, mode, rich_terminal(), color())
}

/// Whether stdout can show rendered Markdown and cursor-addressed redraws.
pub(super) fn rich_terminal() -> bool {
    io::stdout().is_terminal() && std::env::var_os("TERM").is_none_or(|term| term != "dumb")
}

pub(super) fn color() -> bool {
    std::env::var_os("NO_COLOR").is_none()
}

/// A live frame uses the same renderer as the final answer, so the completed
/// answer replaces the last frame without a change of style.
pub(super) fn render_frame(answer: &str, mode: AiOutput, color: bool) -> String {
    render(answer, mode, true, color)
}

fn plain(answer: &str) -> String {
    let mut output = format!("AI> {answer}");
    if !output.ends_with('\n') {
        output.push('\n');
    }
    output
}

fn render(answer: &str, mode: AiOutput, terminal: bool, color: bool) -> String {
    // Defense in depth. Only this renderer's fixed style codes may reach the
    // terminal, never model-supplied escape/OSC/clipboard/control sequences.
    let safe: String = answer
        .chars()
        .filter(|c| !c.is_control() || matches!(c, '\n' | '\t'))
        .collect();
    if mode == AiOutput::Plain || !terminal {
        return plain(&safe);
    }
    let mut view = View::new(color);
    for event in Parser::new_ext(&safe, Options::empty()) {
        view.event(event);
        // Bound formatting expansion from pathological nesting. Fallback is
        // still inert Markdown, not an error or a partially styled terminal.
        if view.output.len() > 64 * 1024 {
            return plain(&safe);
        }
    }
    if !view.structured {
        return plain(&safe);
    }
    if view.output.trim().is_empty() {
        return "AI> [Non-text Markdown content omitted]\n".into();
    }
    // Paragraph separators are presentation, not code: remove only the extra
    // final paragraph blank line, leaving verbatim code whitespace untouched.
    if !view.last_code {
        while view.output.ends_with("\n\n") {
            view.output.pop();
        }
    }
    if !view.output.ends_with('\n') {
        view.output.push('\n');
    }
    format!("AI>\n{}", view.output)
}

struct View {
    output: String,
    color: bool,
    styles: Vec<&'static str>,
    lists: Vec<Option<u64>>,
    links: Vec<(String, usize)>,
    image_depth: usize,
    item_depth: usize,
    in_code: bool,
    quotes: usize,
    last_code: bool,
    structured: bool,
}

impl View {
    fn new(color: bool) -> Self {
        Self {
            output: String::new(),
            color,
            styles: Vec::new(),
            lists: Vec::new(),
            links: Vec::new(),
            image_depth: 0,
            item_depth: 0,
            in_code: false,
            quotes: 0,
            last_code: false,
            structured: false,
        }
    }

    fn newline(&mut self) {
        if !self.output.ends_with('\n') && !self.output.is_empty() {
            self.output.push('\n');
        }
    }

    fn separator(&mut self) {
        self.newline();
        if !self.output.is_empty() && !self.output.ends_with("\n\n") {
            self.output.push('\n');
        }
    }

    fn push_style(&mut self, code: &'static str) {
        self.styles.push(code);
        if self.color {
            self.output.push_str(code);
        }
    }

    fn pop_style(&mut self) {
        self.styles.pop();
        if self.color {
            self.output.push_str("\x1b[0m");
            for style in ["\x1b[1;36m", "\x1b[1m", "\x1b[3m", "\x1b[36m"] {
                if self.styles.contains(&style) {
                    self.output.push_str(style);
                }
            }
        }
    }

    fn event(&mut self, event: Event<'_>) {
        if matches!(event, Event::Start(Tag::Image { .. })) {
            self.structured = true;
            self.image_depth += 1;
            return;
        }
        if self.image_depth > 0 {
            if matches!(event, Event::End(TagEnd::Image)) {
                self.image_depth -= 1;
            }
            return;
        }
        match event {
            Event::Start(Tag::Heading { .. }) => {
                self.structured = true;
                self.last_code = false;
                self.separator();
                self.push_style("\x1b[1;36m");
            }
            Event::End(TagEnd::Heading(_)) => {
                self.pop_style();
                self.separator();
            }
            Event::Start(Tag::Paragraph) => {
                self.last_code = false;
                if self.item_depth == 0 {
                    self.separator();
                }
                if self.quotes > 0 {
                    self.output.push_str(&"│ ".repeat(self.quotes));
                }
            }
            Event::End(TagEnd::Paragraph) => {
                if self.item_depth > 0 {
                    self.newline();
                } else {
                    self.separator();
                }
            }
            Event::Start(Tag::CodeBlock(_)) => {
                self.structured = true;
                self.separator();
                self.in_code = true;
            }
            Event::End(TagEnd::CodeBlock) => {
                self.in_code = false;
                self.last_code = true;
                self.newline();
            }
            Event::Start(Tag::Strong) => {
                self.structured = true;
                self.push_style("\x1b[1m");
            }
            Event::Start(Tag::Emphasis) => {
                self.structured = true;
                self.push_style("\x1b[3m");
            }
            Event::End(TagEnd::Strong | TagEnd::Emphasis) => self.pop_style(),
            Event::Start(Tag::List(start)) => {
                self.structured = true;
                self.last_code = false;
                if self.lists.is_empty() {
                    self.separator();
                } else {
                    self.newline();
                }
                self.lists.push(start);
            }
            Event::End(TagEnd::List(_)) => {
                self.lists.pop();
                self.newline();
            }
            Event::Start(Tag::Item) => {
                self.newline();
                self.item_depth += 1;
                self.output
                    .push_str(&"  ".repeat(self.lists.len().saturating_sub(1)));
                match self.lists.last_mut() {
                    Some(Some(number)) => {
                        self.output.push_str(&format!("{number}. "));
                        *number = number.saturating_add(1);
                    }
                    _ => self.output.push_str("• "),
                }
            }
            Event::End(TagEnd::Item) => {
                self.newline();
                self.item_depth -= 1;
            }
            Event::Start(Tag::BlockQuote(_)) => {
                self.structured = true;
                self.last_code = false;
                self.separator();
                self.quotes += 1;
            }
            Event::End(TagEnd::BlockQuote(_)) => {
                self.quotes -= 1;
                self.separator();
            }
            Event::Start(Tag::Link { dest_url, .. }) => {
                self.structured = true;
                self.links.push((dest_url.into_string(), self.output.len()));
            }
            Event::End(TagEnd::Link) => {
                if let Some((url, start)) = self.links.pop() {
                    if (url.starts_with("https://") || url.starts_with("http://"))
                        && self.output[start..] != url
                    {
                        self.output.push_str(&format!(" ({url})"));
                    }
                }
            }
            Event::Text(text) => self.output.push_str(&text),
            Event::Code(code) => {
                self.structured = true;
                self.push_style("\x1b[36m");
                self.output.push_str(&code);
                self.pop_style();
            }
            Event::SoftBreak | Event::HardBreak => {
                self.output.push('\n');
                if self.quotes > 0 && !self.in_code {
                    self.output.push_str(&"│ ".repeat(self.quotes));
                }
            }
            Event::Rule => {
                self.structured = true;
                self.separator();
                self.output.push_str("───\n");
            }
            Event::Html(_) | Event::InlineHtml(_) => {
                self.structured = true;
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, OutputFormat, Settings};

    #[test]
    fn formats_headings_lists_quotes_and_links_without_raw_markers() {
        let raw = "## Analysis\n\n**Scan** is *large*.\n\n1. Inspect `EXPLAIN`.\n2. Check rows.\n\n> Evidence only\n\n[Docs](https://docs.databend.com/)\n";
        let text = render(raw, AiOutput::Markdown, true, false);
        assert!(text.starts_with("AI>\nAnalysis\n"));
        assert!(text.contains("Scan is large."));
        assert!(text.contains("1. Inspect EXPLAIN."));
        assert!(text.contains("2. Check rows."));
        assert!(text.contains("│ Evidence only"));
        assert!(text.contains("Docs (https://docs.databend.com/)"));
        for marker in ["##", "**", "`", "\x1b"] {
            assert!(!text.contains(marker));
        }
        assert!(render(raw, AiOutput::Markdown, true, true).contains("\x1b[1;36m"));
        assert_eq!(raw, "## Analysis\n\n**Scan** is *large*.\n\n1. Inspect `EXPLAIN`.\n2. Check rows.\n\n> Evidence only\n\n[Docs](https://docs.databend.com/)\n");
    }

    #[test]
    fn sql_blocks_preserve_indentation_tabs_and_do_not_wrap_or_execute() {
        let sql = "SELECT '  spaces',\n\t'line\n  literal';\n\n";
        let raw = format!("```sql\n{sql}```\n");
        assert_eq!(
            render(&raw, AiOutput::Markdown, true, false),
            format!("AI>\n{sql}")
        );
        let open = "```sql\nSELECT 1;\n";
        assert_eq!(
            render(open, AiOutput::Markdown, true, false),
            "AI>\nSELECT 1;\n"
        );
    }

    #[test]
    fn html_images_and_model_terminal_controls_never_become_active_content() {
        let raw = "## Safe\n\n<script>private-html</script>\n\n![image](https://private.example/image)\n\n[link](javascript:alert)\n\n\x1b]52;c;clipboard\x07";
        let text = render(raw, AiOutput::Markdown, true, false);
        for forbidden in [
            "<script>",
            "private-html",
            "private.example",
            "javascript:",
            "\x1b",
            "\x07",
        ] {
            assert!(!text.contains(forbidden));
        }
    }

    #[test]
    fn plain_and_nonterminal_fallback_preserve_markdown_and_raw_history() {
        let raw = "## Heading\n\n```sql\nSELECT 1;\n```";
        let expected = format!("AI> {raw}\n");
        assert_eq!(render(raw, AiOutput::Plain, true, true), expected);
        assert_eq!(render(raw, AiOutput::Markdown, false, true), expected);
        assert_eq!(
            render("Hello", AiOutput::Markdown, true, true),
            "AI> Hello\n"
        );
    }

    #[test]
    fn nested_markup_is_bounded_and_styles_reset_after_emphasis() {
        let text = render(
            "- **Outer**\n  - Inner\n\n    Follow-up\n\n***Bold italic*** end",
            AiOutput::Markdown,
            true,
            false,
        );
        assert!(text.contains("• Outer"));
        assert!(text.contains("  • Inner"));
        assert!(text.contains("Bold italic end"));
        let colored = render("**bold** then plain", AiOutput::Markdown, true, true);
        assert!(colored.contains("bold\x1b[0m then plain"));
        let deep = format!("{}**text**", "> ".repeat(4096));
        assert!(render(&deep, AiOutput::Markdown, true, true).len() < 64 * 1024);
    }

    #[test]
    fn ai_output_setting_is_separate_from_sql_format_and_validated() {
        let mut settings = Settings::default();
        assert_eq!(settings.ai_output, AiOutput::Markdown);
        settings.inject_ctrl_cmd("ai_output", "plain").unwrap();
        assert_eq!(settings.ai_output, AiOutput::Plain);
        assert_eq!(settings.output_format, OutputFormat::Table);
        assert!(settings.inject_ctrl_cmd("ai_output", "html").is_err());
        assert_eq!(settings.ai_output, AiOutput::Plain);
        let config: Config = toml::from_str("[settings]\nai_output='markdown'").unwrap();
        settings.merge_config(&config);
        assert_eq!(settings.ai_output, AiOutput::Markdown);
        assert!(toml::from_str::<Config>("[settings]\nai_output='html'").is_err());
    }
}
