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

use databend_common_ast::parser::token::{Token, TokenKind, Tokenizer};

/// State machine for tracking CREATE/ALTER TASK ... AS BEGIN ... END blocks.
/// Semicolons inside a task script block should not split the statement.
#[derive(PartialEq, Clone, Copy)]
enum TaskBlockState {
    /// No task-related tokens seen yet.
    Init,
    /// Seen CREATE or ALTER.
    SeenCreateAlter,
    /// Seen CREATE/ALTER TASK; waiting for AS.
    SeenTask,
    /// Inside the BEGIN...END block; skipping inner delimiters.
    InBlock,
    /// Saw `; END` inside the block — candidate for block close.
    SeenBlockEnd,
}

/// Tracks whether we are inside a CREATE/ALTER TASK ... AS BEGIN ... END
/// script block, so that inner semicolons are not treated as statement
/// terminators.
struct TaskBlockTracker {
    state: TaskBlockState,
    paren_depth: u32,
    previous_token_kind: Option<TokenKind>,
}

impl TaskBlockTracker {
    fn new() -> Self {
        Self {
            state: TaskBlockState::Init,
            paren_depth: 0,
            previous_token_kind: None,
        }
    }

    /// Process a token and advance the state machine.
    fn feed(&mut self, token: &Token, delimiter_str: &str) {
        // Track parenthesis depth so we can ignore AS inside
        // expressions like WHEN CAST(... AS BOOLEAN).
        match token.kind {
            TokenKind::LParen => self.paren_depth += 1,
            TokenKind::RParen => self.paren_depth = self.paren_depth.saturating_sub(1),
            _ => {}
        }

        match self.state {
            TaskBlockState::Init => {
                if token.kind == TokenKind::CREATE || token.kind == TokenKind::ALTER {
                    self.state = TaskBlockState::SeenCreateAlter;
                }
            }
            TaskBlockState::SeenCreateAlter => {
                if token.kind == TokenKind::TASK {
                    self.state = TaskBlockState::SeenTask;
                } else if token.kind != TokenKind::OR && token.kind != TokenKind::REPLACE {
                    self.state = TaskBlockState::Init;
                }
            }
            TaskBlockState::SeenTask => {
                // Only consider top-level AS (paren_depth == 0).
                // AS inside CAST(... AS type) has paren_depth > 0.
                if self.previous_token_kind == Some(TokenKind::AS) && self.paren_depth == 0 {
                    if token.kind == TokenKind::BEGIN {
                        self.state = TaskBlockState::InBlock;
                    } else {
                        // Task body is a single statement, not a block.
                        // Disable further detection so that AS keywords
                        // inside the body (e.g. column aliases) are not
                        // mistaken for the task-level AS.
                        self.state = TaskBlockState::Init;
                    }
                }
            }
            TaskBlockState::InBlock => {
                if token.kind == TokenKind::END
                    && self.previous_token_kind == Some(TokenKind::SemiColon)
                {
                    self.state = TaskBlockState::SeenBlockEnd;
                }
            }
            TaskBlockState::SeenBlockEnd => {
                // After seeing `; END`, only `;`, the delimiter, `\`
                // (start of `\G`), or `G` after `\` should keep us in
                // this state. Anything else means it wasn't the real
                // block end.
                let is_backslash_g = token.kind == TokenKind::Ident
                    && token.text() == "G"
                    && self.previous_token_kind == Some(TokenKind::Backslash);
                if token.kind != TokenKind::SemiColon
                    && token.kind != TokenKind::Backslash
                    && !is_backslash_g
                    && token.text() != delimiter_str
                {
                    self.state = TaskBlockState::InBlock;
                }
            }
        }

        self.previous_token_kind = Some(token.kind);
    }

    /// Returns true when the parser should skip delimiters (we are
    /// inside a task script block and haven't seen the closing END).
    fn in_block(&self) -> bool {
        self.state == TaskBlockState::InBlock
    }
}

/// Iterative boundary scanner: large strings never enter Logos' recursive
/// debug matcher. The scanner does not validate SQL or modify executed text.
struct SplitTokens<'a> {
    source: &'a str,
    offset: usize,
}

#[derive(Clone, Copy)]
enum SplitError {
    IncompleteComment,
    IncompleteLiteral,
    Invalid,
}

impl<'a> SplitTokens<'a> {
    fn new(source: &'a str) -> Self {
        Self { source, offset: 0 }
    }
    fn token(&self, start: usize, kind: TokenKind) -> Token<'a> {
        Token {
            source: self.source,
            kind,
            span: (start..self.offset).into(),
        }
    }
}

impl<'a> Iterator for SplitTokens<'a> {
    type Item = Result<Token<'a>, SplitError>;
    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self.source.as_bytes();
        loop {
            while self.offset < bytes.len() && bytes[self.offset].is_ascii_whitespace() {
                self.offset += 1;
            }
            if self.offset == bytes.len() {
                return None;
            }
            let start = self.offset;
            if bytes[start..].starts_with(b"--") {
                while self.offset < bytes.len() && !matches!(bytes[self.offset], b'\n' | b'\x0c') {
                    self.offset += 1;
                }
                continue;
            }
            if bytes[start..].starts_with(b"/*") {
                self.offset += 2;
                while self.offset + 1 < bytes.len() && !bytes[self.offset..].starts_with(b"*/") {
                    self.offset += 1;
                }
                if self.offset + 1 >= bytes.len() {
                    self.offset = bytes.len();
                    return Some(Err(SplitError::IncompleteComment));
                }
                self.offset += 2;
                continue;
            }
            if bytes[start..].starts_with(b"$$") {
                self.offset += 2;
                while self.offset + 1 < bytes.len() && !bytes[self.offset..].starts_with(b"$$") {
                    self.offset += 1;
                }
                if self.offset + 1 >= bytes.len() {
                    self.offset = bytes.len();
                    return Some(Err(SplitError::IncompleteLiteral));
                }
                self.offset += 2;
                return Some(Ok(self.token(start, TokenKind::LiteralCodeString)));
            }
            let quote = bytes[start];
            if matches!(quote, b'\'' | b'"' | b'`') {
                self.offset += 1;
                while self.offset < bytes.len() {
                    match bytes[self.offset] {
                        b'\\' if quote != b'`' => self.offset = (self.offset + 2).min(bytes.len()),
                        c if c == quote => {
                            self.offset += 1;
                            if quote != b'`'
                                && self.offset < bytes.len()
                                && bytes[self.offset] == quote
                            {
                                self.offset += 1;
                            } else {
                                return Some(Ok(self.token(start, TokenKind::LiteralString)));
                            }
                        }
                        _ => self.offset += 1,
                    }
                }
                return Some(Err(SplitError::IncompleteLiteral));
            }
            if bytes[start] == b'@' {
                // Mirror the AST lexer's LiteralAtString rule:
                // @([^\s,`;'"()]|\\\s|\\'|\\"|\\\\)+
                // A backslash only escapes whitespace, quotes, or a backslash, so
                // `@stage\G` keeps `\G` and `@stage\;` still ends at `;`.
                self.offset += 1;
                while let Some(c) = self.source[self.offset..].chars().next() {
                    if c == '\\' {
                        self.offset += 1;
                        if let Some(next) = self.source[self.offset..].chars().next() {
                            if next.is_whitespace() || matches!(next, '\'' | '"' | '\\') {
                                self.offset += next.len_utf8();
                            }
                        }
                    } else if c.is_whitespace()
                        || matches!(c, ',' | '`' | ';' | '\'' | '"' | '(' | ')')
                    {
                        break;
                    } else {
                        self.offset += c.len_utf8();
                    }
                }
                // A bare `@` is the abs operator, not an invalid token.
                let kind = if self.offset == start + 1 {
                    TokenKind::Abs
                } else {
                    TokenKind::LiteralAtString
                };
                return Some(Ok(self.token(start, kind)));
            }
            if bytes[start].is_ascii_alphanumeric()
                || matches!(bytes[start], b'_' | b'$')
                || bytes[start] >= 128
            {
                self.offset += 1;
                while self.offset < bytes.len()
                    && (bytes[self.offset].is_ascii_alphanumeric()
                        || matches!(bytes[self.offset], b'_' | b'$')
                        || bytes[self.offset] >= 128)
                {
                    self.offset += 1;
                }
                let word = &self.source[start..self.offset];
                let kind = if word.len() > 256 {
                    TokenKind::Ident
                } else {
                    let mut end = (start + 256).min(bytes.len());
                    while !self.source.is_char_boundary(end) {
                        end -= 1;
                    }
                    match Tokenizer::new(&self.source[start..end]).next() {
                        Some(Ok(token)) => {
                            self.offset = start + token.span.end as usize;
                            token.kind
                        }
                        _ if end < bytes.len()
                            && word.len() == 1
                            && matches!(bytes[start], b'x' | b'X' | b'e' | b'E' | b'r' | b'R')
                            && bytes.get(self.offset) == Some(&b'\'') =>
                        {
                            // A bounded lexer window may end inside a prefixed
                            // literal. Keep the prefix and let the iterative quote
                            // scanner consume the complete original literal next.
                            TokenKind::Ident
                        }
                        _ => return Some(Err(SplitError::Invalid)),
                    }
                };
                return Some(Ok(self.token(start, kind)));
            }
            // Operators have bounded lookahead; no long literal reaches Logos.
            let mut end = (start + 256).min(bytes.len());
            while !self.source.is_char_boundary(end) {
                end -= 1;
            }
            match Tokenizer::new(&self.source[start..end]).next() {
                Some(Ok(token)) if token.kind != TokenKind::EOI => {
                    self.offset += token.span.end as usize;
                    return Some(Ok(self.token(start, token.kind)));
                }
                _ => {
                    self.offset += self.source[start..].chars().next().unwrap().len_utf8();
                    return Some(Err(SplitError::Invalid));
                }
            }
        }
    }
}

/// SQL parser utility for splitting SQL text into individual statements
pub struct SqlParser {
    delimiter: char,
    multi_line: bool,
    is_repl: bool,
}

impl SqlParser {
    pub fn new(delimiter: char, multi_line: bool, is_repl: bool) -> Self {
        Self {
            delimiter,
            multi_line,
            is_repl,
        }
    }

    pub(crate) fn comments_only(text: &str) -> bool {
        SplitTokens::new(text).next().is_none()
    }

    /// Parse SQL text and return a vector of individual SQL statements
    pub fn parse(&self, sql_text: &str) -> Vec<String> {
        let mut queries = Vec::new();
        let mut current_query = String::new();

        for line in sql_text.lines() {
            let line = line.trim();

            if line.is_empty() {
                continue;
            }

            // Handle special commands for REPL mode
            if current_query.is_empty()
                && (line.starts_with('!')
                    || line == "exit"
                    || line == "quit"
                    || line.to_uppercase().starts_with("PUT"))
            {
                queries.push(line.to_owned());
                continue;
            }

            // Handle single line mode
            if !self.multi_line {
                if line.starts_with("--") {
                    continue;
                } else {
                    queries.push(line.to_owned());
                    continue;
                }
            }

            // Append line to current query
            if !current_query.is_empty() {
                current_query.push('\n');
            }
            current_query.push_str(line);

            let parsed = self.parse_statements(&current_query);
            for statement in parsed.statements {
                queries.push(statement);
            }
            current_query = parsed.remaining;
        }

        // Add any remaining query
        if !current_query.is_empty() {
            let trimmed = current_query.trim();
            if !trimmed.is_empty() && trimmed != self.delimiter.to_string() {
                queries.push(trimmed.to_string());
            }
        }

        queries
    }

    /// Parse a single line incrementally, maintaining state
    /// Returns complete statements and updates the provided buffer
    pub fn parse_line(
        &self,
        line: &str,
        query_buffer: &mut String,
        err: &mut String,
    ) -> Vec<String> {
        if line.is_empty() {
            return vec![];
        }

        // Handle special commands for REPL mode
        if query_buffer.is_empty()
            && (line.starts_with('!')
                || line == "exit"
                || line == "quit"
                || line.to_uppercase().starts_with("PUT"))
        {
            return vec![line.to_owned()];
        }

        // Handle single line mode
        if !self.multi_line {
            if line.starts_with("--") {
                return vec![];
            } else {
                return vec![line.to_owned()];
            }
        }

        // Append line to query buffer
        if !query_buffer.is_empty() {
            query_buffer.push('\n');
        }
        query_buffer.push_str(line);

        let parsed = self.parse_statements(query_buffer);

        *err = parsed.err;
        // Update the buffer with remaining text
        *query_buffer = parsed.remaining;

        // Return complete statements
        parsed.statements
    }

    /// Split in one iterative scan with original byte spans. Incomplete literals
    /// and comments stay in the caller's buffer, never partially executed.
    pub(crate) fn parse_statements(&self, query: &str) -> ParseResult {
        let mut statements = Vec::new();
        let mut start = 0;
        let mut valid = true;
        let mut err = String::new();
        let delimiter = self.delimiter.to_string();
        let mut previous_backslash = false;
        let mut tracker = TaskBlockTracker::new();
        for token in SplitTokens::new(query) {
            let token = match token {
                Ok(token) => token,
                Err(SplitError::IncompleteComment) => break,
                Err(SplitError::IncompleteLiteral) => {
                    err = "unable to recognize the rest tokens".into();
                    break;
                }
                Err(SplitError::Invalid) => {
                    valid = false;
                    err = "unable to recognize the rest tokens".into();
                    continue;
                }
            };
            tracker.feed(&token, &delimiter);
            // `\G` lexes as Backslash + Ident, except inside a stage location
            // (`LIST @s1\G`), where it is the tail of the at-string token.
            let slash_g = (self.is_repl
                && previous_backslash
                && token.kind == TokenKind::Ident
                && token.text() == "G")
                || (token.kind == TokenKind::LiteralAtString && token.text().ends_with("\\G"));
            if (token.text() == delimiter || slash_g) && !tracker.in_block() {
                let end = token.span.end as usize;
                let sql = query[start..end].trim_end_matches(self.delimiter).trim();
                if valid && !sql.is_empty() {
                    statements.push(sql.to_owned());
                }
                start = end;
                tracker = TaskBlockTracker::new();
                valid = true;
                previous_backslash = false;
            } else {
                previous_backslash = token.kind == TokenKind::Backslash;
            }
        }
        ParseResult {
            statements,
            remaining: query[start..].to_owned(),
            err,
        }
    }
}

pub(crate) struct ParseResult {
    pub(crate) statements: Vec<String>,
    pub(crate) remaining: String,
    pub(crate) err: String,
}

/// Parse SQL text for web API (non-REPL mode)
pub fn parse_sql_for_web(sql_text: &str) -> Vec<String> {
    let parser = SqlParser::new(';', true, false);
    parser.parse(sql_text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_lexer_does_not_drop_long_prefixed_literals() {
        for prefix in ['x', 'X', 'e', 'E', 'r', 'R'] {
            for length in [240, 252, 253, 254, 255, 256, 400, 8192, 3 * 1024 * 1024] {
                let insert = format!(
                    "INSERT INTO t VALUES ({prefix}'{}')",
                    "ab".repeat(length / 2)
                );
                let input = format!("{insert}; SELECT 2;");
                for multiline in [false, true] {
                    for repl in [false, true] {
                        let parser = SqlParser::new(';', multiline, repl);
                        let parsed = parser.parse_statements(&input);
                        assert!(parsed.err.is_empty(), "prefix={prefix} length={length}");
                        assert_eq!(parsed.statements, [insert.clone(), "SELECT 2".into()]);
                        let expected = if multiline {
                            vec![insert.clone(), "SELECT 2".into()]
                        } else {
                            vec![input.clone()]
                        };
                        assert_eq!(parser.parse(&input), expected);
                        let mut buffer = String::new();
                        let mut error = String::new();
                        assert_eq!(parser.parse_line(&input, &mut buffer, &mut error), expected);
                        assert!(buffer.is_empty() && error.is_empty());
                    }
                }
            }
        }
    }

    #[test]
    fn invalid_token_diagnostic_matches_existing_sql_suite() {
        let parser = SqlParser::new(';', true, false);
        for input in [
            "select * from 🥹;",
            "explain select * from c where b in （'x');",
        ] {
            let mut buffer = String::new();
            let mut error = String::new();
            assert!(parser.parse_line(input, &mut buffer, &mut error).is_empty());
            assert_eq!(error, "unable to recognize the rest tokens");
        }
    }

    #[test]
    fn large_quoted_sql_is_retained_and_split_without_a_ui_stack_overflow() {
        let query = format!("SELECT '{}'; SELECT 2;", "x".repeat(3 * 1024 * 1024));
        let parser = SqlParser::new(';', true, true);
        let parsed = parser.parse_statements(&query);
        assert!(parsed.err.is_empty());
        assert_eq!(parsed.statements.len(), 2);
        assert!(parsed.statements[0].len() > 3 * 1024 * 1024);
        assert_eq!(parsed.statements[1], "SELECT 2");
    }

    #[test]
    fn large_literals_comments_and_multiline_input_use_constant_stack() {
        let parser = SqlParser::new(';', true, true);
        for literal in [
            format!("'{}'", "中;\\G".repeat(200_000)),
            format!("\"{}\"", "x;".repeat(600_000)),
            format!("`{}`", "x;".repeat(600_000)),
            format!("$${}$$", "x;".repeat(600_000)),
        ] {
            let sql = format!("SELECT {literal}; SELECT 2;");
            let parsed = parser.parse_statements(&sql);
            assert!(parsed.err.is_empty());
            assert_eq!(
                parsed.statements,
                [format!("SELECT {literal}"), "SELECT 2".into()]
            );
        }
        let mut buffer = String::new();
        let mut error = String::new();
        assert!(parser
            .parse_line("SELECT '", &mut buffer, &mut error)
            .is_empty());
        for _ in 0..64 {
            assert!(parser
                .parse_line(&"x".repeat(50 * 1024), &mut buffer, &mut error)
                .is_empty());
        }
        let statements = parser.parse_line("'; SELECT 3;", &mut buffer, &mut error);
        assert_eq!(statements.len(), 2);
        assert!(statements[0].len() > 3 * 1024 * 1024);
        assert!(buffer.is_empty());
        assert!(error.is_empty());
        let sql = format!("SELECT 1; /* {}", "x".repeat(3 * 1024 * 1024));
        let pending = parser.parse_statements(&sql);
        assert_eq!(pending.statements, ["SELECT 1"]);
        assert!(pending.remaining.starts_with(" /*"));
    }

    #[test]
    fn escaped_quotes_and_comments_do_not_change_boundaries() {
        let parser = SqlParser::new(';', true, true);
        for text in [
            r#"SELECT 'a\';b'; SELECT 2;"#,
            "SELECT 'a'';b'; SELECT 2;",
            r#"SELECT "a\";b"; SELECT 2;"#,
            "SELECT `a;b`; SELECT 2;",
            "SELECT $$a;b$$; SELECT 2;",
            "SELECT 1 /* ; */; -- ;\n SELECT 2;",
        ] {
            let parsed = parser.parse_statements(text);
            assert!(parsed.err.is_empty(), "{text}");
            assert_eq!(parsed.statements.len(), 2, "{text}");
        }
    }

    #[test]
    fn stage_locations_and_abs_operator_match_the_ast_lexer() {
        let repl = SqlParser::new(';', true, true);
        // `\G` at the end of a stage location still ends the statement.
        for (input, statement) in [
            ("LIST @s1\\G", "LIST @s1\\G"),
            ("LIST @s1/dir/\\G", "LIST @s1/dir/\\G"),
            ("LIST @s1 \\G", "LIST @s1 \\G"),
        ] {
            let parsed = repl.parse_statements(input);
            assert!(parsed.err.is_empty(), "{input}");
            assert_eq!(parsed.statements, [statement], "{input}");
            assert!(parsed.remaining.is_empty(), "{input}");
        }
        // A backslash only escapes whitespace, quotes, and backslashes, so it
        // never hides the delimiter.
        let parsed = repl.parse_statements("LIST @s1\\; SELECT 2;");
        assert_eq!(parsed.statements, ["LIST @s1\\", "SELECT 2"]);
        let parsed = repl.parse_statements("COPY INTO t FROM @s/a\\ b.csv; SELECT 2;");
        assert_eq!(
            parsed.statements,
            ["COPY INTO t FROM @s/a\\ b.csv", "SELECT 2"]
        );
        // A bare `@` is the abs operator, not an unrecognized token.
        for parser in [repl, SqlParser::new(';', true, false)] {
            let parsed = parser.parse_statements("SELECT 2 @ 3; SELECT @(-5);");
            assert!(parsed.err.is_empty());
            assert_eq!(parsed.statements, ["SELECT 2 @ 3", "SELECT @(-5)"]);
        }
    }

    #[test]
    fn split_tokens_match_ast_lexer_for_stage_locations() {
        for input in [
            "LIST @s1\\G",
            "LIST @s1\\;",
            "@s/a\\ b.csv,x",
            "@s/a\\'b x",
            "@s/a\\\"b x",
            "@s\\\\G",
            "@s/中文 x",
            "@~/p (",
            "@\\",
            "SELECT 2 @ 3",
            "SELECT @(-5)",
            "@ @a @",
        ] {
            let expected: Vec<_> = Tokenizer::new(input)
                .map(|t| t.unwrap())
                .filter(|t| t.kind != TokenKind::EOI)
                .map(|t| (t.kind, t.text().to_owned()))
                .collect();
            let actual: Vec<_> = SplitTokens::new(input)
                .map(|t| t.ok().unwrap())
                .map(|t| (t.kind, t.text().to_owned()))
                .collect();
            assert_eq!(actual, expected, "{input:?}");
        }
    }

    #[test]
    fn single_line_task_script_block_is_one_statement() {
        let parser = SqlParser::new(';', true, true);
        let parsed =
            parser.parse_statements("CREATE TASK t AS BEGIN select 1; select 2; END; SELECT 3;");
        assert!(parsed.err.is_empty());
        assert_eq!(
            parsed.statements,
            ["CREATE TASK t AS BEGIN select 1; select 2; END", "SELECT 3"]
        );
    }

    #[test]
    fn test_task_script_block() {
        let parser = SqlParser::new(';', true, false);
        // Block is kept as a single statement, and trailing SQL splits correctly
        let sql = "CREATE TASK IF NOT EXISTS nightly_refresh\n WAREHOUSE = 'default'\n SCHEDULE = USING CRON '0 0 2 * * *' 'UTC'\nAS\nBEGIN\n    select 1;\n    select 2;\nEND;\nSELECT 3;";
        let stmts = parser.parse(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("BEGIN"));
        assert!(stmts[0].contains("select 1;"));
        assert!(stmts[0].contains("select 2;"));
        // The outer ; is the client delimiter and gets trimmed.
        // Server's task_sql_block expects BEGIN...END without trailing ;.
        assert!(stmts[0].ends_with("END"), "got: {}", stmts[0]);
        assert_eq!(stmts[1], "SELECT 3");
    }

    #[test]
    fn test_task_script_block_repl_line_by_line() {
        let parser = SqlParser::new(';', true, true);
        let mut buf = String::new();
        let mut err = String::new();

        // Simulate line-by-line REPL input
        assert!(parser
            .parse_line("CREATE TASK t1 AS", &mut buf, &mut err)
            .is_empty());
        assert!(parser.parse_line("BEGIN", &mut buf, &mut err).is_empty());
        assert!(parser
            .parse_line("    select 1;", &mut buf, &mut err)
            .is_empty());
        assert!(parser
            .parse_line("    select 2;", &mut buf, &mut err)
            .is_empty());
        let stmts = parser.parse_line("END;", &mut buf, &mut err);
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("BEGIN"));
        assert!(stmts[0].contains("END"));
        assert!(stmts[0].contains("select 1;"));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_task_script_block_with_inner_transaction() {
        // Task script block containing BEGIN (transaction) inside
        let parser = SqlParser::new(';', true, false);
        let sql = "CREATE TASK t1 AS\nBEGIN\n    begin;\n    select 1;\n    commit;\nEND;";
        let stmts = parser.parse(sql);
        assert_eq!(stmts.len(), 1, "got: {:?}", stmts);
        assert!(stmts[0].contains("BEGIN"));
        assert!(stmts[0].contains("begin;"));
        assert!(stmts[0].contains("select 1;"));
        assert!(stmts[0].contains("commit;"));
        assert!(stmts[0].ends_with("END"));
    }

    #[test]
    fn test_task_script_block_with_case_expression() {
        // CASE...END; inside a task block should not close the block
        let parser = SqlParser::new(';', true, false);
        let sql = "CREATE TASK t1 AS\nBEGIN\n    INSERT INTO t SELECT CASE WHEN flag THEN 1 ELSE 0 END;\n    select 2;\nEND;";
        let stmts = parser.parse(sql);
        assert_eq!(stmts.len(), 1, "got: {:?}", stmts);
        assert!(stmts[0].contains("CASE"));
        assert!(stmts[0].contains("select 2;"));
        assert!(stmts[0].ends_with("END"));
    }

    #[test]
    fn test_task_script_block_no_trailing_semicolon() {
        // User omits the trailing ; after END
        let parser = SqlParser::new(';', true, false);
        let sql = "CREATE TASK t1 AS\nBEGIN\n    select 1;\n    select 2;\nEND";
        let stmts = parser.parse(sql);
        assert_eq!(stmts.len(), 1, "got: {:?}", stmts);
        assert!(stmts[0].contains("BEGIN"));
        assert!(stmts[0].contains("END"));
    }

    #[test]
    fn test_task_single_statement_with_as_begin_alias() {
        // CREATE TASK with single statement body containing AS begin alias
        let parser = SqlParser::new(';', true, false);
        let sql = "CREATE TASK t1 AS SELECT 1 AS begin; SELECT 2;";
        let stmts = parser.parse(sql);
        assert_eq!(stmts.len(), 2, "got: {:?}", stmts);
        assert_eq!(stmts[0], "CREATE TASK t1 AS SELECT 1 AS begin");
        assert_eq!(stmts[1], "SELECT 2");
    }

    #[test]
    fn test_task_script_block_custom_delimiter() {
        // With a custom delimiter (|), END; is SQL syntax and | is client terminator
        let parser = SqlParser::new('|', true, false);
        let sql = "CREATE TASK t1 AS\nBEGIN\n    select 1;\n    select 2;\nEND;\n|\nSELECT 3|";
        let stmts = parser.parse(sql);
        assert_eq!(stmts.len(), 2, "got: {:?}", stmts);
        assert!(stmts[0].contains("BEGIN"));
        assert!(stmts[0].contains("select 1;"));
        assert!(stmts[0].contains("END;"));
        assert_eq!(stmts[1], "SELECT 3");
    }

    #[test]
    fn test_task_script_block_backslash_g_terminator() {
        // \G as statement terminator should close a task block
        let parser = SqlParser::new(';', true, true);
        let sql = "CREATE TASK t1 AS\nBEGIN\n    select 1;\n    select 2;\nEND\\G";
        let stmts = parser.parse(sql);
        assert_eq!(stmts.len(), 1, "got: {:?}", stmts);
        assert!(stmts[0].contains("BEGIN"));
        assert!(stmts[0].contains("select 1;"));
        assert!(stmts[0].contains("select 2;"));
    }
}
