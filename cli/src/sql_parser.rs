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

            // Parse the accumulated query to find statement boundaries
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

        // Parse the accumulated query to find statement boundaries
        let parsed = self.parse_statements(query_buffer);

        *err = parsed.err;
        // Update the buffer with remaining text
        *query_buffer = parsed.remaining;

        // Return complete statements
        parsed.statements
    }

    /// Find the byte offset where an unclosed block comment (`/*`) begins.
    /// Returns `None` if all block comments are properly closed.
    /// Skips `--` line comments, `$$` dollar-quoted strings, and respects
    /// single/double-quoted string literals.
    /// Block comments are non-nested (matches tokenizer behaviour): the
    /// first `*/` always closes the comment.
    fn unclosed_block_comment_start(s: &str) -> Option<usize> {
        let mut in_block_comment = false;
        let mut open_pos = None;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_dollar_quote = false;
        let mut in_line_comment = false;
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let c = bytes[i];

            // Newline resets line-comment state.
            if c == b'\n' {
                in_line_comment = false;
                i += 1;
                continue;
            }
            if in_line_comment {
                i += 1;
                continue;
            }

            // Inside a block comment, only look for `*/`.
            if in_block_comment {
                if c == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    in_block_comment = false;
                    open_pos = None;
                    i += 2;
                } else {
                    i += 1;
                }
                continue;
            }

            // Inside a dollar-quoted string, only look for `$$`.
            if in_dollar_quote {
                if c == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'$' {
                    in_dollar_quote = false;
                    i += 2;
                } else {
                    i += 1;
                }
                continue;
            }

            match c {
                b'\'' if !in_double_quote => in_single_quote = !in_single_quote,
                b'"' if !in_single_quote => in_double_quote = !in_double_quote,
                b'$' if !in_single_quote
                    && !in_double_quote
                    && i + 1 < bytes.len()
                    && bytes[i + 1] == b'$' =>
                {
                    in_dollar_quote = true;
                    i += 2;
                    continue;
                }
                b'-' if !in_single_quote
                    && !in_double_quote
                    && i + 1 < bytes.len()
                    && bytes[i + 1] == b'-' =>
                {
                    in_line_comment = true;
                    i += 2;
                    continue;
                }
                b'/' if !in_single_quote
                    && !in_double_quote
                    && i + 1 < bytes.len()
                    && bytes[i + 1] == b'*' =>
                {
                    in_block_comment = true;
                    open_pos = Some(i);
                    i += 2;
                    continue;
                }
                _ => {}
            }
            i += 1;
        }
        if in_block_comment {
            open_pos
        } else {
            None
        }
    }

    /// Parse accumulated query text to extract complete statements
    fn parse_statements(&self, query: &str) -> ParseResult {
        // Split off the unclosed block-comment tail so the tokenizer only
        // sees text it can handle.  Statements before the `/*` are still
        // extracted normally; the comment portion stays in `remaining`.
        let (to_parse, comment_tail) = match Self::unclosed_block_comment_start(query) {
            Some(pos) => (&query[..pos], &query[pos..]),
            None => (query, ""),
        };

        let mut statements = Vec::new();
        let mut remaining_query = to_parse.to_string();
        let mut err = String::new();

        let delimiter_str = self.delimiter.to_string();

        'Parser: loop {
            let mut is_valid = true;
            let tokenizer = Tokenizer::new(&remaining_query);
            let mut previous_token_backslash = false;
            let mut tracker = TaskBlockTracker::new();

            for token in tokenizer {
                match token {
                    Ok(token) => {
                        tracker.feed(&token, &delimiter_str);

                        // SQL end with `;` or `\G` in repl
                        let is_end_query = token.text() == delimiter_str;
                        let is_slash_g = self.is_repl
                            && (previous_token_backslash
                                && token.kind == TokenKind::Ident
                                && token.text() == "G")
                            || (token.text().ends_with("\\G"));

                        if is_end_query || is_slash_g {
                            if tracker.in_block() {
                                // Skip inner delimiters within the block.
                                previous_token_backslash =
                                    matches!(token.kind, TokenKind::Backslash);
                                continue;
                            }
                            // Extract the statement and continue with remaining text
                            let (sql, remain) = remaining_query.split_at(token.span.end as usize);
                            if is_valid && !sql.is_empty() && sql.trim() != delimiter_str {
                                let sql = sql.trim_end_matches(self.delimiter);
                                statements.push(sql.trim().to_string());
                            }
                            remaining_query = remain.to_string();
                            continue 'Parser;
                        }
                        previous_token_backslash = matches!(token.kind, TokenKind::Backslash);
                    }
                    Err(e) => {
                        // ignore current query if have invalid token.
                        is_valid = false;
                        err = e.to_string();
                        continue;
                    }
                }
            }
            break;
        }

        // Re-attach the unclosed comment tail so it keeps accumulating.
        if !comment_tail.is_empty() {
            remaining_query.push_str(comment_tail);
        }

        ParseResult {
            statements,
            remaining: remaining_query,
            err,
        }
    }
}

struct ParseResult {
    statements: Vec<String>,
    remaining: String,
    err: String,
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
