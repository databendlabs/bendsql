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

use databend_common_ast::parser::token::{TokenKind, Tokenizer};

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
    pub fn parse_line(&self, line: &str, query_buffer: &mut String) -> Vec<String> {
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

        // Update the buffer with remaining text
        *query_buffer = parsed.remaining;

        // Return complete statements
        parsed.statements
    }

    /// Parse accumulated query text to extract complete statements
    fn parse_statements(&self, query: &str) -> ParseResult {
        let mut statements = Vec::new();
        let mut remaining_query = query.to_string();

        'Parser: loop {
            let mut is_valid = true;
            let tokenizer = Tokenizer::new(&remaining_query);
            let mut previous_token_backslash = false;

            for token in tokenizer {
                match token {
                    Ok(token) => {
                        // SQL end with `;` or `\G` in repl
                        let is_end_query = token.text() == self.delimiter.to_string();
                        let is_slash_g = self.is_repl
                            && (previous_token_backslash
                                && token.kind == TokenKind::Ident
                                && token.text() == "G")
                            || (token.text().ends_with("\\G"));

                        if is_end_query || is_slash_g {
                            // Extract the statement and continue with remaining text
                            let (sql, remain) = remaining_query.split_at(token.span.end as usize);
                            if is_valid
                                && !sql.is_empty()
                                && sql.trim() != self.delimiter.to_string()
                            {
                                let sql = sql.trim_end_matches(self.delimiter);
                                statements.push(sql.trim().to_string());
                            }
                            remaining_query = remain.to_string();
                            continue 'Parser;
                        }
                        previous_token_backslash = matches!(token.kind, TokenKind::Backslash);
                    }
                    Err(_e) => {
                        // ignore current query if have invalid token.
                        is_valid = false;
                        continue;
                    }
                }
            }
            break;
        }

        ParseResult {
            statements,
            remaining: remaining_query,
        }
    }
}

struct ParseResult {
    statements: Vec<String>,
    remaining: String,
}

/// Parse SQL text for web API (non-REPL mode)
pub fn parse_sql_for_web(sql_text: &str) -> Vec<String> {
    let parser = SqlParser::new(';', true, false);
    parser.parse(sql_text)
}
