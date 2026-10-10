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
use databend_common_ast::parser::{parse_sql, tokenize_sql, Dialect};

use crate::sql_parser::SqlParser;

#[derive(Debug, PartialEq)]
pub enum Input<'a> {
    Empty,
    Command(&'a str),
    Sql(&'a str),
    Question(&'a str),
    Ambiguous,
}

/// True only for whitespace or completely closed comments. Unclosed comments
/// and string literals remain pending SQL, even if tokenization fails.
pub fn comments_only(text: &str) -> bool {
    tokenize_sql(text).is_ok_and(|tokens| tokens.iter().all(|t| t.kind == TokenKind::EOI))
}

fn statement_is_sql(text: &str) -> bool {
    let text = text.trim().strip_suffix("\\G").unwrap_or(text).trim();
    tokenize_sql(text).is_ok_and(|tokens| parse_sql(&tokens, Dialect::Experimental).is_ok())
}

/// Validate the entire batch before passing any completed statement to execution.
/// A statement can span lines; inner task-block semicolons and quoted delimiters
/// use the same splitter as the normal REPL. Never accept a valid SQL prefix plus
/// prose, an invalid second statement, or an unfinished tail for eager execution.
pub fn smart_batch_is_safe(text: &str, delimiter: char) -> bool {
    let parsed = SqlParser::new(delimiter, true, true).parse_statements(text);
    if parsed.statements.is_empty() {
        // No execution yet: an incomplete statement/comment may keep accumulating.
        return true;
    }
    parsed.err.is_empty()
        && comments_only(&parsed.remaining)
        && parsed.statements.iter().all(|s| statement_is_sql(s))
}

pub fn smart_input_is_safe(text: &str, delimiter: char, multi_line: bool) -> bool {
    smart_batch_is_safe(text, delimiter)
        && (multi_line || statement_is_sql(text) || comments_only(text))
}

fn first_keyword(text: &str) -> String {
    // Tokenizer skips ordinary comments. A lexical failure is not a reason to
    // send an otherwise recognizable SQL statement to a model.
    for token in Tokenizer::new(text) {
        match token {
            Ok(token) if token.kind == TokenKind::EOI => continue,
            Ok(token) => return token.text().to_ascii_uppercase(),
            Err(_) => return String::new(),
        }
    }
    String::new()
}

/// Deterministic routing only. Never ask a model whether to execute an input.
/// Pending SQL is checked again by the REPL before extracting any statements.
pub fn classify(line: &str, delimiter: char) -> Input<'_> {
    let line = line.trim();
    if line.is_empty() {
        return Input::Empty;
    }
    if (line.starts_with('/') && !line.starts_with("/*"))
        || line.starts_with('!')
        || matches!(line, "exit" | "quit")
    {
        return Input::Command(line);
    }
    if comments_only(line) {
        return Input::Sql(line);
    }
    if statement_is_sql(line) && smart_batch_is_safe(line, delimiter) {
        return Input::Sql(line);
    }
    let first = first_keyword(line);
    // File-transfer/generation commands are client operations, not AST SQL.
    // Require /sql or sql mode rather than guessing whether "put/get" is prose.
    if matches!(first.as_str(), "PUT" | "GET" | "GENDATA") {
        return Input::Ambiguous;
    }
    let sql_start = matches!(
        first.as_str(),
        "SELECT"
            | "WITH"
            | "INSERT"
            | "UPDATE"
            | "DELETE"
            | "CREATE"
            | "ALTER"
            | "DROP"
            | "TRUNCATE"
            | "MERGE"
            | "COPY"
            | "EXPLAIN"
            | "SHOW"
            | "DESCRIBE"
            | "DESC"
            | "USE"
            | "SET"
            | "UNSET"
            | "GRANT"
            | "REVOKE"
            | "BEGIN"
            | "COMMIT"
            | "ROLLBACK"
            | "CALL"
    );
    let commented = line.starts_with("--") || line.starts_with("/*");
    if sql_start || commented {
        if !smart_batch_is_safe(line, delimiter)
            || line.ends_with('?')
            || line.ends_with('？')
            || (matches!(
                first.as_str(),
                "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN" | "USE" | "SET"
            ) && !line.ends_with(delimiter))
        {
            Input::Ambiguous
        } else {
            Input::Sql(line)
        }
    } else if line.ends_with(delimiter) {
        // A SQL-looking typo should not silently become an AI request.
        Input::Ambiguous
    } else {
        Input::Question(line)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_sql_and_questions_never_auto_execute_a_prefix() {
        for input in [
            "SELECT 1; Explain the result",
            "DROP TABLE sensitive; Why would this happen?",
            "SELECT 1; 这个结果是什么意思？",
            "/* example */ SELECT 1; please explain",
            "SELECT 1; SELECT * FROM",
            "SELECT 1; SELECT 'unfinished",
            "SELECT 1; !exit",
            "SELECT 1; /ask why",
            "SELECT 1; definitely_not_sql;",
            "SELECT 1; /* unfinished",
        ] {
            assert_eq!(classify(input, ';'), Input::Ambiguous, "{input}");
            assert!(!smart_batch_is_safe(input, ';'), "{input}");
        }
    }

    #[test]
    fn accepts_complete_batches_quoted_delimiters_and_closed_comments() {
        for input in [
            "SELECT 1; SELECT 2;",
            "SELECT ';why?'; SELECT 2;",
            "SELECT '请问？';",
            "SELECT 1; -- Why?",
            "/* example */ SELECT 1;",
            "SELECT 1; /* trailing */",
            "SELECT 1\\G",
            "SELECT 1; SELECT 2\\G",
        ] {
            assert_eq!(classify(input, ';'), Input::Sql(input), "{input}");
            assert!(smart_batch_is_safe(input, ';'), "{input}");
        }
        assert!(comments_only("-- comment"));
        assert!(comments_only("/* complete */"));
        assert!(!comments_only("/* incomplete"));
        assert!(!comments_only("SELECT 1"));
    }

    #[test]
    fn validates_pending_sql_and_custom_delimiters_before_execution() {
        assert!(smart_batch_is_safe("SELECT\n1; SELECT 2;", ';'));
        assert!(!smart_batch_is_safe("SELECT\n1; Explain this", ';'));
        assert!(smart_batch_is_safe("SELECT 'a|b'| SELECT 2|", '|'));
        assert!(!smart_batch_is_safe("SELECT 1| Explain this", '|'));
        assert_eq!(
            classify("SELECT 1| SELECT 2|", '|'),
            Input::Sql("SELECT 1| SELECT 2|")
        );
        assert!(smart_batch_is_safe("SELECT * FROM", ';'));
        assert!(smart_input_is_safe("SELECT * FROM", ';', true));
        assert!(!smart_input_is_safe("SELECT * FROM", ';', false));
        assert!(smart_input_is_safe("SELECT 1", ';', false));
    }

    #[test]
    fn unrecognized_completed_sql_needs_explicit_routing() {
        for input in [
            "INVALID SQL;",
            "SELECT * FROM;",
            "GET file://result @stage",
            "PUT file://input @stage",
            "GENDATA t(10)",
        ] {
            assert_eq!(classify(input, ';'), Input::Ambiguous, "{input}");
        }
    }

    #[test]
    fn routes_sql_questions_and_commands() {
        for sql in [
            "SELECT 1;",
            "WITH t AS (SELECT 1) SELECT * FROM t;",
            "SELECT * FROM",
            "INSERT INTO t VALUES (1);",
            "/* comment */ SELECT 1;",
            "-- comment",
        ] {
            assert_eq!(classify(sql, ';'), Input::Sql(sql));
        }
        for question in [
            "哪个渠道收入最高？",
            "Why did the last query fail?",
            "Compare Q1 and Q2",
        ] {
            assert_eq!(classify(question, ';'), Input::Question(question));
        }
        assert_eq!(classify("SELECT 为什么这么慢？", ';'), Input::Ambiguous);
        assert_eq!(classify("show me the results", ';'), Input::Ambiguous);
        assert_eq!(
            classify(" /ask explain this ", ';'),
            Input::Command("/ask explain this")
        );
        assert_eq!(classify("quit", ';'), Input::Command("quit"));
        assert_eq!(classify("  ", ';'), Input::Empty);
    }
}
