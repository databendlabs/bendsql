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

mod query_kind;
pub use query_kind::replace_newline_in_box_display;
pub use query_kind::GenType;
pub use query_kind::QueryKind;

use databend_common_ast::parser::{parse_sql, token::TokenKind, tokenize_sql, Dialect};
use sqlformat::{FormatOptions, QueryParams};

pub fn format_query(query: &str) -> String {
    let kind = QueryKind::from(query);
    if matches!(kind, QueryKind::Get(_, _) | QueryKind::Put(_, _)) {
        return query.to_owned();
    }

    if let Ok(tokens) = databend_common_ast::parser::tokenize_sql(query) {
        if let Ok((stmt, _)) = parse_sql(&tokens, Dialect::Experimental) {
            let options = FormatOptions::default();

            let pretty_sql = sqlformat::format(query, &QueryParams::None, &options);
            // if pretty sql could be parsed into same stmt, return pretty sql
            if let Ok(pretty_tokens) = databend_common_ast::parser::tokenize_sql(&pretty_sql) {
                if let Ok((pretty_stmt, _)) = parse_sql(&pretty_tokens, Dialect::Experimental) {
                    if stmt.to_string() == pretty_stmt.to_string() {
                        return pretty_sql;
                    }
                }
            }

            return stmt.to_string();
        }
    }
    query.to_string()
}

pub fn highlight_query(line: &str) -> String {
    let tokens = tokenize_sql(line);
    let mut line = line.to_owned();

    if let Ok(tokens) = tokens {
        for token in tokens.iter().rev() {
            if TokenKind::is_keyword(&token.kind)
                || TokenKind::is_reserved_ident(&token.kind, false)
                || TokenKind::is_reserved_function_name(&token.kind)
            {
                line.replace_range(
                    std::ops::Range::from(token.span),
                    &format!("\x1b[1;32m{}\x1b[0m", token.text()),
                );
            } else if TokenKind::is_literal(&token.kind) {
                line.replace_range(
                    std::ops::Range::from(token.span),
                    &format!("\x1b[1;33m{}\x1b[0m", token.text()),
                );
            }
        }
    }

    line
}
