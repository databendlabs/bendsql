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

//! Best-effort masking of secrets embedded in log payloads such as SQL text.
//!
//! Logging lives in this crate so that every consumer (CLI, Python and Node
//! bindings) benefits, instead of relying on the caller to install a masking
//! log subscriber.

use once_cell::sync::Lazy;
use regex::Regex;

static SENSITIVE_LITERAL: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?ix)
        \b(?:
            identified\s+by
          | pass(?:word|wd)
          | credentials?
          | aws_(?:access_key_id|key_id|secret_(?:access_)?key|session_token)
          | secret_(?:access_)?key
          | access_key(?:_id|_secret)?
          | api_(?:key|token)
          | access_token
          | refresh_token
          | session_token
          | client_secret
          | private_key
        )\b
        \s*(?:=|by)?\s*
        ('(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*")
        "#,
    )
    .expect("sensitive literal regex must compile")
});

/// Mask string literals that follow sensitive keywords (`IDENTIFIED BY`,
/// `PASSWORD`, `CREDENTIALS`, access keys, tokens, ...) in a log payload.
///
/// This is deliberately best-effort rather than a real parser: it keeps
/// credentials out of log files for the common spellings, and callers must not
/// treat the result as secret-free with certainty.
pub fn mask_secrets(text: &str) -> String {
    SENSITIVE_LITERAL
        .replace_all(text, |caps: &regex::Captures<'_>| {
            let matched = caps.get(0).expect("group 0 always exists").as_str();
            let value = caps.get(1).expect("group 1 is captured").as_str();
            let masked = if value.starts_with('\'') {
                "'<redacted>'"
            } else {
                "\"<redacted>\""
            };
            format!("{}{}", &matched[..matched.len() - value.len()], masked)
        })
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::mask_secrets;

    #[test]
    fn masks_identified_by() {
        assert_eq!(
            mask_secrets("CREATE USER u IDENTIFIED BY 'p@ssw0rd'"),
            "CREATE USER u IDENTIFIED BY '<redacted>'"
        );
    }

    #[test]
    fn masks_key_value_assignments() {
        assert_eq!(
            mask_secrets("SET PASSWORD = 'hunter2'"),
            "SET PASSWORD = '<redacted>'"
        );
        assert_eq!(
            mask_secrets("copy credentials=(aws_key_id='AKIAXX' aws_secret_key='shh')"),
            "copy credentials=(aws_key_id='<redacted>' aws_secret_key='<redacted>')"
        );
    }

    #[test]
    fn keeps_double_quote_style() {
        assert_eq!(
            mask_secrets(r#"ALTER USER u SET password = "abc""#),
            r#"ALTER USER u SET password = "<redacted>""#
        );
    }

    #[test]
    fn leaves_unrelated_sql_untouched() {
        let sql = "SELECT a FROM t WHERE b = 'value' AND c = 1";
        assert_eq!(mask_secrets(sql), sql);
    }

    #[test]
    fn does_not_match_key_substrings() {
        // `secret_key` must not match inside `aws_secret_key` twice; the whole
        // word must be masked once and the surrounding text preserved.
        assert_eq!(
            mask_secrets("aws_secret_access_key='x'"),
            "aws_secret_access_key='<redacted>'"
        );
    }
}
