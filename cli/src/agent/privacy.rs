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

/// Conservative exclusion, not a general PII detector. False positives keep data
/// local; no SQL redaction is ever applied to the statement actually executed.
/// Streaming word comparisons keep bulk SQL checks linear with fixed memory.
pub(crate) fn sensitive(text: &str) -> bool {
    const KEYS: &[&str] = &[
        "PASSWORD",
        "IDENTIFIED",
        "CONNECTION",
        "CREDENTIAL",
        "CREDENTIALS",
        "ACCESS_KEY_ID",
        "SECRET_ACCESS_KEY",
        "AWS_KEY_ID",
        "AWS_SECRET_KEY",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "SESSION_TOKEN",
        "AWS_SESSION_TOKEN",
        "API_KEY",
        "API_TOKEN",
        "ACCESS_TOKEN",
        "REFRESH_TOKEN",
        "BEARER_TOKEN",
        "SAS_TOKEN",
        "ACCOUNT_KEY",
        "PRIVATE_KEY",
        "SECRET",
        "TOKEN",
        "CLIENT_SECRET",
        "AUTHORIZATION",
        "BEARER",
        "SIGNATURE",
        "SIG",
        "X_AMZ_SIGNATURE",
        "X_AMZ_CREDENTIAL",
        "AWSACCESSKEYID",
        "X_GOOG_SIGNATURE",
        "AZURE_SAS_TOKEN",
    ];
    let mut words = text
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .filter(|w| !w.is_empty());
    let first = words.next().unwrap_or("");
    let ddl = first.eq_ignore_ascii_case("CREATE") || first.eq_ignore_ascii_case("ALTER");
    if std::iter::once(first).chain(words).any(|word| {
        KEYS.iter().any(|key| word.eq_ignore_ascii_case(key))
            || (ddl && (word.eq_ignore_ascii_case("USER") || word.eq_ignore_ascii_case("STAGE")))
            || sensitive_compound(word)
    }) {
        return true;
    }
    // URI userinfo can carry credentials without a separate PASSWORD keyword.
    text.match_indices("://").any(|(at, _)| {
        text[at + 3..]
            .split(['/', '?', '#', '\'', '"', ' ', '\n'])
            .next()
            .is_some_and(|authority| authority.contains('@'))
    })
}

/// Compound identifiers such as `password_hash`, `user_api_key` or `apiToken`.
/// `KEY` and `TOKEN` alone are too common (`primary_key`, `token_count`), so they
/// only count when paired with a qualifier or as the trailing name component.
fn sensitive_compound(word: &str) -> bool {
    // Sensitive wherever they appear in a compound name.
    const ANYWHERE: &[&str] = &[
        "PASSWORD",
        "PASSWORDS",
        "PASSWD",
        "PWD",
        "PASSPHRASE",
        "SECRET",
        "SECRETS",
        "CREDENTIAL",
        "CREDENTIALS",
        "APIKEY",
        "APIKEYS",
        "ACCESSKEY",
        "SECRETKEY",
        "PRIVATEKEY",
        "AUTHORIZATION",
        "BEARER",
    ];
    // Sensitive as adjacent components, e.g. `api_key`, `privateKeyPem`.
    const PAIRS: &[(&str, &str)] = &[
        ("API", "KEY"),
        ("API", "KEYS"),
        ("ACCESS", "KEY"),
        ("ACCESS", "KEYS"),
        ("AWS", "KEY"),
        ("PRIVATE", "KEY"),
        ("ACCOUNT", "KEY"),
        ("SIGNING", "KEY"),
        ("ENCRYPTION", "KEY"),
        ("MASTER", "KEY"),
        ("SESSION", "KEY"),
        ("AUTH", "KEY"),
        ("SSH", "KEY"),
        ("TOKEN", "VALUE"),
        ("TOKEN", "HASH"),
    ];
    // Sensitive as the final component: `auth_token`, but not `token_count`.
    const TRAILING: &[&str] = &["TOKEN", "SIGNATURE", "SIG"];
    let is = |part: &str, keys: &[&str]| keys.iter().any(|key| part.eq_ignore_ascii_case(key));
    let mut previous = "";
    let mut last = "";
    let mut parts = 0;
    let found = for_each_component(word, |part| {
        let hit = is(part, ANYWHERE)
            || PAIRS
                .iter()
                .any(|(a, b)| previous.eq_ignore_ascii_case(a) && part.eq_ignore_ascii_case(b));
        previous = part;
        last = part;
        parts += 1;
        hit
    });
    found || (parts > 1 && is(last, TRAILING))
}

/// Visits `snake_case` and `camelCase` components; stops early when `visit` is true.
/// Callers pass ASCII-alphanumeric words, so byte boundaries are char boundaries.
fn for_each_component<'a>(word: &'a str, mut visit: impl FnMut(&'a str) -> bool) -> bool {
    for piece in word.split('_').filter(|piece| !piece.is_empty()) {
        let bytes = piece.as_bytes();
        let mut start = 0;
        for i in 1..bytes.len() {
            let (before, current) = (bytes[i - 1], bytes[i]);
            // `apiKey` -> api|Key, `APIKey` -> API|Key.
            let boundary = current.is_ascii_uppercase()
                && (before.is_ascii_lowercase()
                    || before.is_ascii_digit()
                    || (before.is_ascii_uppercase()
                        && bytes.get(i + 1).is_some_and(u8::is_ascii_lowercase)));
            if boundary {
                if visit(&piece[start..i]) {
                    return true;
                }
                start = i;
            }
        }
        if visit(&piece[start..]) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_are_excluded_without_changing_sql() {
        for sql in [
            "CREATE USER alice IDENTIFIED BY 'private'", "ALTER USER USER() IDENTIFIED BY 'private'",
            "CREATE OR REPLACE STAGE s URL='s3://bucket'", "CREATE /* comment */ USER x",
            "CREATE STAGE s URL='s3://bucket' CONNECTION=(access_key_id='id', secret_access_key='private')",
            "CREATE CONNECTION c STORAGE_TYPE='s3' SECRET_ACCESS_KEY='private'",
            "COPY INTO t FROM 's3://bucket' CONNECTION=(AWS_KEY_ID='id', AWS_SECRET_KEY='private')",
            "COPY INTO t FROM 'https://bucket/?X-Amz-Signature=private'",
            "SELECT 'https://user:private@host/path'", "SELECT 'https://host/?token=private'",
            "api_key", "password", "session_token",
        ] { assert!(sensitive(sql), "{sql}"); }
        assert!(!sensitive("SELECT count(*) FROM orders"));
        assert!(!sensitive("SELECT token_count FROM usage_stats"));
    }

    #[test]
    fn compound_identifiers_are_excluded() {
        for sql in [
            "SELECT password_hash FROM users",
            "SELECT id, user_api_key FROM accounts",
            "SELECT aws_secret FROM settings",
            "SELECT github_token FROM integrations",
            "SELECT \"apiKey\", \"passwordHash\" FROM t",
            "SELECT \"APIKey\" FROM t",
            "SELECT private_key_pem FROM certs",
            "SELECT db_pwd FROM legacy",
            "SELECT request_signature FROM webhooks",
            "SELECT token_hash FROM sessions",
            "SELECT s3_access_key_id FROM stages",
        ] {
            assert!(sensitive(sql), "{sql}");
        }
        // Column names are checked one identifier at a time.
        for column in [
            "password_hash",
            "user_api_key",
            "aws_secret",
            "refreshToken",
        ] {
            assert!(sensitive(column), "{column}");
        }
        for benign in [
            "SELECT token_count, max_tokens, input_tokens FROM usage",
            "SELECT primary_key, sort_key, cache_key, public_key FROM t",
            "SELECT keyword, tokenizer, signal FROM t",
            "SELECT o.order_key, sum(price) FROM orders o GROUP BY 1",
            "token_count",
            "partition_key",
        ] {
            assert!(!sensitive(benign), "{benign}");
        }
    }
}
