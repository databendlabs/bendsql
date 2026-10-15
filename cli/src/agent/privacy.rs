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
}
