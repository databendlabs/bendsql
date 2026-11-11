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

use std::collections::BTreeMap;

use crate::error_code::ErrorCode;
use crate::session::SessionState;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug)]
pub struct LoginRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings: Option<BTreeMap<String, String>>,
}

impl From<&SessionState> for LoginRequest {
    fn from(value: &SessionState) -> Self {
        // The server validates `database` against the default catalog during login,
        // so skip it when a non-default catalog is selected. The database is still
        // applied together with the catalog by each `/v1/query` request.
        let database = match value.catalog.as_deref() {
            Some(catalog) if !catalog.eq_ignore_ascii_case("default") => None,
            _ => value.database.clone(),
        };
        Self {
            role: value.role.clone(),
            settings: value.settings.clone(),
            database,
        }
    }
}

fn default_session_token_ttl_in_secs() -> u64 {
    3600
}

#[derive(Deserialize, Debug, Clone)]
pub struct SessionTokenInfo {
    pub session_token: String,
    #[serde(default = "default_session_token_ttl_in_secs")]
    pub session_token_ttl_in_secs: u64,
    pub refresh_token: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct LoginResponse {
    pub version: String,
    pub tokens: Option<SessionTokenInfo>,
}
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum LoginResponseResult {
    Ok(LoginResponse),
    Err { error: ErrorCode },
}

#[derive(Serialize, Debug)]
pub struct RefreshSessionTokenRequest {
    pub session_token: String,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum RefreshResponse {
    Wrapped { tokens: SessionTokenInfo },
    // Older servers returned the token fields without the `tokens` envelope.
    Ok(SessionTokenInfo),
    Err { error: ErrorCode },
}

#[cfg(test)]
mod test {
    use super::*;

    fn assert_refresh_tokens(response: RefreshResponse, ttl: u64) {
        let tokens = match response {
            RefreshResponse::Wrapped { tokens } | RefreshResponse::Ok(tokens) => tokens,
            RefreshResponse::Err { .. } => panic!("expected successful refresh response"),
        };
        assert!(tokens.session_token == "test-session");
        assert!(tokens.refresh_token == "test-refresh");
        assert_eq!(tokens.session_token_ttl_in_secs, ttl);
    }

    #[test]
    fn refresh_response_accepts_wrapped_and_legacy_tokens() {
        let tokens = serde_json::json!({
            "session_token": "test-session",
            "session_token_ttl_in_secs": 120,
            "refresh_token": "test-refresh"
        });
        for body in [tokens.clone(), serde_json::json!({"tokens": tokens})] {
            assert_refresh_tokens(serde_json::from_value(body).unwrap(), 120);
        }
    }

    #[test]
    fn refresh_response_defaults_missing_ttl() {
        let tokens = serde_json::json!({
            "session_token": "test-session",
            "refresh_token": "test-refresh"
        });
        for body in [tokens.clone(), serde_json::json!({"tokens": tokens})] {
            assert_refresh_tokens(serde_json::from_value(body).unwrap(), 3600);
        }
    }

    #[test]
    fn refresh_response_preserves_server_error() {
        let response: RefreshResponse = serde_json::from_value(serde_json::json!({
            "error": {"code": 5101, "message": "session expired", "detail": "test detail"}
        }))
        .unwrap();
        let RefreshResponse::Err { error } = response else {
            panic!("expected refresh error response");
        };
        assert_eq!(error.code, 5101);
        assert_eq!(error.message, "session expired");
        assert_eq!(error.detail.as_deref(), Some("test detail"));
    }

    #[test]
    fn refresh_response_rejects_incomplete_or_malformed_tokens() {
        for tokens in [
            serde_json::json!({}),
            serde_json::json!({"session_token": "test-session"}),
            serde_json::json!({"refresh_token": "test-refresh"}),
            serde_json::json!({"session_token": null, "refresh_token": "test-refresh"}),
            serde_json::json!({"session_token": "test-session", "refresh_token": null}),
            serde_json::json!({"session_token": "test-session", "refresh_token": "test-refresh", "session_token_ttl_in_secs": "invalid"}),
            serde_json::json!({"session_token": "test-session", "refresh_token": "test-refresh", "session_token_ttl_in_secs": null}),
            serde_json::json!(null),
            serde_json::json!([]),
            serde_json::json!("invalid"),
        ] {
            for body in [tokens.clone(), serde_json::json!({"tokens": tokens})] {
                assert!(serde_json::from_value::<RefreshResponse>(body).is_err());
            }
        }
        assert!(serde_json::from_str::<RefreshResponse>(r#"{"tokens":"#).is_err());
    }

    #[test]
    fn login_request_keeps_database_for_default_catalog() {
        let mut session = SessionState::default();
        session.set_database("db1");
        assert_eq!(
            LoginRequest::from(&session).database.as_deref(),
            Some("db1")
        );

        session.set_catalog("default");
        assert_eq!(
            LoginRequest::from(&session).database.as_deref(),
            Some("db1")
        );
    }

    #[test]
    fn login_request_skips_database_for_non_default_catalog() {
        let mut session = SessionState::default();
        session.set_catalog("iceberg");
        session.set_database("db1");
        assert_eq!(LoginRequest::from(&session).database, None);
    }
}
