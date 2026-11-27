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

use crate::error::{Error, Result};
use crate::error_code::ErrorCode;
use crate::session::SessionState;
use semver::Version;
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

#[derive(Deserialize)]
#[serde(untagged)]
enum LoginResponse {
    Err { error: ErrorCode },
    Ok { version: String },
}

pub(crate) fn decode_login_version(body: &[u8]) -> Result<Version> {
    // An unexpected response can still contain credentials. Do not include the
    // body, deserializer input, or invalid version string in decode errors.
    let response = serde_json::from_slice(body).map_err(|error| {
        Error::Decode(format!(
            "invalid login JSON response ({:?}, line {}, column {})",
            error.classify(),
            error.line(),
            error.column()
        ))
    })?;
    match response {
        LoginResponse::Err { error } => Err(Error::AuthFailure(error)),
        LoginResponse::Ok { version } => version
            .parse()
            .map_err(|_| Error::Decode("invalid server version in login response".into())),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn login_decode_errors_do_not_include_response_values() {
        for (body, diagnostic) in [
            (&br#"{"tokens":"sensitive-value""#[..], "invalid login JSON"),
            (br#"{"version":["sensitive-value"]}"#, "invalid login JSON"),
            (
                br#"{"version":"sensitive-value"}"#,
                "invalid server version",
            ),
        ] {
            let error = decode_login_version(body).unwrap_err().to_string();
            assert!(error.contains(diagnostic), "{error}");
            assert!(!error.contains("sensitive-value"));
        }
    }

    #[test]
    fn login_decode_preserves_structured_auth_errors() {
        let result = decode_login_version(br#"{"error":{"code":3900,"message":"unauthorized"}}"#);
        assert!(
            matches!(result, Err(Error::AuthFailure(error)) if error.code == 3900 && error.message == "unauthorized")
        );
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
