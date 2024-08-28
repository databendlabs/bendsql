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
        Self {
            role: value.role.clone(),
            settings: value.settings.clone(),
            database: value.database.clone(),
        }
    }
}

fn default_session_token_ttl_in_secs() -> u64 {
    3600
}

#[derive(Deserialize, Debug, Clone)]
pub struct LoginInfo {
    pub version: String,
    pub session_id: String,
    pub session_token: String,
    #[serde(default = "default_session_token_ttl_in_secs")]
    pub session_token_ttl_in_secs: u64,
    pub refresh_token: String,
}

impl LoginInfo {
    pub fn may_need_refresh_token() {}
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum LoginResponse {
    Ok(LoginInfo),
    Err { error: ErrorCode },
}

#[derive(Serialize, Debug)]
pub struct RefreshSessionTokenRequest {
    pub session_token: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RefreshInfo {
    pub session_token: String,
    pub session_token_ttl_in_secs: u64,
    pub refresh_token: String,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum RefreshResponse {
    Ok(RefreshInfo),
    Err { error: ErrorCode },
}

#[derive(Serialize, Debug)]
pub struct LogoutRequest {
    pub refresh_token: String,
}
