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

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct SessionState {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secondary_roles: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txn_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub need_sticky: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub need_keep_alive: Option<bool>,

    // hide fields of no interest (but need to send back to server in next query)
    #[serde(flatten)]
    additional_fields: HashMap<String, serde_json::Value>,
}

impl SessionState {
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let settings = self.settings.get_or_insert_with(BTreeMap::new);
        settings.insert(key.into(), value.into());
    }

    pub fn set_database(&mut self, database: impl Into<String>) {
        self.database = Some(database.into());
    }

    pub fn set_role(&mut self, role: impl Into<String>) {
        self.role = Some(role.into());
    }
}
