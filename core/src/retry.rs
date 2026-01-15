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

use crate::error::Error;

pub(crate) struct RetryDecision<'a> {
    pub(crate) error: Error,
    pub(crate) should_retry: bool,
    pub(crate) reason: Option<&'a str>,
}

impl<'a> RetryDecision<'a> {
    pub(crate) fn no_retry(error: Error) -> Self {
        Self {
            error,
            should_retry: false,
            reason: None,
        }
    }

    pub(crate) fn retry_with_reason(error: Error, reason: &'a str) -> Self {
        Self {
            error,
            should_retry: true,
            reason: Some(reason),
        }
    }
}
