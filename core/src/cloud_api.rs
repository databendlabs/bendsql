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

use crate::auth::{Auth, SensitiveString};
use crate::error::{Error, Result};
use reqwest::header::{HeaderValue, AUTHORIZATION};
use reqwest::RequestBuilder;
use url::Url;

#[cfg(test)]
mod tests;

pub(crate) const HEADER_CLOUD_TOKEN: &str = "X-DATABENDCLOUD-TOKEN";
pub(crate) const HEADER_CLOUD_ORG: &str = "X-DATABENDCLOUD-ORG";
pub(crate) const HEADER_CLOUD_WAREHOUSE: &str = "X-DATABENDCLOUD-WAREHOUSE";

pub(crate) struct CloudApiAuth {
    endpoint: Url,
    token: SensitiveString,
    token_header: HeaderValue,
    org: String,
}

impl CloudApiAuth {
    pub(crate) fn new(endpoint: Url, token: String, org: String) -> Result<Self> {
        let allowed_origin = match std::env::var("BENDSQL_CLOUD_API_ORIGIN") {
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(Error::BadArgument(
                    "BENDSQL_CLOUD_API_ORIGIN must be UTF-8".into(),
                ))
            }
        };
        Self::with_allowed_origin(endpoint, token, org, allowed_origin.as_deref())
    }

    fn with_allowed_origin(
        endpoint: Url,
        token: String,
        org: String,
        allowed: Option<&str>,
    ) -> Result<Self> {
        let additional = allowed.filter(|value| !value.is_empty()).map(|value| {
            let invalid = || Error::BadArgument("BENDSQL_CLOUD_API_ORIGIN must be an HTTPS origin without userinfo, path, query or fragment".into());
            let origin = Url::parse(value).map_err(|_| invalid())?;
            if origin.scheme() != "https" || origin.host_str().is_none()
                || !origin.username().is_empty() || origin.password().is_some()
                || origin.path() != "/" || origin.query().is_some() || origin.fragment().is_some()
                || origin.port_or_known_default() == Some(0) {
                return Err(invalid());
            }
            Ok(origin.origin())
        }).transpose()?;
        let official = endpoint.port_or_known_default() == Some(443)
            && matches!(
                endpoint.host_str(),
                Some("api.databend.com" | "api.databend.cn")
            );
        if endpoint.scheme() != "https"
            || !(official || additional.as_ref() == Some(&endpoint.origin()))
        {
            return Err(Error::BadArgument("cloud_api_token requires https://api.databend.com or https://api.databend.cn, not a Gateway or private server; authorize a staging HTTPS origin with BENDSQL_CLOUD_API_ORIGIN".into()));
        }
        Self::with_endpoint(endpoint, token, org)
    }

    fn with_endpoint(endpoint: Url, token: String, org: String) -> Result<Self> {
        let mut token_header = HeaderValue::from_str(&token)
            .map_err(|_| Error::BadArgument("invalid cloud_api_token header".into()))?;
        if token.trim().is_empty() {
            return Err(Error::BadArgument(
                "cloud_api_token must not be empty".into(),
            ));
        }
        if org.is_empty()
            || org.trim() != org
            || org.len() > 256
            || HeaderValue::from_str(&org).is_err()
        {
            return Err(Error::BadArgument(
                "Cloud API token authentication requires a valid org_slug".into(),
            ));
        }
        token_header.set_sensitive(true);
        Ok(Self {
            endpoint,
            token: token.into(),
            token_header,
            org,
        })
    }

    fn redact(&self, value: &mut String) {
        // Tokens travel only in headers, not URLs. Cover raw/JSON error echoes;
        // URL-encoded (or arbitrarily transformed) representations are not matched.
        *value = value.replace(self.token.inner(), "**REDACTED**");
        let quoted = serde_json::to_string(self.token.inner()).unwrap();
        *value = value.replace(&quoted[1..quoted.len() - 1], "**REDACTED**");
    }

    #[cfg(test)]
    pub(crate) fn for_test(endpoint: Url, token: String, org: String) -> Self {
        Self::with_endpoint(endpoint, token, org).unwrap()
    }
}

impl Auth for CloudApiAuth {
    fn wrap(&self, builder: RequestBuilder) -> Result<RequestBuilder> {
        let (client, request) = builder.build_split();
        let mut request = request?;
        if request.url().origin() != self.endpoint.origin() {
            return Err(Error::BadArgument(
                "refusing to send Cloud API credentials to another origin".into(),
            ));
        }
        request.headers_mut().remove(AUTHORIZATION);
        request
            .headers_mut()
            .insert(HEADER_CLOUD_TOKEN, self.token_header.clone());
        request
            .headers_mut()
            .insert(HEADER_CLOUD_ORG, HeaderValue::from_str(&self.org).unwrap());
        Ok(RequestBuilder::from_parts(client, request))
    }

    fn username(&self) -> String {
        "cloud_api_token".into()
    }

    fn sanitize_error(&self, error: Error) -> Error {
        match error {
            Error::WithContext {
                inner,
                request_kind,
                query_id,
                retry_times,
            } => Error::WithContext {
                inner: Box::new(self.sanitize_error(*inner)),
                request_kind,
                query_id,
                retry_times,
            },
            Error::Decode(mut message) => {
                self.redact(&mut message);
                Error::Decode(message)
            }
            Error::Response { status, mut msg } => {
                self.redact(&mut msg);
                Error::Response { status, msg }
            }
            Error::AuthFailure(mut code) => {
                self.redact(&mut code.message);
                if let Some(detail) = &mut code.detail {
                    self.redact(detail);
                }
                Error::AuthFailure(code)
            }
            Error::Logic(status, mut code) => {
                self.redact(&mut code.message);
                if let Some(detail) = &mut code.detail {
                    self.redact(detail);
                }
                Error::Logic(status, code)
            }
            Error::QueryFailed(mut code) => {
                self.redact(&mut code.message);
                if let Some(detail) = &mut code.detail {
                    self.redact(detail);
                }
                Error::QueryFailed(code)
            }
            other => other,
        }
    }
}
