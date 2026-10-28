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
use reqwest::header::{HeaderValue, AUTHORIZATION, COOKIE};
use reqwest::RequestBuilder;
use url::Url;

#[cfg(test)]
mod tests;

/// Default host for Cloud WebAPI API-token connections.
pub const DEFAULT_CLOUD_WEBAPI_HOST: &str = "api.databend.com";

const CLOUD_WEBAPI_HOSTS: [&str; 2] = [DEFAULT_CLOUD_WEBAPI_HOST, "api.databend.cn"];

/// Recognize an official Cloud WebAPI hostname (ASCII case-insensitive).
///
/// This only selects the authentication mode. It does not validate the URL's
/// scheme or port, or authorize forwarding credentials to another origin.
pub fn is_cloud_webapi_host(host: &str) -> bool {
    CLOUD_WEBAPI_HOSTS
        .iter()
        .any(|official| host.eq_ignore_ascii_case(official))
}

pub(crate) const HEADER_CLOUD_TOKEN: &str = "X-DATABENDCLOUD-TOKEN";
pub(crate) const HEADER_CLOUD_ORG: &str = "X-DATABENDCLOUD-ORG";
pub(crate) const HEADER_CLOUD_WAREHOUSE: &str = "X-DATABENDCLOUD-WAREHOUSE";

pub(crate) struct CloudApiAuth {
    endpoint: Url,
    token: SensitiveString,
    token_header: HeaderValue,
    org_header: HeaderValue,
}

impl CloudApiAuth {
    pub(crate) fn new(endpoint: Url, token: String, org: String) -> Result<Self> {
        let official = endpoint.port_or_known_default() == Some(443)
            && endpoint.host_str().is_some_and(is_cloud_webapi_host);
        if endpoint.scheme() != "https" {
            return Err(Error::BadArgument(
                "cloud_api_token requires HTTPS; sslmode=disable is not supported".into(),
            ));
        }
        if !official {
            let origins = CLOUD_WEBAPI_HOSTS
                .map(|host| format!("https://{host}"))
                .join(" or ");
            return Err(Error::BadArgument(format!(
                "cloud_api_token requires {origins}, not a Gateway or private server"
            )));
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
        if org.is_empty() || org.trim() != org || org.len() > 256 {
            return Err(Error::BadArgument(
                "Cloud API token authentication requires a valid org_slug".into(),
            ));
        }
        let org_header = HeaderValue::from_str(&org).map_err(|_| {
            Error::BadArgument("Cloud API token authentication requires a valid org_slug".into())
        })?;
        token_header.set_sensitive(true);
        Ok(Self {
            endpoint,
            token: token.into(),
            token_header,
            org_header,
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
        // Cloud SQL session state uses headers, never browser/SQL cookies.
        request.headers_mut().remove(COOKIE);
        request
            .headers_mut()
            .insert(HEADER_CLOUD_TOKEN, self.token_header.clone());
        request
            .headers_mut()
            .insert(HEADER_CLOUD_ORG, self.org_header.clone());
        Ok(RequestBuilder::from_parts(client, request))
    }

    fn username(&self) -> String {
        "cloud_account".into()
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
