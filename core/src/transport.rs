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

use std::sync::Arc;

use parking_lot::Mutex;
use reqwest::cookie::CookieStore;
use reqwest::header::{HeaderMap, HeaderValue, COOKIE};
use reqwest::{ClientBuilder, Method, Request, StatusCode};
use tokio::sync::{Mutex as AsyncMutex, MutexGuard};
use url::Url;

use crate::cloud_api::HEADER_CLOUD_WAREHOUSE;
use crate::error::{Error, RequestKind, Result};
use crate::global_cookie_store::GlobalCookieStore;
use crate::response::QueryResponse;

pub(crate) const HEADER_CLIENT_CAPS: &str = "X-DATABEND-CLIENT-CAPS";
pub(crate) const HEADER_SESSION: &str = "X-DATABEND-SESSION";

/// Protocol policy and connection-local transport state, not credentials.
/// Auth remains responsible for origin-scoped credentials and sanitization.
#[derive(Default)]
pub(crate) enum Transport {
    #[default]
    Native,
    CloudWebApi {
        session: Mutex<CloudSession>,
        session_init: AsyncMutex<()>,
    },
}

pub(crate) enum CloudSession {
    Pending,
    Ready(HeaderValue),
    Unavailable,
}

fn session_unavailable() -> Error {
    Error::Decode("Cloud WebAPI requires session_header support: the backend did not provide a valid X-DATABEND-SESSION header; this connection cannot continue. The query may already have executed; check its outcome before retrying".into())
}

pub(crate) enum UploadKind {
    Stage,
    Streaming,
}

impl Transport {
    pub(crate) fn cloud() -> Self {
        Self::CloudWebApi {
            session: Mutex::new(CloudSession::Pending),
            session_init: AsyncMutex::new(()),
        }
    }

    pub(crate) fn is_cloud_webapi(&self) -> bool {
        matches!(self, Self::CloudWebApi { .. })
    }

    pub(crate) fn configure_client(&self, builder: ClientBuilder) -> ClientBuilder {
        match self {
            Self::Native => {
                let cookies = GlobalCookieStore::new();
                let cookie = HeaderValue::from_static("cookie_enabled=true");
                cookies.set_cookies(
                    &mut [&cookie].into_iter(),
                    &Url::parse("https://a.com").unwrap(),
                );
                builder.cookie_provider(Arc::new(cookies))
            }
            Self::CloudWebApi { .. } => builder.redirect(reqwest::redirect::Policy::none()),
        }
    }

    pub(crate) fn warehouse_header(&self) -> &'static str {
        match self {
            Self::Native => "X-DATABEND-WAREHOUSE",
            Self::CloudWebApi { .. } => HEADER_CLOUD_WAREHOUSE,
        }
    }

    pub(crate) fn end_query_method(&self, is_kill: bool) -> Method {
        match (self, is_kill) {
            (Self::CloudWebApi { .. }, false) => Method::GET,
            (Self::Native, _) | (Self::CloudWebApi { .. }, true) => Method::POST,
        }
    }

    pub(crate) fn uses_native_session_apis(&self) -> bool {
        match self {
            Self::Native => true,
            Self::CloudWebApi { .. } => false,
        }
    }

    pub(crate) fn retry_query_termination(&self) -> bool {
        match self {
            Self::Native => false,
            Self::CloudWebApi { .. } => true,
        }
    }

    pub(crate) fn supports_presign(&self) -> bool {
        match self {
            Self::Native => true,
            Self::CloudWebApi { .. } => false,
        }
    }

    pub(crate) fn validate_query_payload(
        &self,
        has_attachment: bool,
        has_params: bool,
    ) -> Result<()> {
        match self {
            Self::CloudWebApi { .. } if has_attachment || has_params => Err(Error::BadArgument(
                "Cloud WebAPI does not support stage attachments or bound parameters".into(),
            )),
            Self::Native | Self::CloudWebApi { .. } => Ok(()),
        }
    }

    pub(crate) fn validate_upload(&self, kind: UploadKind) -> Result<()> {
        match self {
            Self::Native => Ok(()),
            Self::CloudWebApi { .. } => Err(Error::BadArgument(
                match kind {
                    UploadKind::Stage => "stage upload is not supported through Cloud WebAPI",
                    UploadKind::Streaming => "streaming load is not supported through Cloud WebAPI",
                }
                .into(),
            )),
        }
    }

    pub(crate) fn retry_page_503(&self) -> bool {
        match self {
            Self::Native => false,
            Self::CloudWebApi { .. } => true,
        }
    }

    pub(crate) fn allow_replay(&self, kind: &RequestKind) -> bool {
        match self {
            Self::Native => true,
            Self::CloudWebApi { .. } => !matches!(kind, RequestKind::QueryStart),
        }
    }

    pub(crate) fn body_error_fallback(&self) -> Option<&'static str> {
        match self {
            Self::Native => None,
            Self::CloudWebApi { .. } => Some("response body error"),
        }
    }

    pub(crate) fn is_org_not_found(&self, status: StatusCode, body: &[u8]) -> bool {
        match self {
            Self::Native => false,
            Self::CloudWebApi { .. } => {
                status == StatusCode::NOT_FOUND
                    && serde_json::from_slice::<serde_json::Value>(body).is_ok_and(|value| {
                        value.get("error").and_then(serde_json::Value::as_str)
                            == Some("OrgNotFound")
                    })
            }
        }
    }

    /// Serialize only bootstrap, until one server-issued session is known.
    pub(crate) async fn bootstrap_guard(&self) -> Option<MutexGuard<'_, ()>> {
        if let Self::CloudWebApi {
            session,
            session_init,
        } = self
        {
            if matches!(*session.lock(), CloudSession::Pending) {
                let guard = session_init.lock().await;
                if matches!(*session.lock(), CloudSession::Pending) {
                    return Some(guard);
                }
            }
        }
        None
    }

    pub(crate) fn prepare_request(&self, request: &mut Request, endpoint: &Url) -> Result<()> {
        if let Self::CloudWebApi { session, .. } = self {
            let session = session.lock();
            if matches!(*session, CloudSession::Unavailable) {
                return Err(session_unavailable());
            }
            if request.url().origin() != endpoint.origin() {
                return Err(Error::BadArgument(
                    "refusing to send Cloud session state to another origin".into(),
                ));
            }
            request.headers_mut().remove(COOKIE);
            request.headers_mut().insert(
                HEADER_CLIENT_CAPS,
                HeaderValue::from_static("session_header"),
            );
            request.headers_mut().remove(HEADER_SESSION);
            if let CloudSession::Ready(header) = &*session {
                request.headers_mut().insert(HEADER_SESSION, header.clone());
            }
        }
        Ok(())
    }

    pub(crate) fn observe_response(&self, headers: &HeaderMap) -> Result<()> {
        if let Self::CloudWebApi { session, .. } = self {
            let mut session = session.lock();
            if matches!(*session, CloudSession::Unavailable) {
                return Err(session_unavailable());
            }
            let mut values = headers.get_all(HEADER_SESSION).iter();
            if let Some(header) = values.next() {
                // Opaque and bounded: never decode or synthesize session IDs.
                if header.is_empty()
                    || header.as_bytes().len() > 8192
                    || header.to_str().is_err()
                    || values.next().is_some()
                {
                    if matches!(*session, CloudSession::Pending) {
                        *session = CloudSession::Unavailable;
                    }
                    return Err(Error::Decode(
                        "invalid Cloud session response header".into(),
                    ));
                }
                let mut header = header.clone();
                header.set_sensitive(true);
                *session = CloudSession::Ready(header);
            }
        }
        Ok(())
    }

    /// Check bootstrap while its guard is still held, before a waiting query can
    /// submit SQL. Missing headers on login/errors must not mask those responses.
    pub(crate) fn validate_query_session(
        &self,
        status: StatusCode,
        kind: &RequestKind,
        body: &[u8],
    ) -> Result<()> {
        if status != StatusCode::OK
            || !matches!(kind, RequestKind::QueryStart | RequestKind::QueryPage)
        {
            return Ok(());
        }
        if let Self::CloudWebApi { session, .. } = self {
            let mut session = session.lock();
            if matches!(*session, CloudSession::Pending) {
                // Decode only the header-less bootstrap case. Normal responses
                // are decoded once by APIClient; malformed/SQL errors keep their
                // original diagnostics rather than becoming capability errors.
                if let Ok(response) = serde_json::from_slice::<QueryResponse>(body) {
                    if response.error.is_none() {
                        *session = CloudSession::Unavailable;
                        return Err(session_unavailable());
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_policy_keeps_native_and_cloud_semantics_separate() {
        for (mode, cloud) in [(Transport::Native, false), (Transport::cloud(), true)] {
            assert_eq!(mode.is_cloud_webapi(), cloud);
            assert_eq!(
                mode.end_query_method(false),
                if cloud { Method::GET } else { Method::POST }
            );
            assert_eq!(mode.end_query_method(true), Method::POST);
            assert_eq!(mode.retry_page_503(), cloud);
            assert_eq!(mode.retry_query_termination(), cloud);
            assert_eq!(mode.allow_replay(&RequestKind::QueryStart), !cloud);
            assert!(mode.allow_replay(&RequestKind::QueryPage));
            assert!(mode.allow_replay(&RequestKind::QueryFinal));
            assert_eq!(mode.supports_presign(), !cloud);
            assert_eq!(mode.uses_native_session_apis(), !cloud);
            assert!(mode.validate_query_payload(false, false).is_ok());
            assert_eq!(mode.validate_query_payload(true, false).is_err(), cloud);
            assert_eq!(mode.validate_query_payload(false, true).is_err(), cloud);
            assert_eq!(mode.validate_upload(UploadKind::Stage).is_err(), cloud);
            assert_eq!(mode.validate_upload(UploadKind::Streaming).is_err(), cloud);
            assert_eq!(
                mode.warehouse_header(),
                if cloud {
                    HEADER_CLOUD_WAREHOUSE
                } else {
                    "X-DATABEND-WAREHOUSE"
                }
            );
        }
    }
}
