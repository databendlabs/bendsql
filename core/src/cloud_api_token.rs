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
use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use reqwest::header::{HeaderValue, AUTHORIZATION};
use reqwest::{Client, RequestBuilder};
use serde::Deserialize;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use url::Url;

#[cfg(test)]
pub(crate) mod tests;

const EXCHANGE_PATH: &str = "/api/v1/auth/token/exchange";
const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_SHARED_AUTHS: usize = 256;

// Weak references share refreshes across connection pools without retaining an
// API token after its last connection is dropped. No credential is a map key.
static SHARED_AUTHS: Lazy<Mutex<Vec<Weak<CloudApiTokenAuth>>>> =
    Lazy::new(|| Mutex::new(Vec::new()));

struct CachedToken {
    authorization: HeaderValue,
    username: String,
    refresh_at: Instant,
}

pub(crate) struct CloudApiTokenAuth {
    api_token: SensitiveString,
    tenant_id: String,
    gateway: Url,
    exchange_endpoint: Url,
    client: Client,
    cached: Mutex<Option<CachedToken>>,
    refresh: tokio::sync::Mutex<()>,
}

#[derive(Deserialize)]
struct ExchangeResponse {
    success: bool,
    data: ExchangeData,
}

#[derive(Deserialize)]
struct ExchangeData {
    access_token: SensitiveString,
    token_type: String,
    expires_at: String,
    expires_in: u64,
    tenant_id: String,
    username: String,
}

impl CloudApiTokenAuth {
    pub(crate) fn for_gateway(gateway: Url, api_token: String) -> Result<Arc<Self>> {
        let (tenant_id, exchange_endpoint) = Self::target(&gateway)?;
        Self::validate_api_token(&api_token)?;
        let mut shared = SHARED_AUTHS.lock();
        shared.retain(|entry| entry.strong_count() > 0);
        for entry in shared.iter().filter_map(Weak::upgrade) {
            if entry.gateway == gateway && entry.api_token.inner() == api_token {
                return Ok(entry);
            }
        }
        let auth = Arc::new(Self::new(gateway, tenant_id, exchange_endpoint, api_token)?);
        if shared.len() < MAX_SHARED_AUTHS {
            shared.push(Arc::downgrade(&auth));
        }
        Ok(auth)
    }

    pub(crate) fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    fn validate_api_token(token: &str) -> Result<()> {
        if token.trim().is_empty() || HeaderValue::from_str(token).is_err() {
            return Err(Error::BadArgument(
                "cloud_api_token must be a nonempty HTTP header value".into(),
            ));
        }
        Ok(())
    }

    fn target(gateway: &Url) -> Result<(String, Url)> {
        if gateway.scheme() != "https" {
            return Err(Error::BadArgument(
                "cloud_api_token requires HTTPS; remove sslmode=disable".into(),
            ));
        }
        let host = gateway.host_str().unwrap_or_default();
        let (label, domain) = host.split_once('.').ok_or_else(|| {
            Error::BadArgument(
                "cloud_api_token requires a standard Databend Cloud Gateway host".into(),
            )
        })?;
        let labels: Vec<_> = domain.split('.').collect();
        if labels.len() != 5 || labels[0] != "gw" || labels[1].is_empty() || labels[2].is_empty() {
            return Err(Error::BadArgument(
                "cloud_api_token requires a standard Databend Cloud Gateway host".into(),
            ));
        }
        let api_host = if domain.ends_with(".databend.com") {
            "api.databend.com"
        } else if domain.ends_with(".databend.cn") {
            "api.databend.cn"
        } else {
            return Err(Error::BadArgument(
                "cloud_api_token requires a standard Databend Cloud Gateway host".into(),
            ));
        };
        let tenant = label.split_once("--").map_or(label, |(tenant, _)| tenant);
        if !tenant.starts_with("tn")
            || tenant.len() <= 2
            || !tenant
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        {
            return Err(Error::BadArgument(
                "cannot determine the Cloud tenant from the Gateway host".into(),
            ));
        }
        let endpoint = Url::parse(&format!("https://{api_host}{EXCHANGE_PATH}"))?;
        Ok((tenant.to_string(), endpoint))
    }

    fn new(
        gateway: Url,
        tenant_id: String,
        exchange_endpoint: Url,
        api_token: String,
    ) -> Result<Self> {
        Self::validate_api_token(&api_token)?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .timeout(EXCHANGE_TIMEOUT)
            .user_agent(concat!("databend-client-rust/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            api_token: api_token.into(),
            tenant_id,
            gateway,
            exchange_endpoint,
            client,
            cached: Mutex::new(None),
            refresh: tokio::sync::Mutex::new(()),
        })
    }

    fn fresh(&self) -> bool {
        self.cached
            .lock()
            .as_ref()
            .is_some_and(|token| Instant::now() < token.refresh_at)
    }

    async fn exchange(&self) -> Result<CachedToken> {
        let started = Instant::now();
        let mut secret = HeaderValue::from_str(self.api_token.inner())
            .map_err(|_| Error::BadArgument("invalid cloud_api_token header".into()))?;
        secret.set_sensitive(true);
        let mut response = self
            .client
            .post(self.exchange_endpoint.clone())
            .header("X-DatabendCloud-Token", secret)
            .json(&serde_json::json!({"tenant_id": self.tenant_id}))
            .send()
            .await
            .map_err(|e| {
                Error::Request(format!(
                    "Cloud API token exchange failed: {}",
                    e.without_url()
                ))
            })?;
        let status = response.status();
        if !status.is_success() {
            let retry = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());
            let msg = match retry {
                Some(seconds) => {
                    format!("Cloud API token exchange rejected; retry after {seconds} seconds")
                }
                None => {
                    "Cloud API token exchange rejected; check the token and organization membership"
                        .into()
                }
            };
            // Never echo response bodies: an upstream error can reflect credentials.
            return Err(Error::Response { status, msg });
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| Error::Request("failed to read Cloud token exchange response".into()))?
        {
            if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                return Err(Error::Decode(
                    "Cloud token exchange response is too large".into(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        let reply: ExchangeResponse = serde_json::from_slice(&body)
            .map_err(|_| Error::Decode("invalid Cloud token exchange response".into()))?;
        let data = reply.data;
        if !reply.success
            || data.tenant_id != self.tenant_id
            || data.username.is_empty()
            || !data.token_type.eq_ignore_ascii_case("Bearer")
            || data.access_token.inner().is_empty()
        {
            return Err(Error::Decode(
                "Cloud token exchange returned invalid identity or credential metadata".into(),
            ));
        }
        let expires_at = DateTime::parse_from_rfc3339(&data.expires_at)
            .map_err(|_| Error::Decode("Cloud token exchange returned an invalid expiry".into()))?;
        let absolute = (expires_at.with_timezone(&Utc) - Utc::now())
            .to_std()
            .map_err(|_| {
                Error::Decode("Cloud token exchange returned an expired credential".into())
            })?;
        let relative = Duration::from_secs(data.expires_in).saturating_sub(started.elapsed());
        let lifetime = absolute
            .min(relative)
            .min(Duration::from_secs(24 * 60 * 60));
        if lifetime.is_zero() {
            return Err(Error::Decode(
                "Cloud token exchange returned an expired credential".into(),
            ));
        }
        let advance = Duration::from_secs(60).min(lifetime / 2);
        let mut authorization = HeaderValue::from_str(&format!(
            "Bearer {}",
            data.access_token.inner()
        ))
        .map_err(|_| {
            Error::Decode("Cloud token exchange returned an invalid credential header".into())
        })?;
        authorization.set_sensitive(true);
        Ok(CachedToken {
            authorization,
            username: data.username,
            refresh_at: Instant::now() + lifetime.saturating_sub(advance),
        })
    }
}

#[async_trait::async_trait]
impl Auth for CloudApiTokenAuth {
    async fn prepare(&self) -> Result<()> {
        if self.fresh() {
            return Ok(());
        }
        tokio::time::timeout(EXCHANGE_TIMEOUT, async {
            let _guard = self.refresh.lock().await;
            if !self.fresh() {
                let token = self.exchange().await?;
                *self.cached.lock() = Some(token);
            }
            Ok(())
        })
        .await
        .unwrap_or_else(|_| Err(Error::Request("Cloud API token exchange timed out".into())))
        .map_err(|e: Error| e.with_context("cloud/token/exchange"))
    }

    fn needs_prepare(&self) -> bool {
        true
    }

    fn wrap(&self, builder: RequestBuilder) -> Result<RequestBuilder> {
        let (client, request) = builder.build_split();
        let mut request = request?;
        if request.url().origin() != self.gateway.origin() {
            return Err(Error::BadArgument(
                "refusing to send Cloud SQL credentials to a different origin".into(),
            ));
        }
        let cached = self.cached.lock();
        let token = cached.as_ref().ok_or_else(|| {
            Error::Request("Cloud API token authentication is not initialized".into())
        })?;
        request
            .headers_mut()
            .insert(AUTHORIZATION, token.authorization.clone());
        Ok(RequestBuilder::from_parts(client, request))
    }

    fn username(&self) -> String {
        self.cached
            .lock()
            .as_ref()
            .map_or_else(|| "cloud_api_token".into(), |token| token.username.clone())
    }
}
