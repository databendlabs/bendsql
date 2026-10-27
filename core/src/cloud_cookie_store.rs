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

use cookie::Cookie;
use reqwest::cookie::{CookieStore, Jar};
use reqwest::header::HeaderValue;
use url::Url;

/// Per-connection SQL session state, never a browser/console authentication jar.
/// Exact-origin checks add a port boundary that normal cookie matching lacks.
pub(crate) struct CloudCookieStore {
    endpoint: Url,
    jar: Jar,
}

impl CloudCookieStore {
    pub(crate) fn new(endpoint: Url) -> Self {
        let jar = Jar::default();
        jar.add_cookie_str("cookie_enabled=true; Path=/", &endpoint);
        // Databend accepts client-generated SQL session IDs. Initialize all
        // state before the first request: older WebAPI proxies preserve only
        // one Set-Cookie header, otherwise paging can acquire a different ID.
        let session_id = uuid::Uuid::new_v4();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        jar.add_cookie_str(&format!("session_id={session_id}; Path=/"), &endpoint);
        jar.add_cookie_str(&format!("last_refresh_time={now}; Path=/"), &endpoint);
        Self { endpoint, jar }
    }
}

impl CookieStore for CloudCookieStore {
    fn set_cookies(&self, headers: &mut dyn Iterator<Item = &HeaderValue>, url: &Url) {
        if url.origin() != self.endpoint.origin() {
            return;
        }
        for header in headers {
            // Two fixed names and one host/root-path slot per name bound storage.
            if header.as_bytes().len() > 4096 {
                continue;
            }
            let Some(mut cookie) = header.to_str().ok().and_then(|s| Cookie::parse(s).ok()) else {
                continue;
            };
            if !matches!(cookie.name(), "session_id" | "last_refresh_time") {
                continue;
            }
            // Databend's SQL cookies use Path=/. Never broaden an explicitly
            // narrower path or accept a parent/foreign domain from WebAPI.
            if cookie.path().is_some_and(|path| path != "/")
                || cookie.domain().is_some_and(|domain| {
                    !self
                        .endpoint
                        .host_str()
                        .is_some_and(|host| host.eq_ignore_ascii_case(domain))
                })
            {
                continue;
            }
            cookie.unset_domain();
            cookie.set_path("/");
            // Jar enforces Secure, expiry and Max-Age (including deletion).
            self.jar.add_cookie_str(&cookie.to_string(), url);
        }
    }

    fn cookies(&self, url: &Url) -> Option<HeaderValue> {
        if url.origin() != self.endpoint.origin() {
            return None;
        }
        self.jar.cookies(url).map(|mut header| {
            header.set_sensitive(true);
            header
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint() -> Url {
        Url::parse("https://api.databend.com/").unwrap()
    }

    fn set(store: &CloudCookieStore, url: &Url, values: &[&str]) {
        let headers: Vec<_> = values
            .iter()
            .map(|value| HeaderValue::from_str(value).unwrap())
            .collect();
        store.set_cookies(&mut headers.iter(), url);
    }

    fn values(store: &CloudCookieStore, url: &Url) -> Vec<String> {
        let header = store.cookies(url).unwrap();
        assert!(header.is_sensitive());
        let mut values: Vec<_> = header
            .to_str()
            .unwrap()
            .split("; ")
            .map(str::to_string)
            .collect();
        values.sort();
        values
    }

    #[test]
    fn only_sql_session_cookies_are_stored_and_connections_are_isolated() {
        let first = CloudCookieStore::new(endpoint());
        let second = CloudCookieStore::new(endpoint());
        let original_second = values(&second, &endpoint());
        assert_ne!(values(&first, &endpoint()), original_second);
        assert_eq!(original_second.len(), 3);
        set(
            &first,
            &endpoint(),
            &[
                "session_id=sql-session; Path=/; Secure; HttpOnly",
                "last_refresh_time=123; Path=/",
                "console_auth=secret; Path=/",
                "access_token=secret; Path=/",
                "cookie_enabled=false; Path=/",
            ],
        );
        assert_eq!(
            values(&first, &endpoint()),
            [
                "cookie_enabled=true",
                "last_refresh_time=123",
                "session_id=sql-session"
            ]
        );
        assert_eq!(values(&second, &endpoint()), original_second);
    }

    #[test]
    fn cookies_cannot_be_sent_or_set_across_origin_boundaries() {
        let store = CloudCookieStore::new(endpoint());
        let initial = values(&store, &endpoint());
        for url in [
            "http://api.databend.com",
            "https://api.databend.cn",
            "https://api.databend.com:8443",
            "https://sub.api.databend.com",
        ] {
            let other = Url::parse(url).unwrap();
            set(&store, &other, &["session_id=foreign; Path=/"]);
            assert!(store.cookies(&other).is_none());
        }
        assert_eq!(values(&store, &endpoint()), initial);
        set(
            &store,
            &endpoint(),
            &["session_id=local; Path=/", "last_refresh_time=123; Path=/"],
        );
        assert_eq!(
            values(&store, &endpoint().join("v1/query/q1/page/0").unwrap()),
            [
                "cookie_enabled=true",
                "last_refresh_time=123",
                "session_id=local"
            ]
        );
    }

    #[test]
    fn foreign_domains_narrow_paths_and_oversized_cookies_are_ignored() {
        let store = CloudCookieStore::new(endpoint());
        let initial = values(&store, &endpoint());
        set(
            &store,
            &endpoint(),
            &[
                "session_id=foreign; Domain=other.example; Path=/",
                "session_id=parent; Domain=databend.com; Path=/",
                "session_id=narrow; Path=/v1/query",
                "invalid-cookie",
            ],
        );
        set(
            &store,
            &endpoint(),
            &[&format!("session_id={}; Path=/", "x".repeat(4096))],
        );
        assert_eq!(values(&store, &endpoint()), initial);
        set(
            &store,
            &endpoint(),
            &[
                "session_id=valid; Domain=api.databend.com; Path=/",
                "last_refresh_time=123; Path=/",
            ],
        );
        assert_eq!(
            values(&store, &endpoint()),
            [
                "cookie_enabled=true",
                "last_refresh_time=123",
                "session_id=valid"
            ]
        );
    }

    #[test]
    fn session_cookies_can_be_rotated_expired_and_deleted() {
        let store = CloudCookieStore::new(endpoint());
        set(
            &store,
            &endpoint(),
            &[
                "session_id=first; Path=/",
                "session_id=second; Path=/",
                "last_refresh_time=123; Path=/",
            ],
        );
        assert_eq!(
            values(&store, &endpoint()),
            [
                "cookie_enabled=true",
                "last_refresh_time=123",
                "session_id=second"
            ]
        );
        set(
            &store,
            &endpoint(),
            &[
                "session_id=deleted; Max-Age=0; Path=/",
                "last_refresh_time=deleted; Max-Age=0; Path=/",
            ],
        );
        assert_eq!(values(&store, &endpoint()), ["cookie_enabled=true"]);
        set(
            &store,
            &endpoint(),
            &["session_id=expired; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Path=/"],
        );
        assert_eq!(values(&store, &endpoint()), ["cookie_enabled=true"]);
    }
}
