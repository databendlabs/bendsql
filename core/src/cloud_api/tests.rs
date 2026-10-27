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

use super::*;

#[test]
fn official_origins_are_exact_and_https_only() {
    let endpoint = Url::parse("https://api.databend.com").unwrap();
    let auth =
        CloudApiAuth::new(endpoint.clone(), "fake-token".into(), "org-example".into()).unwrap();
    let client = reqwest::Client::new();
    assert!(auth
        .wrap(client.get(endpoint.join("v1/query").unwrap()))
        .is_ok());
    for other in [
        "http://api.databend.com",
        "https://api.databend.com:8443",
        "https://api.databend.cn",
        "https://staging.example",
    ] {
        assert!(auth.wrap(client.get(other)).is_err());
    }
    assert!(CloudApiAuth::new(
        Url::parse("https://api.databend.cn").unwrap(),
        "fake-token".into(),
        "org-example".into()
    )
    .is_ok());
    for endpoint in [
        "http://api.databend.com",
        "https://api.databend.com:8443",
        "https://localhost",
        "https://staging.example",
    ] {
        assert!(CloudApiAuth::new(
            Url::parse(endpoint).unwrap(),
            "fake-token".into(),
            "org-example".into()
        )
        .is_err());
    }
}

#[test]
fn explicit_browser_cookies_and_bearer_credentials_are_removed() {
    let auth = CloudApiAuth::new(
        Url::parse("https://api.databend.com").unwrap(),
        "fake-token".into(),
        "org-example".into(),
    )
    .unwrap();
    let request = auth
        .wrap(
            reqwest::Client::new()
                .get("https://api.databend.com/v1/query")
                .header(
                    COOKIE,
                    "access_token=console-secret; session_id=not-from-scoped-store",
                )
                .bearer_auth("console-secret"),
        )
        .unwrap()
        .build()
        .unwrap();
    assert!(!request.headers().contains_key(COOKIE));
    assert!(!request.headers().contains_key(AUTHORIZATION));
    assert_eq!(request.headers()[HEADER_CLOUD_TOKEN], "fake-token");
}

#[test]
fn error_redaction_covers_raw_and_json_not_arbitrary_encodings() {
    let auth = CloudApiAuth::new(
        Url::parse("https://api.databend.com").unwrap(),
        "fake\"token".into(),
        "org-example".into(),
    )
    .unwrap();
    let mut message = "raw fake\"token; JSON fake\\\"token".to_string();
    auth.redact(&mut message);
    assert_eq!(message, "raw **REDACTED**; JSON **REDACTED**");
}
