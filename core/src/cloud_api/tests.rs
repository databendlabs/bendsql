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
fn additional_origin_is_exact_and_https_only() {
    let endpoint = Url::parse("https://staging.example:8443").unwrap();
    let auth = CloudApiAuth::with_allowed_origin(
        endpoint.clone(),
        "fake-token".into(),
        "org-example".into(),
        Some("https://staging.example:8443"),
    )
    .unwrap();
    let client = reqwest::Client::new();
    assert!(auth
        .wrap(client.get(endpoint.join("v1/query").unwrap()))
        .is_ok());
    for other in [
        "http://staging.example:8443",
        "https://staging.example",
        "https://other.example:8443",
    ] {
        assert!(auth.wrap(client.get(other)).is_err());
    }
    assert!(CloudApiAuth::with_allowed_origin(
        endpoint,
        "fake-token".into(),
        "org-example".into(),
        None
    )
    .is_err());
    for allowed in [
        "http://staging.example:8443",
        "https://user:secret@staging.example:8443",
        "https://staging.example/path",
        "https://staging.example?query",
        "https://staging.example#fragment",
        "https://staging.example:0",
        "invalid",
    ] {
        let err = CloudApiAuth::with_allowed_origin(
            Url::parse("https://api.databend.com").unwrap(),
            "fake-token".into(),
            "org-example".into(),
            Some(allowed),
        )
        .err()
        .unwrap();
        assert!(!format!("{err:?}").contains("secret"));
    }
}

#[test]
fn error_redaction_covers_raw_and_json_not_arbitrary_encodings() {
    let auth = CloudApiAuth::with_allowed_origin(
        Url::parse("https://api.databend.com").unwrap(),
        "fake\"token".into(),
        "org-example".into(),
        None,
    )
    .unwrap();
    let mut message = "raw fake\"token; JSON fake\\\"token".to_string();
    auth.redact(&mut message);
    assert_eq!(message, "raw **REDACTED**; JSON **REDACTED**");
}
