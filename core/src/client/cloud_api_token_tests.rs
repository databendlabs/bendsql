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
use crate::cloud_api_token::tests::{expire, mock_auth, token_reply, MockResponse, MockServer};
use std::sync::atomic::AtomicUsize;

#[tokio::test]
async fn cloud_dsn_ignores_password_and_rejects_conflicting_auth() {
    let base = "databend://cloudapp:%FF@tnexample.gw.aws-us-east-2.default.databend.com/default?cloud_api_token=dbc_fake";
    let client = APIClient::from_dsn(base).await.unwrap();
    assert!(client.auth.needs_prepare());
    assert_eq!(client.auth.username(), "cloud_api_token");
    let settings = client.session_state();
    assert!(!format!("{settings:?}").contains("dbc_fake"));
    for extra in [
        "access_token=secret",
        "access_token_file=missing",
        "private_key_file=missing",
        "private_key_passphrase_file=missing",
        "cloud_api_token=second",
        "tenant=tnother",
    ] {
        let err = APIClient::from_dsn(&format!("{base}&{extra}"))
            .await
            .err()
            .unwrap();
        assert!(
            err.to_string().contains("cloud_api_token") || err.to_string().contains("Cloud tenant")
        );
        assert!(!err.to_string().contains("dbc_fake"));
    }
    for dsn in [
        "databend://localhost?cloud_api_token=dbc_fake",
        "databend://tnexample.gw.aws-us-east-2.default.databend.com?cloud_api_token=dbc_fake&sslmode=disable",
        "databend+http://tnexample.gw.aws-us-east-2.default.databend.com?cloud_api_token=dbc_fake",
        "databend://tnexample.gw.aws-us-east-2.default.databend.com?cloud_api_token=",
    ] {
        assert!(APIClient::from_dsn(dsn).await.is_err());
    }
}

fn query_reply() -> Value {
    json!({"id":"q1","schema":[],"data":[],"state":"Succeeded",
        "stats":{"scan_progress":{"rows":0,"bytes":0},"write_progress":{"rows":0,"bytes":0},
                 "result_progress":{"rows":0,"bytes":0},"running_time_ms":0.0}})
}

async fn connected_mock(
    gateway: &MockServer,
    exchange: &MockServer,
) -> (Arc<APIClient>, Arc<CloudApiTokenAuth>) {
    let mut client = APIClient::from_dsn(&format!("databend://ignored:ignored@127.0.0.1:{}/default?sslmode=disable&presign=off&retry_delay_secs=0", gateway.url.port().unwrap())).await.unwrap();
    let auth = mock_auth(gateway.url.clone(), exchange.url.clone());
    client.auth = auth.clone();
    client.build_client(None).await.unwrap();
    client.auth.prepare().await.unwrap();
    client.login().await.unwrap();
    (Arc::new(client), auth)
}

#[tokio::test]
async fn cloud_gateway_uses_jwt_for_login_query_pages_and_logout() {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let exchange = MockServer::new(move |_| {
        MockResponse::json(token_reply(&format!(
            "jwt-{}",
            seen.fetch_add(1, Ordering::SeqCst)
        )))
    })
    .await;
    let gateway = MockServer::new(|request| {
        if request.target.starts_with("/v1/session/login") {
            MockResponse::json(json!({"version":"1.2.952"}))
        } else {
            MockResponse::json(query_reply())
        }
    })
    .await;
    let (client, auth) = connected_mock(&gateway, &exchange).await;
    assert_eq!(client.username(), "alice@example.com");
    expire(&auth);
    client
        .start_query_inner("SELECT 1", None, true, None)
        .await
        .unwrap();
    expire(&auth);
    client
        .query_request_helper(
            client
                .auth
                .wrap(
                    client
                        .cli
                        .get(gateway.url.join("v1/query/q1/page/0").unwrap()),
                )
                .unwrap()
                .build()
                .unwrap(),
            false,
            false,
            false,
            RequestKind::QueryPage,
        )
        .await
        .unwrap();
    expire(&auth);
    client.session_state.lock().need_keep_alive = Some(true);
    client.close().await;
    assert_eq!(count.load(Ordering::SeqCst), 4);
    let requests = gateway.requests.lock();
    assert_eq!(requests.len(), 4);
    for (i, request) in requests.iter().enumerate() {
        assert_eq!(request.headers["authorization"], format!("Bearer jwt-{i}"));
        assert!(!request.headers.contains_key("x-databendcloud-token"));
        assert!(!String::from_utf8_lossy(&request.body).contains("dbc_test_secret"));
        assert!(!String::from_utf8_lossy(&request.body).contains("ignored"));
    }
    assert!(requests
        .last()
        .unwrap()
        .target
        .starts_with("/v1/session/logout"));
}

#[tokio::test]
async fn cloud_logout_preserves_explicit_server_session_credentials() {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let exchange = MockServer::new(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
        MockResponse::json(token_reply("jwt-initial"))
    })
    .await;
    let gateway = MockServer::new(|_| MockResponse::json(json!({"version":"1.2.952"}))).await;
    let (mut client, auth) = connected_mock(&gateway, &exchange).await;
    Arc::get_mut(&mut client).unwrap().session_token_info = Some(Arc::new(Mutex::new((
        SessionTokenInfo {
            session_token: "session-token".into(),
            session_token_ttl_in_secs: 3600,
            refresh_token: "refresh-token".into(),
        },
        Instant::now(),
    ))));
    expire(&auth);
    client.close().await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(
        gateway.requests.lock().last().unwrap().headers["authorization"],
        "Bearer session-token"
    );
}

#[tokio::test]
async fn cloud_auth_failure_does_not_resubmit_sql_or_fall_back() {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let exchange = MockServer::new(move |_| {
        if seen.fetch_add(1, Ordering::SeqCst) == 0 {
            MockResponse::json(token_reply("jwt-initial"))
        } else {
            MockResponse {
                status: 403,
                body: "dbc_test_secret".into(),
                headers: vec![],
                delay: Duration::ZERO,
            }
        }
    })
    .await;
    let gateway = MockServer::new(|request| {
        if request.target.starts_with("/v1/session/login") {
            MockResponse::json(json!({"version":"1.2.952"}))
        } else {
            MockResponse {
                status: 401,
                body: r#"{"error":{"code":5100,"message":"authentication failed"}}"#.into(),
                headers: vec![],
                delay: Duration::ZERO,
            }
        }
    })
    .await;
    let (client, auth) = connected_mock(&gateway, &exchange).await;
    assert!(client
        .start_query_inner("INSERT INTO t VALUES(1)", None, true, None)
        .await
        .is_err());
    assert_eq!(gateway.requests.lock().len(), 2);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    expire(&auth);
    let err = client
        .start_query_inner("INSERT INTO t VALUES(2)", None, true, None)
        .await
        .err()
        .unwrap();
    assert_eq!(err.status_code(), Some(StatusCode::FORBIDDEN));
    assert_eq!(gateway.requests.lock().len(), 2);
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert!(!err.to_string().contains("dbc_test_secret"));
}
