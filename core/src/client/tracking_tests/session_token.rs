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
use std::sync::atomic::AtomicUsize;

// All credentials in this fixture are synthetic, not real server tokens.
const OLD_SESSION: &str = "test-old-session";
const OLD_REFRESH: &str = "test-old-refresh";
const NEW_SESSION: &str = "test-new-session";
const NEW_REFRESH: &str = "test-new-refresh";

fn token_info(session: &str, refresh: &str) -> SessionTokenInfo {
    SessionTokenInfo {
        session_token: session.into(),
        session_token_ttl_in_secs: 3600,
        refresh_token: refresh.into(),
    }
}

fn token_reply(session: &str, refresh: &str, wrapped: bool) -> MockReply {
    let tokens = json!({
        "session_token": session,
        "session_token_ttl_in_secs": 3600,
        "refresh_token": refresh,
    });
    MockReply::json(if wrapped {
        json!({"tokens": tokens})
    } else {
        tokens
    })
}

fn expired_reply(code: u16) -> MockReply {
    let mut reply = MockReply::json(json!({"error": {"code": code, "message": "expired"}}));
    reply.status = 401;
    reply
}

fn assert_authorization(request: &MockRequest, token: &str) {
    // Avoid printing credentials even if the assertion fails.
    assert!(request.headers.get("authorization") == Some(&format!("Bearer {token}")));
}

fn assert_refresh_request(request: &MockRequest, session: &str, refresh: &str) {
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/v1/session/refresh");
    assert_authorization(request, refresh);
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert!(body == json!({"session_token": session}));
}

fn assert_token_state(client: &APIClient, session: &str, refresh: &str) -> Instant {
    let state = client.session_token_info.as_ref().unwrap().lock();
    assert!(state.0.session_token == session);
    assert!(state.0.refresh_token == refresh);
    assert_eq!(state.0.session_token_ttl_in_secs, 3600);
    state.1
}

async fn session_client(server: &MockServer, acquired: Instant) -> Arc<APIClient> {
    let mut client = client(server).await;
    let inner = Arc::get_mut(&mut client).unwrap();
    inner.retry_delay_secs = 0;
    inner.session_token_info = Some(Arc::new(Mutex::new((
        token_info(OLD_SESSION, OLD_REFRESH),
        acquired,
    ))));
    client
}

#[tokio::test]
async fn session_token_refresh_on_401_rotates_both_tokens() {
    for wrapped in [true, false] {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let server = MockServer::new(move |request| match seen.fetch_add(1, Ordering::SeqCst) {
            index @ (0 | 3) => {
                assert_eq!(request.path, "/v1/query");
                let first = index == 0;
                assert_authorization(request, if first { OLD_SESSION } else { NEW_SESSION });
                // Cover both server error codes that permit a refresh.
                expired_reply(if first { 5101 } else { 5103 })
            }
            1 => {
                assert_refresh_request(request, OLD_SESSION, OLD_REFRESH);
                token_reply(NEW_SESSION, NEW_REFRESH, wrapped)
            }
            2 => {
                assert_eq!(request.path, "/v1/query");
                assert_authorization(request, NEW_SESSION);
                MockReply::json(query_reply(None, json!([["1"]])))
            }
            4 => {
                // The second refresh must use BOTH tokens saved by the first.
                assert_refresh_request(request, NEW_SESSION, NEW_REFRESH);
                token_reply("test-next-session", "test-next-refresh", wrapped)
            }
            5 => {
                assert_eq!(request.path, "/v1/query");
                assert_authorization(request, "test-next-session");
                MockReply::json(query_reply(None, json!([["1"]])))
            }
            _ => panic!("unexpected request after refresh"),
        })
        .await;
        let acquired = Instant::now() - Duration::from_secs(60);
        let client = session_client(&server, acquired).await;
        assert!(client.need_pre_refresh_session().await.is_none());
        let before = Instant::now();
        server
            .wait_for(client.start_query_inner("SELECT 1", None, false, None))
            .await
            .unwrap();
        let refreshed = assert_token_state(&client, NEW_SESSION, NEW_REFRESH);
        assert!(refreshed >= before);
        assert!(refreshed > acquired);
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        let before = Instant::now();
        server
            .wait_for(client.start_query_inner("SELECT 1", None, false, None))
            .await
            .unwrap();
        assert!(assert_token_state(&client, "test-next-session", "test-next-refresh") >= before);
        assert_eq!(server.requests.lock().len(), 6);
    }
}

#[tokio::test]
async fn session_token_pre_refresh_updates_upload_credentials() {
    let server = MockServer::new(|request| match request.path.as_str() {
        "/v1/session/refresh" => {
            assert_refresh_request(request, OLD_SESSION, OLD_REFRESH);
            token_reply(NEW_SESSION, NEW_REFRESH, true)
        }
        "/v1/upload_to_stage" => {
            assert_eq!(request.method, "PUT");
            assert_authorization(request, NEW_SESSION);
            MockReply::json(json!({}))
        }
        _ => panic!("unexpected request during pre-refresh"),
    })
    .await;
    // Expire the stored acquisition time rather than changing the advertised TTL.
    let acquired = Instant::now() - Duration::from_secs(7200);
    let client = session_client(&server, acquired).await;
    assert!(client.need_pre_refresh_session().await.is_some());
    let before = Instant::now();
    server
        .wait_for(client.upload_to_stage_with_stream(
            "@test/file",
            Box::new(std::io::Cursor::new(Vec::<u8>::new())),
            0,
        ))
        .await
        .unwrap();
    assert!(assert_token_state(&client, NEW_SESSION, NEW_REFRESH) >= before);
    assert!(client.need_pre_refresh_session().await.is_none());
    let requests = server.requests.lock();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].path, "/v1/session/refresh");
    assert_eq!(requests[1].path, "/v1/upload_to_stage");
}

#[tokio::test]
async fn session_token_refresh_failure_preserves_credentials_without_retrying() {
    for failure in ["error", "unauthorized", "missing", "null", "malformed"] {
        let server = MockServer::new(move |request| match request.path.as_str() {
            "/v1/query" => {
                assert_authorization(request, OLD_SESSION);
                expired_reply(5101)
            }
            "/v1/session/refresh" => {
                assert_refresh_request(request, OLD_SESSION, OLD_REFRESH);
                match failure {
                    "error" => {
                        MockReply::json(json!({"error": {"code": 5101, "message": "expired"}}))
                    }
                    // A refresh 401 must not recursively trigger another refresh.
                    "unauthorized" => expired_reply(5101),
                    "missing" => MockReply::json(json!({"tokens": {"session_token": NEW_SESSION}})),
                    "null" => MockReply::json(json!({"tokens": null})),
                    "malformed" => {
                        let mut reply = MockReply::json(json!({}));
                        reply.body = r#"{"tokens":"#.into();
                        reply
                    }
                    _ => unreachable!(),
                }
            }
            _ => panic!("unexpected request during failed refresh"),
        })
        .await;
        let acquired = Instant::now() - Duration::from_secs(60);
        let client = session_client(&server, acquired).await;
        let error = server
            .wait_for(client.start_query_inner("SELECT 1", None, false, None))
            .await
            .unwrap_err();
        match failure {
            "error" => assert!(matches!(error, Error::AuthFailure(_))),
            "unauthorized" => {
                let Error::WithContext {
                    inner,
                    request_kind: Some(RequestKind::SessionRefresh),
                    ..
                } = error
                else {
                    panic!("expected HTTP refresh error with request context");
                };
                assert!(matches!(*inner, Error::AuthFailure(_)));
            }
            _ => assert!(matches!(error, Error::Decode(_))),
        }
        assert_eq!(
            assert_token_state(&client, OLD_SESSION, OLD_REFRESH),
            acquired
        );
        let requests = server.requests.lock();
        assert_eq!(
            requests.len(),
            2,
            "failed refresh must not retry SQL or refresh"
        );
        assert_eq!(requests[0].path, "/v1/query");
        assert_eq!(requests[1].path, "/v1/session/refresh");
    }
}

#[tokio::test]
async fn session_token_repeated_401_stops_after_one_successful_refresh() {
    let server = MockServer::new(|request| match request.path.as_str() {
        "/v1/query" => expired_reply(5101),
        "/v1/session/refresh" => {
            assert_refresh_request(request, OLD_SESSION, OLD_REFRESH);
            token_reply(NEW_SESSION, NEW_REFRESH, true)
        }
        _ => panic!("unexpected request during repeated 401"),
    })
    .await;
    let acquired = Instant::now() - Duration::from_secs(60);
    let client = session_client(&server, acquired).await;
    assert!(server
        .wait_for(client.start_query_inner("SELECT 1", None, false, None))
        .await
        .is_err());
    assert!(assert_token_state(&client, NEW_SESSION, NEW_REFRESH) > acquired);
    let requests = server.requests.lock();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].path, "/v1/query");
    assert_authorization(&requests[0], OLD_SESSION);
    assert_eq!(requests[1].path, "/v1/session/refresh");
    assert_eq!(requests[2].path, "/v1/query");
    assert_authorization(&requests[2], NEW_SESSION);
}
