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

fn establish_cloud_session(client: &APIClient, value: &str) {
    let mut headers = HeaderMap::new();
    headers.insert(HEADER_SESSION, HeaderValue::from_str(value).unwrap());
    client.transport.observe_response(&headers).unwrap();
}

#[tokio::test]
async fn cloud_lifecycle_requires_an_established_session_and_actual_work() {
    for invalid in [false, true] {
        let server =
            MockServer::new(|_| panic!("unestablished session must not use lifecycle APIs")).await;
        let client = client(&server).await;
        client.session_state.lock().need_sticky = Some(true);
        client.session_state.lock().need_keep_alive = Some(true);
        if invalid {
            let mut headers = HeaderMap::new();
            headers.insert(HEADER_SESSION, HeaderValue::from_static(""));
            assert!(client.transport.observe_response(&headers).is_err());
        }
        client.try_heartbeat().await.unwrap();
        assert!(!client.need_logout());
        client.close().await;
        assert!(server.requests.lock().is_empty());
    }
    let server =
        MockServer::new(|_| panic!("idle stateless session must not use lifecycle APIs")).await;
    let client = client(&server).await;
    establish_cloud_session(&client, "idle-session");
    client.try_heartbeat().await.unwrap();
    assert!(!client.need_logout());
    client.close().await;
    assert!(server.requests.lock().is_empty());
}

#[tokio::test]
async fn cloud_unread_query_heartbeat_refreshes_session_and_releases_finished_tracking() {
    let server = MockServer::new(|request| {
        assert_cloud_session_transport(request);
        if request.path == "/v1/session/heartbeat" {
            assert_eq!(request.method, "POST");
            assert_eq!(request.headers["x-databend-session"], "query-session");
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(body, json!({"node_to_queries":{"node-1":["q1"]}}));
            let mut reply = MockReply::json(json!({"queries_to_remove":["q1"]}));
            reply
                .headers
                .push((HEADER_SESSION, "heartbeat-session".into()));
            return reply;
        }
        let mut body = query_reply();
        body["node_id"] = json!("node-1");
        body["result_timeout_secs"] = json!(8);
        let mut reply = MockReply::json(body);
        if request.method == "POST" {
            reply.headers.push((HEADER_SESSION, "query-session".into()));
        } else {
            assert_eq!(request.headers["x-databend-session"], "heartbeat-session");
        }
        reply
    })
    .await;
    let client = Arc::new(client(&server).await);
    let pages = client.start_query("SELECT 1", false, None).await.unwrap();
    let (pages, _, _) = pages.wait_for_schema(false).await.unwrap();
    {
        let states = client.queries_need_heartbeat.lock();
        *states["q1"].last_access_time.lock() = Instant::now() - Duration::from_secs(8);
    }
    client.try_heartbeat().await.unwrap();
    assert!(client.queries_need_heartbeat.lock().is_empty());
    client
        .query_page("q1", "/v1/query/q1/page/0", &Some("node-1".into()))
        .await
        .unwrap();
    drop(pages);
    client.try_heartbeat().await.unwrap();
    client.close().await;
    assert_eq!(server.requests.lock().len(), 3);
}

#[tokio::test]
async fn cloud_sticky_heartbeat_and_logout_use_current_session_without_native_tokens() {
    let server = MockServer::new(|request| {
        assert_cloud_session_transport(request);
        assert_eq!(request.headers["x-databendcloud-token"], TOKEN);
        assert_eq!(request.headers["x-databendcloud-org"], ORG);
        assert_eq!(request.headers["x-databendcloud-warehouse"], "wh");
        assert_eq!(request.headers["x-databend-sticky-node"], "sticky-node");
        assert!(request.headers.contains_key("x-databend-route-hint"));
        assert_eq!(request.method, "POST");
        if request.path == "/v1/session/heartbeat" {
            assert_eq!(request.headers["x-databend-session"], "initial-session");
            assert_eq!(
                serde_json::from_slice::<Value>(&request.body).unwrap(),
                json!({"node_to_queries":{}})
            );
            let mut reply = MockReply::json(json!({"queries_to_remove":[]}));
            reply
                .headers
                .push((HEADER_SESSION, "refreshed-session".into()));
            reply
        } else {
            assert_eq!(request.path, "/v1/session/logout");
            assert_eq!(request.headers["x-databend-session"], "refreshed-session");
            assert!(request.body.is_empty());
            MockReply::json(json!({"error":null}))
        }
    })
    .await;
    let client = client(&server).await;
    establish_cloud_session(&client, "initial-session");
    client.session_state.lock().need_sticky = Some(true);
    client.session_state.lock().need_keep_alive = Some(true);
    client.set_last_node_id("sticky-node".into());
    client.try_heartbeat().await.unwrap();
    assert!(client.need_logout());
    let request = client.build_log_out_request().unwrap();
    assert!(request.headers()[HEADER_SESSION].is_sensitive());
    assert!(request.headers()[crate::cloud_api::HEADER_CLOUD_TOKEN].is_sensitive());
    assert_eq!(request.timeout(), Some(&client.page_request_timeout));
    assert!(!format!("{request:?}").contains("refreshed-session"));
    assert!(!format!("{request:?}").contains(TOKEN));
    client.close().await;
    client.close().await;
    client.close_with_spawn();
    assert!(client.session_token_info.is_none());
    assert_eq!(server.requests.lock().len(), 2);
}

#[tokio::test]
async fn cloud_spawned_close_logs_out_once_with_opaque_session() {
    let notified = Arc::new(tokio::sync::Notify::new());
    let notification = notified.clone();
    let server = MockServer::new(move |request| {
        assert_cloud_session_transport(request);
        assert_eq!(request.path, "/v1/session/logout");
        assert_eq!(request.headers["x-databend-session"], "drop-session");
        notification.notify_one();
        MockReply::json(json!({"error":null}))
    })
    .await;
    let client = client(&server).await;
    establish_cloud_session(&client, "drop-session");
    client.session_state.lock().need_keep_alive = Some(true);
    client.close_with_spawn();
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), notified.notified())
        .await
        .unwrap();
    assert_eq!(server.requests.lock().len(), 1);
}

#[tokio::test]
async fn cloud_heartbeat_safe_retries_use_refreshed_session() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let server = MockServer::new(move |request| {
        assert_cloud_session_transport(request);
        assert_eq!(request.path, "/v1/session/heartbeat");
        let attempt = seen.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            request.headers["x-databend-session"],
            if attempt == 0 {
                "initial-session"
            } else {
                "retry-session"
            }
        );
        let mut reply = if attempt == 0 {
            MockReply::error(503)
        } else {
            MockReply::json(json!({"queries_to_remove":[]}))
        };
        reply.headers.push((HEADER_SESSION, "retry-session".into()));
        reply
    })
    .await;
    let client = client(&server).await;
    establish_cloud_session(&client, "initial-session");
    client.session_state.lock().need_sticky = Some(true);
    client.try_heartbeat().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cloud_logout_failure_and_redirect_remain_best_effort_without_replay() {
    for status in [403, 503, 307] {
        let other = MockServer::new(|_| panic!("Cloud logout must not follow a redirect")).await;
        let location = other.url.to_string();
        let server = MockServer::new(move |request| {
            assert_cloud_session_transport(request);
            assert_eq!(request.path, "/v1/session/logout");
            let mut reply = MockReply::error(status);
            reply.headers.push(("Location", location.clone()));
            reply
        })
        .await;
        let client = client(&server).await;
        establish_cloud_session(&client, "logout-session");
        client.session_state.lock().need_keep_alive = Some(true);
        client.close().await;
        client.close().await;
        assert_eq!(server.requests.lock().len(), 1);
        assert!(other.requests.lock().is_empty());
    }
}
