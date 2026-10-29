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
use crate::transport::{HEADER_CLIENT_CAPS, HEADER_SESSION};
use std::sync::atomic::AtomicUsize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::{JoinHandle, JoinSet};

const TOKEN: &str = "dbc_test_secret";
const ORG: &str = "org-example";

struct MockRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn assert_cloud_session_transport(request: &MockRequest) {
    assert_eq!(request.headers["x-databend-client-caps"], "session_header");
    assert!(!request.headers.contains_key("cookie"));
    assert!(!request.headers.contains_key("authorization"));
}

struct MockReply {
    status: u16,
    body: String,
    headers: Vec<(&'static str, String)>,
}

impl MockReply {
    fn json(body: Value) -> Self {
        Self {
            status: 200,
            body: body.to_string(),
            headers: vec![],
        }
    }
    fn without_session(mut self) -> Self {
        self.headers.push(("X-Mock-No-Session", "1".into()));
        self
    }

    fn error(status: u16) -> Self {
        Self {
            status,
            body: format!("rejected {TOKEN}"),
            headers: vec![],
        }
    }
}

struct MockServer {
    url: Url,
    requests: Arc<Mutex<Vec<MockRequest>>>,
    task: JoinHandle<()>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl MockServer {
    async fn new(handler: impl Fn(&MockRequest) -> MockReply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let handler = Arc::new(handler);
        let task = tokio::spawn(async move {
            let mut workers = JoinSet::new();
            loop {
                tokio::select! {
                    connection = listener.accept() => {
                        let Ok((mut stream, _)) = connection else { break };
                        let captured = captured.clone();
                        let handler = handler.clone();
                        workers.spawn(async move {
                            let mut bytes = Vec::new();
                            let (end, length) = loop {
                                let mut chunk = [0; 4096];
                                let Ok(count) = stream.read(&mut chunk).await else { return };
                                if count == 0 { return; }
                                bytes.extend_from_slice(&chunk[..count]);
                                assert!(bytes.len() < 128 * 1024);
                                if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                                    let head = String::from_utf8_lossy(&bytes[..end]);
                                    let length = head.lines().filter_map(|line| line.split_once(':'))
                                        .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                                        .map_or(0, |(_, value)| value.trim().parse::<usize>().unwrap());
                                    break (end + 4, length);
                                }
                            };
                            while bytes.len() < end + length {
                                let mut chunk = [0; 4096];
                                let Ok(count) = stream.read(&mut chunk).await else { return };
                                if count == 0 { return; }
                                bytes.extend_from_slice(&chunk[..count]);
                            }
                            let head = String::from_utf8_lossy(&bytes[..end]);
                            let mut first = head.lines().next().unwrap().split_whitespace();
                            let request = MockRequest {
                                method: first.next().unwrap().into(), path: first.next().unwrap().into(),
                                headers: head.lines().skip(1).filter_map(|line| line.split_once(':'))
                                    .map(|(key, value)| (key.to_ascii_lowercase(), value.trim().to_string())).collect(),
                                body: bytes[end..end + length].to_vec(),
                            };
                            let mut reply = handler(&request);
                            // Model a session-header-capable backend by default;
                            // tests of unsupported backends opt out explicitly.
                            if request.headers.get("x-databend-client-caps").map(String::as_str) == Some("session_header")
                                && reply.status == 200
                                && !reply.headers.iter().any(|(key, _)| key.eq_ignore_ascii_case(HEADER_SESSION) || *key == "X-Mock-No-Session") {
                                reply.headers.push((HEADER_SESSION, "opaque-default-session".into()));
                            }
                            captured.lock().push(request);
                            if let Some((_, delay)) = reply.headers.iter().find(|(name, _)| *name == "X-Mock-Delay") {
                                tokio::time::sleep(Duration::from_millis(delay.parse().unwrap())).await;
                            }
                            let length = reply.body.len() + if reply.headers.iter().any(|(name, _)| *name == "X-Mock-Truncate") { 100 } else { 0 };
                            let mut response = format!("HTTP/1.1 {} Mock\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n", reply.status, length);
                            for (key, value) in reply.headers {
                                if key != "X-Mock-No-Session" { response.push_str(&format!("{key}: {value}\r\n")); }
                            }
                            response.push_str("\r\n"); response.push_str(&reply.body);
                            let _ = stream.write_all(response.as_bytes()).await;
                        });
                    },
                    _ = workers.join_next(), if !workers.is_empty() => {},
                }
            }
        });
        Self {
            url,
            requests,
            task,
        }
    }
}

fn query_reply() -> Value {
    json!({"id":"q1","schema":[{"name":"n","type":"Int64"}],"data":[["1"]],"state":"Succeeded",
        "session":{"database":"default","need_keep_alive":true},
        "next_uri":"/v1/query/q1/page/0",
        "stats":{"scan_progress":{"rows":0,"bytes":0},"write_progress":{"rows":0,"bytes":0},
        "result_progress":{"rows":1,"bytes":1},"running_time_ms":0.0}})
}

async fn client(server: &MockServer) -> APIClient {
    let mut client = APIClient::from_dsn(&format!("databend://ignored:ignored@127.0.0.1:{}/default?sslmode=disable&warehouse=wh&retry_delay_secs=0", server.url.port().unwrap())).await.unwrap();
    client.transport = Transport::cloud();
    client.auth = Arc::new(CloudApiAuth::for_test(
        server.url.clone(),
        TOKEN.into(),
        ORG.into(),
    ));
    client.build_client(None).await.unwrap();
    client
}

#[tokio::test]
async fn cloud_missing_session_header_fails_closed_without_replaying_sql() {
    for sql in [
        "SELECT 1",
        "BEGIN",
        "CREATE TEMP TABLE missing_session(n INT)",
    ] {
        let server = MockServer::new(|request| {
            assert_cloud_session_transport(request);
            let mut reply = MockReply::json(query_reply()).without_session();
            reply
                .headers
                .push(("Set-Cookie", "session_id=not-a-fallback; Path=/".into()));
            reply
        })
        .await;
        let client = client(&server).await;
        for _ in 0..2 {
            let error = client
                .start_query_inner(sql, None, false, None)
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("requires session_header support"), "{error}");
            assert!(error.contains("may already have executed"));
            assert!(!error.contains(TOKEN));
            assert!(!error.contains("not-a-fallback"));
        }
        assert_eq!(
            server.requests.lock().len(),
            1,
            "SQL must not be replayed or sent on an unusable connection"
        );
    }
}

#[tokio::test]
async fn cloud_login_is_skipped_without_importing_manage_capabilities() {
    let server = MockServer::new(|_| panic!("Cloud login must not send a request")).await;
    let mut client = client(&server).await;
    client.login().await.unwrap();
    assert!(client.server_version.is_none());
    assert!(client.session_token_info.is_none());
    assert!(!client.capability.arrow_data);
    assert!(!client.capability.server_side_params);
    assert!(!client.capability.streaming_load);
    assert!(server.requests.lock().is_empty());
}

#[tokio::test]
async fn cloud_missing_header_does_not_mask_http_decode_or_sql_errors() {
    for failure in ["http", "decode", "sql"] {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let server = MockServer::new(move |_| {
            if seen.fetch_add(1, Ordering::SeqCst) > 0 {
                return MockReply::json(query_reply());
            }
            match failure {
                "http" => MockReply::error(403),
                "decode" => MockReply::json(json!({"invalid":"response"})).without_session(),
                "sql" => {
                    let mut body = query_reply();
                    body["error"] = json!({"code":5100, "message":TOKEN});
                    MockReply::json(body).without_session()
                }
                _ => unreachable!(),
            }
        })
        .await;
        let client = client(&server).await;
        let error = client
            .start_query_inner("SELECT 1", None, false, None)
            .await
            .unwrap_err();
        assert!(!error
            .to_string()
            .contains("requires session_header support"));
        assert!(!format!("{error:?}").contains(TOKEN));
        match failure {
            "http" => assert_eq!(error.status_code(), Some(StatusCode::FORBIDDEN)),
            "decode" => assert!(matches!(error, Error::Decode(_))),
            "sql" => assert!(matches!(error, Error::QueryFailed(_))),
            _ => unreachable!(),
        }
        client
            .start_query_inner("SELECT 1", None, false, None)
            .await
            .unwrap();
        assert_eq!(server.requests.lock().len(), 2);
    }
}

#[tokio::test]
async fn cloud_established_session_survives_omitted_response_headers() {
    let server = MockServer::new(|request| {
        if request.headers.contains_key("x-databend-session") {
            assert_eq!(
                request.headers["x-databend-session"],
                "opaque-default-session"
            );
            MockReply::json(query_reply()).without_session()
        } else {
            MockReply::json(query_reply())
        }
    })
    .await;
    let client = client(&server).await;
    for _ in 0..3 {
        client
            .start_query_inner("SELECT 1", None, false, None)
            .await
            .unwrap();
    }
    assert_eq!(server.requests.lock().len(), 3);
}

#[tokio::test]
async fn cloud_invalid_initial_session_header_makes_connection_unusable() {
    let server = MockServer::new(|_| {
        let mut reply = MockReply::json(query_reply());
        reply.headers.push((HEADER_SESSION, String::new()));
        reply
    })
    .await;
    let client = client(&server).await;
    let error = client
        .start_query_inner("SELECT 1", None, false, None)
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("invalid Cloud session response header"));
    let error = client
        .start_query_inner("SELECT 1", None, false, None)
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("requires session_header support"));
    assert_eq!(server.requests.lock().len(), 1);
}

#[tokio::test]
async fn cloud_management_results_without_next_uri_are_complete() {
    for with_rows in [false, true] {
        for with_stats in [false, true] {
            let server = MockServer::new(move |request| {
                assert_eq!(request.method, "POST");
                assert_eq!(request.path, "/v1/query");
                let mut body = json!({
                    "id": "management-query", "schema": [], "data": [], "state": "Succeeded"
                });
                if with_rows {
                    body["schema"] = json!([{"name":"n", "type":"Int64"}]);
                    body["data"] = json!([["1"]]);
                }
                if with_stats {
                    body["stats"] = query_reply()["stats"].clone();
                }
                let mut reply = MockReply::json(body);
                reply
                    .headers
                    .push(("X-Databend-Query-Source", "gateway".into()));
                reply
            })
            .await;
            let client = Arc::new(client(&server).await);
            let page = client
                .query_all("management statement", None)
                .await
                .unwrap();
            assert_eq!(page.data.len(), usize::from(with_rows));
            assert_eq!(page.raw_schema.len(), usize::from(with_rows));
            assert_eq!(
                page.stats.progresses.result_progress.rows,
                usize::from(with_stats)
            );
            if with_rows {
                assert_eq!(page.data[0][0].as_deref(), Some("1"));
            }

            let pages = client
                .start_query("management statement", true, None)
                .await
                .unwrap();
            let (mut pages, schema, _) = pages.wait_for_schema(true).await.unwrap();
            assert_eq!(schema.fields().len(), usize::from(with_rows));
            while let Some(page) = pages.next().await {
                page.unwrap();
            }
            drop(pages);
            client.try_heartbeat().await.unwrap();
            assert!(client.queries_need_heartbeat.lock().is_empty());
            assert!(client.last_node_id().is_none());
            assert_eq!(server.requests.lock().len(), 2);
        }
    }
}

#[tokio::test]
async fn missing_stats_defaults_do_not_hide_malformed_rows_or_query_errors() {
    for data in [json!([[123]]), json!("invalid-rows")] {
        let server = MockServer::new(move |_| {
            MockReply::json(json!({
                "id":"engine-query", "schema":[], "data":data, "state":"Succeeded"
            }))
        })
        .await;
        let client = Arc::new(client(&server).await);
        assert!(client.query_all("SELECT 1", None).await.is_err());
    }

    let server = MockServer::new(|_| {
        let mut reply = MockReply::json(json!({
            "id":"management-query", "schema":[], "data":[], "state":"Failed",
            "error":{"code":5100, "message":TOKEN}
        }));
        reply
            .headers
            .push(("X-Databend-Query-Source", "gateway".into()));
        reply
    })
    .await;
    let client = Arc::new(client(&server).await);
    let err = client
        .query_all("management statement", None)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, Error::QueryFailed(_)));
    assert!(!format!("{err:?}").contains(TOKEN));
    assert_eq!(server.requests.lock().len(), 1);
}

#[tokio::test]
async fn gateway_pagination_without_stats_preserves_all_metadata_and_rows() {
    for source_header in [false, true] {
        let server = MockServer::new(move |request| {
            let mut body = json!({
                "id":"q1", "node_id":"node-1", "session_id":"session-1",
                "schema":[{"name":"n","type":"Int64"}], "data":[["1"]],
                "state":"Running", "result_timeout_secs":123,
                "stats_uri":"/v1/query/q1/stats", "next_uri":"/v1/query/q1/page/0",
                "final_uri":"/v1/query/q1/final", "kill_uri":"/v1/query/q1/kill"
            });
            if request.method == "GET" {
                assert_eq!(request.headers["x-databend-sticky-node"], "node-1");
                if request.path.ends_with("/page/0") {
                    body["data"] = json!([["2"]]);
                    body["next_uri"] = json!("/v1/query/q1/final");
                } else {
                    assert!(request.path.ends_with("/final"));
                    body["data"] = json!([]);
                    body["next_uri"] = Value::Null;
                    body["state"] = json!("Succeeded");
                }
            }
            let mut reply = MockReply::json(body);
            if source_header {
                reply
                    .headers
                    .push(("X-Databend-Query-Source", "gateway".into()));
            }
            reply
        })
        .await;
        let client = Arc::new(client(&server).await);
        let (first, batches) = client
            .start_query_inner("SELECT 1", None, false, None)
            .await
            .unwrap();
        assert_eq!(first.node_id.as_deref(), Some("node-1"));
        assert_eq!(first.session_id.as_deref(), Some("session-1"));
        assert_eq!(first.result_timeout_secs, Some(123));
        assert_eq!(first.stats_uri.as_deref(), Some("/v1/query/q1/stats"));
        assert_eq!(first.final_uri.as_deref(), Some("/v1/query/q1/final"));
        assert_eq!(first.kill_uri.as_deref(), Some("/v1/query/q1/kill"));
        assert_eq!(first.stats.running_time_ms, 0.0);
        let pages = crate::pages::Pages::new(client.clone(), first, batches, false).unwrap();
        let (mut pages, _, _) = pages.wait_for_schema(false).await.unwrap();
        let mut rows = Vec::new();
        while let Some(page) = pages.next().await {
            rows.extend(page.unwrap().data);
        }
        assert_eq!(rows, vec![vec![Some("1".into())], vec![Some("2".into())]]);
        assert!(client.queries_need_heartbeat.lock().is_empty());
        assert_eq!(server.requests.lock().len(), 3);
    }
}

#[tokio::test]
async fn cloud_completed_queries_release_tracking_without_heartbeat() {
    for paginated in [false, true] {
        for need_progress in [false, true] {
            let queries = Arc::new(AtomicUsize::new(0));
            let seen = queries.clone();
            let server = MockServer::new(move |request| {
                let mut body = query_reply();
                let id = if request.method == "POST" {
                    format!("q{}", seen.fetch_add(1, Ordering::SeqCst))
                } else {
                    assert!(request.path.contains("/page/"));
                    assert_eq!(request.headers["x-databend-sticky-node"], "node-1");
                    request.path.split('/').nth(3).unwrap().to_owned()
                };
                body["id"] = json!(id);
                body["node_id"] = json!("node-1");
                body["next_uri"] = if paginated && request.method == "POST" {
                    json!(format!("/v1/query/{id}/page/0"))
                } else {
                    Value::Null
                };
                MockReply::json(body)
            })
            .await;
            let client = Arc::new(client(&server).await);
            for _ in 0..2 {
                let pages = client
                    .start_query("SELECT 1", need_progress, None)
                    .await
                    .unwrap();
                let (mut pages, _, _) = pages.wait_for_schema(need_progress).await.unwrap();
                assert_eq!(
                    client.queries_need_heartbeat.lock().len(),
                    usize::from(paginated)
                );
                pages.next().await.unwrap().unwrap();
                if paginated {
                    pages.next().await.unwrap().unwrap();
                }
                // Cleanup happens on the terminal response, not only on Drop
                // or on a subsequent poll returning None.
                assert!(client.queries_need_heartbeat.lock().is_empty());
                assert!(pages.next().await.is_none());
            }
            client.try_heartbeat().await.unwrap();
            assert!(client.queries_need_heartbeat.lock().is_empty());
            assert_eq!(server.requests.lock().len(), if paginated { 4 } else { 2 });
        }
    }
}

#[tokio::test]
async fn cloud_transactions_preserve_session_and_sticky_routing_with_session_headers() {
    let server = MockServer::new(|request| {
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        let sql = body["sql"].as_str().unwrap();
        let mut reply = query_reply();
        reply["next_uri"] = Value::Null;
        reply["node_id"] = json!("transaction-node");
        assert_cloud_session_transport(request);
        assert!(body.get("string_fields").is_none());
        if sql == "BEGIN" {
            reply["session"] =
                json!({"database":"default", "txn_state":"Active", "need_sticky":true});
        } else {
            assert_eq!(body["session"]["txn_state"], "Active");
            assert_eq!(
                request.headers["x-databend-sticky-node"],
                "transaction-node"
            );
            if sql == "SELECT 1" {
                reply["session"] = body["session"].clone();
            } else {
                assert!(sql == "COMMIT" || sql == "ROLLBACK");
                reply["session"] =
                    json!({"database":"default", "txn_state":"AutoCommit", "need_sticky":false});
            }
        }
        let mut reply = MockReply::json(reply);
        reply
            .headers
            .push((HEADER_SESSION, "opaque-transaction-session".into()));
        reply
    })
    .await;
    let client = Arc::new(client(&server).await);
    for end in ["COMMIT", "ROLLBACK"] {
        client.query_all("BEGIN", None).await.unwrap();
        assert!(client.in_active_transaction());
        client.query_all("SELECT 1", None).await.unwrap();
        assert!(client.in_active_transaction());
        client.query_all(end, None).await.unwrap();
        assert!(!client.in_active_transaction());
    }
    let requests = server.requests.lock();
    assert_eq!(requests.len(), 6);
    assert!(!requests[0].headers.contains_key("x-databend-session"));
    assert!(requests
        .iter()
        .skip(1)
        .all(|request| request.headers["x-databend-session"] == "opaque-transaction-session"));
}

#[tokio::test]
async fn cloud_page_errors_release_query_tracking() {
    for failure in ["http", "decode", "query"] {
        let server = MockServer::new(move |request| {
            if request.method == "POST" {
                let mut body = query_reply();
                body["node_id"] = json!("node-1");
                return MockReply::json(body);
            }
            match failure {
                "http" => MockReply::error(404),
                "decode" => MockReply::json(json!({"invalid":"response"})),
                "query" => {
                    let mut body = query_reply();
                    body["error"] = json!({"code":5100, "message":"query failed"});
                    MockReply::json(body)
                }
                _ => unreachable!(),
            }
        })
        .await;
        let client = Arc::new(client(&server).await);
        let pages = client.start_query("SELECT 1", false, None).await.unwrap();
        let (mut pages, _, _) = pages.wait_for_schema(false).await.unwrap();
        assert_eq!(client.queries_need_heartbeat.lock().len(), 1);
        pages.next().await.unwrap().unwrap();
        assert!(pages.next().await.unwrap().is_err());
        assert!(client.queries_need_heartbeat.lock().is_empty());
        assert!(pages.next().await.is_none());
        drop(pages);
        client.try_heartbeat().await.unwrap();
        assert_eq!(server.requests.lock().len(), 2);
    }
}

#[tokio::test]
async fn cloud_early_drop_still_finalizes_on_the_tracked_node() {
    let finalized = Arc::new(tokio::sync::Notify::new());
    let notification = finalized.clone();
    let server = MockServer::new(move |request| {
        if request.path.ends_with("/final") {
            assert_eq!(request.method, "GET");
            assert_eq!(request.headers["x-databend-sticky-node"], "node-1");
            notification.notify_one();
        } else {
            assert_eq!(request.path, "/v1/query");
        }
        let mut body = query_reply();
        body["node_id"] = json!("node-1");
        MockReply::json(body)
    })
    .await;
    let client = Arc::new(client(&server).await);
    let pages = client.start_query("SELECT 1", false, None).await.unwrap();
    let (pages, _, _) = pages.wait_for_schema(false).await.unwrap();
    assert_eq!(client.queries_need_heartbeat.lock().len(), 1);
    drop(pages);
    assert!(client.queries_need_heartbeat.lock().is_empty());
    tokio::time::timeout(Duration::from_secs(5), finalized.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn cloud_drop_during_final_request_releases_tracking_without_duplicate_cleanup() {
    let started = Arc::new(tokio::sync::Notify::new());
    let notification = started.clone();
    let server = MockServer::new(move |request| {
        let mut body = query_reply();
        body["node_id"] = json!("node-1");
        body["next_uri"] = json!("/v1/query/q1/final");
        let mut reply = MockReply::json(body);
        if request.method == "GET" {
            assert_eq!(request.path, "/v1/query/q1/final");
            notification.notify_one();
            reply.headers.push(("X-Mock-Delay", "200".into()));
        }
        reply
    })
    .await;
    let client = Arc::new(client(&server).await);
    let pages = client.start_query("SELECT 1", false, None).await.unwrap();
    let (mut pages, _, _) = pages.wait_for_schema(false).await.unwrap();
    pages.next().await.unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = pages.next() => panic!("expected the final request to remain in flight"),
            _ = started.notified() => {},
        }
    })
    .await
    .unwrap();
    assert_eq!(client.queries_need_heartbeat.lock().len(), 1);
    drop(pages);
    assert!(client.queries_need_heartbeat.lock().is_empty());
    assert_eq!(server.requests.lock().len(), 2);
}

#[tokio::test]
async fn cloud_dsn_is_webapi_only_and_consumes_credentials() {
    let dsn = format!("databend://ignored:%FF@api.databend.com/default?cloud_api_token={TOKEN}&org={ORG}&warehouse=wh");
    let client = APIClient::from_dsn(&dsn).await.unwrap();
    assert!(client.is_cloud_webapi());
    assert_eq!(client.username(), "cloud_account");
    assert!(!client.auth.can_reload());
    for host in ["api.databend.com", "api.databend.cn", "API.DATABEND.COM"] {
        let error = APIClient::from_dsn(&format!("databend://root:%FF@{host}/default"))
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("requires org"));
    }
    assert!(!format!("{:?}", client.session_state()).contains(TOKEN));
    assert!(!format!("{:?}", client.session_state()).contains("org"));
    assert!(matches!(client.get_presign_mode(), PresignMode::Off));
    let req = client
        .auth
        .wrap(client.cli.get(client.endpoint.clone()))
        .unwrap()
        .build()
        .unwrap();
    assert!(req.headers()[crate::cloud_api::HEADER_CLOUD_TOKEN].is_sensitive());
    assert!(!req.headers().contains_key(reqwest::header::AUTHORIZATION));
    // Authentication is origin-scoped, not restricted to query paths: future
    // management/debug operations can use the same transport without a JWT.
    let management = client
        .auth
        .wrap(
            client
                .cli
                .get(client.endpoint.join("api/v1/orgs/org-example").unwrap())
                .basic_auth("ignored", Some("ignored")),
        )
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(
        management.headers()[crate::cloud_api::HEADER_CLOUD_TOKEN],
        TOKEN
    );
    assert_eq!(
        management.headers()[crate::cloud_api::HEADER_CLOUD_ORG],
        ORG
    );
    assert!(!management
        .headers()
        .contains_key(reqwest::header::AUTHORIZATION));
    for option in [
        "access_token=other",
        "access_token_file=missing",
        "private_key_file=missing",
        "private_key_passphrase_file=missing",
        "cloud_api_token=second",
        "org=second",
        "tenant=tn-other",
        "query_result_format=arrow",
        "max_rows_per_page=10",
        "sslmode=disable",
    ] {
        let err = APIClient::from_dsn(&format!("{dsn}&{option}"))
            .await
            .err()
            .unwrap();
        assert!(!format!("{err:?}").contains(TOKEN));
    }
    for dsn in [
        "databend://api.databend.cn/default?cloud_api_token=fake&org=org-example",
        "databend:///?cloud_api_token=fake&org=org-example",
    ] {
        assert!(APIClient::from_dsn(dsn).await.is_ok(), "{dsn}");
    }
    for dsn in [
        "databend://api.databend.com?cloud_api_token=fake",
        "databend://api.databend.com?org=org-example",
        "databend://api.databend.com?cloud_api_token=&org=org-example",
        "databend://api.databend.com?cloud_api_token=fake&org=",
        "databend://api.databend.com?cloud_api_token=fake&org=%0D%0Ainjected",
        "databend://api.databend.com.evil.test?cloud_api_token=fake&org=org-example",
        "databend://tnexample.gw.aws-us-east-2.default.databend.com?cloud_api_token=fake&org=org-example",
        "databend://localhost?cloud_api_token=fake&org=org-example",
        "databend://api.databend.com:8443?cloud_api_token=fake&org=org-example",
        "databend+http://api.databend.com?cloud_api_token=fake&org=org-example",
    ] { assert!(APIClient::from_dsn(dsn).await.is_err(), "{dsn}"); }
}

#[tokio::test]
async fn cloud_query_lifecycle_stays_on_webapi_without_login() {
    let responses = Arc::new(AtomicUsize::new(0));
    let count = responses.clone();
    let server = MockServer::new(move |request| {
        assert!(!request.path.starts_with("/v1/session/"));
        let mut reply = MockReply::json(query_reply());
        reply
            .headers
            .push(("Set-Cookie", "session_id=not-used; Path=/".into()));
        reply.headers.push((
            HEADER_SESSION,
            format!("opaque-session-{}", count.fetch_add(1, Ordering::SeqCst)),
        ));
        reply
    })
    .await;
    let mut client = client(&server).await;
    client.login().await.unwrap();
    let client = Arc::new(client);
    client.check_presign().await.unwrap();
    let (reply, _) = client
        .start_query_inner("SELECT 1", None, false, None)
        .await
        .unwrap();
    assert_eq!(reply.data[0][0], Some("1".into()));
    client
        .query_page("q1", reply.next_uri.as_ref().unwrap(), &None)
        .await
        .unwrap();
    client.kill_query("q1").await.unwrap();
    client.final_query("q1", None).await.unwrap();
    client.try_heartbeat().await.unwrap();
    client.close().await;
    let requests = server.requests.lock();
    assert_eq!(requests.len(), 4);
    for (index, request) in requests.iter().enumerate() {
        assert_eq!(request.headers["x-databendcloud-token"], TOKEN);
        assert_eq!(request.headers["x-databendcloud-org"], ORG);
        assert_eq!(request.headers["x-databendcloud-warehouse"], "wh");
        assert!(!request.headers.contains_key("authorization"));
        assert_cloud_session_transport(request);
        if index == 0 {
            assert!(!request.headers.contains_key("x-databend-session"));
        } else {
            assert_eq!(
                request.headers["x-databend-session"],
                format!("opaque-session-{}", index - 1)
            );
        }
        assert!(!request.headers.contains_key("x-databend-warehouse"));
    }
    assert_eq!(requests[0].method, "POST");
    let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert!(body.get("string_fields").is_none());
    for key in [
        "pagination",
        "params",
        "stage_attachment",
        "arrow_result_version_max",
        "cloud_api_token",
        "org",
    ] {
        assert!(body.get(key).is_none(), "{key}");
    }
    assert_eq!(
        (requests[1].method.as_str(), requests[1].path.as_str()),
        ("GET", "/v1/query/q1/page/0")
    );
    assert_eq!(
        (requests[2].method.as_str(), requests[2].path.as_str()),
        ("POST", "/v1/query/q1/kill")
    );
    assert_eq!(
        (requests[3].method.as_str(), requests[3].path.as_str()),
        ("GET", "/v1/query/q1/final")
    );
}

#[tokio::test]
async fn cloud_dsn_forces_login_disabled_even_when_explicitly_enabled() {
    for option in [
        "",
        "&login=enable",
        "&login=disable",
        "&login=enable&session_token=enable",
    ] {
        let client = APIClient::from_dsn(&format!(
            "databend://api.databend.com/default?org={ORG}&cloud_api_token={TOKEN}{option}"
        ))
        .await
        .unwrap();
        assert!(client.disable_login);
        assert!(client.server_version.is_none());
        assert!(client.session_token_info.is_none());
        assert!(client.is_cloud_webapi());
    }
    for (option, disabled) in [("enable", false), ("disable", true)] {
        let native = APIClient::from_dsn(&format!("databend://localhost?login={option}"))
            .await
            .unwrap();
        assert_eq!(native.disable_login, disabled);
    }
}

#[tokio::test]
async fn cloud_unsupported_payloads_fail_before_network_requests() {
    let server = MockServer::new(|_| MockReply::json(query_reply())).await;
    let client = Arc::new(client(&server).await);
    assert!(client
        .start_query_inner("SELECT ?", None, false, Some(json!([1])))
        .await
        .is_err());
    assert!(client
        .insert_with_stage(
            "INSERT INTO t",
            "@stage/file",
            BTreeMap::new(),
            BTreeMap::new()
        )
        .await
        .is_err());
    assert!(client
        .upload_to_stage("@stage/file", Box::new(std::io::Cursor::new(vec![1])), 1)
        .await
        .is_err());
    assert!(client
        .streaming_load(
            "INSERT INTO t",
            Box::new(std::io::Cursor::new(vec![1])),
            "file"
        )
        .await
        .is_err());
    assert!(server.requests.lock().is_empty());
}

#[tokio::test]
async fn cloud_session_headers_are_not_shared_by_clients_with_the_same_origin() {
    let server = MockServer::new(|_| {
        let mut reply = MockReply::json(query_reply());
        reply
            .headers
            .push((HEADER_SESSION, "first-client-session".into()));
        reply
            .headers
            .push(("Set-Cookie", "session_id=not-used; Path=/".into()));
        reply
    })
    .await;
    let first = client(&server).await;
    let second = client(&server).await;
    first
        .start_query_inner("SELECT 1", None, false, None)
        .await
        .unwrap();
    first
        .query_page("q1", "/v1/query/q1/page/0", &None)
        .await
        .unwrap();
    second
        .start_query_inner("SELECT 1", None, false, None)
        .await
        .unwrap();
    let requests = server.requests.lock();
    assert!(!requests[0].headers.contains_key("x-databend-session"));
    assert_eq!(
        requests[1].headers["x-databend-session"],
        "first-client-session"
    );
    assert!(!requests[2].headers.contains_key("x-databend-session"));
    for request in requests.iter() {
        assert_cloud_session_transport(request);
    }
}

#[tokio::test]
async fn cloud_concurrent_first_queries_are_not_serialized() {
    let server = MockServer::new(|_| {
        let mut reply = MockReply::json(query_reply());
        reply
            .headers
            .push((HEADER_SESSION, "shared-bootstrap-session".into()));
        reply.headers.push(("X-Mock-Delay", "1000".into()));
        reply
    })
    .await;
    let client = Arc::new(client(&server).await);
    let queries = async {
        tokio::join!(
            client.start_query_inner("SELECT 1", None, false, None),
            client.start_query_inner("SELECT 2", None, false, None),
        )
    };
    let in_flight = async {
        tokio::time::timeout(Duration::from_millis(500), async {
            while server.requests.lock().len() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both SQL requests must be sent before either response arrives");
    };
    let ((first, second), ()) = tokio::join!(queries, in_flight);
    first.unwrap();
    second.unwrap();
    let requests = server.requests.lock();
    assert_eq!(requests.len(), 2);
    assert!(!requests[0].headers.contains_key("x-databend-session"));
    assert!(!requests[1].headers.contains_key("x-databend-session"));
    for request in requests.iter() {
        assert_cloud_session_transport(request);
    }
}

#[tokio::test]
async fn cloud_session_header_is_opaque_sensitive_and_origin_scoped() {
    let server = MockServer::new(|_| MockReply::json(query_reply())).await;
    let client = client(&server).await;
    let mut request = client
        .cli
        .get(server.url.clone())
        .header(reqwest::header::COOKIE, "browser=not-allowed")
        .header(HEADER_SESSION, "caller-supplied-session")
        .build()
        .unwrap();
    client
        .transport
        .prepare_request(&mut request, &client.endpoint)
        .unwrap();
    assert!(!request.headers().contains_key(HEADER_SESSION));
    assert!(!request.headers().contains_key(reqwest::header::COOKIE));
    assert_eq!(request.headers()[HEADER_CLIENT_CAPS], "session_header");

    // Echo unknown/future fields byte-for-byte; do not decode or synthesize IDs.
    let opaque = r#"{"id":"server-session-id", "last_refresh_time":7, "future":"value"}"#;
    let mut headers = HeaderMap::new();
    headers.insert(HEADER_SESSION, HeaderValue::from_str(opaque).unwrap());
    client.transport.observe_response(&headers).unwrap();
    client
        .transport
        .observe_response(&HeaderMap::new())
        .unwrap();
    client
        .transport
        .prepare_request(&mut request, &client.endpoint)
        .unwrap();
    assert_eq!(request.headers()[HEADER_SESSION], opaque);
    assert!(request.headers()[HEADER_SESSION].is_sensitive());
    assert!(!format!("{request:?}").contains("server-session-id"));
    let mut foreign = client
        .cli
        .get("http://other.invalid/v1/query")
        .build()
        .unwrap();
    assert!(client
        .transport
        .prepare_request(&mut foreign, &client.endpoint)
        .is_err());
    assert!(!foreign.headers().contains_key(HEADER_SESSION));
    assert!(server.requests.lock().is_empty());
}

#[tokio::test]
async fn cloud_rejects_invalid_response_session_headers_without_overwriting_state() {
    let server = MockServer::new(|_| MockReply::json(query_reply())).await;
    let client = client(&server).await;
    let mut valid = HeaderMap::new();
    valid.insert(HEADER_SESSION, HeaderValue::from_static("known-session"));
    client.transport.observe_response(&valid).unwrap();
    for value in [
        HeaderValue::from_static(""),
        HeaderValue::from_str(&"x".repeat(8193)).unwrap(),
        HeaderValue::from_bytes(b"\x80").unwrap(),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_SESSION, value);
        let error = client.transport.observe_response(&headers).unwrap_err();
        assert_eq!(
            error.to_string(),
            "DecodeError: invalid Cloud session response header"
        );
        let mut request = client.cli.get(server.url.clone()).build().unwrap();
        client
            .transport
            .prepare_request(&mut request, &client.endpoint)
            .unwrap();
        assert_eq!(request.headers()[HEADER_SESSION], "known-session");
    }
    valid.append(
        HEADER_SESSION,
        HeaderValue::from_static("ambiguous-session"),
    );
    assert!(client.transport.observe_response(&valid).is_err());
}

#[tokio::test]
async fn native_cookie_behavior_does_not_adopt_cloud_session_headers() {
    let server = MockServer::new(|request| {
        assert_eq!(request.headers["authorization"], "Basic cm9vdDo=");
        assert!(request.headers["cookie"].contains("cookie_enabled=true"));
        assert!(!request.headers.contains_key("x-databend-client-caps"));
        assert!(!request.headers.contains_key("x-databend-session"));
        let mut reply = MockReply::json(query_reply());
        reply
            .headers
            .push((HEADER_SESSION, "not-for-native".into()));
        reply
    })
    .await;
    let mut client = client(&server).await;
    client.transport = Transport::Native;
    client.auth = Arc::new(BasicAuth::new("root", ""));
    client.build_client(None).await.unwrap();
    client
        .start_query_inner("SELECT 1", None, false, None)
        .await
        .unwrap();
    client
        .query_page("q1", "/v1/query/q1/page/0", &None)
        .await
        .unwrap();
    let mut request = client.cli.get(server.url.clone()).build().unwrap();
    client
        .transport
        .prepare_request(&mut request, &client.endpoint)
        .unwrap();
    assert!(!request.headers().contains_key(HEADER_SESSION));
    assert_eq!(server.requests.lock().len(), 2);
}

#[tokio::test]
async fn cloud_redirect_and_foreign_page_never_forward_credentials() {
    let other = MockServer::new(|_| MockReply::json(query_reply())).await;
    let location = other.url.to_string();
    let server = MockServer::new(move |_| MockReply {
        status: 307,
        body: TOKEN.into(),
        headers: vec![("Location", location.clone())],
    })
    .await;
    let client = client(&server).await;
    assert!(client
        .query_page("q1", other.url.as_str(), &None)
        .await
        .is_err());
    let err = client
        .start_query_inner("SELECT 1", None, false, None)
        .await
        .err()
        .unwrap();
    assert_eq!(err.status_code(), Some(StatusCode::TEMPORARY_REDIRECT));
    assert!(!format!("{err:?}").contains(TOKEN));
    assert!(other.requests.lock().is_empty());
    assert_eq!(server.requests.lock().len(), 1);
}

#[tokio::test]
async fn cloud_org_not_found_is_actionable_without_hiding_other_failures() {
    let server = MockServer::new(|_| MockReply {
        status: 404,
        body: format!("{{\"error\":\"OrgNotFound\",\"message\":\"{TOKEN}\"}}"),
        headers: vec![],
    })
    .await;
    let cloud = client(&server).await;
    let error = cloud
        .start_query_inner("SELECT 1", None, false, None)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("Organization ID does not exist or has been changed"));
    assert!(!error.contains(TOKEN));
    let body = br#"{"error":"OrgNotFound","message":"Org not found"}"#;
    assert!(!cloud
        .transport
        .is_org_not_found(StatusCode::FORBIDDEN, body));
    assert!(!cloud
        .transport
        .is_org_not_found(StatusCode::NOT_FOUND, br#"{"error":"QueryNotFound"}"#));
    assert!(!cloud
        .transport
        .is_org_not_found(StatusCode::NOT_FOUND, b"not found"));
    let native = APIClient::default();
    assert!(!native
        .transport
        .is_org_not_found(StatusCode::NOT_FOUND, body));
}

#[tokio::test]
async fn cloud_query_auth_errors_are_not_retried_and_are_redacted() {
    for (status, body) in [
        (
            401,
            format!("{{\"error\":\"AuthFailed\",\"message\":\"{TOKEN}\"}}"),
        ),
        (
            403,
            format!(
                "{{\"error\":{{\"code\":5100,\"message\":\"{TOKEN}\",\"detail\":\"{TOKEN}\"}}}}"
            ),
        ),
        (503, format!("service unavailable {TOKEN}")),
        (200, format!("{{invalid {TOKEN}")),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let server = MockServer::new(move |_| {
            seen.fetch_add(1, Ordering::SeqCst);
            MockReply {
                status,
                body: body.clone(),
                headers: vec![],
            }
        })
        .await;
        let client = client(&server).await;
        let err = client
            .start_query_inner("INSERT INTO t VALUES(1)", None, false, None)
            .await
            .err()
            .unwrap();
        assert!(!format!("{err:?}").contains(TOKEN));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn cloud_safe_requests_retry_503() {
    for kind in [RequestKind::QueryPage, RequestKind::QueryFinal] {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let server = MockServer::new(move |request| {
            assert_cloud_session_transport(request);
            let attempt = seen.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                assert!(!request.headers.contains_key("x-databend-session"));
            } else {
                assert_eq!(request.headers["x-databend-session"], "retry-session-0");
            }
            let mut reply = if attempt == 0 {
                MockReply::error(503)
            } else {
                MockReply::json(query_reply())
            };
            reply
                .headers
                .push((HEADER_SESSION, format!("retry-session-{attempt}")));
            reply
        })
        .await;
        let client = client(&server).await;
        match kind {
            RequestKind::QueryPage => {
                client
                    .query_page("q1", "/v1/query/q1/page/0", &None)
                    .await
                    .unwrap();
            }
            RequestKind::QueryFinal => client.final_query("q1", None).await.unwrap(),
            _ => unreachable!(),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn cloud_page_retries_timeout_and_truncated_body_but_submission_does_not() {
    for fault in ["X-Mock-Delay", "X-Mock-Truncate"] {
        for submission in [false, true] {
            let calls = Arc::new(AtomicUsize::new(0));
            let seen = calls.clone();
            let server = MockServer::new(move |request| {
                assert_cloud_session_transport(request);
                let attempt = seen.fetch_add(1, Ordering::SeqCst);
                if attempt > 0 && fault == "X-Mock-Truncate" {
                    assert_eq!(request.headers["x-databend-session"], "read-session-0");
                } else {
                    assert!(!request.headers.contains_key("x-databend-session"));
                }
                let mut reply = MockReply::json(query_reply());
                reply
                    .headers
                    .push((HEADER_SESSION, format!("read-session-{attempt}")));
                if attempt == 0 {
                    reply.headers.push((fault, "200".into()));
                }
                reply
            })
            .await;
            let mut client = client(&server).await;
            client.page_request_timeout = Duration::from_millis(50);
            if submission {
                assert!(client
                    .start_query_inner("INSERT INTO t VALUES(1)", None, false, None)
                    .await
                    .is_err());
                assert_eq!(calls.load(Ordering::SeqCst), 1, "{fault}");
            } else {
                client
                    .query_page("q1", "/v1/query/q1/page/0", &None)
                    .await
                    .unwrap();
                assert_eq!(calls.load(Ordering::SeqCst), 2, "{fault}");
            }
        }
    }
}

#[tokio::test]
async fn cloud_submission_retries_connect_errors_before_sending_bytes() {
    let server = MockServer::new(|_| MockReply::json(query_reply())).await;
    let mut client = client(&server).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    client.endpoint = Url::parse(&format!("http://{addr}/")).unwrap();
    let request = client
        .cli
        .post(format!("http://{addr}/v1/query"))
        .build()
        .unwrap();
    let err = client
        .query_request_helper(request, true, false, false, RequestKind::QueryStart)
        .await
        .err()
        .unwrap();
    assert!(
        matches!(
            err,
            Error::WithContext {
                retry_times: Some(2),
                ..
            }
        ),
        "{err:?}"
    );
}
