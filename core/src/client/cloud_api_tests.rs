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
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};

mod lifecycle;

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
        Self::with_query_gate(handler, None).await
    }

    async fn with_query_gate(
        handler: impl Fn(&MockRequest) -> MockReply + Send + Sync + 'static,
        query_gate: Option<Arc<Notify>>,
    ) -> Self {
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
                        let query_gate = query_gate.clone();
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
                            let is_query_start = request.method == "POST" && request.path == "/v1/query";
                            captured.lock().push(request);
                            if is_query_start {
                                if let Some(gate) = query_gate {
                                    gate.notified().await;
                                }
                            }
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
        "session":{"database":"default"},
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

async fn query_id_client(server: &MockServer, cloud: bool) -> Arc<APIClient> {
    let mut client = client(server).await;
    if !cloud {
        client.transport = Transport::Native;
        client.auth = Arc::new(BasicAuth::new("root", ""));
        client.build_client(None).await.unwrap();
    }
    Arc::new(client)
}

async fn received_query_id(server: &MockServer) -> String {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(request) = server.requests.lock().first() {
                assert_eq!(request.method, "POST");
                assert_eq!(request.path, "/v1/query");
                return request.headers["x-databend-query-id"].clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("query request must arrive while its response is blocked")
}

#[tokio::test]
async fn last_query_id_is_available_before_first_response() {
    for cloud in [false, true] {
        for path in [
            "start_query",
            "query_all",
            "query_all_json",
            "insert_with_stage",
        ] {
            if cloud && path == "insert_with_stage" {
                // Cloud rejects stage attachments before generating a query ID.
                continue;
            }
            let gate = Arc::new(Notify::new());
            let server = MockServer::with_query_gate(
                |_| {
                    let mut reply = query_reply();
                    reply["next_uri"] = Value::Null;
                    MockReply::json(reply)
                },
                Some(gate.clone()),
            )
            .await;
            let client = query_id_client(&server, cloud).await;
            client.set_last_query_id(Some("previous-query".into()));
            let query_client = client.clone();
            let query = tokio::spawn(async move {
                match path {
                    "start_query" => query_client
                        .start_query("SELECT 1", false, None)
                        .await
                        .map(|_| ()),
                    "query_all" => query_client.query_all("SELECT 1", None).await.map(|_| ()),
                    "query_all_json" => query_client
                        .query_all_inner("SELECT 1", true, None)
                        .await
                        .map(|_| ()),
                    "insert_with_stage" => query_client
                        .insert_with_stage(
                            "INSERT INTO t VALUES",
                            "@stage/file",
                            BTreeMap::new(),
                            BTreeMap::new(),
                        )
                        .await
                        .map(|_| ()),
                    _ => unreachable!(),
                }
            });
            let sent_id = received_query_id(&server).await;
            assert_ne!(sent_id, "previous-query");
            assert_eq!(client.last_query_id().as_deref(), Some(sent_id.as_str()));
            assert!(!query.is_finished(), "the first response is still blocked");
            gate.notify_one();
            tokio::time::timeout(Duration::from_secs(5), query)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            // The mock deliberately returns a different ID, like a rewriting gateway.
            assert_eq!(client.last_query_id().as_deref(), Some("q1"));
            assert!(client.last_query_id.lock().pending.is_empty());
        }
    }
}

#[tokio::test]
async fn interrupted_submission_retains_query_id_before_first_response() {
    for cloud in [false, true] {
        let server = MockServer::with_query_gate(
            |_| MockReply::json(query_reply()),
            Some(Arc::new(Notify::new())),
        )
        .await;
        let client = query_id_client(&server, cloud).await;
        let query_client = client.clone();
        let query =
            tokio::spawn(async move { query_client.start_query("SELECT 1", false, None).await });
        let sent_id = received_query_id(&server).await;
        query.abort();
        assert!(matches!(query.await, Err(error) if error.is_cancelled()));
        assert_eq!(client.last_query_id().as_deref(), Some(sent_id.as_str()));
        assert!(client.last_query_id.lock().pending.is_empty());
    }
}

#[test]
fn unsent_submission_preserves_response_id_on_either_side_of_publication() {
    for response_first in [true, false] {
        let client = APIClient::default();
        client.set_last_query_id(Some("older-query".into()));
        if response_first {
            client.set_last_query_id(Some("server-response-id".into()));
        }

        let submission = QuerySubmission::new(&client, "new-submission-id".into());
        assert_eq!(client.last_query_id().as_deref(), Some("new-submission-id"));
        assert_eq!(
            client.last_query_id.lock().pending["new-submission-id"].as_deref(),
            Some(if response_first {
                "server-response-id"
            } else {
                "older-query"
            }),
        );
        if !response_first {
            client.set_last_query_id(Some("server-response-id".into()));
        }

        // A response before publication must be restored; a response after
        // publication must not be overwritten by an unsent query's rollback.
        drop(submission);
        assert_eq!(
            client.last_query_id().as_deref(),
            Some("server-response-id")
        );
    }
}

#[test]
fn concurrent_submission_rollback_never_resurrects_an_unsent_id() {
    let completion_orders: &[&[usize]] = &[
        &[0, 1],
        &[1, 0],
        &[0, 1, 2],
        &[0, 2, 1],
        &[1, 0, 2],
        &[1, 2, 0],
        &[2, 0, 1],
        &[2, 1, 0],
    ];
    for previous_id in [None, Some("previous-server-id".to_string())] {
        for order in completion_orders {
            // Exercise every mix of definitely-unsent and possibly-sent queries.
            for sent_mask in 0..(1 << order.len()) {
                let client = APIClient::default();
                client.set_last_query_id(previous_id.clone());
                let mut submissions: Vec<_> = (0..order.len())
                    .map(|index| {
                        let mut submission =
                            QuerySubmission::new(&client, format!("query-{index}"));
                        submission.may_have_reached_server = sent_mask & (1 << index) != 0;
                        Some(submission)
                    })
                    .collect();
                let mut valid = vec![true; order.len()];
                for &index in *order {
                    drop(submissions[index].take());
                    if sent_mask & (1 << index) == 0 {
                        valid[index] = false;
                    }
                    let expected = valid
                        .iter()
                        .rposition(|&valid| valid)
                        .map(|index| format!("query-{index}"))
                        .or_else(|| previous_id.clone());
                    assert_eq!(
                        client.last_query_id(),
                        expected,
                        "order={order:?}, sent_mask={sent_mask}, finished={index}"
                    );
                    assert_eq!(
                        client.last_query_id.lock().pending.len(),
                        submissions
                            .iter()
                            .filter(|submission| submission.is_some())
                            .count()
                    );
                }
            }
        }
    }
}

#[test]
fn rollback_invalidation_propagates_to_new_submissions() {
    let client = APIClient::default();
    client.set_last_query_id(Some("server-response-id".into()));
    let first = QuerySubmission::new(&client, "first-unsent".into());
    let second = QuerySubmission::new(&client, "second-unsent".into());
    drop(first);
    let third = QuerySubmission::new(&client, "third-unsent".into());
    drop(second);
    drop(third);
    assert_eq!(
        client.last_query_id().as_deref(),
        Some("server-response-id")
    );
    assert!(client.last_query_id.lock().pending.is_empty());
}

#[test]
fn concurrent_unsent_rollbacks_preserve_response_overrides() {
    for response_at in 0..=3 {
        let client = APIClient::default();
        let submissions: Vec<_> = (0..3)
            .map(|index| Some(QuerySubmission::new(&client, format!("unsent-{index}"))))
            .collect();
        for (step, submission) in submissions
            .into_iter()
            .chain(std::iter::once(None))
            .enumerate()
        {
            if step == response_at {
                client.set_last_query_id(Some("server-response-id".into()));
            }
            drop(submission);
            if step >= response_at {
                assert_eq!(
                    client.last_query_id().as_deref(),
                    Some("server-response-id")
                );
            }
        }
        assert!(client.last_query_id.lock().pending.is_empty());
    }
}

#[tokio::test]
async fn unsent_submission_restores_previous_query_id() {
    for cloud in [false, true] {
        for previous_id in [None, Some("previous-query".to_string())] {
            let server = MockServer::new(|_| panic!("request must not be sent")).await;
            let mut client = Arc::try_unwrap(query_id_client(&server, cloud).await)
                .ok()
                .unwrap();
            client.set_last_query_id(previous_id.clone());
            client.tenant = Some("invalid\nheader".into());
            assert!(client
                .start_query_inner("SELECT 1", None, false, None)
                .await
                .is_err());
            assert_eq!(client.last_query_id(), previous_id);
            assert!(server.requests.lock().is_empty());

            client.tenant = None;
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            client.endpoint =
                Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
            drop(listener);
            if cloud {
                client.auth = Arc::new(CloudApiAuth::for_test(
                    client.endpoint.clone(),
                    TOKEN.into(),
                    ORG.into(),
                ));
            }
            client.retry_delay_secs = 0;
            let error = client
                .start_query_inner("SELECT 1", None, false, None)
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("retry_times=2"), "{error}");
            assert_eq!(client.last_query_id(), previous_id);
        }
    }
}

#[tokio::test]
async fn failed_response_retains_submitted_query_id() {
    for cloud in [false, true] {
        for fault in ["http", "decode", "body", "sql"] {
            let server = MockServer::new(move |_| match fault {
                "http" => MockReply::error(403),
                "decode" => MockReply::json(json!({"invalid": "response"})),
                "sql" => {
                    let mut reply = query_reply();
                    reply["id"] = json!("server-failed-query-id");
                    reply["error"] = json!({"code": 5100, "message": "SQL failed"});
                    MockReply::json(reply)
                }
                "body" => {
                    let mut reply = MockReply::json(query_reply());
                    reply.headers.push(("X-Mock-Truncate", "1".into()));
                    reply
                }
                _ => unreachable!(),
            })
            .await;
            let client = query_id_client(&server, cloud).await;
            client.set_last_query_id(Some("previous-query".into()));
            let error = client
                .start_query_inner("SELECT 1", None, false, None)
                .await
                .unwrap_err();
            let sent_id = received_query_id(&server).await;
            if fault == "sql" {
                assert!(matches!(error, Error::QueryFailed(_)));
                assert_ne!(sent_id, "server-failed-query-id");
                assert_eq!(
                    client.last_query_id().as_deref(),
                    Some("server-failed-query-id")
                );
            } else {
                assert_eq!(client.last_query_id().as_deref(), Some(sent_id.as_str()));
            }
            assert!(client.last_query_id.lock().pending.is_empty());
        }
    }
}

#[tokio::test]
async fn later_connect_failure_does_not_forget_an_earlier_attempt() {
    let server = MockServer::new(|_| unreachable!()).await;
    let mut client = Arc::try_unwrap(query_id_client(&server, false).await)
        .ok()
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    client.endpoint = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    client.retry_delay_secs = 0;
    let first_attempt = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        // Refuse subsequent connections, but let the first attempt receive 503.
        drop(listener);
        let mut bytes = Vec::new();
        loop {
            let mut chunk = [0; 4096];
            let count = stream.read(&mut chunk).await.unwrap();
            assert_ne!(count, 0);
            bytes.extend_from_slice(&chunk[..count]);
            if bytes.windows(4).any(|v| v == b"\r\n\r\n") {
                break;
            }
        }
        stream
            .write_all(b"HTTP/1.1 503 Mock\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
    });
    client.set_last_query_id(Some("previous-query".into()));
    assert!(client
        .start_query_inner("SELECT 1", None, false, None)
        .await
        .is_err());
    first_attempt.await.unwrap();
    assert!(client.last_query_id().is_some());
    assert_ne!(client.last_query_id().as_deref(), Some("previous-query"));
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
async fn cloud_early_drop_still_finalizes_on_the_tracked_node() {
    // Generic completion/error/cancellation matrices live in tracking_tests.rs;
    // here assert only Cloud's final method, node routing and session transport.
    let finalized = Arc::new(tokio::sync::Notify::new());
    let notification = finalized.clone();
    let server = MockServer::new(move |request| {
        if request.path.ends_with("/final") {
            assert_cloud_session_transport(request);
            assert_eq!(
                request.headers["x-databend-session"],
                "opaque-default-session"
            );
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
        "query_result_format=arrow&max_rows_per_page=10",
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
async fn cloud_pagination_options_are_forwarded_even_when_server_ignores_them() {
    for (options, expected) in [
        ("", None),
        ("wait_time_secs=0", Some(json!({"wait_time_secs": 0}))),
        (
            "max_rows_in_buffer=1000",
            Some(json!({"max_rows_in_buffer": 1000})),
        ),
        ("max_rows_per_page=1", Some(json!({"max_rows_per_page": 1}))),
        (
            "wait_time_secs=1&max_rows_in_buffer=1000&max_rows_per_page=1",
            Some(json!({"wait_time_secs": 1, "max_rows_in_buffer": 1000, "max_rows_per_page": 1})),
        ),
    ] {
        let server = MockServer::new(move |request| {
            assert_cloud_session_transport(request);
            assert_eq!(request.path, "/v1/query");
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(body.get("pagination"), expected.as_ref());
            for key in ["wait_time_secs", "max_rows_in_buffer", "max_rows_per_page"] {
                assert!(body["session"]["settings"].get(key).is_none());
            }
            // Model the current WebAPI ignoring the requested one-row page size.
            let mut reply = query_reply();
            reply["data"] = json!([["1"], ["2"]]);
            reply["next_uri"] = Value::Null;
            MockReply::json(reply)
        })
        .await;
        let mut client = APIClient::from_dsn(&format!(
            "databend://api.databend.com/default?org={ORG}&cloud_api_token={TOKEN}&{options}"
        ))
        .await
        .unwrap();
        client.endpoint = server.url.clone();
        client.auth = Arc::new(CloudApiAuth::for_test(
            server.url.clone(),
            TOKEN.into(),
            ORG.into(),
        ));
        client.build_client(None).await.unwrap();
        let client = Arc::new(client);
        let reply = client.query_all("SELECT 1", None).await.unwrap();
        assert_eq!(
            reply.data,
            vec![vec![Some("1".into())], vec![Some("2".into())]]
        );
        assert_eq!(server.requests.lock().len(), 1);
    }
    for key in ["wait_time_secs", "max_rows_in_buffer", "max_rows_per_page"] {
        let result = APIClient::from_dsn(&format!(
            "databend://api.databend.com/default?org={ORG}&cloud_api_token={TOKEN}&{key}=not-an-integer"
        )).await;
        assert!(result.is_err(), "pagination options still require integers");
    }
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
        .query_request_helper(request, true, false, false, RequestKind::QueryStart, None)
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
