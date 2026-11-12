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

//! Pages lifecycle tests live under client to inspect its private heartbeat registry.

use super::*;
use std::any::Any;
use std::future::Future;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};

struct MockRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

struct MockReply {
    status: u16,
    body: String,
    gate: Option<Arc<Notify>>,
}

impl MockReply {
    fn json(body: Value) -> Self {
        Self {
            status: 200,
            body: body.to_string(),
            gate: None,
        }
    }
}

#[derive(Default)]
struct MockFailures {
    messages: Mutex<Vec<String>>,
    changed: Notify,
}

impl MockFailures {
    fn record(&self, message: String) {
        self.messages.lock().push(message);
        self.changed.notify_one();
    }

    fn assert_empty(&self) {
        let messages = self.messages.lock();
        assert!(
            messages.is_empty(),
            "mock server failure: {}",
            messages.join("\n")
        );
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("non-string panic payload")
}

type MockHandler = dyn Fn(&MockRequest) -> MockReply + Send + Sync;

// Minimal HTTP/1.1 fixture: one request per connection, Content-Length only.
// This intentionally avoids an HTTP-server dev-dependency for these tests.
async fn read_request(stream: &mut TcpStream) -> io::Result<MockRequest> {
    const LIMIT: usize = 128 * 1024;
    let invalid = |message| io::Error::new(io::ErrorKind::InvalidData, message);
    let mut bytes = Vec::new();
    let (end, length) = loop {
        let mut chunk = [0; 4096];
        let count = stream.read(&mut chunk).await?;
        if count == 0 {
            return Err(invalid("incomplete HTTP request"));
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.len() > LIMIT {
            return Err(invalid("HTTP request exceeds fixture limit"));
        }
        if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&bytes[..end]);
            let length = head
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                .map(|(_, value)| value.trim().parse::<usize>())
                .transpose()
                .map_err(|_| invalid("invalid Content-Length"))?
                .unwrap_or(0);
            let end = end + 4;
            if length > LIMIT - end {
                return Err(invalid("HTTP body exceeds fixture limit"));
            }
            break (end, length);
        }
    };
    while bytes.len() < end + length {
        let mut chunk = [0; 4096];
        let count = stream.read(&mut chunk).await?;
        if count == 0 {
            return Err(invalid("incomplete HTTP body"));
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    let head = String::from_utf8_lossy(&bytes[..end]);
    let mut first = head
        .lines()
        .next()
        .ok_or_else(|| invalid("missing request line"))?
        .split_whitespace();
    Ok(MockRequest {
        method: first
            .next()
            .ok_or_else(|| invalid("missing method"))?
            .into(),
        path: first.next().ok_or_else(|| invalid("missing path"))?.into(),
        headers: head
            .lines()
            .skip(1)
            .filter_map(|line| line.split_once(':'))
            .map(|(key, value)| (key.to_ascii_lowercase(), value.trim().to_string()))
            .collect(),
        body: bytes[end..end + length].to_vec(),
    })
}

async fn handle_connection(
    mut stream: TcpStream,
    requests: Arc<Mutex<Vec<MockRequest>>>,
    handler: Arc<MockHandler>,
    failures: Arc<MockFailures>,
) {
    let request = match read_request(&mut stream).await {
        Ok(request) => request,
        Err(error) => {
            failures.record(format!("mock connection: {error}"));
            return;
        }
    };
    let context = format!("{} {}", request.method, request.path);
    // Publish before the handler notifies a waiting test. Catch assertions here
    // so the original failure is recorded before the client receives an error.
    let reply = {
        let mut requests = requests.lock();
        requests.push(request);
        catch_unwind(AssertUnwindSafe(|| handler(requests.last().unwrap())))
    };
    let reply = match reply {
        Ok(reply) => reply,
        Err(payload) => {
            let message = format!("{context}: {}", panic_message(payload.as_ref()));
            failures.record(message.clone());
            let mut reply = MockReply::json(json!({"error":{"code":5100,"message":message}}));
            reply.status = 500;
            reply
        }
    };
    if let Some(gate) = reply.gate {
        gate.notified().await;
    }
    let response = format!(
        concat!(
            "HTTP/1.1 {} Mock\r\n",
            "Content-Length: {}\r\n",
            "Content-Type: application/json\r\n",
            "Connection: close\r\n\r\n{}"
        ),
        reply.status,
        reply.body.len(),
        reply.body
    );
    // Dropping Pages can deliberately close a pending connection.
    let _ = stream.write_all(response.as_bytes()).await;
}

struct MockServer {
    url: Url,
    requests: Arc<Mutex<Vec<MockRequest>>>,
    failures: Arc<MockFailures>,
    task: JoinHandle<()>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.task.abort();
        if std::thread::panicking() {
            // Preserve the root cause without causing a double-panic abort.
            for message in self.failures.messages.lock().iter() {
                eprintln!("mock server failure: {message}");
            }
        } else {
            self.failures.assert_empty();
        }
    }
}

impl MockServer {
    async fn new(handler: impl Fn(&MockRequest) -> MockReply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let handler: Arc<MockHandler> = Arc::new(handler);
        let failures = Arc::new(MockFailures::default());
        let errors = failures.clone();
        let task = tokio::spawn(async move {
            let mut workers = JoinSet::new();
            loop {
                tokio::select! {
                    connection = listener.accept() => {
                        match connection {
                            Ok((stream, _)) => {
                                workers.spawn(handle_connection(
                                    stream, captured.clone(), handler.clone(), errors.clone(),
                                ));
                            },
                            Err(error) => {
                                errors.record(format!("mock accept: {error}"));
                                break;
                            },
                        }
                    },
                    result = workers.join_next(), if !workers.is_empty() => {
                        if let Some(Err(error)) = result {
                            errors.record(format!("mock worker: {error}"));
                        }
                    },
                }
            }
        });
        Self {
            url,
            requests,
            failures,
            task,
        }
    }

    async fn wait_for<T>(&self, future: impl Future<Output = T>) -> T {
        self.failures.assert_empty();
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                biased;
                _ = self.failures.changed.notified() => {
                    self.failures.assert_empty();
                    unreachable!("mock failure notification without a recorded failure")
                },
                output = future => {
                    self.failures.assert_empty();
                    output
                },
            }
        })
        .await;
        self.failures.assert_empty();
        result.expect("timed out waiting for local mock")
    }

    fn assert_native_requests(&self, count: usize) {
        self.failures.assert_empty();
        let requests = self.requests.lock();
        assert_eq!(requests.len(), count);
        for request in requests.iter() {
            assert_eq!(request.headers["authorization"], "Basic cm9vdDo=");
        }
    }
}

fn query_reply(next_uri: Option<&str>, data: Value) -> Value {
    json!({
        "id":"q1", "node_id":"node-1", "schema":[{"name":"n","type":"Int64"}],
        "data":data, "state":if next_uri.is_some() { "Running" } else { "Succeeded" },
        "next_uri":next_uri, "result_timeout_secs":2,
        "stats":{"scan_progress":{"rows":0,"bytes":0}, "write_progress":{"rows":0,"bytes":0},
            "result_progress":{"rows":0,"bytes":0}, "running_time_ms":0.0}
    })
}

async fn client(server: &MockServer) -> Arc<APIClient> {
    let mut client = APIClient::from_dsn(&format!(
        "databend://root:@127.0.0.1:{}/default?sslmode=disable&login=disable&presign=off&retry_count=1",
        server.url.port().unwrap()
    )).await.unwrap();
    client.build_client(None).await.unwrap();
    // Do not register the test client with the background client manager:
    // heartbeat scheduling is driven explicitly and requires no sleeps.
    Arc::new(client)
}

async fn tracked_pages(client: &Arc<APIClient>, need_progress: bool) -> Pages {
    let pages = client
        .start_query("SELECT 1", need_progress, None)
        .await
        .unwrap();
    let (mut pages, _, _) = pages.wait_for_schema(need_progress).await.unwrap();
    assert!(client.queries_need_heartbeat.lock().contains_key("q1"));
    let first = pages.next().await.unwrap().unwrap();
    assert_eq!(first.data, vec![vec![Some("1".into())]]);
    assert!(client.queries_need_heartbeat.lock().contains_key("q1"));
    pages
}

#[tokio::test]
async fn mock_handler_panics_keep_the_original_error_and_accept_loop_alive() {
    let server = MockServer::new(|request| {
        assert_ne!(request.path, "/fail", "handler assertion sentinel");
        MockReply::json(json!({}))
    })
    .await;
    let http = HttpClient::new();
    let response = http
        .get(server.url.join("fail").unwrap())
        .basic_auth("root", Some(""))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(response
        .text()
        .await
        .unwrap()
        .contains("handler assertion sentinel"));
    let response = http
        .get(server.url.join("ok").unwrap())
        .basic_auth("root", Some(""))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let panic = catch_unwind(AssertUnwindSafe(|| server.assert_native_requests(2))).unwrap_err();
    let message = panic_message(panic.as_ref());
    assert!(message.contains("GET /fail"), "{message}");
    assert!(message.contains("handler assertion sentinel"), "{message}");
    // This test intentionally caused and verified the failure; acknowledge it
    // so Drop does not report the same deliberately induced failure again.
    server.failures.messages.lock().clear();
    server.assert_native_requests(2);
}

#[tokio::test]
async fn mock_drop_reports_unacknowledged_failures() {
    let server = MockServer::new(|_| MockReply::json(json!({}))).await;
    server.failures.record("drop assertion sentinel".into());
    let panic = catch_unwind(AssertUnwindSafe(|| drop(server))).unwrap_err();
    assert!(panic_message(panic.as_ref()).contains("drop assertion sentinel"));
}

#[tokio::test]
#[should_panic(expected = "notification assertion sentinel")]
async fn mock_wait_reports_failures_without_waiting_for_timeout() {
    let server = MockServer::new(|_| MockReply::json(json!({}))).await;
    server
        .wait_for(async {
            server
                .failures
                .record("notification assertion sentinel".into());
            std::future::pending::<()>().await;
        })
        .await;
}

#[tokio::test]
async fn completed_first_response_is_never_registered() {
    let server = MockServer::new(|_| MockReply::json(query_reply(None, json!([["1"]])))).await;
    let client = client(&server).await;
    for need_progress in [false, true] {
        let pages = client
            .start_query("SELECT 1", need_progress, None)
            .await
            .unwrap();
        let (mut pages, _, _) = pages.wait_for_schema(need_progress).await.unwrap();
        assert!(client.queries_need_heartbeat.lock().is_empty());
        assert_eq!(
            pages.next().await.unwrap().unwrap().data,
            vec![vec![Some("1".into())]]
        );
        assert!(pages.next().await.is_none());
    }
    client.try_heartbeat().await.unwrap();
    server.assert_native_requests(2);
}

#[tokio::test]
async fn terminal_response_unregisters_before_none_or_drop() {
    for need_progress in [false, true] {
        for empty_terminal in [false, true] {
            let server = MockServer::new(move |request| {
                let (next, data) = if request.method == "POST" {
                    (Some("/v1/query/q1/page/0"), json!([["1"]]))
                } else {
                    assert_eq!(request.path, "/v1/query/q1/page/0");
                    assert_eq!(request.headers["x-databend-sticky-node"], "node-1");
                    (
                        None,
                        if empty_terminal {
                            json!([])
                        } else {
                            json!([["2"]])
                        },
                    )
                };
                MockReply::json(query_reply(next, data))
            })
            .await;
            let client = client(&server).await;
            let mut pages = tracked_pages(&client, need_progress).await;
            let terminal = pages.next().await;
            if empty_terminal && !need_progress {
                assert!(terminal.is_none());
            } else {
                let page = terminal.unwrap().unwrap();
                assert_eq!(page.data.is_empty(), empty_terminal);
            }
            // Nonempty terminal pages must release tracking before a caller
            // polls None or drops Pages; neither is allowed to hide the bug.
            assert!(client.queries_need_heartbeat.lock().is_empty());
            client.try_heartbeat().await.unwrap();
            assert!(pages.next().await.is_none());
            drop(pages);
            server.assert_native_requests(2);
        }
    }
}

#[tokio::test]
async fn active_queries_still_receive_heartbeats() {
    let server = MockServer::new(|request| {
        if request.path == "/v1/session/heartbeat" {
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(body, json!({"node_to_queries":{"node-1":["q1"]}}));
            return MockReply::json(json!({}));
        }
        let next = (request.method == "POST").then_some("/v1/query/q1/page/0");
        MockReply::json(query_reply(next, json!([["1"]])))
    })
    .await;
    let client = client(&server).await;
    let mut pages = tracked_pages(&client, false).await;
    {
        let queries = client.queries_need_heartbeat.lock();
        *queries["q1"].last_access_time.lock() = Instant::now() - Duration::from_secs(3);
    }
    client.try_heartbeat().await.unwrap();
    assert!(client.queries_need_heartbeat.lock().contains_key("q1"));
    pages.next().await.unwrap().unwrap();
    assert!(client.queries_need_heartbeat.lock().is_empty());
    client.try_heartbeat().await.unwrap();
    server.assert_native_requests(3);
}

#[tokio::test]
async fn completing_one_query_does_not_unregister_another() {
    let server = MockServer::new(|request| {
        let id = if request.method == "POST" {
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            if body["sql"] == "SELECT 2" {
                "q2"
            } else {
                "q1"
            }
        } else {
            request.path.split('/').nth(3).unwrap()
        };
        let next = format!("/v1/query/{id}/page/0");
        let mut body = query_reply(
            (request.method == "POST").then_some(next.as_str()),
            json!([["1"]]),
        );
        body["id"] = json!(id);
        MockReply::json(body)
    })
    .await;
    let client = client(&server).await;
    let mut first = tracked_pages(&client, false).await;
    let second = client.start_query("SELECT 2", false, None).await.unwrap();
    let (mut second, _, _) = second.wait_for_schema(false).await.unwrap();
    assert_eq!(client.queries_need_heartbeat.lock().len(), 2);
    first.next().await.unwrap().unwrap();
    {
        let queries = client.queries_need_heartbeat.lock();
        assert!(!queries.contains_key("q1"));
        assert!(queries.contains_key("q2"));
    }
    second.next().await.unwrap().unwrap();
    second.next().await.unwrap().unwrap();
    assert!(client.queries_need_heartbeat.lock().is_empty());
    server.assert_native_requests(4);
}

#[tokio::test]
async fn page_errors_unregister_before_none_or_drop() {
    for failure in ["http", "decode", "query"] {
        let server = MockServer::new(move |request| {
            if request.method == "POST" {
                return MockReply::json(query_reply(Some("/v1/query/q1/page/0"), json!([["1"]])));
            }
            match failure {
                "http" => MockReply {
                    status: 404,
                    body: "not found".into(),
                    gate: None,
                },
                "decode" => MockReply::json(json!({"invalid":"response"})),
                "query" => {
                    let mut body = query_reply(None, json!([]));
                    body["error"] = json!({"code":5100,"message":"query failed"});
                    MockReply::json(body)
                }
                _ => unreachable!(),
            }
        })
        .await;
        let client = client(&server).await;
        let mut pages = tracked_pages(&client, false).await;
        assert!(pages.next().await.unwrap().is_err());
        assert!(client.queries_need_heartbeat.lock().is_empty());
        client.try_heartbeat().await.unwrap();
        assert!(pages.next().await.is_none());
        drop(pages);
        server.assert_native_requests(2);
    }
}

#[tokio::test]
async fn early_drop_finalizes_once_on_the_tracked_node() {
    let finalized = Arc::new(Notify::new());
    let notification = finalized.clone();
    let server = MockServer::new(move |request| {
        if request.path.ends_with("/final") {
            assert_eq!(request.method, "POST");
            assert_eq!(request.headers["x-databend-sticky-node"], "node-1");
            notification.notify_one();
        }
        MockReply::json(query_reply(Some("/v1/query/q1/page/0"), json!([["1"]])))
    })
    .await;
    let client = client(&server).await;
    let pages = tracked_pages(&client, false).await;
    drop(pages);
    assert!(client.queries_need_heartbeat.lock().is_empty());
    server.wait_for(finalized.notified()).await;
    client.finalize_query("q1");
    client.try_heartbeat().await.unwrap();
    server.assert_native_requests(2);
}

#[tokio::test]
async fn drop_during_final_request_unregisters_without_duplicate_final() {
    let started = Arc::new(Notify::new());
    let notification = started.clone();
    let gate = Arc::new(Notify::new());
    let response_gate = gate.clone();
    let server = MockServer::new(move |request| {
        let mut reply = MockReply::json(query_reply(Some("/v1/query/q1/final"), json!([["1"]])));
        if request.method == "GET" {
            assert_eq!(request.path, "/v1/query/q1/final");
            reply.gate = Some(response_gate.clone());
            notification.notify_one();
        }
        reply
    })
    .await;
    let client = client(&server).await;
    let mut pages = tracked_pages(&client, false).await;
    server
        .wait_for(async {
            tokio::select! {
                _ = pages.next() => panic!("expected final request to remain pending"),
                _ = started.notified() => {},
            }
        })
        .await;
    assert!(client.queries_need_heartbeat.lock().contains_key("q1"));
    drop(pages);
    assert!(client.queries_need_heartbeat.lock().is_empty());
    client.finalize_query("q1");
    client.try_heartbeat().await.unwrap();
    server.assert_native_requests(2);
    gate.notify_one();
}

#[tokio::test]
async fn drop_during_page_request_still_finalizes_once() {
    let started = Arc::new(Notify::new());
    let page_notification = started.clone();
    let finalized = Arc::new(Notify::new());
    let final_notification = finalized.clone();
    let gate = Arc::new(Notify::new());
    let response_gate = gate.clone();
    let server = MockServer::new(move |request| {
        let mut reply = MockReply::json(query_reply(Some("/v1/query/q1/page/0"), json!([["1"]])));
        if request.method == "GET" {
            reply.gate = Some(response_gate.clone());
            page_notification.notify_one();
        } else if request.path.ends_with("/final") {
            assert_eq!(request.headers["x-databend-sticky-node"], "node-1");
            final_notification.notify_one();
        }
        reply
    })
    .await;
    let client = client(&server).await;
    let mut pages = tracked_pages(&client, false).await;
    server
        .wait_for(async {
            tokio::select! {
                _ = pages.next() => panic!("expected page request to remain pending"),
                _ = started.notified() => {},
            }
        })
        .await;
    drop(pages);
    assert!(client.queries_need_heartbeat.lock().is_empty());
    server.wait_for(finalized.notified()).await;
    client.finalize_query("q1");
    server.assert_native_requests(3);
    gate.notify_one();
}
