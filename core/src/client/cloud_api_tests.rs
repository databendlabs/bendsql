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
                            let reply = handler(&request);
                            captured.lock().push(request);
                            if let Some((_, delay)) = reply.headers.iter().find(|(name, _)| *name == "X-Mock-Delay") {
                                tokio::time::sleep(Duration::from_millis(delay.parse().unwrap())).await;
                            }
                            let length = reply.body.len() + if reply.headers.iter().any(|(name, _)| *name == "X-Mock-Truncate") { 100 } else { 0 };
                            let mut response = format!("HTTP/1.1 {} Mock\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n", reply.status, length);
                            for (key, value) in reply.headers { response.push_str(&format!("{key}: {value}\r\n")); }
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
    client.is_cloud_webapi = true;
    client.auth = Arc::new(CloudApiAuth::for_test(
        server.url.clone(),
        TOKEN.into(),
        ORG.into(),
    ));
    client.build_client(None).await.unwrap();
    client
}

#[tokio::test]
async fn cloud_gateway_management_results_do_not_use_engine_lifecycle() {
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
                if with_rows {
                    // Even if these engine fields are present, a gateway-local
                    // response must never register a heartbeat or follow them.
                    body["node_id"] = json!("not-an-engine-query");
                    body["next_uri"] = json!("/v1/query/management-query/page/0");
                    body["final_uri"] = json!("/v1/query/management-query/final");
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
async fn cloud_gateway_defaults_do_not_relax_engine_decoding_or_hide_errors() {
    let server = MockServer::new(|_| {
        MockReply::json(json!({
            "id":"engine-query", "schema":[], "data":[], "state":"Succeeded"
        }))
    })
    .await;
    let engine_client = Arc::new(client(&server).await);
    let err = engine_client
        .query_all("SELECT 1", None)
        .await
        .err()
        .unwrap();
    assert!(err.to_string().contains("missing field `stats`"));

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
    let dsn = format!("databend://ignored:%FF@api.databend.com/default?cloud_api_token={TOKEN}&org_slug={ORG}&warehouse=wh");
    let client = APIClient::from_dsn(&dsn).await.unwrap();
    assert!(client.is_cloud_webapi);
    assert!(!client.auth.can_reload());
    assert!(!format!("{:?}", client.session_state()).contains(TOKEN));
    assert!(!format!("{:?}", client.session_state()).contains("org_slug"));
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
        "org_slug=second",
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
        "databend://api.databend.cn/default?cloud_api_token=fake&org_slug=org-example",
        "databend:///?cloud_api_token=fake&org_slug=org-example",
    ] {
        assert!(APIClient::from_dsn(dsn).await.is_ok(), "{dsn}");
    }
    for dsn in [
        "databend://api.databend.com?cloud_api_token=fake",
        "databend://api.databend.com?org_slug=org-example",
        "databend://api.databend.com?cloud_api_token=&org_slug=org-example",
        "databend://api.databend.com?cloud_api_token=fake&org_slug=",
        "databend://api.databend.com?cloud_api_token=fake&org_slug=%0D%0Ainjected",
        "databend://api.databend.com.evil.test?cloud_api_token=fake&org_slug=org-example",
        "databend://tnexample.gw.aws-us-east-2.default.databend.com?cloud_api_token=fake&org_slug=org-example",
        "databend://localhost?cloud_api_token=fake&org_slug=org-example",
        "databend://api.databend.com:8443?cloud_api_token=fake&org_slug=org-example",
        "databend+http://api.databend.com?cloud_api_token=fake&org_slug=org-example",
    ] { assert!(APIClient::from_dsn(dsn).await.is_err(), "{dsn}"); }
}

#[tokio::test]
async fn cloud_login_404_and_query_lifecycle_stay_on_webapi() {
    let server = MockServer::new(|request| {
        if request.path.starts_with("/v1/session/login") {
            let mut reply = MockReply::error(404);
            reply
                .headers
                .push(("Set-Cookie", "console-cookie=not-for-api; Path=/".into()));
            reply
        } else {
            MockReply::json(query_reply())
        }
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
    assert_eq!(requests.len(), 5);
    for request in requests.iter() {
        assert_eq!(request.headers["x-databendcloud-token"], TOKEN);
        assert_eq!(request.headers["x-databendcloud-org"], ORG);
        assert_eq!(request.headers["x-databendcloud-warehouse"], "wh");
        assert!(!request.headers.contains_key("authorization"));
        assert!(!request.headers.contains_key("cookie"));
        assert!(!request.headers.contains_key("x-databend-warehouse"));
    }
    assert_eq!(requests[1].method, "POST");
    let body: Value = serde_json::from_slice(&requests[1].body).unwrap();
    assert_eq!(body["string_fields"], true);
    for key in [
        "pagination",
        "params",
        "stage_attachment",
        "arrow_result_version_max",
        "cloud_api_token",
        "org_slug",
    ] {
        assert!(body.get(key).is_none(), "{key}");
    }
    assert_eq!(
        (requests[2].method.as_str(), requests[2].path.as_str()),
        ("GET", "/v1/query/q1/page/0")
    );
    assert_eq!(
        (requests[3].method.as_str(), requests[3].path.as_str()),
        ("POST", "/v1/query/q1/kill")
    );
    assert_eq!(
        (requests[4].method.as_str(), requests[4].path.as_str()),
        ("GET", "/v1/query/q1/final")
    );
}

#[tokio::test]
async fn cloud_login_does_not_hide_auth_failures_or_switch_credentials() {
    for status in [401, 403, 500] {
        let server = MockServer::new(move |_| MockReply::error(status)).await;
        let mut client = client(&server).await;
        let err = client.login().await.unwrap_err();
        assert_eq!(err.status_code().unwrap().as_u16(), status);
        assert!(!format!("{err:?}").contains(TOKEN));
        assert_eq!(server.requests.lock().len(), 1);
    }
    let server = MockServer::new(|_| MockReply::json(json!({"version":"1.2.952", "tokens":{"session_token":"do-not-send","refresh_token":"do-not-send"}}))).await;
    let mut client = client(&server).await;
    client.login().await.unwrap();
    assert!(client.session_token_info.is_none());
    assert!(client.is_cloud_webapi);
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
    for kind in [
        RequestKind::Login,
        RequestKind::QueryPage,
        RequestKind::QueryFinal,
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let server = MockServer::new(move |request| {
            if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                return MockReply::error(503);
            }
            if request.path.starts_with("/v1/session/login") {
                MockReply::json(json!({"version":"1.2.952"}))
            } else {
                MockReply::json(query_reply())
            }
        })
        .await;
        let mut client = client(&server).await;
        match kind {
            RequestKind::Login => client.login().await.unwrap(),
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
            let server = MockServer::new(move |_| {
                let mut reply = MockReply::json(query_reply());
                if seen.fetch_add(1, Ordering::SeqCst) == 0 {
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
    let client = client(&server).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
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
