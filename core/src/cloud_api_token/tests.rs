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
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::{JoinHandle, JoinSet};

pub(crate) struct MockRequest {
    pub target: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

pub(crate) struct MockResponse {
    pub status: u16,
    pub body: String,
    pub headers: Vec<(&'static str, String)>,
    pub delay: Duration,
}

impl MockResponse {
    pub fn json(value: Value) -> Self {
        Self {
            status: 200,
            body: value.to_string(),
            headers: vec![],
            delay: Duration::ZERO,
        }
    }
}

pub(crate) struct MockServer {
    pub url: Url,
    pub requests: Arc<Mutex<Vec<MockRequest>>>,
    task: JoinHandle<()>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl MockServer {
    pub async fn new(
        handler: impl Fn(&MockRequest) -> MockResponse + Send + Sync + 'static,
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
                            let request = MockRequest {
                                target: head.lines().next().unwrap().split_whitespace().nth(1).unwrap().into(),
                                headers: head.lines().skip(1).filter_map(|line| line.split_once(':'))
                                    .map(|(key, value)| (key.to_ascii_lowercase(), value.trim().to_string())).collect(),
                                body: bytes[end..end + length].to_vec(),
                            };
                            let reply = handler(&request);
                            captured.lock().push(request);
                            tokio::time::sleep(reply.delay).await;
                            let mut response = format!("HTTP/1.1 {} Mock\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n", reply.status, reply.body.len());
                            for (key, value) in reply.headers { response.push_str(&format!("{key}: {value}\r\n")); }
                            response.push_str("\r\n");
                            response.push_str(&reply.body);
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

pub(crate) fn token_reply(token: &str) -> Value {
    json!({"success":true,"data":{
        "access_token":token,"token_type":"Bearer","tenant_id":"tnexample",
        "username":"alice@example.com","expires_in":900,
        "expires_at":(Utc::now() + chrono::Duration::minutes(15)).to_rfc3339()
    }})
}

pub(crate) fn mock_auth(gateway: Url, exchange: Url) -> Arc<CloudApiTokenAuth> {
    Arc::new(
        CloudApiTokenAuth::new(
            gateway,
            "tnexample".into(),
            exchange,
            "dbc_test_secret".into(),
        )
        .unwrap(),
    )
}

pub(crate) fn expire(auth: &CloudApiTokenAuth) {
    auth.cached.lock().as_mut().unwrap().refresh_at = Instant::now() - Duration::from_secs(1);
}

#[test]
fn cloud_gateway_target_is_strict() {
    for (host, tenant, api) in [
        (
            "tnexample.gw.aws-us-east-2.default.databend.com",
            "tnexample",
            "api.databend.com",
        ),
        (
            "tnexample--wh.gw.aws-us-east-2.default.databend.com",
            "tnexample",
            "api.databend.com",
        ),
        (
            "tnexample.gw.aliyun-cn-beijing.default.databend.cn",
            "tnexample",
            "api.databend.cn",
        ),
    ] {
        let (actual_tenant, endpoint) =
            CloudApiTokenAuth::target(&Url::parse(&format!("https://{host}")).unwrap()).unwrap();
        assert_eq!(actual_tenant, tenant);
        assert_eq!(
            endpoint,
            Url::parse(&format!("https://{api}{EXCHANGE_PATH}")).unwrap()
        );
    }
    for url in [
        "http://tnexample.gw.aws-us-east-2.default.databend.com",
        "https://localhost",
        "https://tnexample.gw.aws-us-east-2.default.databend.com.evil.test",
        "https://tnexample.not-gw.aws-us-east-2.default.databend.com",
        "https://root.gw.aws-us-east-2.default.databend.com",
        "https://tnexample.gw.azure-centralus.default.azure.databend.com",
    ] {
        assert!(
            CloudApiTokenAuth::target(&Url::parse(url).unwrap()).is_err(),
            "{url}"
        );
    }
}

#[tokio::test]
async fn cloud_exchange_caches_and_serializes_refresh() {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let server = MockServer::new(move |request| {
        assert_eq!(request.headers["x-databendcloud-token"], "dbc_test_secret");
        assert!(!request.headers.contains_key("authorization"));
        assert!(!request.headers.contains_key("cookie"));
        assert_eq!(
            serde_json::from_slice::<Value>(&request.body).unwrap(),
            json!({"tenant_id":"tnexample"})
        );
        let mut reply = MockResponse::json(token_reply(&format!(
            "jwt-{}",
            seen.fetch_add(1, Ordering::SeqCst)
        )));
        reply.delay = Duration::from_millis(20);
        reply
    })
    .await;
    let auth = mock_auth(server.url.clone(), server.url.join(EXCHANGE_PATH).unwrap());
    let mut clients = JoinSet::new();
    for _ in 0..20 {
        let auth = auth.clone();
        clients.spawn(async move {
            auth.prepare().await.unwrap();
        });
    }
    while let Some(result) = clients.join_next().await {
        result.unwrap();
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(auth.username(), "alice@example.com");
    let request = auth
        .wrap(Client::new().get(server.url.clone()))
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(request.headers()[AUTHORIZATION], "Bearer jwt-0");
    assert!(request.headers()[AUTHORIZATION].is_sensitive());
    assert!(!request.headers().contains_key("x-databendcloud-token"));
    expire(&auth);
    auth.prepare().await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 2);
    let request = auth
        .wrap(Client::new().get(server.url.clone()))
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(request.headers()[AUTHORIZATION], "Bearer jwt-1");
    assert!(auth
        .wrap(Client::new().get("https://untrusted.example/"))
        .is_err());
}

#[tokio::test]
async fn cloud_exchange_never_follows_redirect_or_echoes_errors() {
    let other = MockServer::new(|_| MockResponse::json(json!({}))).await;
    for status in [302, 307, 401, 403, 429, 500] {
        let location = other.url.to_string();
        let server = MockServer::new(move |_| MockResponse {
            status,
            body: "dbc_test_secret reflected-jwt".into(),
            headers: vec![("Location", location.clone()), ("Retry-After", "1".into())],
            delay: Duration::ZERO,
        })
        .await;
        let auth = mock_auth(server.url.clone(), server.url.clone());
        let err = auth.prepare().await.unwrap_err();
        assert_eq!(err.status_code().unwrap().as_u16(), status);
        assert!(!err.to_string().contains("dbc_test_secret"));
        assert!(!format!("{err:?}").contains("reflected-jwt"));
        assert!(!auth.fresh());
    }
    assert!(other.requests.lock().is_empty());
}

#[tokio::test]
async fn cloud_exchange_validates_response_without_secret_leaks() {
    let mut replies = Vec::new();
    for (key, value) in [
        ("tenant_id", json!("tnother")),
        ("token_type", json!("Basic")),
        ("username", json!("")),
        ("access_token", json!("")),
        ("access_token", json!("bad\r\nheader")),
        ("expires_in", json!(0)),
        ("expires_at", json!("reflected-secret")),
        (
            "expires_at",
            json!((Utc::now() - chrono::Duration::minutes(1)).to_rfc3339()),
        ),
    ] {
        let mut reply = token_reply("reflected-jwt");
        reply["data"][key] = value;
        replies.push(reply);
    }
    let mut failed = token_reply("reflected-jwt");
    failed["success"] = json!(false);
    replies.push(failed);
    for reply in replies {
        let server = MockServer::new(move |_| MockResponse::json(reply.clone())).await;
        let auth = mock_auth(server.url.clone(), server.url.clone());
        let err = auth.prepare().await.unwrap_err();
        let message = format!("{err:?}");
        assert!(!message.contains("reflected-secret"));
        assert!(!message.contains("reflected-jwt"));
        assert!(!auth.fresh());
    }
}

#[tokio::test]
async fn cloud_exchange_rejects_oversized_and_malformed_bodies() {
    for body in [
        "not-json dbc_test_secret".to_string(),
        "x".repeat(MAX_RESPONSE_BYTES + 1),
    ] {
        let server = MockServer::new(move |_| MockResponse {
            status: 200,
            body: body.clone(),
            headers: vec![],
            delay: Duration::ZERO,
        })
        .await;
        let auth = mock_auth(server.url.clone(), server.url.clone());
        let err = auth.prepare().await.unwrap_err();
        assert!(!format!("{err:?}").contains("dbc_test_secret"));
        assert!(!auth.fresh());
    }
}

#[tokio::test]
async fn cloud_canceled_refresh_does_not_hold_the_pool_lock() {
    let started = Arc::new(tokio::sync::Notify::new());
    let notification = started.clone();
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let server = MockServer::new(move |_| {
        let mut reply = MockResponse::json(token_reply("jwt-after-cancel"));
        if seen.fetch_add(1, Ordering::SeqCst) == 0 {
            reply.delay = Duration::from_secs(10);
            notification.notify_one();
        }
        reply
    })
    .await;
    let auth = mock_auth(server.url.clone(), server.url.clone());
    let first = auth.clone();
    let task = tokio::spawn(async move { first.prepare().await });
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    task.abort();
    let _ = task.await;
    tokio::time::timeout(Duration::from_secs(2), auth.prepare())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert!(auth.fresh());
}

#[tokio::test]
async fn cloud_auth_pool_sharing_does_not_retain_credentials() {
    let gateway = Url::parse("https://tnexample.gw.aws-us-east-2.default.databend.com/").unwrap();
    let secret = format!("dbc_{}", uuid::Uuid::new_v4());
    let first = CloudApiTokenAuth::for_gateway(gateway.clone(), secret.clone()).unwrap();
    let second = CloudApiTokenAuth::for_gateway(gateway.clone(), secret).unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    let other = CloudApiTokenAuth::for_gateway(gateway, "dbc_different_secret".into()).unwrap();
    assert!(!Arc::ptr_eq(&first, &other));
    let weak = Arc::downgrade(&first);
    drop(first);
    drop(second);
    assert!(weak.upgrade().is_none());
}
