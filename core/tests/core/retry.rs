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

use std::io;
use std::sync::{Arc, Mutex};

use databend_client::{APIClient, Error};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct RecordedRequest {
    method: String,
    path: String,
    authorization: Option<String>,
}

type ResponseHandler = dyn Fn(usize, &RecordedRequest) -> (u16, &'static str, String) + Send + Sync;

fn ok_query_response(value: &str) -> String {
    format!(
        r#"{{
  "id": "query-id",
  "node_id": null,
  "session_id": null,
  "session": null,
  "schema": [{{"name":"c1","type":"String"}}],
  "data": [["{value}"]],
  "state": "Succeeded",
  "settings": null,
  "error": null,
  "warnings": null,
  "stats": {{
    "scan_progress": {{"rows":0,"bytes":0}},
    "write_progress": {{"rows":0,"bytes":0}},
    "result_progress": {{"rows":0,"bytes":0}},
    "spill_progress": {{"file_nums":0,"bytes":0}},
    "running_time_ms": 0.0,
    "total_scan": null
  }},
  "result_timeout_secs": null,
  "stats_uri": null,
  "final_uri": null,
  "next_uri": null,
  "kill_uri": null
}}"#
    )
}

fn unauthorized_json_body() -> String {
    r#"{"error":{"code":3900,"message":"unauthorized","detail":null}}"#.to_string()
}

fn logic_error_json_body(code: u16, message: &str) -> String {
    format!(r#"{{"error":{{"code":{code},"message":"{message}","detail":null}}}}"#)
}

fn query_response_with_next_uri(value: &str, next_uri: &str) -> String {
    format!(
        r#"{{
  "id": "query-id",
  "node_id": null,
  "session_id": null,
  "session": null,
  "schema": [{{"name":"c1","type":"String"}}],
  "data": [["{value}"]],
  "state": "Running",
  "settings": null,
  "error": null,
  "warnings": null,
  "stats": {{
    "scan_progress": {{"rows":0,"bytes":0}},
    "write_progress": {{"rows":0,"bytes":0}},
    "result_progress": {{"rows":0,"bytes":0}},
    "spill_progress": {{"file_nums":0,"bytes":0}},
    "running_time_ms": 0.0,
    "total_scan": null
  }},
  "result_timeout_secs": null,
  "stats_uri": null,
  "final_uri": null,
  "next_uri": "{next_uri}",
  "kill_uri": null
}}"#
    )
}

fn build_dsn(port: u16, retry_count: u32, extra: &str) -> String {
    if extra.is_empty() {
        format!(
            "databend://127.0.0.1:{port}/default?sslmode=disable&login=disable&retry_delay_secs=0&retry_count={retry_count}"
        )
    } else {
        format!(
            "databend://127.0.0.1:{port}/default?sslmode=disable&login=disable&retry_delay_secs=0&retry_count={retry_count}&{extra}"
        )
    }
}

fn spawn_test_server(
    handler: Arc<ResponseHandler>,
) -> (u16, Arc<Mutex<Vec<RecordedRequest>>>, JoinHandle<()>) {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    std_listener.set_nonblocking(true).unwrap();
    let port = std_listener.local_addr().unwrap().port();
    let listener = TcpListener::from_std(std_listener).unwrap();

    let requests = Arc::new(Mutex::new(Vec::<RecordedRequest>::new()));
    let requests_for_task = Arc::clone(&requests);
    let handle = tokio::spawn(async move {
        let mut idx = 0usize;
        while let Ok((mut stream, _)) = listener.accept().await {
            let req = match read_request(&mut stream).await {
                Ok(req) => req,
                Err(_) => continue,
            };
            requests_for_task.lock().unwrap().push(req.clone());
            let (status, content_type, body) = handler(idx, &req);
            idx += 1;
            let _ = write_response(&mut stream, status, content_type, &body).await;
        }
    });

    (port, requests, handle)
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

async fn read_request(stream: &mut TcpStream) -> io::Result<RecordedRequest> {
    let mut buf = Vec::<u8>::new();
    let mut chunk = [0u8; 1024];

    let header_end = loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before request headers",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find_header_end(&buf) {
            break pos;
        }
        if buf.len() > 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request headers too large",
            ));
        }
    };

    let headers_end = header_end + 4;
    let headers_bytes = &buf[..header_end];
    let headers_str = std::str::from_utf8(headers_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid request headers"))?;

    let mut lines = headers_str.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request line"))?;
    let mut request_line_parts = request_line.split_whitespace();
    let method = request_line_parts.next().unwrap_or_default().to_string();
    let path = request_line_parts.next().unwrap_or_default().to_string();

    let mut content_length = 0usize;
    let mut authorization = None::<String>;

    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        let key = k.trim().to_ascii_lowercase();
        let value = v.trim().to_string();
        if key == "content-length" {
            content_length = value.parse::<usize>().unwrap_or(0);
        }
        if key == "authorization" {
            authorization = Some(value);
        }
    }

    let already_read_body = buf.len().saturating_sub(headers_end);
    if content_length > already_read_body {
        let mut remaining = vec![0u8; content_length - already_read_body];
        stream.read_exact(&mut remaining).await?;
    }

    Ok(RecordedRequest {
        method,
        path,
        authorization,
    })
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &str,
) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        401 => "Unauthorized",
        503 => "Service Unavailable",
        _ => "Unknown",
    };
    let body_bytes = body.as_bytes();
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body_bytes.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body_bytes).await?;
    stream.shutdown().await
}

fn create_temp_token_file(initial_token: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "databend-client-retry-token-{}-{}.txt",
        std::process::id(),
        Uuid::new_v4()
    ));
    std::fs::write(&path, initial_token).unwrap();
    path
}

#[tokio::test]
async fn retry_503_then_success() {
    let handler: Arc<ResponseHandler> = Arc::new(|idx, req| {
        assert_eq!(req.method, "POST");
        assert_eq!(req.path, "/v1/query");
        if idx == 0 {
            (
                503,
                "text/plain",
                "server unavailable, retry later".to_string(),
            )
        } else {
            (200, "application/json", ok_query_response("42"))
        }
    });
    let (port, requests, server) = spawn_test_server(handler);

    let dsn = build_dsn(port, 2, "");
    let client = APIClient::new(&dsn, None).await.unwrap();
    let result = client.query_all("select 42").await.unwrap();

    assert_eq!(result.data, vec![vec![Some("42".to_string())]]);
    assert_eq!(requests.lock().unwrap().len(), 2);

    server.abort();
}

#[tokio::test]
async fn retry_401_with_access_token_file_reload_then_success() {
    let token_file = create_temp_token_file("old-token");
    let token_file_for_server = token_file.clone();

    let handler: Arc<ResponseHandler> = Arc::new(move |idx, req| {
        assert_eq!(req.method, "POST");
        assert_eq!(req.path, "/v1/query");
        if idx == 0 {
            assert_eq!(
                req.authorization.as_deref(),
                Some("Bearer old-token"),
                "first request should use token before reload"
            );
            std::fs::write(&token_file_for_server, "new-token").unwrap();
            (401, "application/json", unauthorized_json_body())
        } else {
            assert_eq!(
                req.authorization.as_deref(),
                Some("Bearer new-token"),
                "second request should use reloaded token file value"
            );
            (200, "application/json", ok_query_response("reloaded"))
        }
    });
    let (port, requests, server) = spawn_test_server(handler);

    let dsn = build_dsn(
        port,
        3,
        &format!("access_token_file={}", token_file.to_string_lossy()),
    );
    let client = APIClient::new(&dsn, None).await.unwrap();
    let result = client.query_all("select 'reloaded'").await.unwrap();

    assert_eq!(result.data, vec![vec![Some("reloaded".to_string())]]);
    assert_eq!(requests.lock().unwrap().len(), 2);

    server.abort();
    let _ = std::fs::remove_file(&token_file);
}

#[tokio::test]
async fn retry_401_auth_reload_stops_at_max_retries() {
    let token_file = create_temp_token_file("still-invalid-token");
    let handler: Arc<ResponseHandler> = Arc::new(|_idx, req| {
        assert_eq!(req.method, "POST");
        assert_eq!(req.path, "/v1/query");
        (401, "application/json", unauthorized_json_body())
    });
    let (port, requests, server) = spawn_test_server(handler);

    let retry_count = 3u32;
    let dsn = build_dsn(
        port,
        retry_count,
        &format!("access_token_file={}", token_file.to_string_lossy()),
    );
    let client = APIClient::new(&dsn, None).await.unwrap();
    let err = match client.query_all("select 1").await {
        Ok(_) => panic!("expected unauthorized error"),
        Err(err) => err,
    };

    match err {
        Error::AuthFailure(ec) => assert_eq!(ec.code, 3900),
        other => panic!("expected AuthFailure, got {other}"),
    }
    assert_eq!(requests.lock().unwrap().len(), retry_count as usize);

    server.abort();
    let _ = std::fs::remove_file(&token_file);
}

#[tokio::test]
async fn start_query_404_keeps_logic_error() {
    let handler: Arc<ResponseHandler> = Arc::new(|idx, req| {
        assert_eq!(idx, 0);
        assert_eq!(req.method, "POST");
        assert_eq!(req.path, "/v1/query");
        (
            404,
            "application/json",
            logic_error_json_body(2001, "route /v1/query not found"),
        )
    });
    let (port, _requests, server) = spawn_test_server(handler);

    let dsn = build_dsn(port, 2, "");
    let client = APIClient::new(&dsn, None).await.unwrap();
    let err = match client.query_all("select 1").await {
        Ok(_) => panic!("expected logic error"),
        Err(err) => err,
    };
    match err {
        Error::Logic(status, ec) => {
            assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
            assert_eq!(ec.code, 2001);
            assert_eq!(ec.message, "route /v1/query not found");
        }
        other => panic!("expected Logic(404), got {other}"),
    }

    server.abort();
}

#[tokio::test]
async fn query_page_404_maps_to_query_not_found() {
    let handler: Arc<ResponseHandler> = Arc::new(|idx, req| match idx {
        0 => {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/query");
            (
                200,
                "application/json",
                query_response_with_next_uri("first-page", "/v1/query/page/1"),
            )
        }
        1 => {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/query/page/1");
            (
                404,
                "application/json",
                logic_error_json_body(1003, "query no longer exists"),
            )
        }
        _ => panic!("unexpected request index: {idx}"),
    });
    let (port, _requests, server) = spawn_test_server(handler);

    let dsn = build_dsn(port, 2, "");
    let client = APIClient::new(&dsn, None).await.unwrap();
    let err = match client.query_all("select 1").await {
        Ok(_) => panic!("expected QueryNotFound"),
        Err(err) => err,
    };
    match err {
        Error::QueryNotFound(msg) => assert_eq!(msg, "query no longer exists"),
        other => panic!("expected QueryNotFound, got {other}"),
    }

    server.abort();
}
