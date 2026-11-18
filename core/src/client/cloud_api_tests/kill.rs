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

//! `kill_query` routing: session-bound `/kill` for this connection's queries,
//! detached `KILL QUERY` SQL for everything else.

use super::*;

fn running_reply(id: &str) -> Value {
    let mut reply = query_reply();
    reply["id"] = json!(id);
    reply["node_id"] = json!("n1");
    reply["state"] = json!("Running");
    reply["next_uri"] = json!(format!("/v1/query/{id}/page/1"));
    reply
}

fn kill_statement_reply(error: Option<Value>) -> Value {
    let mut reply = query_reply();
    reply["id"] = json!("k1");
    reply["data"] = json!([]);
    reply["schema"] = json!([]);
    // Must not leak into the connection's session.
    reply["session"] = json!({"database": "other", "txn_state": "AutoCommit"});
    match error {
        Some(error) => {
            reply["state"] = json!("Failed");
            reply["error"] = error;
            reply["next_uri"] = Value::Null;
        }
        None => reply["next_uri"] = json!("/v1/query/k1/final"),
    }
    reply
}

fn ok_reply() -> MockReply {
    MockReply::json(json!({}))
}

fn not_found_reply() -> MockReply {
    // The server's query_id_not_found response.
    let mut reply = MockReply::json(
        json!({"error": {"code": 404, "message": "Query ID q not found on node n1"}}),
    );
    reply.status = 404;
    reply
}

fn query_failed_code(error: &Error) -> Option<u16> {
    match error {
        Error::WithContext { inner, .. } => query_failed_code(inner),
        Error::QueryFailed(code) => Some(code.code),
        _ => None,
    }
}

fn requests(server: &MockServer) -> Vec<(String, String)> {
    server
        .requests
        .lock()
        .iter()
        .map(|r| (r.method.clone(), r.path.clone()))
        .collect()
}

#[tokio::test]
async fn owned_query_is_killed_through_its_session_on_its_node() {
    for cloud in [false, true] {
        let server = MockServer::new(|request| match request.path.as_str() {
            "/v1/query" => MockReply::json(running_reply("q1")),
            "/v1/query/q1/kill" => ok_reply(),
            path => panic!("unexpected {path}"),
        })
        .await;
        let client = query_id_client(&server, cloud).await;
        let pages = client.start_query("SELECT 1", false, None).await.unwrap();
        client.kill_query("q1").await.unwrap();

        let recorded = server.requests.lock();
        assert_eq!(recorded.len(), 2, "no KILL statement is submitted");
        assert_eq!(
            (recorded[1].method.as_str(), recorded[1].path.as_str()),
            ("POST", "/v1/query/q1/kill")
        );
        assert_eq!(recorded[1].headers["x-databend-sticky-node"], "n1");
        drop(recorded);
        drop(pages);
    }
}

#[tokio::test]
async fn kill_query_without_first_response_returns_server_error_once_submission_ended() {
    // Once the submission is gone the query is not waiting for registration,
    // so a 404 is the real answer and reaches the caller without retries.
    let server = MockServer::with_query_gate(
        |request| match request.path.as_str() {
            "/v1/query" => MockReply::json(running_reply("q1")),
            _ => not_found_reply(),
        },
        Some(Arc::new(Notify::new())),
    )
    .await;
    let client = query_id_client(&server, false).await;
    let query_client = client.clone();
    let query =
        tokio::spawn(async move { query_client.start_query("SELECT 1", false, None).await });
    let sent_id = received_query_id(&server).await;
    query.abort();
    let _ = query.await;
    assert!(client.kill_query(&sent_id).await.is_err());
    assert_eq!(
        requests(&server)[1],
        ("POST".to_string(), format!("/v1/query/{sent_id}/kill"))
    );
}

#[tokio::test]
async fn other_query_is_killed_by_detached_sql() {
    for cloud in [false, true] {
        let server = MockServer::new(|request| match request.path.as_str() {
            "/v1/query" => MockReply::json(kill_statement_reply(None)),
            "/v1/query/k1/final" => ok_reply(),
            path => panic!("unexpected {path}"),
        })
        .await;
        let client = query_id_client(&server, cloud).await;
        {
            let mut state = client.session_state.lock();
            state.txn_state = Some(TXN_STATE_ACTIVE.into());
            state.need_sticky = Some(true);
            state.role = Some("r1".into());
            state.secondary_roles = Some(vec!["r2".into()]);
            state.set_database("db1");
        }
        client.set_last_node_id("txn-node".into());
        client.set_last_query_id(Some("previous-query".into()));
        let route_hint = client.route_hint.current();

        client.kill_query(r"it's\x").await.unwrap();

        let recorded = server.requests.lock();
        assert_eq!(recorded.len(), 2);
        let body: Value = serde_json::from_slice(&recorded[0].body).unwrap();
        assert_eq!(body["sql"], r"KILL QUERY 'it''s\\x'");
        // Only roles travel; never the transaction or the sticky node.
        assert_eq!(
            body["session"],
            json!({"role": "r1", "secondary_roles": ["r2"]})
        );
        assert!(!recorded[0].headers.contains_key("x-databend-sticky-node"));
        assert_eq!(recorded[0].headers["x-databend-route-hint"], route_hint);
        assert_ne!(recorded[0].headers["x-databend-query-id"], "previous-query");
        assert_eq!(recorded[1].path, "/v1/query/k1/final");
        drop(recorded);

        assert_eq!(client.last_query_id().as_deref(), Some("previous-query"));
        assert_eq!(client.route_hint.current(), route_hint);
        assert_eq!(client.last_node_id().as_deref(), Some("txn-node"));
        let state = client.session_state();
        assert_eq!(state.txn_state.as_deref(), Some(TXN_STATE_ACTIVE));
        assert_eq!(state.database.as_deref(), Some("db1"));
    }
}

#[tokio::test]
async fn detached_kill_returns_server_error_codes() {
    for code in [1053, 1063] {
        let server = MockServer::new(move |_| {
            MockReply::json(kill_statement_reply(Some(
                json!({"code": code, "message": "denied"}),
            )))
        })
        .await;
        let client = query_id_client(&server, false).await;
        let err = client.kill_query("other").await.unwrap_err();
        assert_eq!(query_failed_code(&err), Some(code), "{err:?}");
        assert!(format!("{err}").contains("other"), "{err}");
        assert_eq!(requests(&server).len(), 1);
    }
}

#[tokio::test]
async fn finished_query_is_no_longer_killed_through_its_session() {
    let server = MockServer::new(|request| {
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        if body["sql"] == "SELECT 1" {
            let mut reply = running_reply("q1");
            reply["state"] = json!("Succeeded");
            reply["next_uri"] = Value::Null;
            MockReply::json(reply)
        } else {
            MockReply::json(kill_statement_reply(Some(
                json!({"code": 1053, "message": "not found"}),
            )))
        }
    })
    .await;
    let client = query_id_client(&server, false).await;
    client.query_all("SELECT 1", None).await.unwrap();
    assert!(client.kill_query("q1").await.is_err());
    let recorded = server.requests.lock();
    assert_eq!(recorded.len(), 2);
    let body: Value = serde_json::from_slice(&recorded[1].body).unwrap();
    assert_eq!(body["sql"], "KILL QUERY 'q1'");
}

#[tokio::test]
async fn dropped_result_stays_owned_and_fully_read_one_is_forgotten() {
    let server =
        MockServer::new(
            |request| match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/v1/query") => {
                    let body: Value = serde_json::from_slice(&request.body).unwrap();
                    if body["sql"].as_str().unwrap().starts_with("KILL") {
                        MockReply::json(kill_statement_reply(None))
                    } else {
                        MockReply::json(running_reply("q1"))
                    }
                }
                ("GET", "/v1/query/q1/page/1") => {
                    let mut reply = running_reply("q1");
                    reply["state"] = json!("Succeeded");
                    reply["next_uri"] = Value::Null;
                    MockReply::json(reply)
                }
                _ => ok_reply(),
            },
        )
        .await;
    let client = query_id_client(&server, false).await;

    // Dropping a result (e.g. Ctrl-C in the CLI) may leave the query running.
    drop(client.start_query("SELECT 1", false, None).await.unwrap());
    client.kill_query("q1").await.unwrap();
    assert_eq!(
        requests(&server).last().unwrap(),
        &("POST".to_string(), "/v1/query/q1/kill".to_string())
    );

    // Reading to the end proves the query finished.
    client.query_all("SELECT 1", None).await.unwrap();
    client.kill_query("q1").await.unwrap();
    let recorded = server.requests.lock();
    let kill_sql = recorded
        .iter()
        .rev()
        .find(|r| r.path == "/v1/query")
        .unwrap();
    let body: Value = serde_json::from_slice(&kill_sql.body).unwrap();
    assert_eq!(body["sql"], "KILL QUERY 'q1'");
}

#[tokio::test]
async fn detached_kill_requires_a_supporting_server_version() {
    for (version, supported) in [("1.2.960", false), ("1.2.966", true)] {
        let server = MockServer::new(|_| MockReply::json(kill_statement_reply(None))).await;
        let mut client = Arc::try_unwrap(query_id_client(&server, false).await)
            .ok()
            .unwrap();
        let version: Version = version.parse().unwrap();
        client.capability = client.capability_for(&version);
        client.server_version = Some(version);
        let result = client.kill_query("other").await;
        if supported {
            assert!(!server.requests.lock().is_empty());
        } else {
            assert!(matches!(&result, Err(Error::BadArgument(msg)) if msg.contains("v1.2.965")));
            assert!(server.requests.lock().is_empty());
        }
    }
}

#[tokio::test]
async fn invalid_kill_ids_are_rejected_before_sending() {
    let server = MockServer::new(|_| panic!("request must not be sent")).await;
    let client = query_id_client(&server, false).await;
    for id in ["", "a\nb", "a\0b"] {
        assert!(matches!(
            client.kill_query(id).await,
            Err(Error::BadArgument(_))
        ));
    }
    assert!(server.requests.lock().is_empty());
}

#[test]
fn submitted_id_is_owned_before_it_is_visible() {
    let client = APIClient::default();
    let submission = QuerySubmission::new(&client, "q".into());
    // Any reader that sees the ID must find it owned.
    assert_eq!(client.last_query_id().as_deref(), Some("q"));
    assert!(client.owned_queries.lock().kill_target("q").is_some());
    drop(submission);
    // An unsent ID is no longer visible nor owned.
    assert_eq!(client.last_query_id(), None);
    assert!(client.owned_queries.lock().kill_target("q").is_none());
}

#[tokio::test]
async fn undecodable_first_response_keeps_the_query_owned() {
    for cloud in [false, true] {
        let server = MockServer::new(|request| match request.path.as_str() {
            "/v1/query" => MockReply {
                status: 200,
                body: "not json".into(),
                headers: vec![],
            },
            path if path.ends_with("/kill") => ok_reply(),
            path => panic!("unexpected {path}"),
        })
        .await;
        let client = query_id_client(&server, cloud).await;
        assert!(client.start_query("SELECT 1", false, None).await.is_err());
        let sent_id = client.last_query_id().unwrap();

        // The server may be running the query: cancel it through its session.
        client.kill_query(&sent_id).await.unwrap();
        assert_eq!(
            requests(&server).last().unwrap(),
            &("POST".to_string(), format!("/v1/query/{sent_id}/kill"))
        );
    }
}

#[tokio::test]
async fn detached_kill_is_not_replayed_after_its_response_began() {
    for cloud in [false, true] {
        let server = MockServer::new(|_| {
            let mut reply = MockReply::json(kill_statement_reply(None));
            // The server ran the statement, but the body is cut short.
            reply.headers.push(("X-Mock-Truncate", "1".into()));
            reply
        })
        .await;
        let client = query_id_client(&server, cloud).await;
        let err = client.kill_query("other").await.unwrap_err();
        // A replay could report 1053 for a kill that already succeeded.
        assert_eq!(requests(&server).len(), 1, "cloud={cloud}: {err:?}");
        let message = format!("{err}");
        assert!(
            message.contains("query/kill") && message.contains("other"),
            "{message}"
        );
    }
}

#[tokio::test]
async fn detached_kill_is_not_replayed_after_a_timeout() {
    let server = MockServer::new(|_| {
        let mut reply = MockReply::json(kill_statement_reply(None));
        // The server applies the kill, but answers after the client timeout.
        reply.headers.push(("X-Mock-Delay", "1500".into()));
        reply
    })
    .await;
    let mut client = APIClient::from_dsn(&format!(
        "databend://root:@127.0.0.1:{}/default?sslmode=disable&retry_delay_secs=0&page_request_timeout_secs=1",
        server.url.port().unwrap()
    ))
    .await
    .unwrap();
    client.build_client(None).await.unwrap();
    assert!(client.retry_count > 1, "native retries timeouts by default");
    assert!(client.kill_query("other").await.is_err());
    assert_eq!(
        requests(&server).len(),
        1,
        "a timed-out KILL must not be resent"
    );
}

#[test]
fn only_query_not_found_404s_are_recognized() {
    let query_body = r#"{"error":{"code":404,"message":"Query ID q not found on node n1"}}"#;
    for (error, expected) in [
        // Native /kill keeps the raw body.
        (Error::response_error(StatusCode::NOT_FOUND, query_body.as_bytes()), true),
        (
            Error::response_error(StatusCode::NOT_FOUND, br#"{"error":"QueryNotFound"}"#),
            true,
        ),
        (
            APIClient::default()
                .status_body_to_error(StatusCode::NOT_FOUND, query_body.as_bytes())
                .with_context(RequestKind::QueryKill),
            true,
        ),
        (Error::QueryNotFound("q".into()), true),
        // Routing errors and anything not about the query must surface.
        (
            Error::response_error(
                StatusCode::NOT_FOUND,
                br#"{"error":{"code":404,"message":"Routing error: query q should be on server n2"}}"#,
            ),
            false,
        ),
        (Error::response_error(StatusCode::NOT_FOUND, b"Not Found"), false),
        (Error::response_error(StatusCode::BAD_REQUEST, query_body.as_bytes()), false),
    ] {
        assert_eq!(is_query_not_found(&error), expected, "{error:?}");
    }
}

#[tokio::test]
async fn cloud_org_not_found_from_an_early_kill_reaches_the_caller() {
    let gate = Arc::new(Notify::new());
    let kills = Arc::new(AtomicUsize::new(0));
    let counted = kills.clone();
    let server = MockServer::with_query_gate(
        move |request| match request.path.as_str() {
            "/v1/query" => MockReply::json(running_reply("q1")),
            path if path.ends_with("/kill") => {
                counted.fetch_add(1, Ordering::SeqCst);
                let mut reply = MockReply::json(json!({"error": "OrgNotFound"}));
                reply.status = 404;
                reply
            }
            path => panic!("unexpected {path}"),
        },
        Some(gate.clone()),
    )
    .await;
    let client = query_id_client(&server, true).await;
    let query_client = client.clone();
    let query =
        tokio::spawn(async move { query_client.start_query("SELECT 1", false, None).await });
    let sent_id = received_query_id(&server).await;
    let err = client.kill_query(&sent_id).await.unwrap_err();
    assert!(format!("{err}").contains("Organization ID"), "{err}");
    // A non-query 404 is final: no retry and nothing sent later.
    gate.notify_one();
    let pages = tokio::time::timeout(Duration::from_secs(5), query)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(kills.load(Ordering::SeqCst), 1);
    drop(pages);
}

#[tokio::test]
async fn corrupt_arrow_batch_still_records_the_server_query_id() {
    use arrow_array::Int64Array;
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    let header = running_reply("server-id").to_string();
    let schema = Arc::new(ArrowSchema::new_with_metadata(
        vec![Field::new("n", DataType::Int64, false)],
        HashMap::from([("response_header".to_string(), header)]),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(0..1000))],
    )
    .unwrap();
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    // Cut into the batch: the schema and its response_header stay intact.
    bytes.truncate(bytes.len() - 100);

    let client = APIClient::default();
    let submission = QuerySubmission::new(&client, "client-id".into());
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(CONTENT_TYPE_ARROW));
    let response = HttpResponseData {
        status: StatusCode::OK,
        headers,
        body: Bytes::from(bytes),
    };
    let err = client
        .handle_page(response, Some("client-id"))
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("arrow batch"), "{err}");
    assert_eq!(client.last_query_id().as_deref(), Some("server-id"));
    drop(submission);
    // Both IDs now reach the server-side query.
    let owned = client.owned_queries.lock();
    for id in ["server-id", "client-id"] {
        assert_eq!(owned.kill_target(id).unwrap().query_id, "server-id");
    }
}

/// Start a query whose first response is held back by `gate`.
async fn pending_submission(
    server: &MockServer,
    client: &Arc<APIClient>,
) -> (JoinHandle<Result<Pages>>, String) {
    let query_client = client.clone();
    let query =
        tokio::spawn(async move { query_client.start_query("SELECT 1", false, None).await });
    (query, received_query_id(server).await)
}

async fn wait_for_kills(kills: &AtomicUsize, count: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while kills.load(Ordering::SeqCst) < count {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("expected kill requests");
}

#[tokio::test]
async fn kill_retries_until_the_query_is_registered() {
    for cloud in [false, true] {
        let kills = Arc::new(AtomicUsize::new(0));
        let counted = kills.clone();
        let server = MockServer::with_query_gate(
            move |request| match request.path.as_str() {
                "/v1/query" => MockReply::json(running_reply("q1")),
                path if path.ends_with("/kill") => {
                    // Not registered for the first two attempts.
                    if counted.fetch_add(1, Ordering::SeqCst) < 2 {
                        not_found_reply()
                    } else {
                        ok_reply()
                    }
                }
                path => panic!("unexpected {path}"),
            },
            // The first response never arrives.
            Some(Arc::new(Notify::new())),
        )
        .await;
        let client = query_id_client(&server, cloud).await;
        let (query, sent_id) = pending_submission(&server, &client).await;

        client.kill_query(&sent_id).await.unwrap();
        let expected = format!("/v1/query/{sent_id}/kill");
        let kill_paths: Vec<_> = requests(&server)
            .into_iter()
            .filter(|(_, path)| path.ends_with("/kill"))
            .map(|(_, path)| path)
            .collect();
        assert_eq!(kill_paths, [expected.clone(), expected.clone(), expected]);
        query.abort();
    }
}

#[tokio::test]
async fn kill_follows_the_server_id_from_the_first_response() {
    for cloud in [false, true] {
        let gate = Arc::new(Notify::new());
        let kills = Arc::new(AtomicUsize::new(0));
        let counted = kills.clone();
        let server = MockServer::with_query_gate(
            move |request| {
                if request.path.ends_with("/kill") {
                    counted.fetch_add(1, Ordering::SeqCst);
                }
                match request.path.as_str() {
                    // The gateway rewrites the client ID and does not know it.
                    "/v1/query" => MockReply::json(running_reply("q1")),
                    "/v1/query/q1/kill" => ok_reply(),
                    path if path.ends_with("/kill") => not_found_reply(),
                    path => panic!("unexpected {path}"),
                }
            },
            Some(gate.clone()),
        )
        .await;
        let client = query_id_client(&server, cloud).await;
        let (query, sent_id) = pending_submission(&server, &client).await;

        let kill_client = client.clone();
        let kill_id = sent_id.clone();
        let kill = tokio::spawn(async move { kill_client.kill_query(&kill_id).await });
        wait_for_kills(&kills, 1).await;
        assert!(
            !kill.is_finished(),
            "kill_query waits instead of acknowledging"
        );
        gate.notify_one();
        tokio::time::timeout(Duration::from_secs(5), kill)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let kills: Vec<(String, Option<String>)> = server
            .requests
            .lock()
            .iter()
            .filter(|r| r.path.ends_with("/kill"))
            .map(|r| {
                (
                    r.path.clone(),
                    r.headers.get("x-databend-sticky-node").cloned(),
                )
            })
            .collect();
        let (last, earlier) = kills.split_last().unwrap();
        assert_eq!(
            last,
            &("/v1/query/q1/kill".to_string(), Some("n1".to_string()))
        );
        let client_kill = format!("/v1/query/{sent_id}/kill");
        assert!(earlier.iter().all(|(path, _)| *path == client_kill));
        drop(query.await.unwrap().unwrap());
    }
}

#[tokio::test]
async fn kill_reports_not_found_when_the_submission_ends_while_retrying() {
    let kills = Arc::new(AtomicUsize::new(0));
    let counted = kills.clone();
    let server = MockServer::with_query_gate(
        move |request| match request.path.as_str() {
            "/v1/query" => MockReply::json(running_reply("q1")),
            _ => {
                counted.fetch_add(1, Ordering::SeqCst);
                not_found_reply()
            }
        },
        Some(Arc::new(Notify::new())),
    )
    .await;
    let client = query_id_client(&server, false).await;
    let (query, sent_id) = pending_submission(&server, &client).await;
    let kill_client = client.clone();
    let kill = tokio::spawn(async move { kill_client.kill_query(&sent_id).await });
    wait_for_kills(&kills, 1).await;
    // The caller gives up on the query; the kill must not keep waiting.
    query.abort();
    let _ = query.await;
    let result = tokio::time::timeout(Duration::from_secs(5), kill)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err_and(|e| is_query_not_found(&e)));
}

#[tokio::test]
async fn kill_gives_up_after_the_registration_wait() {
    let server = MockServer::with_query_gate(
        |request| match request.path.as_str() {
            "/v1/query" => MockReply::json(running_reply("q1")),
            _ => not_found_reply(),
        },
        Some(Arc::new(Notify::new())),
    )
    .await;
    let mut client = Arc::try_unwrap(query_id_client(&server, false).await)
        .ok()
        .unwrap();
    // The first response is due within wait_time_secs, plus the slack.
    client.wait_time_secs = Some(0);
    let client = Arc::new(client);
    let (query, sent_id) = pending_submission(&server, &client).await;
    let start = Instant::now();
    let result = client.kill_query(&sent_id).await;
    let elapsed = start.elapsed();
    assert!(result.is_err_and(|e| is_query_not_found(&e)));
    assert!(elapsed >= KILL_REGISTRATION_SLACK - Duration::from_millis(100));
    assert!(
        elapsed < KILL_REGISTRATION_SLACK + Duration::from_secs(2),
        "{elapsed:?}"
    );
    query.abort();
}
