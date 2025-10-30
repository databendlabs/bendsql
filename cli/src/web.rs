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

use std::collections::HashMap;
use std::env;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use crate::sql_parser::parse_sql_for_web;
use actix_session::config::PersistentSession;
use actix_session::storage::CookieSessionStore;
use actix_session::{Session, SessionMiddleware};
use actix_web::cookie::time::Duration;
use actix_web::cookie::Key;
use actix_web::dev::Server;
use actix_web::middleware::Logger;
use actix_web::web::Query;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use dashmap::DashMap;
use databend_driver::Connection;
use databend_driver::{Client, RowWithStats};
use databend_driver_core::error::Result;
use mime_guess::from_path;
use once_cell::sync::Lazy;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

#[derive(RustEmbed)]
#[folder = "frontend/build/"]
struct Asset;

// Check if we're in development mode
fn is_dev_mode() -> bool {
    env::var("BENDSQL_DEV_MODE").unwrap_or_default() == "1"
}

// Development mode: proxy to Next.js dev server
async fn dev_proxy(path: web::Path<String>) -> HttpResponse {
    let dev_server_url =
        env::var("BENDSQL_DEV_SERVER").unwrap_or_else(|_| "http://localhost:3000".to_string());
    let full_path = path.into_inner();
    let url = if full_path.is_empty() {
        dev_server_url.clone()
    } else {
        format!("{}/{}", dev_server_url, full_path)
    };

    // Use reqwest to proxy the request
    match reqwest::get(&url).await {
        Ok(response) => {
            let status = response.status();
            let content_type = response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("text/html")
                .to_string();

            match response.bytes().await {
                Ok(body) => HttpResponse::build(
                    actix_web::http::StatusCode::from_u16(status.as_u16())
                        .unwrap_or(actix_web::http::StatusCode::OK),
                )
                .content_type(content_type)
                .body(body),
                Err(_) => HttpResponse::InternalServerError().body("Failed to read response"),
            }
        }
        Err(_) => {
            // If dev server is not running, show helpful message
            let dev_help = format!(
                r#"
<!DOCTYPE html>
<html>
<head>
    <title>BendSQL Development Mode</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        .container {{ max-width: 800px; margin: 0 auto; }}
        .info {{ background: #e3f2fd; border: 1px solid #2196f3; padding: 20px; border-radius: 5px; margin: 20px 0; }}
        pre {{ background: #f5f5f5; padding: 10px; border-radius: 5px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>BendSQL Development Mode</h1>
        <div class="info">
            <h3>Frontend Development Server Not Running</h3>
            <p>To start the frontend development server:</p>
            <pre>cd frontend && npm start</pre>
            <p>Or set a custom dev server URL:</p>
            <pre>export BENDSQL_DEV_SERVER=http://localhost:3001</pre>
            <p>Current dev server URL: <code>{}</code></p>
        </div>
        <p>For production mode, run: <code>make build-frontend && cargo run</code></p>
    </div>
</body>
</html>"#,
                dev_server_url
            );

            HttpResponse::Ok().content_type("text/html").body(dev_help)
        }
    }
}

async fn embed_file(path: web::Path<String>) -> HttpResponse {
    // In development mode, proxy to Next.js dev server
    if is_dev_mode() {
        return dev_proxy(path).await;
    }

    // Production mode: serve embedded files
    let file_path = if path.is_empty() {
        "index.html".to_string()
    } else {
        let requested_path = path.into_inner();
        if requested_path == "perf" || requested_path.starts_with("perf/") {
            // Handle Next.js static export structure for /perf/ routes
            // trailingSlash: false generates perf/[...slug].html
            "perf/[...slug].html".to_string()
        } else if requested_path
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
            && requested_path.len() >= 3
        {
            // Handle query IDs - use catch-all route
            // trailingSlash: false generates [...slug].html
            "[...slug].html".to_string()
        } else {
            requested_path
        }
    };

    match Asset::get(&file_path) {
        Some(content) => {
            let mime_type = from_path(&file_path).first_or_octet_stream();
            HttpResponse::Ok()
                .content_type(mime_type.as_ref())
                .body(content.data)
        }
        None => {
            // If file not found and it doesn't look like a static file, try index.html for SPA routing
            if !file_path.contains('.') && file_path != "index.html" {
                match Asset::get("index.html") {
                    Some(content) => {
                        let mime_type = from_path("index.html").first_or_octet_stream();
                        HttpResponse::Ok()
                            .content_type(mime_type.as_ref())
                            .body(content.data)
                    }
                    None => HttpResponse::NotFound().body("File not found"),
                }
            } else {
                HttpResponse::NotFound().body("File not found")
            }
        }
    }
}

static APP_DATA: Lazy<Arc<Mutex<HashMap<usize, String>>>> =
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

// Storage for shared queries using actual query IDs
static SHARED_QUERIES: Lazy<Arc<Mutex<HashMap<String, SharedQuery>>>> =
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

// Store connections by session ID
static SESSION_CONNECTIONS: Lazy<Arc<DashMap<String, Connection>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

const SESSION_ID_KEY: &str = "session_id";

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SharedQuery {
    sql: String,
    kind: i32,
    results: Vec<QueryResult>,
}

#[derive(Deserialize, Debug)]
struct MessageQuery {
    perf_id: Option<String>,
}

#[derive(Deserialize, Debug)]
struct QueryRequest {
    sql: String,
    // default 0: query, 1: EXPLAIN ANALYZE GRAPHICAL, 2: EXPLAIN PERF
    kind: i32,
}

impl QueryRequest {
    fn to_sql(&self) -> String {
        match self.kind {
            1 => format!("EXPLAIN ANALYZE GRAPHICAL {}", self.sql),
            2 => format!("EXPLAIN PERF {}", self.sql),
            _ => self.sql.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct QueryResult {
    columns: Vec<String>,
    types: Vec<String>,
    data: Vec<Vec<String>>,
    #[serde(rename = "rowCount")]
    row_count: usize,
    duration: String,
}

#[derive(Serialize, Debug)]
struct QueryResponse {
    results: Vec<QueryResult>,
    #[serde(rename = "queryId")]
    query_id: Option<String>,
}

static DSN: Lazy<Arc<Mutex<Option<String>>>> = Lazy::new(|| Arc::new(Mutex::new(None)));

pub fn set_dsn(dsn: String) {
    let dsn_guard = DSN.as_ref();
    *dsn_guard.lock().unwrap() = Some(dsn);
}

#[post("/api/query")]
async fn execute_query(session: Session, req: web::Json<QueryRequest>) -> impl Responder {
    let dsn = {
        let dsn_guard = DSN.as_ref();
        let dsn_option = dsn_guard.lock().unwrap();

        match dsn_option.as_ref() {
            Some(dsn) => dsn.clone(),
            None => {
                return HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": "Database DSN not available"
                }));
            }
        }
    }; // Lock is automatically dropped here

    let sql = req.to_sql();
    if sql.is_empty() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "SQL query cannot be empty"
        }));
    }

    // Parse SQL into multiple statements using proper tokenizer
    let statements = parse_sql_for_web(&sql);

    if statements.is_empty() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "No valid SQL statements found"
        }));
    }

    // Use session id to store connections and prevent duplicate connections.
    let session_id = if let Ok(Some(session_id)) = session.get::<String>(SESSION_ID_KEY) {
        session_id
    } else {
        let session_id = uuid::Uuid::new_v4().to_string();
        let _ = session.insert::<String>(SESSION_ID_KEY, session_id.clone());
        session_id
    };

    let (last_query_id, results) = match run_query(&dsn, &session_id, &statements).await {
        Ok((last_query_id, results)) => (last_query_id, results),
        Err(err) => {
            // If connection is unauthenticated, generate a new session id,
            // and try to reconnet to the server.
            if err.is_unauthenticated() {
                SESSION_CONNECTIONS.remove(&session_id);
                let new_session_id = uuid::Uuid::new_v4().to_string();
                let _ = session.insert::<String>(SESSION_ID_KEY, new_session_id.clone());

                let (last_query_id, results) =
                    match run_query(&dsn, &new_session_id, &statements).await {
                        Ok((last_query_id, results)) => (last_query_id, results),
                        Err(err) => {
                            return HttpResponse::InternalServerError().json(serde_json::json!({
                                "error": format!("Query execution failed: {}", err)
                            }));
                        }
                    };
                (last_query_id, results)
            } else {
                return HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": format!("Query execution failed: {}", err)
                }));
            }
        }
    };

    if let Some(ref last_id) = last_query_id {
        let shared_query = SharedQuery {
            sql: req.sql.clone(),
            kind: req.kind,
            results: results.clone(),
        };
        // Store the query for sharing
        {
            let shared_queries_guard = SHARED_QUERIES.as_ref();
            shared_queries_guard
                .lock()
                .unwrap()
                .insert(last_id.clone(), shared_query);
        }
    }
    HttpResponse::Ok().json(QueryResponse {
        results,
        query_id: last_query_id,
    })
}

async fn run_query(
    dsn: &str,
    session_id: &str,
    statements: &Vec<String>,
) -> Result<(Option<String>, Vec<QueryResult>)> {
    if !SESSION_CONNECTIONS.contains_key(session_id) {
        let client = Client::new(dsn.to_string());
        let conn = client.get_conn().await?;
        SESSION_CONNECTIONS.insert(session_id.to_string(), conn);
    }
    let conn = SESSION_CONNECTIONS.get(session_id).unwrap();

    let mut last_query_id = None;
    let mut results = Vec::new();
    for statement in statements {
        let start_time = std::time::Instant::now();
        let rows = &mut conn.query_iter_ext(statement).await?;
        let mut data = Vec::new();
        let mut columns = Vec::new();
        let mut types = Vec::new();
        let mut row_count = 0;

        while let Some(row_result) = rows.next().await {
            let row_with_stats = row_result?;
            match row_with_stats {
                RowWithStats::Row(row) => {
                    if columns.is_empty() && !row.is_empty() {
                        // Extract column names from schema
                        let schema = row.schema();
                        for field in schema.fields().iter() {
                            columns.push(field.name.clone());
                            types.push(field.data_type.to_string());
                        }
                    }

                    // Convert row values to string array
                    let mut row_values = Vec::new();
                    for value in row.values() {
                        let str_value = value.to_string();
                        row_values.push(str_value);
                    }
                    data.push(row_values);
                    row_count += 1;
                }
                RowWithStats::Stats(_stats) => {
                    // Skip stats for now, we could use them for additional info
                    continue;
                }
            }
        }

        let duration = format!("{}ms", start_time.elapsed().as_millis());
        last_query_id = conn.last_query_id();
        results.push(QueryResult {
            columns,
            types,
            data,
            row_count,
            duration,
        });
    }

    Ok((last_query_id, results))
}

#[get("/api/query/{query_id}")]
async fn get_shared_query(path: web::Path<String>) -> impl Responder {
    let query_id = path.into_inner();

    let shared_queries_guard = SHARED_QUERIES.as_ref();
    let shared_queries = shared_queries_guard.lock().unwrap();

    match shared_queries.get(&query_id) {
        Some(shared_query) => HttpResponse::Ok().json(shared_query),
        None => HttpResponse::NotFound().json(serde_json::json!({
            "error": format!("Query ID '{}' not found", query_id)
        })),
    }
}

#[get("/api/message")]
async fn get_message(query: Query<MessageQuery>) -> impl Responder {
    query
        .perf_id
        .as_deref()
        .unwrap_or("")
        .parse::<usize>()
        .ok()
        .and_then(|id| {
            APP_DATA.as_ref().lock().unwrap().get(&id).map(|result| {
                HttpResponse::Ok().json(serde_json::json!({
                    "result": result,
                }))
            })
        })
        .unwrap_or_else(|| {
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Perf ID {:?} not found", query.perf_id),
            }))
        })
}

pub fn start_server(listener: TcpListener) -> Server {
    let secret_key = Key::generate();

    HttpServer::new(move || {
        App::new()
            .wrap(
                SessionMiddleware::builder(CookieSessionStore::default(), secret_key.clone())
                    .cookie_name("bendsql_session".to_string())
                    .cookie_secure(false)
                    .session_lifecycle(
                        PersistentSession::default().session_ttl(Duration::minutes(60)),
                    )
                    .build(),
            )
            .wrap(Logger::default())
            .service(get_message)
            .service(execute_query)
            .service(get_shared_query)
            .route("/{filename:.*}", web::get().to(embed_file))
    })
    .listen(listener)
    .unwrap_or_else(|e| panic!("Cannot listen to address: {e}"))
    .run()
}
