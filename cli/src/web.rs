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
use std::net::TcpListener;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};

use crate::sql_parser::parse_sql_for_web;
use actix_web::dev::Server;
use actix_web::middleware::Logger;
use actix_web::web::Query;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use databend_driver::{Client, RowWithStats};
use mime_guess::from_path;
use once_cell::sync::Lazy;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

#[derive(RustEmbed)]
#[folder = "frontend/build/"]
struct Asset;

async fn embed_file(path: web::Path<String>) -> HttpResponse {
    let file_path = if path.is_empty() {
        "index.html".to_string()
    } else {
        let requested_path = path.into_inner();

        if requested_path.starts_with("perf/") {
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

static PERF_ID: AtomicUsize = AtomicUsize::new(0);
static APP_DATA: Lazy<Arc<Mutex<HashMap<usize, String>>>> =
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

// Storage for shared queries using actual query IDs
static SHARED_QUERIES: Lazy<Arc<Mutex<HashMap<String, SharedQuery>>>> =
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SharedQuery {
    sql: String,
    results: Vec<QueryResult>,
}

#[derive(Deserialize, Debug)]
struct MessageQuery {
    perf_id: Option<String>,
}

#[derive(Deserialize, Debug)]
struct QueryRequest {
    sql: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct QueryResult {
    columns: Vec<String>,
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

pub fn set_data(result: String) -> usize {
    let perf_id = PERF_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let l = APP_DATA.as_ref();
    l.lock().unwrap().insert(perf_id, result);
    perf_id
}

#[post("/api/query")]
async fn execute_query(req: web::Json<QueryRequest>) -> impl Responder {
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

    let sql = req.sql.trim();
    if sql.is_empty() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "SQL query cannot be empty"
        }));
    }

    // Parse SQL into multiple statements using proper tokenizer
    let statements = parse_sql_for_web(sql);

    if statements.is_empty() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "No valid SQL statements found"
        }));
    }

    let mut results = Vec::new();
    // use one client for each http query
    let client = Client::new(dsn.clone());
    let mut last_query_id = None;
    for statement in &statements {
        let start_time = std::time::Instant::now();

        match client.get_conn().await {
            Ok(conn) => {
                match conn.query_iter_ext(statement).await {
                    Ok(mut rows) => {
                        let mut data = Vec::new();
                        let mut columns = Vec::new();
                        let mut row_count = 0;

                        while let Some(row_result) = rows.next().await {
                            match row_result {
                                Ok(row_with_stats) => {
                                    match row_with_stats {
                                        RowWithStats::Row(row) => {
                                            if columns.is_empty() && !row.is_empty() {
                                                // Extract column names from schema
                                                let schema = row.schema();
                                                for field in schema.fields().iter() {
                                                    columns.push(field.name.clone());
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
                                Err(e) => {
                                    return HttpResponse::InternalServerError().json(
                                        serde_json::json!({
                                            "error": format!("Error processing row: {}", e)
                                        }),
                                    );
                                }
                            }
                        }

                        let duration = format!("{}ms", start_time.elapsed().as_millis());
                        last_query_id = conn.last_query_id();
                        results.push(QueryResult {
                            columns,
                            data,
                            row_count,
                            duration,
                        });
                    }
                    Err(e) => {
                        return HttpResponse::InternalServerError().json(serde_json::json!({
                            "error": format!("Query execution failed: {}", e)
                        }));
                    }
                }
            }
            Err(e) => {
                return HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": format!("Failed to create database connection: {}", e)
                }));
            }
        }
    }

    if let Some(ref last_id) = last_query_id {
        let shared_query = SharedQuery {
            sql: sql.to_string(),
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
    HttpServer::new(move || {
        App::new()
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
