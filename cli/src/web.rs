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

use rusqlite::{params, Connection};
use std::env;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use crate::sql_parser::parse_sql_for_web;
use actix_web::dev::Server;
use actix_web::middleware::Logger;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use databend_driver::{Client, RowWithStats};
use mime_guess::from_path;
use once_cell::sync::Lazy;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::process::Command as StdCommand;
use std::time::Instant;
use tempfile::tempdir;
use tokio::process::Command;
use tokio_stream::StreamExt;
use uuid::Uuid;

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
        } else if requested_path == "notebooks" || requested_path.starts_with("notebooks/") {
            // Static notebooks page
            "notebooks.html".to_string()
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

// SQLite database for persistent query storage
static DB: Lazy<Arc<Mutex<Connection>>> = Lazy::new(|| {
    let home_dir = dirs::home_dir().expect("Failed to get home directory");
    let bendsql_dir = home_dir.join(".bendsql");
    std::fs::create_dir_all(&bendsql_dir).expect("Failed to create bendsql directory");

    let db_path = bendsql_dir.join("queries.db");
    let conn = Connection::open(&db_path).expect("Failed to open SQLite database");

    // Create table if not exists
    conn.execute(
        "CREATE TABLE IF NOT EXISTS shared_queries (
            query_id TEXT PRIMARY KEY,
            sql TEXT NOT NULL,
            kind INTEGER NOT NULL,
            results TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )",
        [],
    )
    .expect("Failed to create shared_queries table");

    // Create index for better performance
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_created_at ON shared_queries(created_at)",
        [],
    )
    .expect("Failed to create index");

    // Clean up old queries (older than 90 days)
    let _ = conn.execute(
        "DELETE FROM shared_queries WHERE created_at < datetime('now', '-90 days')",
        [],
    );

    Arc::new(Mutex::new(conn))
});

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SharedQuery {
    sql: String,
    kind: i32,
    results: Vec<QueryResult>,
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

    if req.kind == 3 {
        return run_python_script(&req.sql, &dsn)
            .await
            .unwrap_or_else(|err| err);
    }

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

    let mut results = Vec::new();
    // use one client for each http query
    let client = Client::new(dsn.clone());
    let conn = client.get_conn().await;
    let conn = match conn {
        Ok(conn) => conn,
        Err(e) => {
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Failed to create database connection: {}", e)
            }));
        }
    };
    let mut last_query_id = None;
    for statement in &statements {
        let start_time = std::time::Instant::now();
        let mut stats_running_time: Option<f64> = None;

        match conn.query_iter_ext(statement).await {
            Ok(mut rows) => {
                let mut data = Vec::new();
                let mut columns = Vec::new();
                let mut types = Vec::new();
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
                                RowWithStats::Stats(stats) => {
                                    stats_running_time = Some(stats.running_time_ms);
                                }
                            }
                        }
                        Err(e) => {
                            return HttpResponse::InternalServerError().json(serde_json::json!({
                                "error": format!("Error processing row: {}", e)
                            }));
                        }
                    }
                }

                let duration = if let Some(ms) = stats_running_time {
                    if ms.fract() == 0.0 {
                        format!("{:.0}ms", ms)
                    } else {
                        format!("{:.2}ms", ms)
                    }
                } else {
                    format!("{}ms", start_time.elapsed().as_millis())
                };
                last_query_id = conn.last_query_id();
                results.push(QueryResult {
                    columns,
                    types,
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

    if let Some(ref last_id) = last_query_id {
        let shared_query = SharedQuery {
            sql: req.sql.clone(),
            kind: req.kind,
            results: results.clone(),
        };

        // Store the query in SQLite database
        if let Ok(serialized_results) = serde_json::to_string(&shared_query.results) {
            let db_guard = DB.as_ref();
            let conn = db_guard.lock().unwrap();

            let _ = conn.execute(
                "INSERT OR REPLACE INTO shared_queries (query_id, sql, kind, results) VALUES (?1, ?2, ?3, ?4)",
                params![last_id, &shared_query.sql, shared_query.kind, serialized_results],
            );
        }
    }
    HttpResponse::Ok().json(QueryResponse {
        results,
        query_id: last_query_id,
    })
}

async fn run_python_script(code: &str, dsn: &str) -> Result<HttpResponse, HttpResponse> {
    match StdCommand::new("docker").arg("--version").output() {
        Ok(output) if output.status.success() => {}
        _ => {
            return Err(HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Docker is required to execute Python scripts. Please install Docker and try again."
            })));
        }
    }

    let dir = tempdir().map_err(|e| {
        HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to create temp directory: {}", e)
        }))
    })?;

    let script_path = dir.path().join("script.py");
    let mut file = File::create(&script_path).map_err(|e| {
        HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to write script: {}", e)
        }))
    })?;

    let bootstrap = format!(
        r##"# /// script
# requires-python = ">=3.12"
# dependencies = ["databend-driver"]
# ///
import asyncio
from databend_driver import AsyncDatabendClient, BlockingDatabendClient

_BENDSQL_DSN = {dsn}
async_client = AsyncDatabendClient(_BENDSQL_DSN)
client = BlockingDatabendClient(_BENDSQL_DSN)

"##,
        dsn = serde_json::to_string(dsn).unwrap_or_else(|_| "\"\"".to_string())
    );

    file.write_all(bootstrap.as_bytes()).map_err(|e| {
        HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to write bootstrap: {}", e)
        }))
    })?;
    file.write_all(code.as_bytes()).map_err(|e| {
        HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to write script: {}", e)
        }))
    })?;
    drop(file);

    let mount_arg = format!("{}:/workspace", dir.path().display());
    let start_time = Instant::now();
    let output = Command::new("docker")
        .arg("run")
        .arg("--rm")
        .arg("-v")
        .arg(&mount_arg)
        .arg("-w")
        .arg("/workspace")
        .arg("ghcr.io/astral-sh/uv:debian")
        .arg("uv")
        .arg("run")
        .arg("--script")
        .arg("/workspace/script.py")
        .output()
        .await
        .map_err(|e| {
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Failed to invoke Docker: {}", e)
            }))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Python execution failed: {}", stderr.trim())
        })));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let rows: Vec<Vec<String>> = stdout.lines().map(|line| vec![line.to_string()]).collect();

    let result = QueryResult {
        columns: vec!["stdout".to_string()],
        types: vec!["String".to_string()],
        data: rows.clone(),
        row_count: rows.len(),
        duration: format!("{}ms", start_time.elapsed().as_millis()),
    };
    let results_vec = vec![result.clone()];
    let query_id = Uuid::new_v4().to_string();

    if let Ok(serialized_results) = serde_json::to_string(&results_vec) {
        let db_guard = DB.as_ref();
        let conn = db_guard.lock().unwrap();
        let _ = conn.execute(
            "INSERT OR REPLACE INTO shared_queries (query_id, sql, kind, results) VALUES (?1, ?2, ?3, ?4)",
            params![query_id, code, 3, serialized_results],
        );
    }

    Ok(HttpResponse::Ok().json(QueryResponse {
        results: results_vec,
        query_id: Some(query_id),
    }))
}

#[get("/api/query/{query_id}")]
async fn get_shared_query(path: web::Path<String>) -> impl Responder {
    let query_id = path.into_inner();

    let db_guard = DB.as_ref();
    let conn = db_guard.lock().unwrap();

    let mut stmt = conn
        .prepare("SELECT sql, kind, results FROM shared_queries WHERE query_id = ?1")
        .unwrap();

    match stmt.query_row(params![&query_id], |row| {
        let sql: String = row.get(0)?;
        let kind: i32 = row.get(1)?;
        let results_json: String = row.get(2)?;

        let results: Vec<QueryResult> =
            serde_json::from_str(&results_json).map_err(|_| rusqlite::Error::InvalidQuery)?;

        Ok(SharedQuery { sql, kind, results })
    }) {
        Ok(shared_query) => HttpResponse::Ok().json(shared_query),
        Err(_) => HttpResponse::NotFound().json(serde_json::json!({
            "error": format!("Query ID '{}' not found", query_id)
        })),
    }
}

pub fn start_server(listener: TcpListener) -> Server {
    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .service(execute_query)
            .service(get_shared_query)
            .route("/{filename:.*}", web::get().to(embed_file))
    })
    .listen(listener)
    .unwrap_or_else(|e| panic!("Cannot listen to address: {e}"))
    .run()
}
