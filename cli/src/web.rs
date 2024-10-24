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

use actix_web::middleware::Logger;
use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use anyhow::Result;
use tokio::net::TcpListener;
use rust_embed::RustEmbed;

struct AppState {
    result: String,
}

#[get("/api/message")]
async fn get_message(data: web::Data<AppState>) -> impl Responder {
    let response = serde_json::json!({
        "result": data.result,
    });
    HttpResponse::Ok().json(response)
}

// Define the struct to embed the frontend build directory
#[derive(RustEmbed)]
#[folder = "frontend/build/"]
struct Asset;

// Serve embedded files from rust-embed
async fn embed_file(path: web::Path<String>) -> HttpResponse {
    let file = match Asset::get(&path) {
        Some(content) => HttpResponse::Ok().body(content.data),
        None => HttpResponse::NotFound().body("File not found"),
    };
    file
}

pub async fn start_server_and_open_browser<'a>(explain_result: String) -> Result<()> {
    let port = find_available_port(8080).await;
    let server = tokio::spawn(async move {
        start_server(port, explain_result.to_string()).await;
    });

    // Open the browser in a separate task
    tokio::spawn(async move {
        if webbrowser::open(&format!("http://127.0.0.1:{}", port)).is_ok() {
            // eprintln!("Browser opened successfully at http://127.0.0.1:{}", port);
        } else {
            println!("Failed to open browser.");
        }
    });

    // Continue with the rest of the code
    server.await.expect("Server task failed");

    Ok(())
}

pub async fn start_server<'a>(port: u16, result: String) {
    let app_state = web::Data::new(AppState {
        result: result.clone(),
    });

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(app_state.clone())
            .service(get_message)
            .route("/{filename:.*}", web::get().to(embed_file))
    })
    .bind(("127.0.0.1", port))
    .expect("Cannot bind to port")
    .run()
    .await
    .expect("Server run failed");
}

async fn find_available_port(start: u16) -> u16 {
    let mut port = start;
    loop {
        if TcpListener::bind(("127.0.0.1", port)).await.is_ok() {
            return port;
        }
        port += 1;
    }
}
