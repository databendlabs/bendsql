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
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};

use actix_web::dev::Server;
use actix_web::middleware::Logger;
use actix_web::web::Query;
use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use mime_guess::from_path;
use once_cell::sync::Lazy;
use rust_embed::RustEmbed;
use serde::Deserialize;
use tokio::net::TcpListener;

#[derive(RustEmbed)]
#[folder = "frontend/build/"]
struct Asset;

async fn embed_file(path: web::Path<String>) -> HttpResponse {
    let file_path = if path.is_empty() {
        "index.html".to_string()
    } else {
        path.into_inner()
    };

    match Asset::get(&file_path) {
        Some(content) => {
            let mime_type = from_path(&file_path).first_or_octet_stream();
            HttpResponse::Ok()
                .content_type(mime_type.as_ref())
                .body(content.data)
        }
        None => HttpResponse::NotFound().body("File not found"),
    }
}

static PERF_ID: AtomicUsize = AtomicUsize::new(0);

static APP_DATA: Lazy<Arc<Mutex<HashMap<usize, String>>>> =
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

#[derive(Deserialize, Debug)]
struct MessageQuery {
    perf_id: Option<String>,
}

pub fn set_data(result: String) -> usize {
    let perf_id = PERF_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let l = APP_DATA.as_ref();
    l.lock().unwrap().insert(perf_id, result);
    perf_id
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

pub fn start_server<'a>(addr: &str, port: u16) -> Server {
    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .service(get_message)
            .route("/{filename:.*}", web::get().to(embed_file))
    })
    .bind((addr, port))
    .expect("Cannot bind to port")
    .run()
}

pub async fn find_available_port(start: u16) -> u16 {
    let mut port = start;
    loop {
        if TcpListener::bind(("127.0.0.1", port)).await.is_ok() {
            return port;
        }
        port += 1;
    }
}
